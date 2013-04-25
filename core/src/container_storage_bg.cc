/*
 * dedupv1 - iSCSI based Deduplication System for Linux
 *
 * (C) 2008 Dirk Meister
 * (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
 * (C) 2012 Dirk Meister, Johannes Gutenberg University Mainz
 *
 * This file is part of dedupv1.
 *
 * dedupv1 is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * dedupv1 is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with dedupv1. If not, see http://www.gnu.org/licenses/.
 */

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>

#include <sstream>
#include <list>

#include "dedupv1.pb.h"

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <core/storage.h>
#include <base/bitutil.h>
#include <base/index.h>
#include <base/hashing_util.h>
#include <base/strutil.h>
#include <base/crc32.h>
#include <base/logging.h>
#include <core/container.h>
#include <base/fileutil.h>
#include <base/timer.h>
#include <base/compress.h>
#include <core/container_storage_gc.h>
#include <core/container_storage_bg.h>
#include <core/container_storage.h>
#include <core/log.h>

#include <tbb/tick_count.h>

using std::pair;
using std::string;
using std::set;
using std::stringstream;
using dedupv1::base::timed_bool;
using dedupv1::base::TIMED_TRUE;
using dedupv1::base::TIMED_FALSE;
using dedupv1::base::TIMED_TIMEOUT;
using dedupv1::base::ScopedLock;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using std::make_pair;
using dedupv1::base::Option;
using dedupv1::base::make_option;

LOGGER("ContainerStorage");

namespace dedupv1 {
namespace chunkstore {

ContainerStorageBackgroundCommitter::ContainerStorageBackgroundCommitter() : handover_finished_barrier_(2) {
    this->storage_ = NULL;
    this->run_state_ = ContainerStorageBackgroundCommitter::CREATED;
    this->thread_count_ = kDefaultThreadCount;
    this->start_barrier_ = NULL;
    // we need to have the thread count before we know how
    // to init the start barrier.
}

timed_bool ContainerStorageBackgroundCommitter::ProcessContainer(Container* worker_container) {
    CHECK_RETURN(worker_container, TIMED_FALSE, "Worker container not set");

    pair<Container*, ContainerStorageAddressData> c;
    c.first = NULL;

    timed_bool result = this->handover_store_.Get(&c, 1);
    // The container has been removed from handover_store by this get
    if (result == TIMED_FALSE) {
        ERROR("Failed to wait for handover");
        return TIMED_FALSE;
    }
    if (result == TIMED_TIMEOUT) {
        // Check run state and try again
        return TIMED_TIMEOUT;
    }
    CHECK_RETURN(c.first, TIMED_FALSE, "Container not set during handover: " <<
        "get result " << result <<
        ", address " << c.second.ShortDebugString());
    this->stats_.threads_busy_count_++;
    uint64_t container_id = c.first->primary_id();
    TRACE("Commit thread got container " << container_id);

    ScopedLock scoped_lock(&this->handover_container_set_lock_);
    CHECK_RETURN(scoped_lock.AcquireLock(), TIMED_FALSE, "Failed to acquire set lock");
    this->current_handover_container_set_.insert(container_id); // the matching request thread still holds the lock
    CHECK_RETURN(scoped_lock.ReleaseLock(), TIMED_FALSE, "Failed to release set lock");

    // So now we can work
    worker_container->Reuse(container_id);
    if (!worker_container->CopyFrom(*c.first)) {
        WARNING("Cannot copy to handover container for container " << container_id << ": " << c.first->DebugString());
        CHECK_RETURN(this->handover_finished_barrier_.Wait(), TIMED_FALSE, "Cannot broadcast finished message");
        return TIMED_TRUE;
    }
    ContainerStorageAddressData address = c.second;
    TRACE("Commit thread finished copy container " << container_id);
    // c (the write container and other data) can be used for normal purposes now
    // DO NOT access c after this wait
    CHECK_RETURN(this->handover_finished_barrier_.Wait(), TIMED_FALSE, "Cannot broadcast finished message");

    // Now I commit the container
    if (!this->storage_->CommitContainer(worker_container, address)) {
        ERROR("Cannot commit container " << container_id << " (background)" <<
            ", container " << worker_container->DebugString() <<
            ", active commit threads " << this->stats_.threads_busy_count_);
    }

    // Declare that we are finished with the container
    CHECK_RETURN(scoped_lock.AcquireLock(), TIMED_FALSE, "Failed to acquire set lock");
    this->current_handover_container_set_.erase(container_id);
    CHECK_RETURN(scoped_lock.ReleaseLock(), TIMED_FALSE, "Failed to release set lock");
    TRACE("Commit thread finished committing container " << container_id);
    CHECK_RETURN(this->commit_finished_condition_.Broadcast(), TIMED_FALSE, "Cannot broadcast commit message");

    worker_container->Reuse(Storage::ILLEGAL_STORAGE_ADDRESS);
    this->stats_.threads_busy_count_--;

    // I'm ready for the next
    return TIMED_TRUE;
}

bool ContainerStorageBackgroundCommitter::SetOption(const string& option_name, const string& option) {
    CHECK(this->storage_ == NULL, "Background committer already started");

    if (option_name == "thread-count") {
        CHECK(To<size_t>(option).valid(), "Illegal option " << option);
        this->thread_count_ = To<size_t>(option).value();
        CHECK(this->thread_count_ > 0, "Illegal background thread count: " << this->thread_count_);
        return true;
    }
    ERROR("Illegal option: " << option_name);
    return false;
}

bool ContainerStorageBackgroundCommitter::Loop(size_t thread_id) {
    this->run_state_ = ContainerStorageBackgroundCommitter::RUNNING;

    TRACE("Running commit thread: thread id " << thread_id);
    CHECK(this->start_barrier_->Wait(), "Cannot wait for start barrier");
    while (this->run_state_ == ContainerStorageBackgroundCommitter::RUNNING) {
        Container* worker_container = this->current_container_[thread_id];
        ProcessContainer(worker_container);
        // I do not care about the result here. The show must go on
    }
    TRACE("Commit thread exited: thread id " << thread_id);
    return true;
}

timed_bool ContainerStorageBackgroundCommitter::Handover(Container* c, const ContainerStorageAddressData& address) {
    CHECK_RETURN(c,  TIMED_FALSE, "Container not set");
    CHECK_RETURN(this->run_state_,  TIMED_FALSE, "Background committer is not running");

    this->stats_.waiting_thread_count_++;
    tbb::tick_count start_tick = tbb::tick_count::now();

    TRACE("Handover : " << c->DebugString() << ", address " << address.ShortDebugString());

#ifndef NDEBUG
    // used only the TRACE statements
    uint64_t container_id = c->primary_id();
#endif
    ScopedLock scoped_handover_lock(&this->handover_lock_);
    // Only I communicate with the committer thread now
    // TODO(fermat): Have a look at the lock at the HandoverLockFree value. If this is relevant smaller 1 we could work on more concurrency here.
    CHECK_RETURN(scoped_handover_lock.AcquireLockWithStatistics(
            &storage_->stats_.handover_lock_free_,
            &storage_->stats_.handover_lock_busy_), TIMED_FALSE, "Cannot acquire handover lock");

    // Put container to the store, wait if handover container is used by other thread's container
    TRACE("Waiting for ready container");
    timed_bool result = this->handover_store_.Put(make_pair(c, address), 1);
    CHECK_RETURN(result != TIMED_FALSE, TIMED_FALSE, "Cannot handover container");
    if (result == TIMED_TIMEOUT) {
        DEBUG("Put Timeout");
        return TIMED_TIMEOUT;
    }
    // Now the container is copying the data

    // Wait until the container finished copying the data
    TRACE("Waiting for copying container " << container_id);
    CHECK_RETURN(this->handover_finished_barrier_.Wait(), TIMED_FALSE, "Handover copy wait failed");
    CHECK_RETURN(scoped_handover_lock.ReleaseLock(), TIMED_FALSE, "Cannot release handover lock");

    this->stats_.waiting_thread_count_--;
    tbb::tick_count end_tick = tbb::tick_count::now();
    this->stats_.average_waiting_time_.Add((end_tick - start_tick).seconds() * 1000);

    // Now the contained finished coping the data;
    // The container can be used normally now
    return TIMED_TRUE;
}

dedupv1::base::timed_bool ContainerStorageBackgroundCommitter::CommitFinishedConditionWaitTimeout(uint32_t s) {
    CHECK_RETURN(this->commit_finished_condition_lock_.AcquireLock(), TIMED_FALSE, "Failed to acquire lock");
    dedupv1::base::timed_bool b = commit_finished_condition_.ConditionWaitTimeout(&commit_finished_condition_lock_, s);
    CHECK_RETURN(this->commit_finished_condition_lock_.ReleaseLock(), TIMED_FALSE, "Failed to release lock");
    return b;
}

bool ContainerStorageBackgroundCommitter::Start(ContainerStorage* storage) {
    INFO("Starting background committer");
    CHECK(this->thread_count_ > 0, "Thread count cannot be zero");
    CHECK(this->thread_count_ <= kMaxThreads, "Thread count limited to at most " << kMaxThreads << " threads");
    CHECK(storage, "Storage not set");

    this->storage_ = storage;

    this->start_barrier_ = new dedupv1::base::Barrier(this->thread_count_ + 1);
    CHECK(this->start_barrier_, "Failed to create start barrier");

    this->current_container_.resize(this->thread_count_);
    for (size_t i = 0; i < this->thread_count_; i++) {
        this->current_container_[i] = new Container(Storage::ILLEGAL_STORAGE_ADDRESS,
            storage->GetContainerSize(), false);
    }
    this->run_state_ = STARTING;
    return true;
}

bool ContainerStorageBackgroundCommitter::Run() {
    CHECK(this->run_state_ == STARTING, "Illegal run state " << this->run_state_);

    DEBUG("Starting background committer");
    this->threads_.resize(this->thread_count_);
    for (size_t i = 0; i < this->thread_count_; i++) {
        this->threads_[i] = new Thread<bool>(NewRunnable(this, &ContainerStorageBackgroundCommitter::Loop, i),
                                             "commit " + ToString(i));
    }

    for (size_t i = 0; i < this->thread_count_; i++) {
        CHECK(this->threads_[i]->Start(), "Failed to start commit thread");
    }

    CHECK(this->start_barrier_->Wait(), "Cannot wait for start barrier");
    this->run_state_ = RUNNING;
    return true;
}

bool ContainerStorageBackgroundCommitter::Stop(const dedupv1::StopContext& stop_context) {
    DEBUG("Stopping background committer");
    if (this->run_state_ != RUNNING &&
        this->run_state_ != STARTING) {
        return true;
    }

    this->run_state_ = STOPPING; // now the thread should eventually stop

    for (size_t i = 0; i < this->threads_.size(); i++) {
        if (threads_[i]) {
            if (threads_[i]->IsStarted() || threads_[i]->IsFinished()) {
                bool thread_result = false;
                CHECK(this->threads_[i]->Join(&thread_result), "Cannot join background thread");
                CHECK(thread_result, "Background committer exited with error");
            }
        }
    }

    this->run_state_ = STOPPED;
    return true;
}

ContainerStorageBackgroundCommitter::~ContainerStorageBackgroundCommitter() {
    if (!this->Stop(dedupv1::StopContext::FastStopContext())) {
        ERROR("Failed to stop background committer");
    }
    for (size_t i = 0; i < this->current_container_.size(); i++) {
        delete this->current_container_[i];
        this->current_container_[i] = NULL;
    }
    this->current_container_.clear();
    for (size_t i = 0; i < this->threads_.size(); i++) {
        delete this->threads_[i];
        this->threads_[i] = NULL;
    }
    this->threads_.clear();
    if (this->start_barrier_) {
        delete this->start_barrier_;
        this->start_barrier_ = NULL;
    }
}

std::string ContainerStorageBackgroundCommitter::PrintEmbeddedTrace() {
    stringstream sstr;
    sstr << "\"busy commit threads\": " << this->stats_.threads_busy_count_ << "," << std::endl;
    sstr << "\"waiting threads\": " << this->stats_.waiting_thread_count_ << std::endl;
    return sstr.str();
}

std::string ContainerStorageBackgroundCommitter::PrintEmbeddedProfile() {
    stringstream sstr;
    sstr << "\"average handover latency\": " << this->stats_.average_waiting_time_.GetAverage() << std::endl;
    return sstr.str();
}

bool ContainerStorageBackgroundCommitter::WaitUntilProcessedContainerFinished() {
    ScopedLock scoped_lock(&this->handover_container_set_lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire set lock");

    set<uint64_t> handover_container_set = current_handover_container_set_;
    CHECK(scoped_lock.ReleaseLock(), "Failed to release set lock");

    while (!handover_container_set.empty()) {
        uint64_t address = *handover_container_set.begin();
        Option<bool> b = IsCurrentlyProcessedContainerId(address);
        CHECK(b.valid(), "Failed to check processing state");

        if (!b.value()) {
            handover_container_set.erase(address);
        }

        timed_bool tb = CommitFinishedConditionWaitTimeout(1);
        CHECK(tb != TIMED_FALSE, "Failed to wait for commit finish condition");
    }
    return true;
}

Option<bool> ContainerStorageBackgroundCommitter::IsCurrentlyProcessedContainerId(uint64_t address) {
    ScopedLock scoped_lock(&this->handover_container_set_lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire set lock");
    bool b = (this->current_handover_container_set_.find(address) != this->current_handover_container_set_.end());
    CHECK(scoped_lock.ReleaseLock(), "Failed to release set lock");
    return make_option(b);
}

ContainerStorageBackgroundCommitter::Statistics::Statistics() : average_waiting_time_(8) {
    this->threads_busy_count_ = 0;
    this->waiting_thread_count_ = 0;
}

}
}
