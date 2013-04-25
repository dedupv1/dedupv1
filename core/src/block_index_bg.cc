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

#include <core/block_index_bg.h>
#include <core/block_index.h>
#include <base/runnable.h>
#include <base/logging.h>
#include <base/strutil.h>

using dedupv1::base::strutil::ToString;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::ScopedLock;
using dedupv1::base::ThreadUtil;

LOGGER("BlockIndex");

namespace dedupv1 {
namespace blockindex {

BlockIndexBackgroundCommitter::BlockIndexBackgroundCommitter(
    BlockIndex* block_index,
    bool stop_mode) {
    this->block_index_ = block_index; // this call might be early. the object should not do more with the block_index than store the pointer
    this->state_ = CREATED;
    thread_count_ = 0;
    stop_mode_ = stop_mode;
}

bool BlockIndexBackgroundCommitter::Loop(int thread_id) {
    DEBUG("Start background committer thread " << thread_id);
    CHECK(this->lock_.AcquireLock(), "Failed to acquire block index bg lock");
    bool is_running = (this->state_ == RUNNING);
    CHECK(this->lock_.ReleaseLock(), "Failed to release block index bg lock");

    bool import_state = false;
    while (is_running) {
        bool imported = false;
        // do not wait in stop mode
        if (!this->block_index_->TryImportBlock(!stop_mode_).valid()) {
            WARNING("Failed to import ready block");
            ThreadUtil::Sleep(10);
        }
        if (imported != import_state) {
            if (imported) {
                DEBUG("Started importing ready blocks:" << this->block_index_->ready_queue_size());
            } else {
                DEBUG("Stopped importing ready blocks:" << this->block_index_->ready_queue_size());
            }
            import_state = imported;
        }

        // update is running state
        CHECK(this->lock_.AcquireLock(), "Failed to acquire block index bg lock");
        is_running = (this->state_ == RUNNING);
        CHECK(this->lock_.ReleaseLock(), "Failed to release block index bg lock");
    }
    DEBUG("Finished background committer thread " << thread_id);
    return true;
}

bool BlockIndexBackgroundCommitter::Start(int thread_count) {
    CHECK(this->block_index_, "Block index not set");
    CHECK(thread_count >= 1, "Illegal thread count");

    thread_count_ = thread_count;
    threads_.resize(thread_count_);

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire block index bg lock");
    CHECK(this->state_ == CREATED, "block index bg already started");

    threads_.resize(thread_count_);
    for (int i = 0; i < threads_.size(); i++) {
        threads_[i] = new Thread<bool>(NewRunnable(this, &BlockIndexBackgroundCommitter::Loop, i),
                                       "block index bg " + ToString(i));
    }
    this->state_ = STARTED;
    CHECK(scoped_lock.ReleaseLock(), "Failed to release block index bg lock");

    DEBUG("Starting block index background thread");
    return true;
}

bool BlockIndexBackgroundCommitter::Run() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire block index bg lock");
    if (this->state_ == STARTED) {
        for (int i = 0; i < threads_.size(); i++) {
            if (this->threads_[i]) {
                CHECK(this->threads_[i]->Start(),
                    "Failed to start block index bg thread");
            }
        }
        this->state_ = RUNNING;
    }
    // TODO (dmeister): Calling from Run from the Created state should fail
    CHECK(scoped_lock.ReleaseLock(), "Failed to release block index bg lock");
    return true;
}

bool BlockIndexBackgroundCommitter::Stop(const dedupv1::StopContext& stop_context) {
    DEBUG("Stopping background committer thread");
    ScopedLock scoped_lock(&this->lock_);

    CHECK(scoped_lock.AcquireLock(), "Failed to acquire block index bg lock");
    if (this->state_ == RUNNING) {
        this->state_ = STOPPED;
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release block index bg lock");

    bool failed = false;
    for (int i = 0; i < threads_.size(); i++) {
        if (threads_[i]) {
            bool thread_result = false;
            if (this->threads_[i]->IsStarted() || this->threads_[i]->IsFinished()) {
                DEBUG("Stopping block index background thread");
                if (!this->threads_[i]->Join(&thread_result)) {
                    ERROR("Failed to join thread");
                    failed = true;
                }
                if (!thread_result) {
                    ERROR("Thread existed with error");
                    failed = true;
                }
                DEBUG("Stopped block index background thread");
            }
            delete this->threads_[i];
            threads_[i] = NULL;
        }
    }
    threads_.clear();

    DEBUG("Stopped background committer thread");
    return !failed;
}

}
}
