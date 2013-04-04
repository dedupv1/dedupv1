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

#include <core/chunk_index_bg.h>
#include <core/chunk_index.h>
#include <base/runnable.h>
#include <base/logging.h>
#include <base/strutil.h>

using std::string;
using std::vector;
using dedupv1::base::NewRunnable;
using dedupv1::base::strutil::ToString;
using dedupv1::base::ScopedLock;
using dedupv1::base::Thread;
using dedupv1::base::ThreadUtil;

LOGGER("ChunkIndex");

namespace dedupv1 {
namespace chunkindex {

ChunkIndexBackgroundCommitter::ChunkIndexBackgroundCommitter(ChunkIndex* chunk_index,
        uint32_t thread_count,
        uint32_t check_interval,
        uint32_t wait_interval,
        bool stoppingMode) {
    this->chunk_index_ = chunk_index; // this call might be early. the object should not do more with the chunk_index than store the pointer
    this->state_ = CREATED;
    this->wait_interval_ = wait_interval;
    this->check_interval_ = check_interval;
    this->stoppingMode_ = stoppingMode;

    this->threads_.resize(thread_count);
    for (uint32_t i = 0; i < thread_count; i++) {
        threads_[i] = new Thread<bool>(NewRunnable(this, &ChunkIndexBackgroundCommitter::Loop, i), "chunk index bg " + ToString(i));
    }
}

bool ChunkIndexBackgroundCommitter::Loop(uint32_t thread_id) {
    DEBUG("Starting chunk index bg committer " << thread_id);
    bool is_running = (this->state_ == RUNNING || this->state_ == STARTED);
    while (is_running) {
        ChunkIndex::import_result ir = this->chunk_index_->TryImportDirtyChunks();
        if (ir == ChunkIndex::IMPORT_ERROR) {
            WARNING("Failed to import chunk index entries");
        }

        if (this->stoppingMode_ == true && ir == ChunkIndex::IMPORT_NO_MORE) {
            TRACE("Stopping: No more items");
            break;
            // stopping thread if there are no more items
        }

        if (ir == ChunkIndex::IMPORT_NO_MORE && this->check_interval_ > 0) {
            ThreadUtil::Sleep(this->check_interval_, ThreadUtil::MILLISECONDS);
        }
        if (ir != ChunkIndex::IMPORT_NO_MORE && this->wait_interval_ > 0) {
            ThreadUtil::Sleep(this->wait_interval_, ThreadUtil::MILLISECONDS);
        }

        // update is running state
        is_running = (this->state_ == RUNNING || this->state_ == STARTED);
    }
    DEBUG("Finished chunk index bg committer " << thread_id);
    return true;
}

bool ChunkIndexBackgroundCommitter::Start() {
    CHECK(this->chunk_index_, "Chunk index not set");
    CHECK(this->threads_.size() > 0, "Illegal thread count");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire chunk index bg lock");
    CHECK(this->state_ == CREATED, "chunk index bg already started");

    this->state_ = STARTED;
    CHECK(scoped_lock.ReleaseLock(), "Failed to release chunk index bg lock");

    DEBUG("Starting chunk index background thread");
    return true;
}

bool ChunkIndexBackgroundCommitter::Run() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire chunk index bg lock");
    if (this->state_ == STARTED) {
        vector<Thread<bool>* >::iterator i;
        for (i = this->threads_.begin(); i != this->threads_.end(); i++) {
            Thread<bool>* t = *i;
            CHECK(t->Start(), "Failed to start thread");
        }
        this->state_ = RUNNING;
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release chunk index bg lock");
    return true;
}

bool ChunkIndexBackgroundCommitter::Wait() {
    vector<Thread<bool>* >::iterator i;
    for (i = this->threads_.begin(); i != this->threads_.end(); i++) {
        Thread<bool>* t = *i;
        bool thread_result = false;
        CHECK(t->Join(&thread_result), "Failed to join thread: " << t->name());
        CHECK(thread_result, "Thread existed with error: " << t->name());
    }

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire chunk index bg lock");
    this->state_ = STOPPED;
    CHECK(scoped_lock.ReleaseLock(), "Failed to release chunk index bg lock");
    return true;
}

bool ChunkIndexBackgroundCommitter::Stop(const dedupv1::StopContext& stop_context) {
    ScopedLock scoped_lock(&this->lock_);
    bool started = false;
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire chunk index bg lock");
    if (this->state_ == RUNNING) {
        this->state_ = STOPPED;
        started = true;
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release chunk index bg lock");

    if (started) {
        CHECK(this->Wait(), "Failed to wait for threads");
    }
    return true;
}

ChunkIndexBackgroundCommitter:: ~ChunkIndexBackgroundCommitter() {
    if (!this->Stop(dedupv1::StopContext::FastStopContext())) {
        WARNING("Failed to stop chunk index bg");
    }

    vector<Thread<bool>* >::iterator i;
    for (i = this->threads_.begin(); i != this->threads_.end(); i++) {
        delete *i;
    }
    this->threads_.clear();
}

}
}
