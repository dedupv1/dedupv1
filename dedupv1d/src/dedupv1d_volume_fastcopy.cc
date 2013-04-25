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

#include <tbb/spin_rw_mutex.h>

#include "dedupv1d_volume_detacher.h"

#include "dedupv1d.pb.h"

#include <base/logging.h>
#include <base/locks.h>
#include <base/strutil.h>
#include <core/block_index.h>
#include <core/storage.h>
#include <core/dedup_system.h>
#include <core/dedup_volume.h>
#include <base/memory.h>

#include "dedupv1d_volume.h"
#include "dedupv1d_volume_info.h"

using std::map;
using std::list;
using std::string;
using std::pair;
using std::make_pair;
using std::multimap;
using dedupv1::base::ScopedLock;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::delete_result;
using dedupv1::base::DELETE_ERROR;
using dedupv1::IdleDetector;
using dedupv1::DedupSystem;
using dedupv1::base::put_result;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::PUT_KEEP;
using dedupv1::chunkstore::Storage;
using dedupv1::base::IndexCursor;
using dedupv1::StartContext;
using dedupv1::base::Index;
using dedupv1::base::ScopedPtr;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::timed_bool;
using dedupv1::DedupVolume;

LOGGER("Dedupv1dVolumeFastCopy");

namespace dedupv1d {

Dedupv1dVolumeFastCopy::Dedupv1dVolumeFastCopy(Dedupv1dVolumeInfo* volume_info) : fastcopy_thread_(NewRunnable(this,
                                                                                                               &Dedupv1dVolumeFastCopy::FastCopyThreadRunner), "fastcopy"){
    this->volume_info_ = volume_info;
    this->state_ = CREATED;
    info_store_ = NULL;
}

bool Dedupv1dVolumeFastCopy::SetOption(const string& option_name, const string& option) {
    return false;
}

bool Dedupv1dVolumeFastCopy::ProcessFastCopyStep(VolumeFastCopyJobData* data) {
    DCHECK(data, "FastCopy data not set");

    DedupVolume* src_base_volume = NULL;
    DedupVolume* target_base_volume = NULL;

    dedupv1::base::MutexLock* lock = NULL;
    Dedupv1dVolume* src_volume = this->volume_info()->FindVolume(data->src_volume_id(), &lock);
    CHECK(src_volume, "Failed to find volume");
    src_base_volume = src_volume->volume();
    CHECK(lock->ReleaseLock(), "Failed to release lock");
    lock = NULL;
    src_volume = NULL;

    lock = NULL;
    Dedupv1dVolume* target_volume = this->volume_info()->FindVolume(data->target_volume_id(), &lock);
    CHECK(target_volume, "Failed to find volume");
    target_base_volume = target_volume->volume();
    CHECK(lock->ReleaseLock(), "Failed to release lock");
    lock = NULL;
    target_volume = NULL;

    CHECK(src_base_volume, "Source base volume not set");
    CHECK(target_base_volume, "Source base volume not set");

    uint32_t fastcopy_step_size = kFastCopyStepSize;
    if (fastcopy_step_size > (data->size() - data->current_offset())) {
        fastcopy_step_size = (data->size() - data->current_offset());
    }

    DEBUG("FastCopy step: " << data->ShortDebugString() << ", fastcopy step size " << fastcopy_step_size);

    dedupv1::base::ErrorContext ec;

    CHECK(src_base_volume->FastCopyTo(target_base_volume,
            data->src_start_offset() + data->current_offset(),
            data->target_start_offset() + data->current_offset(),
            fastcopy_step_size, &ec), "Failed to fastcopy: " << data->DebugString());

    data->set_current_offset(data->current_offset() + fastcopy_step_size);

    DEBUG("FastCopy step finished: " << data->ShortDebugString() << ", fastcopy step size " << fastcopy_step_size);

    return true;
}

dedupv1::base::Option<VolumeFastCopyJobData> Dedupv1dVolumeFastCopy::GetFastCopyJob(uint32_t target_id) {
    tbb::concurrent_hash_map<uint32_t, VolumeFastCopyJobData>::const_accessor a;
    if (fastcopy_map_.find(a, target_id)) {
        return make_option(a->second);
    }
    return false;
}

bool Dedupv1dVolumeFastCopy::FastCopyThreadRunner() {

    DEBUG("Start fastcopy thread");

    CHECK(lock_.AcquireLock(), "Failed to acquire lock");
    while (state_ == RUNNING) {
        CHECK(lock_.ReleaseLock(), "Failed to release lock");
        uint32_t target_id = 0;
        if (!fastcopy_queue_.try_pop(target_id)) {

            CHECK(lock_.AcquireLock(), "Failed to acquire lock");
            timed_bool tb = change_condition_.ConditionWaitTimeout(&lock_, kQueueRetryTimeout);
            if (tb == dedupv1::base::TIMED_FALSE) {
                WARNING("Failed to wait for change condition");
            }
            if (!lock_.ReleaseLock()) {
                ERROR("Failed to release lock");
            }
            // state and fastcopy queue are rechecked now
        } else {
            // we found a target to process
            tbb::concurrent_hash_map<uint32_t, VolumeFastCopyJobData>::accessor a;
            if (fastcopy_map_.find(a, target_id)) {
                VolumeFastCopyJobData fastcopy_data = a->second;
                a.release();

                if (!ProcessFastCopyStep(&fastcopy_data)) {
                    WARNING("Failed to process fastcopy step: " << fastcopy_data.ShortDebugString());
                    fastcopy_data.set_job_failed(true);
                }
                // ok
                if (fastcopy_data.current_offset() == fastcopy_data.size()) {
                    // FastCopy finished

                    DEBUG("Finished fastcopy operation: " << fastcopy_data.ShortDebugString());
                    fastcopy_map_.erase(target_id);

                    CHECK(lock_.AcquireLock(), "Failed to acquire lock");

                    // delete entry in the source map
                    pair<multimap<uint32_t, uint32_t>::iterator, multimap<uint32_t, uint32_t>::iterator> r
                        = this->source_map_.equal_range(fastcopy_data.src_volume_id());
                    multimap<uint32_t, uint32_t>::iterator i = r.first;
                    while (i != r.second) {
                        uint32_t target_id = i->second;

                        multimap<uint32_t, uint32_t>::iterator erase_iter = i++;
                        if (target_id == fastcopy_data.target_volume_id()) {
                            this->source_map_.erase(erase_iter);
                            break;
                        }
                    }

                    if (!lock_.ReleaseLock()) {
                        ERROR("Failed to release lock");
                    }

                    CHECK(DeleteFromFastCopyData(target_id), "Failed to delete fastcopy data: target id " << target_id <<
                        ", fastcopy data " << fastcopy_data_.DebugString());

                    CHECK(PersistFastCopyData(), "Failed to persist fastcopy data");
                } else {
                    fastcopy_map_.insert(a,target_id);
                    a->second = fastcopy_data;
                    a.release();

                    CHECK(UpdateFastCopyData(fastcopy_data), "Failed to update fastcopy data: " << fastcopy_data.DebugString() <<
                        ", fastcopy data " << fastcopy_data_.DebugString());

                    CHECK(PersistFastCopyData(), "Failed to persist fastcopy data");
                    fastcopy_queue_.push(target_id); // not finished, re schedule
                }

            }
        }
        CHECK(lock_.AcquireLock(), "Failed to acquire lock");
    }
    CHECK(lock_.ReleaseLock(), "Failed to release lock");
    DEBUG("Stop fastcopy thread");
    return true;
}

bool Dedupv1dVolumeFastCopy::Start(const StartContext& start_context, dedupv1::InfoStore* info_store) {
    CHECK(this->state_ == CREATED, "Illegal state: " << this->state_);

    CHECK(info_store, "Into store not set");
    info_store_ = info_store;

    DEBUG("Start volume fast copier");

    CHECK(info_store_->RestoreInfo("volume-fastcopy", &fastcopy_data_) != LOOKUP_ERROR,
        "Failed to restore fastcopy info");

    for (int i = 0; i < fastcopy_data_.jobs_size(); i++) {
        DEBUG("Found fastcopy operation: " << fastcopy_data_.jobs(i).ShortDebugString());

        source_map_.insert(make_pair(fastcopy_data_.jobs(i).src_volume_id(),
                fastcopy_data_.jobs(i).target_volume_id()));

        fastcopy_map_.insert(make_pair(fastcopy_data_.jobs(i).target_volume_id(), fastcopy_data_.jobs(i)));
        fastcopy_queue_.push(fastcopy_data_.jobs(i).target_volume_id());
    }
    this->state_ = STARTED;
    return true;
}

bool Dedupv1dVolumeFastCopy::Run() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");

    CHECK(this->state_ == STARTED, "Illegal state: " << this->state_);

    DEBUG("Run volume fastcopy");

    CHECK(fastcopy_thread_.Start(), "Failed to start fastcopy thread");

    this->state_ = RUNNING;
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeFastCopy::DeleteFromFastCopyData(int target_id) {
    for (int i = 0; i < fastcopy_data_.jobs_size(); i++) {
        if (fastcopy_data_.jobs(i).target_volume_id() == target_id) {
            fastcopy_data_.mutable_jobs()->SwapElements(i, fastcopy_data_.jobs_size() - 1);
            fastcopy_data_.mutable_jobs()->RemoveLast();
            return true;
        }
    }
    return false;
}

bool Dedupv1dVolumeFastCopy::UpdateFastCopyData(const VolumeFastCopyJobData& fastcopy_data) {
    for (int i = 0; i < fastcopy_data_.jobs_size(); i++) {
        if (fastcopy_data_.jobs(i).target_volume_id() == fastcopy_data.target_volume_id()) {
            fastcopy_data_.mutable_jobs(i)->CopyFrom(fastcopy_data);
            return true;
        }
    }
    // not found
    fastcopy_data_.add_jobs()->CopyFrom(fastcopy_data);
    return true;
}

bool Dedupv1dVolumeFastCopy::PersistFastCopyData() {
    DCHECK(info_store_, "Info store not set");

    CHECK(info_store_->PersistInfo("volume-fastcopy", fastcopy_data_),
        "Failed to persist fastcopy data: " << fastcopy_data_.DebugString());
    return true;
}

bool Dedupv1dVolumeFastCopy::StartNewFastCopyJob(uint32_t src_volume_id,
                                                 uint32_t target_volume_id,
                                                 uint64_t source_offset,
                                                 uint64_t target_offset,
                                                 uint64_t size) {

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");

    VolumeFastCopyJobData fastcopy_data;
    fastcopy_data.set_src_volume_id(src_volume_id);
    fastcopy_data.set_target_volume_id(target_volume_id);
    fastcopy_data.set_src_start_offset(source_offset);
    fastcopy_data.set_target_start_offset(target_offset);
    fastcopy_data.set_size(size);
    fastcopy_data.set_current_offset(0);

    DEBUG("Start new fastcopy: " << fastcopy_data.ShortDebugString());

    CHECK(UpdateFastCopyData(fastcopy_data), "Failed to update fastcopy data: " << fastcopy_data.DebugString() <<
        ", fastcopy data " << fastcopy_data_.DebugString());

    CHECK(PersistFastCopyData(), "Failed to persist fastcopy data");

    source_map_.insert(make_pair(src_volume_id, target_volume_id));
    fastcopy_map_.insert(make_pair(target_volume_id, fastcopy_data));
    fastcopy_queue_.push(target_volume_id);

    CHECK(change_condition_.Broadcast(), "Failed to broadcast change condition");

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeFastCopy::Stop(const dedupv1::StopContext& stop_context) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");

    DEBUG("Stopping volume fastcopy");

    if (this->state_ == RUNNING) {
        this->state_ = STOPPED;

        CHECK(change_condition_.Broadcast(), "Failed to broadcast change condition");

        CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
        // if we do not release the lock here, the threads have no chance to notice that the state has changed

        if (fastcopy_thread_.IsStarted() || fastcopy_thread_.IsFinished()) {
            bool thread_result = false;
            CHECK(fastcopy_thread_.Join(&thread_result), "Failed to join fastcopy thread");
            if (!thread_result) {
                WARNING("FastCopy thread existed with error");
            }
        }
    } else {
        CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    }
    return true;
}

bool Dedupv1dVolumeFastCopy::IsFastCopySource(uint32_t volume_id) {
    ScopedLock scoped_lock(&this->lock_);
    if (!scoped_lock.AcquireLock()) {
        ERROR("Failed to acquire lock");
        return true; // error on the safe side
    }

    return source_map_.find(volume_id) != source_map_.end();
}

bool Dedupv1dVolumeFastCopy::IsFastCopyTarget(uint32_t volume_id) {
    ScopedLock scoped_lock(&this->lock_);
    if (!scoped_lock.AcquireLock()) {
        ERROR("Failed to acquire lock");
        return true; // error on the safe side
    }

    tbb::concurrent_hash_map<uint32_t, VolumeFastCopyJobData>::const_accessor a;
    return fastcopy_map_.find(a, volume_id);
}

Dedupv1dVolumeFastCopy::~Dedupv1dVolumeFastCopy() {
    if (!this->Stop(dedupv1::StopContext::FastStopContext())) {
        ERROR("Failed to stop volume fastcopy");
    }
}

#ifdef DEDUPV1D_TEST
void Dedupv1dVolumeFastCopy::ClearData() {
}
#endif

}
