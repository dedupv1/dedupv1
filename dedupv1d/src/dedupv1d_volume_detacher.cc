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
#include <base/memory.h>

#include "dedupv1d_volume.h"
#include "dedupv1d_volume_info.h"

using std::map;
using std::list;
using std::string;
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

LOGGER("Dedupv1dVolumeDetacher");

namespace dedupv1d {

Dedupv1dVolumeDetacher::Dedupv1dVolumeDetacher(Dedupv1dVolumeInfo* volume_info) {
    this->volume_info_ = volume_info;
    this->detaching_info_ = NULL;
    this->state = STATE_CREATED;
}

bool Dedupv1dVolumeDetacher::SetOption(const string& option_name, const string& option) {
    if (option_name == "type") {
        CHECK(this->detaching_info_ == NULL, "Detaching state info index already set");
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Cannot create detaching state info index: " << option);
        this->detaching_info_ = index->AsPersistentIndex();
        CHECK(this->detaching_info_, "Detacher info should be persistent");
        CHECK(this->detaching_info_->SetOption("max-key-size", "4"), "Failed to set max key size");
        return true;
    }
    CHECK(this->detaching_info(), "Detaching state info index type not configured");
    CHECK(this->detaching_info()->SetOption(option_name, option), "Failed to configure detaching state info index");
    return true;
}

bool Dedupv1dVolumeDetacher::DetachingThreadRunner(uint32_t volume_id) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");

    DEBUG("Starting detacher thread: volume id " << volume_id);

    VolumeInfoDetachingData detaching_data;
    enum lookup_result r = this->detaching_info()->Lookup(&volume_id, sizeof(volume_id), &detaching_data);
    CHECK(r != LOOKUP_ERROR, "Failed to lookup detaching state for volume: volume id " << volume_id);
    CHECK(r != LOOKUP_NOT_FOUND, "No detaching state info volume: volume id " << volume_id);
    // LOOKUP_FOUND

    DedupSystem* base_dedup_system = this->volume_info()->base_dedup_system();
    CHECK(base_dedup_system, "Base dedup system not set");
    dedupv1::blockindex::BlockIndex* block_index = base_dedup_system->block_index();
    CHECK(block_index, "Block index not set");
    IdleDetector* id = base_dedup_system->idle_detector();

    uint64_t current_block_id = detaching_data.start_block_id();
    if (detaching_data.has_current_block_id()) {
        current_block_id = detaching_data.current_block_id();
    }

    while (this->state == STATE_RUNNING && current_block_id < detaching_data.end_block_id()) {
        CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");

        int batch_size = kDefaultBusyBatchSize;
        int sleep_time = kDefaultBusyDetachSleepTime;
        if (id && id->IsIdle()) {
            batch_size = kDefaultIdleBatchSize;
            sleep_time = kDefaultIdleDetachSleepTime;
        }
        for (int i = 0; i < batch_size && current_block_id < detaching_data.end_block_id(); i++) {
            DEBUG("Delete block mapping: block id " << current_block_id << ", volume id " << volume_id);
            enum delete_result dr = block_index->DeleteBlockInfo(current_block_id, NO_EC);
            if (dr == DELETE_ERROR) {
                // If the data is not delete, it is not actually wrong, it is just not perfect
                WARNING("Failed to delete the block mapping: volume id " << volume_id << ", block id " << current_block_id);
                break;
            }
            current_block_id++;
            usleep(sleep_time);
        }

        CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");
    }

    if (current_block_id == detaching_data.end_block_id()) {
        // all block mappings of the volume have been deleted
        INFO("Volume is fully detached: volume id " << volume_id);
        enum delete_result r = this->detaching_info()->Delete(&volume_id, sizeof(volume_id));
        CHECK(r != DELETE_ERROR, "Failed to lookup detaching info for volume " << volume_id);
    } else {
        // system stopped or processing error: there are still block mappings left that must be deleted
        detaching_data.set_current_block_id(current_block_id);
        enum put_result pr = this->detaching_info()->Put(&volume_id, sizeof(volume_id), detaching_data);
        CHECK(pr != PUT_ERROR, "Failed to update detaching state info of volume: volume id " << volume_id);
    }

    DEBUG("Exiting detacher thread: volume " << volume_id);
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeDetacher::Start(const StartContext& start_context) {
    CHECK(this->state == STATE_CREATED, "Illegal state: " << this->state);
    CHECK(this->detaching_info() != NULL, "Detached state info not set");
    CHECK(this->detaching_info()->SupportsCursor(),
        "Detached state info doesn't support cursors");
    CHECK(this->detaching_info()->HasCapability(dedupv1::base::PUT_IF_ABSENT),
        "Detached state info doesn't support put if absent operation");
    CHECK(this->detaching_info()->IsPersistent(),
        "Detached state info is not persistent");

    DEBUG("Start volume detacher");

    CHECK(this->detaching_info()->Start(start_context),
        "Failed to start detaching state info");
    this->state = STATE_STARTED;
    return true;
}

bool Dedupv1dVolumeDetacher::Run() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");

    CHECK(this->state == STATE_STARTED, "Illegal state: " << this->state);

    DEBUG("Run volume detacher");

    IndexCursor* cursor = this->detaching_info()->CreateCursor();
    CHECK(cursor, "Failed to create cursor");
    ScopedPtr<IndexCursor> scoped_cursor(cursor); // release when exists

    this->state = STATE_RUNNING;
    enum lookup_result r = cursor->First();
    while (r == LOOKUP_FOUND) {
        uint32_t volume_id = 0;
        size_t volume_id_size = sizeof(volume_id);
        VolumeInfoDetachingData detaching_data;
        CHECK(cursor->Get(&volume_id, &volume_id_size, &detaching_data), "Failed to get index data (volume_id: " << volume_id << ")");
        CHECK(volume_id_size == sizeof(volume_id), "Illegal volume data (volume_id: " << volume_id << "; size of volume_id: " << sizeof(volume_id) << "; volume_id_size: " << volume_id_size << ")");

        DEBUG("Found volume in detaching state: volume id " << volume_id << ", former device name " << detaching_data.former_device_name());

        Thread<bool>* t = new Thread<bool>(NewRunnable(this, &Dedupv1dVolumeDetacher::DetachingThreadRunner, volume_id),
                                           "detacher " + dedupv1::base::strutil::ToString(volume_id));
        CHECK(t, "Memalloc for thread failed");
        this->detaching_threads_[volume_id] = t;
        r = cursor->Next();
    }

    for (map<uint32_t, Thread<bool>* >::iterator i = this->detaching_threads()->begin(); i != this->detaching_threads()->end(); i++) {
        Thread<bool>* detaching_thread = i->second;
        CHECK(detaching_thread, "Detaching thread not set");
        CHECK(detaching_thread->Start(), "Failed to start detaching thread: volume id " << i->first);
    }

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

Option<list<uint32_t> > Dedupv1dVolumeDetacher::GetDetachingVolumeList() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");

    list<uint32_t> l;
    for (map<uint32_t, Thread<bool>* >::iterator i = this->detaching_threads()->begin(); i != this->detaching_threads()->end(); i++) {
        l.push_back(i->first);
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return make_option(l);
}

bool Dedupv1dVolumeDetacher::Stop(const dedupv1::StopContext& stop_context) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");

    DEBUG("Stopping volume detacher");

    if (this->state == STATE_RUNNING) {
        this->state = STATE_STOPPED;
        CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
        // if we do not release the lock here, the threads have no chance to notice that the state has changed

        for (map<uint32_t, Thread<bool>* >::iterator i = this->detaching_threads()->begin(); i != this->detaching_threads()->end(); i++) {
            Thread<bool>* detaching_thread = i->second;
            CHECK(detaching_thread, "Detaching thread not set");
            bool thread_result = false;
            CHECK(detaching_thread->Join(&thread_result), "Failed to join detaching thread: volume id " << i->first);
            if (!thread_result) {
                WARNING("Detaching thread existed with error: volume id " << i->first);
            }
        }

    } else {
        CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    }
    return true;
}

Dedupv1dVolumeDetacher::~Dedupv1dVolumeDetacher() {
    if (!this->Stop(dedupv1::StopContext::FastStopContext())) {
        ERROR("Failed to stop volume detacher");
    }
    for (map<uint32_t, Thread<bool>* >::iterator i = this->detaching_threads()->begin(); i != this->detaching_threads()->end(); i++) {
        Thread<bool>* thread = i->second;
        if (thread) {
            delete thread;
        }
    }
    this->detaching_threads()->clear();

    if (this->detaching_info_) {
        delete detaching_info_;
        this->detaching_info_ = NULL;
    }
}

bool Dedupv1dVolumeDetacher::DetachVolume(const Dedupv1dVolume& volume) {
    // Flush data before the volume is detached as this makes the processing much easier.
    // A call of flush ensures that all volatile data of the block of the volume
    // are flushed to disk and that all data in the volatile block store is cleared.
    // As the processing of the volume is stopped, there is no way that new
    // volatile data regarding this volume is created.

    // See comment on BlockIndex::DeleteBlockInfo.
    DedupSystem* base_dedup_system = this->volume_info()->base_dedup_system();
    CHECK(base_dedup_system, "Base dedup system not set");
    Storage* storage = base_dedup_system->storage();
    CHECK(storage, "Storage system not set");

    CHECK(storage->Flush(NO_EC), "Failed to flush storage");

    // we flush the storage before we acquire this lock might cause deadlock
    // avoiding errors at other places
    // see issue 46
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");
    CHECK(this->state != STATE_CREATED, "Illegal state " << this->state);

    VolumeInfoDetachingData detaching_data;
    detaching_data.set_volume_id(volume.id());
    detaching_data.set_former_device_name(volume.device_name());
    detaching_data.set_former_logical_size(volume.logical_size());

    uint64_t start_block_id = 0;
    uint64_t end_block_id = 0;
    CHECK(volume.volume()->GetBlockInterval(&start_block_id, &end_block_id), "Failed to get block interval: " << volume.DebugString());
    detaching_data.set_start_block_id(start_block_id);
    detaching_data.set_end_block_id(end_block_id);
    // leave current_block_id unset

    int32_t volume_id_key = volume.id();
    enum put_result r = this->detaching_info()->PutIfAbsent(&volume_id_key, sizeof(volume_id_key), detaching_data);
    CHECK(r != PUT_ERROR,
        "Failed to put volume into detaching info index: " << volume.DebugString());
    CHECK(r != PUT_KEEP, "Volume is already in detaching state: " << volume.DebugString());
    if (state == STATE_RUNNING) {
        // start processing thread
        Thread<bool>* t = new Thread<bool>(NewRunnable(this, &Dedupv1dVolumeDetacher::DetachingThreadRunner, volume.id()),
                                           "detacher " + dedupv1::base::strutil::ToString(volume.id()));
        CHECK(t, "Memalloc for thread failed");
        this->detaching_threads_[volume.id()] = t;
        CHECK(t->Start(), "Failed to start detaching thread: volume id " << volume.id());
    }

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeDetacher::IsDetaching(uint32_t volume_id, bool* detaching_state) {
    CHECK(detaching_state, "Detaching state not set");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");
    CHECK(this->state != STATE_CREATED, "Illegal state " << this->state);

    VolumeInfoDetachingData detaching_data;
    enum lookup_result r = this->detaching_info()->Lookup(&volume_id, sizeof(volume_id), &detaching_data);
    CHECK(r != LOOKUP_ERROR, "Failed to lookup detaching info for volume " << volume_id);
    *detaching_state = (r == LOOKUP_FOUND);
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeDetacher::DeclareFullyDetached(uint32_t volume_id) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");
    CHECK(this->state != STATE_CREATED, "Illegal state " << this->state);

    enum delete_result r = this->detaching_info()->Delete(&volume_id, sizeof(volume_id));
    CHECK(r != DELETE_ERROR, "Failed to lookup detaching info for volume " << volume_id);
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

#ifdef DEDUPV1D_TEST
void Dedupv1dVolumeDetacher::ClearData() {
    Stop(dedupv1::StopContext::FastStopContext());
    delete detaching_info_;
    this->detaching_info_ = NULL;
}
#endif

}
