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

#include <core/usage_count_garbage_collector.h>
#include <string>
#include <list>
#include <sstream>

#include "dedupv1.pb.h"
#include "dedupv1_stats.pb.h"

#include <core/dedup.h>

#include <base/locks.h>
#include <base/logging.h>
#include <base/index.h>

#include <core/log_consumer.h>
#include <core/log.h>
#include <core/block_mapping.h>
#include <core/block_mapping_pair.h>
#include <core/chunk_index.h>
#include <base/runnable.h>
#include <base/thread.h>
#include <core/dedup_system.h>
#include <base/strutil.h>
#include <base/memory.h>
#include <core/storage.h>
#include <base/fault_injection.h>
#include <core/garbage_collector.h>
#include <core/chunk_locks.h>
#include <core/block_index.h>

#include <tbb/tick_count.h>
#include <tbb/task.h>

using std::string;
using std::stringstream;
using std::vector;
using std::list;
using std::set;
using std::map;
using std::multimap;
using std::make_pair;
using std::pair;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::MutexLock;
using dedupv1::base::ScopedLock;
using dedupv1::base::TIMED_FALSE;
using dedupv1::base::NewRunnable;
using dedupv1::base::Thread;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::DELETE_ERROR;
using dedupv1::base::DELETE_OK;
using dedupv1::base::Index;
using dedupv1::base::IndexIterator;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingPair;
using dedupv1::chunkstore::StorageSession;
using dedupv1::Fingerprinter;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::put_result;
using dedupv1::log::event_type;
using dedupv1::log::LogReplayContext;
using dedupv1::log::EVENT_TYPE_OPHRAN_CHUNKS;
using dedupv1::log::EVENT_REPLAY_MODE_DIRECT;
using dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG;
using dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_DELETED;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED;
using dedupv1::log::EVENT_TYPE_LOG_EMPTY;
using dedupv1::base::ProfileTimer;
using dedupv1::base::delete_result;
using dedupv1::chunkstore::Container;
using dedupv1::chunkstore::ContainerStorage;
using dedupv1::chunkstore::ContainerItem;
using dedupv1::chunkstore::storage_commit_state;
using dedupv1::chunkstore::STORAGE_ADDRESS_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_ERROR;
using dedupv1::chunkstore::STORAGE_ADDRESS_NOT_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_WILL_NEVER_COMMITTED;
using tbb::tick_count;
using dedupv1::chunkstore::Storage;
using dedupv1::base::ScopedPtr;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::PUT_KEEP;
using dedupv1::base::timed_bool;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::chunkindex::ChunkLocks;
using dedupv1::base::Future;
using dedupv1::base::Runnable;
using dedupv1::blockindex::BlockIndex;
using dedupv1::base::strutil::FriendlySubstr;
using dedupv1::base::ThreadUtil;

LOGGER("GarbageCollector");

namespace dedupv1 {
namespace gc {


void UsageCountGarbageCollector::RegisterGC() {
  GarbageCollector::Factory().Register("usage-count",
      &UsageCountGarbageCollector::CreateGC);
}

GarbageCollector* UsageCountGarbageCollector::CreateGC() {
  return new UsageCountGarbageCollector();
}

UsageCountGarbageCollector::UsageCountGarbageCollector() :
    GarbageCollector(USAGE_COUNT),
    gc_thread_(NewRunnable(this, &UsageCountGarbageCollector::GCLoop), "gc") {
    chunk_index_ = NULL;
    info_store_ = NULL;
    log_ = NULL;
    idle_detector_ = NULL;
    state_ = CREATED;
    candidate_info_ = NULL;
    max_candidate_processing_time_ = 2.0;
    tp_ = NULL;
    paused_ = false;
#ifdef DEDUPV1_CORE_TEST
    no_gc_candidates_during_last_try_ = false;
#endif
}
UsageCountGarbageCollector::~UsageCountGarbageCollector() {
}

bool UsageCountGarbageCollector::PauseProcessing() {
    paused_ = true;
    if (this->state_ == CANDIDATE_PROCESSING) {
        StopProcessing();
    }
    DEBUG("Garbage Collector is paused");
    return true;
}

bool UsageCountGarbageCollector::ResumeProcessing() {
    paused_ = false;
    DEBUG("Garbage Collector is no more paused");
    return true;
}

bool UsageCountGarbageCollector::Start(const StartContext& start_context,
    DedupSystem* system) {
    CHECK(system, "System not set");
    CHECK(this->state_ == CREATED, "GC already started");
    CHECK(this->candidate_info_, "Candidate info not set");
    CHECK(this->candidate_info_->IsPersistent(),
        "gc candidates index should be persistent");
    CHECK(this->candidate_info_->HasCapability(dedupv1::base::PERSISTENT_ITEM_COUNT),
        "gc candidates index has no persistent item count");

    INFO("Starting gc");
    this->log_ = system->log();
    CHECK(this->log_, "Log not set");

    this->chunk_index_ = system->chunk_index();
    CHECK(this->chunk_index_, "Chunk index not set");

    info_store_ = system->info_store();
    CHECK(info_store_, "Info store not set");

    this->block_size_ = system->block_size();
    CHECK(this->block_size_ > 0, "Block size not set");

    idle_detector_ = system->idle_detector();
    CHECK(this->idle_detector_, "Idle detector not set");

    this->storage_ = dynamic_cast<ContainerStorage*>(system->storage());
    CHECK(this->storage_, "Storage not set");

    this->tp_ = system->threadpool();
    CHECK(tp_, "Threadpool not set");

    CHECK(this->candidate_info_->Start(start_context),
        "Cannot open candidate info");

    this->state_ = STARTED;
    DEBUG("Started gc");

    CHECK(this->idle_detector_->RegisterIdleConsumer("gc", this),
        "Cannot register gc as idle tick consumer");
    CHECK(this->log_->RegisterConsumer("gc", this),
        "Cannot register gc consumer");

    CHECK(ReadMetaInfo(), "Failed to read gc info data");
    return true;
}

bool UsageCountGarbageCollector::DumpMetaInfo() {
    DCHECK(info_store_, "Info store not set");

    GarbageCollectionInfoData info_data;
    set<int64_t>::iterator i;

    for (i = replayed_block_failed_event_set_.begin(); i != replayed_block_failed_event_set_.end(); i++) {
        info_data.add_replayed_block_failed_event_log_id(*i);
    }
    CHECK(info_store_->PersistInfo("gc", info_data),
        "Failed to persist gc info data: " << info_data.ShortDebugString());

    DEBUG("Dumped gc info data: " <<
        FriendlySubstr(info_data.ShortDebugString(), 0, 256, "..."));
    return true;
}

bool UsageCountGarbageCollector::ReadMetaInfo() {
    DCHECK(info_store_, "Info store not set");

    GarbageCollectionInfoData info_data;
    lookup_result lr = info_store_->RestoreInfo("gc", &info_data);
    CHECK(lr != LOOKUP_ERROR, "Failed to lookup gc info data");

    replayed_block_failed_event_set_.clear();

    for (int i = 0; i < info_data.replayed_block_failed_event_log_id_size(); i++) {
        replayed_block_failed_event_set_.insert(info_data.replayed_block_failed_event_log_id(i));
    }

    DEBUG("Restored gc info data: " <<
        FriendlySubstr(info_data.ShortDebugString(), 0, 256, "..."));

    return true;
}

bool UsageCountGarbageCollector::Close() {
    DEBUG("Closing gc");

    if (this->state_ == RUNNING || this->state_ == CANDIDATE_PROCESSING || this->state_ == STOPPING) {
        CHECK(this->Stop(dedupv1::StopContext::FastStopContext()),
            "Cannot stop gc");
    }

    if (this->idle_detector_ && idle_detector_->IsRegistered("gc").value()) {
        DEBUG("Remove from idle detector");
        if (!this->idle_detector_->UnregisterIdleConsumer("gc")) {
            WARNING("Failed to unregister gc idle tick consumer");
        }
    }

    DEBUG("Closing index");
    if (candidate_info_) {
        CHECK(candidate_info_->Close(), "Cannot close candidate info");
        candidate_info_ = NULL;
    }

    DEBUG("Remove from log");
    if (this->log_) {
        if (log_->IsRegistered("gc").value()) {
            if (!this->log_->UnregisterConsumer("gc")) {
                WARNING("Failed to unregister gc log consumer");
            }
        }
        this->log_ = NULL;
    }
    delete this;
    return true;
}

bool UsageCountGarbageCollector::SetOption(const std::string& option_name,
    const std::string& option) {
    if (option_name == "type") {
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Index creation failed: " << option);
        this->candidate_info_ = index->AsPersistentIndex();
        CHECK(this->candidate_info_, "Candidate index should be persistent");
        return true;
    }
    CHECK(this->candidate_info_, "Candidate info type not set");
    return this->candidate_info_->SetOption(option_name, option);
}

bool UsageCountGarbageCollector::Diff(const BlockMapping& original_block_mapping,
    const BlockMapping& modified_block_mapping,
    map<bytestring, pair<int, uint64_t> >* diff_result) {
    map<bytestring, int> diff;
    map<bytestring, uint64_t> container_map;
    list<BlockMappingItem>::const_iterator i;

    for (i = original_block_mapping.items().begin(); i != original_block_mapping.items().end(); i++) {
        diff[i->fingerprint_string()]--;
        container_map[i->fingerprint_string()] = i->data_address();
    }

    for (i = modified_block_mapping.items().begin(); i != modified_block_mapping.items().end(); i++) {
        diff[i->fingerprint_string()]++;
        container_map[i->fingerprint_string()] = i->data_address();
    }

    map<bytestring, int>::iterator j;
    for (j = diff.begin(); j != diff.end(); j++) {
        if (j->second != 0) {
            (*diff_result)[j->first] = make_pair(j->second, container_map[j->first]);
        }
    }
    return true;
}

bool UsageCountGarbageCollector::GCLoop() {
    DEBUG("Starting gc thread");

    ScopedLock scoped_lock(&this->gc_lock_);
    scoped_lock.AcquireLock(); // auto release

    int waiting_time = 1;
    while (state_ == RUNNING || state_ == CANDIDATE_PROCESSING) {
        TRACE("GC waiting time: " << waiting_time << "s, state " << state_);
        timed_bool tb = gc_condition_.ConditionWaitTimeout(&gc_lock_, waiting_time);
        CHECK(tb != TIMED_FALSE, "Failed to wait for gc signal");

        if (state_ == CANDIDATE_PROCESSING) {

            lookup_result r = ProcessGCCandidates();
            if (r == LOOKUP_ERROR) {
#ifdef DEDUPV1_CORE_TEST
                no_gc_candidates_during_last_try_ = false;
#endif
                WARNING("Failed to process candidates");
            } else if (r == LOOKUP_NOT_FOUND) {
#ifdef DEDUPV1_CORE_TEST
                no_gc_candidates_during_last_try_ = true;
#endif
                waiting_time += 1;
                if (waiting_time > kMaxWaitingTime) {
                    waiting_time = kMaxWaitingTime;
                }
            } else {
#ifdef DEDUPV1_CORE_TEST
                no_gc_candidates_during_last_try_ = false;
#endif
                // r == LOOKUP_FOUND
                if (!this->log_->IsReplaying() && this->idle_detector_->IsIdle()) {
                    // don't throttle gc if system is idle and while there is no log replay occurring
                    waiting_time = 0;
                } else {
                    waiting_time = 1;
                }
            }
        } else {
            waiting_time += 1;
            if (waiting_time > kMaxWaitingTime) {
                waiting_time = kMaxWaitingTime;
            }
        }
    }
    DEBUG("Exiting gc thread: state " << state_);
    return true;
}

bool UsageCountGarbageCollector::Stop(const dedupv1::StopContext& stop_context) {
    if (state_ != STOPPED) {
        INFO("Stopping gc");
    }

    state old_state = state_;

    if (this->idle_detector_ && idle_detector_->IsRegistered("gc").value()) {
        DEBUG("Remove from idle detector");
        if (!this->idle_detector_->UnregisterIdleConsumer("gc")) {
            WARNING("Failed to unregister gc idle tick consumer");
        }
    }

    // we change the state after we removed the idle detector listener
    // so the idle callback cannot change the state

    this->state_ = STOPPING;
    if (!this->gc_condition_.Broadcast()) {
        WARNING("Failed to broadcast gc state change");
    }
    bool result = false;

    if (old_state == RUNNING || old_state == CANDIDATE_PROCESSING || old_state == STOPPING) {
        if (this->gc_thread_.IsStarted() || this->gc_thread_.IsFinished()) {
            CHECK(this->gc_thread_.Join(&result), "Cannot join gc thread");
            if (!result) {
                WARNING("gc thread finished with error");
            }
        }
    }
    state_ = STOPPED;
    DEBUG("Stopped gc");
    return true;
}

bool UsageCountGarbageCollector::Run() {
    state original_state = state_.compare_and_swap(RUNNING, STARTED);
    CHECK(original_state == STARTED, "Illegal state: " << this->state_);

    // pick up partly processed candidates
    // We execute them before Run because we can only here assume that e.g.
    // the container allocation data
    // is up to date. It MUST be after the dirty log replay.
    ScopedLock scoped_lock(&candidate_info_lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire candidate info lock");

    IndexIterator* i = this->candidate_info_->CreateIterator();
    CHECK(i, "Failed to create candidate info iterator");
    ScopedPtr<IndexIterator> scoped_iterator(i);

    GarbageCollectionCandidateData candidate_data;
    std::list<GarbageCollectionCandidateData> changed_cadidates;
    std::list<uint64_t> deleted_candidates;
    lookup_result lr = i->Next(NULL, NULL, &candidate_data);
    for (; lr == LOOKUP_FOUND; lr = i->Next(NULL, NULL, &candidate_data)) {
        if (candidate_data.processing()) {
            INFO("Finish processing: container id " << candidate_data.address() <<
                ", item count " << candidate_data.item_size());
            bool candidate_changes = false;
            if (!DoProcessGCCandidate(&candidate_data, &candidate_changes)) {
                WARNING("Failed to process GC candidate: " <<
                    "container id " << candidate_data.address());
                return false;
            } else {
                if (candidate_data.item_size() == 0) {
                    deleted_candidates.push_back(candidate_data.address());
                } else {
                    candidate_data.set_processing(false); // we finished processing

                    if (candidate_changes) {
                        candidate_data.set_unchanged_processing_count(0);
                    } else {
                        candidate_data.set_unchanged_processing_count(candidate_data.unchanged_processing_count() + 1);
                    }

                    changed_cadidates.push_back(candidate_data);
                }
            }
        }
    }
    CHECK(lr != LOOKUP_ERROR, "Error while trying to read from candidate infos during run of garbage collector");

    for (std::list<uint64_t>::iterator i = deleted_candidates.begin(); i != deleted_candidates.end(); i++) {
        uint64_t id = *i;
        CHECK(this->candidate_info_->Delete(&id, sizeof(id)) != DELETE_ERROR,
            "Error deleting garbage info element " << *i);
    }

    for (std::list<GarbageCollectionCandidateData>::iterator i = changed_cadidates.begin(); i
         != changed_cadidates.end(); i++) {
        uint64_t id = i->address();
        CHECK(this->candidate_info_->Put(&id, sizeof(id), *i) != PUT_ERROR,
            "Error updating garbage info element " << i->DebugString());
    }

    DEBUG("Running gc");

    CHECK(this->gc_thread_.Start(), "Cannot start gc thread");
    return true;
}

void UsageCountGarbageCollector::IdleStart() {
    TRACE("GarbageCollector Idle started");
    if (this->paused_) {
        DEBUG("Idle time started but Garbage Collector is paused");
        return;
    }
    if (!StartProcessing()) {
        WARNING("Failed to start gc processing");
    }
}

void UsageCountGarbageCollector::IdleEnd() {
    TRACE("GarbageCollector Idle ended");
    if (!StopProcessing()) {
        WARNING("Failed to stop gc processing");
    }
}

bool UsageCountGarbageCollector::IsProcessing() {
    return this->state_ == CANDIDATE_PROCESSING;
}

bool UsageCountGarbageCollector::StartProcessing() {
    // we try to switch from CANDIDATE_PROCESSING to RUNNING. We don't care if
    // this fails.
    if (paused_) {
        WARNING("StartProcessing was called while GarbageCollector is in paused mode.");
    }
    state old_state = this->state_.compare_and_swap(CANDIDATE_PROCESSING, RUNNING);
    if (old_state != RUNNING && old_state != CANDIDATE_PROCESSING) {
        ERROR("Illegal state: " << old_state);
        return false;
    } else {
        TRACE("Switch to candidate processing state: new state " << this->state_);
        if (!this->gc_condition_.Broadcast()) {
            WARNING("Failed to broadcast gc state change");
        }
    }
    return true;
}

bool UsageCountGarbageCollector::StopProcessing() {
    // we try to switch from RUNNING to CANDIDATE_PROCESSING. We don't care if this fails.
    state old_state = this->state_.compare_and_swap(RUNNING, CANDIDATE_PROCESSING);
    if (old_state != RUNNING && old_state != CANDIDATE_PROCESSING) {
        ERROR("Illegal state: " << old_state);
        return false;
    } else {
        TRACE("Switch to running state: new state " << this->state_);
        if (!this->gc_condition_.Broadcast()) {
            WARNING("Failed to broadcast gc state change");
        }
    }
    return true;
}

bool UsageCountGarbageCollector::ProcessGCCandidateItem(const GarbageCollectionCandidateData& candidate_data,
                                                        const GarbageCollectionCandidateItemData& item_data, std::pair<bool, bool>* del) {
    DCHECK(del, "Delete items not set");

    uint64_t container_id = candidate_data.address();
    // Never, ever return from here without releasing the chunk lock

    CHECK(chunk_index_->chunk_locks().Lock((const byte *) item_data.fp().data(), item_data.fp().size()),
        "Failed to acquire chunk lock: " << item_data.ShortDebugString());

    bool failed = false;
    bool del_set = false;
    ChunkMapping chunk_mapping((const byte *) item_data.fp().data(), item_data.fp().size());
    chunk_mapping.set_data_address(container_id);

    Option<bool> o = chunk_index_->in_combats().Contains(chunk_mapping.fingerprint(), chunk_mapping.fingerprint_size());
    if (!o.valid()) {
        ERROR("Failed to check in-combats: " << chunk_mapping.DebugString());
        failed = true;
    } else if (o.value()) {
        // chunk is in-combat, defer it to a later time
        TRACE("Skip processing fingerprint on in-combat list: " << Fingerprinter::DebugString(item_data.fp()));
    } else {

        lookup_result lr = this->chunk_index_->Lookup(&chunk_mapping, false, NO_EC);
        if (lr == LOOKUP_ERROR) {
            ERROR("Failed to lookup chunk index: reason: error, " << chunk_mapping.DebugString());
            failed = true;
        } else if (lr == LOOKUP_NOT_FOUND) {
            if (item_data.has_type() && item_data.type() == GarbageCollectionCandidateItemData::FAILED) {
                // this was a failed fp gc candidate
                // it might therefore be ok to not having a entry in the chunk index

                if (candidate_data.processing()) {
                    // when the gc candidate was in processing state (before a crash), it might be the case
                    // that the chunk was already deleted from the chunk index. The normal reason why a chunk is not available
                    // in the chunk index for failed block writes is not given after crashes.
                    DEBUG("Failed to lookup chunk index: reason not found" <<
                        ", chunk " << chunk_mapping.DebugString() <<
                        ", failed-mode gc candidate");
                    *del = make_pair(true, true); // delete from storage
                    del_set = true;
                } else {
                    DEBUG("Found failed-mode gc candidate without matching chunk mapping: " << chunk_mapping.DebugString());
                    // we do not delete the gc entry, but wait until a time that the chunk entry exists
                    // at some time it should exists because the container was committed
                    // this is similar to an in-combat chunks.
                }
            } else {
                // Read container for reporting
                enum lookup_result r = LOOKUP_ERROR;
                Container c;
                if (c.Init(chunk_mapping.data_address(), this->storage_->GetContainerSize())) {
                    r = this->storage_->ReadContainerWithCache(&c);
                }
                string container_debug_string;
                if (r == LOOKUP_ERROR) {
                    container_debug_string += "<read error>";
                } else if (r == LOOKUP_NOT_FOUND) {
                    container_debug_string += "<not found>";
                } else {
                    container_debug_string += c.DebugString();

                    ContainerItem* item = c.FindItem(chunk_mapping.fingerprint(), chunk_mapping.fingerprint_size(),
                        true);
                    if (item) {
                        container_debug_string += ", item " + item->DebugString();
                    } else {
                        container_debug_string += ", item <not found>";
                    }
                }

                // when this is the first candidate processed and if the system crashed, we are
                // in a split situation where the chunk index entries have been deleted, then system crashed.
                // The problem is that the candidate item has not been deleted from the gc info database and
                // the data has not been removed from the storage (or at least it might be possible that this hasn't
                // happen
                if (candidate_data.processing()) {
                    DEBUG("Failed to lookup chunk index: reason not found" <<
                        ", chunk " << chunk_mapping.DebugString() <<
                        ", container " << container_debug_string);
                    *del = make_pair(true, true); // delete from storage
                    del_set = true;
                } else {
                    ERROR("Failed to lookup chunk index: reason not found" <<
                        ", chunk " << chunk_mapping.DebugString() <<
                        ", container " << container_debug_string);
                    failed = true;
                }
            }
        } else {
            // lr == LOOKUP_FOUND
            DEBUG("Process gc candidate: " << chunk_mapping.DebugString());
            if (chunk_mapping.usage_count() != 0) {
                *del = make_pair(true, false);
                del_set = true;
                // chunk has new user since the candidate check
                // => Delete the candidate
                TRACE("Chunk has new user since the candidate check: " << chunk_mapping.DebugString());
            } else {
                FAULT_POINT("gc.process.before-chunk-index-delete");
                // now we have a chunk that should be deleted
                if (this->chunk_index_->Delete(chunk_mapping) == DELETE_ERROR) {
                    ERROR("Cannot delete from chunk mapping: " << chunk_mapping.DebugString());
                    failed = true;
                }
                *del = make_pair(true, true);
                del_set = true;
                FAULT_POINT("gc.process.after-chunk-index-delete");
            }
            this->stats_.processed_gc_candidates_++;
        }
    }

    CHECK(chunk_index_->chunk_locks().Unlock((const byte *) item_data.fp().data(), item_data.fp().size()),
        "Failed to release chunk lock: " << item_data.ShortDebugString());

    if (!del_set) {
        *del = make_pair(false, false);
    }
    return !failed;
}

bool UsageCountGarbageCollector::ProcessGCCandidate(GarbageCollectionCandidateData* candidate_data, bool* changed) {
    DCHECK(candidate_data, "Candidate data not set");
    DCHECK(chunk_index_, "Chunk index not set");
    DCHECK(candidate_info_, "Candidate info not set");

    bool failed = false;
    CHECK(candidate_data->has_address(), "Illegal candidate data: " << candidate_data->ShortDebugString());
    uint64_t container_id = candidate_data->address();

    // mark the candidate has in process. However, we should set the flag only on a copy of the data
    // we want to avoid that all candidate data objects in ProcessGCCandidate has the flag set. We use
    // the flag for crash handling. The flag indicates that a object has not been fully processed at the last run.
    GarbageCollectionCandidateData updated_candidate_data = *candidate_data;
    updated_candidate_data.set_processing(true);
    TRACE("Update gc candidate: " << updated_candidate_data.ShortDebugString());
    if (this->candidate_info_->Put(&container_id, sizeof(container_id), updated_candidate_data) == PUT_ERROR) {
        WARNING("Failed to delete gc candidate: " << updated_candidate_data.ShortDebugString());
        return false;
    }

    bool candidate_changes = false;

    if (!DoProcessGCCandidate(candidate_data, &candidate_changes)) {
        WARNING("Failed to process GC candidate: container id " << candidate_data->address());
        return false;
    }
    FAULT_POINT("gc.process.before-gc-delete");
    if (candidate_data->item_size() == 0) {
        TRACE("Delete gc candidate: container id " << candidate_data->address());
        if (this->candidate_info_->Delete(&container_id, sizeof(container_id)) == DELETE_ERROR) {
            WARNING("Failed to delete gc candidate: container id " << candidate_data->address());
            return false;
        }
    } else {
        candidate_data->set_processing(false); // we finished processing

        if (candidate_changes) {
            candidate_data->set_unchanged_processing_count(0);
        } else {
            candidate_data->set_unchanged_processing_count(candidate_data->unchanged_processing_count() + 1);
        }

        TRACE("Update gc candidate: " << candidate_data->ShortDebugString());
        if (this->candidate_info_->Put(&container_id, sizeof(container_id), *candidate_data) == PUT_ERROR) {
            WARNING("Failed to delete gc candidate: " << candidate_data->ShortDebugString());
            return false;
        }

        if (changed) {
            *changed = candidate_changes;
        }
    }

    return !failed;
}

bool UsageCountGarbageCollector::DoProcessGCCandidate(GarbageCollectionCandidateData* candidate_data, bool* changed) {
    DCHECK(candidate_data, "Candidate data not set");
    DCHECK(this->chunk_index_, "Chunk index not set");

    StorageSession* session = this->storage_->CreateSession();
    CHECK(session, "Cannot create storage session");

    DEBUG("Process gc candidates: " <<
        "container id " << candidate_data->address() <<
        ", item count " << candidate_data->item_size());
    tick_count start = tick_count::now();

    bool failed = false;
    list<pair<int, bool> > delete_items;
    for (int i = 0; i < candidate_data->item_size(); i++) {
        const GarbageCollectionCandidateItemData& item_data(candidate_data->item(i));

        std::pair<bool, bool> del;
        if (!ProcessGCCandidateItem(*candidate_data, item_data, &del)) {
            ERROR("Failed to process gc candidate item: " <<
                item_data.ShortDebugString() <<
                ", container data " << candidate_data->ShortDebugString());
        }
        if (del.first) {
            delete_items.push_back(make_pair(i, del.second));
        }

        if ((tick_count::now() - start).seconds() > this->max_candidate_processing_time_) {
            DEBUG("Stop gc candidate processing. Timeout");
            break;
        }
    }

    if (delete_items.size() > 0) {
        if (changed) {
            *changed = true;
        }
    }

    std::list<bytestring> key_list;
    for (list<pair<int, bool> >::reverse_iterator j = delete_items.rbegin(); j != delete_items.rend(); j++) {
        int index = j->first;
        bool delete_storage = j->second;
        TRACE("Remove candidate item: " << index << ", fp " << Fingerprinter::DebugString(
                candidate_data->item(index).fp()) << ", container id " << candidate_data->address()
                                        << ", delete from storage " << ToString(delete_storage));
        if (delete_storage) {
            FAULT_POINT("gc.process.before-storage-delete");
            const GarbageCollectionCandidateItemData& item_data(candidate_data->item(index));
            bytestring key = dedupv1::base::make_bytestring(item_data.fp());
            key_list.push_back(key);
        }

        if (candidate_data->item_size() > 1) {
            *candidate_data->mutable_item(index) = candidate_data->item(candidate_data->item_size() - 1);
        }
        candidate_data->mutable_item()->RemoveLast();
    }

    if (key_list.size() > 0) {
        TRACE("Delete from storage: container id " << candidate_data->address() << ", count " << key_list.size());
        bool delete_result = session->Delete(candidate_data->address(), key_list, NO_EC);
        if (!delete_result) {
            // something went wrong
            if (candidate_data->processing()) {
                // the candidate was also in process before a crash to we are here in a split processing
                // situation.
                WARNING("Failed to delete chunk from storage: " << candidate_data->ShortDebugString());
            } else {
                ERROR("Failed to delete chunk from storage: " << candidate_data->ShortDebugString());
                failed = true;
            }
        }
    }

    if (!session->Close()) {
        WARNING("Failed to close session");
    }

    FAULT_POINT("gc.process.post");
    return !failed;
}

lookup_result UsageCountGarbageCollector::ProcessGCCandidates() {
    DCHECK_RETURN(this->state_ == CANDIDATE_PROCESSING || state_ == STOPPING, LOOKUP_ERROR,
        "Illegal state: " << this->state_);
    // the gc state is set during the log processing to avoid bad stuff
    ProfileTimer timer(this->stats_.gc_thread_time_);

    ScopedLock scoped_lock(&candidate_info_lock_);
    if (!scoped_lock.AcquireLock()) {
        ERROR("Failed to acquire candidate info lock");
        return LOOKUP_ERROR;
    }

    IndexIterator* i = this->candidate_info_->CreateIterator();
    if (i == NULL) {
        ERROR("Cannot create iterator");
        return LOOKUP_ERROR;
    }
    ScopedPtr<IndexIterator> scoped_iterator(i);

    GarbageCollectionCandidateData candidate_data;
    lookup_result lr = i->Next(NULL, NULL, &candidate_data);
    if (lr == LOOKUP_ERROR) {
        ERROR("Failed to move cursor");
        return LOOKUP_ERROR;
    }
    if (lr == LOOKUP_NOT_FOUND) {
        TRACE("No gc candidate found");
        return lr;
    }
    // lr == LOOKUP_FOUND

    bool changing_processing = false;
    if (!ProcessGCCandidate(&candidate_data, &changing_processing)) {
        WARNING("Failed to process GC candidate: container id " << candidate_data.address());
        lr = LOOKUP_ERROR;
    }

    if (!scoped_lock.ReleaseLock()) {
        WARNING("Failed to release candidate info lock");
        lr = LOOKUP_ERROR;
    }

    // if the processing has not changed at least something, the reason is that all chunks in the processing candidate
    // is still "in-combat". It does not make any sense to repeat the same check seconds later. Therefore we delay the next
    // processing step a bit here.
    if (!changing_processing) {
        lr = LOOKUP_NOT_FOUND;
    }
    return lr;
}

bool UsageCountGarbageCollector::ProcessDeletedBlockMapping(const BlockMapping& orig_mapping, const LogReplayContext& context) {

    BlockMapping modified_mapping(orig_mapping.block_id(), orig_mapping.block_size());
    CHECK(modified_mapping.FillEmptyBlockMapping(), "Failed to fill in empty block mapping");

    // processing a delete block mapping is (from the GC standpoint) equivalent to processing
    // the original mapping with an empty block mapping (zeros).

    BlockMappingPair mapping_pair(block_size_);
    CHECK(mapping_pair.CopyFrom(orig_mapping, modified_mapping), "Failed to create mapping pair");

    return ProcessBlockMapping(mapping_pair, context);
}

bool UsageCountGarbageCollector::ProcessDeletedBlockMappingDirect(const BlockMapping& orig_mapping,
                                                                  const LogReplayContext& context) {
    BlockMapping modified_mapping(orig_mapping.block_id(), orig_mapping.block_size());
    CHECK(modified_mapping.FillEmptyBlockMapping(), "Failed to fill in empty block mapping");

    BlockMappingPair mapping_pair(block_size_);
    CHECK(mapping_pair.CopyFrom(orig_mapping, modified_mapping), "Failed to create mapping pair");

    // processing a delete block mapping is (from the GC standpoint) equivalent to processing
    // the original mapping with an empty block mapping (zeros).
    return ProcessBlockMappingDirect(mapping_pair, context);
}

bool UsageCountGarbageCollector::ProcessDeletedBlockMappingDirtyStart(const BlockMapping& orig_mapping,
                                                                      const LogReplayContext& context) {
    BlockMapping modified_mapping(orig_mapping.block_id(), orig_mapping.block_size());
    CHECK(modified_mapping.FillEmptyBlockMapping(), "Failed to fill in empty block mapping");

    BlockMappingPair mapping_pair(block_size_);
    CHECK(mapping_pair.CopyFrom(orig_mapping, modified_mapping), "Failed to create mapping pair");

    // processing a delete block mapping is (from the GC standpoint) equivalent to processing
    // the original mapping with an empty block mapping (zeros).
    return ProcessBlockMappingDirtyStart(mapping_pair, context);
}

class ProcessMappingDiffTask : public Runnable<bool> {
private:
    UsageCountGarbageCollector* gc_;
    int usage_modifier_;
    LogReplayContext context_;
    bool invert_failed_write_;

    ChunkMapping* mapping_;
public:

    /**
     * Constructor
     */
    ProcessMappingDiffTask(UsageCountGarbageCollector* gc, const LogReplayContext& context, bool invert_failed_write,
                           ChunkMapping* mapping, int usage_mod) :
        gc_(gc), usage_modifier_(usage_mod), context_(context), invert_failed_write_(invert_failed_write),
        mapping_(mapping) {
    }

    /**
     * Destructor
     */
    virtual ~ProcessMappingDiffTask() {
    }

    /**
     * Note on the write-back cache of the chunk index: The usage of PutOverwrite/Put is ok here.
     * If the page is persisted.
     *
     * TODO (dmeister) We should change this that ALL uc changes are applied before as dirty change, so
     * that always a EnsurePersistent is all we need to do here.
     */
    bool ProcessMapping() {
        TRACE("Process gc: " << mapping_->DebugString() <<
            ", usage modifier " << usage_modifier_);
        bool failed = false;
        bool should_ensure_persistence = false;

        ChunkLocks& chunk_locks(gc_->chunk_index_->chunk_locks());
        if (!chunk_locks.Lock(mapping_->fingerprint(),
                mapping_->fingerprint_size())) {
            ERROR("Failed to lock chunk index for mapping: " <<
                mapping_->DebugString());
            mapping_->set_data_address(Storage::ILLEGAL_STORAGE_ADDRESS);
            return false;
        }

        enum lookup_result lookup_result = gc_->chunk_index_->LookupPersistentIndex(
            mapping_,
            dedupv1::base::CACHE_LOOKUP_DEFAULT,
            dedupv1::base::CACHE_ALLOW_DIRTY, NO_EC);
        if (lookup_result == LOOKUP_ERROR) {
            ERROR("Cannot lookup chunk index for chunk mapping " << mapping_->DebugString());
            failed = true;
        } else if (lookup_result == LOOKUP_NOT_FOUND) {
            // there is no chunk mapping item in the chunk index, but it really
            // should be there.
            ERROR("Chunk not found in chunk index: " <<
                mapping_->DebugString() <<
                ", usage modifier " << usage_modifier_);
            failed = true;
        } else {
            DEBUG("Check gc usage count: " <<
                mapping_->DebugString() <<
                ", usage modifier " << usage_modifier_);

            // we are now sure that the chunk index exists and that the container
            // for all entries is committed.
            // if the change result should now be applied to chunk index

            if (!invert_failed_write_ &&
                mapping_->usage_count_change_log_id() >= context_.log_id()) {
                // This happens when the system crashes during a log replay and
                // the last entry has already been processed
                // In this case, we skip the processing as what should be done
                // is already done.
                DEBUG("Current event has already been processed: " <<
                    "current log id " << context_.log_id() <<
                    ", chunk " << mapping_->DebugString());
                gc_->stats_.already_processed_chunk_count_++;

                should_ensure_persistence = true;

            } else if (invert_failed_write_
                       && mapping_->usage_count_failed_write_change_log_id()
                       >= context_.log_id()) {
                // This happens when the system crashes during a log replay and
                // the last entry has already been processed
                // In this case, we skip the processing as what should be done
                // is already done.
                DEBUG("Current event (failed write) has already been processed: " <<
                    "current log id " << context_.log_id() <<
                    ", chunk " << mapping_->DebugString());
                gc_->stats_.already_processed_chunk_count_++;

                should_ensure_persistence = true;
            } else {
                int64_t old_usage_count = mapping_->usage_count();
                mapping_->set_usage_count(old_usage_count + usage_modifier_);

                if (!invert_failed_write_) {
                    mapping_->set_usage_count_change_log_id(context_.log_id());
                } else {
                    mapping_->set_usage_count_failed_write_change_log_id(context_.log_id());
                }

                DEBUG("Chunk " << mapping_->DebugString() <<
                    ", old usage count " << old_usage_count <<
                    ", usage modifier " << (usage_modifier_ > 0 ? "+" : "") <<
                    usage_modifier_ <<
                    ", current log id " << context_.log_id());

                // Overwrites the data in the persistent chunk index
                if (gc_->chunk_index_->PutPersistentIndex(*mapping_, true, false, NO_EC)
                    == PUT_ERROR) {
                    ERROR("Failed to put usage change to index: " <<
                        "usage modifier " << usage_modifier_ <<
                        ", chunk " << mapping_->DebugString());
                    failed = true;
                }
            }
        }
        if (!chunk_locks.Unlock(mapping_->fingerprint(), mapping_->fingerprint_size())) {
            ERROR("Failed to unlock chunk index for mapping: " << mapping_->DebugString());
            failed = true;
        }

        // I shouldn't hold any locks while doing ensure persistent
        if (!failed && should_ensure_persistence) {
            // First we try to ensure the persistence. If the chunk is dirty and
            // not pinned, every thing is fine after step 1)
            // If the item is still pinned and this can happen due to a bad timing,
            // we wait until the direct replay queue is replayed and
            // then try again.

            // The complete idea here is to handle the common case fast and throw
            // bigger guns at the problems if the next least hard/fast
            // way fails.
            bool is_still_pinned = false;
            put_result pr = gc_->chunk_index_->EnsurePersistent(*mapping_, &is_still_pinned);
            if (pr == PUT_ERROR) {
                ERROR("Failed to persist chunk mapping: " << mapping_->DebugString());
                failed = true;
            } else if (pr == PUT_KEEP && is_still_pinned) {

                // I am sure that the container is committed
                // It is checked in ProcessBlockMappingParallel
                enum lookup_result lr = gc_->chunk_index_->ChangePinningState(
                    mapping_->fingerprint(),
                    mapping_->fingerprint_size(),
                    false);
                if (lr == LOOKUP_ERROR) {
                    ERROR("Failed to changed pinning state: " << mapping_->DebugString());
                    failed = true;
                } else if (lr == LOOKUP_NOT_FOUND) {
                    ERROR("Failed to change pinning state: " << mapping_->DebugString() <<
                        ", reason element not found");
                } else {
                    // ok
                    pr = gc_->chunk_index_->EnsurePersistent(*mapping_, &is_still_pinned);
                    if (pr == PUT_ERROR) {
                        ERROR("Failed to persist chunk mapping: " << mapping_->DebugString());
                        failed = true;
                    } else if (pr == PUT_KEEP && is_still_pinned) {
                        ERROR("Mapping should not be still pinned: " << mapping_->DebugString());
                        failed = true;
                    }
                }
            } else if (pr == PUT_KEEP) {
                DEBUG("Mapping wasn't dirty in cache: " << mapping_->DebugString());
            } else {
                // it is persisted, we are fine here
            }
        }

        gc_->stats_.processed_chunk_count_++;

        return !failed;
    }

    bool Run() {
        NESTED_LOG_CONTEXT("gc bg");
        bool b = ProcessMapping();
        delete this;
        return b;
    }

    void Close() {
        delete this;
    }
};

namespace {
void ClearChunkMappings(list<ChunkMapping*>* mappings) {
    for (list<ChunkMapping*>::iterator k = mappings->begin(); k != mappings->end(); ++k) {
        delete *k;
    }
    mappings->clear();
}
}

bool UsageCountGarbageCollector::ProcessBlockMappingParallel(const map<bytestring, pair<int, uint64_t> >& diff,
                                                             bool invert_failed_write, const LogReplayContext& context) {

    DEBUG("Process block mapping diff: " <<
        "diff size " << diff.size() <<
        ", invert failed write " << ToString(invert_failed_write) <<
        ", log id " << context.log_id());

    bool failed = false;
    multimap<uint64_t, ChunkMapping> gc_chunks;

    // We want to determine the replay time used to process the diffs from the time
    // to update the gc index
    ProfileTimer timer(stats_.diff_replay_time_);
    list<Future<bool>*> futures;
    list<ChunkMapping*> mappings;
    map<bytestring, pair<int, uint64_t> >::const_iterator j;
    map<uint64_t, storage_commit_state> cached_storage_commit_state;
    for (j = diff.begin(); j != diff.end(); j++) {
        const byte* fp = reinterpret_cast<const byte*>(j->first.data());
        size_t fp_size = j->first.size();
        int usage_modifier = j->second.first;
        uint64_t address = j->second.second;

        if (Fingerprinter::IsEmptyDataFingerprint(fp, fp_size)) {
            // the artificial fingerprint for an unused block should never be garbage collected.
            continue;
        }

        ChunkMapping* mapping = new ChunkMapping(fp, fp_size);
        mapping->set_data_address(address);

        if (!Storage::IsValidAddress(address)) {
            ERROR("Chunk mapping has invalid address: " <<
                mapping->DebugString());
            delete mapping;
            failed = true;
            break;
        }

        enum storage_commit_state s;
        map<uint64_t, storage_commit_state>::iterator k = cached_storage_commit_state.find(address);
        if (k != cached_storage_commit_state.end()) {
            s = k->second;
        } else {
            s = storage_->IsCommitted(address);
            if (s == STORAGE_ADDRESS_ERROR) {
                ERROR("Error getting commit state of mapping " << mapping->DebugString());
                delete mapping;
                failed = true;
                break;
            }
            if (s == STORAGE_ADDRESS_NOT_COMMITED) {
                uint16_t iteration = 0;
                DEBUG("Will wait for commit for mapping " << mapping->DebugString());
                while ((s == STORAGE_ADDRESS_NOT_COMMITED) && (iteration < 300)) {
                    ThreadUtil::Sleep(1);
                    s = storage_->IsCommitted(address);
                    if (s == STORAGE_ADDRESS_ERROR) {
                        ERROR("Error in iteration " << iteration << " getting commit state of mapping " <<
                            mapping->DebugString());
                        delete mapping;
                        failed = true;
                        break; // breaks only out of the while loop
                    }
                    iteration++;
                }
                if (failed) {
                    break;
                }
                if (s == STORAGE_ADDRESS_NOT_COMMITED) {
                    ERROR("Container for mapping still not imported: " << mapping->DebugString());
                    delete mapping;
                    failed = true;
                    break;
                }
            }
            cached_storage_commit_state[address] = s;
        }

        // Now the container is committed or it will never be committed.
        if (s == STORAGE_ADDRESS_WILL_NEVER_COMMITTED) {
            // mapping should not be considered a gc candidate ever. This chunk
            // mapping is skipped.
            // However, this is not an error. All other chunk mappings should be
            // executed as normal.
            delete mapping;
        } else {
            if (s != STORAGE_ADDRESS_COMMITED) {
                ERROR("Mapping shall never be used, because container will never be committed: " <<
                    mapping->DebugString() << ", commit state " << s);
                delete mapping;
                failed = true;
                break;
            }
            ProcessMappingDiffTask* t = new ProcessMappingDiffTask(this, context, invert_failed_write, mapping,
                usage_modifier);
            Future<bool>* future = tp_->Submit(t);
            if (!future) {
                ERROR("Failed to submit gc task");
                delete mapping;
                failed = true;
                break;
            }
            futures.push_back(future);
            mappings.push_back(mapping);
        }
    }

    // wait for all generated futures
    for (list<Future<bool>*>::iterator j = futures.begin(); j != futures.end(); ++j) {
        Future<bool>* future = *j;
        if (!future->Wait()) {
            future->Close();
            ERROR("Failed to wait for gc diff task execution");
            failed = true;
        } else if (future->is_abort()) {
            ERROR("GC diff task was aborted");
            failed = true;
        } else {
            bool result = false;
            future->Get(&result);
            if (!result) {
                failed = true;
            }
        }
        future->Close();
    }
    futures.clear();
    TRACE("Waiting finished");

    if (!failed) {
        // do not mark items as gc candidates if the processing had failed
        for (list<ChunkMapping*>::iterator k = mappings.begin(); k != mappings.end(); ++k) {
            ChunkMapping* mapping = *k;
            if (mapping->usage_count() <= 0 && mapping->data_address() != Storage::ILLEGAL_STORAGE_ADDRESS) {
                if (mapping->usage_count_change_log_id() > context.log_id()
                    || mapping->usage_count_failed_write_change_log_id() > context.log_id()) {
                    // there is an event with an higher log id that has changes the usage count of this event. We should wait so that
                    // that event marks this event as gc candidate
                    delete mapping;
                    continue;
                }
                if (mapping->usage_count() < 0) {
                    DEBUG("Usage count of chunk is lower zero: " << mapping->DebugString());
                }
                // We mark a mapping with an illegal storage address, if the mapping should not
                // be used considered a gc (with with a <= 0 usage count). Usually this happens
                // when the chunk that is considered here is not committed
                DEBUG("Insert as gc candidate: " << mapping->DebugString());
                gc_chunks.insert(make_pair(mapping->data_address(), *mapping));
            }
            delete mapping;
        }
        mappings.clear();
    } else {
        // free everything even if something failed
        for (list<ChunkMapping*>::iterator k = mappings.begin(); k != mappings.end(); ++k) {
            ChunkMapping* mapping = *k;
            delete mapping;
            mapping = NULL;
        }
        mappings.clear();
    }
    timer.stop();

    if (gc_chunks.size() > 0) {
        CHECK(this->PutGCCandidates(gc_chunks, false), "Failed to store candidate data");
    }
    return !failed;
}

bool UsageCountGarbageCollector::ProcessBlockMapping(const BlockMappingPair& mapping_pair, const LogReplayContext& context) {
    map<bytestring, pair<int, uint64_t> > diff;

    DEBUG("Process block mapping write event: " <<
        mapping_pair.DebugString() <<
        ", event log id " << context.log_id());

    diff = mapping_pair.GetDiff();

    if (!ProcessBlockMappingParallel(diff, false, context)) {
        return false;
    }

    FAULT_POINT("gc.after.block-mapping.processing");
    this->stats_.processed_blocks_++;
    return true;
}

bool UsageCountGarbageCollector::ProcessOphranChunks(const OphranChunksEventData& event_data,
                                                     const dedupv1::log::LogReplayContext& context) {
    TRACE("Process ophran chunk event: " << event_data.ShortDebugString());

    // here we are not interested in rechecking the uc, the data of the failed block mapping had never been
    // persistent. but a new chunk in this block might never see another user and without this re-check
    // if is a zero-user chunk that is no gc candidate

    multimap<uint64_t, ChunkMapping> gc_chunks;
    {
        // We want to determine the replay time used to process the diffs from the time
        // to update the gc index
        ProfileTimer timer(stats_.diff_replay_time_);
        for (int i = 0; i < event_data.chunk_fp_size(); i++) {
            const byte* fp = reinterpret_cast<const byte*>(event_data.chunk_fp(i).data());
            size_t fp_size = event_data.chunk_fp(i).size();

            if (Fingerprinter::IsEmptyDataFingerprint(fp, fp_size)) {
                // the artificial fingerprint for an unused block should never be garbage collected.
                continue;
            }

            DEBUG("Process possible ophran fingerprint: " << ToHexString(fp, fp_size) << ", log id "
                                                          << context.log_id());

            // there is a race condition, so be sure that only the gc is changing the chunk mapping.
            ChunkMapping chunk_mapping(fp, fp_size);
            enum lookup_result lookup_result = this->chunk_index_->Lookup(&chunk_mapping, false, NO_EC);
            CHECK(lookup_result != LOOKUP_ERROR, "Cannot lookup chunk index for chunk mapping " << chunk_mapping.DebugString());
            bool is_gc_candidate = false;
            if (lookup_result == LOOKUP_NOT_FOUND) {
                // we can ignore this fp this it was never committed.
                // this usually indicates that the system crashed shortly after the failed block mapping
                // this is not good, but in this case the system state is fine
            } else if (chunk_mapping.usage_count() <= 0) {
                is_gc_candidate = true;
            }
            if (is_gc_candidate) {
                TRACE("Add to gc: " << ToHexString(fp, fp_size));
                // if the chunk has no user it might be possible that the failed
                // block was the only user. Therefore it might be a gc candidate.

                // it might be possible, that the "block mapping failed" entry is written because the container of the fp
                // is committed. Therefore this message is processed before the container commit event
                // We here fail on the side of security and add the fp as gc index. The later
                // recheck should give the concrete answer.
                // It is an important case what happens, wenn the container of the fp is never committed.
                gc_chunks.insert(make_pair(chunk_mapping.data_address(), chunk_mapping));
            }
        }
    }

    if (gc_chunks.size() > 0) {
        CHECK(this->PutGCCandidates(gc_chunks, true), "Failed to store candidate data");
    }
    return true;
}

bool UsageCountGarbageCollector::ProcessFailedBlockMapping(const BlockMappingPair& mapping_pair,
                                                           Option<int64_t> write_event_log_id, const LogReplayContext& context) {
    map<bytestring, pair<int, uint64_t> > diff;

    diff = mapping_pair.GetDiff();

    DEBUG("Process failed block mapping: " <<
        mapping_pair.DebugString() <<
        ", diff size " << diff.size() <<
        ", event log id " << context.log_id() <<
        ", write event log id " << (write_event_log_id.valid() ? ToString(write_event_log_id.value()) : "<not set>"));

    if (write_event_log_id.valid()) {
        // the block mapping was written and now has to be reverted.
        // See dedupv1.proto for details

        /**
         * true iff no block mapping failed event for the given block write event log id was replayed before.
         * It might happen that multiple failed events are generated for the same block write event, but only a single one should
         * be executed.
         */
        bool already_replayed = replayed_block_failed_event_set_.find(write_event_log_id.value())
                                != replayed_block_failed_event_set_.end();

        if (!already_replayed) {
            map<bytestring, pair<int, uint64_t> > inverted_diff = diff;
            map<bytestring, pair<int, uint64_t> >::iterator j;
            for (j = inverted_diff.begin(); j != inverted_diff.end(); j++) {
                int usage_modifier = j->second.first;
                j->second.first = -1 * usage_modifier; // revert
            }

            // we filter the processing so that the usage count is only updated when the last usage count
            // change id is the log id of the matching block mapping written event
            // This check avoids double replay error. It also avoids problems when for some reason
            // two failed events for the same block failure are generated as it can happen in crash
            // situations.
            if (!ProcessBlockMappingParallel(inverted_diff, true, context)) {
                return false;
            }

            // Here we have no race condition between check and insert as only a single thread is allowed to replay log events
            replayed_block_failed_event_set_.insert(write_event_log_id.value());
            CHECK(DumpMetaInfo(), "Failed to persist gc info data");
            // persist the change to the event set
        } else {
            DEBUG("Skip failed block mapping event: " <<
                mapping_pair.DebugString() <<
                ", event log id " << context.log_id() <<
                ", write event log id " << write_event_log_id.value() <<
                ", already replayed");
        }
    } else {
        DEBUG("Skip failed block mapping: " <<
            mapping_pair.DebugString() <<
            ", event log id " << context.log_id() <<
            ", write event log not set");
    }
    // here we are not interested in rechecking the uc, the data of the failed block mapping had never been
    // persistent. but a new chunk in this block might never see another user and without this re-check
    // if is a zero-user chunk that is no gc candidate
    {
        // We want to determine the replay time used to process the diffs from the time
        // to update the gc index
        ProfileTimer timer(stats_.diff_replay_time_);
        multimap<uint64_t, ChunkMapping> gc_chunks;
        map<bytestring, pair<int, uint64_t> >::iterator j;
        for (j = diff.begin(); j != diff.end(); j++) {
            const byte* fp = reinterpret_cast<const byte*>(j->first.data());
            size_t fp_size = j->first.size();
            int usage_modifier = j->second.first;
            uint64_t address = j->second.second;

            if (Fingerprinter::IsEmptyDataFingerprint(fp, fp_size)) {
                // the artificial fingerprint for an unused block should never be garbage collected.
                continue;
            }
            if (usage_modifier <= 0) {
                // here we only care about the chunks that would have been increased by this
                // block mapping that never happened.
                continue;
            }

            TRACE("Process failed fingerprint: " << ToHexString(fp, fp_size) << ", usage modifier " << usage_modifier
                                                 << ", address " << address);

            // there is a race condition, so be sure that only the gc is changing the chunk mapping.
            ChunkMapping chunk_mapping(fp, fp_size);
            chunk_mapping.set_data_address(address);
            enum lookup_result lookup_result = this->chunk_index_->Lookup(&chunk_mapping, false, NO_EC);
            CHECK(lookup_result != LOOKUP_ERROR, "Cannot lookup chunk index for chunk mapping " << chunk_mapping.DebugString());
            bool is_gc_candidate = false;
            if (lookup_result == LOOKUP_NOT_FOUND) {
                dedupv1::chunkstore::storage_commit_state state = this->storage_->IsCommitted(address);
                CHECK(state != dedupv1::chunkstore::STORAGE_ADDRESS_ERROR, "Failed to check commit state: address " << address <<
                    ", chunk mapping " << chunk_mapping.DebugString());
                if (state == dedupv1::chunkstore::STORAGE_ADDRESS_COMMITED) {
                    is_gc_candidate = true;
                } else {
                    // we can ignore this fp this it was never committed.
                    // this usually indicates that the system crashed shortly after the failed block mapping
                    // this is not good, but in this case the system state is fine
                }
            } else if (chunk_mapping.usage_count() <= 0) {
                is_gc_candidate = true;
            }
            if (is_gc_candidate) {
                TRACE("Add to gc: " << ToHexString(fp, fp_size));
                // if the chunk has no user it might be possible that the failed
                // block was the only user. Therefore it might be a gc candidate.

                // it might be possible, that the "block mapping failed" entry is written because the container of the fp
                // is committed. Therefore this message is processed before the container commit event
                // We here fail on the side of security and add the fp as gc index. The later
                // recheck should give the concrete answer.
                // It is an important case what happens, when the container of the fp is never committed.
                gc_chunks.insert(make_pair(chunk_mapping.data_address(), chunk_mapping));
            }
        }

        if (gc_chunks.size() > 0) {
            CHECK(this->PutGCCandidates(gc_chunks, true), "Failed to store candidate data");
        }
    }

    this->stats_.processed_blocks_++;
    return true;
}

bool UsageCountGarbageCollector::ProcessFailedBlockMappingDirect(const BlockMappingPair& mapping_pair, dedupv1::base::Option<
                                                                     int64_t> write_event_committed, const dedupv1::log::LogReplayContext& context) {

    DEBUG("Process failed block mapping (direct): " <<
        mapping_pair.DebugString() <<
        ", log id " << context.log_id());

    // failed writes are never processed in the direct replay
    return true;
}

bool UsageCountGarbageCollector::ProcessFailedBlockMappingDirtyStart(const BlockMappingPair& mapping_pair, dedupv1::base::Option<
                                                                         int64_t> write_event_committed, const dedupv1::log::LogReplayContext& context) {

    DEBUG("Process failed block mapping (dirty start): " <<
        mapping_pair.DebugString() <<
        ", log id " << context.log_id());

    // failed writes are never processed in the direct replay
    return true;
}

class ProcessMappingDirtyDiffTask : public Runnable<bool> {
private:
    UsageCountGarbageCollector* gc_;
    int usage_modifier_;
    LogReplayContext context_;
    uint64_t block_id_;
    bytestring fp_;
    uint32_t address_;
public:

    /**
     * Constructor
     */
    ProcessMappingDirtyDiffTask(UsageCountGarbageCollector* gc,
                                const LogReplayContext& context,
                                uint64_t block_id,
                                const bytestring& fp,
                                uint64_t address,
                                int usage_mod) :
        gc_(gc), usage_modifier_(usage_mod), context_(context),
        block_id_(block_id),fp_(fp), address_(address) {
    }

    /**
     * Destructor
     */
    virtual ~ProcessMappingDirtyDiffTask() {
    }

    bool Run() {
        NESTED_LOG_CONTEXT("gc dirty");

        ChunkMapping mapping(fp_.data(), fp_.size());
        mapping.set_data_address(address_);

        bool b = gc_->ProcessDiffDirtyStart(&mapping, block_id_, usage_modifier_, context_);
        delete this;
        return b;
    }

    void Close() {
        delete this;
    }
};

bool UsageCountGarbageCollector::ProcessDiffDirtyStart(ChunkMapping* mapping,
                                                       uint64_t block_id,
                                                       int usage_modifier,
                                                       const dedupv1::log::LogReplayContext& context) {
    DCHECK(mapping, "Mapping not set");

    Option<bool> r = chunk_index_->IsContainerImported(mapping->data_address());
    CHECK(r.valid(), "Failed to check container import state " << mapping->DebugString());
    if (r.value()) {
        // imported before, I need the current data. I need it from disk.

        ChunkMapping aux_mapping(mapping->fingerprint(), mapping->fingerprint_size());
        enum lookup_result ci_lr = chunk_index_->LookupPersistentIndex(&aux_mapping,
            dedupv1::base::CACHE_LOOKUP_DEFAULT, dedupv1::base::CACHE_ALLOW_DIRTY, NO_EC);
        CHECK(ci_lr != LOOKUP_ERROR,
            "Failed to lookup chunk index: " << mapping->DebugString());
        if (ci_lr == LOOKUP_NOT_FOUND) {
            uint64_t container_id = mapping->data_address();
            storage_commit_state commit_state = storage_->IsCommittedWait(container_id);
            CHECK(commit_state != STORAGE_ADDRESS_ERROR,
                "Failed to check commit state: " << container_id);
            if (commit_state == STORAGE_ADDRESS_WILL_NEVER_COMMITTED) {
                INFO( "Missing container for block write during dirty start: " <<
                    "usage modifier " << (usage_modifier > 0 ? "+" : "") <<
                    usage_modifier <<
                    ", chunk " << mapping->DebugString());
                return true; // leave method
            } else if (commit_state == STORAGE_ADDRESS_NOT_COMMITED) {
                INFO("Missing container for block write during dirty start: " <<
                    "usage modifier " << (usage_modifier > 0 ? "+" : "") <<
                    usage_modifier <<
                    ", chunk " << mapping->DebugString());
                return true; // leave method
            }
            // the container is actually committed (and imported), and we cannot
            // find the chunk in the chunk index
            WARNING("Failed to find chunk in chunk index for block write during dirty start: " <<
                "usage modifier " << (usage_modifier > 0 ? "+" : "") << usage_modifier <<
                ", chunk " << mapping->DebugString());
        } else if (aux_mapping.usage_count_change_log_id() < context.log_id()) {
            DEBUG(
                "Load chunk into gc startup index: " << mapping->DebugString() <<
                ", stored mapping " << aux_mapping.DebugString());
            mapping->set_usage_count(aux_mapping.usage_count() + usage_modifier);
            mapping->set_usage_count_change_log_id(context.log_id());

            CHECK(chunk_index_->PutPersistentIndex(*mapping, false, false, NO_EC)
                != PUT_ERROR,
                "Failed to put usage change to index: " <<
                ", usage modifier " << usage_modifier <<
                ", chunk " << mapping->DebugString());
        } else {
            DEBUG("Skip update chunk mapping during dirty start: " <<
                "current mapping " << mapping->DebugString() <<
                ", stored mapping " << aux_mapping.DebugString() <<
                ", usage modifier " << (usage_modifier > 0 ? "+" : "") <<
                usage_modifier <<
                ", log id " << context.log_id());
        }
    } else {
        // container not imported before
        ChunkMapping aux_mapping(mapping->fingerprint(), mapping->fingerprint_size());
        // if the container was not imported before, we there is need to go to disk
        // is relys on the fact that we pin the data to memory
        enum lookup_result ci_lr = chunk_index_->LookupPersistentIndex(&aux_mapping,
            dedupv1::base::CACHE_LOOKUP_ONLY,
            dedupv1::base::CACHE_ALLOW_DIRTY, NO_EC);
        CHECK(ci_lr != LOOKUP_ERROR, "Failed to lookup chunk index: " <<
            mapping->DebugString());
        if (ci_lr == LOOKUP_NOT_FOUND) {
            mapping->set_block_hint(block_id);
            mapping->set_usage_count(usage_modifier);
            mapping->set_usage_count_change_log_id(context.log_id());
            DEBUG("Load chunk into cache: " << mapping->DebugString());

            bool has_to_pin = true;
            storage_commit_state commit_state = storage_->IsCommitted(mapping->data_address());
            CHECK(commit_state != STORAGE_ADDRESS_ERROR, "Failed to check commit state");
            if (commit_state == STORAGE_ADDRESS_COMMITED) {
                has_to_pin = false;
                // this is one of the major cases where we know that a container
                // is committed even before the end of the dirty replay. But we
                // cannot get the correct location of the container or similar
                // things. Some container might be committed, but we don't know
                // that yet. Therefore we have to pin them.
                // Some container might not be committed, we pin them, too.
            }
            CHECK(chunk_index_->PutPersistentIndex(*mapping, false, has_to_pin, NO_EC) != PUT_ERROR,
                "Failed to put usage change to index: " <<
                ", usage modifier " << usage_modifier <<
                ", chunk " << mapping->DebugString());

        } else if (aux_mapping.usage_count_change_log_id() < context.log_id()) {
            DEBUG("Load chunk into gc startup index: " <<
                mapping->DebugString() <<
                ", stored mapping " << aux_mapping.DebugString());
            mapping->set_block_hint(block_id);
            mapping->set_usage_count(aux_mapping.usage_count() + usage_modifier);
            mapping->set_usage_count_change_log_id(context.log_id());

            // The problem here is that we have no information if the address is
            // committed or not.
            // Therefore we pin it to the main memory
            CHECK(chunk_index_->PutPersistentIndex(*mapping, false, true, NO_EC)
                != PUT_ERROR,
                "Failed to put usage change to index: " <<
                "usage modifier " << usage_modifier <<
                ", chunk " << mapping->DebugString());
        } else {
            DEBUG("Skip update chunk mapping during dirty start: " <<
                "current mapping " << mapping->DebugString() <<
                ", stored mapping " << aux_mapping.DebugString() <<
                ", usage modifier " << (usage_modifier > 0 ? "+" : "") << usage_modifier <<
                ", log id " << context.log_id());
        }
    }
    return true;
}

bool UsageCountGarbageCollector::ProcessDiffDirect(ChunkMapping* mapping,
                                                   uint64_t block_id,
                                                   int usage_modifier,
                                                   const dedupv1::log::LogReplayContext& context) {
    DCHECK(mapping, "Mapping not set");
    DCHECK(usage_modifier != 0, "Illegal usage modifier: " << usage_modifier);

    bool failed = false;
    ChunkLocks& chunk_locks(chunk_index_->chunk_locks());
    if (!chunk_locks.Lock(mapping->fingerprint(), mapping->fingerprint_size())) {
        ERROR("Failed to lock chunk index for mapping: " << mapping->DebugString());
        mapping->set_data_address(Storage::ILLEGAL_STORAGE_ADDRESS);
        return false;
    }
    enum lookup_result lookup_result = chunk_index_->LookupPersistentIndex(mapping,
        dedupv1::base::CACHE_LOOKUP_DEFAULT, dedupv1::base::CACHE_ALLOW_DIRTY, NO_EC);
    if (lookup_result == LOOKUP_ERROR) {
        ERROR("Cannot lookup chunk index for chunk mapping " << mapping->DebugString());
        failed = true;
    }
    if (lookup_result == LOOKUP_FOUND) {
        // we are now sure that the chunk index exists and that the container
        // for all entries is committed.
        // if the change result should now be applied to chunk index
        // This happens when the background replay is faster than the direct replay
        if (mapping->usage_count_change_log_id() >= context.log_id()) {
            ERROR("Current event has already been processed: " <<
                "current log id " << context.log_id() <<
                ", chunk " << mapping->DebugString());
            failed = true;
        } else {
            mapping->set_block_hint(block_id);
            int64_t old_usage_count = mapping->usage_count();
            mapping->set_usage_count(old_usage_count + usage_modifier);

            mapping->set_usage_count_change_log_id(context.log_id());
            DEBUG("Chunk " << mapping->DebugString() <<
                ", old usage count " << old_usage_count <<
                ", block hint " << mapping->block_hint() <<
                ", usage modifier " << (usage_modifier > 0 ? "+" : "") <<
                usage_modifier <<
                ", replay type " <<
                dedupv1::log::Log::GetReplayModeName(context.replay_mode()));

            // Overwrites the data in the chunk index as dirty chunk
            if (chunk_index_->PutPersistentIndex(*mapping, false, false, NO_EC) == PUT_ERROR) {
                ERROR("Failed to put usage change to index: " <<
                    "usage modifier " << usage_modifier <<
                    ", chunk " << mapping->DebugString());
                failed = true;
            }
        }
    }
    if (!chunk_locks.Unlock(mapping->fingerprint(), mapping->fingerprint_size())) {
        ERROR("Failed to unlock chunk index for mapping: " << mapping->DebugString());
        failed = true;
    }
    return !failed;
}

bool UsageCountGarbageCollector::ProcessBlockMappingDirect(const BlockMappingPair& mapping_pair,
                                                           const dedupv1::log::LogReplayContext& context) {

    DEBUG("Process block mapping (direct): " << mapping_pair.DebugString() <<
        ", log id " << context.log_id());

    bool failed = false;
    map<bytestring, pair<int, uint64_t> > diff = mapping_pair.GetDiff();

    map<bytestring, pair<int, uint64_t> >::const_iterator j;
    for (j = diff.begin(); j != diff.end(); j++) {
        const byte* fp = reinterpret_cast<const byte*>(j->first.data());
        size_t fp_size = j->first.size();
        int usage_modifier = j->second.first;
        uint64_t address = j->second.second;

        if (Fingerprinter::IsEmptyDataFingerprint(fp, fp_size)) {
            // the artificial fingerprint for an unused block should never be
            // garbage collected.
            continue;
        }
        if (usage_modifier == 0) {
            continue;
        }
        ChunkMapping mapping(fp, fp_size);
        mapping.set_data_address(address);

        if (!ProcessDiffDirect(&mapping,
                mapping_pair.block_id(),
                usage_modifier,
                context)) {
            ERROR("Failed to process block mapping pair for chunk: " <<
                "mapping " << mapping.DebugString() <<
                ", block mapping pair " << mapping_pair.DebugString() <<
                ", log context " << context.DebugString());
            failed = true;
            break;
        }
    }
    return !failed;
}

bool UsageCountGarbageCollector::ProcessBlockMappingDirtyStart(const BlockMappingPair& mapping_pair,
                                                               const dedupv1::log::LogReplayContext& context) {

    DEBUG("Process process block mapping (dirty start): " << mapping_pair.DebugString() <<
        ", log id " << context.log_id());

    map<bytestring, pair<int, uint64_t> > diff = mapping_pair.GetDiff();
    map<bytestring, pair<int, uint64_t> >::const_iterator j;

    bool failed = false;
    list<Future<bool>*> futures;
    for (j = diff.begin(); j != diff.end(); j++) {
        const byte* fp = reinterpret_cast<const byte*>(j->first.data());
        size_t fp_size = j->first.size();
        int usage_modifier = j->second.first;
        uint64_t address = j->second.second;

        if (Fingerprinter::IsEmptyDataFingerprint(fp, fp_size)) {
            // the artificial fingerprint for an unused block should never be
            // garbage collected.
            continue;
        }
        if (usage_modifier == 0) {
            continue;
        }

        bytestring fp_str;
        fp_str.assign(fp, fp_size);

        Runnable<bool>* r = new ProcessMappingDirtyDiffTask(this, context,
            mapping_pair.block_id(),
            fp_str,
            address,
            usage_modifier);

        Future<bool>* future = this->tp_->Submit(r);
        if (future == NULL) {
            ERROR("Failed to submit gc task");
            failed = true;
            continue;
        }
        futures.push_back(future);
    }

    // wait for all generated futures
    for (list<Future<bool>*>::iterator j = futures.begin(); j != futures.end(); ++j) {
        Future<bool>* future = *j;
        if (!future->Wait()) {
            future->Close();
            WARNING("Failed to wait for gc diff task execution");
            failed = true;
        } else if (future->is_abort()) {
            WARNING("GC diff task was aborted");
            failed = true;
        } else {
            bool result = false;
            future->Get(&result);
            if (!result) {
                failed = true;
            }
        }
        future->Close();
    }
    futures.clear();
    return !failed;
}

Option<bool> UsageCountGarbageCollector::IsGCCandidate(uint64_t address,
                                                       const void* fp,
                                                       size_t fp_size) {
    ScopedLock scoped_lock(&this->candidate_info_lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire candidate info lock");

    GarbageCollectionCandidateData candidate_data;
    lookup_result r = this->candidate_info_->Lookup(&address, sizeof(address),
        &candidate_data);
    CHECK(r != LOOKUP_ERROR,
        "Failed to lookup candidate data: container id " << address);
    bool result = false;
    if (r == LOOKUP_FOUND) {
        bool found = false;
        for (int k = 0; k < candidate_data.item_size(); k++) {
            if (fp_size != candidate_data.item(k).fp().size()) {
                continue;
            }
            if (memcmp(candidate_data.item(k).fp().data(), fp, fp_size) == 0) {
                found = true;
                break;
            }
        }
        result = found;
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release candidate info lock");
    return make_option(result);
}

bool UsageCountGarbageCollector::PutGCCandidates(
    const multimap<uint64_t, ChunkMapping>& gc_chunks,
    bool failed_mode) {
    ProfileTimer timer(stats_.update_index_time_);

    ScopedLock scoped_lock(&this->candidate_info_lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire candidate info lock");
    // the idiom is based on http://www.ureader.de/msg/12291786.aspx
    multimap<uint64_t, ChunkMapping>::const_iterator i;
    multimap<uint64_t, ChunkMapping>::const_iterator j;
    bool changed = false;
    for (i = gc_chunks.begin(); i != gc_chunks.end(); ) {
        uint64_t address = i->first;

        TRACE("Process new gc chunks: container id " << address);

        GarbageCollectionCandidateData candidate_data;
        lookup_result r = this->candidate_info_->Lookup(&address, sizeof(address), &candidate_data);
        CHECK(r != LOOKUP_ERROR, "Failed to lookup candidate data: container id " << address);
        if (r == LOOKUP_FOUND) {
            TRACE("Found candidate data: container id " << candidate_data.address());
            CHECK(candidate_data.has_address() && candidate_data.address() == address,
                "GC candidate address mismatch: " <<
                "container id " << address <<
                ", candidate data " << candidate_data.ShortDebugString());
        } else {
            // not found => init
            candidate_data.set_address(address);
            TRACE("Init candidate data: " << candidate_data.ShortDebugString());
        }

        changed = false;
        j = gc_chunks.upper_bound(i->first);
        do {
            const ChunkMapping& chunk_mapping(i->second);

            // We check that the same fp is not added as gc candidate twice
            bool found = false;
            for (int k = 0; k < candidate_data.item_size(); k++) {
                if (chunk_mapping.fingerprint_size() != candidate_data.item(k).fp().size()) {
                    continue;
                }
                if (memcmp(candidate_data.item(k).fp().data(), chunk_mapping.fingerprint(),
                        chunk_mapping.fingerprint_size()) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                GarbageCollectionCandidateItemData* item_data = candidate_data.add_item();
                item_data->set_fp(chunk_mapping.fingerprint(), chunk_mapping.fingerprint_size());
                if (failed_mode) {
                    item_data->set_type(GarbageCollectionCandidateItemData::FAILED);
                } else {
                    item_data->set_type(GarbageCollectionCandidateItemData::STANDARD);
                }
                changed = true;
            }
        } while (++i != j);
        // do not access j or i here

        if (changed) {
            CHECK(candidate_data.has_address() && candidate_data.address() == address,
                "GC candidate address mismatch: " <<
                ", container id " << address <<
                ", candidate data " << candidate_data.ShortDebugString());
            TRACE("Update gc candidate data: " <<
                "container id " << address <<
                ", data " << candidate_data.ShortDebugString());
            CHECK(this->candidate_info_->Put(&address, sizeof(address), candidate_data),
                "Cannot store candidate data: " << candidate_data.ShortDebugString());
        }
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release candidate info lock");
    return true;
}

bool UsageCountGarbageCollector::LogReplay(enum event_type event_type, const LogEventData& event_value,
                                           const LogReplayContext& context) {

    Profile* profile = NULL;
    if (context.replay_mode() == EVENT_REPLAY_MODE_DIRECT) {
        profile = &stats_.direct_log_replay_time_;
    } else if (context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
        profile = &stats_.dirty_start_log_replay_time_;
    } else {
        profile = &stats_.log_replay_time_;
    }
    ProfileTimer timer(*profile);
    if (event_type == EVENT_TYPE_BLOCK_MAPPING_WRITTEN) {
        BlockMappingWrittenEventData event_data = event_value.block_mapping_written_event();
        CHECK(event_data.has_mapping_pair(), "Event data has no block mapping");

        BlockMappingPair mapping_pair(this->block_size_);
        CHECK(mapping_pair.CopyFrom(event_data.mapping_pair()),
            "Cannot copy block mapping: " << event_data.mapping_pair().ShortDebugString());

        if (context.replay_mode() == EVENT_REPLAY_MODE_REPLAY_BG) {
            CHECK(ProcessBlockMapping(mapping_pair, context),
                "Cannot processes committed block mapping: " << mapping_pair.DebugString());
        } else if (context.replay_mode() == EVENT_REPLAY_MODE_DIRECT) {
            CHECK(ProcessBlockMappingDirect(mapping_pair, context),
                "Cannot processes committed block mapping (direct): " << mapping_pair.DebugString());
        } else if (context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
            CHECK(ProcessBlockMappingDirtyStart(mapping_pair, context),
                "Cannot processes committed block mapping (dirty start): " << mapping_pair.DebugString());
        }
    } else if (event_type == EVENT_TYPE_BLOCK_MAPPING_DELETED) {
        BlockMappingDeletedEventData event_data = event_value.block_mapping_deleted_event();
        CHECK(event_data.has_original_block_mapping(), "Event data has no original block mapping");

        BlockMapping orig_mapping(this->block_size_);
        CHECK(orig_mapping.CopyFrom(event_data.original_block_mapping()), "Cannot copy from original block mapping");

        if (context.replay_mode() == EVENT_REPLAY_MODE_REPLAY_BG) {
            CHECK(ProcessDeletedBlockMapping(orig_mapping, context),
                "Cannot processes deleted block mapping: " << orig_mapping.DebugString());
        } else if (context.replay_mode() == EVENT_REPLAY_MODE_DIRECT) {
            CHECK(ProcessDeletedBlockMappingDirect(orig_mapping, context),
                "Cannot processes deleted block mapping (direct): " << orig_mapping.DebugString());
        } else if (context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
            CHECK(ProcessDeletedBlockMappingDirtyStart(orig_mapping, context),
                "Cannot processes deleted block mapping (dirty start): " << orig_mapping.DebugString());
        }
    } else if (event_type == EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED) {
        BlockMappingWriteFailedEventData event_data = event_value.block_mapping_write_failed_event();
        DCHECK(event_data.has_mapping_pair(),
            "Event data has no block mapping pair: " << event_data.ShortDebugString());

        BlockMappingPair mapping_pair(block_size_);
        CHECK(mapping_pair.CopyFrom(event_data.mapping_pair()),
            "Failed to copy block mapping pair: " << event_data.ShortDebugString());

        Option<int64_t> wec;
        if (event_data.has_write_event_log_id()) {
            wec = make_option(event_data.write_event_log_id());
        }

        if (context.replay_mode() == EVENT_REPLAY_MODE_REPLAY_BG) {
            CHECK(ProcessFailedBlockMapping(mapping_pair, wec, context),
                "Cannot processes failed block mapping: " << mapping_pair.DebugString());
        } else if (context.replay_mode() == EVENT_REPLAY_MODE_DIRECT) {
            CHECK(ProcessFailedBlockMappingDirect(mapping_pair, wec, context),
                "Cannot processes failed block mapping (direct): " << mapping_pair.DebugString());
        } else if (context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
            CHECK(ProcessFailedBlockMappingDirtyStart(mapping_pair, wec, context),
                "Cannot processes failed block mapping (dirty start): " << mapping_pair.DebugString());
        }
    } else if (context.replay_mode() == EVENT_REPLAY_MODE_REPLAY_BG && event_type == EVENT_TYPE_OPHRAN_CHUNKS) {
        OphranChunksEventData event_data = event_value.ophran_chunks_event();
        CHECK(ProcessOphranChunks(event_data, context),
            "Failed to process ophran chunks: event data " << event_data.ShortDebugString());
    } else if (event_type == EVENT_TYPE_LOG_EMPTY) {
        TRACE("Clear replay block failed event set");
        replayed_block_failed_event_set_.clear();
        if (!DumpMetaInfo()) {
            WARNING("Failed to dump gc meta data");
        }
    }
    return true;
}

bool UsageCountGarbageCollector::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    GarbageCollectorStatsData data;
    data.set_processed_block_count(this->stats_.processed_blocks_);
    data.set_processed_gc_candidate_count(this->stats_.processed_gc_candidates_);
    data.set_skipped_chunk_count(this->stats_.skipped_chunk_mapping_count_);
    data.set_already_processed_chunk_count(stats_.already_processed_chunk_count_);
    data.set_processed_chunk_count(stats_.processed_chunk_count_);
    CHECK(ps->Persist(prefix, data), "Failed to persist gc stats");
    return true;
}

bool UsageCountGarbageCollector::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    GarbageCollectorStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to load gc stats");
    this->stats_.processed_blocks_ = data.processed_block_count();
    this->stats_.processed_gc_candidates_ = data.processed_gc_candidate_count();
    this->stats_.skipped_chunk_mapping_count_ = data.skipped_chunk_count();
    this->stats_.already_processed_chunk_count_ = data.already_processed_chunk_count();
    stats_.processed_chunk_count_ = data.processed_chunk_count();
    return true;
}

string UsageCountGarbageCollector::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"gc candidate count\": " << this->candidate_info_->GetItemCount() << "," << std::endl;
    sstr << "\"processed blocks\": " << this->stats_.processed_blocks_ << "," << std::endl;
    sstr << "\"skipped chunk count\": " << this->stats_.skipped_chunk_mapping_count_ << "," << std::endl;
    sstr << "\"already processed chunk count\": " << this->stats_.already_processed_chunk_count_ << "," << std::endl;
    sstr << "\"processed chunk count\": " << this->stats_.processed_chunk_count_ << "," << std::endl;
    sstr << "\"processed gc candidates\": " << this->stats_.processed_gc_candidates_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string UsageCountGarbageCollector::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"gc thread time\": " << this->stats_.gc_thread_time_.GetSum() << "," << std::endl;
    sstr << "\"diff replay time\": " << this->stats_.diff_replay_time_.GetSum() << "," << std::endl;
    sstr << "\"update index time\": " << this->stats_.update_index_time_.GetSum() << "," << std::endl;
    sstr << "\"direct replay time\": " << this->stats_.direct_log_replay_time_.GetSum() << "," << std::endl;
    sstr << "\"dirty start replay time\": " << this->stats_.dirty_start_log_replay_time_.GetSum() << "," << std::endl;
    sstr << "\"log replay time\": " << this->stats_.log_replay_time_.GetSum() << ", " << std::endl;

    sstr << "\"candidate info index\": ";
    if (candidate_info_) {
        sstr << candidate_info_->PrintProfile();
    } else {
        sstr << "null";
    }

    sstr << "}";
    return sstr.str();
}

string UsageCountGarbageCollector::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"candidate info index\": ";
    if (candidate_info_) {
        sstr << candidate_info_->PrintTrace();
    } else {
        sstr << "null";
    }

    sstr << "}";
    return sstr.str();
}

string UsageCountGarbageCollector::PrintLockStatistics() {
    return "null";
}

#ifdef DEDUPV1_CORE_TEST
void UsageCountGarbageCollector::ClearData() {
    Stop(dedupv1::StopContext::WritebackStopContext());

    if (this->candidate_info_) {
        if (!this->candidate_info_->Close()) {
            WARNING("Failed to close gc candidate info");
        }
        this->candidate_info_ = NULL;
    }
}
#endif

dedupv1::base::PersistentIndex* UsageCountGarbageCollector::candidate_info() {
    return this->candidate_info_;
}

UsageCountGarbageCollector::Statistics::Statistics() {
    processed_blocks_ = 0;
    processed_gc_candidates_ = 0;
    skipped_chunk_mapping_count_ = 0;
    already_processed_chunk_count_ = 0;
    processed_chunk_count_ = 0;
}

}
}
