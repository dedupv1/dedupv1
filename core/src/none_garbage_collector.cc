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

#include <core/none_garbage_collector.h>
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


void NoneGarbageCollector::RegisterGC() {
  GarbageCollector::Factory().Register("none",
      &NoneGarbageCollector::CreateGC);
}

GarbageCollector* NoneGarbageCollector::CreateGC() {
  return new NoneGarbageCollector();
}

NoneGarbageCollector::NoneGarbageCollector() :
    GarbageCollector(NONE) {
    chunk_index_ = NULL;
    info_store_ = NULL;
    log_ = NULL;
    idle_detector_ = NULL;
    state_ = CREATED;
    paused_ = false;
}
NoneGarbageCollector::~NoneGarbageCollector() {
}

bool NoneGarbageCollector::PauseProcessing() {
    paused_ = true;
    if (this->state_ == CANDIDATE_PROCESSING) {
        StopProcessing();
    }
    DEBUG("Garbage Collector is paused");
    return true;
}

bool NoneGarbageCollector::ResumeProcessing() {
    paused_ = false;
    DEBUG("Garbage Collector is no more paused");
    return true;
}

bool NoneGarbageCollector::Start(const StartContext& start_context,
    DedupSystem* system) {
    CHECK(system, "System not set");
    CHECK(this->state_ == CREATED, "GC already started");

    INFO("Starting gc");
    this->log_ = system->log();
    CHECK(this->log_, "Log not set");

    this->chunk_index_ = system->chunk_index();
    CHECK(this->chunk_index_, "Chunk index not set");

    info_store_ = system->info_store();
    CHECK(info_store_, "Info store not set");

    idle_detector_ = system->idle_detector();
    CHECK(this->idle_detector_, "Idle detector not set");

    block_size_ = system->block_size();

    tp_ = system->threadpool();
    CHECK(tp_, "Threadpool not set");

    this->state_ = STARTED;
    DEBUG("Started gc");

    CHECK(this->idle_detector_->RegisterIdleConsumer("gc", this),
        "Cannot register gc as idle tick consumer");
    CHECK(this->log_->RegisterConsumer("gc", this),
        "Cannot register gc consumer");
    return true;
}

bool NoneGarbageCollector::Close() {
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

bool NoneGarbageCollector::SetOption(const std::string& option_name,
    const std::string& option) {
    ERROR("Illegal option: " << option_name << "=" << option);
    return false;
}

bool NoneGarbageCollector::Stop(const dedupv1::StopContext& stop_context) {
    if (state_ != STOPPED) {
        INFO("Stopping gc");
    }

    if (this->idle_detector_ && idle_detector_->IsRegistered("gc").value()) {
        DEBUG("Remove from idle detector");
        if (!this->idle_detector_->UnregisterIdleConsumer("gc")) {
            WARNING("Failed to unregister gc idle tick consumer");
        }
    }

    // we change the state after we removed the idle detector listener
    // so the idle callback cannot change the state

    this->state_ = STOPPING;
    state_ = STOPPED;
    DEBUG("Stopped gc");
    return true;
}

bool NoneGarbageCollector::Run() {
    state original_state = state_.compare_and_swap(RUNNING, STARTED);
    CHECK(original_state == STARTED, "Illegal state: " << this->state_);

    DEBUG("Running gc");
    return true;
}

void NoneGarbageCollector::IdleStart() {
    TRACE("GarbageCollector Idle started");
    if (this->paused_) {
        DEBUG("Idle time started but Garbage Collector is paused");
        return;
    }
    if (!StartProcessing()) {
        WARNING("Failed to start gc processing");
    }
}

void NoneGarbageCollector::IdleEnd() {
    TRACE("GarbageCollector Idle ended");
    if (!StopProcessing()) {
        WARNING("Failed to stop gc processing");
    }
}

bool NoneGarbageCollector::IsProcessing() {
    return this->state_ == CANDIDATE_PROCESSING;
}

bool NoneGarbageCollector::StartProcessing() {
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
    }
    return true;
}

bool NoneGarbageCollector::StopProcessing() {
    // we try to switch from RUNNING to CANDIDATE_PROCESSING. We don't care if this fails.
    state old_state = this->state_.compare_and_swap(RUNNING, CANDIDATE_PROCESSING);
    if (old_state != RUNNING && old_state != CANDIDATE_PROCESSING) {
        ERROR("Illegal state: " << old_state);
        return false;
    } else {
        TRACE("Switch to running state: new state " << this->state_);
    }
    return true;
}

bool NoneGarbageCollector::ProcessDiffDirtyStart(ChunkMapping* mapping,
                                                       uint64_t block_id,
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
          // the chunk is not in the chunk index, here the block hint is not updated
        } else if (aux_mapping.usage_count_change_log_id() < context.log_id()) {
            DEBUG(
                "Load chunk into gc startup index: " << mapping->DebugString() <<
                ", stored mapping " << aux_mapping.DebugString());
            mapping->set_block_hint(block_id);
            mapping->set_usage_count_change_log_id(context.log_id());

            CHECK(chunk_index_->PutPersistentIndex(*mapping, false, false, NO_EC)
                != PUT_ERROR,
                "Failed to put usage change to index: " <<
                ", chunk " << mapping->DebugString());
        } else {
            DEBUG("Skip update chunk mapping during dirty start: " <<
                "current mapping " << mapping->DebugString() <<
                ", stored mapping " << aux_mapping.DebugString() <<
                ", log id " << context.log_id());
        }
    } else {
        // container not imported before
    }
    return true;
}

bool NoneGarbageCollector::ProcessDiffDirect(ChunkMapping* mapping,
                                                   uint64_t block_id,
                                                   const dedupv1::log::LogReplayContext& context) {
    DCHECK(mapping, "Mapping not set");

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
            mapping->set_usage_count_change_log_id(context.log_id());
            DEBUG("Chunk " << mapping->DebugString() <<
                ", block hint " << mapping->block_hint() <<
                ", replay type " <<
                dedupv1::log::Log::GetReplayModeName(context.replay_mode()));

            // Overwrites the data in the chunk index as dirty chunk
            if (chunk_index_->PutPersistentIndex(*mapping, false, false, NO_EC) == PUT_ERROR) {
                ERROR("Failed to put usage change to index: " <<
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

bool NoneGarbageCollector::ProcessBlockMappingDirect(const BlockMappingPair& mapping_pair,
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
        if (usage_modifier <= 0) {
            // we here are only interessed in new usages
            continue;
        }
        ChunkMapping mapping(fp, fp_size);
        mapping.set_data_address(address);

        if (!ProcessDiffDirect(&mapping,
                mapping_pair.block_id(),
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

bool NoneGarbageCollector::ProcessBlockMappingDirtyStart(const BlockMappingPair& mapping_pair,
                                                               const dedupv1::log::LogReplayContext& context) {

    DEBUG("Process process block mapping (dirty start): " << mapping_pair.DebugString() <<
        ", log id " << context.log_id());

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

        bool b = ProcessDiffDirtyStart(&mapping, mapping_pair.block_id(),
            context);
        CHECK(b, "Failed to process chunk on dirty start: " <<
            mapping.DebugString());
    }
    return true;
}

Option<bool> NoneGarbageCollector::IsGCCandidate(uint64_t address,
                                                       const void* fp,
                                                       size_t fp_size) {
    return make_option(false);
}

bool NoneGarbageCollector::PutGCCandidates(
    const multimap<uint64_t, ChunkMapping>& gc_chunks,
    bool failed_mode) {
    return true;
}

bool NoneGarbageCollector::LogReplay(enum event_type event_type, const LogEventData& event_value,
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
          // pass
        } else if (context.replay_mode() == EVENT_REPLAY_MODE_DIRECT) {
            CHECK(ProcessBlockMappingDirect(mapping_pair, context),
                "Cannot processes committed block mapping (direct): " << mapping_pair.DebugString());
        } else if (context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
            CHECK(ProcessBlockMappingDirtyStart(mapping_pair, context),
                "Cannot processes committed block mapping (dirty start): " << mapping_pair.DebugString());
        }
    }
    return true;
}

bool NoneGarbageCollector::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    GarbageCollectorStatsData data;
    data.set_processed_block_count(this->stats_.processed_blocks_);
    CHECK(ps->Persist(prefix, data), "Failed to persist gc stats");
    return true;
}

bool NoneGarbageCollector::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    GarbageCollectorStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to load gc stats");
    this->stats_.processed_blocks_ = data.processed_block_count();
    return true;
}

string NoneGarbageCollector::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"processed blocks\": " << this->stats_.processed_blocks_ << "," << std::endl;
    sstr << "}";
    return sstr.str();
}

string NoneGarbageCollector::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"direct replay time\": " << this->stats_.direct_log_replay_time_.GetSum() << "," << std::endl;
    sstr << "\"dirty start replay time\": " << this->stats_.dirty_start_log_replay_time_.GetSum() << "," << std::endl;
    sstr << "\"log replay time\": " << this->stats_.log_replay_time_.GetSum()  << std::endl;

    sstr << "}";
    return sstr.str();
}

string NoneGarbageCollector::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "}";
    return sstr.str();
}

string NoneGarbageCollector::PrintLockStatistics() {
    return "null";
}

#ifdef DEDUPV1_CORE_TEST
void NoneGarbageCollector::ClearData() {
    Stop(dedupv1::StopContext::WritebackStopContext());
}
#endif

dedupv1::base::PersistentIndex* NoneGarbageCollector::candidate_info() {
    return NULL;
}

NoneGarbageCollector::Statistics::Statistics() {
    processed_blocks_ = 0;
}

}
}
