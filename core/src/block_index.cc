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
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <algorithm>
#include <iostream>
#include <iomanip>
#include <list>
#include <sstream>
#include <limits>

#include <dedupv1.pb.h>

#include <core/dedup.h>
#include <base/bitutil.h>
#include <base/index.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/locks.h>
#include <base/timer.h>
#include <base/thread.h>
#include <base/runnable.h>
#include <core/log_consumer.h>
#include <core/block_mapping_pair.h>
#include <core/container_storage.h>
#include <core/dedup_system.h>
#include <core/chunk_store.h>
#include <core/volatile_block_store.h>
#include <core/dedup_system.h>
#include <core/chunk_store.h>
#include <core/container_storage.h>
#include <base/crc32.h>
#include <base/fileutil.h>
#include <base/memory.h>
#include <base/fault_injection.h>

#include <core/log.h>
#include <core/session.h>
#include <core/block_index.h>

#include "dedupv1_stats.pb.h"

using std::string;
using std::stringstream;
using std::make_pair;
using std::vector;
using std::list;
using std::set;
using std::pair;
using std::map;
using std::tr1::tuple;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::strutil::Join;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::ToString;
using dedupv1::base::ProfileTimer;
using dedupv1::base::File;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::lookup_result;
using dedupv1::base::DELETE_ERROR;
using dedupv1::base::DELETE_NOT_FOUND;
using dedupv1::base::DELETE_OK;
using dedupv1::base::delete_result;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::PUT_KEEP;
using dedupv1::base::PUT_OK;
using dedupv1::base::put_result;
using dedupv1::base::Index;
using dedupv1::base::ScopedRunnable;
using dedupv1::base::NewRunnable;
using dedupv1::base::Thread;
using dedupv1::base::ThreadUtil;
using dedupv1::base::Callback1;
using dedupv1::chunkstore::Storage;
using dedupv1::chunkstore::storage_commit_state;
using dedupv1::chunkstore::STORAGE_ADDRESS_NOT_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_ERROR;
using dedupv1::chunkstore::STORAGE_ADDRESS_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_WILL_NEVER_COMMITTED;
using dedupv1::base::ScopedArray;
using dedupv1::Session;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_DELETED;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN;
using dedupv1::base::Option;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN;
using dedupv1::log::EVENT_REPLAY_MODE_DIRECT;
using dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG;
using dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_DELETED;
using dedupv1::log::EVENT_TYPE_CONTAINER_COMMITED;
using dedupv1::log::EVENT_TYPE_CONTAINER_COMMIT_FAILED;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED;
using dedupv1::base::ErrorContext;
using dedupv1::base::make_option;
using dedupv1::base::ThreadUtil;
using google::protobuf::Message;
using dedupv1::base::make_bytestring;
using std::tr1::make_tuple;

LOGGER("BlockIndex");

namespace dedupv1 {
namespace blockindex {

BlockIndex::BlockIndex()
    : bg_committer_(this, false) {
    this->block_index_ = NULL;
    this->auxiliary_block_index_ = NULL;
    this->log_ = NULL;
    this->block_size_ = 0;
    this->storage_ = NULL;
    this->info_store_ = NULL;
    this->max_auxiliary_block_index_size_ = 0;
    this->auxiliary_block_index_hard_limit_ = 0;
    this->block_locks_ = NULL;
    this->open_new_block_count_ = 0;
    idle_detector_ = NULL;
    import_if_idle_ = true;
    import_if_replaying_ = true;
    is_replaying_ = false;
    this->state_ = CREATED;
    log_replay_import_delay_ = 0;
    full_log_replay_import_delay_ = 0;
    hard_limit_import_delay_ = 0;
    soft_limit_import_delay_ = 100;
    default_import_delay_ = 1000;
    idle_import_delay_ = 0;
    minimal_replay_import_size_ = 16 * 1024;
    is_full_log_replay_ = false;
    failed_block_write_index_ = NULL;
#ifndef DEDUPV1_CORE_TEST
    stop_thread_count_ = 8;
#else
    stop_thread_count_ = 1;
#endif
    import_thread_count_ = 2;
    import_batch_size_ = kDefaultImportBatchSize;
}

BlockIndex::Statistics::Statistics() : import_latency_(1024) {
    this->index_reads_ = 0;
    this->index_writes_ = 0;
    this->index_real_writes_ = 0;
    this->imported_block_count_ = 0;
    this->ready_map_lock_free_ = 0;
    this->ready_map_lock_busy_ = 0;
    this->throttle_count_ = 0;
    this->failed_block_write_count_ = 0;
    incomplete_imports_ = 0;
}

bool BlockIndex::SetOption(const string& option_name, const string& option) {
    CHECK(this->state_ == CREATED, "Block index already started");
    CHECK(option.size() > 0, "Option not set");
    CHECK(option_name.size() > 0, "Option name not set");

    if (option_name == "auxiliary") {
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Auxiliary index creation failed");
        this->auxiliary_block_index_ = index->AsMemoryIndex();
        CHECK(this->auxiliary_block_index_, "Auxiliary index should not be persistent");
        CHECK(this->auxiliary_block_index_->HasCapability(dedupv1::base::COMPARE_AND_SWAP), "Index doesn't support compare-and-swap");
        return true;
    }
    if (StartsWith(option_name, "auxiliary.")) {
        CHECK(this->auxiliary_block_index_, "Auxiliary index not set");
        CHECK(this->auxiliary_block_index_->SetOption(option_name.substr(strlen("auxiliary.")),
                option), "Configuration failed");
        return true;
    }
    if (option_name == "max-auxiliary-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->max_auxiliary_block_index_size_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "auxiliary-size-hard-limit") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->auxiliary_block_index_hard_limit_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "idle-import") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->import_if_idle_ = To<bool>(option).value();
        return true;
    }
    if (option_name == "replaying-import") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->import_if_replaying_ = To<bool>(option).value();
        return true;
    }
    if (option_name == "persistent") {
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Persistent index creation failed");
        this->block_index_ = index->AsPersistentIndex();
        CHECK(this->block_index_, "Index is not persistent");
        CHECK(this->block_index_->HasCapability(dedupv1::base::PERSISTENT_ITEM_COUNT), "Index has no persistent item count");

        // Set default options
        this->block_index_->SetOption("max-key-size", "8");
        this->block_index_->SetOption("max-value-size", "2048");
        return true;
    }
    if (StartsWith(option_name, "persistent.")) {
        CHECK(this->block_index_, "Persistent data index not set");
        CHECK(this->block_index_->SetOption(option_name.substr(strlen("persistent.")),
                option), "Configuration failed");
        return true;
    }
    if (option_name == "persistent-failed-write") {
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Persistent index creation failed");
        this->failed_block_write_index_ = index->AsPersistentIndex();
        CHECK(this->failed_block_write_index_, "Index is not persistent");
        CHECK(this->failed_block_write_index_->HasCapability(dedupv1::base::PERSISTENT_ITEM_COUNT), "Index has no persistent item count");

        // Set default options
        this->failed_block_write_index_->SetOption("max-key-size", "12");
        this->failed_block_write_index_->SetOption("max-value-size", "16");
        return true;
    }
    if (StartsWith(option_name, "persistent-failed-write.")) {
        CHECK(this->failed_block_write_index_, "Persistent data index not set");
        CHECK(this->failed_block_write_index_->SetOption(option_name.substr(strlen("persistent-failed-write.")),
                option), "Configuration failed");
        return true;
    }
    if (option_name == "import-thread-count") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        CHECK(To<uint32_t>(option).value() > 0, "Illegal option " << option);
        import_thread_count_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "import-batch-size") {
        CHECK(To<int>(option).valid(), "Illegal option " << option);
        CHECK(To<int>(option).value() > 0, "Illegal option " << option);
        import_batch_size_ = To<int>(option).value();
        return true;
    }
    if (option_name == "log-replay-import-delay") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        log_replay_import_delay_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "full-log-replay-import-delay") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        full_log_replay_import_delay_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "hard-limit-import-delay") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        hard_limit_import_delay_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "soft-limit-import-delay") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        soft_limit_import_delay_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "idle-import-delay") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        idle_import_delay_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "default-import-delay") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        default_import_delay_ = To<uint32_t>(option).value();
        if (default_import_delay_ == 0) {
            WARNING("Setting default import delay to 0 results in busy waiting");
        }
        return true;
    }
    if (StartsWith(option_name, "throttle.")) {
        CHECK(this->throttling_.SetOption(option_name.substr(strlen("throttle.")), option),
            "Failed to configure log throttling");
        return true;
    }
    CHECK(this->block_index_, "Block index not set");
    CHECK(this->block_index_->SetOption(option_name, option), "Config failed");
    return true;
}

bool BlockIndex::Stop(dedupv1::StopContext stop_context) {

    if (state_ != STOPPED) {
        INFO("Stopping block index");
    }

    CHECK(this->bg_committer_.Stop(stop_context), "Failed to stop bg committer");
    CHECK(this->bg_committer_.Close(), "Failed to close bg committer");

    if ((state_ == STARTED || state_ == RUNNING) && stop_context.mode() == dedupv1::StopContext::WRITEBACK) {
        DCHECK(block_index_, "Persistent block index not set");
        DCHECK(auxiliary_block_index_, "Auxilairy block index not set");

        // Here all the work is done
        CHECK(this->ImportAllReadyBlocks(),
            "Failed to import all ready blocks");

        if (this->auxiliary_block_index_ && this->auxiliary_block_index_->GetItemCount() > 0) {
            WARNING("Still " << this->auxiliary_block_index_->GetItemCount() << " items in auxiliary block index");
        } else if (this->auxiliary_block_index_ && this->auxiliary_block_index_->GetItemCount() == 0) {
            // everything is imported
            this->ready_queue_.clear();
        }
        if (this->volatile_blocks_.GetContainerCount() > 0) {
            WARNING("Still " << this->volatile_blocks_.GetContainerCount() << " open containers in volatile block store");
        }
        if (this->volatile_blocks_.GetBlockCount() > 0) {
            WARNING("Still " << this->volatile_blocks_.GetBlockCount() << " open blocks in volatile block store");
        }
        if (this->open_new_block_count_ > 0) {
            WARNING("Still " << this->open_new_block_count_ << " open new blocks");
        }
    }
    this->state_ = STOPPED;
    return true;
}

bool BlockIndex::Close() {
    DEBUG("Closing block index");

    CHECK(this->Stop(dedupv1::StopContext::FastStopContext()), "Failed to stop block index");

    if (info_store_) {
        // if the block index is not started, there is nothing to dump
        if (!DumpMetaInfo()) {
            WARNING("Failed to dump meta info");
        }
    }

    if (this->block_index_) {
        CHECK(this->block_index_->Close(), "Error closing block index");
        this->block_index_ = NULL;
    }
    if (this->failed_block_write_index_) {
        CHECK(this->failed_block_write_index_->Close(), "Error closing failed write index");
        this->failed_block_write_index_ = NULL;
    }

    if (!this->volatile_blocks_.Clear()) {
        WARNING("Error clearing volatile block store");
    }

    Callback1<Option<bool>, uint64_t>* commit_state_callback =
        dirty_volatile_blocks_.set_commit_state_check_callback(NULL);
    if (commit_state_callback) {
        delete commit_state_callback;
    }
    if (this->auxiliary_block_index_) {
        if (!this->auxiliary_block_index_->Close()) {
            WARNING("Error closing auxiliary block index");
        }
        this->auxiliary_block_index_ = NULL;
    }

    if (this->log_) {
        if (this->log_->IsRegistered("block-index").value()) {
            if (!this->log_->UnregisterConsumer("block-index")) {
                WARNING("Failed to unregister block index from log");
            }
        }
        log_ = NULL;

    }
    return true;
}

bool BlockIndex::DumpMetaInfo() {
    DCHECK(info_store_, "Info store not set");

    BlockIndexLogfileData logfile_data;
    CHECK(this->volatile_blocks_.GetContainerTracker()->SerializeTo(logfile_data.mutable_container_tracker()),
        "Failed to serialize container tracker");

    CHECK(info_store_->PersistInfo("block-index", logfile_data), "Failed to persist info: " << logfile_data.ShortDebugString());
    return true;
}

#ifdef DEDUPV1_CORE_TEST
void BlockIndex::ClearData() {
    this->bg_committer_.Stop(StopContext::WritebackStopContext());
    if (this->block_index_) {
        this->block_index_->Close();
        block_index_ = NULL;
    }
    if (this->failed_block_write_index_) {
        this->failed_block_write_index_->Close();
        this->failed_block_write_index_ = NULL;
    }

    if (this->auxiliary_block_index_) {
        this->auxiliary_block_index_->Close();
        auxiliary_block_index_ = NULL;
    }
    if (this->log_) {
        log_->UnregisterConsumer("block-index");
        log_ = NULL;
    }
    volatile_blocks_.ClearData();
    this->ready_queue_.clear();
    this->open_new_block_count_ = 0;
}
#endif

lookup_result BlockIndex::ReadMetaInfo() {
    DCHECK_RETURN(info_store_, LOOKUP_ERROR, "Info store not set");

    BlockIndexLogfileData logfile_data;
    lookup_result lr = info_store_->RestoreInfo("block-index", &logfile_data);
    CHECK_RETURN(lr != LOOKUP_ERROR, LOOKUP_ERROR, "Failed to restore block index info");
    if (lr == LOOKUP_NOT_FOUND) {
        return lr;
    }
    if (logfile_data.has_container_tracker()) {
        CHECK_RETURN(this->volatile_blocks_.GetContainerTracker()->ParseFrom(logfile_data.container_tracker()),
            LOOKUP_ERROR, "Failed to parse container tracker: " << logfile_data.ShortDebugString());
    }
    return LOOKUP_FOUND;
}

bool BlockIndex::Start(const StartContext& start_context, DedupSystem* system) {
    CHECK(this->state_ == CREATED, "Block index already started");
    CHECK(this->block_index_, "Persistent block index not set");
    CHECK(this->failed_block_write_index_, "Persistent failed write index not set");
    CHECK(this->auxiliary_block_index_, "Auxiliary block index not set");
    CHECK(system, "System not set");

    INFO("Starting block index");

    this->block_size_ = system->block_size();

    this->block_locks_ = system->block_locks();
    CHECK(this->block_locks_, "Block locks not set");

    this->log_ = system->log();
    CHECK(this->log_, "Log not set");

    this->chunk_in_combats_ = &(system->chunk_index()->in_combats());
    CHECK(chunk_in_combats_, "Chunk in-combat data structure not set");

    this->info_store_ = system->info_store();
    CHECK(this->info_store_, "Info store not set");

    this->idle_detector_ = system->idle_detector();
    if (import_if_idle_) {
        CHECK(idle_detector_, "Idle detector not set");
    }

    // if not configured, use default
    if (auxiliary_block_index_hard_limit_ == 0) {
        auxiliary_block_index_hard_limit_ = this->max_auxiliary_block_index_size_ * kDefaultHardLimitFactor;
        INFO("Auto configuring: auxiliary block index hard limit: " << dedupv1::base::strutil::FormatStorageUnit(auxiliary_block_index_hard_limit_));
    }
    if (auxiliary_block_index_hard_limit_ < KMinimalHardLimit) {
        auxiliary_block_index_hard_limit_ = KMinimalHardLimit;
    }
    if (max_auxiliary_block_index_size_ >= auxiliary_block_index_hard_limit_ * throttling_.hard_limit_factor()) {
        WARNING("System is fully throttled before soft limit auxiliary block index import starts.");
    }

    CHECK(this->block_index_->Start(start_context), "Index start failed");
    CHECK(this->failed_block_write_index_->Start(start_context), "Index start failed");
    CHECK(this->auxiliary_block_index_->Start(start_context), "Could not start auxiliary index");

    this->storage_ = system->storage();
    CHECK(this->storage_, "Storage not set");

    lookup_result info_lookup = ReadMetaInfo();
    CHECK(info_lookup != LOOKUP_ERROR, "Failed to read meta info");
    CHECK(!(info_lookup == LOOKUP_NOT_FOUND && !start_context.create()), "Failed to lookup meta info in non-create startup mode");
    if (info_lookup == LOOKUP_NOT_FOUND && start_context.create()) {
        CHECK(DumpMetaInfo(), "Failed to dump info");
    }

    dirty_volatile_blocks_.set_commit_state_check_callback(NewCallback(this, &BlockIndex::CheckContainerAddress));

    CHECK(this->bg_committer_.Start(import_thread_count_),
        "Failed to start bg committer");
    CHECK(log_->RegisterConsumer("block-index", this), "Cannot register block index");

    this->state_ = STARTED;
    return true;
}

bool BlockIndex::Run() {
    CHECK(this->state_ == STARTED, "Illegal state: " << state_);
    CHECK(this->bg_committer_.Run(), "Failed to run bg committer");

    // do not need the dirty volatile store anymore
    Callback1<Option<bool>, uint64_t>* commit_state_callback =
        dirty_volatile_blocks_.set_commit_state_check_callback(NULL);
    if (commit_state_callback) {
        delete commit_state_callback;
    }

    DEBUG("Run block index");

    this->state_ = RUNNING;
    return true;
}

BlockIndex::read_result BlockIndex::ReadBlockInfo(const Session* session, BlockMapping* block_mapping,
                                                  dedupv1::base::ErrorContext* ec) {
    Session::block_mapping_open_state is_open = Session::BLOCK_MAPPING_IS_NOT_OPEN;
    ProfileTimer timer(this->stats_.read_time_);

    DCHECK_RETURN(block_mapping, READ_RESULT_ERROR, "Block mapping not set");
    DCHECK_RETURN(block_mapping->block_id() != BlockMapping::ILLEGAL_BLOCK_ID, READ_RESULT_ERROR, "Block id not set");
    DCHECK_RETURN(this->block_index_, READ_RESULT_ERROR, "Block index not set");

    // "open" means that the block_id was recently written by the same session, but the chunk is not finished and therefore some parts of
    // the block are not yet stored to disk
    if (session) {
        is_open = session->AppendIfOpen(block_mapping);
        CHECK_RETURN(is_open != Session::BLOCK_MAPPING_OPEN_ERROR, READ_RESULT_ERROR,
            "Failed to append open block mapping " << block_mapping->DebugString());
        if (is_open == Session::BLOCK_MAPPING_IS_OPEN) {
            TRACE("Read mapping: " << block_mapping->DebugString() << ", source session");
            return READ_RESULT_SESSION;
        }
    }
    // Read block mapping normally from the storage
    BlockIndex::read_result read_result = this->ReadBlockInfoFromIndex(block_mapping);
    DCHECK_RETURN(read_result != READ_RESULT_ERROR, READ_RESULT_ERROR, "Block reading failed: " << block_mapping->DebugString());

    TRACE("Read mapping: " << block_mapping->DebugString() << ", source " <<
        (read_result == READ_RESULT_AUX ? "auxiliary" : (read_result == READ_RESULT_MAIN ? "persistent" : "not found")));
    return read_result;
}

BlockIndex::read_result BlockIndex::ReadBlockInfoFromIndex(BlockMapping* block_mappings) {
    CHECK_RETURN(block_mappings, READ_RESULT_ERROR, "Block Mapping not set");
    CHECK_RETURN(auxiliary_block_index_, READ_RESULT_ERROR, "Auxiliary index not set");
    CHECK_RETURN(block_index_, READ_RESULT_ERROR, "Persistent index not set");

    uint64_t block_id = block_mappings->block_id();
    CHECK_RETURN(block_id != BlockMapping::ILLEGAL_BLOCK_ID, READ_RESULT_ERROR, "Block ID not set");

    this->stats_.index_reads_++;

    BlockMappingData block_mapping_data;
    enum lookup_result lookup_result = this->auxiliary_block_index_->Lookup(&block_id,  sizeof(uint64_t), &block_mapping_data);
    CHECK_RETURN(lookup_result != LOOKUP_ERROR, READ_RESULT_ERROR, "Block index lookup failed");
    if (lookup_result == LOOKUP_FOUND) {
        CHECK_RETURN(block_mappings->UnserializeFrom(block_mapping_data, true), READ_RESULT_ERROR,
            "Cannot unserialize block mapping " << block_mappings->block_id());
        return READ_RESULT_AUX;
    }

    // Not found in aux index
    lookup_result = this->block_index_->Lookup(&block_id, sizeof(uint64_t), &block_mapping_data);
    CHECK_RETURN(lookup_result != LOOKUP_ERROR, READ_RESULT_ERROR, "Block index lookup failed: " << block_mappings->DebugString());
    if (lookup_result == LOOKUP_FOUND) {
        CHECK_RETURN(block_mappings->UnserializeFrom(block_mapping_data, true), READ_RESULT_ERROR,
            "Cannot unserialize block mapping " << block_mappings->block_id());
        return READ_RESULT_MAIN;
    }

    // Not find in aux nor main index
    CHECK_RETURN(block_mappings->FillEmptyBlockMapping(), READ_RESULT_ERROR, "Failed to fill empty block mapping");
    return READ_RESULT_NOT_FOUND;
}

lookup_result BlockIndex::ReadBlockInfoFromIndex(Index* index, BlockMapping* block_mapping) {
    DCHECK_RETURN(block_mapping, LOOKUP_ERROR, "Block Mapping not set");
    DCHECK_RETURN(index, LOOKUP_ERROR, "Index not set");

    uint64_t block_id = block_mapping->block_id();
    DCHECK_RETURN(block_id != BlockMapping::ILLEGAL_BLOCK_ID, LOOKUP_ERROR,
        "Block ID not set");

    BlockMappingData block_mapping_data;
    enum lookup_result lookup_result = index->Lookup(&block_id, sizeof(uint64_t), &block_mapping_data);
    CHECK_RETURN(lookup_result != LOOKUP_ERROR, LOOKUP_ERROR,
        "Block index lookup failed: block mapping " << block_mapping->DebugString());
    if (lookup_result == LOOKUP_FOUND) {
        CHECK_RETURN(block_mapping->UnserializeFrom(block_mapping_data, true), LOOKUP_ERROR,
            "Cannot unserialize block mapping " << block_mapping->block_id() <<
            ", block mapping data: " << block_mapping_data.ShortDebugString());
    }
    return lookup_result;
}

bool BlockIndex::WriteDeleteLogEntry(const BlockMapping& previous_block_mapping,
                                     int64_t* event_log_id,
                                     dedupv1::base::ErrorContext* ec) {
    TRACE("Write log entry for deleted block mapping: " << previous_block_mapping.DebugString());
    ProfileTimer log_timer(this->stats_.log_time_);

    FAULT_POINT("block-index.write-deleted-log.pre");

    BlockMappingDeletedEventData event_data;
    CHECK(previous_block_mapping.SerializeTo(event_data.mutable_original_block_mapping(), true, false), "Cannot serialize data");
    CHECK(log_->CommitEvent(EVENT_TYPE_BLOCK_MAPPING_DELETED, &event_data, event_log_id, NULL, ec),
        "Cannot commit log entry: " <<
        "original block mapping " << previous_block_mapping.DebugString());

    FAULT_POINT("block-index.write-deleted-log.post");
    return true;
}

bool BlockIndex::WriteLogEntry(const BlockMapping& previous_block_mapping,
                               const BlockMapping& updated_block_mapping,
                               int64_t* event_log_id,
                               dedupv1::base::ErrorContext* ec) {
    TRACE("Write log entry for block mapping: " << updated_block_mapping.DebugString());
    ProfileTimer log_timer(this->stats_.log_time_);

    FAULT_POINT("block-index.write-log.pre");
    BlockMappingPair mapping_pair(block_size_);
    CHECK(mapping_pair.CopyFrom(previous_block_mapping, updated_block_mapping), "Failed to create mapping pair: " <<
        previous_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString());

    BlockMappingWrittenEventData event_data;
    CHECK(mapping_pair.SerializeTo(event_data.mutable_mapping_pair()), "Cannot serialize mapping pair: " <<
        mapping_pair.DebugString());

    CHECK(log_->CommitEvent(EVENT_TYPE_BLOCK_MAPPING_WRITTEN, &event_data, event_log_id, NULL, ec),
        "Cannot commit log entry: " <<
        "mapping pair " << mapping_pair.DebugString() <<
        ", original block mapping " << previous_block_mapping.DebugString() <<
        ", modified block mapping " << updated_block_mapping.DebugString());

    FAULT_POINT("block-index.write-log.post");
    return true;
}

bool BlockIndex::MarkBlockWriteAsFailed(const BlockMapping& previous_block_mapping,
                                        const BlockMapping& modified_mapping, Option<int64_t> write_event_log_id,
                                        dedupv1::base::ErrorContext* ec) {

    DEBUG("Mark block write as failed: " <<
        previous_block_mapping.DebugString() << " >> " << modified_mapping.DebugString() <<
        ", write event log id " << (write_event_log_id.valid() ? ToString(write_event_log_id.value()) : "<not set>"));

    BlockMappingPair mapping_pair(block_size_);
    CHECK(mapping_pair.CopyFrom(previous_block_mapping, modified_mapping), "Failed to create mapping pair: " <<
        previous_block_mapping.DebugString() << " => " << modified_mapping.DebugString());
    bool b = MarkBlockWriteAsFailed(mapping_pair, write_event_log_id, ec);
    if (!b) {
        return b;
    }

    BlockMapping aux_mapping(modified_mapping.block_id(), this->block_size_);
    lookup_result aux_lr = this->ReadBlockInfoFromIndex(this->auxiliary_block_index_, &aux_mapping);
    CHECK(aux_lr != LOOKUP_ERROR, "Cannot read block info");

    BlockMapping pers_mapping(modified_mapping.block_id(), this->block_size_);
    lookup_result pers_lr = this->ReadBlockInfoFromIndex(this->block_index_, &pers_mapping);
    CHECK(pers_lr != LOOKUP_ERROR, "Cannot read block info")

    bool need_update = false;
    BlockMapping import_mapping(modified_mapping.block_id(), this->block_size_);
    import_mapping.CopyFrom(previous_block_mapping);
    if (aux_lr == LOOKUP_FOUND && aux_mapping.version() == modified_mapping.version()) {
        need_update = true;
    } else if (aux_lr == LOOKUP_NOT_FOUND && pers_lr == LOOKUP_FOUND && pers_mapping.version() == modified_mapping.version()) {
        need_update = true;
    } else if (aux_lr == LOOKUP_NOT_FOUND && pers_lr == LOOKUP_NOT_FOUND) {
        need_update = true;
    }

    if (need_update) {
        // aka this version is newer then what we had stored before
        // Note here that the aux_mapping/pers_mapping version is 0 if they are not found

        this->ready_queue_.push(std::tr1::make_tuple(modified_mapping.block_id(), modified_mapping.version()));
        import_mapping.set_version(modified_mapping.version()); // we update the version in any case, but we use
        // the old mapping data.
        import_mapping.set_event_log_id(write_event_log_id.value());

        DEBUG("Update auxiliary index with failed version: " << import_mapping.DebugString());

        uint64_t block_id = modified_mapping.block_id();
        BlockMappingData mapping_value;
        CHECK(import_mapping.SerializeTo(&mapping_value, false, true),
            "Failed to serialize block mapping: " << import_mapping.DebugString());
        enum put_result result = this->auxiliary_block_index_->Put(&block_id, sizeof(uint64_t),
            mapping_value);
        CHECK(result != PUT_ERROR, "Update of auxiliary block index failed: " <<
            import_mapping.DebugString());
    }

    return b;
}

namespace {
void BuildBlockWriteFailedKey(byte* key, uint64_t block_id, uint32_t version) {
    memcpy(key, &block_id, sizeof(block_id));
    memcpy(key + 8, &version, sizeof(version));
}
}

bool BlockIndex::MarkBlockWriteAsFailed(const BlockMappingPair& mapping_pair, Option<int64_t> write_event_log_id,
                                        dedupv1::base::ErrorContext* ec) {
    DEBUG("Mark block write as failed: " << mapping_pair.DebugString() <<
        ", write event log id " << (write_event_log_id.valid() ? ToString(write_event_log_id.value()) : "<not set>"));

    ProfileTimer log_timer(this->stats_.log_time_);

    // the block event has been written with wrong data. We have to make sure that no data
    // gc'ed based on the bad block write event
    for (list<BlockMappingPairItem>::const_iterator i = mapping_pair.items().begin();
         i != mapping_pair.items().end();
         i++) {
        CHECK(chunk_in_combats_->Touch(i->fingerprint(), i->fingerprint_size()),
            "Failed to touch in-combat chunks: " << i->DebugString());
    }

    BlockMappingWriteFailedEventData event_data;
    CHECK(mapping_pair.SerializeTo(event_data.mutable_mapping_pair()), "Cannot serialize mapping pair: " <<
        mapping_pair.DebugString());
    if (write_event_log_id.valid()) {
        event_data.set_write_event_log_id(write_event_log_id.value());
    }

    CHECK(log_->CommitEvent(EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED, &event_data, NULL, NULL, ec),
        "Cannot commit log entry: " <<
        ", mapping pair " << mapping_pair.DebugString() <<
        ", event data " << event_data.ShortDebugString());

    // Add the block/version pair to the persistent failed write info
    byte key[12];
    BuildBlockWriteFailedKey(key, mapping_pair.block_id(), mapping_pair.version());
    BlockWriteFailedData value;

    put_result pr = failed_block_write_index_->Put(key, 12, value);
    CHECK(pr != PUT_ERROR, "Failed to update failed block write index");

    this->stats_.failed_block_write_count_++;
    return true;
}

Option<bool> BlockIndex::CheckContainerAddress(uint64_t container_id) {
    DCHECK(storage_, "Storage not set");

    enum storage_commit_state commit_check_result = storage_->IsCommitted(container_id);
    CHECK(commit_check_result != STORAGE_ADDRESS_ERROR, "Failed to check commit state: container id " << container_id);

    return make_option(commit_check_result == STORAGE_ADDRESS_COMMITED);
}

namespace {
bool BlockMappingItemStorageCheck(set<uint64_t>* checked_addresses, set<uint64_t>* uncommitted_address_set,
                                  Storage* storage, const BlockMappingItem& item) {
    uint64_t data_address = item.data_address();
    CHECK(Storage::IsValidAddress(data_address, true),
        "Data address of mapping not set: " << item.DebugString());

    // there is no need to check the same address multiple times
    if (checked_addresses->find(data_address) != checked_addresses->end()) {
        return true;
    }
    enum storage_commit_state commit_check_result = storage->IsCommittedWait(data_address);
    CHECK(commit_check_result != STORAGE_ADDRESS_ERROR, "Cannot check commit state");
    if (commit_check_result == STORAGE_ADDRESS_NOT_COMMITED) {
        uncommitted_address_set->insert(data_address);
    } else if (commit_check_result == STORAGE_ADDRESS_WILL_NEVER_COMMITTED) {
        uncommitted_address_set->insert(data_address);
        TRACE("Block mapping references a never committing container: " << item.DebugString() << ", container " << data_address);
    } else if (commit_check_result == STORAGE_ADDRESS_COMMITED) {
        // normal case => skip
    } else {
        ERROR("Illegal commit check result: " << commit_check_result);
        return false;
    }
    checked_addresses->insert(data_address);
    return true;
}
}

bool BlockIndex::BlockMappingStorageCheck(const BlockMapping& modified_block_mapping, set<uint64_t>* address_set) {
    CHECK(address_set, "Address set not set");
    ProfileTimer timer(this->stats_.check_time_);

    set<uint64_t> checked_addresses;
    for (list<BlockMappingItem>::const_iterator i = modified_block_mapping.items().begin();
         i != modified_block_mapping.items().end();
         i++) {
        if (!BlockMappingItemStorageCheck(&checked_addresses, address_set, this->storage_, *i)) {
            return false;
        }
    }
    return true;
}

bool BlockIndex::CheckIfFullWith(const BlockMapping& previous_block_mapping, const BlockMapping& updated_block_mapping) {
    if (previous_block_mapping.version() == 0 && updated_block_mapping.version() == 1) {
        // here we have a new block
        if (this->block_index_->GetItemCount() + this->open_new_block_count_ + 1 > this->block_index_->GetEstimatedMaxItemCount()) {
            WARNING("Block index full: "
                << previous_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString() <<
                ", persistent item count " << this->block_index_->GetItemCount() <<
                ", open new block count " << this->open_new_block_count_ <<
                ", auxiliary item count " << this->auxiliary_block_index_->GetItemCount());
            return true;
        }
    }
    return false;
}

Option<bool> BlockIndex::Throttle(int thread_id, int thread_count) {
    ProfileTimer timer(this->stats_.throttle_time_);

    double item_count =  this->auxiliary_block_index_->GetItemCount();
    double fill_ratio = item_count / auxiliary_block_index_hard_limit_;
    double thread_ratio = 1.0 * thread_id / thread_count;
    Option<bool> r = throttling_.Throttle(fill_ratio, thread_ratio);
    if (r.valid() && r.value()) {
        this->stats_.throttle_count_++;
    }
    return r;
}

uint64_t BlockIndex::GetActiveBlockCount() {
    if (persistent_block_index()) {
        return persistent_block_index()->GetItemCount() + open_new_block_count_;
    }
    return open_new_block_count_;
}

bool BlockIndex::StoreBlock(const BlockMapping& previous_block_mapping,
                            const BlockMapping& updated_block_mapping,
                            ErrorContext* ec) {
    ProfileTimer timer(this->stats_.write_time_);

    DCHECK(previous_block_mapping.version() != updated_block_mapping.version(),
        "Illegal version: " << previous_block_mapping.DebugString() << " => " <<
        updated_block_mapping.DebugString());
    DCHECK(this->block_size_ == updated_block_mapping.block_size(), "Illegal block mapping (size mismatch)");
    DCHECK(previous_block_mapping.block_id() == updated_block_mapping.block_id(),
        "block id mismatch: " << previous_block_mapping.DebugString() << " => " <<
        updated_block_mapping.DebugString());
    DCHECK(updated_block_mapping.event_log_id() == 0,
        "Illegel event log id for updated block mapping: " <<
        previous_block_mapping.DebugString() << " => " <<
        updated_block_mapping.DebugString());

    uint64_t block_id = updated_block_mapping.block_id();
    DEBUG("Store block " << previous_block_mapping.DebugString() <<
        " => " << updated_block_mapping.DebugString());

    {
        ProfileTimer timer(this->stats_.open_new_block_check_time_);
        if (previous_block_mapping.version() == 0 && updated_block_mapping.version() == 1) {
            // here we have a new block
            if (this->block_index_->GetItemCount() + this->open_new_block_count_ + 1 > this->block_index_->GetEstimatedMaxItemCount()) {
                WARNING("Block index full: "
                    << previous_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString() <<
                    ", persistent item count " << this->block_index_->GetItemCount() <<
                    ", open new block count " << this->open_new_block_count_ <<
                    ", auxiliary item count " << this->auxiliary_block_index_->GetItemCount());
                if (ec) {
                    ec->set_full();
                }
                return false;
            }
            TRACE("New block: " << previous_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString() <<
                ", open new blocks " << (open_new_block_count_ + 1));
            this->open_new_block_count_++;
        }
    }

    // gather all containers
    set<uint64_t> container_id_set;
    for (list<BlockMappingItem>::const_iterator i = updated_block_mapping.items().begin();
         i != updated_block_mapping.items().end();
         i++) {
        container_id_set.insert(i->data_address());
    }

    BlockMappingData block_mapping_data;
    CHECK(updated_block_mapping.SerializeTo(&block_mapping_data, false, true), "Cannot serialize block mapping: " <<
        updated_block_mapping.DebugString());

    put_result result = this->auxiliary_block_index_->Put(&block_id, sizeof(uint64_t), block_mapping_data);
    CHECK(result != PUT_ERROR, "Cannot put block mapping: " << updated_block_mapping.DebugString());

    // We have to make sure that no data is gc'ed based on the this event before it is replayed
    // TODO (dmeister): Here we in-combat simply to much. It would be ok only to mark fp with a negative usage count modifier
    for (list<BlockMappingItem>::const_iterator i = previous_block_mapping.items().begin();
         i != previous_block_mapping.items().end();
         i++) {
        CHECK(chunk_in_combats_->Touch(i->fingerprint(), i->fingerprint_size()),
            "Failed to touch in-combat chunks: " << i->DebugString());
    }

    // here not all container might be committed
    int64_t event_log_id = 0;
    if (!this->WriteLogEntry(previous_block_mapping, updated_block_mapping, &event_log_id, ec)) {
        if (event_log_id) {
            WARNING("Failed to write block index log entry: " <<
                "mapping " << previous_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString() <<
                ", data is committed");
        } else {
            ERROR("Failed to write block index log entry: " <<
                "mapping " << previous_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString() <<
                ", data is not committed");
            return false;
        }
    }
    // we can call "AddBlock" also if there are not open containers
    if (!this->volatile_blocks_.AddBlock(previous_block_mapping,
            updated_block_mapping,
            NULL,
            container_id_set,
            event_log_id,
            this)) {
        WARNING("Cannot add block as uncommitted: " << previous_block_mapping.DebugString() << ", "
                                                    << updated_block_mapping.DebugString());
    }

    this->stats_.index_writes_++;
    if (result != PUT_KEEP) {
        this->stats_.index_real_writes_++;
    }
    return true;
}

delete_result BlockIndex::DeleteBlockInfo(uint64_t block_id,
                                          dedupv1::base::ErrorContext* ec) {
    DEBUG("Delete block info: block id " << block_id);

    Option<bool> is_volatile = this->volatile_blocks_.IsVolatileBlock(block_id);
    CHECK_RETURN(is_volatile.valid(),
        DELETE_ERROR, "Failed to check volatile state of block: block id " << block_id);
    // We avoid deleting block info for block ids that are currently
    // volatile as it is very, very hard to do it right.
    CHECK_RETURN(!is_volatile.value(), DELETE_ERROR,
        "Cannot delete block infos about a volatile block: block id " << block_id);

    BlockMapping block_mapping(block_id, this->block_size_);
    read_result r = this->ReadBlockInfoFromIndex(&block_mapping);
    CHECK_RETURN(r != READ_RESULT_ERROR, DELETE_ERROR,
        "Failed to read block mapping: block id " << block_id);

    if (r == READ_RESULT_NOT_FOUND) {
        // if the mapping has not been found, there is not need to delete anything or to log anything
        return DELETE_NOT_FOUND;
    }

    TRACE("Delete block " << block_mapping.DebugString());

    // We have to make sure that no data is gc'ed based on the this event before it is replayed
    for (list<BlockMappingItem>::const_iterator i = block_mapping.items().begin();
         i != block_mapping.items().end();
         i++) {
        CHECK_RETURN(chunk_in_combats_->Touch(i->fingerprint(), i->fingerprint_size()), DELETE_ERROR,
            "Failed to touch in-combat chunks: " << i->DebugString());
    }

    CHECK_RETURN(WriteDeleteLogEntry(block_mapping, NULL, ec), DELETE_ERROR,
        "Failed to write deleted log entry");

    delete_result dr = this->auxiliary_block_index_->Delete(&block_id, sizeof(block_id));
    CHECK_RETURN(dr != DELETE_ERROR,
        DELETE_ERROR, "Failed to delete block mapping from auxiliary index: block id " << block_id);
    CHECK_RETURN(this->block_index_->Delete(&block_id, sizeof(block_id)) != DELETE_ERROR,
        DELETE_ERROR, "Failed to delete block mapping from persistent index: block id " << block_id);

    if (block_mapping.version() == 1 && dr == DELETE_OK) {
        // if the block is new and if the block was still in the auxiliary index
        if (open_new_block_count_ > 0) {
            open_new_block_count_--;
        }
        TRACE("Reduce open block count: count " << open_new_block_count_ <<
            ", block mapping " << block_mapping.DebugString());
    }
    return DELETE_OK;
}

string BlockIndex::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"ready map free\": " << this->stats_.ready_map_lock_free_ << "," << std::endl;
    sstr << "\"ready map busy\": " << this->stats_.ready_map_lock_busy_ << "," << std::endl;
    sstr << "\"index\": " << (block_index_ ? this->block_index_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"auxiliary index\": " <<
    (this->auxiliary_block_index_ ? this->auxiliary_block_index_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"volatile block store\": " << this->volatile_blocks_.PrintLockStatistics() << std::endl;
    sstr << "}";
    return sstr.str();
}

string BlockIndex::PrintTrace() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{";
    sstr << "\"average import latency\": " << this->stats_.import_latency_.GetAverage() << "," << std::endl;
    sstr << "\"replaying state\": " << ToString(static_cast<bool>(is_replaying_)) << "," << std::endl;
    sstr << "\"throttle count\": " << this->stats_.throttle_count_ << "," << std::endl;
    sstr << "\"incomplete import count\": " << this->stats_.incomplete_imports_ << "," << std::endl;
    sstr << "\"open new block count\": " << this->open_new_block_count_ << "," << std::endl;
    sstr << "\"ready queue size\": " << this->ready_queue_.unsafe_size() << "," << std::endl;
    sstr << "\"auxiliary index hard limit\": " << this->auxiliary_block_index_hard_limit_ << "," << std::endl;
    sstr << "\"auxiliary index soft limit\": " << this->max_auxiliary_block_index_size_ << "," << std::endl;
    sstr << "\"auxiliary index item count\": " <<
    (auxiliary_block_index_ ? ToString(this->auxiliary_block_index_->GetItemCount()) : "null") <<
    ", " << std::endl;
    sstr << "\"auxiliary index size\": " <<
    (auxiliary_block_index_ ? ToString(this->auxiliary_block_index_->GetMemorySize()) : "null") <<
    ", " << std::endl;
    sstr << "\"persistent index size\": " << (block_index_ ?
                                              ToString(this->block_index_->GetPersistentSize()) : "null") << "," << std::endl;
    sstr << "\"volatile block store\": " << this->volatile_blocks_.PrintTrace() << std::endl;
    sstr << "}";
    return sstr.str();
}

bool BlockIndex::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    BlockIndexStatsData data;
    data.set_index_read_count(this->stats_.index_reads_);
    data.set_index_write_count(this->stats_.index_writes_);
    data.set_index_real_write_count(this->stats_.index_real_writes_);
    data.set_imported_block_count(this->stats_.imported_block_count_);
    data.set_failed_block_write_count(this->stats_.failed_block_write_count_);
    CHECK(ps->Persist(prefix, data), "Failed to persist block index stats");

    CHECK(this->volatile_blocks_.PersistStatistics(prefix + ".volatile", ps),
        "Failed to persist volatile block store");

    return true;
}

bool BlockIndex::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    BlockIndexStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore block index stats");
    this->stats_.index_reads_ = data.index_read_count();
    this->stats_.index_writes_ = data.index_write_count();
    this->stats_.index_real_writes_ = data.index_real_write_count();
    this->stats_.imported_block_count_ = data.imported_block_count();

    if (data.has_failed_block_write_count()) {
        this->stats_.failed_block_write_count_ = data.failed_block_write_count();
    }
    CHECK(this->volatile_blocks_.RestoreStatistics(prefix + ".volatile", ps),
        "Failed to restore volatile block store");

    return true;
}

string BlockIndex::PrintStatistics() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{";
    sstr << "\"index reads\": " << this->stats_.index_reads_ << "," << std::endl;
    sstr << "\"index writes\": " << this->stats_.index_writes_ << "," << std::endl;
    sstr << "\"index real writes\": " << this->stats_.index_real_writes_ << "," << std::endl;
    sstr << "\"imported block count\": " << this->stats_.imported_block_count_ << "," << std::endl;
    sstr << "\"failed block write count\": " << this->stats_.failed_block_write_count_ << "," << std::endl;

    sstr << "\"index item count\": " << (block_index_ ?
                                         ToString(this->block_index_->GetItemCount()) : "null") << "," << std::endl;

    if (auxiliary_block_index_hard_limit_ && auxiliary_block_index_) {
        double aux_fill_ratio = 1.0 * this->auxiliary_block_index_->GetItemCount() / this->auxiliary_block_index_hard_limit_;
        sstr << "\"auxiliary index fill ratio\": " << aux_fill_ratio << "," << std::endl;
    } else {
        sstr << "\"auxiliary index fill ratio\": null," << std::endl;
    }

    if (block_index_ && this->block_index_->GetEstimatedMaxItemCount()) {
        double main_fill_ratio = (1.0 * this->block_index_->GetItemCount() + this->open_new_block_count_) / this->block_index_->GetEstimatedMaxItemCount();
        sstr << "\"index fill ratio\": " << main_fill_ratio << "," << std::endl;
    } else {
        sstr << "\"index fill ratio\": null," << std::endl;
    }

    sstr << "\"volatile block store\": " << this->volatile_blocks_.PrintStatistics() << std::endl;
    sstr << "}";
    return sstr.str();
}

string BlockIndex::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"block index read\": " << this->stats_.read_time_.GetSum() << "," << std::endl;
    sstr << "\"block index write\": " << this->stats_.write_time_.GetSum() << "," << std::endl;
    sstr << "\"replay time\": " << this->stats_.replay_time_.GetSum() << "," << std::endl;
    sstr << "\"log time\": " << this->stats_.log_time_.GetSum() << "," << std::endl;
    sstr << "\"open new block check time\": " << this->stats_.open_new_block_check_time_.GetSum() << "," << std::endl;
    sstr << "\"check time\": " << this->stats_.check_time_.GetSum() << "," << std::endl;
    sstr << "\"throttle time\": " << this->stats_.throttle_time_.GetSum() << "," << std::endl;
    sstr << "\"index\": " << (block_index_ ? this->block_index_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"volatile block store\": " << this->volatile_blocks_.PrintProfile()  << "," << std::endl;
    sstr << "\"failed write index\": " << (failed_block_write_index_ ? this->failed_block_write_index_->PrintProfile() : "null") << ", " << std::endl;
    sstr << "\"auxiliary index\": " << (auxiliary_block_index_ ? this->auxiliary_block_index_->PrintProfile() : "null") << std::endl;
    sstr << "}";
    return sstr.str();
}

bool BlockIndex::ImportReadyBlockLoop() {
    list<tuple<uint64_t, uint32_t> > ready_infos;
    map<uint64_t, list<tuple<uint64_t, uint32_t> >::iterator> block_map;

    lookup_result lr = LOOKUP_FOUND;
    while (lr == LOOKUP_FOUND) {
        while (ready_infos.size() < this->import_batch_size_) {
            tuple<uint64_t, uint32_t> ready_info;
            if (!this->ready_queue_.try_pop(ready_info)) {
                lr = LOOKUP_NOT_FOUND;
                break;
            }

            uint64_t block_id = std::tr1::get<0>(ready_info);
            if (block_map.find(block_id) != block_map.end()) {
                list<tuple<uint64_t, uint32_t> >::iterator existing_iterator = block_map[block_id];
                uint32_t existing_version = std::tr1::get<1>(*existing_iterator);
                uint32_t new_version = std::tr1::get<1>(ready_info);

                if (existing_version < new_version) {
                    // there is an older than the current version in the map. We delete it
                    ready_infos.erase(existing_iterator);
                } else {
                    // the existing version is newer than the current. We skip this version
                    continue;
                }
            }

            ready_infos.push_back(ready_info);
            block_map[block_id] = --(ready_infos.end());
        }
        if (ready_infos.size() > 0) {
            list<tuple<uint64_t, uint32_t> > unimported_ready_infos;
            bool r = ImportModifiedBlockMappings(ready_infos, &unimported_ready_infos);
            if (!r) {
                ERROR("Failed to import modified block mappings");
                // If the import failed, we put every ready info tuple back
                unimported_ready_infos = ready_infos;
            }
            list<tuple<uint64_t, uint32_t> >::iterator i;
            for (i = unimported_ready_infos.begin(); i != unimported_ready_infos.end(); i++) {
                TRACE("Block was not imported: block id " << std::tr1::get<0>(*i) << ", version " << std::tr1::get<1>(*i));
                this->ready_queue_.push(*i);
            }
        }
    }
    return true;
}

bool BlockIndex::ImportAllReadyBlocks() {
    DEBUG("Import all ready blocks: ready queue size " << this->ready_queue_.unsafe_size());

    std::vector<Thread<bool>* > stop_threads;
    stop_threads.resize(stop_thread_count_);

    bool failed = false;
    for (int i = 0; i < stop_threads.size(); i++) {
        stop_threads[i] = new Thread<bool>(
            NewRunnable(this, &BlockIndex::ImportReadyBlockLoop), "bi stop " + ToString(i));
        if (!stop_threads[i]->Start()) {
            ERROR("Failed to start block index stop thread");
            failed = true;
        }
    }

    for (int i = 0; i < stop_threads.size(); i++) {
        if (stop_threads[i]) {
            bool result = false;
            if (!stop_threads[i]->Join(&result)) {
                ERROR("Failed to join stop thread");
                failed = true;
            }
            if (!result) {
                ERROR("Stop thread exited with error");
                failed = true;
            }
            delete stop_threads[i];
            stop_threads[i] = NULL;
        }
    }
    stop_threads.clear();

    return !failed;
}

enum lookup_result BlockIndex::ImportNextBlockBatch() {
    TRACE("Import next block: ready queue size " << this->ready_queue_.unsafe_size());

    FAULT_POINT("block-index.import-next-block.pre");

    list<tuple<uint64_t, uint32_t> > ready_infos;
    map<uint64_t, list<tuple<uint64_t, uint32_t> >::iterator> block_map;

    lookup_result lr = LOOKUP_FOUND;
    while (ready_infos.size() < this->import_batch_size_) {
        tuple<uint64_t, uint32_t> ready_info;
        if (!this->ready_queue_.try_pop(ready_info)) {
            lr = LOOKUP_NOT_FOUND;
            break;
        }

        uint64_t block_id = std::tr1::get<0>(ready_info);
        if (block_map.find(block_id) != block_map.end()) {
            list<tuple<uint64_t, uint32_t> >::iterator existing_iterator = block_map[block_id];
            uint32_t existing_version = std::tr1::get<1>(*existing_iterator);
            uint32_t new_version = std::tr1::get<1>(ready_info);

            if (existing_version < new_version) {
                // there is an older than the current version in the map. We delete it
                ready_infos.erase(existing_iterator);
            } else {
                // the existing version is newer than the current. We skip this version
                continue;
            }
        }

        ready_infos.push_back(ready_info);
        block_map[block_id] = --(ready_infos.end());
    }

    if (ready_infos.size() > 0) {
        list<tuple<uint64_t, uint32_t> > unimported_ready_infos;
        bool r = ImportModifiedBlockMappings(ready_infos, &unimported_ready_infos);
        if (!r) {
            ERROR("Failed to import modified block mappings");
            // If the import failed, we put every ready info tuple back
            unimported_ready_infos = ready_infos;
            lr = LOOKUP_ERROR;
        }
        list<tuple<uint64_t, uint32_t> >::iterator i;
        for (i = unimported_ready_infos.begin(); i != unimported_ready_infos.end(); i++) {
            TRACE("Block was not imported: block id " << std::tr1::get<0>(*i) << ", version " << std::tr1::get<1>(*i));
            this->ready_queue_.push(*i);
        }
    }
    return lr;
}

bool BlockIndex::ImportModifiedBlockMappings(const list<tuple<uint64_t, uint32_t> >& ready_infos,
                                             list<tuple<uint64_t, uint32_t> >* unimported_ready_infos) {
    DCHECK(unimported_ready_infos, "Unimported ready infos not set");

    list<uint64_t> block_list;
    list<tuple<uint64_t, uint32_t> >::const_iterator i;
    for (i = ready_infos.begin(); i != ready_infos.end(); i++) {
        uint64_t block_id = std::tr1::get<0>(*i);
        block_list.push_back(block_id);
    }

    list<tuple<uint64_t, uint32_t> > aux_delete_list;
    list<uint64_t> locked_block_list;
    list<uint64_t> unlocked_block_list;
    CHECK(block_locks_->TryWriteLocks(block_list, &locked_block_list, &unlocked_block_list, LOCK_LOCATION_INFO),
        "Failed to acquire locks for block list");

    if (locked_block_list.size() != block_list.size()) {
        stats_.incomplete_imports_++;
    }
    set<uint64_t> unlocked_block_set;
    unlocked_block_set.insert(unlocked_block_list.begin(), unlocked_block_list.end());
    bool failed = false;

    vector<tuple<bytestring, const Message*> > batch_put_data;

    for (i = ready_infos.begin(); i != ready_infos.end(); i++) {
        uint64_t block_id = std::tr1::get<0>(*i);
        uint32_t version = std::tr1::get<1>(*i);

        if (unlocked_block_set.find(block_id) != unlocked_block_set.end()) {
            // the block is not locked!
            TRACE("Block not locked: block id " << block_id);
            unimported_ready_infos->push_back(make_tuple(block_id, version));
            continue;
        }

        BlockMapping aux_block_mapping(block_id, this->block_size_);
        lookup_result aux_lookup = ReadBlockInfoFromIndex(this->auxiliary_block_index_, &aux_block_mapping);
        if (aux_lookup == LOOKUP_ERROR) {
            ERROR("Cannot read block info: " << aux_block_mapping.DebugString());
            failed = true;
            break;
        }
        if (aux_lookup == LOOKUP_NOT_FOUND) {
            // This might happen if the block mapping has been deleted before the
            // ready queue has been processed
            // We returns LOOKUP_FOUND instead of LOOKUP_NOT_FOUND has "NOT_FOUND" in
            // this context would mean that there are no more block index items to process.
            continue;
        }
        // if the version of the current auxiliary index is higher than
        // the version that we are allowed to import. The version might be allowed to be imported but there
        // is no way to check if that is the case.
        // we skip the entry
        if (aux_block_mapping.version() != version) {
            continue;
        }

        BlockMapping pers_block_mapping(block_id, this->block_size_);
        lookup_result pers_lookup = ReadBlockInfoFromIndex(this->block_index_, &pers_block_mapping);
        if (pers_lookup == LOOKUP_ERROR) {
            ERROR("Cannot read block info: " << pers_block_mapping.DebugString());
            failed = true;
            break;
        }

        // here we can have a version counter difference larger than 1 if a lots of the same block are written in a
        // short time.
        if (!(pers_lookup == LOOKUP_NOT_FOUND || pers_block_mapping.version() < aux_block_mapping.version())) {
            TRACE("Skip importing block mapping: " <<
                ", import mapping " << aux_block_mapping.DebugString() <<
                ", current persistent mapping " << (pers_lookup == LOOKUP_FOUND ? pers_block_mapping.DebugString() : "<not set>"));
            continue;
        }

        DEBUG("Prepare for batch import: " <<
            "block mapping " << aux_block_mapping.DebugString() <<
            ", ready queue version " << version <<
            ", current persistent mapping " << (pers_lookup == LOOKUP_FOUND ? pers_block_mapping.DebugString() : "<not set>"));
        BlockMappingData* value = new BlockMappingData();
        if (!aux_block_mapping.SerializeTo(value, false, true)) {
            ERROR("Cannot serialize data: " << aux_block_mapping.DebugString());
            failed = true;
            break;
        }

        bytestring key = make_bytestring(reinterpret_cast<const byte*>(&block_id), sizeof(uint64_t));
        batch_put_data.push_back(make_tuple(key, value));
        aux_delete_list.push_back(make_tuple(block_id, version));

        if (pers_lookup == LOOKUP_NOT_FOUND) {
            // here we move a new block from the auxiliary index to the persistent index
            if (this->open_new_block_count_ > 0) {
                TRACE("Persist new block: " << aux_block_mapping.DebugString() <<
                    ", open new block count " << (this->open_new_block_count_ - 1));
                this->open_new_block_count_--;
            } else {
                WARNING("Illegal open new block count: " << this->open_new_block_count_ << ", modifier -1" <<
                    ", auxiliary block mapping " << aux_block_mapping.DebugString() <<
                    ", ready queue version " << version <<
                    ", current persistent mapping " << (pers_lookup == LOOKUP_FOUND ? pers_block_mapping.DebugString() : "<not set>"));
            }
        }

        this->stats_.index_writes_++;
        this->stats_.index_real_writes_++;
        this->stats_.imported_block_count_.fetch_and_increment();
    } // end loop over all elements

    if (!failed) {
        put_result r = this->block_index_->PutBatch(batch_put_data);
        if (r == PUT_ERROR) {
            ERROR("Failed to write import data to block index");
            failed = true;
        }

        if (!failed) {
            vector<tuple<bytestring, const Message*> >::iterator j;
            for (j = batch_put_data.begin(); j != batch_put_data.end(); j++) {
                bytestring& key(std::tr1::get<0>(*j));

                if (this->auxiliary_block_index_->Delete(key.data(), key.size()) == DELETE_ERROR) {
                    ERROR("Cannot delete committed block from aux index");
                    failed = true;
                }
            }
        }
    }

    vector<tuple<bytestring, const Message*> >::iterator j;
    for (j = batch_put_data.begin(); j != batch_put_data.end(); j++) {
        const Message* message = std::tr1::get<1>(*j);
        delete message;
    }

    CHECK(block_locks_->WriteUnlocks(locked_block_list, LOCK_LOCATION_INFO), "Failed to release locks for block list");
    return !failed;
}

bool BlockIndex::FailVolatileBlock(const BlockMapping& original_mapping,
                                   const BlockMapping& modified_mapping,
                                   const google::protobuf::Message* extra_message,
                                   int64_t block_mapping_written_event_log_id
                                   ) {
    if (state_ == STARTED) {
        return true; // dirty
    }
    uint64_t block_id = original_mapping.block_id();

    DEBUG("Fail volatile block mapping " << original_mapping.DebugString() << " => " << modified_mapping.DebugString());

    BlockMapping aux_mapping(block_id, this->block_size_);
    lookup_result aux_lr = this->ReadBlockInfoFromIndex(this->auxiliary_block_index_, &aux_mapping);
    CHECK(aux_lr != LOOKUP_ERROR, "Cannot read block info: " << aux_mapping.DebugString());
    if (aux_lr == LOOKUP_NOT_FOUND) {
        // There is not wrong state that we need to correct
        // This does e.g. happen when this is a block change from V1 to V2 where V1 is still volatile. When
        // V1 fails, the entry is deleted because an deleted entry is equal to an stored entry with V0.
        return true;
    }
    // aux_lr == LOOKUP_FOUND

    // if the block in the auxiliary index is newer, we are fine
    if (aux_mapping.version() == modified_mapping.version()) {
        BlockMappingData block_mapping_data;
        CHECK(original_mapping.SerializeTo(&block_mapping_data, false, true),
            "Cannot serialize block mapping: " << original_mapping.DebugString());

        TRACE("Revert block mapping " << original_mapping.block_id() << " in auxiliary index:\n" <<
            original_mapping.DebugString());

        if (original_mapping.version() > 0) {
            put_result result = this->auxiliary_block_index_->Put(&block_id, sizeof(uint64_t), block_mapping_data);
            CHECK(result != PUT_ERROR, "Cannot put block mapping: " << original_mapping.DebugString());
            result = this->block_index_->Put(&block_id, sizeof(uint64_t), block_mapping_data);
            CHECK(result != PUT_ERROR, "Cannot put block mapping: " << original_mapping.DebugString());
        } else {
            // when the original version is zero, then we delete the entry instead of overwritting it
            delete_result aux_result = this->auxiliary_block_index_->Delete(&block_id, sizeof(uint64_t));
            CHECK(aux_result != DELETE_ERROR, "Failed to delete block mapping: " << block_id);
            delete_result pers_result = this->block_index_->Delete(&block_id, sizeof(uint64_t));
            CHECK(pers_result != DELETE_ERROR, "Failed to delete block mapping: " << block_id);

            if (aux_result == DELETE_OK) {
                open_new_block_count_--;
                TRACE("Reduce open block count: count " << open_new_block_count_);
            }
        }
    } else {
        TRACE("Skip processing failed block: " <<
            original_mapping.DebugString() << " => " << modified_mapping.DebugString() <<
            ", auxiliary " << aux_mapping.DebugString());
    }
    return true;
}

bool BlockIndex::ProcessCommittedBlockWrite(const BlockMapping& original_mapping,
                                            const BlockMapping& modified_mapping,
                                            int64_t block_mapping_written_event_log_id) {
    // block lock is held
    // the method is called in the volatile block store callback

    uint64_t block_id = modified_mapping.block_id();
    BlockMapping aux_mapping(block_id, this->block_size_);
    lookup_result aux_lr = this->ReadBlockInfoFromIndex(this->auxiliary_block_index_, &aux_mapping);
    CHECK(aux_lr != LOOKUP_ERROR, "Cannot read block info: " << original_mapping.DebugString() << " => " << modified_mapping.DebugString());

    if (aux_lr == LOOKUP_NOT_FOUND) {
        // This happens in strange situations where
        // a block 0 is replayed with an lower version no
        // and the most current version is completely committed, but this version was not committable
        // up to now. However, because the higher version is committable, there is not need to process this
        // block
        TRACE("Skipping ready queue: " << original_mapping.DebugString() << " => " << aux_mapping.DebugString() <<
            ", event log id " << block_mapping_written_event_log_id <<
            ", aux mapping not found");
    } else if (aux_mapping.version() == modified_mapping.version()) {
        BlockMapping modified_aux_mapping(block_id, this->block_size_);
        modified_aux_mapping.CopyFrom(aux_mapping);
        modified_aux_mapping.set_event_log_id(block_mapping_written_event_log_id);

        BlockMappingData aux_block_mapping_data;
        CHECK(aux_mapping.SerializeTo(&aux_block_mapping_data, false, true),
            "Cannot serialize block mapping: " << aux_mapping.DebugString());
        BlockMappingData modified_aux_block_mapping_data;
        CHECK(modified_aux_mapping.SerializeTo(&modified_aux_block_mapping_data, false, true),
            "Cannot serialize block mapping: " << modified_aux_mapping.DebugString());

        BlockMappingData result_block_mapping_data;
        put_result result = this->auxiliary_block_index_->CompareAndSwap(&block_id, sizeof(block_id),
            modified_aux_block_mapping_data, aux_block_mapping_data, &result_block_mapping_data);
        CHECK(result != PUT_ERROR, "Failed to update auxiliary block mapping after commit: " <<
            aux_mapping.DebugString());
        if (result == PUT_OK) {
            TRACE("Add to ready queue: " << original_mapping.DebugString() << " => " << aux_mapping.DebugString() <<
                ", event log id " << block_mapping_written_event_log_id);

            this->ready_queue_.push(std::tr1::make_tuple(
                    aux_mapping.block_id(),
                    aux_mapping.version()));
        } else {
            INFO("Skipping ready queue: " <<
                "auxiliary mapping already changed: expected: " << aux_block_mapping_data.ShortDebugString() <<
                ", target " << modified_aux_block_mapping_data.ShortDebugString() <<
                ", was " << result_block_mapping_data.ShortDebugString());
        }
    } else {
        TRACE("Skipping ready queue: " << original_mapping.DebugString() << " => " << aux_mapping.DebugString() <<
            ", event log id " << block_mapping_written_event_log_id <<
            ", aux mapping " << aux_mapping.DebugString());
    }
    return true;
}

bool BlockIndex::CommitVolatileBlock(const BlockMapping& original_mapping,
                                     const BlockMapping& modified_mapping,
                                     const google::protobuf::Message* extra_message,
                                     int64_t block_mapping_written_event_log_id,
                                     bool direct) {
    bool failed = false;
    if (state_ == STARTED) {
        // dirty
        CHECK(extra_message, "Extra message not set");
        DEBUG("Commit volatile block (dirty): " <<
            original_mapping.DebugString() << " => " << modified_mapping.DebugString() <<
            ", event id " << block_mapping_written_event_log_id);

        this->ready_queue_.push(std::tr1::make_tuple(
                modified_mapping.block_id(),
                modified_mapping.version()));

        // bring the non-imported data back to the auxiliary index
        uint64_t block_id = modified_mapping.block_id();

        BlockMapping aux_mapping(block_id, this->block_size_);
        lookup_result aux_lr = this->ReadBlockInfoFromIndex(this->auxiliary_block_index_, &aux_mapping);
        CHECK(aux_lr != LOOKUP_ERROR, "Cannot read block info");

        if (aux_lr == LOOKUP_NOT_FOUND || aux_mapping.version() < modified_mapping.version()) {
            BlockMappingData mapping_value;
            CHECK(modified_mapping.SerializeTo(&mapping_value, false, true), "Failed to serialize block mapping: " << modified_mapping.DebugString());
            enum put_result result = this->auxiliary_block_index_->Put(&block_id, sizeof(uint64_t), mapping_value);
            CHECK(result != PUT_ERROR, "Update of auxiliary block index failed: " << modified_mapping.DebugString());
        }
    } else {
        // normal
        CHECK(auxiliary_block_index_, "Auxiliary index not set");
        DEBUG("Commit volatile block (direct): " <<
            original_mapping.DebugString() << " => " << modified_mapping.DebugString() <<
            ", event id " << block_mapping_written_event_log_id);

        if (!ProcessCommittedBlockWrite(original_mapping, modified_mapping, block_mapping_written_event_log_id)) {
            ERROR("Failed to process committed block write: " <<
                original_mapping.DebugString() << " => " << modified_mapping.DebugString());
            failed = true;
        }
    }
    return !failed;
}

bool BlockIndex::LogReplayBlockMappingDeleted(
    dedupv1::log::replay_mode replay_mode,
    const LogEventData& event_value,
    const dedupv1::log::LogReplayContext& context) {
    if (replay_mode == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        // ignore event
        return true;
    }
    FAULT_POINT("block-index.log-deleted-replay.pre");

    BlockMappingDeletedEventData event_data = event_value.block_mapping_deleted_event();
    CHECK(event_data.has_original_block_mapping(), "Event data has no block mapping");

    BlockMapping original_mapping(this->block_size_);
    CHECK(original_mapping.CopyFrom(event_data.original_block_mapping()), "Cannot copy from stored block mapping");
    uint64_t block_id = original_mapping.block_id();

    CHECK(this->block_locks_->WriteLock(block_id, LOCK_LOCATION_INFO),
        "Cannot acquire write lock: " << original_mapping.DebugString());
    ScopedRunnable<bool> scoped_unlock(NewRunnable(this->block_locks_,
                                           &BlockLocks::WriteUnlock, block_id, LOCK_LOCATION_INFO)); // we unlock the block at the end

    BlockMapping current_mapping(block_id, this->block_size_);
    lookup_result lr = this->ReadBlockInfoFromIndex(this->block_index_, &current_mapping);
    CHECK(lr != LOOKUP_ERROR, "Cannot read block info");
    BlockMapping aux_mapping(block_id, this->block_size_);
    lookup_result aux_lr = this->ReadBlockInfoFromIndex(this->auxiliary_block_index_, &aux_mapping);
    CHECK(aux_lr != LOOKUP_ERROR, "Cannot read block info");

    bool delete_from_aux = true;
    if (lr == LOOKUP_NOT_FOUND) {
        TRACE("Block " << block_id << " not in persistent index");
        // we are fine
    } else if (lr == LOOKUP_FOUND) {
        // do we need to delete it?
        CHECK(current_mapping.event_log_id() > 0, "Illegal event log id " << current_mapping.DebugString());
        if (current_mapping.event_log_id() < context.log_id()) {

            if (replay_mode == EVENT_REPLAY_MODE_REPLAY_BG) {
                // the persistent entry has been created based on an already committed entry
                // and the entry is older than the log id of the deletion
                // => It must be deleted
                TRACE("Delete re-imported deleted block from persistent block index: " <<
                    "original mapping " << original_mapping.DebugString() <<
                    ", persistent mapping " << current_mapping.DebugString());
                CHECK(this->block_index_->Delete(&block_id, sizeof(block_id)) != DELETE_ERROR,
                    "Failed to delete mapping from persistent index: "
                    "original mapping " << original_mapping.DebugString() <<
                    ", persistent mapping " << current_mapping.DebugString());
            } else {
                // later read requests are not allowed to see the currently persistent block mapping
                // either we delete the entry from the persistent block index, but we don't want that or
                // we insert a sentinel value in the auxiliary index

                BlockMapping empty_mapping(current_mapping.block_id(), current_mapping.block_size());
                CHECK(empty_mapping.FillEmptyBlockMapping(), "Failed to fill block mapping: " << empty_mapping.DebugString());
                empty_mapping.set_version(original_mapping.version() + 1);

                BlockMappingData mapping_value;
                CHECK(empty_mapping.SerializeTo(&mapping_value, false, true),
                    "Failed to serialize block mapping: " << mapping_value.DebugString());
                enum put_result result = this->auxiliary_block_index_->Put(&block_id, sizeof(uint64_t), mapping_value);
                CHECK(result != PUT_ERROR, "Update of auxiliary block index failed: " << empty_mapping.DebugString());

                delete_from_aux = false; // we yet inserted a new entry in the auxiliary index, we don't want that
                // it is deleted a few ms later
            }
        } else {
            // the persistent is newer than the deletion entry
            // we are not allowed to delete it
        }
    }

    if (delete_from_aux) {
        if (aux_lr == LOOKUP_NOT_FOUND) {
            TRACE("Block " << block_id << " not in auxiliary index");
            // we are fine
        } else if (aux_lr == LOOKUP_FOUND) {
            // do we need to delete it?

            if (aux_mapping.event_log_id() > 0 && aux_mapping.event_log_id()
                < context.log_id()) {
                // the auxiliary entry has been created based on an already committed entry
                // and the entry is older than the log id of the deletion
                // => It must be deleted
                CHECK(this->auxiliary_block_index_->Delete(&block_id, sizeof(block_id)) != DELETE_ERROR,
                    "Failed to delete mapping from auxiliary index: "
                    "original mapping " << original_mapping.DebugString() <<
                    ", auxiliary mapping " << aux_mapping.DebugString());
            } else {
                // the auxiliary entry is based on a newly created write and must
                // be therefore newer than the deletion
            }
        }
    }

    FAULT_POINT("block-index.log-deleted-replay.post");
    return true;
}

bool BlockIndex::LogReplayBlockMappingWrittenDirty(dedupv1::log::replay_mode replay_mode,
                                                   const LogEventData& event_value,
                                                   const dedupv1::log::LogReplayContext& context) {
    FAULT_POINT("block-index.log-replay.pre");

    if (replay_mode == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        // ignore event, block is written to auxiliary index
        return true;
    }
    BlockMappingWrittenEventData event_data = event_value.block_mapping_written_event();

    CHECK(event_data.has_mapping_pair(), "Event data has no block mapping pair: " << event_data.ShortDebugString());
    BlockMappingPair mapping_pair(block_size_);
    CHECK(mapping_pair.CopyFrom(event_data.mapping_pair()),
        "Cannot copy from stored block mapping: " << event_data.mapping_pair().ShortDebugString());
    BlockMapping stored_mapping = mapping_pair.GetModifiedBlockMapping(context.log_id());

    BlockMapping current_mapping(stored_mapping.block_id(), this->block_size_);
    lookup_result lr = this->ReadBlockInfoFromIndex(this->block_index_, &current_mapping);
    CHECK(lr != LOOKUP_ERROR, "Cannot read block info");
    BlockMapping aux_mapping(stored_mapping.block_id(), this->block_size_);
    lookup_result aux_lr = this->ReadBlockInfoFromIndex(this->auxiliary_block_index_, &aux_mapping);
    CHECK(aux_lr != LOOKUP_ERROR, "Cannot read block info");

    DEBUG("Replay block mapping written (dirty): " << mapping_pair.DebugString() << "\n" <<
        " replay mode " << dedupv1::log::Log::GetReplayModeName(replay_mode) <<
        ", event log id " << context.log_id() << "\n" <<
        (lr == LOOKUP_FOUND ? ", persistent " + current_mapping.DebugString() : ", persistent <not found>") << "\n" <<
        (aux_lr == LOOKUP_FOUND ? ", auxiliary " + aux_mapping.DebugString() : ", auxiliary <not found>") );

    if (aux_lr == LOOKUP_FOUND) {
        CHECK(aux_mapping.event_log_id() < context.log_id(),
            "Log id mismatch: log id " << context.log_id() <<
            ", event data " << event_data.ShortDebugString() <<
            ", auxiliary index data " << aux_mapping.DebugString());
    }
    bool import_block = lr == LOOKUP_NOT_FOUND || stored_mapping.version() > current_mapping.version();

    if (!import_block) {
        TRACE("Skip updating persistent block index: modified " << stored_mapping.DebugString() <<
            ", current " << current_mapping.DebugString() );
        return true;
    }
    stored_mapping.set_event_log_id(context.log_id());

    if (lr == LOOKUP_NOT_FOUND && aux_lr == LOOKUP_NOT_FOUND) {
        TRACE("Restore new block from log: " << stored_mapping.DebugString() <<
            ", open new block count " << (this->open_new_block_count_ + 1));
        this->open_new_block_count_++;
    }

    // gather all containers
    set<uint64_t> container_id_set;
    for (list<BlockMappingItem>::const_iterator i = stored_mapping.items().begin();
         i != stored_mapping.items().end();
         i++) {
        container_id_set.insert(i->data_address());
    }

    BlockMappingPairData* extra_message = new BlockMappingPairData();
    CHECK(extra_message, "Failed to allocate extra message");
    extra_message->CopyFrom(event_data.mapping_pair());

    // we can call "AddBlock" also if there are not open containers
    Option<bool> failed_state = IsKnownAsFailedBlock(stored_mapping.block_id(), stored_mapping.version());
    CHECK(failed_state.valid(), "Failed to check failed block write state: " << stored_mapping.DebugString());
    if (failed_state.value()) {
        DEBUG("Block is known to be failed: " << stored_mapping.DebugString());
    } else {
        CHECK(this->dirty_volatile_blocks_.AddBlock(stored_mapping,
                stored_mapping,
                extra_message,
                container_id_set,
                context.log_id(),
                this), "Cannot add block as uncommitted: " << mapping_pair.DebugString());
    }
    FAULT_POINT("block-index.log-replay.post");
    return true;
}

dedupv1::base::Option<bool> BlockIndex::IsKnownAsFailedBlock(uint64_t block_id, uint32_t version) {
    byte key[12];
    BuildBlockWriteFailedKey(key, block_id, version);

    lookup_result lr = failed_block_write_index_->Lookup(key, 12, NULL);
    CHECK(lr != LOOKUP_ERROR, "Failed to lookup failed block write index");

    if (lr == LOOKUP_FOUND) {
        return make_option(true);
    } else {
        return make_option(false);
    }
}

bool BlockIndex::LogReplayBlockMappingWriteFailedDirty(dedupv1::log::replay_mode replay_mode,
                                                       const LogEventData& event_value,
                                                       const dedupv1::log::LogReplayContext& context) {
    DCHECK(failed_block_write_index_, "Failed block write index not set");

    BlockMappingWriteFailedEventData event_data = event_value.block_mapping_write_failed_event();

    DEBUG("Replay block mapping write failed event (dirty): "
        "block id " << event_data.mapping_pair().block_id() <<
        ", version " << event_data.mapping_pair().version_counter() <<
        ", current log id " << context.log_id() <<
        ", replay type " << dedupv1::log::Log::GetReplayModeName(context.replay_mode()));

    uint64_t block_id = event_data.mapping_pair().block_id();
    uint32_t version = event_data.mapping_pair().version_counter();

    // We need this for cases when the system crashes directly after
    // the BlockMappingWriteFailed event is committed.
    byte key[12];
    BuildBlockWriteFailedKey(key, block_id, version);
    BlockWriteFailedData value;

    lookup_result lr = failed_block_write_index_->Lookup(key, 12, &value);
    CHECK(lr != LOOKUP_ERROR, "Failed to lookup failed block write index");

    if (lr == LOOKUP_NOT_FOUND) {
        put_result pr = failed_block_write_index_->Put(key, 12, value);
        CHECK(pr != PUT_ERROR, "Failed to update failed block write index");
    }
    return true;
}

bool BlockIndex::LogReplayBlockMappingWriteFailedBackground(dedupv1::log::replay_mode replay_mode,
                                                            const LogEventData& event_value,
                                                            const dedupv1::log::LogReplayContext& context) {
    BlockMappingWriteFailedEventData event_data = event_value.block_mapping_write_failed_event();

    DEBUG("Replay block mapping write failed event (background): "
        "block id " << event_data.mapping_pair().block_id() <<
        ", version " << event_data.mapping_pair().version_counter() <<
        ", current log id " << context.log_id() <<
        ", replay type " << dedupv1::log::Log::GetReplayModeName(context.replay_mode()));

    // remove a failed block/version pair from the persistent info, when the log is replayed.
    byte key[12];
    BuildBlockWriteFailedKey(key,
        event_data.mapping_pair().block_id(),
        event_data.mapping_pair().version_counter());

    delete_result dr = failed_block_write_index_->Delete(key, 12);
    CHECK(dr != DELETE_ERROR, "Failed to update failed block write index");

    return true;
}

bool BlockIndex::LogReplayBlockMappingWrittenBackground(dedupv1::log::replay_mode replay_mode,
                                                        const LogEventData& event_value,
                                                        const dedupv1::log::LogReplayContext& context) {
    FAULT_POINT("block-index.log-replay.pre");

    if (replay_mode == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        // ignore event, block is written to auxiliary index
        return true;
    }
    BlockMappingWrittenEventData event_data = event_value.block_mapping_written_event();
    CHECK(event_data.has_mapping_pair(), "Event data has no block mapping: " << event_data.ShortDebugString());

    BlockMappingPair mapping_pair(this->block_size_);
    CHECK(mapping_pair.CopyFrom(event_data.mapping_pair()),
        "Cannot copy from stored block mapping: " << event_data.mapping_pair().ShortDebugString());

    BlockMapping stored_mapping = mapping_pair.GetModifiedBlockMapping(context.log_id());

    CHECK(this->block_locks_->WriteLock(stored_mapping.block_id(), LOCK_LOCATION_INFO),
        "Cannot acquire write lock: " << stored_mapping.DebugString());
    ScopedRunnable<bool> scoped_unlock(NewRunnable(this->block_locks_,
                                           &BlockLocks::WriteUnlock, stored_mapping.block_id(), LOCK_LOCATION_INFO)); // we unlock the block at the end

    BlockMapping aux_mapping(stored_mapping.block_id(), this->block_size_);
    lookup_result aux_lr = this->ReadBlockInfoFromIndex(this->auxiliary_block_index_, &aux_mapping);
    CHECK(aux_lr != LOOKUP_ERROR, "Cannot read block info");

    if (aux_lr == LOOKUP_NOT_FOUND) {
        TRACE("Skip replay block mapping written: " << mapping_pair.DebugString() << "\n" <<
            " replay mode " << dedupv1::log::Log::GetReplayModeName(replay_mode) <<
            ", event log id " << context.log_id() << "\n" <<
            (aux_lr == LOOKUP_FOUND ? ", auxiliary " + aux_mapping.DebugString() : ", auxiliary <not found>") );
        return true;
    }

    BlockMapping current_mapping(stored_mapping.block_id(), this->block_size_);
    lookup_result lr = this->ReadBlockInfoFromIndex(this->block_index_, &current_mapping);
    CHECK(lr != LOOKUP_ERROR, "Cannot read block info");

    DEBUG("Replay block mapping written: " << mapping_pair.DebugString() << "\n" <<
        " replay mode " << dedupv1::log::Log::GetReplayModeName(replay_mode) <<
        ", event log id " << context.log_id() << "\n" <<
        (lr == LOOKUP_FOUND ? ", persistent " + current_mapping.DebugString() : ", persistent <not found>") << "\n" <<
        (aux_lr == LOOKUP_FOUND ? ", auxiliary " + aux_mapping.DebugString() : ", auxiliary <not found>") );

    bool import_block = stored_mapping.version() > current_mapping.version() || lr == LOOKUP_NOT_FOUND;
    if (aux_lr == LOOKUP_NOT_FOUND) {
        import_block = false;
    }
    // if the item was deleted from the auxiliary index, the item is deleted and it should not be reimported in the background.
    if (!import_block) {
        TRACE("Skip updating persistent block index: modified " << stored_mapping.DebugString() <<
            ", current " << current_mapping.DebugString() );
        return true;
    }

    Option<bool> failed_state = IsKnownAsFailedBlock(stored_mapping.block_id(), stored_mapping.version());
    CHECK(failed_state.valid(), "Failed to check failed state: block " << stored_mapping.block_id() << ", version " << stored_mapping.version());
    if (failed_state.value()) {
        INFO("Unrecoverable block write: block " << stored_mapping.block_id() << ", version " << stored_mapping.version());
        return true;
    }

    BlockMapping* import_mapping = &stored_mapping;
    set<uint64_t> commit_check_result;
    CHECK(this->BlockMappingStorageCheck(*import_mapping, &commit_check_result),
        "Failed to check storage state: " <<
        import_mapping->DebugString());
    if (commit_check_result.size() > 0) {
        DEBUG("Mapping has open containers that cannot be recovered: " <<
            "block mapping " << import_mapping->DebugString() <<
            ", open containers " <<
            "[" << Join(commit_check_result.begin(), commit_check_result.end(), ", ") + "]");
        INFO("Unrecoverable block write: block " << import_mapping->block_id() << ", version " << import_mapping->version());
        // skip the block

        Option<int64_t> write_event_log_id = make_option(context.log_id());
        CHECK(MarkBlockWriteAsFailed(mapping_pair, write_event_log_id, NO_EC),
            "Failed to mark block as failed: " <<
            "open containers [" << Join(commit_check_result.begin(), commit_check_result.end(), ", ") + "]" <<
            ", mapping pair " << mapping_pair.DebugString() <<
            ", imported " << import_mapping->DebugString());
        return true;
    }

    bool delete_from_aux = true;
    // here we set the event log id to be used if the stored version is imported.
    // will be overwritten if the auxiliary version is used.
    import_mapping->set_event_log_id(context.log_id());
    if (aux_lr == LOOKUP_FOUND && aux_mapping.version() > stored_mapping.version()) {
        // there is a auxiliary version that is newer than the replayed one
        if (aux_mapping.event_log_id() == 0) {
            // auxiliary version is volatile
            delete_from_aux = false;
        } else {
            // auxiliary version is not volatile and can be imported.
            import_mapping = &aux_mapping;

            // How can a higher version can have a lower log id
            // in addition this checks if the event log id is really set.
            // It may happen that it is equal due to the recovery of failed versions
            DCHECK(import_mapping->event_log_id() >= context.log_id(), "Illegal event log id: " <<
                "event log id " << context.log_id() <<
                ", stored " << stored_mapping.DebugString() <<
                ", auxiliary " << aux_mapping.DebugString());
        }
    }

    // all containers are committed
    uint64_t block_id = import_mapping->block_id();
    TRACE("Update persistent block index (replay): "
        << import_mapping->DebugString() <<
        ", current version " << current_mapping.version());
    BlockMappingData mapping_value;
    CHECK(import_mapping->SerializeTo(&mapping_value, false, true), "Cannot serialize block mapping: " << import_mapping->DebugString());

    if (lr == LOOKUP_NOT_FOUND) {
        if (this->open_new_block_count_ > 0) {
            TRACE("Persist new block: " << import_mapping->DebugString() <<
                ", open new block count " << (this->open_new_block_count_ - 1));
            this->open_new_block_count_--;
        } else {
            WARNING("Illegal open new block count: " << this->open_new_block_count_ <<
                ", modifier -1" <<
                ", mapping " << import_mapping->DebugString() <<
                ", context " << context.DebugString());
        }
    }

    // move the data from the auxiliary index to the persistent index
    enum put_result result = this->block_index_->Put(&block_id, sizeof(uint64_t), mapping_value);
    CHECK(result != PUT_ERROR, "Update of block index failed: " << import_mapping->DebugString());

    if (delete_from_aux) {
        CHECK(this->auxiliary_block_index_->Delete(&block_id, sizeof(uint64_t)) != DELETE_ERROR,
            "Cannot delete committed block from aux index: block id " << block_id);
    }
    this->stats_.index_writes_++;
    if (result != PUT_KEEP) {
        this->stats_.index_real_writes_++;
    }
    this->stats_.imported_block_count_.fetch_and_increment();

    FAULT_POINT("block-index.log-replay.post");
    return true;
}

bool BlockIndex::FinishDirtyLogReplay() {
    DEBUG("Finish dirty log replay");

    // there is no need to maintain this data

    std::multimap<uint64_t, UncommitedBlockEntry>::const_iterator i;
    for (i = dirty_volatile_blocks_.uncommited_block_map().begin();
         i != dirty_volatile_blocks_.uncommited_block_map().end(); i++) {
        const BlockMapping original_mapping(i->second.original_mapping());
        const BlockMapping modified_mapping(i->second.modified_mapping());
        const google::protobuf::Message* extra_message = i->second.extra_message();
        uint64_t open_container_count = i->second.open_container_count();
        uint32_t open_predecessor_count = i->second.open_predecessor_count();

        DCHECK(original_mapping.block_id() == modified_mapping.block_id(), "Illegal uncommit block entry: " << i->second.DebugString());
        DCHECK(modified_mapping.event_log_id() > 0, "Log id should be set: " << modified_mapping.DebugString());
        DCHECK(extra_message, "Extra message not set");
        int64_t log_id = modified_mapping.event_log_id();

        BlockMapping aux_mapping(modified_mapping.block_id(), this->block_size_);
        lookup_result aux_lr = this->ReadBlockInfoFromIndex(this->auxiliary_block_index_, &aux_mapping);
        CHECK(aux_lr != LOOKUP_ERROR, "Cannot read block info")

        BlockMapping pers_mapping(modified_mapping.block_id(), this->block_size_);
        lookup_result pers_lr = this->ReadBlockInfoFromIndex(this->block_index_, &pers_mapping);
        CHECK(pers_lr != LOOKUP_ERROR, "Cannot read block info")

        TRACE("Finish restore of block mapping: " <<
            "" << original_mapping.DebugString() << " => " << modified_mapping.DebugString()  <<
            ", mapping pair " << extra_message->DebugString() <<
            ", auxiliary index data " << (aux_lr == LOOKUP_FOUND ? aux_mapping.DebugString() : "<not found>"));

        set<uint64_t> container_id_set;
        for (list<BlockMappingItem>::const_iterator j = modified_mapping.items().begin();
             j != modified_mapping.items().end();
             j++) {
            container_id_set.insert(j->data_address());
        }
        set<uint64_t> unrecoverable_container_id_set;
        for (set<uint64_t>::iterator j = container_id_set.begin(); j != container_id_set.end(); j++) {
            TRACE("Check container: container id " << *j);
            enum storage_commit_state commit_check_result = storage_->IsCommitted(*j);
            CHECK(commit_check_result != STORAGE_ADDRESS_ERROR, "Cannot check commit state");
            if (commit_check_result != STORAGE_ADDRESS_COMMITED) {
                TRACE("Container not committed: contaienr id " << *j);
                unrecoverable_container_id_set.insert(*j);
            }
        }
        DCHECK(unrecoverable_container_id_set.size() == open_container_count,
            "Open container mismatch: expected open container count: " << open_container_count <<
            ", open containers " << "[" << Join(unrecoverable_container_id_set.begin(), unrecoverable_container_id_set.end(), ", ") + "]");

        Option<bool> failed_state = IsKnownAsFailedBlock(modified_mapping.block_id(), modified_mapping.version());
        CHECK(failed_state.valid(), "Failed to check failed write state: " << modified_mapping.DebugString());
        bool already_marked_as_failed = failed_state.value();

        DEBUG("Block mapping cannot be restored: " <<
            "open containers " << "[" << Join(unrecoverable_container_id_set.begin(), unrecoverable_container_id_set.end(), ", ") + "]" <<
            ", open predecessor count " << open_predecessor_count <<
            ", already marked as failed " << ToString(already_marked_as_failed) <<
            ", " << original_mapping.DebugString() << " => " << modified_mapping.DebugString()  <<
            ", auxiliary index data " << (aux_lr == LOOKUP_FOUND ? aux_mapping.DebugString() : "<not found>") <<
            ", persistent index data " << (pers_lr == LOOKUP_FOUND ? pers_mapping.DebugString() : "<not found>"));

        uint64_t block_id = modified_mapping.block_id();

        bool need_update = false;
        BlockMapping* import_mapping = NULL;
        if (aux_lr == LOOKUP_FOUND && aux_mapping.version() < modified_mapping.version()) {
            need_update = true;
            import_mapping = &aux_mapping;
        } else if (aux_lr == LOOKUP_NOT_FOUND && pers_lr == LOOKUP_FOUND && pers_mapping.version() < modified_mapping.version()) {
            need_update = true;
            import_mapping = &pers_mapping;
        } else if (aux_lr == LOOKUP_NOT_FOUND && pers_lr == LOOKUP_NOT_FOUND) {
            need_update = true;
            aux_mapping.FillEmptyBlockMapping();
            import_mapping = &aux_mapping;
        }
        if (need_update) {
            // aka this version is newer then what we had stored before
            // Note here that the aux_mapping/pers_mapping version is 0 if they are not found

            this->ready_queue_.push(std::tr1::make_tuple(modified_mapping.block_id(), modified_mapping.version()));
            import_mapping->set_version(modified_mapping.version()); // we update the version in any case, but we use
            // the old mapping data.
            import_mapping->set_event_log_id(i->second.block_mapping_written_event_log_id());

            DEBUG("Update auxiliary index with failed version: " << import_mapping->DebugString());

            BlockMappingData mapping_value;
            CHECK(import_mapping->SerializeTo(&mapping_value, false, true),
                "Failed to serialize block mapping: " << import_mapping->DebugString());
            enum put_result result = this->auxiliary_block_index_->Put(&block_id, sizeof(uint64_t), mapping_value);
            CHECK(result != PUT_ERROR, "Update of auxiliary block index failed: " << import_mapping->DebugString());
        }

        if (!already_marked_as_failed) {
            // TODO (dmeister): Can this still happen, when we remove all failed versions before?
            BlockMappingPairData mapping_pair_data;
            mapping_pair_data.CopyFrom(*extra_message);

            BlockMappingPair mapping_pair(block_size_);
            CHECK(mapping_pair.CopyFrom(mapping_pair_data), "Failed to copy from data: " << mapping_pair_data.ShortDebugString())

            CHECK(MarkBlockWriteAsFailed(mapping_pair, make_option(log_id),NO_EC),
                "Failed to mark mapping as failed: " << original_mapping.DebugString() << " => " << modified_mapping.DebugString());
        }
    }

    dirty_volatile_blocks_.Clear();
    return true;
}

bool BlockIndex::LogReplay(dedupv1::log::event_type event_type,
                           const LogEventData& event_value,
                           const dedupv1::log::LogReplayContext& context) {
    ProfileTimer timer(this->stats_.replay_time_);
    if (event_type == dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN) {
        // Background Block mapping written

        if (context.replay_mode() == EVENT_REPLAY_MODE_REPLAY_BG) {
            return LogReplayBlockMappingWrittenBackground(context.replay_mode(), event_value, context);
        } else if (context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
            return LogReplayBlockMappingWrittenDirty(context.replay_mode(), event_value, context);
        }
    } else if (event_type == dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_DELETED &&
               context.replay_mode() != EVENT_REPLAY_MODE_DIRECT) {
        // Background Block mapping deleted
        return LogReplayBlockMappingDeleted(context.replay_mode(), event_value, context);
    } else if (event_type == dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED &&
               context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
        return LogReplayBlockMappingWriteFailedDirty(context.replay_mode(), event_value, context);
    } else if (event_type == dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED &&
               context.replay_mode() == EVENT_REPLAY_MODE_REPLAY_BG) {
        return LogReplayBlockMappingWriteFailedBackground(context.replay_mode(), event_value, context);
    } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_COMMITED &&
               context.replay_mode() == EVENT_REPLAY_MODE_DIRECT) {
        // Direct Container Commit
        ContainerCommittedEventData event_data = event_value.container_committed_event();

        CHECK(this->volatile_blocks_.Commit(event_data.container_id(),  this),
            "Cannot process container commit: " << event_data.ShortDebugString());
    } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_COMMITED &&
               context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
        // Direct Container Commit
        ContainerCommittedEventData event_data = event_value.container_committed_event();

        DEBUG("Replay container commit: container id " << event_data.container_id());

        CHECK(this->dirty_volatile_blocks_.Commit(event_data.container_id(),  this),
            "Cannot process container commit (dirty): " << event_data.ShortDebugString());
    } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_COMMIT_FAILED &&
               context.replay_mode() == EVENT_REPLAY_MODE_DIRECT) {
        ContainerCommitFailedEventData event_data = event_value.container_commit_failed_event();

        // Direct Container Commit Failed
        CHECK(this->volatile_blocks_.Abort(event_data.container_id(), this),
            "Failed to abort container id " << event_data.container_id());
    } else if (event_type == dedupv1::log::EVENT_TYPE_LOG_EMPTY &&
               context.replay_mode() == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        this->volatile_blocks_.ResetTracker();
    } else if (event_type == dedupv1::log::EVENT_TYPE_REPLAY_STARTED &&
               context.replay_mode() == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        is_replaying_ = true;

        ReplayStartEventData event_data = event_value.replay_start_event();
        is_full_log_replay_ = event_data.full_log_replay();
    }  else if (event_type == dedupv1::log::EVENT_TYPE_REPLAY_STOPPED &&
                context.replay_mode() == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        is_replaying_ = false;

        ReplayStopEventData event_data = event_value.replay_stop_event();

        if (event_data.replay_type() == dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START) {
            return FinishDirtyLogReplay();
        }
    }
    return true;
}

dedupv1::base::Option<bool> BlockIndex::TryImportBlock(bool with_waiting_period) {
    DCHECK(this->auxiliary_block_index_, "Auxiliary block index not set");

    bool has_items_to_import = IsMinimalImportSizeReached();
    bool hard_limit_reached = false;
    bool soft_limit_reached = false;
    if (has_items_to_import) {
        hard_limit_reached = this->IsHardLimitReached();
        if (!hard_limit_reached) {
            soft_limit_reached = this->IsSoftLimitReached();
        } else {
            soft_limit_reached = true;
        }
    }
    bool is_idle = (idle_detector_ && idle_detector_->IsIdle());

    if (with_waiting_period) {
        int waiting_period = 0; // in ms
        if (has_items_to_import && is_replaying_) {
            waiting_period = log_replay_import_delay_;
            if (is_full_log_replay_) {
                waiting_period = full_log_replay_import_delay_;
            }
        } else if (hard_limit_reached) {
            waiting_period = hard_limit_import_delay_;
        } else if (soft_limit_reached) {
            waiting_period = soft_limit_import_delay_;
        } else if (has_items_to_import && is_idle) {
            waiting_period = idle_import_delay_;
        } else {
            waiting_period = default_import_delay_;
        }

        if (waiting_period > 0) {
            TRACE("Delay import: delay time " << waiting_period << "ms");
            ThreadUtil::Sleep(waiting_period, ThreadUtil::MILLISECONDS);
        }
    }

    bool should_import = false;
    if (has_items_to_import) {
        should_import = soft_limit_reached ||
                        (import_if_idle_ && is_idle) ||
                        (import_if_replaying_ && is_replaying_);
    }
    if (!should_import) {
        return make_option(false);
    }

    TRACE("Try import next block: " <<
        "current auxiliary index size " << this->auxiliary_block_index_->GetItemCount() <<
        ", max auxiliary index size " << this->max_auxiliary_block_index_size_ <<
        ", ready queue size " << this->ready_queue_.unsafe_size());
    // stop property

    tbb::tick_count start_tick = tbb::tick_count::now();
    lookup_result r = ImportNextBlockBatch();
    tbb::tick_count end_tick = tbb::tick_count::now();

    CHECK(r != LOOKUP_ERROR, "Failed to import next ready block");
    if (r == LOOKUP_NOT_FOUND) {
        // if there are no ready container we cannot import them
    } else if (r == LOOKUP_FOUND) {
        this->stats_.import_latency_.Add((end_tick - start_tick).seconds() * 1000);
    }

    return make_option(true);
}

bool BlockIndex::IsMinimalImportSizeReached() {
    if (this->auxiliary_block_index_ == NULL) {
        return false;
    }
    return this->auxiliary_block_index_->GetItemCount() > this->minimal_replay_import_size_;
}

bool BlockIndex::IsSoftLimitReached() {
    if (this->auxiliary_block_index_ == NULL) {
        return false;
    }
    return this->auxiliary_block_index_->GetItemCount() > this->max_auxiliary_block_index_size_;
}

bool BlockIndex::IsHardLimitReached() {
    if (this->auxiliary_block_index_ == NULL) {
        return false;
    }
    return this->auxiliary_block_index_->GetItemCount() > this->auxiliary_block_index_hard_limit_;
}

}
}
