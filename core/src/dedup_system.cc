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

#include <core/dedup_system.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <string>
#include <list>
#include <sstream>

#include <core/dedup.h>
#include <base/locks.h>

#include <core/log_consumer.h>
#include <core/block_index.h>
#include <core/chunk_index.h>
#include <core/filter_chain.h>
#include <base/strutil.h>
#include <core/chunk_store.h>
#include <core/content_storage.h>
#include <base/crc32.h>
#include <core/session_management.h>
#include <base/thread.h>
#include <base/runnable.h>
#include <core/container_storage.h>
#include <core/dedup_system.h>
#include <core/dedup_volume.h>
#include <core/filter.h>
#include <core/chunk_index_filter.h>
#include <core/sparse_chunk_index_filter.h>
#include <core/block_index_filter.h>
#include <core/bytecompare_filter.h>
#include <core/zerochunk_filter.h>
#include <core/bloom_filter.h>
#include <core/chunker.h>
#include <core/static_chunker.h>
#include <core/rabin_chunker.h>
#include <core/fingerprinter.h>
#include <core/crypto_fingerprinter.h>
#include <base/logging.h>
#include <core/chunk.h>
#include <base/timer.h>
#include <core/open_request.h>
#include <core/session.h>
#include <base/fileutil.h>
#include <core/dedup_volume_info.h>
#include <core/container_storage_gc.h>
#include <core/container_storage_alloc.h>
#include <core/idle_detector.h>
#include <core/garbage_collector.h>
#include <core/usage_count_garbage_collector.h>
#include <core/none_garbage_collector.h>
#include <core/container_storage.h>
#include <core/container_storage_bg.h>
#include <core/session.h>
#include <core/container_storage_write_cache.h>
#include <base/callback.h>
#include <base/config_loader.h>

using std::list;
using std::string;
using std::stringstream;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::SessionResourceType;
using dedupv1::base::ProfileTimer;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::chunkstore::ChunkStore;
using dedupv1::filter::FilterChain;
using dedupv1::ContentStorage;
using dedupv1::Session;
using dedupv1::OpenRequest;
using dedupv1::base::ResourceManagement;
using dedupv1::log::Log;
using dedupv1::gc::GarbageCollector;
using dedupv1::gc::UsageCountGarbageCollector;
using dedupv1::chunkindex::ChunkIndexFactory;
using dedupv1::chunkindex::ChunkIndex;
using dedupv1::log::EVENT_TYPE_SYSTEM_START;
using dedupv1::log::EVENT_TYPE_SYSTEM_RUN;
using dedupv1::scsi::SCSI_CHECK_CONDITION;
using dedupv1::scsi::SCSI_KEY_MEDIUM_ERROR;
using dedupv1::scsi::SCSI_KEY_ILLEGAL_REQUEST;
using dedupv1::scsi::SCSI_KEY_NOT_READY;
using dedupv1::scsi::SCSI_KEY_DATA_PROTECTED;
using dedupv1::scsi::SCSI_KEY_RECOVERD;
using dedupv1::scsi::SCSI_OK;
using dedupv1::base::NewCallback;
using dedupv1::base::ConfigLoader;
using dedupv1::base::DebugStringLockParam;
using dedupv1::blockindex::BlockIndex;
using dedupv1::scsi::ScsiResult;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::ErrorContext;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::ThreadUtil;

LOGGER("DedupSystem");

namespace dedupv1 {

const size_t DedupSystem::kChunkResourceFactor = 512;
const uint32_t DedupSystem::kDefaultLogFullPauseTime = 100 * 1000;
const uint32_t DedupSystem::kDefaultBlockSize = 256 * 1024;
const uint32_t DedupSystem::kDefaultSessionCount = 128;

const ScsiResult DedupSystem::kFullError(dedupv1::scsi::SCSI_CHECK_CONDITION,
                                         dedupv1::scsi::SCSI_KEY_HARDWARE_ERROR, 0x03, 0x00);
const ScsiResult DedupSystem::kReadChecksumError(dedupv1::scsi::SCSI_CHECK_CONDITION,
                                                 dedupv1::scsi::SCSI_KEY_MEDIUM_ERROR, 0x11, 0x04);

DedupSystem::Statistics::Statistics() : average_waiting_time_(256) {
    active_session_count_ = 0;
    processsed_session_count_ = 0;
    long_running_request_count_ = 0;
}

DedupSystem::DedupSystem() {
    chunk_index_ = NULL;
    block_index_ = NULL;
    chunk_store_ = NULL;
    filter_chain_ = NULL;
    content_storage_ = NULL;
    volume_info_ = NULL;
    log_ = NULL;
    block_size_ = kDefaultBlockSize;
    disable_sync_cache_ = false;
    info_store_ = NULL;
    readonly_ = false;

    volume_info_ = NULL;
    chunk_management_ = NULL;
    gc_ = NULL;
    tp_ = NULL;
    write_retry_count_ = 0;
    read_retry_count_ = 0;

#ifdef NDEBUG
    report_long_running_requests_ = false;
#else
    report_long_running_requests_ = true;
#endif
    state_ = CREATED;
}

DedupSystem::~DedupSystem() {
}

bool DedupSystem::Init() {
    filter_chain_ = new FilterChain();
    CHECK(this->filter_chain_, "Filter chain Init failed");

    this->content_storage_ = new ContentStorage();
    CHECK(this->content_storage_, "Content storage Init failed");

    this->log_ = new Log();
    CHECK(this->log_, "Cannot create log");
    CHECK(this->log_->Init(), "Cannot init log");

    // chunk index is a configurable type and it therefore created
    // later
    this->chunk_index_ = NULL;

    this->block_index_ = new BlockIndex();
    CHECK(this->block_index_, "Block index creation failed");
    CHECK(this->block_index_->Init(), "Block index init failed");

    this->volume_info_ = new DedupVolumeInfo();
    CHECK(this->volume_info_, "cannot create volume info");

    gc_ = NULL;

    state_ = CREATED;
    return true;
}

bool DedupSystem::LoadOptions(const string& filename) {
    CHECK(this->state_ == CREATED, "Dedup system already started");

    ConfigLoader config_load(NewCallback(this, &DedupSystem::SetOption));
    CHECK(config_load.ProcessFile(filename),
        "Cannot process configuration file: " << filename);
    return true;
}

bool DedupSystem::SetOption(const string& option_name, const string& option) {
    CHECK(this->state_ == CREATED, "Dedup system already started");
    CHECK(option_name.size() > 0, "Option name not set");
    CHECK(option.size() > 0, "Option not set");

    if (option_name == "block-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        int32_t bs = ToStorageUnit(option).value();
        CHECK(bs % 512 == 0, "Block size is not aligned to 512 sectors");
        CHECK(bs > 0, "Block size must be larger than 0");
        this->block_size_ = bs;
        return true;
    }
    if (option_name == "disable-sync-cache") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->disable_sync_cache_ = To<bool>(option).value();
        return true;
    }
    if (option_name == "write-retries") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->write_retry_count_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "read-retries") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->read_retry_count_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "session-count") {
        WARNING("Option session-count is depreciated");
        return true;
    }
    // Block index
    if (StartsWith(option_name, "block-index.")) {
        CHECK(this->block_index_, "Block index not set");
        return this->block_index_->SetOption(option_name.substr(strlen("block-index.")), option);
    }

    // Chunk index
    if (option_name == "chunk-index") {
        CHECK(this->chunk_index_ == NULL, "Chunk index already set");
        this->chunk_index_ = ChunkIndex::Factory().Create(option);
        CHECK(this->chunk_index_, "Failed to create chunk index: " << option);
        return true;
    }
    if (StartsWith(option_name, "chunk-index.")) {
        if (this->chunk_index_ == NULL) {
            this->chunk_index_ = ChunkIndex::Factory().Create("disk");
            CHECK(this->chunk_index_, "Failed to create default chunk index");
        }
        return this->chunk_index_->SetOption(option_name.substr(strlen("chunk-index.")), option);
    }

    // Storage
    if (option_name == "storage") {
        this->chunk_store_ = new ChunkStore();
        CHECK(this->chunk_store_, "Storage creation failed");
        CHECK(this->chunk_store_->Init(option), "Storage init failed");
        return true;
    }
    if (StartsWith(option_name, "storage.")) {
        CHECK(this->chunk_store_, "Chunk store not set");
        return this->chunk_store_->SetOption(option_name.substr(strlen("storage.")), option);
    }

    // Filter
    // Filter
    if (option_name == "filter") {
        CHECK(this->filter_chain_->AddFilter(option), "Filter Chain Insertion failed");
        return true;
    }
    if (StartsWith(option_name, "filter.")) {
        return this->filter_chain_->SetOption(option_name.substr(strlen("filter.")), option);
    }

    if (StartsWith(option_name, "chunking")) {
        // delete default chunking information
        CHECK(this->content_storage_->SetOption(option_name, option),
            "Default chunking configuration failed");
        return true;
    }
    // Content Storage
    if (option_name == "fingerprinting") {
        // "fingerprinting." is trimmed by content_storage.
        CHECK(this->content_storage_->SetOption(option_name, option),
            "Fingerprinting configuration failed");
        return true;
    }
    if (StartsWith(option_name, "content-storage.")) {
        // "content-storage." is trimmed by content_storage.
        CHECK(this->content_storage_->SetOption(option_name, option),
            "Chunking configuration failed");
        return true;
    }
    // log
    if (StartsWith(option_name, "log.")) {
        CHECK(this->log_->SetOption(option_name.substr(strlen("log.")), option),
            "Log configuration failed");
        return true;
    }

    // gc
    if (option_name == "gc") {
        CHECK(gc_ == NULL, "GC already set");
        gc_ = GarbageCollector::Factory().Create(option);
        CHECK(gc_, "Failed to create garbage collector");
        return true;
    }
    if (StartsWith(option_name, "gc.")) {
        if (gc_ == NULL) {
            // GC not set, use default
            gc_ = GarbageCollector::Factory().Create("usage-count");
            CHECK(gc_, "Failed to create garbage collector");
        }
        return this->gc_->SetOption(option_name.substr(strlen("gc.")), option);
    }

    // idle detector
    if (StartsWith(option_name, "idle-detection.")) {
        return this->idle_detector_.SetOption(
            option_name.substr(strlen("idle-detection.")),
            option);
    }
    if (StartsWith(option_name, "block-locks.")) {
        CHECK(this->block_locks_.SetOption(
                option_name.substr(strlen("block-locks.")),
                option),
            "Block locks configuration failed");
        return true;
    }
    if (option_name == "report-long-running-requests") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->report_long_running_requests_ = To<bool>(option).value();
        return true;
    }
#ifdef DEDUPV1_CORE_TEST
    if (StartsWith(option_name, "raw-volume.")) {
        CHECK(this->volume_info_->SetOption(
                option_name.substr(strlen("raw-volume.")),
                option),
            "Raw volume configuration failed");
        return true;
    }
#endif
    ERROR("Illegal option: " << option_name);
    return false;
}

bool DedupSystem::Start(const StartContext& start_context,
                        dedupv1::InfoStore* info_store,
                        dedupv1::base::Threadpool* tp) {
    CHECK(this->state_ == CREATED, "Dedup system already started");
    CHECK(this->block_size_ > 0, "Dedup system not ready: Block size not set");
    CHECK(this->chunk_index_, "Dedup system not ready: Chunk index not ready");
    CHECK(this->chunk_store_, "Chunk store not configured");

    dedupv1::base::Walltimer startup_timer;

    DEBUG("Started dedup system: " << start_context.DebugString());

    readonly_ = start_context.readonly();

    if (gc_ == NULL) {
        // use default gc
        gc_ = GarbageCollector::Factory().Create("usage-count");
        CHECK(gc_, "Failed to create garbage collector");
    }

    this->chunk_management_ = new ResourceManagement<Chunk>();
    CHECK(this->chunk_management_, "Cannot create chunk resource management");
    CHECK(this->chunk_management_->Init("chunk", 32 * kChunkResourceFactor,
            new ChunkResourceType(), false),
        "Cannot init chunk resource management");

    tp_ = tp;
    info_store_ = info_store;
    CHECK(info_store_, "Info store not set");

    CHECK(this->log_->Start(start_context, this), "Log starting failed");

    CHECK(this->block_index_, "Block index not configured");
    CHECK(this->block_index_->Start(start_context, this), "Block index starting failed");

    CHECK(this->chunk_index_, "Chunk index not configured");
    CHECK(this->chunk_index_->Start(start_context, this), "Chunk index starting failed");

    CHECK(this->chunk_store_, "Storage not configured");
    CHECK(this->chunk_store_->Start(start_context, this), "Storage starting failed");

    CHECK(this->filter_chain_, "Filter chain not configured");
    CHECK(this->filter_chain_->Start(this), "Filter chain starting failed");

    CHECK(this->volume_info_->Start(this), "Cannot start volume info");

    CHECK(this->content_storage_, "Content Storage not configured");
    CHECK(this->content_storage_->Start(
            this->tp_,
            this->block_index_,
            this->chunk_index_,
            this->chunk_store_,
            this->filter_chain_,
            this->chunk_management_,
            this->log_,
            &this->block_locks_,
            this->block_size_),
        "Content Storage start failed");

    CHECK(this->gc_->Start(start_context, this), "Cannot start gc");
    CHECK(this->idle_detector_.Start(), "Cannot start idle detection");
    CHECK(this->block_locks_.Start(), "Failed to start block locks");

    if (!start_context.readonly()) {
        SystemStartEventData event_data;
        event_data.set_create(start_context.create());
        event_data.set_crashed(start_context.has_crashed());
        event_data.set_forced(start_context.force());
        event_data.set_dirty(start_context.dirty());
        CHECK(this->log_->CommitEvent(EVENT_TYPE_SYSTEM_START,
                &event_data, NULL, NULL, NO_EC),
            "Cannot log system start event");
    }
    DEBUG("Started dedup system (startup time: " << startup_timer.GetTime() << "ms)");
    this->state_ = STARTED;
    return true;
}

bool DedupSystem::Run() {
    CHECK(this->state_ == STARTED, "Illegal state: " << state_);

    CHECK(this->block_index_->Run(), "Failed to run block index");
    CHECK(this->chunk_index_->Run(), "Failed to run chunk index");
    CHECK(this->chunk_store_->Run(), "Failed to run chunk store");
    CHECK(this->log_->Run(), "Failed to run log");
    CHECK(this->gc_->Run(), "Cannot start gc");
    CHECK(this->idle_detector_.Run(), "Cannot run idle detection");

    if (!readonly_) {
        CHECK(this->log_->CommitEvent(EVENT_TYPE_SYSTEM_RUN, NULL, NULL, NULL, NO_EC),
            "Cannot log system start event");
    }
    INFO("Dedup subsystem is running");
    this->state_ = RUNNING;
    return true;
}

bool DedupSystem::Stop(const dedupv1::StopContext& stop_context) {
    DEBUG("Stopping dedup subsystem");
    bool failed = false;

    if (!this->idle_detector_.Stop(stop_context)) {
        ERROR("Cannot stop idle detection");
        failed = true;
    }

    // gc has to be stopped before the chunk index
    if (this->gc_) {
        if (!this->gc_->Stop(stop_context)) {
            ERROR("Cannot stop gc");
            failed = true;
        }
    }

    // flush the last data to disk so that chunk index and block index can rely
    // on this
    if (this->chunk_store_) {
        if (!this->chunk_store_->Flush(NULL)) {
            ERROR("Failed to flush chunk store");
            failed = true;
        }
    }
    // we also want to make sure that there is no container open in the background
    // committing system.
    // By the way: I don't think that the timeout thread is a problem because
    // either the timeout thread commits a due container before the flush or the flush does it.
    dedupv1::chunkstore::ContainerStorage* cs = dynamic_cast<dedupv1::chunkstore::ContainerStorage*>(this->storage());
    if (cs) {
        if (!cs->background_committer()->Stop(stop_context)) {
            ERROR("Failed to stop background committer");
            failed = true;
        }
    }

    if (this->log_) {
        if (!this->log_->Stop(stop_context)) {
            ERROR("Failed to stop log");
            failed = true;
        }
    }

    // we stop the chunk index and the block index in parallel
    Thread<bool>* chunk_index_stop_thread = NULL;
    Thread<bool>* block_index_stop_thread = NULL;

    if (this->chunk_index_) {
        dedupv1::base::Runnable<bool>* r = NewRunnable(this->chunk_index(),
            &ChunkIndex::Stop,
            stop_context);
        chunk_index_stop_thread = new Thread<bool>(r, "chunk index stop");
    }
    if (this->block_index_) {
        block_index_stop_thread = new Thread<bool>(NewRunnable(this->block_index(),
                                                       &BlockIndex::Stop, stop_context),
                                                   "block index stop");
    }

    bool start1 = false;
    if (chunk_index_stop_thread) {
        start1 = chunk_index_stop_thread->Start();
        if (!start1) {
            WARNING("Failed to start the shutdown of the chunk index");
        }
    }

    bool start2 = false;
    if (block_index_stop_thread) {
        start2 = block_index_stop_thread->Start();
        if (!start2) {
            WARNING("Failed to start the shutdown of the block index");
        }
    }

    if (start1) {
        bool result = false;
        if (!chunk_index_stop_thread->Join(&result)) {
            WARNING("Failed to join chunk index shutdown thread");
        }
        if (!result) {
            WARNING("Shutdown of chunk index resulted in error");
        }
    }
    if (start2) {
        bool result = false;
        if (!block_index_stop_thread->Join(&result)) {
            WARNING("Failed to join block index shutdown thread");
        }
        if (!result) {
            WARNING("Shutdown of block index resulted in error");
        }
    }

    delete chunk_index_stop_thread;
    chunk_index_stop_thread = NULL;
    delete block_index_stop_thread;
    block_index_stop_thread = NULL;

    if (this->chunk_store_) {
        if (!this->chunk_store_->Stop(stop_context)) {
            ERROR("Cannot stop chunk store");
            failed = true;
        }
    }

    return !failed;
}

bool DedupSystem::Close() {
    bool result = true;
    DEBUG("Closing dedup subsystem");

    if (!this->Stop(StopContext::FastStopContext())) {
        ERROR("Failed to stop dedup system");
        result = false;
    }

    if (this->gc_) {
        if (!this->gc_->Close()) {
            ERROR("gc close failed");
            result = false;
        }
        this->gc_ = NULL;
    }

    if (this->chunk_management_) {
        if (!this->chunk_management_->Close()) {
            ERROR("Chunk management close failed");
            result = false;
        }
        this->chunk_management_ = NULL;
    }

    if (this->chunk_index_) {
        if (!this->chunk_index_->Close()) {
            ERROR("Chunk index close failed");
            result = false;
        }
        this->chunk_index_ = NULL;
    }

    if (this->block_index_) {
        if (!this->block_index_->Close()) {
            ERROR("Block index close failed");
            result = false;
        }
        delete this->block_index_;
        this->block_index_ = NULL;
    }

    if (this->chunk_store_) {
        if (!this->chunk_store_->Close()) {
            WARNING("Chunk store close failed");
            result = false;
        }
        this->chunk_store_ = NULL;
    }

    if (this->filter_chain_) {
        if (!this->filter_chain_->Close()) {
            ERROR("Filter chain close failed");
            result = false;
        }
        this->filter_chain_ = NULL;
    }

    // Close volume info before the content storage
    // as the sessions in the volumes held a
    // reference to the content storage
    if (this->volume_info_) {
        if (!this->volume_info_->Close()) {
            ERROR("Volume info close failed");
            result = false;
        }
        this->volume_info_ = NULL;
    }

    if (this->content_storage_) {
        if (!this->content_storage_->Close()) {
            ERROR("Content store close failed");
            result = false;
        }
        this->content_storage_ = NULL;
    }

    if (this->log_) {
        if (!this->log_->Close()) {
            ERROR("Log close failed");
            result = false;
        }
        this->log_ = NULL;
    }

    if (!this->idle_detector_.Close()) {
        ERROR("idle detection close failed");
        result = false;
    }

    DEBUG("Closed dedup subsystem");
    delete this;
    return result;
}

ScsiResult DedupSystem::SyncCache() {
    ProfileTimer timer(this->stats_.profiling_total_);

    CHECK_RETURN(this->state_ == RUNNING,
        ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_NOT_READY, 0x04, 0x00),
        "System not started");

    DEBUG("Dedup Sync Cache");

    if (disable_sync_cache_) {
        // skip the processing
        // we say that we do not support the message
        return ScsiResult::kIllegalMessage;
    }

    dedupv1::base::ErrorContext ec;
    if (chunk_store_) {
        // TODO (dmeister): in an error we report a write error as I don't know
        // what to really return
        CHECK_RETURN(chunk_store_->Flush(&ec),
            ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_MEDIUM_ERROR, 0x0C, 0x00),
            "Failed to flush storage");
    } else {
        WARNING("Chunk store not set");
    }

    return ScsiResult::kOk;
}

bool DedupSystem::FastBlockCopy(
    uint64_t src_block_id,
    uint64_t src_offset,
    uint64_t target_block_id,
    uint64_t target_offset,
    uint64_t size,
    dedupv1::base::ErrorContext* ec) {

    DEBUG("Fast block block: src block " << src_block_id <<
        ", target block " << target_block_id <<
        ", src offset " << src_offset <<
        ", target offset " << target_offset <<
        ", size " << size);

    // deadlock prevention
    list<uint64_t> blocks;
    blocks.push_back(src_block_id);
    blocks.push_back(target_block_id);

    list<uint64_t> locked_block_list;
    list<uint64_t> unlocked_block_list;

    // In this loop we try to get all necessary locks. If this fails, we back
    // off and try again later
    // We cannot make any guarantees about the lock ordering here, therefore
    // we have to break deadlocks by backing off
    while (true) {
        locked_block_list.clear();
        unlocked_block_list.clear();
        CHECK(block_locks_.TryWriteLocks(blocks,
                &locked_block_list,
                &unlocked_block_list,
                LOCK_LOCATION_INFO),
            "Failed to acquire locks for block list");

        if (locked_block_list.size() == blocks.size()) {
            // We now have all locks
            break;
        } else {
            CHECK(block_locks_.WriteUnlocks(locked_block_list, LOCK_LOCATION_INFO),
                "Failed to unlock block locks");
            ThreadUtil::Sleep(100, ThreadUtil::MILLISECONDS);
        }
    }

    bool r = this->content_storage_->FastCopyBlock(src_block_id,
        src_offset,
        target_block_id,
        target_offset,
        size,
        ec);

    CHECK(this->block_locks_.WriteUnlocks(blocks, LOCK_LOCATION_INFO),
        "Read unlock failure for block locks");

    return r;
}

dedupv1::scsi::ScsiResult DedupSystem::FastCopy(
    uint64_t src_block_id,
    uint64_t src_offset,
    uint64_t target_block_id,
    uint64_t target_offset,
    uint64_t size,
    dedupv1::base::ErrorContext* ec) {

    ProfileTimer timer(this->stats_.profiling_total_);

    DCHECK_RETURN(this->state_ == RUNNING,
        ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_NOT_READY, 0x04, 0x00),
        "System not running");

    DEBUG("Fast copy: src block id " << src_block_id <<
        ", target block id " << target_block_id <<
        ", src offset " << src_offset <<
        ", target offset " << target_offset <<
        ", size " << size);

    // In contrast to the MakeRequest method, I do not check the chunk index here as a clone
    // nearly by definition doesn't insert new items into it

    uint32_t request_size = 0;
    bool failed = false;
    while (size > 0 && !failed) {
        if (src_offset + size > block_size_ || target_offset + size > block_size_) {
            request_size = std::min(block_size_ - src_offset, block_size_ - target_offset);
        } else {
            request_size = size;
        }
        TRACE("Size " << size <<
            ", request size " << request_size);

        bool r = FastBlockCopy(src_block_id,
            src_offset,
            target_block_id,
            target_offset,
            request_size,
            ec);
        if (!r) {
            failed = true;
        }

        size -= request_size;
        if (target_offset + request_size >= block_size_) {
            target_block_id++;
            target_offset = 0;
        } else {
            target_offset += request_size;
        }

        if (src_offset + request_size >= block_size_) {
            src_block_id++;
            src_offset = 0;
        } else {
            src_offset += request_size;
        }
    }

    if (failed) {
        if (ec) {
            if (ec->is_full()) {
                return kFullError;
            }
            if (ec->has_checksum_error()) {
                return kReadChecksumError;
            }
        }
    }
    if (failed) {
        // r was not set to a failure state, so we use a default
        return ScsiResult::kWriteError;
    }

    return ScsiResult::kOk;
}

bool DedupSystem::MakeBlockRequest(
    Session* sess,
    Request* block_request,
    bool last_block_request,
    ErrorContext* ec) {
    DCHECK(sess, "Session not set");
    DCHECK(block_request, "Block request not set");

    NESTED_LOG_CONTEXT(", block " + ToString(block_request->block_id()));

    RequestStatistics request_stats;
    request_stats.Start(RequestStatistics::TOTAL);
    if (block_request->request_type() == REQUEST_READ) {
        request_stats.Start(RequestStatistics::WAITING);
        CHECK(this->block_locks_.ReadLock(block_request->block_id(), LOCK_LOCATION_INFO),
            "Read lock failure for block lock: " << block_request->DebugString());
        request_stats.Finish(RequestStatistics::WAITING);
        this->stats_.average_waiting_time_.Add(request_stats.latency(RequestStatistics::WAITING));

        this->stats_.processsed_session_count_.fetch_and_increment();
        // postpone the jump until the read lock is released

        bool result = this->content_storage_->ReadBlock(sess, block_request, &request_stats, ec);
        this->stats_.processsed_session_count_.fetch_and_decrement();

        CHECK(this->block_locks_.ReadUnlock(block_request->block_id(), LOCK_LOCATION_INFO),
            "Cannot unlock block lock: " << block_request->DebugString());
        CHECK(result,
            "Read failure: request " << block_request->DebugString() <<
            ", active requests " << this->stats_.active_session_count_);
    } else {
        request_stats.Start(RequestStatistics::WAITING);
        CHECK(this->block_locks_.WriteLock(block_request->block_id(), LOCK_LOCATION_INFO),
            "Write lock failure for block lock: " << block_request->DebugString());
        request_stats.Finish(RequestStatistics::WAITING);
        this->stats_.average_waiting_time_.Add(request_stats.latency(RequestStatistics::WAITING));

        this->stats_.processsed_session_count_.fetch_and_increment();

        if (!this->content_storage_->WriteBlock(sess, block_request, &request_stats, last_block_request, ec)) {
            if (ec && ec->is_full()) {
                ERROR("Write failure: request " << block_request->DebugString() <<
                    ", active requests " << this->stats_.active_session_count_ << ", reason: full");
            } else {
                ERROR("Write failure: request " << block_request->DebugString() <<
                    ", active requests " << this->stats_.active_session_count_);
            }
            this->stats_.processsed_session_count_.fetch_and_decrement();
            return false;
        }
        this->stats_.processsed_session_count_.fetch_and_decrement();
        // the write lock is unlocked at other place.
    }

    request_stats.Finish(RequestStatistics::TOTAL);
    if (request_stats.latency(RequestStatistics::TOTAL) > 1000.0) {
        if (report_long_running_requests_) {
            DEBUG("Long running request: " << block_request->DebugString() <<
                ", stats " << request_stats.DebugString());
        }
        stats_.long_running_request_count_++;
    }
    if (!this->idle_detector_.OnRequestEnd(
            block_request->request_type(),
            block_request->block_id(),
            block_request->offset(),
            block_request->size(),
            request_stats.latency(RequestStatistics::TOTAL))) {
        WARNING("Idle detection data update failed: " << block_request->DebugString());
    }
    return true;
}

ScsiResult DedupSystem::MakeRequest(
    Session* session,
    enum request_type rw,
    uint64_t request_index,
    uint64_t request_offset,
    uint64_t size,
    byte* buffer,
    ErrorContext* ec) {
    DCHECK_RETURN(this->state_ == RUNNING,
        ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_NOT_READY, 0x04, 0x00),
        "System not running");
    DCHECK_RETURN(rw == REQUEST_READ || rw == REQUEST_WRITE,
        ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00),
        "Illegal rw value");
    DCHECK_RETURN(buffer,
        ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00),
        "Buffer not set");
    DCHECK_RETURN(session,
        ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00),
        "Session not set");

    // first request
    ScsiResult r = DoMakeRequest(session, rw, request_index, request_offset, size, buffer, ec);
    if (!r && (ec == NULL || !ec->is_fatal())) {
        sched_yield(); // give some cpu away. We will sleep some time before later retries

        // retry if the request failed, and the error was not fatal
        uint32_t retry_count = (rw == REQUEST_READ ? read_retry_count_ : write_retry_count_);
        uint32_t sleep_time = 100000; // 0.1 of a second
        for (int i = 0; i < retry_count; i++, sleep_time *= 2) {
            // retries

            if (rw == REQUEST_READ) {
                WARNING("Retry Read: index " << request_index << ", offset " << request_offset << ", size " << size << ", retry count " << (i + 1));
            } else {
                WARNING("Retry Write: index " << request_index << ", offset " <<  request_offset << ", size " << size << ", retry count " << (i + 1));
            }

            r = DoMakeRequest(session, rw, request_index, request_offset, size, buffer, ec);
            if (r) {
                // here we have to correct the return code

                if (rw == REQUEST_READ) {
                    // recovered with retries
                    r = ScsiResult(SCSI_OK, SCSI_KEY_RECOVERD, 0x17, 0x01);
                } else {
                    // Write Error Recovered With Auto-Reallocation
                    // Well, this is not exactly the correct response code, but
                    // the nearest I could find
                    r = ScsiResult(SCSI_OK, SCSI_KEY_RECOVERD, 0x0C, 0x01);
                }
                break;
            }
            usleep(sleep_time);
        }
    }
    if (session->open_request_count() > 0) {
        WARNING("Session has open requests: " << session->open_request_count());
    }
    return r;
}

Option<bool> DedupSystem::Throttle(int thread_id, int thread_count) {
    if (unlikely(state_ != RUNNING)) {
        // we do not throttle when we are not running.
        // returning an error is an alternative here.
        return make_option(false);
    }
    bool throttled = false;

    Option<bool> b = this->log_->Throttle(thread_id, thread_count);
    if (unlikely(!b.valid())) {
        WARNING("Log throttle failed");
    } else if (unlikely(b.value())) {
        throttled = true;
    }

    b = this->block_index_->Throttle(thread_id, thread_count);
    if (unlikely(!b.valid())) {
        WARNING("Block index throttle failed");
    } else if (unlikely(b.value())) {
        throttled = true;
    }

    b = this->chunk_index_->Throttle(thread_id, thread_count);
    if (unlikely(!b.valid())) {
        WARNING("Chunk index throttle failed");
    } else if (unlikely(b.value())) {
        throttled = true;
    }
    return make_option(throttled);
}

ScsiResult DedupSystem::DoMakeRequest(
    Session* sess,
    enum request_type rw,
    uint64_t request_index,
    uint64_t request_offset,
    uint64_t size,
    byte* buffer,
    ErrorContext* ec) {

    ProfileTimer timer(this->stats_.profiling_total_);

    DCHECK_RETURN(this->state_ == RUNNING,
        ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_NOT_READY, 0x04, 0x00),
        "System not running");
    DCHECK_RETURN(rw == REQUEST_READ || rw == REQUEST_WRITE,
        ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00),
        "Illegal rw value");
    DCHECK_RETURN(buffer,
        ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00),
        "Buffer not set");
    DCHECK_RETURN(sess,
        ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00),
        "Session not set");

    DCHECK_RETURN(sess->open_request_count() == 0,
        (rw == REQUEST_READ ? ScsiResult::kReadError : ScsiResult::kWriteError),
        "Non cleared session: " <<
        "active session " << this->stats_.active_session_count_);

    if (rw == REQUEST_READ) {
        DEBUG("Read: index " << request_index << ", offset " << request_offset << ", size " << size);
    } else {
        DEBUG("Write: index " << request_index << ", offset " <<  request_offset << ", size " << size);
    }
    CHECK_RETURN(sess->Lock(), (rw == REQUEST_READ ? ScsiResult::kReadError : ScsiResult::kWriteError), "Failed to lock session");

    uint64_t idx = request_index;
    uint32_t request_size = 0;
    bool failed = false;
    while (size > 0) {
        if (request_offset + size > block_size_) {
            request_size = block_size_ - request_offset;
        } else {
            request_size = size;
        }
        Request block_request(rw, idx, request_offset, request_size, buffer, block_size_);

        DEBUG("Request " << block_request.DebugString());

        if (!this->MakeBlockRequest(sess, &block_request, (size == request_size), ec)) {
            failed = true;
            break;
        }

        request_offset = 0;
        buffer += request_size;
        size -= request_size;
        idx++;
    }
    if (failed) {
        for (int i = 0; i < sess->open_request_count(); i++) {
            OpenRequest* request = sess->GetRequest(i);
            if (request) {
                if (!this->block_locks_.WriteUnlock(request->block_id(), LOCK_LOCATION_INFO)) {
                    WARNING("Failed to unlock block lock " << request->DebugString());
                }
            }
        }
        if (!sess->Clear()) {
            WARNING("Failed to clear session");
        }
    }

    if (!sess->Unlock()) {
        WARNING("Failed to unlock session");
    }

    CHECK_RETURN(sess->open_request_count() == 0,
        (rw == REQUEST_READ ? ScsiResult::kReadError : ScsiResult::kWriteError),
        "Illegal session: " << sess->open_request_count() << " open requests");

    if (failed) {
        if (ec) {
            if (rw == REQUEST_WRITE && ec->is_full()) {
                return kFullError;
            }
            if (rw == REQUEST_READ && ec->has_checksum_error()) {
                return kReadChecksumError;
            }
        }
        return rw == REQUEST_READ ? ScsiResult::kReadError : ScsiResult::kWriteError;
    }

    return ScsiResult::kOk;
}

bool DedupSystem::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    DCHECK(ps, "Persistent statistics not set");
    if (gc_) {
        CHECK(gc_->PersistStatistics(prefix + ".gc", ps), "Failed to persist gc stats");
    }
    if (log_) {
        CHECK(log_->PersistStatistics(prefix + ".log", ps), "Failed to persist log stats");
    }
    if (block_index_) {
        CHECK(block_index_->PersistStatistics(prefix + ".block-index", ps),
            "Failed to persist block index stats");
    }
    if (chunk_index_) {
        CHECK(chunk_index_->PersistStatistics(prefix + ".chunk-index", ps),
            "Failed to persist chunk index stats");
    }
    if (chunk_store_) {
        CHECK(chunk_store_->PersistStatistics(prefix + ".chunk-store", ps),
            "Failed to persist chunk store stats");
    }
    if (filter_chain_) {
        CHECK(filter_chain_->PersistStatistics(prefix + ".filter-chain", ps),
            "Failed to persist filter chain stats");
    }
    if (volume_info_) {
        CHECK(volume_info_->PersistStatistics(prefix + ".volumes", ps),
            "Failed to persist volume stats");
    }
    if (content_storage_) {
        CHECK(content_storage_->PersistStatistics(prefix + ".content-storage", ps),
            "Failed to persist content storage stats");
    }
    CHECK(idle_detector_.PersistStatistics(prefix + ".idle", ps),
        "Failed to persist idle detector stats");
    return true;
}

bool DedupSystem::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    DCHECK(ps, "Persistent statistics not set");
    if (gc_) {
        CHECK(gc_->RestoreStatistics(prefix + ".gc", ps), "Failed to restore gc stats");
    }
    if (log_) {
        CHECK(log_->RestoreStatistics(prefix + ".log", ps), "Failed to restore log stats");
    }
    if (block_index_) {
        CHECK(block_index_->RestoreStatistics(prefix + ".block-index", ps),
            "Failed to restore block index stats");
    }
    if (chunk_index_) {
        CHECK(chunk_index_->RestoreStatistics(prefix + ".chunk-index", ps),
            "Failed to restore chunk index stats");
    }
    if (chunk_store_) {
        CHECK(chunk_store_->RestoreStatistics(prefix + ".chunk-store", ps),
            "Failed to restore chunk store stats");
    }
    if (filter_chain_) {
        CHECK(filter_chain_->RestoreStatistics(prefix + ".filter-chain", ps),
            "Failed to restore filter chain stats");
    }
    if (volume_info_) {
        CHECK(volume_info_->RestoreStatistics(prefix + ".volumes", ps),
            "Failed to restore volume stats");
    }
    if (content_storage_) {
        CHECK(content_storage_->RestoreStatistics(prefix + ".content-storage", ps),
            "Failed to restore content storage stats");
    }

    CHECK(idle_detector_.RestoreStatistics(prefix + ".idle", ps),
        "Failed to restore idle detector stats");

    return true;
}

string DedupSystem::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"block locks\": " << this->block_locks_.PrintLockStatistics() << "," << std::endl;

    sstr << "\"gc\": " << (this->gc_ ? this->gc_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"idle\": " << this->idle_detector_.PrintLockStatistics() << "," << std::endl;
    sstr << "\"log\": " << (log_ ? this->log_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"block index\": " << (block_index_ ? this->block_index_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"chunk index\": " << (chunk_index_ ? this->chunk_index_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"chunk store\": " << (chunk_store_ ? this->chunk_store_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"filter chain\": " << (filter_chain_ ? this->filter_chain_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"volumes\": " << (volume_info_ ? this->volume_info_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"content storage\": " << (content_storage_ ? this->content_storage_->PrintLockStatistics() : "null")  << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

string DedupSystem::PrintStatistics() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{" << std::endl;
    sstr << "\"currently processed requests\": " << this->stats_.processsed_session_count_ << "," << std::endl;
    sstr << "\"active requests\": " << this->stats_.active_session_count_ << "," << std::endl;

    if (block_index_ && block_index_->persistent_block_index() && storage()) {
        uint64_t logical_active_data = this->block_index_->GetActiveBlockCount() * block_size();
        uint64_t phy_active_data = this->storage()->GetActiveStorageDataSize();

        if (logical_active_data > 0) {
            double compression_rate = 1.0 - (1.0 * phy_active_data / logical_active_data);
            sstr << "\"compression rate\": " << compression_rate << "," << std::endl;
        }
        sstr << "\"logical data size\": " << logical_active_data << "," << std::endl;
        sstr << "\"physical data size\": " << phy_active_data << "," << std::endl;
    }

    sstr << "\"gc\": " << (this->gc_ ? this->gc_->PrintStatistics() : "null") << "," << std::endl;
    sstr << "\"idle\": " << this->idle_detector_.PrintStatistics() << "," << std::endl;
    sstr << "\"log\": " << (log_ ? this->log_->PrintStatistics() : "null") << "," << std::endl;
    sstr << "\"block index\": " << (block_index_ ? this->block_index_->PrintStatistics() : "null") << "," << std::endl;
    sstr << "\"chunk index\": " << (chunk_index_ ? this->chunk_index_->PrintStatistics() : "null") << "," << std::endl;
    sstr << "\"chunk store\": " << (chunk_store_ ? this->chunk_store_->PrintStatistics() : "null") << "," << std::endl;
    sstr << "\"filter chain\": " << (filter_chain_ ? this->filter_chain_->PrintStatistics() : "null") << "," << std::endl;
    sstr << "\"volumes\": " << (volume_info_ ? this->volume_info_->PrintStatistics() : "null") << "," << std::endl;
    sstr << "\"content storage\": " << (content_storage_ ? this->content_storage_->PrintStatistics() : "\null") << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

string DedupSystem::PrintTrace() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"long running request count\": " << this->stats_.long_running_request_count_ << "," << std::endl;
    sstr << "\"gc\": " << (this->gc_ ? this->gc_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"idle\": " << this->idle_detector_.PrintTrace() << "," << std::endl;
    sstr << "\"log\": " << (log_ ? this->log_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"block locks\": " << block_locks_.PrintTrace() << ", " << std::endl;
    sstr << "\"block index\": " << (block_index_ ? this->block_index_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"chunk index\": " << (chunk_index_ ? this->chunk_index_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"chunk store\": " << (chunk_store_ ? this->chunk_store_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"filter chain\": " << (filter_chain_ ? this->filter_chain_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"volumes\": " << (volume_info_ ? this->volume_info_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"content storage\": " << (content_storage_ ? this->content_storage_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"threadpool\": " << (tp_ ? this->tp_->PrintTrace() : "null") << "" << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

string DedupSystem::PrintProfile() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"log\": " << (log_ ? this->log_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"block locks\": " << this->block_locks_.PrintProfile() << "," << std::endl;
    sstr << "\"average waiting latency\": " << this->stats_.average_waiting_time_.GetAverage() << "," << std::endl;
    sstr << "\"gc\": " << (this->gc_ ? this->gc_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"idle\": " << this->idle_detector_.PrintProfile() << "," << std::endl;
    sstr << "\"block index\": " <<
    (block_index_ ? this->block_index_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"chunk index\": " <<
    (chunk_index_ ? this->chunk_index_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"chunk store\": " <<
    (chunk_store_ ? this->chunk_store_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"filter chain\": " << (filter_chain_ ? this->filter_chain_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"volumes\": " << (volume_info_ ? this->volume_info_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"content storage\": " <<
    (content_storage_ ? this->content_storage_->PrintProfile() : "null")  << "," << std::endl;
    sstr << "\"threadpool\": " << (tp_ ? this->tp_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"base dedup system\":" << this->stats_.profiling_total_.GetSum() << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

ChunkIndex* DedupSystem::chunk_index() {
    return this->chunk_index_;
}

dedupv1::blockindex::BlockIndex* DedupSystem::block_index() {
    return this->block_index_;
}

dedupv1::chunkstore::ChunkStore* DedupSystem::chunk_store() {
    return this->chunk_store_;
}

dedupv1::chunkstore::Storage* DedupSystem::storage() {
    if (this->chunk_store_ == NULL) {
        return NULL;
    }
    return this->chunk_store_->storage();
}

FilterChain* DedupSystem::filter_chain() {
    return this->filter_chain_;
}

DedupVolumeInfo* DedupSystem::volume_info() {
    return this->volume_info_;
}

ContentStorage* DedupSystem::content_storage() {
    return this->content_storage_;
}

Log* DedupSystem::log() {
    return this->log_;
}

IdleDetector* DedupSystem::idle_detector() {
    return &this->idle_detector_;
}

uint32_t DedupSystem::block_size() const {
    return this->block_size_;
}

void DedupSystem::RegisterDefaults() {
    dedupv1::base::RegisterDefaults();

    dedupv1::chunkstore::ContainerStorage::RegisterStorage();

    dedupv1::filter::ChunkIndexFilter::RegisterFilter();
    dedupv1::filter::SparseChunkIndexFilter::RegisterFilter();
    dedupv1::filter::BlockIndexFilter::RegisterFilter();
    dedupv1::filter::ByteCompareFilter::RegisterFilter();
    dedupv1::filter::BloomFilter::RegisterFilter();
    dedupv1::filter::ZeroChunkFilter::RegisterFilter();

    dedupv1::StaticChunker::RegisterChunker();
    dedupv1::RabinChunker::RegisterChunker();

    dedupv1::CryptoFingerprinter::RegisterFingerprinter();

    dedupv1::gc::UsageCountGarbageCollector::RegisterGC();
    dedupv1::gc::NoneGarbageCollector::RegisterGC();

    dedupv1::chunkstore::GreedyContainerGCStrategy::RegisterGC();

    dedupv1::chunkstore::MemoryBitmapContainerStorageAllocator::RegisterAllocator();

    dedupv1::chunkstore::RoundRobinContainerStorageWriteCacheStrategy::RegisterWriteCacheStrategy();
    dedupv1::chunkstore::EarliestFreeContainerStorageWriteCacheStrategy::RegisterWriteCacheStrategy();

    dedupv1::chunkindex::ChunkIndex::RegisterChunkIndex();
}

dedupv1::InfoStore* DedupSystem::info_store() {
    return info_store_;
}

dedupv1::base::ResourceManagement<Chunk>* DedupSystem::chunk_management() {
    return chunk_management_;
}

dedupv1::BlockLocks* DedupSystem::block_locks() {
    return &this->block_locks_;
}

dedupv1::gc::GarbageCollector* DedupSystem::garbage_collector() {
    return this->gc_;
}

#ifdef DEDUPV1_CORE_TEST
void DedupSystem::ClearData() {
    idle_detector_.ClearData();
    if (gc_ != NULL) {
        gc_->ClearData();
    }
    if (dynamic_cast<dedupv1::chunkstore::ContainerStorage*>(storage())) {
        dedupv1::chunkstore::ContainerStorage* cs =
            dynamic_cast<dedupv1::chunkstore::ContainerStorage*>(storage());
        cs->ClearData();
    }
    block_index_->ClearData();
    chunk_index_->ClearData();
    log_->ClearData();
}
#endif

}
