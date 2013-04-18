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
#include <math.h>
#include <time.h>

#include <set>
#include <string>
#include <sstream>

#include <tr1/memory>
#include <tbb/tick_count.h>

#include "dedupv1_stats.pb.h"

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <core/chunker.h>
#include <core/chunk_mapping.h>
#include <core/block_mapping.h>
#include <core/chunk_store.h>
#include <base/bitutil.h>
#include <core/block_index.h>
#include <core/chunk_index.h>
#include <base/hashing_util.h>
#include <base/strutil.h>
#include <base/timer.h>
#include <core/filter_chain.h>
#include <base/resource_management.h>
#include <base/logging.h>
#include <core/open_request.h>
#include <core/session.h>
#include <core/chunk.h>
#include <base/crc32.h>
#include <core/content_storage.h>
#include <base/memory.h>
#include <core/storage.h>
#include <base/fault_injection.h>
#include <base/option.h>
#include <base/future.h>
#include <base/runnable.h>

using std::set;
using std::list;
using std::vector;
using std::string;
using std::stringstream;
using std::tr1::tuple;
using std::tr1::make_tuple;
using dedupv1::base::Future;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using std::tr1::shared_ptr;
using dedupv1::base::crc;
using dedupv1::base::Barrier;
using dedupv1::BlockLocks;
using dedupv1::base::ProfileTimer;
using dedupv1::base::SlidingAverageProfileTimer;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::blockindex::BlockMapping;
using dedupv1::chunkstore::ChunkStore;
using dedupv1::chunkstore::Storage;
using dedupv1::base::ScopedArray;
using dedupv1::base::ResourceManagement;
using dedupv1::filter::FilterChain;
using dedupv1::base::CRC;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::log::Log;
using dedupv1::scsi::ScsiResult;
using dedupv1::base::ErrorContext;
using dedupv1::base::Option;
using dedupv1::blockindex::BlockIndex;
using dedupv1::base::raw_compare;
using dedupv1::base::Runnable;
using dedupv1::base::NewRunnable;
using dedupv1::filter::Filter;
using dedupv1::base::Threadpool;
using dedupv1::base::MultiSignalCondition;

LOGGER("ContentStorage");

namespace dedupv1 {

ContentStorage::ContentStorage() {
    block_index_ = NULL;
    chunk_index_ = NULL;
    chunk_store_ = NULL;
    block_locks_ = NULL;
    chunk_management_ = NULL;
    filter_chain_ = NULL;
    tp_ = NULL;
    parallel_filter_chain_ = true;

    log_ = 0;
    fingerprinter_name_ = "sha1";
    default_chunker_ = NULL;

    reported_full_block_index_before_ = false;
    reported_full_chunk_index_before_ = false;
    reported_full_storage_before_ = false;
}

ContentStorage::~ContentStorage() {
}

ContentStorage::Statistics::Statistics() :
    average_write_block_latency_(256),
    average_processing_time_(256),
    average_filter_chain_time_(256),
    average_chunking_latency_(256),
    average_fingerprint_latency_(256),
    average_chunk_store_latency_(256),
    average_block_read_latency_(256),
    average_sync_latency_(256),
    average_open_request_handling_latency_(256),
    average_block_storing_latency_(256),
    average_process_chunk_filter_chain_latency_(256),
    average_process_filter_chain_barrier_wait_latency_(256),
    average_process_chunk_filter_chain_read_chunk_info_latency_(256),
    average_process_chunk_filter_chain_write_block_latency_(256),
    average_process_chunk_filter_chain_store_chunk_info_latency_(256) {
    reads_ = 0;
    read_size_ = 0;
    writes_ = 0;
    write_size_ = 0;
    sync_ = 0;
    threads_in_filter_chain_ = 0;
}

bool ContentStorage::InitEmptyFingerprint(Chunker* chunker,
    Fingerprinter* fp_gen,
    bytestring* empty_fp) {
    DCHECK(chunker, "Chunker not set");
    DCHECK(empty_fp, "Empty fp not set");
    DCHECK(fp_gen, "FP generator not set");

    bytestring buffer;
    buffer.resize(chunker->GetMaxChunkSize(), 0);

    byte fp[Fingerprinter::kMaxFingerprintSize];
    size_t fp_size = Fingerprinter::kMaxFingerprintSize;
    bool failed = false;
    if (!fp_gen->Fingerprint(buffer.data(), buffer.size(), fp, &fp_size)) {
        ERROR("Failed to fingerprint an empty chunk");
        failed = true;
    } else {
        empty_fp->assign(fp, fp_size);
    }
    return !failed;
}

bool ContentStorage::Start(dedupv1::base::Threadpool* tp,
    dedupv1::blockindex::BlockIndex* block_index,
    dedupv1::chunkindex::ChunkIndex* chunk_index,
    ChunkStore* chunk_store,
    FilterChain* filter_chain,
    ResourceManagement<Chunk>* chunk_management,
    Log* log,
    BlockLocks* block_locks,
    uint32_t block_size) {
    DCHECK(tp, "Threadpool not set");

    this->tp_ = tp;
    this->block_index_ = block_index;
    this->chunk_index_ = chunk_index;
    this->filter_chain_ = filter_chain;
    this->chunk_store_ = chunk_store;
    this->chunk_management_ = chunk_management;
    this->log_ = log;
    this->block_locks_ = block_locks;
    this->block_size_ = block_size;

    if (!default_chunker_) {
        DEBUG("Create default chunker");
        this->default_chunker_ = Chunker::Factory().Create("rabin");
        CHECK(default_chunker_, "Failed to create default chunker");
    }
    CHECK(this->default_chunker_->Start(chunk_management),
        "Cannot start chunker");

    return true;
}

bool ContentStorage::Close() {
    DEBUG("Close content storage");
    this->fingerprinter_name_.clear();

    bool failed = false;
    if (default_chunker_) {
        if (!default_chunker_->Close()) {
            ERROR("Failed to close default chunker");
            failed = true;
        }
        default_chunker_ = NULL;
    }

    delete this;
    return !failed;
}

Session* ContentStorage::CreateSession(Chunker* chunker,
    const std::set<std::string>* enabled_filter_names) {
    ProfileTimer timer(this->stats_.profiling_);
    CHECK_RETURN(log_, NULL, "Content storage not started");
    CHECK_RETURN(filter_chain_, NULL, "Filter chain not set");
    CHECK_RETURN(this->fingerprinter_name_.size() > 0, NULL, "Fingerprinter not configured");

    Fingerprinter* fingerprint = Fingerprinter::Factory().Create(this->fingerprinter_name_);
    CHECK_RETURN(fingerprint, NULL, "Error creating fingerprint context");

    Session* session = new Session();
    if (!session) {
        ERROR("Failed to create a session");
        if (!fingerprint->Close()) {
            WARNING("Failed to close fingerprint");
        }
        return NULL;
    }
    std::set<const Filter*> filters;
    if (enabled_filter_names && enabled_filter_names->size() > 0) {
        for (set<string>::const_iterator i = enabled_filter_names->begin(); i != enabled_filter_names->end(); ++i) {
            Filter* filter = filter_chain_->GetFilterByName(*i);
            CHECK_RETURN(filter, NULL, "Filter not configured: " << *i);
            filters.insert(filter);
        }
    } else {
        // use defaults
        list<Filter*> all_filters = filter_chain_->GetChain();
        for (list<Filter*>::const_iterator i = all_filters.begin(); i != all_filters.end(); ++i) {
            Filter* filter = *i;
            if (filter->is_enabled_by_default()) {
                filters.insert(filter);
            }
        }
    }
    Chunker* chunker_to_use = chunker;
    if (chunker_to_use == NULL) {
        chunker_to_use = default_chunker_;
    }

    if (!session->Init(block_size_, this->chunk_store_, chunker_to_use, fingerprint, filters)) {
        ERROR("Error initing session: chunker " << (chunker_to_use == chunker ? "volume" : "default"));
        if (session->fingerprinter() == NULL) {
            // Init failed before fingerprinter was assigned, we still have to ownership
            if (!fingerprint->Close()) {
                WARNING("Failed to close fingerprint");
            }
        }
        session->Close();
        return NULL;
    }
    return session;
}

bool ContentStorage::SetOption(const string& option_name, const string& option) {
    CHECK(option_name.size() > 0, "Option name not set");
    CHECK(option.size() > 0, "Option not set");

    if (option_name == "fingerprinting") {
        Fingerprinter* fingerprint = NULL;
        this->fingerprinter_name_ = option;
        fingerprint = Fingerprinter::Factory().Create(this->fingerprinter_name_);
        CHECK(fingerprint, "Invalid fingerprinter");
        fingerprint->Close();
        return true;
    }
    if (option_name == "filter-chain.parallel") {
        CHECK(To<bool>(option).valid(), "Illegal option");
        this->parallel_filter_chain_ = To<bool>(option).value();
        return true;
    }
    if (option_name == "chunking") {
        CHECK(!this->default_chunker_, "Chunker already set");
        this->default_chunker_ = Chunker::Factory().Create(option);
        CHECK(this->default_chunker_, "Chunker Creation failed");
        return true;
    }
    if (StartsWith(option_name, "chunking.")) {
        if (!this->default_chunker_) {
            this->default_chunker_ = Chunker::Factory().Create("rabin");
        }
        CHECK(this->default_chunker_, "Chunker not set");
        CHECK(this->default_chunker_->SetOption(option_name.substr(strlen("chunking.")), option), "Config failed");
        return true;
    }
    ERROR("Illegal content storage option: " << option_name);
    return false;
}

bool ContentStorage::ReadBlock(Session* session,
    Request* request,
    RequestStatistics* request_stats,
    dedupv1::base::ErrorContext* ec) {
    ProfileTimer timer(this->stats_.profiling_);

    DCHECK(session, "Session not set");
    DCHECK(request, "Request not set");
    DCHECK(request->IsValid(), "Request not valid: " << request->DebugString());

    unsigned int data_pos = 0; // Current position in the data
    int count = 0; // Number of bytes already read
    byte* data_buffer = static_cast<byte*>(request->buffer()); // data as byte array

    DEBUG("Read block: request " << request->DebugString());

    BlockMapping mapping(request->block_id(), block_size_);
    CHECK(this->block_index_->ReadBlockInfo(session, &mapping, ec),
        "Reading block info failed");
    DCHECK(mapping.item_count() != 0,
        "Illegal block mapping: " << mapping.DebugString());

    // Append the chunks/mapping items to each other.
    // data_pos denotes the current position
    int num = 0;
    uint32_t todo_size = request->size();
    uint64_t offset = request->offset();
    for (list<BlockMappingItem>::iterator i = mapping.items().begin(); i != mapping.items().end() && todo_size > 0; i++) {
        num++;
        BlockMappingItem* item = &(*i);
        if (i->size() <= offset) {
            offset -= i->size();
            continue;
        }

        count = todo_size;
        if (offset + count > i->size()) {
            count = i->size() - offset;
        }

        CHECK(ContentStorage::ReadDataForItem(
                item, session, data_buffer,
                data_pos, count, offset, ec),
            "Read chunk failed: chunk " << item->DebugString() <<
            ", block " << mapping.DebugString() <<
            ", request " << request->DebugString());

        todo_size -= count;
        offset = 0;
        data_pos += count;
    }

    IF_TRACE() {
        TRACE("Read block: request " << request->DebugString() <<
            ", crc " << crc(request->buffer(), request->size()));
    }
    stats_.reads_++;
    stats_.read_size_ += request->size();
    // now append un-chunked data
    return true;
}

bool ContentStorage::FastCopyBlock(uint64_t src_block_id,
    uint64_t src_offset,
    uint64_t target_block_id,
    uint64_t target_offset,
    uint64_t size,
    dedupv1::base::ErrorContext* ec) {

    DEBUG("Fast copy block: src block " << src_block_id <<
        ", target block " << target_block_id <<
        ", src offset " << src_offset <<
        ", target offset " << target_offset <<
        ", size " << size);

    BlockMapping updated_block_mapping(this->block_size_);
    BlockMapping original_block_mapping(target_block_id, block_size_);

    BlockMapping src_block_mapping(src_block_id, block_size_);

    BlockIndex::read_result rr1 = this->block_index_->ReadBlockInfo(NULL, &src_block_mapping, ec);
    CHECK(rr1 != BlockIndex::READ_RESULT_ERROR, "Failed to read source mapping");
    if (rr1 == BlockIndex::READ_RESULT_NOT_FOUND) {
        TRACE("Fast copy skipped: src block " << src_block_id << " not found");
        // no entry => not written yet => we are done
        return true;
    }

    BlockIndex::read_result rr2 = this->block_index_->ReadBlockInfo(NULL, &original_block_mapping, ec);
    CHECK(rr2 != BlockIndex::READ_RESULT_ERROR, "Failed to read target original mapping");

    CHECK(updated_block_mapping.CopyFrom(original_block_mapping),
        "Failed to copy block mapping. " << original_block_mapping.DebugString());
    updated_block_mapping.set_version(updated_block_mapping.version() + 1);
    updated_block_mapping.set_event_log_id(0);

    DEBUG("Fast copy: src " << src_block_mapping.DebugString() <<
        ", target " << updated_block_mapping.DebugString() <<
        ", src offset " << src_offset <<
        ", target offset " << target_offset <<
        ", size " << size);

    // slice here from the existing (source) block mapping
    // there is no need to run through the filter chain. In some way this can be seen
    // as an block index filter against a different block

    CHECK(updated_block_mapping.MergePartsFrom(src_block_mapping, target_offset, src_offset, size),
        "Failed to merge");

    TRACE("Finished merge");

    if (!this->block_index_->StoreBlock(original_block_mapping, updated_block_mapping, ec)) {
        ERROR("Storing of block index failed: " << original_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString());

        // Here MarkBlockWriteAsFailed is not necessary, but I do it anyway
        if (!this->block_index_->MarkBlockWriteAsFailed(original_block_mapping, updated_block_mapping, false, ec)) {
            WARNING("Failed to mark block write as failed: " << original_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString())
        }
        return false;
    }
    TRACE("Finished store fast-copied block");
    return true;
}

bool ContentStorage::WriteBlock(Session* session, Request* request, RequestStatistics* request_stats, bool last_block,
                                dedupv1::base::ErrorContext* ec) {
    ProfileTimer timer(this->stats_.profiling_);
    SlidingAverageProfileTimer average_timer(this->stats_.average_write_block_latency_);
    REQUEST_STATS_START(request_stats, RequestStatistics::PROCESSING);

    // tbb::tick_count end_tick;
    tbb::tick_count block_read_start_tick;
    DCHECK(session, "Session not set");
    DCHECK(request, "Request not set");
    // request_stats may be NULL
    DCHECK(request->IsValid(), "Illegal request: " << request->DebugString());

    IF_TRACE() {
        TRACE("Write block: request " << request->DebugString() <<
            ", crc " << crc(request->buffer(), request->size()) <<
            ", last " << ToString(last_block));
    } else {
        DEBUG("Write block: request " << request->DebugString() <<
            ", last " << ToString(last_block));
    }

    block_read_start_tick = tbb::tick_count::now();
    BlockMapping updated_block_mapping(this->block_size_);
    BlockMapping original_block_mapping(request->block_id(), request->block_size());

    CHECK(this->block_index_->ReadBlockInfo(session, &original_block_mapping, ec), "Reading block mapping failed");

    CHECK(updated_block_mapping.CopyFrom(original_block_mapping),
        "Failed to copy block mapping. " << original_block_mapping.DebugString());
    updated_block_mapping.set_version(updated_block_mapping.version() + 1);
    updated_block_mapping.set_event_log_id(0);
    this->stats_.average_block_read_latency_.Add((tbb::tick_count::now() - block_read_start_tick).seconds() * 1000);

    bool result = true;
    if (unlikely(this->block_index_->CheckIfFullWith(original_block_mapping, updated_block_mapping))) {
        bool old_state = reported_full_block_index_before_.compare_and_swap(true, false);
        if (!old_state) {
            WARNING("Block index full: " <<
                original_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString() <<
                ", state: not processed" <<
                ", stats " << block_index_->PrintStatistics());
        } else {
            DEBUG("Block index full: " <<
                original_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString() <<
                ", state: not processed" <<
                ", block index item count " << block_index_->GetActiveBlockCount());
        }
        if (ec) {
            ec->set_full();
        }
        result = false;
    } else if (reported_full_block_index_before_) {
        // reset the reported state
        reported_full_block_index_before_ = false;
    }
    if (unlikely(result && this->chunk_store_->CheckIfFull())) {
        bool old_state = reported_full_storage_before_.compare_and_swap(true, false);
        if (!old_state) {
            WARNING("Chunk store full: " <<
                original_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString() <<
                ", state: not processed" <<
                ", stats " << chunk_store_->PrintStatistics());
        } else {
            DEBUG("Chunk store full: " <<
                original_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString() <<
                ", state: not processed" <<
                ", stats " << chunk_store_->PrintStatistics());
        }
        if (ec) {
            ec->set_full();
        }
        result = false;
    } else if (reported_full_storage_before_) {
        // reset the reported state
        reported_full_storage_before_ = false;
    }

    // while it is in a strict sense is possible to complete the write with any new chunks
    // it is better to exit early
    // Note: This is only the first line of defense. It only "fires" when at this point in time
    // the chunk index is already full.
    if (unlikely(result && !this->chunk_index_->IsAcceptingNewChunks())) {
        bool old_state = reported_full_chunk_index_before_.compare_and_swap(true, false);
        if (!old_state) {
            WARNING("Chunk index full: " <<
                original_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString() <<
                ", state: not processed" <<
                ", stats " << chunk_index_->PrintStatistics());
        } else {
            DEBUG("Chunk index full: " <<
                original_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString() <<
                ", state: not processed" <<
                ", stats " << chunk_index_->PrintStatistics());
        }
        if (ec) {
            ec->set_full();
        }
        result = false;
    } else if (reported_full_chunk_index_before_) {
        // reset the reported state
        reported_full_chunk_index_before_ = false;
    }

    if (result) {
        REQUEST_STATS_START(request_stats, RequestStatistics::CHECKSUM);
        ProfileTimer checksum_timer(this->stats_.checksum_time_);
        REQUEST_STATS_FINISH(request_stats, RequestStatistics::CHECKSUM);
    }
    list<Chunk*> chunks;
    if (result) {
        // Chunk everything
        ProfileTimer chunker_timer(stats_.chunking_time_);
        REQUEST_STATS_START(request_stats, RequestStatistics::CHUNKING);
        FAULT_POINT("content-storage.write.pre-chunking");
        if (!session->chunker_session()->ChunkData(request->buffer(), request->offset(), request->size(), last_block,
                &chunks)) {
            ERROR("Block chunking failed");
            result = false;
        }
        REQUEST_STATS_FINISH(request_stats, RequestStatistics::CHUNKING);
        if (request_stats) {
            this->stats_.average_chunking_latency_.Add(request_stats->latency(RequestStatistics::CHUNKING));
        }
    }
    if (result) {
        if (chunks.size() == 0) {
            // No chunk finished in this block => Append the block mapping to the open request queue
            BlockMappingItem mapping_item(session->open_chunk_position(), request->size());
            session->set_open_chunk_position(session->open_chunk_position() + request->size());
            if (!session->AppendBlock(original_block_mapping, updated_block_mapping)) {
                ERROR("Cannot append block to session: " << original_block_mapping.DebugString());
                result = false;
            } else if (!session->AppendRequest(request->block_id(), request->offset(), mapping_item)) {
                ERROR("Cannot append block request to session: " <<
                    ", request " << request->DebugString() <<
                    ", open request " << session->GetRequest(session->open_request_count() - 1)->DebugString() <<
                    ", block mapping item " << mapping_item.DebugString());
                result = false;
            }
            // do not unlock block lock, because block_id is in the open chunk list
        } else {
            // Chunks are finished in this block => Handle chunks in separate function
            result = this->HandleChunks(session, request, request_stats, &original_block_mapping,
                &updated_block_mapping, chunks, ec);
            if (!result) {
                ERROR("Failed to write block: " << request->DebugString() <<
                    ", block mapping " <<
                    original_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString());

                /*
                 * TODO (dmeister) The error handling seems be in a bad shape. It is not clear in which situations
                 * we are handling in this state. But is seems to be safe to write two failed events per block (at
                 * least) when the written_event_commit is set to false).
                 */
                if (!block_index_->MarkBlockWriteAsFailed(original_block_mapping, updated_block_mapping, false, ec)) {
                    WARNING("Failed to mark block write as failed: " <<
                        original_block_mapping.DebugString() << " => " << updated_block_mapping.DebugString());
                }
            }
        }
    }

    list<Chunk*>::const_iterator ci;
    for (ci = chunks.begin(); ci != chunks.end(); ci++) {
        if (!this->chunk_management_->Release(*ci)) {
            WARNING("Cannot release chunk");
        }
    }
    chunks.clear();

    REQUEST_STATS_FINISH(request_stats, RequestStatistics::PROCESSING);
    if (request_stats) {
        this->stats_.average_processing_time_.Add(request_stats->latency(RequestStatistics::PROCESSING));
        DEBUG("Write finished: request " << request->DebugString() <<
            ", latency " << request_stats->latency(RequestStatistics::PROCESSING) << "ms");
    } else {
        DEBUG("Write finished: request " << request->DebugString() <<
            ", latency null");
    }

    if (!result) {
        // Error Handling.

        // mark all open requests (that are not the current request) as failed
        // there is no way that a) these requests are ok because then they would have been removed from the list or b)
        // that we can "fix" them now.
        for (int i = 0; i < session->open_request_count(); i++) {
            OpenRequest* request = session->GetRequest(i);
            if (request) {
                if (!(request->modified_block_mapping().block_id() == updated_block_mapping.block_id()
                      && request->modified_block_mapping().version() == updated_block_mapping.version())) {
                    // the current block mapping should not be handled here
                    if (!block_index_->MarkBlockWriteAsFailed(request->original_block_mapping(),
                            request->modified_block_mapping(), false, ec)) {
                        WARNING("Failed to mark block write as failed: " <<
                            request->original_block_mapping().DebugString() << " => " << request->modified_block_mapping().DebugString());
                    }
                }
            }
        }

        bool unlock = true;
        if (session->open_request_count() > 0) {
            // the last open request is from this block
            OpenRequest* request = session->GetRequest(session->open_request_count() - 1);
            if (request->modified_block_mapping().block_id() == updated_block_mapping.block_id()
                && request->modified_block_mapping().version() == updated_block_mapping.version()) {
                unlock = false; // the current block mapping should not be unlocked here
            }
        }
        if (unlock) {
            if (!this->block_locks_->WriteUnlock(request->block_id(), LOCK_LOCATION_INFO)) {
                WARNING("Failed to unlock block lock " << request->block_id());
            }
        }
    }

    stats_.writes_++;
    stats_.write_size_ += request->size();
    return result;
}

bool ContentStorage::FingerprintChunks(Session* session,
    Request* request,
    RequestStatistics* request_stats,
    Fingerprinter* fingerprinter,
    const list<Chunk*>& chunks,
    vector<ChunkMapping>* chunk_mappings,
    ErrorContext* ec) {
    DCHECK(chunk_mappings, "Chunk mappings not set");
    DCHECK(fingerprinter, "Fingerprinter not set");
    DCHECK(chunk_mappings->size() == 0, "Chunk mapping not empty");

    REQUEST_STATS_START(request_stats, RequestStatistics::FINGERPRINTING);

    chunk_mappings->resize(chunks.size());
    ProfileTimer timer(this->stats_.fingerprint_profiling_);

    size_t fp_size;
    int i = 0;
    for (list<Chunk*>::const_iterator ci = chunks.begin(); ci != chunks.end(); ci++) {
        const Chunk* c = *ci;

        fp_size = Fingerprinter::kMaxFingerprintSize;
        CHECK(chunk_mappings->at(i).Init(c), "Cannot init chunk mapping");
        CHECK(fingerprinter->Fingerprint(c->data(),
                c->size(), chunk_mappings->at(i).mutable_fingerprint(), &fp_size),
            "Fingerprinting failed");
        chunk_mappings->at(i).set_fingerprint_size(fp_size);

        // here we rewrite the calculated fp of the empty chunk with the static empty fp.
        if (raw_compare(chunk_mappings->at(i).fingerprint(), chunk_mappings->at(i).fingerprint_size(),
                session->empty_fp().data(), session->empty_fp().size()) == 0) {
            // the fp is the empty fp

            CHECK(Fingerprinter::SetEmptyDataFingerprint(chunk_mappings->at(i).mutable_fingerprint(),
                    chunk_mappings->at(i).mutable_fingerprint_size()),
                "Failed to set the empty fingerprint");
        }
        i++;
    }
    // TODO(fermat): i is in any case chunks.size here, as we have not checked for duplicates before.
    chunk_mappings->resize(i); // cut at the end if we have removed some items

    REQUEST_STATS_FINISH(request_stats, RequestStatistics::FINGERPRINTING);
    if (request_stats) {
        stats_.average_fingerprint_latency_.Add(request_stats->latency(RequestStatistics::FINGERPRINTING));
    }

    return true;
}

bool ContentStorage::ProcessChunkFilterChain(std::tr1::tuple<Session*, const BlockMapping*, ChunkMapping*,
                                                             MultiSignalCondition*, tbb::atomic<bool>*, ErrorContext*> t) {
    SlidingAverageProfileTimer method_timer(this->stats_.average_process_chunk_filter_chain_latency_);
    Session* session = std::tr1::get<0>(t);
    const BlockMapping* block_mapping = std::tr1::get<1>(t);
    ChunkMapping* chunk_mapping = std::tr1::get<2>(t);
    MultiSignalCondition* barrier = std::tr1::get<3>(t);
    tbb::atomic<bool>* filter_chain_failed = std::tr1::get<4>(t);
    ErrorContext* ec = std::tr1::get<5>(t);

    DCHECK(block_mapping, "Block mapping not set");
    DCHECK(filter_chain_failed, "Filter chain failed not set");

    tbb::tick_count start_step_tick, end_step_tick;
    NESTED_LOG_CONTEXT("block " + ToString(block_mapping->block_id()));

    TRACE("Executing filter chain task: " << chunk_mapping->DebugString());

    bool failed = false;
    start_step_tick = tbb::tick_count::now();
    if (!filter_chain_->ReadChunkInfo(session, block_mapping, chunk_mapping, ec)) {
        ERROR("Reading of chunk infos failed: " << chunk_mapping->DebugString() <<
            ", " << (ec ? ec->DebugString() : ""));
        failed = true;
    }
    end_step_tick = tbb::tick_count::now();
    this->stats_.average_process_chunk_filter_chain_read_chunk_info_latency_.Add(
        (end_step_tick - start_step_tick).seconds() * 1000);

    // Write data to data storage for every chunk with an unknown data address
    FAULT_POINT("content-storage.handle.pre-chunk-store");
    if (likely(!failed)) {
        SlidingAverageProfileTimer method_timer2(this->stats_.average_process_chunk_filter_chain_write_block_latency_);
        if (!this->chunk_store_->WriteBlock(session->storage_session(), chunk_mapping, ec)) {
            ERROR("Storing of chunk data failed: " <<
                "block mapping " << (block_mapping ? block_mapping->DebugString() : "<no block mapping>") <<
                ", chunk mapping " << chunk_mapping->DebugString());
            failed = true;
        }
    }

    if (likely(!failed)) {
        SlidingAverageProfileTimer method_timer2(
            this->stats_.average_process_chunk_filter_chain_store_chunk_info_latency_);
        // Store the chunks
        FAULT_POINT("content-storage.handle.pre-filter-update");
        if (!filter_chain_->StoreChunkInfo(session,
              block_mapping,
              chunk_mapping,
              ec)) {
            ERROR("Storing of chunk failed: " << chunk_mapping->DebugString());
            failed = true;
        }
        FAULT_POINT("content-storage.handle.post-filter-update");

    } else {
        if (!filter_chain_->AbortChunkInfo(session, block_mapping, chunk_mapping, ec)) {
            ERROR("Failed to abort chunk mapping filter: " << chunk_mapping->DebugString());
            // failed already set to true
        }
    }
    if (failed && filter_chain_failed) {
        filter_chain_failed->compare_and_swap(true, false); // mark as failed
    }
    if (barrier) {
        barrier->Signal();
    }
    TRACE("Finished filter chain task: " << chunk_mapping->DebugString() << ", failed " << ToString(failed));
    return !failed;
}

bool ContentStorage::ProcessFilterChain(Session* session,
    Request* request,
    RequestStatistics* request_stats,
    const BlockMapping* block_mapping,
    vector<ChunkMapping>* chunk_mappings,
    ErrorContext* ec) {
    DCHECK(chunk_mappings, "Chunk mappings not set");

    stats_.threads_in_filter_chain_++;
    bool failed = false;
    REQUEST_STATS_START(request_stats, RequestStatistics::FILTER_CHAIN);
    vector<ChunkMapping>::iterator i;

    if (parallel_filter_chain_) {
        tbb::atomic<bool> filter_chain_failed;
        filter_chain_failed = false;
        MultiSignalCondition barrier(chunk_mappings->size());
        for (i = chunk_mappings->begin(); i != chunk_mappings->end(); i++) {
            ChunkMapping* chunk_mapping = &(*i);
            DCHECK(chunk_mapping, "Chunk mapping not set");

            TRACE("Create task for chunk: " << chunk_mapping->DebugString());
            Runnable<bool>* t = NewRunnable(this, &ContentStorage::ProcessChunkFilterChain, make_tuple(session,
                block_mapping, chunk_mapping, &barrier, &filter_chain_failed, ec));
            tp_->SubmitNoFuture(t, Threadpool::HIGH_PRIORITY, Threadpool::CALLER_RUNS);
        }

        {
            SlidingAverageProfileTimer timer(this->stats_.average_process_filter_chain_barrier_wait_latency_);
            CHECK(barrier.Wait(), "Failed to wait for filter chain chunks");
        }

        if (filter_chain_failed) {
            // the error was reported (and better) before this
            failed = true;
        }
    } else {
        for (i = chunk_mappings->begin(); i != chunk_mappings->end(); i++) {
            ChunkMapping* chunk_mapping = &(*i);
            DCHECK(chunk_mapping, "Chunk mapping not set");
            bool r = ProcessChunkFilterChain(make_tuple(session,
                block_mapping,
                chunk_mapping,
                (MultiSignalCondition*)NULL,
                (tbb::atomic<bool>*)NULL, ec));
            if (!r) {
              failed = true;
              break;
            }
        }
    }
    TRACE("Filter chain tasks finished");
    REQUEST_STATS_FINISH(request_stats, RequestStatistics::FILTER_CHAIN);
    if (request_stats) {
        stats_.average_filter_chain_time_.Add(request_stats->latency(RequestStatistics::FILTER_CHAIN));
    }
    stats_.threads_in_filter_chain_--;
    return !failed;
}

bool ContentStorage::MergeChunksIntoCurrentRequest(uint64_t block_id,
    RequestStatistics* request_stats,
    unsigned int block_offset,
    unsigned long open_chunk_pos,
    bool already_failed,
    Session* session,
    const BlockMapping* original_block_mapping,
    const BlockMapping* updated_block_mapping,
    vector<ChunkMapping>* chunk_mappings,
    ErrorContext* ec) {
    DCHECK(session->open_chunk_position() <= chunk_mappings->at(0).chunk()->size(),
        "Illegal open chunk position");

    BlockMappingItem request(session->open_chunk_position(), chunk_mappings->at(0).chunk()->size()
                             - session->open_chunk_position());
    CHECK(request.Convert(chunk_mappings->at(0)),
        "Failed to convert chunk mapping: " << chunk_mappings->at(0).DebugString());

    // TODO(fermat): Is this necessary? Is is used anywhere? It is also set in the last line of this method.
    session->set_open_chunk_position(session->open_chunk_position() + request.size());

    CHECK(session->AppendBlock(*original_block_mapping, *updated_block_mapping),
        "Cannot append block to session: " <<
        original_block_mapping->DebugString() << " => " << updated_block_mapping->DebugString());
    if (request.size() > 0) {
        CHECK(session->AppendRequest(block_id, block_offset, request),
            "Cannot append block request to session");
        block_offset += request.size();
    }
    // Create new block mappings
    for (int i = 1; i < chunk_mappings->size(); i++) {
        if (chunk_mappings->at(i).chunk() == NULL) {
            // is deleted chunk
            // TODO (fermat): Can this happen? Why?
            continue;
        }
        BlockMappingItem request(0, chunk_mappings->at(i).chunk()->size());
        CHECK(request.Convert(chunk_mappings->at(i)),
            "Cannot convert chunk mapping: " << chunk_mappings->at(i).DebugString());

        // We append entries to the block mapping here, we do not hold the requests.
        CHECK(session->AppendRequest(block_id, block_offset, request),
            "Cannot append block request to session");
        block_offset += request.size();
    }

    bool failed = false;
    if (likely(!already_failed)) {

        // last mapping
        if (open_chunk_pos > 0) {

            // If the chunk position is zero, the chunk of the block id has been chunked
            // that means that a request item for the block already is queued
            BlockMappingItem mapping_item(0, open_chunk_pos);
            // Fingerprint not known yet
            CHECK(session->AppendRequest(block_id, block_offset, mapping_item),
                "Cannot append last block mapping to session: block id " << block_id <<
                ", block offset " << block_offset <<
                ", request " << request.DebugString());
        } else {
            OpenRequest* open_request = session->GetRequest(0);
            DCHECK(open_request, "Open request not set");

            TRACE("Store open request: " << open_request->DebugString() <<
                ", reason chunk not open" <<
                ", open request count " << session->open_request_count());

            REQUEST_STATS_START(request_stats, RequestStatistics::BLOCK_STORING);
            if (!this->block_index_->StoreBlock(open_request->original_block_mapping(),
                    open_request->modified_block_mapping(), ec)) {
                ERROR("Storing of block index failed: " << open_request->DebugString());
                failed = true;
                if (!this->block_index_->MarkBlockWriteAsFailed(open_request->original_block_mapping(),
                        open_request->modified_block_mapping(), false, ec)) {
                    WARNING("Failed to mark block write as failed: " << open_request->DebugString());
                }
            }
            REQUEST_STATS_FINISH(request_stats, RequestStatistics::BLOCK_STORING);
            CHECK(this->block_locks_->WriteUnlock(open_request->block_id(), LOCK_LOCATION_INFO),
                "Failed to unlock block lock: open request: " << open_request->DebugString());

            // Clear the requests by forwarding the cyclic buffer position
            CHECK(session->ClearRequests(1), "Failed to clear request");
        }
    } else {
        OpenRequest* open_request = session->GetRequest(0);
        DCHECK(open_request, "Open request not set");

        // processing already failed
        if (!this->block_index_->MarkBlockWriteAsFailed(open_request->original_block_mapping(),
                open_request->modified_block_mapping(), false, ec)) {
            WARNING("Failed to mark block write as failed: " << open_request->DebugString());
        }
        CHECK(this->block_locks_->WriteUnlock(open_request->block_id(), LOCK_LOCATION_INFO),
            "Failed to unlock block lock: open request: " << open_request->DebugString());

        // Clear the requests by forwarding the cyclic buffer position
        CHECK(session->ClearRequests(1), "Failed to clear request");

    }

    session->set_open_chunk_position(open_chunk_pos);
    return !failed;
}

bool ContentStorage::MergeChunksIntoOpenRequests(uint64_t block_id, Session* session, Request* request,
                                                 RequestStatistics* request_stats, const BlockMapping* original_block_mapping,
                                                 const BlockMapping* updated_block_mapping, vector<ChunkMapping>* chunk_mappings, ErrorContext* ec) {
    DCHECK(chunk_mappings, "Chunk mappings not set");
    REQUEST_STATS_START(request_stats, RequestStatistics::OPEN_REQUEST_HANDLING);

    unsigned int i = 0;
    uint64_t last_open_block_id = -1;

    // Write all finished block mappings to disk (in the following) no access to/from chunk-index and chunk-storage
    unsigned long open_chunk_pos = 0; // if we are in the closing phase, the chunker session can be zero, but then we have no open data
    if (session->chunker_session()) { // TODO(dmeister): How can the chunker session be NULL (there is no more a closing phase)
        open_chunk_pos = session->chunker_session()->open_chunk_position();
    }

    bool failed_store_open_requests = false;
    unsigned int clear_count = 0; // Holds the number of stored block mappings, Used to clear these mappings

    for (i = 0; i < session->open_request_count() && !failed_store_open_requests; i++) { // all block mappings with open chunks
        OpenRequest* open_request = session->GetRequest(i);
        DCHECK(open_request, "Open request not set");
        DCHECK(chunk_mappings->at(0).data_address() != Storage::ILLEGAL_STORAGE_ADDRESS,
            "Data address not set: " << chunk_mappings->at(0).DebugString())

        TRACE("Store open request: " << open_request->DebugString());

        bool failed_open_request = false;
        if (unlikely(!open_request->mutable_block_mapping()->Merge(chunk_mappings->at(0)))) {
            ERROR("Failed to merge chunk mapping with open request: " <<
                "chunk mapping " << chunk_mappings->at(0).DebugString() <<
                ", open request " << open_request->DebugString());
            failed_open_request = true;
        } else {
            if (open_request->block_id() == block_id) {
                break; // Do not store a mapping of the current block. It may not be finished yet
            }
            DCHECK(open_request->block_id() != last_open_block_id, "Illegal open blocks");
            clear_count++;

            REQUEST_STATS_START(request_stats, RequestStatistics::BLOCK_STORING);
            if (!this->block_index_->StoreBlock(open_request->original_block_mapping(), // this is the original block mapping of the yet closed block!
                    open_request->modified_block_mapping(), ec)) {
                ERROR("Storing of block index failed: " << open_request->DebugString());
                failed_open_request = true;
            }
            REQUEST_STATS_FINISH(request_stats, RequestStatistics::BLOCK_STORING);
        }
        if (unlikely(failed_open_request)) {
            if (!this->block_index_->MarkBlockWriteAsFailed(open_request->original_block_mapping(),
                    open_request->modified_block_mapping(), false, ec)) {
                WARNING("Failed to mark block write as failed: " << open_request->DebugString());
            }
            failed_store_open_requests = true;
        }
        if (!this->block_locks_->WriteUnlock(open_request->block_id(), LOCK_LOCATION_INFO)) {
            ERROR("Failed to release block lock: block id " << open_request->block_id());
            failed_store_open_requests = true;
        }
        last_open_block_id = open_request->block_id();
    }

    // Clear the delayed/old requests by forwarding the cyclic buffer position (ok since they are processed now)
    if (!session->ClearRequests(clear_count)) {
        ERROR("Failed to clear requests: clear count " << clear_count);
        failed_store_open_requests = true;
    }

    CHECK(MergeChunksIntoCurrentRequest(block_id,
            request_stats,
            request->offset(),
            open_chunk_pos,
            failed_store_open_requests, // mark the request as failed
            session,
            original_block_mapping,
            updated_block_mapping,
            chunk_mappings,
            ec), "Failed to merge chunks into current request");

    REQUEST_STATS_FINISH(request_stats, RequestStatistics::OPEN_REQUEST_HANDLING);
    if (request_stats) {
        stats_.average_open_request_handling_latency_.Add(request_stats->latency(
                RequestStatistics::OPEN_REQUEST_HANDLING));
        stats_.average_block_storing_latency_.Add(request_stats->latency(RequestStatistics::BLOCK_STORING));
    }
    FAULT_POINT("content-storage.handle.post");
    return !failed_store_open_requests;
}

bool ContentStorage::MarkChunksAsOphran(const vector<ChunkMapping>& chunk_mappings) {
    CHECK(log_, "Log not set");

    OphranChunksEventData event_data;

    vector<ChunkMapping>::const_iterator i;
    for (i = chunk_mappings.begin(); i != chunk_mappings.end(); i++) {
        event_data.add_chunk_fp(i->fingerprint(), i->fingerprint_size());
    }

    DEBUG("Mark chunks as possible ophran: " << event_data.ShortDebugString());

    CHECK(log_->CommitEvent(dedupv1::log::EVENT_TYPE_OPHRAN_CHUNKS, &event_data, NULL, NULL, NO_EC),
        "Cannot commit log entry: " << event_data.ShortDebugString());
    return true;
}

bool ContentStorage::HandleChunks(Session* session, Request* request, RequestStatistics* request_stats,
                                  const BlockMapping* original_block_mapping, const BlockMapping* updated_block_mapping,
                                  const list<Chunk*>& chunks, ErrorContext* ec) {
    DCHECK(request, "Request not set");
    DCHECK(session, "Session not set");
    DCHECK(chunks.size() > 0, "No Chunks to handle");

    DCHECK(original_block_mapping, "Block mapping not set");
    DCHECK(updated_block_mapping, "Block mapping not set");
    // request may be NULL, but if it is set it must be valid
    DCHECK(request->IsValid(), "Request not valid: " << request->DebugString());

    DCHECK(original_block_mapping->block_id() == updated_block_mapping->block_id(),
        "Block id mismatch");
    DCHECK(original_block_mapping->block_size() == updated_block_mapping->block_size(),
        "Block size mismatch");

    uint64_t block_id = original_block_mapping->block_id();
    FAULT_POINT("content-storage.handle.pre");

    vector<ChunkMapping> chunk_mappings;
    CHECK(FingerprintChunks(session, request, request_stats, session->fingerprinter(), chunks, &chunk_mappings, ec),
        "Failed to fingerprint chunks: " << (request ? request->DebugString() : "no request"));
    // do not use the chunks list directly after this

    if (unlikely(!ProcessFilterChain(session, request, request_stats, updated_block_mapping, &chunk_mappings, ec))) {
        ERROR("Failed to process filter chain: " << (request ? request->DebugString() : "no request") <<
            (ec ? ", " + ec->DebugString() : ""));

        // no open requests have been changed, but we might have chunks that should be considered a gc candidate
        if (!MarkChunksAsOphran(chunk_mappings)) {
            WARNING("Failed to mark chunks as possible ophran");
        }

        // when there are open requests than there might be chunks that are also ohprans (or possible ophrans).
        // In addition, we have to mark these requests as failed.

        for (int i = 0; i < session->open_request_count(); i++) {
            OpenRequest* open_request = session->GetRequest(i);
            DCHECK(open_request, "Open request not set");

            TRACE("Fail open request: " << open_request->DebugString());

            if (!this->block_index_->MarkBlockWriteAsFailed(open_request->original_block_mapping(),
                    open_request->modified_block_mapping(), false, ec)) {
                WARNING("Failed to mark block write as failed: " << open_request->DebugString());
            }
        }
        return false;
    }

    // After running through the filter chain the new chunks are also in container storage

    return MergeChunksIntoOpenRequests(block_id, session, request, request_stats, original_block_mapping,
        updated_block_mapping, &chunk_mappings, ec);
}

bool ContentStorage::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ContentStorageStatsData data;
    data.set_read_count(this->stats_.reads_);
    data.set_write_count(this->stats_.writes_);
    data.set_read_size(this->stats_.read_size_);
    data.set_write_size(this->stats_.write_size_);
    CHECK(ps->Persist(prefix, data), "Failed to persist content storage stats: " << data.ShortDebugString());
    return true;
}

bool ContentStorage::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ContentStorageStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore content storage stats: " << data.ShortDebugString());
    this->stats_.reads_ = data.read_count();
    this->stats_.writes_ = data.write_count();
    this->stats_.read_size_ = data.read_size();
    this->stats_.write_size_ = data.write_size();
    return true;
}

string ContentStorage::PrintLockStatistics() {
    return "null";
}

string ContentStorage::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"threads in filter chain\": " << this->stats_.threads_in_filter_chain_ << "" << std::endl;
    sstr << "}";
    return sstr.str();
}

string ContentStorage::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"reads\": " << this->stats_.reads_ << "," << std::endl;
    sstr << "\"writes\": " << this->stats_.writes_ << "," << std::endl;
    sstr << "\"read size\": " << this->stats_.read_size_ << "," << std::endl;
    sstr << "\"write size\": " << this->stats_.write_size_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string ContentStorage::PrintProfile() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{";
    sstr << "\"average write request processing latency\": " << this->stats_.average_processing_time_.GetAverage()
         << "," << std::endl;
    sstr << "\"average filter chain latency\": " << this->stats_.average_filter_chain_time_.GetAverage() << ","
         << std::endl;
    sstr << "\"average process filter chain latency\": "
         << this->stats_.average_process_chunk_filter_chain_latency_.GetAverage() << "," << std::endl;
    sstr << "\"average process filter chain barrier wait latency\": "
         << this->stats_.average_process_filter_chain_barrier_wait_latency_.GetAverage() << "," << std::endl;
    sstr << "\"average process filter chain read chunk info latency\": "
         << this->stats_.average_process_chunk_filter_chain_read_chunk_info_latency_.GetAverage() << ","
         << std::endl;
    sstr << "\"average process filter chain write block latency\": "
         << this->stats_.average_process_chunk_filter_chain_write_block_latency_.GetAverage() << "," << std::endl;
    sstr << "\"average process filter chain store chunk info latency\": "
         << this->stats_.average_process_chunk_filter_chain_store_chunk_info_latency_.GetAverage() << ","
         << std::endl;
    sstr << "\"average chunk store latency\": " << this->stats_.average_chunk_store_latency_.GetAverage() << ","
         << std::endl;
    sstr << "\"average chunking latency\": " << this->stats_.average_chunking_latency_.GetAverage() << "," << std::endl;
    sstr << "\"average fingerprinting latency\": " << this->stats_.average_fingerprint_latency_.GetAverage() << ","
         << std::endl;
    sstr << "\"average block read latency\": " << this->stats_.average_block_read_latency_.GetAverage() << ","
         << std::endl;
    sstr << "\"average sync latency\": " << this->stats_.average_sync_latency_.GetAverage() << "," << std::endl;
    sstr << "\"average open request handling latency\": "
         << this->stats_.average_open_request_handling_latency_.GetAverage() << "," << std::endl;
    sstr << "\"average block storing latency\": " << this->stats_.average_block_storing_latency_.GetAverage() << ","
         << std::endl;
    sstr << "\"average write block latency\": " << this->stats_.average_write_block_latency_.GetAverage() << ","
         << std::endl;
    sstr << "\"checksum\": " << this->stats_.checksum_time_.GetSum() << "," << std::endl;
    sstr << "\"content store\": " << this->stats_.profiling_.GetSum() << "," << std::endl;
    sstr << "\"chunking\": " << this->stats_.chunking_time_.GetSum() << "," << std::endl;
    sstr << "\"fingerprinting\": " << this->stats_.fingerprint_profiling_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

bool ContentStorage::ReadDataForItem(BlockMappingItem* item, Session* session, byte* data_buffer,
                                     unsigned int data_pos, int count, int offset, dedupv1::base::ErrorContext* ec) {
    if (item->fingerprint_size() > 0) {
        // Chunk is finished and normally stored in the index => Read data from storage
        size_t local_buffer_size = session->buffer_size();
        memset(session->buffer(), 0, session->buffer_size());

        CHECK(this->chunk_store_->ReadBlock(session->storage_session(), item, session->buffer(), &local_buffer_size, ec),
            "Chunk reading failed " << item->DebugString() << ", offset = " << offset);

        CHECK(local_buffer_size >= item->chunk_offset() + item->size(),
            "Buffer length error (offset " << item->chunk_offset() <<
            ", size " << item->size() << ", buffer size " << local_buffer_size << ")");
        CHECK(item->chunk_offset() + item->size() <= session->buffer_size(), "Item doesn't fit in buffer");

        // If we want to read data then read it to the data buffer.
        if (count > 0) {
            memcpy(data_buffer + data_pos, &session->buffer()[item->chunk_offset() + offset], count);
        }
    } else {
        /* No fingerprint stored => Chunk isn't finished => Get data from open chunk */
        unsigned long open_chunk_pos = session->chunker_session()->open_chunk_position();
        CHECK(open_chunk_pos >= item->chunk_offset() + item->size(), "Open Chunk Assertion Chunk Position");
        ScopedArray<byte> buffer(new byte[item->size()]);
        CHECK(buffer.Get() != NULL, "Failed to alloc buffer");

        CHECK(session->chunker_session()->GetOpenChunkData(buffer.Get(), item->chunk_offset(), item->size()), "Open data reading failed");

        // If we want to read data then read it to the data buffer.
        if (count > 0) {
            memcpy(data_buffer + data_pos, buffer.Get() + offset, count);
        }
    }
    return true;
}

}
