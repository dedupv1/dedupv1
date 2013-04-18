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
#include <core/chunk_index_filter.h>

#include <sstream>

#include <base/index.h>
#include <core/chunk_mapping.h>
#include <core/filter.h>
#include <base/strutil.h>
#include <core/dedup_system.h>
#include <core/chunk_index.h>
#include <base/timer.h>
#include <base/logging.h>
#include <base/hashing_util.h>
#include <core/fingerprinter.h>
#include <core/storage.h>

#include "dedupv1_stats.pb.h"

using std::string;
using std::stringstream;
using dedupv1::base::ProfileTimer;
using dedupv1::base::SlidingAverageProfileTimer;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::blockindex::BlockMapping;
using dedupv1::Session;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::strutil::To;
using dedupv1::Fingerprinter;
using dedupv1::chunkstore::Storage;
using dedupv1::base::ErrorContext;
using dedupv1::chunkindex::ChunkIndexSamplingStrategy;
using dedupv1::base::Option;

LOGGER("ChunkIndexFilter");

namespace dedupv1 {
namespace filter {

ChunkIndexFilter::Statistics::Statistics() : average_latency_(256) {
    strong_hits_ = 0;
    weak_hits_ = 0;
    miss_ = 0;
    reads_ = 0;
    writes_ = 0;
    failures_ = 0;
    anchor_count_ = 0;
}

ChunkIndexFilter::ChunkIndexFilter() :
    Filter("chunk-index-filter", FILTER_STRONG_MAYBE) {
    this->chunk_index_ = NULL;
}

ChunkIndexFilter::~ChunkIndexFilter() {
}

void ChunkIndexFilter::RegisterFilter() {
    Filter::Factory().Register("chunk-index-filter", &ChunkIndexFilter::CreateFilter);
}

Filter* ChunkIndexFilter::CreateFilter() {
    Filter* filter = new ChunkIndexFilter();
    return filter;
}

bool ChunkIndexFilter::Start(DedupSystem* system) {
    DCHECK(system, "System not set");
    DCHECK(system->chunk_index(), "Chunk Index not set");

    this->chunk_index_ = system->chunk_index();
    return true;
}

bool ChunkIndexFilter::ReleaseChunkLock(const dedupv1::chunkindex::ChunkMapping& mapping) {
    DCHECK(this->chunk_index_ != NULL, "Chunk index filter not started");
    return this->chunk_index_->chunk_locks().Unlock(mapping.fingerprint(), mapping.fingerprint_size());
    return true;
}

bool ChunkIndexFilter::AcquireChunkLock(const dedupv1::chunkindex::ChunkMapping& mapping) {
    DCHECK(this->chunk_index_ != NULL, "Chunk index filter not started");
    return this->chunk_index_->chunk_locks().Lock(mapping.fingerprint(), mapping.fingerprint_size());
}

Filter::filter_result ChunkIndexFilter::Check(Session* session,
                                              const BlockMapping* block_mapping,
                                              ChunkMapping* mapping,
                                              ErrorContext* ec) {
    DCHECK_RETURN(mapping, FILTER_ERROR, "Chunk mapping not set");
    enum filter_result result = FILTER_ERROR;
    ProfileTimer timer(this->stats_.time_);
    SlidingAverageProfileTimer timer2(this->stats_.average_latency_);

    TRACE("Check " << mapping->DebugString());
    this->stats_.reads_++;

    if (!mapping->is_indexed()) {
        // no anchor => no indexing
        stats_.weak_hits_++;
        return FILTER_WEAK_MAYBE;
    }
    stats_.anchor_count_++;

    TRACE("Chunk is anchor: " << mapping->DebugString());

    CHECK_RETURN(AcquireChunkLock(*mapping), FILTER_ERROR,
        "Failed to acquire chunk lock: " << mapping->DebugString());

    enum lookup_result index_result = this->chunk_index_->Lookup(mapping, true, ec);
    if (index_result == LOOKUP_NOT_FOUND) {
        if (likely(chunk_index_->IsAcceptingNewChunks())) {
            result = FILTER_NOT_EXISTING;
            this->stats_.miss_++;
        } else {
            if (ec) {
                ec->set_full();
            }
            stats_.failures_++;
            result = FILTER_ERROR;
        }
        // with the normal chunk index filter, all chunks are indexed
    } else if (index_result == LOOKUP_FOUND) {
        mapping->set_usage_count(0); // TODO (dmeister): Why???
        this->stats_.strong_hits_++;
        result = FILTER_STRONG_MAYBE;
    } else if (index_result == LOOKUP_ERROR) {
        ERROR("Chunk index filter lookup failed: " <<
            "mapping " << mapping->DebugString());
        stats_.failures_++;
        result = FILTER_ERROR;
    }
    if (result == FILTER_ERROR) {
        // if this check failed, there will be no abort call
        if (!ReleaseChunkLock(*mapping)) {
            WARNING("Failed to release chunk lock: " << mapping->DebugString());
        }
    }
    return result;
}

bool ChunkIndexFilter::Update(Session* session,
                              const BlockMapping* block_mapping,
                              ChunkMapping* mapping,
                              ErrorContext* ec) {
    ProfileTimer timer(this->stats_.time_);

    DCHECK(mapping, "Mapping must be set");
    TRACE("Update " << mapping->DebugString());

    if (!mapping->is_indexed()) {
        return true;
    }
    this->stats_.writes_++;

    bool r = this->chunk_index_->Put(*mapping, ec);

    if (unlikely(!ReleaseChunkLock(*mapping))) {
        WARNING("Failed to release chunk lock: " << mapping->DebugString());
    }
    return r;
}

bool ChunkIndexFilter::Abort(Session* session,
                             const BlockMapping* block_mapping,
                             ChunkMapping* chunk_mapping,
                             ErrorContext* ec) {
    DCHECK(chunk_mapping, "Chunk mapping not set");

    TRACE("Abort " << chunk_mapping->DebugString());

    if (!chunk_mapping->is_indexed()) {
        return true;
    }

    // if we have the empty fingerprint, we do not need to release the chunk lock
    if (chunk_mapping->data_address() == Storage::EMPTY_DATA_STORAGE_ADDRESS) {
        return true;
    }

    if (!ReleaseChunkLock(*chunk_mapping)) {
        WARNING("Failed to release chunk lock: " << chunk_mapping->DebugString());
    }
    return true;
}

bool ChunkIndexFilter::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ChunkIndexFilterStatsData data;
    data.set_strong_hit_count(stats_.strong_hits_);
    data.set_weak_hit_count(stats_.weak_hits_);
    data.set_anchor_count(stats_.anchor_count_);
    data.set_miss_count(stats_.miss_);
    data.set_read_count(stats_.reads_);
    data.set_write_count(stats_.writes_);
    data.set_failure_count(stats_.failures_);
    CHECK(ps->Persist(prefix, data), "Failed to persist chunk index filter stats");
    return true;
}

bool ChunkIndexFilter::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ChunkIndexFilterStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore chunk index filter stats");
    stats_.reads_ = data.read_count();
    stats_.strong_hits_ = data.strong_hit_count();
    stats_.weak_hits_ = data.weak_hit_count();
    stats_.anchor_count_ = data.anchor_count();
    stats_.miss_ = data.miss_count();
    stats_.writes_ = data.write_count();
    stats_.failures_ = data.failure_count();
    return true;
}

string ChunkIndexFilter::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"reads\": " << this->stats_.reads_ << "," << std::endl;
    sstr << "\"writes\": " << this->stats_.writes_ << "," << std::endl;
    sstr << "\"strong\": " << this->stats_.strong_hits_ << "," << std::endl;
    sstr << "\"weak\": " << this->stats_.weak_hits_ << "," << std::endl;
    sstr << "\"failures\": " << this->stats_.failures_ << "," << std::endl;
    sstr << "\"anchor count\": " << this->stats_.anchor_count_ << "," << std::endl;
    sstr << "\"miss\": " << this->stats_.miss_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string ChunkIndexFilter::PrintLockStatistics() {
    return "null";
}

string ChunkIndexFilter::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"used time\": " << this->stats_.time_.GetSum() << "," << std::endl;
    sstr << "\"average latency\": " << this->stats_.average_latency_.GetAverage() << std::endl;
    sstr << "}";
    return sstr.str();
}

}
}

