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
#include <core/sparse_chunk_index_filter.h>

#include <sstream>

#include <base/index.h>
#include <base/bitutil.h>
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
using dedupv1::base::Option;

LOGGER("SparseChunkIndexFilter");

namespace dedupv1 {
namespace filter {

SparseChunkIndexFilter::Statistics::Statistics() : average_latency_(256) {
    weak_hits_ = 0;
    strong_hits_ = 0;
    miss_ = 0;
    reads_ = 0;
    writes_ = 0;
    failures_ = 0;
    anchor_count_ = 0;
}

SparseChunkIndexFilter::SparseChunkIndexFilter() :
    Filter("sparse-chunk-index-filter", FILTER_STRONG_MAYBE) {
    this->chunk_index_ = NULL;
    sampling_factor_ = 32;
    sampling_mask_ = 0;
}

SparseChunkIndexFilter::~SparseChunkIndexFilter() {
}

void SparseChunkIndexFilter::RegisterFilter() {
    Filter::Factory().Register("sparse-chunk-index-filter", &SparseChunkIndexFilter::CreateFilter);
}

Filter* SparseChunkIndexFilter::CreateFilter() {
    Filter* filter = new SparseChunkIndexFilter();
    return filter;
}

bool SparseChunkIndexFilter::Start(DedupSystem* system) {
    DCHECK(system, "System not set");
    DCHECK(system->chunk_index(), "Chunk Index not set");
    DCHECK(sampling_factor_ > 0, "Sampling factor not set");

    sampling_mask_ = 1;
    for (int i = 1; i < dedupv1::base::bits(sampling_factor_); i++) {
        sampling_mask_ <<= 1;
        sampling_mask_ |= 1;
    }
    DEBUG("Started sparse chunk index filter: " <<
        "sampling factor " << sampling_factor_ <<
        ", sampling mask " << sampling_mask_);
    this->chunk_index_ = system->chunk_index();
    return true;
}

bool SparseChunkIndexFilter::SetOption(const string& option_name,
                                       const string& option) {
    if (option_name == "sampling-factor") {
        Option<uint32_t> o = To<uint32_t>(option);
        CHECK(o.valid(), "Illegal sampling factor: Should be an integer");
        CHECK(!o.value() == 0, "Illegal sampling factor: Should not be zero");
        bool is_power = !(o.value() & (o.value() - 1));
        CHECK(is_power, "Illegal sampling factor: Should be a power of two");
        sampling_factor_ = o.value();
        return true;
    }
    return Filter::SetOption(option_name, option);
}

bool SparseChunkIndexFilter::IsAnchor(const ChunkMapping& mapping) {
    DCHECK(mapping.fingerprint_size() >= 4, "Illegal fingerprint");
    DCHECK(sampling_mask_ != 0, "Illegal sampling mask");

    uint32_t suffix;
    memcpy(&suffix,
        mapping.fingerprint() + (mapping.fingerprint_size() - 4),
        4);
    if ((suffix & sampling_mask_) == sampling_mask_) {
        TRACE("Fingerprint is anchor: " << mapping.DebugString());
        return true;
    } else {
        TRACE("Fingerprint is no anchor: " << mapping.DebugString());
        return false;
    }
}

bool SparseChunkIndexFilter::ReleaseChunkLock(const ChunkMapping& mapping) {
    DCHECK(this->chunk_index_ != NULL, "Chunk index filter not started");
    return this->chunk_index_->chunk_locks().Unlock(mapping.fingerprint(), mapping.fingerprint_size());
    return true;
}

bool SparseChunkIndexFilter::AcquireChunkLock(const ChunkMapping& mapping) {
    DCHECK(this->chunk_index_ != NULL, "Chunk index filter not started");
    return this->chunk_index_->chunk_locks().Lock(mapping.fingerprint(), mapping.fingerprint_size());
}

Filter::filter_result SparseChunkIndexFilter::Check(Session* session,
                                                    const BlockMapping* block_mapping,
                                                    ChunkMapping* mapping,
                                                    ErrorContext* ec) {
    DCHECK_RETURN(mapping, FILTER_ERROR, "Chunk mapping not set");
    enum filter_result result = FILTER_ERROR;
    ProfileTimer timer(this->stats_.time_);
    SlidingAverageProfileTimer timer2(this->stats_.average_latency_);

    this->stats_.reads_++;
    if (!IsAnchor(*mapping)) {
        // no anchor => no indexing
        mapping->set_indexed(false);
        stats_.weak_hits_++;
        return FILTER_WEAK_MAYBE;
    }
    stats_.anchor_count_++;

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
        mapping->set_indexed(true);
    } else if (index_result == LOOKUP_FOUND) {
        mapping->set_indexed(true);
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

bool SparseChunkIndexFilter::Update(Session* session,
    const BlockMapping* block_mapping,
    ChunkMapping* mapping,
    ErrorContext* ec) {
    ProfileTimer timer(this->stats_.time_);

    DCHECK(mapping, "Mapping must be set");

    if (!IsAnchor(*mapping)) {
        return true;
    }
    this->stats_.writes_++;

    bool r = this->chunk_index_->Put(*mapping, ec);

    if (unlikely(!ReleaseChunkLock(*mapping))) {
        WARNING("Failed to release chunk lock: " << mapping->DebugString());
    }
    return r;
}

bool SparseChunkIndexFilter::Abort(Session* session,
    const BlockMapping* block_mapping,
    ChunkMapping* chunk_mapping,
    ErrorContext* ec) {
    DCHECK(chunk_mapping, "Chunk mapping not set");

    // if we have the empty fingerprint, we do not need to release the chunk lock
    if (chunk_mapping->data_address() == Storage::EMPTY_DATA_STORAGE_ADDRESS) {
        return true;
    }
    if (!IsAnchor(*chunk_mapping)) {
        return true;
    }
    if (!ReleaseChunkLock(*chunk_mapping)) {
        WARNING("Failed to release chunk lock: " << chunk_mapping->DebugString());
    }
    return true;
}

bool SparseChunkIndexFilter::PersistStatistics(std::string prefix,
                                               dedupv1::PersistStatistics* ps) {
    SparseChunkIndexFilterStatsData data;
    data.set_strong_hit_count(this->stats_.strong_hits_);
    data.set_weak_hit_count(stats_.weak_hits_);
    data.set_miss_count(this->stats_.miss_);
    data.set_read_count(this->stats_.reads_);
    data.set_write_count(this->stats_.writes_);
    data.set_anchor_count(stats_.anchor_count_);
    data.set_failure_count(stats_.failures_);
    CHECK(ps->Persist(prefix, data), "Failed to persist sparse chunk index filter stats");
    return true;
}

bool SparseChunkIndexFilter::RestoreStatistics(std::string prefix,
                                               dedupv1::PersistStatistics* ps) {
    SparseChunkIndexFilterStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore sparse chunk index filter stats");
    stats_.reads_ = data.read_count();
    stats_.strong_hits_ = data.strong_hit_count();
    stats_.weak_hits_ = data.weak_hit_count();
    stats_.miss_ = data.miss_count();
    stats_.writes_ = data.write_count();
    stats_.anchor_count_ = data.anchor_count();
    stats_.failures_ = data.failure_count();
    return true;
}

string SparseChunkIndexFilter::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"anchor count\": " << stats_.anchor_count_ << "," << std::endl;
    sstr << "\"reads\": " << this->stats_.reads_ << "," << std::endl;
    sstr << "\"writes\": " << this->stats_.writes_ << "," << std::endl;
    sstr << "\"strong\": " << this->stats_.strong_hits_ << "," << std::endl;
    sstr << "\"weak\": " << stats_.weak_hits_ << "," << std::endl;
    sstr << "\"failures\": " << this->stats_.failures_ << "," << std::endl;
    sstr << "\"miss\": " << this->stats_.miss_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string SparseChunkIndexFilter::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"used time\": " << this->stats_.time_.GetSum() << "," << std::endl;
    sstr << "\"average latency\": " << this->stats_.average_latency_.GetAverage() << std::endl;
    sstr << "}";
    return sstr.str();
}

}
}

