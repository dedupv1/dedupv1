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

#include <core/bytecompare_filter.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sstream>

#include <core/dedup.h>
#include <base/index.h>
#include <core/chunk_mapping.h>
#include <core/filter.h>
#include <base/strutil.h>
#include <core/storage.h>
#include <core/chunker.h>
#include <core/chunk_store.h>
#include <core/dedup_system.h>
#include <base/locks.h>
#include <core/session.h>
#include <base/timer.h>
#include <base/logging.h>
#include <core/chunk.h>
#include <base/memory.h>
#include <core/fingerprinter.h>
#include "dedupv1_stats.pb.h"

using std::string;
using std::stringstream;
using dedupv1::base::ProfileTimer;
using dedupv1::base::SlidingAverageProfileTimer;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::Session;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::chunkstore::Storage;
using dedupv1::base::ScopedArray;
using dedupv1::Fingerprinter;
using dedupv1::DedupSystem;

LOGGER("ByteCompareFilter");

namespace dedupv1 {
namespace filter {

ByteCompareFilter::ByteCompareFilter() :
    Filter("bytecompare-filter", FILTER_EXISTING) {
    buffer_size_ = Chunk::kMaxChunkSize;
}

ByteCompareFilter::Statistics::Statistics() : average_latency_(256) {
    hits_ = 0;
    miss_ = 0;
    reads_ = 0;
}

ByteCompareFilter::~ByteCompareFilter() {
}

void ByteCompareFilter::RegisterFilter() {
    Filter::Factory().Register("bytecompare-filter", &ByteCompareFilter::CreateFilter);
}

Filter* ByteCompareFilter::CreateFilter() {
    return new ByteCompareFilter();
}

bool ByteCompareFilter::Start(DedupSystem* dedup_system) {
    DCHECK(dedup_system, "Dedup system not set");
    DCHECK(dedup_system->storage(), "Storage not set");

    storage_ = dedup_system->storage();
    return true;
}

Filter::filter_result ByteCompareFilter::Check(Session* session, const BlockMapping* block_mapping,
                                               ChunkMapping* mapping, dedupv1::base::ErrorContext* ec) {
    // session not always set
    // block mapping not always set
    CHECK_RETURN(mapping, FILTER_ERROR, "Chunk mapping not set");
    DCHECK_RETURN(storage_, FILTER_ERROR, "Storage not set")

    // if the chunk is not set, we have not comparison
    if (mapping->chunk() == NULL) {
        return FILTER_WEAK_MAYBE;
    }
    ProfileTimer timer(this->stats_.time_);
    SlidingAverageProfileTimer timer2(this->stats_.average_latency_);

    // The address it not set, the chunk cannot be a duplicate.
    if (mapping->data_address() == Storage::ILLEGAL_STORAGE_ADDRESS) {
        return FILTER_NOT_EXISTING;
    }
    if (mapping->data_address() == Storage::EMPTY_DATA_STORAGE_ADDRESS) {
        return FILTER_EXISTING;
    }

    byte* buffer = new byte[this->buffer_size_];
    CHECK_RETURN(buffer, FILTER_ERROR, "Alloc for buffer failed");
    ScopedArray<byte> shared_buffer(buffer);
    size_t data_size = this->buffer_size_;

    stats_.reads_.fetch_and_increment();

    CHECK_RETURN(storage_->Read(
            mapping->data_address(),
            mapping->fingerprint(),
            mapping->fingerprint_size(), buffer, &data_size, ec), FILTER_ERROR,
        "Storage error reading address: " << mapping->DebugString());

    if (data_size == 0) {
        WARNING("Byte compare mismatch for fp " <<
            Fingerprinter::DebugString(mapping->fingerprint(), mapping->fingerprint_size()));
        stats_.miss_.fetch_and_increment();
        return FILTER_ERROR;
    } else if (data_size != mapping->chunk()->size()) {
        WARNING("Byte compare mismatch for fp " <<
            Fingerprinter::DebugString(mapping->fingerprint(), mapping->fingerprint_size()) << ":" <<
            "chunk size " << mapping->chunk()->size() <<
            ", stored chunk size " << data_size);
        stats_.miss_.fetch_and_increment();
        return FILTER_ERROR;
    } else if (memcmp(mapping->chunk()->data(), buffer, data_size) == 0) {
        stats_.hits_.fetch_and_increment();
        return FILTER_EXISTING;
    } else {
        WARNING("Byte compare mismatch for fp " <<
            Fingerprinter::DebugString(mapping->fingerprint(), mapping->fingerprint_size()));
        stats_.miss_.fetch_and_increment();
        return FILTER_ERROR;
    }
}

bool ByteCompareFilter::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ByteCompareFilterStatsData data;
    data.set_hit_count(this->stats_.hits_);
    data.set_miss_count(this->stats_.miss_);
    data.set_read_count(this->stats_.reads_);
    CHECK(ps->Persist(prefix, data), "Failed to persist byte-compare filter stats");
    return true;
}

bool ByteCompareFilter::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ByteCompareFilterStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore byte-compare filter stats");
    this->stats_.reads_ = data.read_count();
    this->stats_.hits_ = data.hit_count();
    this->stats_.miss_ = data.miss_count();
    return true;
}

string ByteCompareFilter::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"reads\": " << this->stats_.reads_ << "," << std::endl;
    sstr << "\"existing\": " << this->stats_.hits_ << "," << std::endl;
    sstr << "\"miss\": " << this->stats_.miss_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string ByteCompareFilter::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"used time\": " << this->stats_.time_.GetSum() << "," << std::endl;
    sstr << "\"average latency\": " << this->stats_.average_latency_.GetAverage() << std::endl;
    sstr << "}";
    return sstr.str();
}

}
}

