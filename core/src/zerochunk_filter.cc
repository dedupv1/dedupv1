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

#include <core/zerochunk_filter.h>

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

LOGGER("ZeroChunkFilter");

namespace dedupv1 {
namespace filter {

ZeroChunkFilter::ZeroChunkFilter() :
    Filter("zerochunk-filter", FILTER_EXISTING) {
}

ZeroChunkFilter::Statistics::Statistics() {
    existing_hits_ = 0;
    weak_hits_ = 0;
    reads_ = 0;
}

ZeroChunkFilter::~ZeroChunkFilter() {
}

void ZeroChunkFilter::RegisterFilter() {
    Filter::Factory().Register("zerochunk-filter", &ZeroChunkFilter::CreateFilter);
}

Filter* ZeroChunkFilter::CreateFilter() {
    return new ZeroChunkFilter();
}

Filter::filter_result ZeroChunkFilter::Check(Session* session,
                                             const BlockMapping* block_mapping,
                                             ChunkMapping* mapping,
                                             dedupv1::base::ErrorContext* ec) {
    // session not always set
    // block mapping not always set
    CHECK_RETURN(mapping, FILTER_ERROR, "Chunk mapping not set");

    stats_.reads_++;
    if (Fingerprinter::IsEmptyDataFingerprint(mapping->fingerprint(), mapping->fingerprint_size())) {
        TRACE("Found zero-chunk fingerprint");
        mapping->set_indexed(false);
        mapping->set_data_address(Storage::EMPTY_DATA_STORAGE_ADDRESS);
        this->stats_.existing_hits_++;
        return FILTER_EXISTING;
    } else {
        this->stats_.weak_hits_++;
        return FILTER_WEAK_MAYBE;
    }
}

bool ZeroChunkFilter::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ZeroChunkFilterStatsData data;
    data.set_existing_hit_count(this->stats_.existing_hits_);
    data.set_weak_hit_count(this->stats_.weak_hits_);
    data.set_read_count(this->stats_.reads_);
    CHECK(ps->Persist(prefix, data), "Failed to persist zero-chunk filter stats");
    return true;
}

bool ZeroChunkFilter::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ZeroChunkFilterStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore zero-chunk filter stats");
    this->stats_.reads_ = data.read_count();
    this->stats_.existing_hits_ = data.existing_hit_count();
    this->stats_.weak_hits_ = data.weak_hit_count();
    return true;
}

string ZeroChunkFilter::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"reads\": " << this->stats_.reads_ << "," << std::endl;
    sstr << "\"existing\": " << this->stats_.existing_hits_ << "," << std::endl;
    sstr << "\"weak\": " << this->stats_.weak_hits_ << std::endl;
    sstr << "}";
    return sstr.str();
}

}
}

