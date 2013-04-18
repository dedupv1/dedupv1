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
#include <core/sampling_filter.h>

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

LOGGER("SamplingFilter");

namespace dedupv1 {
namespace filter {

SamplingFilter::Statistics::Statistics()  {
    weak_hits_ = 0;
    reads_ = 0;
}

SamplingFilter::SamplingFilter() :
    Filter("sampling-filter", FILTER_STRONG_MAYBE) {
    this->chunk_index_ = NULL;
}

SamplingFilter::~SamplingFilter() {
}

void SamplingFilter::RegisterFilter() {
    Filter::Factory().Register("sampling-filter",
        &SamplingFilter::CreateFilter);
}

Filter* SamplingFilter::CreateFilter() {
    Filter* filter = new SamplingFilter();
    return filter;
}

bool SamplingFilter::Start(DedupSystem* system) {
    DCHECK(system, "System not set");
    DCHECK(system->chunk_index(), "Chunk Index not set");

    this->chunk_index_ = system->chunk_index();
    return true;
}

Option<bool> SamplingFilter::IsAnchor(const ChunkMapping& mapping) {

    ChunkIndexSamplingStrategy* sampling_strategy = chunk_index_->sampling_strategy();
    DCHECK(sampling_strategy, "Sampling strategy not set");

    return sampling_strategy->IsAnchor(mapping);
}

Filter::filter_result SamplingFilter::Check(Session* session,
                                              const BlockMapping* block_mapping,
                                              ChunkMapping* mapping,
                                              ErrorContext* ec) {
    DCHECK_RETURN(mapping, FILTER_ERROR, "Chunk mapping not set");
    ProfileTimer timer(this->stats_.time_);

    TRACE("Check " << mapping->DebugString());
    this->stats_.reads_++;
    stats_.weak_hits_++;

    Option<bool> b = IsAnchor(*mapping);
    DCHECK_RETURN(b.valid(), FILTER_ERROR,
        "Failed to check anchor state:" <<
        mapping->DebugString());

    if (!b.value()) {
        // no anchor => no indexing
        mapping->set_indexed(false);
        TRACE("Chunk is no anchor: " << mapping->DebugString());
    } else {
        mapping->set_indexed(true);
        TRACE("Chunk is anchor: " << mapping->DebugString());
    }
    return FILTER_WEAK_MAYBE;
}

bool SamplingFilter::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    SamplingFilterStatsData data;
    data.set_weak_hit_count(stats_.weak_hits_);
    data.set_read_count(stats_.reads_);
    CHECK(ps->Persist(prefix, data), "Failed to persist chunk index filter stats");
    return true;
}

bool SamplingFilter::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    SamplingFilterStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore chunk index filter stats");
    stats_.reads_ = data.read_count();
    stats_.weak_hits_ = data.weak_hit_count();
    return true;
}

string SamplingFilter::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"reads\": " << this->stats_.reads_ << "," << std::endl;
    sstr << "\"weak\": " << this->stats_.weak_hits_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string SamplingFilter::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"used time\": " << this->stats_.time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

}
}

