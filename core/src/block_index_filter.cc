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

#include <core/block_index_filter.h>

#include <sstream>

#include <base/index.h>
#include <core/dedup.h>
#include <core/chunk_mapping.h>
#include <core/filter.h>
#include <core/dedup_system.h>

#include <base/strutil.h>
#include <base/hashing_util.h>
#include <base/timer.h>
#include <base/logging.h>

#include "dedupv1_stats.pb.h"

using std::list;
using std::string;
using std::stringstream;
using dedupv1::base::ProfileTimer;
using dedupv1::base::SlidingAverageProfileTimer;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::Session;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::raw_compare;
using dedupv1::base::Option;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::StartsWith;

LOGGER("BlockIndexFilter");

namespace dedupv1 {
namespace filter {

BlockIndexFilter::BlockIndexFilter()
    : Filter("block-index-filter", FILTER_STRONG_MAYBE) {
    block_index_ = NULL;
    block_chunk_cache_ = new BlockChunkCache();
    use_block_chunk_cache_ = false;
}

BlockIndexFilter::Statistics::Statistics() : average_latency_(256) {
    this->hits_ = 0;
    this->miss_ = 0;
    this->reads_ = 0;
}

BlockIndexFilter::~BlockIndexFilter() {
}

void BlockIndexFilter::RegisterFilter() {
    Filter::Factory().Register("block-index-filter", &BlockIndexFilter::CreateFilter);
}

Filter* BlockIndexFilter::CreateFilter() {
    Filter* filter = new BlockIndexFilter();
    return filter;
}

bool BlockIndexFilter::Start(DedupSystem* system) {
    block_index_ = system->block_index();
    CHECK(block_index_, "Block index not set");

    if (use_block_chunk_cache_) {
        CHECK(block_chunk_cache_->Start(block_index_), "Failed to start block chunk cache");
    }
    return true;
}

bool BlockIndexFilter::Close() {
    if (block_chunk_cache_) {
        block_chunk_cache_->Close();
        block_chunk_cache_ = NULL;
    }
    return Filter::Close();
}

bool BlockIndexFilter::SetOption(const string& option_name, const string& option) {
    if (option_name == "block-chunk-cache") {
        Option<bool> b = To<bool>(option);
        CHECK(b.valid(), "Illegal option value: " << option_name << "=" << option);
        use_block_chunk_cache_ = b.value();
        return true;
    }
    if (StartsWith(option_name, "block-chunk-cache.")) {
        CHECK(this->block_chunk_cache_, "Block chunk cache not set");
        CHECK(this->block_chunk_cache_->SetOption(option_name.substr(strlen("block-chunk-cache.")),
                option), "Configuration failed");
        return true;
    }
    return Filter::SetOption(option_name, option);
}

Filter::filter_result BlockIndexFilter::Check(Session* session,
                                              const BlockMapping* block_mapping,
                                              ChunkMapping* mapping,
                                              dedupv1::base::ErrorContext* ec) {
    ProfileTimer timer(this->stats_.time_);
    SlidingAverageProfileTimer timer2(this->stats_.average_latency_);

    DEBUG("Check old block mapping for chunk: " <<
        "chunk " << mapping->DebugString() <<
        ", block mapping " << (block_mapping ? block_mapping->DebugString() : "null"));

    enum filter_result result = FILTER_ERROR;
    if (block_mapping == NULL) { /* Not sure if really needed */
        result = FILTER_WEAK_MAYBE;
    } else {
        this->stats_.reads_++;
        list<BlockMappingItem>::const_iterator i;
        for (i = block_mapping->items().begin(); i != block_mapping->items().end(); i++) {

            /*
             * Check for each block mapping item if the chunk mappings (new)
             * fingerprint is known. If this is the case it is a strong indication
             * that the fingerprint chunk is known.
             */
            if (raw_compare(i->fingerprint(), i->fingerprint_size(),
                    mapping->fingerprint(),
                    mapping->fingerprint_size()) == 0) {
                mapping->set_data_address(i->data_address());

                TRACE("Found in previous block: " <<
                    "block mapping item " << i->DebugString() <<
                    ", chunk mapping " << mapping->DebugString());

                this->stats_.hits_++;
                result = FILTER_STRONG_MAYBE;
                break;
            }
        }
    }
    if (result == FILTER_ERROR && block_mapping && use_block_chunk_cache_) {
        // result not overwritten

        uint64_t data_address = 0;
        if (block_chunk_cache_->Contains(mapping, block_mapping->block_id(), &data_address)) {
            mapping->set_data_address(data_address);
            this->stats_.hits_++;
            result = FILTER_STRONG_MAYBE;
        }

    }

    if (result == FILTER_ERROR) {
        // result still not overwritten

        // We cannot make any real statement
        this->stats_.miss_++;
        result = FILTER_WEAK_MAYBE;
    }

    return result;
}

bool BlockIndexFilter::UpdateKnownChunk(Session* session,
                                        const dedupv1::blockindex::BlockMapping* block_mapping,
                                        ChunkMapping* mapping,
                                        dedupv1::base::ErrorContext* ec) {
    ProfileTimer timer(this->stats_.time_);

    DCHECK(mapping, "Mapping must be set");

    if (!use_block_chunk_cache_) {
        return true;
    }
    if (Fingerprinter::IsEmptyDataFingerprint(mapping->fingerprint(), mapping->fingerprint_size())) {
        return true;
    }

    DEBUG("Filter update: " <<
        "chunk " << mapping->DebugString() <<
        ", block mapping " << (block_mapping ? block_mapping->DebugString() : "null"));

    if (block_chunk_cache_ && block_mapping) {
        CHECK(block_chunk_cache_->UpdateKnownChunk(mapping, block_mapping->block_id()),
            "Failed to update block chunk cache");
    }

    return true;
}

bool BlockIndexFilter::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    BlockIndexFilterStatsData data;
    data.set_hit_count(this->stats_.hits_);
    data.set_miss_count(this->stats_.miss_);
    data.set_read_count(this->stats_.reads_);
    CHECK(ps->Persist(prefix, data), "Failed to persist block index filter stats");
    if (block_chunk_cache_) {
        CHECK(block_chunk_cache_->PersistStatistics(
                prefix + ".block-chunk-cache", ps),
            "Failed to persist block chunk cache stats");
    }
    return true;
}

bool BlockIndexFilter::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    BlockIndexFilterStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore block index filter stats");
    this->stats_.reads_ = data.read_count();
    this->stats_.hits_ = data.hit_count();
    this->stats_.miss_ = data.miss_count();

    if (block_chunk_cache_) {
        CHECK(block_chunk_cache_->RestoreStatistics(prefix + ".block-chunk-cache",
                ps), "Failed to restore block chunk cache stats");
    }
    return true;
}

string BlockIndexFilter::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    if (block_chunk_cache_) {
        sstr << "\"block chunk cache\": " <<
        block_chunk_cache_->PrintStatistics() << "," << std::endl;
    } else {
        sstr << "\"block chunk cache\": null," << std::endl;
    }
    sstr << "\"reads\": " << this->stats_.reads_ << "," << std::endl;
    sstr << "\"strong\": " << this->stats_.hits_ << "," << std::endl;
    sstr << "\"weak\": " << this->stats_.miss_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string BlockIndexFilter::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    if (block_chunk_cache_) {
        sstr << "\"block chunk cache\": " <<
        block_chunk_cache_->PrintTrace() << std::endl;
    } else {
        sstr << "\"block chunk cache\": null" << std::endl;
    }
    sstr << "}";
    return sstr.str();

}

string BlockIndexFilter::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    if (block_chunk_cache_) {
        sstr << "\"block chunk cache\": " <<
        block_chunk_cache_->PrintProfile() << "," << std::endl;
    } else {
        sstr << "\"block chunk cache\": null," << std::endl;
    }
    sstr << "\"used time\": " << this->stats_.time_.GetSum() << "," << std::endl;
    sstr << "\"average latency\": " << this->stats_.average_latency_.GetAverage() <<
    std::endl;
    sstr << "}";
    return sstr.str();
}

}
}
