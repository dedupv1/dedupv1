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

#include <fcntl.h>
#include <unistd.h>

#include <sstream>
#include <limits>

#include <core/dedup.h>
#include <core/chunk_index_sampling_strategy.h>
#include <base/logging.h>
#include <base/bitutil.h>
#include <base/strutil.h>

LOGGER("ChunkIndex");

using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::strutil::To;

namespace dedupv1 {
namespace chunkindex {

MetaFactory<ChunkIndexSamplingStrategy> ChunkIndexSamplingStrategy::factory_(
    "ChunkIndexSamplingStrategy", "chunk index sampling strategy");

MetaFactory<ChunkIndexSamplingStrategy>& ChunkIndexSamplingStrategy::Factory() {
    return factory_;
}

ChunkIndexSamplingStrategy::ChunkIndexSamplingStrategy() {
}

ChunkIndexSamplingStrategy::~ChunkIndexSamplingStrategy() {
}

bool ChunkIndexSamplingStrategy::Init() {
    return true;
}

bool ChunkIndexSamplingStrategy::Close() {
    delete this;
    return true;
}

bool ChunkIndexSamplingStrategy::SetOption(const std::string& option_name,
                                           const std::string& option) {
    ERROR("Illegal option: " << option_name << "=" << option);
    return false;
}

bool ChunkIndexSamplingStrategy::Start(const dedupv1::StartContext& start_context,
                                       DedupSystem* system) {
    return true;
}

FullChunkIndexSamplingStrategy::FullChunkIndexSamplingStrategy() {
}

FullChunkIndexSamplingStrategy::~FullChunkIndexSamplingStrategy() {
}

Option<bool> FullChunkIndexSamplingStrategy::IsAnchor(const ChunkMapping& mapping) {
    return dedupv1::base::make_option(true);
}

ChunkIndexSamplingStrategy* FullChunkIndexSamplingStrategy::CreateStrategy() {
    return new FullChunkIndexSamplingStrategy();
}

void FullChunkIndexSamplingStrategy::RegisterStrategy() {
    ChunkIndexSamplingStrategy::Factory().Register("full",
        &FullChunkIndexSamplingStrategy::CreateStrategy);
}

SuffixMaskChunkIndexSamplingStrategy::SuffixMaskChunkIndexSamplingStrategy() :
    sampling_factor_(0), sampling_mask_(0) {
}

SuffixMaskChunkIndexSamplingStrategy::~SuffixMaskChunkIndexSamplingStrategy() {
}

bool SuffixMaskChunkIndexSamplingStrategy::SetOption(const std::string& option_name,
                                                     const std::string& option) {
    if (option_name == "factor") {
        Option<uint32_t> o = To<uint32_t>(option);
        CHECK(o.valid(), "Illegal sampling factor: Should be an integer");
        CHECK(!o.value() == 0, "Illegal sampling factor: Should not be zero");
        bool is_power = !(o.value() & (o.value() - 1));
        CHECK(is_power, "Illegal sampling factor: Should be a power of two");
        uint32_t sampling_factor_bits = dedupv1::base::bits(sampling_factor_);
        CHECK(sampling_factor_bits <= 63, "Illegal sampling factor");
        sampling_factor_ = o.value();
        return true;
    }
    return ChunkIndexSamplingStrategy::SetOption(option_name, option);
}

bool SuffixMaskChunkIndexSamplingStrategy::Start(const dedupv1::StartContext& start_context,
                                                 DedupSystem* system) {
    DCHECK(sampling_mask_ == 0, "Sampling mask already set");
    CHECK(sampling_factor_ != 0, "Sampling factor not set");

    sampling_mask_ = 1;
    for (int i = 1; i < dedupv1::base::bits(sampling_factor_); i++) {
        sampling_mask_ <<= 1;
        sampling_mask_ |= 1;
    }
    DEBUG("Started chunk index sampling strategy: " <<
        "sampling factor " << sampling_factor_ <<
        ", sampling mask " << sampling_mask_);
    return true;
    return true;
}

Option<bool> SuffixMaskChunkIndexSamplingStrategy::IsAnchor(const ChunkMapping& mapping) {
    DCHECK(sampling_mask_ != 0, "Sampling mask not set");
    DCHECK(mapping.fingerprint_size() >= 4, "Illegal fingerprint");

    uint32_t suffix;
    memcpy(&suffix,
        mapping.fingerprint() + (mapping.fingerprint_size() - 4),
        4);
    if ((suffix & sampling_mask_) == sampling_mask_) {
        TRACE("Fingerprint is anchor: " << mapping.DebugString());
        return make_option(true);
    } else {
        TRACE("Fingerprint is no anchor: " << mapping.DebugString());
        return make_option(false);
    }
}

ChunkIndexSamplingStrategy* SuffixMaskChunkIndexSamplingStrategy::CreateStrategy() {
    return new SuffixMaskChunkIndexSamplingStrategy();
}

void SuffixMaskChunkIndexSamplingStrategy::RegisterStrategy() {
    ChunkIndexSamplingStrategy::Factory().Register("sampling",
        &SuffixMaskChunkIndexSamplingStrategy::CreateStrategy);
}

}
}
