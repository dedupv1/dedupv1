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
#ifndef CHUNK_INDEX_SAMPLING_STRATEGY
#define CHUNK_INDEX_SAMPLING_STRATEGY

#include <core/dedup.h>
#include <base/factory.h>
#include <core/chunk_mapping.h>
#include <base/option.h>
#include <base/startup.h>

namespace dedupv1 {

class DedupSystem;

namespace chunkindex {

class ChunkIndexSamplingStrategy {
    DISALLOW_COPY_AND_ASSIGN(ChunkIndexSamplingStrategy);
public:
    ChunkIndexSamplingStrategy();

    virtual bool Init();

    virtual ~ChunkIndexSamplingStrategy();

    virtual bool Close();

    virtual bool SetOption(const std::string& option_name, const std::string& option);

    virtual bool Start(const dedupv1::StartContext& start_context,
                       DedupSystem* system);

    virtual dedupv1::base::Option<bool> IsAnchor(const ChunkMapping& mapping) = 0;

    static MetaFactory<ChunkIndexSamplingStrategy>& Factory();
private:
    static MetaFactory<ChunkIndexSamplingStrategy> factory_;
};

class FullChunkIndexSamplingStrategy : public ChunkIndexSamplingStrategy {
    DISALLOW_COPY_AND_ASSIGN(FullChunkIndexSamplingStrategy);
public:
    FullChunkIndexSamplingStrategy();

    virtual ~FullChunkIndexSamplingStrategy();

    virtual dedupv1::base::Option<bool> IsAnchor(const ChunkMapping& mapping);

    static ChunkIndexSamplingStrategy* CreateStrategy();

    static void RegisterStrategy();
};

class SuffixMaskChunkIndexSamplingStrategy : public ChunkIndexSamplingStrategy {
    DISALLOW_COPY_AND_ASSIGN(SuffixMaskChunkIndexSamplingStrategy);
public:
    SuffixMaskChunkIndexSamplingStrategy();

    virtual ~SuffixMaskChunkIndexSamplingStrategy();

    virtual bool SetOption(const std::string& option_name, const std::string& option);

    virtual bool Start(const dedupv1::StartContext& start_context,
                       DedupSystem* system);

    virtual dedupv1::base::Option<bool> IsAnchor(const ChunkMapping& mapping);

    static ChunkIndexSamplingStrategy* CreateStrategy();

    static void RegisterStrategy();
private:
    uint32_t sampling_factor_;

    uint64_t sampling_mask_;
};

}
}

#endif
