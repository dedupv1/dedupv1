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

/**
 * @file block_index_filter.h
 * @brief Block Index Filter
 *
 * A filter that uses the block mapping of the current block as
 * an additional filter. The filter is nearly free of cost, but does only
 * work in very limited situations.
 */

#ifndef BLOCK_INDEX_FILTER_H__
#define BLOCK_INDEX_FILTER_H__

#include <tbb/atomic.h>

#include <core/dedup.h>
#include <core/filter.h>
#include <core/chunk_index_in_combat.h>
#include <base/profile.h>
#include <base/strutil.h>
#include <core/block_index.h>
#include <core/block_chunk_cache.h>

#include <string>

namespace dedupv1 {

namespace filter {

/**
 * The block index filter uses the block mapping of the current block as
 * an additional filter. The filter is nearly free of cost, but does only
 * work in very limited situations.
 *
 * \ingroup filterchain
 */
class BlockIndexFilter : public Filter {
private:

    /**
     * Statistics about the block index filter
     */
    class Statistics {
public:
        /**
         * Constructor for the statistics
         * @return
         */
        Statistics();

        /**
         * Number of filter reads
         */
        tbb::atomic<uint64_t> reads_;

        /**
         * Number of times the filter check hits
         */
        tbb::atomic<uint64_t> hits_;

        /**
         * Number of times the filter check misses
         */
        tbb::atomic<uint64_t> miss_;

        /**
         * Profiling information (filter time in ms)
         */
        dedupv1::base::Profile time_;

        /**
         * Profiling information (filter latency in ms)
         */
        dedupv1::base::SimpleSlidingAverage average_latency_;
    };

    /**
     * Statistics about the block index filter
     */
    Statistics stats_;

    blockindex::BlockIndex* block_index_;

    BlockChunkCache* block_chunk_cache_;

    bool use_block_chunk_cache_;
public:
    /**
     * Constructor.
     */
    BlockIndexFilter();

    /**
     * Destructor
     */
    virtual ~BlockIndexFilter();

    bool Start(DedupSystem* system);

    bool Close();

    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Performs a check by searching the chunk mapping to check in the current block mapping (items).
     * This way of optimizing the duplication detection is limited to certain situations, but takes nearly no time and no I/O.
     *
     * @param session current user session (Unused in this method)
     * @param block_mapping Unchanged (aka old) block mapping for the current block (CanBeNull)
     * @param mapping chunk data to check for duplication (NotNull, Unchecked)
     * @param ec Error context that can be filled if case of special errors
     */
    virtual enum filter_result Check(dedupv1::Session* session,
                                     const dedupv1::blockindex::BlockMapping* block_mapping,
                                     dedupv1::chunkindex::ChunkMapping* mapping,
                                     dedupv1::base::ErrorContext* ec);

    virtual bool UpdateKnownChunk(dedupv1::Session* session,
                                  const dedupv1::blockindex::BlockMapping* block_mapping,
                                  dedupv1::chunkindex::ChunkMapping* mapping,
                                  dedupv1::base::ErrorContext* ec);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * Print statistics.
     * @return
     */
    virtual std::string PrintStatistics();

    /**
     * Print profile informations about the usage of the block index filter.
     *
     * @return
     */
    virtual std::string PrintProfile();

    /**
     * Print trace information about the block index filter.
     */
    virtual std::string PrintTrace();

    /**
     * @internal
     * Creates a new block index filter. The static method
     * is used for the filter factory.
     * @return
     */
    static Filter* CreateFilter();

    /**
     * Registers this filter at the filter factory.
     */
    static void RegisterFilter();

    DISALLOW_COPY_AND_ASSIGN(BlockIndexFilter);
};

} // namespace
} // namespace

#endif  // BLOCK_INDEX_FILTER_H__
