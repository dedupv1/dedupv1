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

#ifndef FILTER_CHAIN_H__
#define FILTER_CHAIN_H__

#include <tbb/atomic.h>

#include <base/profile.h>
#include <core/chunk_mapping.h>
#include <core/block_mapping.h>
#include <core/filter.h>

#include <list>
#include <vector>

/**
 * \defgroup filterchain Filter Chain
 * The filter chain detects whether a chunk is a duplicate.
 * A series of different filters can be executed and the result of the steps determines the further execution.
 * Each filter step returns with one of the following results:
 * - EXISTING: The current chunk is a duplicate, e.g. the filter has performed a byte-wise comparison.
 * - STRONG-MAYBE: The current chunk is a duplicate with very high probability. This is the case
 *   after a fingerprint comparison. Only filters that can deliver EXISTING should be executed afterwards.
 * - WEAK-MAYBE: The filter cannot make any statement about the duplication state of the chunk.
 * - NOT-EXISTING: The filter rules out the possibility that the chunk is already known, e.g.
 *   after a Chunk Index lookup with a negative result.
 *
 * When a new chunk is found, the filter chain is executed a second time so that filters can
 * update their internal state.
 */

namespace dedupv1 {

class DedupSystem;

namespace filter {

/**
 *  \ingroup filterchain
 * Class that maintains the filter chain.
 * All filters in the filter chain are managed by this object.
 * It controls the execution.
 */
class FilterChain : public StatisticProvider {
private:
    DISALLOW_COPY_AND_ASSIGN(FilterChain);

    /**
     * \ingroup filterchain
     * Statistics for the filter chain
     */
    class Statistics {
public:
        /**
         * Constructor
         */
        Statistics();

        /**
         * Number of reads of the filter chain
         */
        tbb::atomic<uint64_t> index_reads_;

        /**
         * Number of updates of the filter chain
         */
        tbb::atomic<uint64_t> index_writes_;

        /**
         * Total time spent checking the filter chain
         */
        dedupv1::base::Profile check_time_;

        /**
         * Total time spent updating the filter chain
         */
        dedupv1::base::Profile update_time_;
    };

    /**
     * The chain of filters
     */
    std::list<Filter*> chain_;

    /**
     * The last created filter.
     * Used during the configuration phase
     */
    Filter* last_filter_;

    /**
     * Pointer to statistics for the filter_chain
     */
    Statistics stats_;

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool CheckChunk(dedupv1::Session* session,
                    const dedupv1::blockindex::BlockMapping* block_mapping,
                    dedupv1::chunkindex::ChunkMapping* chunk_mapping,
                    dedupv1::base::ErrorContext* ec);
public:
    /**
     * Constructor
     * @return
     */
    FilterChain();

    virtual ~FilterChain();

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Adds a new filter type to the filter chain
     * @param filter_type
     * @return true iff ok, otherwise an error has occurred
     */
    bool AddFilter(const std::string& filter_type);

    /**
     * Starts the filter chain.
     *
     * @param start_context
     * @param system
     * @return true iff ok, otherwise an error has occurred
     */
    bool Start(DedupSystem* system);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool Close();

    /**
     * Should only be called with a matching and successful ReadChunkInfo call
     * before.
     *
     * Once the StoreChunkInfo call is started, the the Update call is
     * done for all chunk mappings and all filters of the filter chain.
     * If one of these Update calls fails, an error is logged, all other filters
     * are executed and false is returned at the end.
     *
     * @param session
     * @param chunk_mapping
     * @param ec Error context that can be filled if case of special errors
     * @return true iff ok, otherwise an error has occurred
     */
    bool StoreChunkInfo(
        dedupv1::Session* session,
        const dedupv1::blockindex::BlockMapping* block_mapping,
        dedupv1::chunkindex::ChunkMapping* chunk_mapping,
        dedupv1::base::ErrorContext* ec);

    /**
     *
     * Wenn a Read filter call fails in the middle of the processing, the
     * Abort filter call is executed for all chunk mappings and all filters
     * (except the failed one) that have received a Read call.
     *
     * @param session
     * @param block_mapping current block mapping, can be NULL
     * @param chunk_mapping
     * @param ec Error context that can be filled if case of special errors
     * @return true iff ok, otherwise an error has occurred
     */
    bool ReadChunkInfo(dedupv1::Session* session,
                       const dedupv1::blockindex::BlockMapping* block_mapping,
                       dedupv1::chunkindex::ChunkMapping* chunk_mapping,
                       dedupv1::base::ErrorContext* ec);

    /**
     * Not implemented well. O(number of filters) operation
     */
    virtual dedupv1::filter::Filter* GetFilterByName(const std::string& name);

    /**
     * Called when something went wrong between the read of the chunk info for a set of
     * chunk mappings and the storing of the chunk infos.
     *
     * @param session
     * @param chunk_mapping
     * @param ec Error context that can be filled if case of special errors
     * @return true iff ok, otherwise an error has occurred
     */
    bool AbortChunkInfo(
        dedupv1::Session* session,
        const dedupv1::blockindex::BlockMapping* block_mapping,
        dedupv1::chunkindex::ChunkMapping* chunk_mapping,
        dedupv1::base::ErrorContext* ec);

    virtual bool PersistStatistics(std::string prefix,
                                   dedupv1::PersistStatistics* ps);

    virtual bool RestoreStatistics(std::string prefix,
                                   dedupv1::PersistStatistics* ps);

    virtual std::string PrintLockStatistics();
    virtual std::string PrintProfile();
    virtual std::string PrintStatistics();
    virtual std::string PrintTrace();

    /**
     * Returns the list of configured filters
     */
    inline const std::list<Filter*>& GetChain();
};

const std::list<Filter*>& FilterChain::GetChain() {
    return chain_;
}

}
}

#endif  // FILTER_CHAIN_H__
