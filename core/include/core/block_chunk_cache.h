/*
 * dedupv1 - iSCSI based Deduplication System for Linux
 *
 * (C) 2008 Dirk Meister
 * (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
 * (C) 2012, Dirk Meister, Johannes Gutenberg University Mainz
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
#ifndef BLOCK_CHUNK_CACHE_H__
#define BLOCK_CHUNK_CACHE_H__

#include <tbb/atomic.h>

#include <core/dedup.h>
#include <core/filter.h>
#include <core/chunk_index_in_combat.h>
#include <base/profile.h>
#include <base/strutil.h>
#include <base/cache_strategy.h>
#include <core/block_index.h>
#include <base/locks.h>
#include <base/hashing_util.h>

#include <tr1/unordered_map>
#include <tbb/concurrent_hash_map.h>

#include <string>
#include <map>
#include <set>
namespace dedupv1 {

namespace filter {

class BlockChunkCache {
private:
    dedupv1::blockindex::BlockIndex* block_index_;
    uint32_t diff_cache_size_;
    uint32_t block_cache_size_;
    uint32_t prefetchWindow_;

    uint32_t min_diff_value_;
    bool remove_diff_value_on_lookup_not_found_;

    std::tr1::unordered_map<int64_t, int> diff_map_;
    dedupv1::base::LRUCacheStrategy<int64_t> diff_map_cache_strategy_;
    tbb::spin_mutex diff_mutex_;

    struct ChunkMapData {
        std::set<uint64_t> block_set;
        uint64_t data_adress;
    };

    tbb::concurrent_hash_map<bytestring, ChunkMapData, dedupv1::base::bytestring_fp_murmur_hash> block_chunk_map_;
    tbb::concurrent_hash_map<uint64_t, std::set<bytestring> > block_map_[4];

    dedupv1::base::LRUCacheStrategy<uint64_t> block_map_cache_strategy_[4];
    tbb::spin_mutex block_map_cache_mutex_[4];

    dedupv1::base::MutexLock fetch_lock_[4];

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
         * Profiling information (filter time in ms)
         */
        dedupv1::base::Profile time_;
        dedupv1::base::Profile fetch_time_;
        dedupv1::base::Profile lock_time_;

        dedupv1::base::Profile block_handling_time_;
        dedupv1::base::Profile diff_handling_time_;
        dedupv1::base::Profile diff_iteration_time_;

        /**
         * Number of filter reads
         */
        tbb::atomic<uint64_t> fetch_;

        /**
         * Number of times the filter check hits
         */
        tbb::atomic<uint64_t> hits_;

        /**
         * Number of times the filter check misses
         */
        tbb::atomic<uint64_t> miss_;

        tbb::atomic<uint64_t> block_lookup_missing_;

        tbb::atomic<uint64_t> block_evict_count_;

        tbb::atomic<uint64_t> diff_evict_count_;

        tbb::atomic<uint64_t> no_hint_count_;
    };

    Statistics stats_;

    bool EvictBlock(uint64_t event_block_id);
    dedupv1::base::lookup_result FetchBlockIntoCache(uint64_t fetch_block_id);

    bool TouchBlock(uint64_t block_id);
    bool TouchDiff(int64_t diff, bool allow_insert);
public:
    BlockChunkCache();

    bool Start(dedupv1::blockindex::BlockIndex* block_index);

    bool SetOption(const std::string& option_name, const std::string& option);

    bool Close();

    bool Contains(const dedupv1::chunkindex::ChunkMapping* mapping, uint64_t current_block_id, uint64_t* data_address);

    bool UpdateKnownChunk(const dedupv1::chunkindex::ChunkMapping* mapping, uint64_t current_block_id);

    std::string PrintStatistics();

    std::string PrintProfile();

    std::string PrintTrace();

    bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);
};

} // namespace
} // namespace

#endif  // BLOCK_CHUNK_CACHE_H__
