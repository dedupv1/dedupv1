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

#ifndef CHUNK_INDEX_IN_COMPAT_H_
#define CHUNK_INDEX_IN_COMPAT_H_

#include <core/dedup.h>
#include <base/startup.h>
#include <core/log.h>
#include <core/statistics.h>
#include <base/bloom_set.h>
#include <base/option.h>
#include <base/profile.h>

#include <map>
#include <tbb/spin_rw_mutex.h>

namespace dedupv1 {
namespace chunkindex {

/**
 * Maintains informations about all chunks that have been referenced by block mappings, but are not yet processed by the log
 * replay.
 *
 * An chunk is than in combat if the usage count has or might be increased.
 *
 * This information is used by e.g. the garbage collection to ensure that the usage counter of a chunk in the chunk index is still
 * the correct usage counter
 *
 *  During a crash, the in-combats state is lost, However, we can now recover the exact state when we observe
 *  all dirty replay events. It allows the gc of tons of chunks that are really not used anymore much faster.
 *  Because we do not have to wait until the log is fully replayed. In some sense, the restart/crash can reduce the t
 *  ime-to-gc because the rebuild bloom filter is more uptodate than the bloom filter before the restart/crash.
 */
class ChunkIndexInCombats : public dedupv1::StatisticProvider {
    private:

    static const size_t kDefaultMaxSize = 1024 * 1024;

    /**
     * Map storing all in-combat chunks.
     *
     * We define a chunk as in-combat if after the
     * candidate processing starts a chunk is used to that its usage count
     * might have changed.
     *
     */
    dedupv1::base::BloomSet* in_combat_chunks_;

    /**
     * Approximate number of chunks that are in combat
     */
    tbb::atomic<uint64_t> in_combat_count_;

    /**
     * Size of the bloom set for the in combat chunks in byte
     */
    size_t size_;

    /**
     * Number of hash functions to use
     */
    uint8_t k_;

    /**
     * Used for auto-configuration
     */
    uint64_t capacity_;

    /**
     * Used for auto-configuration
     */
    double error_rate_;

    /**
     * Log to use.
     */
    dedupv1::log::Log* log_;

    /**
     * Statistics about the in-combats
     */
    class Statistics {
        public:
            dedupv1::base::Profile touch_time_;

            dedupv1::base::Profile contains_time_;
    };

    Statistics stats_;

    public:

    /**
     * Constructor
     */
    ChunkIndexInCombats();

    /**
     * Destructor
     */
    virtual ~ChunkIndexInCombats();

    /**
     * Configures the in-combat chunk data
     *
     * Available options:
     * - size: StorageUnit
     * - k: uint32_t
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool Start(const StartContext& start_context, dedupv1::log::Log* log);

    /**
     * Called while holding the chunk lock of the fingerprint.
     *
     * @param fp
     * @param fp_size
     * @return true iff ok, otherwise an error has occurred
     */
    bool Touch(const void* fp, size_t fp_size);

    /**
     * Checks if the given fp is in-combat
     */
    dedupv1::base::Option<bool> Contains(const void* fp, size_t fp_size);

    /**
     * Clears the in-combats
     * @return true iff ok, otherwise an error has occurred
     */
    bool Clear();

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool LogReplay(dedupv1::log::event_type event_type, 
            const LogEventData& event_value,
            const dedupv1::log::LogReplayContext& context);

    /**
     * Prints debugging/tracing information about the in-combat data
     */
    virtual std::string PrintTrace();

    /**
     * Prints profile information about the in-combat data
     */
    virtual std::string PrintProfile();
};

}
}

#endif /* CHUNK_INDEX_IN_COMPAT_H_ */
