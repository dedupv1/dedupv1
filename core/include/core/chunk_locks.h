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

#ifndef CHUNK_LOCKS_H_
#define CHUNK_LOCKS_H_


#include <tbb/atomic.h>
#include "tbb/recursive_mutex.h"

#include <vector>
#include <string>

#include <core/dedup.h>
#include <base/locks.h>
#include <base/profile.h>
#include <core/statistics.h>

namespace dedupv1 {
namespace chunkindex {

/**
 * The chunk locks protect a chunks against concurrent accesses.
 *
 * A client is not allowed to held more than a single chunk lock. This is
 * required to avoid deadlocks.
 */
class ChunkLocks : public dedupv1::StatisticProvider {
    private:
    /**
     * Type for statistics about chunk locks.
     */
    class Statistics {
        public:
        Statistics();

        tbb::atomic<uint32_t> lock_free_;
        tbb::atomic<uint32_t> lock_busy_;

        /**
         * Profiling information about the chunk lock contention
         */
        dedupv1::base::Profile profiling_lock_;

        /**
         * Current number of locks held by client threads.
         */
        tbb::atomic<uint32_t> held_count_;
    };
    public:
    /**
     * Default number of chunk locks
     */
    static const size_t kDefaultChunkLocks = 1021;

    private:
    /**
     * A series of read write locks to ensure
     * that at each point in time only a single thread/user writes to a
     * block.
     */
    std::vector<tbb::recursive_mutex> locks_;

    /**
     * Number of chunk locks.
     */
    uint32_t chunk_lock_count_;

    /**
     * Statistics about the chunk locks.
     */
    Statistics stats_;

    bool started_;

    unsigned int GetLockIndex(const void* fp, size_t fp_size);

    public:
    /**
     * Constructor
     *
     * @return
     */
    ChunkLocks();

    /**
     * Destructor
     */
    virtual ~ChunkLocks() {
    }

    /**
     * Configures the chunk locks.
     *
     * Available options:
     * - count: StorageUnit, >0
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Starts the chunk locks. The locking operations are available after the
     * start.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool Start(const StartContext& start_context);

    /**
     * Prints profile statistics.
     * @return
     */
    std::string PrintProfile();

    /**
     * Prints lock statistics.
     * @return
     */
    std::string PrintLockStatistics();

    /**
     * Locks the given chunk.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool Lock(const void* fp, size_t fp_size);

    /**
     * Tries to acquire the lock of the given chunk.
     *
     * @param fp
     * @param fp_size
     * @param locked
     * @return true iff ok, otherwise an error has occurred
     */
    bool TryLock(const void* fp, size_t fp_size, bool* locked);

    /**
     * Unlocks the given chunk id.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool Unlock(const void* fp, size_t fp_size);
};

}
}

#endif /* CHUNK_LOCKS_H_ */
