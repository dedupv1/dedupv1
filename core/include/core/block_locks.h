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

#ifndef BLOCK_LOCKS_H__
#define BLOCK_LOCKS_H__

#include <tbb/atomic.h>

#include <vector>
#include <string>
#include <list>

#include <core/dedup.h>
#include <base/locks.h>
#include <base/profile.h>
#include <core/statistics.h>

namespace dedupv1 {

/**
 * The block locks protect a block against concurrent accesses so that
 * an consistent state of a block mapping is possible.
 *
 * A client may hold two adjacent locks. e.g. the lock for block i and the lock for block i + 1 iff the
 * lock for block i was acquired before the block for i + 1. This avoids deadlocks. Every other usage should
 * use trying methods to acquire locks so that no deadlocks occur.
 *
 * It is not allowed to change block mappings in the block index without holding
 * a write block lock. There is an exception: Non content-changes, especially the
 * event log id are allowed if the user uses compare-and-swap operations to avoid
 * accidental overwritten of existing data.
 **/
class BlockLocks : public dedupv1::StatisticProvider {
    private:
    /**
     * Type for statistics about block locks.
     */
    class Statistics {
        public:
        Statistics();

        tbb::atomic<uint32_t> block_lock_read_free_;
        tbb::atomic<uint32_t> block_lock_read_busy_;
        tbb::atomic<uint32_t> block_lock_write_busy_;
        tbb::atomic<uint32_t> block_lock_write_free_;

        /**
         * Profiling information about the block lock contention
         */
        dedupv1::base::Profile profiling_lock_;

        /**
         * Current number of locks held by client threads.
         */
        tbb::atomic<uint32_t> write_held_count_;
        tbb::atomic<uint32_t> read_held_count_;

        tbb::atomic<uint32_t> read_waiting_count_;
        tbb::atomic<uint32_t> write_waiting_count_;
    };
    public:
    /**
     * Default number of block locks
     */
    static const size_t kDefaultBlockLocks = 1021;

    /**
     * Flag denoting that a lock is not held by a thread.
     */
    static const uint64_t kLockNotHeld = -1;

    static const uint64_t kLockHeldByUnknown = -2;
    private:
    /**
     * A series of "block_lock_count" read write locks to ensure
     * that at each point in time only a single thread/user writes to a
     * block.
     *
     * While this is real burden for large blocks (around 10-20% of the
     * overall time), the alternative (an
     * Eventual Consistency model) would be extremely complex.
     */
    dedupv1::base::ReadWriteLockVector block_locks_;

    /**
     * Number of block locks.
     */
    uint32_t block_lock_count_;

    /**
     * Statistics about the block locks.
     */
    Statistics stats_;

    /**
     * Stores the current lock holder if a block lock is acquired.
     * If a lock is not locked or locked for read,  the entry is set to kLockNotHeld.
     * It is impossible for track the read lock block ids, because there is no single exclusive owner for a
     * read lock.
     *
     */
    std::vector<uint64_t> lock_holder_;

    /**
     * Get the matching lock for the given block id
     */
    unsigned int GetLockIndex(uint64_t block_id);

    public:
    /**
     * Constructor
     *
     * @return
     */
    BlockLocks();

    virtual ~BlockLocks() {
    }

    /**
     * Configures the block locks.
     *
     * Available options:
     * - count: uint32_t, >0
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Starts the block locks. The locking operations are available after the
     * start.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool Start();

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

    std::string PrintTrace();

    /**
     * Locks the given block for writing.
     *
     * Set the lock location information (function, file, line) via the macro LOCK_LOCATION_INFO.
     *
     * @param block_id
     * @return true iff ok, otherwise an error has occurred
     */
    bool WriteLock(uint64_t block_id, LOCK_LOCATION_PARAM);

    /**
     * Locks the given blocks for writing.
     *
     * Orders the locks in an order of the lock numbers to prevent deadlocks
     * @return true iff ok, otherwise an error has occurred
     */
    bool TryWriteLocks(const  std::list<uint64_t>& blocks,  std::list<uint64_t>* locked_blocks,  std::list<uint64_t>* unlocked_blocks,
            LOCK_LOCATION_PARAM);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool WriteUnlocks(const std::list<uint64_t>& blocks, LOCK_LOCATION_PARAM);

    /**
     * Tries to acquire the write lock of the given block.
     *
     * Set the lock location information (function, file, line) via the macro LOCK_LOCATION_INFO.
     *
     * @param block_id
     * @param locked
     * @return true iff ok, otherwise an error has occurred
     */
    bool TryWriteLock(uint64_t block_id, bool* locked, LOCK_LOCATION_PARAM);

    /**
     * Unlocks the given block id.
     *
     * @param block_id
     * @return true iff ok, otherwise an error has occurred
     */
    bool ReadUnlock(uint64_t block_id, LOCK_LOCATION_PARAM);

    /**
     * Unlocks the given block id.
     *
     * @param block_id
     * @return true iff ok, otherwise an error has occurred
     */
    bool WriteUnlock(uint64_t block_id, LOCK_LOCATION_PARAM);

    /**
     * Locks the given block for reading.
     *
     * Set the lock location information (function, file, line) via the macro LOCK_LOCATION_INFO.
     *
     * @param block_id
     * @return true iff ok, otherwise an error has occurred
     */
    bool ReadLock(uint64_t block_id, LOCK_LOCATION_PARAM);

    /**
     * Tries to acquire the read lock of the given block.
     *
     * Set the lock location information (function, file, line) via the macro LOCK_LOCATION_INFO.
     *
     * @param block_id
     * @param locked
     * @return true iff ok, otherwise an error has occurred
     */
    bool TryReadLock(uint64_t block_id, bool* locked, LOCK_LOCATION_PARAM);

    /**
     *
     * @return true iff the lock of the given block held by the current thread for writing. There is a chance
     * of a false positive error where the current has not acquired the lock for this block but for another
     * block mapped to the same thread.
     *
     */
    bool IsHeldForWrites(uint64_t block_id);
};

}

#endif  // BLOCK_LOCKS_H__
