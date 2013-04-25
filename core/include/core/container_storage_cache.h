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

#ifndef CONTAINER_STORAGE_CACHE_H__
#define CONTAINER_STORAGE_CACHE_H__

#include <core/statistics.h>
#include <core/dedup.h>

#include <base/profile.h>
#include <base/locks.h>
#include <base/index.h>
#include <base/strutil.h>

#include <tbb/spin_rw_mutex.h>
#include <tbb/spin_mutex.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/tick_count.h>
#include <tbb/atomic.h>

#include <vector>
#include <string>
#include <map>

namespace dedupv1 {
namespace chunkstore {

class ContainerStorage;
class Container;

/**
 * Reference to an Cache entry that a client of the cache gets while requesting the cache.
 * For details when a cache entry is set, please refer to the GetCache and CheckCache method.
 */
class CacheEntry {
    private:
        /**
         * cache line used
         */
        int line_;

        /**
         * lock of the cache line
         */
        dedupv1::base::ReadWriteLock* lock_;

    public:
        /**
         * Constructor
         */
        CacheEntry() :
            line_(-1), lock_(NULL) {
        }

        /**
         * Constructor
         */
        CacheEntry(int line, dedupv1::base::ReadWriteLock* lock) :
            line_(line), lock_(lock) {
        }

        /**
         * returns the cache line
         */
        int line() const {
            return line_;
        }

        /**
         * sets the cache line
         */
        CacheEntry& set_line(int line) {
            line_ = line;
            return *this;
        }

        /**
         * returns the cache line lock
         */
        dedupv1::base::ReadWriteLock* lock() {
            return lock_;
        }

        /**
         * sets the cache line lock
         */
        CacheEntry& set_lock(dedupv1::base::ReadWriteLock* lock) {
            lock_ = lock;
            return *this;
        }

        /**
         * returns true iff the cache line is set
         */
        bool is_set() const {
            return line_ >= 0 && lock_;
        }

        /**
         * returns a developer-readable version of the cache entry
         */
        std::string DebugString() {
            if (!is_set()) {
                return "cache line <not set>";
            }
            return "cache line " + dedupv1::base::strutil::ToString(line_);
        }

        /**
         * cleary the cache entry
         */
        void clear() {
            line_ = -1;
            lock_ = NULL;
        }
};

/**
 * Read cache for the container storage.
 *
 */
class ContainerStorageReadCache: public dedupv1::StatisticProvider {
        DISALLOW_COPY_AND_ASSIGN(ContainerStorageReadCache);
    public:
        /**
         * Default size of the read cache.
         */
        static const uint32_t kDefaultReadCacheSize = 32;

        /**
         * Type for statistics about the read cache.
         */
        class Statistics {
            public:
                /**
                 * Statistics
                 */
                Statistics();

                dedupv1::base::Profile cache_check_time_;
                dedupv1::base::Profile cache_update_time_;

                tbb::atomic<uint64_t> cache_checks_;
                tbb::atomic<uint64_t> cache_updates_;

                tbb::atomic<uint32_t> read_cache_lock_busy_;
                tbb::atomic<uint32_t> read_cache_lock_free_;

                tbb::atomic<uint64_t> cache_hits_;
                tbb::atomic<uint64_t> cache_miss_;
        };
    private:
        /**
         * Statistics about the read cache
         */
        Statistics stats_;

        /**
         * Pointer to the container storage system.
         */
        ContainerStorage* storage_;

        /**
         * Number of containers in the read cache
         */
        uint32_t read_cache_size_;

        /**
         * Pointer to the read cache array. The read cache is set associative.
         *
         * The read cache is only allowed to be accessed with the matching (same id) read cache lock
         * acquired.
         */
        std::vector<Container*> read_cache_;

        /**
         * maps from the container id to the read cache entry.
         *
         * To avoid deadlocks, it is not allowed to acquire a lock on a read cache lock entry while holding
         * a lock to the reverse cache map.
         */
        tbb::concurrent_hash_map<uint64_t, int> reverse_cache_map_;

        /**
         * Last currently used time in millisecond resolution.
         */
        std::vector<tbb::tick_count> read_cache_used_time_;

        /**
         * lock to protected the read_cache_used_time field.
         * We use a rw spin lock because the critical region is so short.
         * Phases where this lock is held should not overlap with release or acquire operations of any
         * other lock.
         */
        tbb::spin_rw_mutex read_cache_used_time_lock_;

        /**
         * Locks of the read caches
         */
        dedupv1::base::ReadWriteLockVector read_cache_lock_;

        /**
         * Aquires the cache line lock in the given cache line
         */
        bool AcquireCacheLineLock(uint64_t future_container_id, int cache_line, CacheEntry* entry);

        /**
         * Here the caller still holds a write lock and the reverse cache map lock for the container id
         * @param accessor held accessor to a reverse cache map entry for the future container id. Will be released on success by this
         * method. Shall not be released by this method in cases for an error.
         */
        bool ReuseCacheLine(uint64_t future_container_id, tbb::concurrent_hash_map<uint64_t, int>::accessor* accessor,
                CacheEntry* entry);

    public:
        /**
         * Constructor for the read cache
         * @param storage
         * @return
         */
        explicit ContainerStorageReadCache(ContainerStorage* storage);

        virtual ~ContainerStorageReadCache();

        /**
         * Configures the read cache.
         *
         * Available options:
         * - size: StorageUnit, >0
         *
         * @param option_name
         * @param option
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Starts the read cache
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start();

        /**
         * Persists the statistics about the read cache
         */
        bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        /**
         * Restored the statistics about the read cache
         */
        bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        /**
         * returns statistics about the locks of the read cache.
         * @return
         */
        std::string PrintLockStatistics();

        /**
         * returns statistics about the read cache
         * @return
         */
        std::string PrintStatistics();

        /**
         * returns profile information about the read cache
         * @return
         */
        std::string PrintProfile();

        /**
         * Checks the cache if it contains a given container id. The main purpose of this method is to
         * acquire a cache line (lock) to update the cache when the container is not yet in the cache.
         *
         * The method can be used in two modes. If cache_line and cache_lock are set and if the container has not been
         * found in the cache, a cache entry is replaced for the given container.
         *
         * If The cache container has not been found, the method tries to make a cache line free for the container id.
         * client. If this succeeds, the cache entry will be set (entry->is_set returns true).
         * The cache entry will never be set, when the cache container is found. Also the cache entry will never be set,
         * when LOOKUP_ERROR is returned.
         *
         * @param container_id container id to check the cache for
         * @param cache_entry holds the cache line and the cache lock. Shall not be NULL. May be set when LOOKUP_NOT_FOUND is returned,
         * May not be set when LOOKUP_FOUND and LOOKUP_ERROR is returned. If it is set, the client is responsible for releasing
         * the lock.
         * @return
         */
        dedupv1::base::lookup_result GetCache(uint64_t container_id, CacheEntry* entry);

        /**
         * Checks the cache for the given container.
         * If the container id has been found in the cache, the container contains the data found in the read
         * cache.
         *
         * The major difference to the GetCache is that we gain access to the cache data.
         * We here hold a lock to the container cache line that must manually be released when the container is in the cache.
         *
         * If the cache container has not been found and no_update is not set, the method tries to make a
         * cache line free for the container id. If this succeeds, the cache entry will be set (entry->is_set returns true).
         * The cache entry will never be set, when the cache container is found. Also the cache entry will never be set,
         * when LOOKUP_ERROR is returned.
         *
         * @param container_id container id to check the cache for.
         * @param container pointer to a pointer for the cached container data.
         * @param cache_entry holds the cache line and the cache lock. Shall be set, May be set when LOOKUP_NOT_FOUND is returned,
         * Is be set when LOOKUP_FOUND is returned. Is never set when LOOKUP_ERROR is returned. If it is set, the client
         * is responsible for releasing. If not set, no cache line is re-freed to hold the container
         * @return
         */
        dedupv1::base::lookup_result CheckCache(uint64_t container_id, const Container** container,
                bool no_update,
                bool write_lock,
                CacheEntry* entry);

        /**
         * Copies the container to the read cache. The cache entry should point to a cache entry
         * set by GetCache or CheckCache()
         *
         * @param container Container that should be copied to the read cache
         * @param cache_entry valid and set cache entry. If successful, the cache entry is invalidated and the cache line
         * lock is released by this method.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool CopyToReadCache(const Container& container, CacheEntry* entry);

        /**
         * Removes the given container from the cache if was in the cache.
         * If the container was not in the case, the method does nothing.

         * @param container_id container id to release from the cache line
         * @param cache_entry valid and set cache entry. If successful, the cache entry is invalidated and the cache line
         * lock is released by this method.
         * @return true iff ok, otherwise an error has occurred
         */
        bool RemoveFromReadCache(uint64_t container_id, CacheEntry* entry);

        /**
         * The cache entry should point to a cache entry
         * set by GetCache or CheckCache()
         *
         * @param cache_entry valid and set cache entry. If successful, the cache entry is invalidated and the cache line
         * lock is released by this method.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool ReleaseCacheline(uint64_t container_id, CacheEntry* entry);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool ClearCache();

        /**
         * returns the cache statistics
         */
        const Statistics& stats() const {
            return stats_;
        }
};

}
}

#endif  // CONTAINER_STORAGE_CACHE_H__
