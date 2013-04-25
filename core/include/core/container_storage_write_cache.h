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

#ifndef CONTAINER_STORAGE_WRITE_CACHE_H__
#define CONTAINER_STORAGE_WRITE_CACHE_H__

#include <vector>
#include <string>
#include <map>

#include "tbb/concurrent_hash_map.h"
#include "tbb/tick_count.h"
#include "tbb/spin_rw_mutex.h"
#include "tbb/atomic.h"

#include <core/dedup.h>
#include <base/profile.h>
#include <base/locks.h>
#include <base/index.h>
#include <core/statistics.h>

namespace dedupv1 {
namespace chunkstore {

class ContainerStorage;
class Container;

class ContainerStorageWriteCacheStrategy;

/**
 * Write cache of the storage container.
 * The write cache contains the contains before they are committed.
 */
class ContainerStorageWriteCache : public dedupv1::StatisticProvider {
    DISALLOW_COPY_AND_ASSIGN(ContainerStorageWriteCache);
    public:
    static const uint32_t kDefaultWriteCacheSize = 8;
    static const std::string kDefaultCacheStrategyType;

    class Statistics {
        public:
            Statistics();

            dedupv1::base::Profile cache_check_time_;
            dedupv1::base::Profile cache_update_time_;
            tbb::atomic<uint64_t> cache_checks_;

            tbb::atomic<uint32_t> write_container_lock_busy_;
            tbb::atomic<uint32_t> write_container_lock_free_;

            tbb::atomic<uint64_t> cache_hits_;
            tbb::atomic<uint64_t> cache_miss_;

            dedupv1::base::Profile write_lock_wait_time_;
    };
    private:
    Statistics stats_;

    ContainerStorage* storage_;

    /**
     * Number of parallel open write containers.
     */
    unsigned int write_container_count_;

    /**
     * Pointer to the active write container(s)
     */
    std::vector<Container*> write_container;

    /**
     * Pointer to the write container lock
     */
    dedupv1::base::ReadWriteLockVector write_container_lock_;

    /**
     * Last currently change time in millisecond resolution.
     */
    std::vector<tbb::tick_count> write_cache_changed_time_;

    tbb::spin_rw_mutex write_cache_changed_time_lock_;

    /**
     * Strategy how to choose the write container for the next data
     */
    ContainerStorageWriteCacheStrategy* write_cache_strategy_;

    public:
    explicit ContainerStorageWriteCache(ContainerStorage* storage);

    virtual ~ContainerStorageWriteCache();

    /**
     *
     * Available options:
     * - size: StorageUnit
     * - strategy
     * - strategy.*
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    bool Start();

    bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    std::string PrintLockStatistics();

    std::string PrintStatistics();

    std::string PrintProfile();

    bool IsTimedOut(int index, double timeout_seconds);

    bool ResetTimeout(int index);

#ifdef DEDUPV1_CORE_TEST
    void ClearData();
#endif

    /**
     *
     * @param address
     * @param write_container
     * @param write_cache_lock
     * @param write_lock
     * @return
     */
    dedupv1::base::lookup_result GetWriteCacheContainer(uint64_t address, Container** write_container,
            dedupv1::base::ReadWriteLock** write_cache_lock, bool write_lock);

    /**
     * The write cache lock is acquired when the method returns successfully.
     *
     * @param write_container
     * @param write_cache_lock
     * @return
     */
    bool GetNextWriteCacheContainer(Container** write_container,
            dedupv1::base::ReadWriteLock** write_cache_lock);

    /**
     * Lock is NOT acquired
     *
     * @param index
     * @param write_container
     * @param write_cache_lock
     * @return
     */
    bool GetWriteCacheContainerByIndex(int index, Container** write_container,
            dedupv1::base::ReadWriteLock** write_cache_lock);

    inline std::vector<Container*>& GetCache();
    inline const std::vector<Container*>& GetCache() const;

    inline dedupv1::base::ReadWriteLockVector& GetCacheLock();
    inline const dedupv1::base::ReadWriteLockVector& GetCacheLock() const;

    inline std::vector<tbb::tick_count>& GetCacheChangedTime();
    inline const std::vector<tbb::tick_count>& GetCacheChangedTime() const;

    inline uint32_t GetSize();

    inline Statistics* GetStatistics();
};


/**
 * Abstract base class who to choose a write container for new data
 */
class ContainerStorageWriteCacheStrategy {
        DISALLOW_COPY_AND_ASSIGN(ContainerStorageWriteCacheStrategy);
    public:
        ContainerStorageWriteCacheStrategy();
        virtual ~ContainerStorageWriteCacheStrategy();

        virtual bool GetNextWriteCacheContainer(Container** write_container,
                dedupv1::base::ReadWriteLock** write_cache_lock) = 0;

        virtual bool Init();

        virtual bool SetOption(const std::string& option_name, const std::string& option);

        virtual bool Start(ContainerStorageWriteCache* write_cache);
};

class RoundRobinContainerStorageWriteCacheStrategy : public ContainerStorageWriteCacheStrategy {
    private:
        DISALLOW_COPY_AND_ASSIGN(RoundRobinContainerStorageWriteCacheStrategy);

        ContainerStorageWriteCache* write_cache;

        /**
         * Indicating the next write container to use.
         */
        tbb::atomic<uint64_t> next_write_container;
    public:
        static ContainerStorageWriteCacheStrategy* CreateWriteCacheStrategy();

        static void RegisterWriteCacheStrategy();

        RoundRobinContainerStorageWriteCacheStrategy();
        virtual ~RoundRobinContainerStorageWriteCacheStrategy();

        virtual bool Start(ContainerStorageWriteCache* write_cache);

        virtual bool GetNextWriteCacheContainer(Container** write_container,
                dedupv1::base::ReadWriteLock** write_cache_lock);
};

/**
 * Write cache strategy that uses the earliest write container that is not locked by another thread.
 * The goal is to use much less container in low traffic situations, but use all available container in high
 * traffic situations.
 */
class EarliestFreeContainerStorageWriteCacheStrategy : public ContainerStorageWriteCacheStrategy {
    private:
        DISALLOW_COPY_AND_ASSIGN(EarliestFreeContainerStorageWriteCacheStrategy);

        ContainerStorageWriteCache* write_cache;
        ContainerStorageWriteCacheStrategy* fallback_strategy;
    public:
        static ContainerStorageWriteCacheStrategy* CreateWriteCacheStrategy();

        static void RegisterWriteCacheStrategy();

        EarliestFreeContainerStorageWriteCacheStrategy();
        virtual ~EarliestFreeContainerStorageWriteCacheStrategy();

        virtual bool Start(ContainerStorageWriteCache* write_cache);

        virtual bool GetNextWriteCacheContainer(Container** write_container,
                dedupv1::base::ReadWriteLock** write_cache_lock);
};

class ContainerStorageWriteCacheStrategyFactory {
        DISALLOW_COPY_AND_ASSIGN(ContainerStorageWriteCacheStrategyFactory);
    public:
        ContainerStorageWriteCacheStrategyFactory();
        bool Register(const std::string& name, ContainerStorageWriteCacheStrategy*(*factory)(void));
        static ContainerStorageWriteCacheStrategy* Create(const std::string& name);

        static ContainerStorageWriteCacheStrategyFactory* GetFactory() { return &factory; }

    private:
        std::map<std::string, ContainerStorageWriteCacheStrategy*(*)(void)> factory_map;

        static ContainerStorageWriteCacheStrategyFactory factory;
};

uint32_t ContainerStorageWriteCache::GetSize() {
    return this->write_container_count_;
}

std::vector<Container*>& ContainerStorageWriteCache::GetCache() {
    return this->write_container;
}

const std::vector<Container*>& ContainerStorageWriteCache::GetCache() const {
    return this->write_container;
}

dedupv1::base::ReadWriteLockVector& ContainerStorageWriteCache::GetCacheLock() {
    return this->write_container_lock_;
}

const dedupv1::base::ReadWriteLockVector& ContainerStorageWriteCache::GetCacheLock() const {
    return this->write_container_lock_;
}

std::vector<tbb::tick_count>& ContainerStorageWriteCache::GetCacheChangedTime() {
    return this->write_cache_changed_time_;
}

const std::vector<tbb::tick_count>& ContainerStorageWriteCache::GetCacheChangedTime() const {
    return this->write_cache_changed_time_;
}

ContainerStorageWriteCache::Statistics* ContainerStorageWriteCache::GetStatistics() {
    return &this->stats_;
}

}
}

#endif  // CONTAINER_STORAGE_WRITE_CACHE_H__

