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

#ifndef CONTAINER_STORAGE_GC_H__
#define CONTAINER_STORAGE_GC_H__

#include <map>
#include <core/dedup.h>
#include <base/index.h>
#include <base/option.h>
#include <base/cache_strategy.h>
#include <tbb/recursive_mutex.h>
#include <gtest/gtest_prod.h>

namespace dedupv1 {
namespace chunkstore {

class ContainerStorage;
class Container;

/**
 * Strategy about the garbage collection inside the
 * container storage.
 *
 * The responsibility of the gc is to free the space used by
 * non-full or empty containers (by merging or deleting them) so that
 * an intelligent container storage allocator can reuse the space
 */
class ContainerGCStrategy : public dedupv1::StatisticProvider {
    public:
    /**
     * Constructor
     * @return
     */
    ContainerGCStrategy();

    /**
     * Destructor
     * @return
     */
    virtual ~ContainerGCStrategy();

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(const dedupv1::StartContext& start_context, ContainerStorage* storge);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Stop(const dedupv1::StopContext& stop_context);

    /**
     *
     * Available options:
     * - type: String
     * - threshold: uint32_t
     * - item-count-threshold: uint32_t
     * - bucket-size: uint32_t
     * - eviction-timeout: uint32_t
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool SetOption(const std::string& option_name, const std::string& option) = 0;

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnCommit(const ContainerCommittedEventData& data) = 0;

    /**
     * Note: Container may not be committed at this point.
     *
     * @param container
     * @param key_list list of item keys to delete
     * @param old_active_data_size data size before the fingerprints given in the key list have
     * been deleted from the container.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnMove(const ContainerMoveEventData& data);

    virtual bool OnDeleteContainer(const Container& container);

    /**
     * Note: Container may not be committed at this point.
     *
     * @param container
     * @param key
     * @param key_size
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnRead(const Container& container, const void* key, size_t key_size) = 0;

    /**
     *
     * @param data
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnMerge(const ContainerMergedEventData& data);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnIdle();

    /**
     * Called on storage pressure.
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnStoragePressure();

#ifdef DEDUPV1_CORE_TEST
    virtual void ClearData();
#endif
    DISALLOW_COPY_AND_ASSIGN(ContainerGCStrategy);
};

/**
 * "greedy" and stupid simple gc strategy for the container storage.
 */
class GreedyContainerGCStrategy : public ContainerGCStrategy {
        FRIEND_TEST(GreedyContainerGCStrategyTest, OnCommitEmptyContainer);
        FRIEND_TEST(GreedyContainerGCStrategyTest, OnCommitEmptyContainerWithExistingBucket);
        FRIEND_TEST(GreedyContainerGCStrategyTest, OnDeleteFull);
        FRIEND_TEST(GreedyContainerGCStrategyTest, OnDeleteHalfFull);
        FRIEND_TEST(GreedyContainerGCStrategyTest, OnIdleNoCandidates);
        FRIEND_TEST(GreedyContainerGCStrategyTest, OnIdleOneCandidates);
        FRIEND_TEST(GreedyContainerGCStrategyTest, OnIdleTwoCandidates);
        FRIEND_TEST(GreedyContainerGCStrategyTest, OnIdleThreeCandidatesinSingleBucket);
        FRIEND_TEST(GreedyContainerGCStrategyTest, OnIdleThreeCandidatesinTwoBucket);
    private:
        class Statistics {
            public:
            /**
             * time spend in the gc
             */
            dedupv1::base::Profile gc_time_;
        };

        /**
         * Statistics
         */
        Statistics stats_;

        static const uint32_t kDefaultBucketSize = 100 * 1024;

        /**
         * Reference to the storage
         */
        ContainerStorage* storage_;

        /**
         * Container size
         */
        uint32_t container_size_;

        uint32_t container_data_size_;

        /**
         * Index storing all candidates for merging.
         * Using this index is optional, but necessary for garbage collection.
         */
        dedupv1::base::PersistentIndex* merge_candidates_;

        /**
         * Threshold under that a container is seen as a merge candidate.
         */
        uint32_t merge_candidate_data_size_threshold_;

        /**
         * If a container has more than this number of items, the container
         * is no merge candidate as a merging might lead to problems.
         */
        uint32_t merge_candidate_item_count_threshold_;

        /**
         * TODO (dmeister) Why to we need here a recursive lock?
         */
        tbb::recursive_mutex lock_;

        uint32_t bucket_size_;

        uint32_t maximal_bucket_;

        bool started_;

        /**
         * set that stored all container that have been touched in the last seconds.
         * Protected by the lock.
         */
        dedupv1::base::TimeEvictionSet<uint64_t> touched_set_;

        /**
         * We do not touch containers that have been used in recent time.
         *
         */
        uint32_t eviction_timeout_;

        tbb::atomic<uint32_t> merge_candidate_count_;

        dedupv1::base::Option<bool> CheckIfPrimaryContainerId(uint64_t container_id);

        /**
         * Hold the lock when calling this method.
         *
         * @param bucket
         * @param address
         * @return true iff ok, otherwise an error has occurred
         */
        bool DeleteFromBucket(uint64_t bucket, uint64_t address);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool ProcessCommit(uint64_t primary_container_id, uint32_t item_count, uint32_t active_data_size, bool new_commit);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool ProcessMergeCandidates();
    public:
        static ContainerGCStrategy* CreateGC();

        static void RegisterGC();

        /**
         * Constructor
         */
        GreedyContainerGCStrategy();

        /**
         * Destructor
         */
        virtual ~GreedyContainerGCStrategy();

        /**
         * Starts the container gc
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Start(const dedupv1::StartContext& start_context, ContainerStorage* storage);

        /**
         *
         * Available options:
         * - type: String
         * - threshold: uint32_t
         * - item-count-threshold: uint32_t
         * - bucket-size: uint32_t
         * - eviction-timeout: uint32_t
         *
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool OnCommit(const ContainerCommittedEventData& data);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool OnMove(const ContainerMoveEventData& data);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool OnRead(const Container& container, const void* key, size_t key_size);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool OnDeleteContainer(const ContainerDeletedEventData& data);

        /**
         * @param data
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool OnMerge(const ContainerMergedEventData& data);

        virtual bool OnIdle();

        virtual bool OnStoragePressure();

        inline dedupv1::base::PersistentIndex* merge_candidates();

        virtual std::string PrintStatistics();

        virtual std::string PrintProfile();

        uint64_t GetBucket(uint64_t active_data_size);

#ifdef DEDUPV1_CORE_TEST
        virtual void ClearData();
#endif
        DISALLOW_COPY_AND_ASSIGN(GreedyContainerGCStrategy);
};

dedupv1::base::PersistentIndex* GreedyContainerGCStrategy::merge_candidates() {
    return this->merge_candidates_;
}

/**
 * Factory for container gc strategies
 */
class ContainerGCStrategyFactory {

    public:
        ContainerGCStrategyFactory();

        /**
         * registers a new container storage gc strategy type
         *
         * @param name
         * @param factory
         * @return
         */
        bool Register(const std::string& name, ContainerGCStrategy*(*factory)(void));
        static ContainerGCStrategy* Create(const std::string& name);

        static ContainerGCStrategyFactory* GetFactory() { return &factory; }

    private:
        std::map<std::string, ContainerGCStrategy*(*)(void)> factory_map;

        static ContainerGCStrategyFactory factory;
        DISALLOW_COPY_AND_ASSIGN(ContainerGCStrategyFactory);
};
}
}

#endif  // CONTAINER_STORAGE_GC_H__
