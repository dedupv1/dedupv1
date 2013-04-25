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

#include <string>
#include <list>

#include <gtest/gtest.h>

#include <core/dedup.h>
#include <base/locks.h>

#include <core/log_consumer.h>
#include <base/index.h>
#include <core/container.h>
#include <core/log.h>
#include <core/storage.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/crc32.h>
#include <core/container_storage.h>
#include <core/container_storage_write_cache.h>

#include "storage_test.h"
#include "container_test_helper.h"
#include <test_util/log_assert.h>
#include <test/dedup_system_mock.h>
#include <test/chunk_index_mock.h>

using dedupv1::base::crc;
using dedupv1::base::strutil::ToString;
using dedupv1::log::Log;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::ReadWriteLock;
using testing::Return;
using testing::_;

LOGGER("ContainerStorageCacheTest");

namespace dedupv1 {
namespace chunkstore {

class ContainerStorageCacheTest : public testing::Test {
public:
    static const size_t TEST_DATA_SIZE = 128 * 1024;
    static const size_t TEST_DATA_COUNT = 64;
protected:
    USE_LOGGING_EXPECTATION();

    ContainerStorageReadCache* read_cache;
    ContainerStorage* storage;

    Log* log;
    IdleDetector* idle_detector;
    dedupv1::MemoryInfoStore info_store;
    MockDedupSystem system;
    MockChunkIndex chunk_index;

    ContainerTestHelper* container_helper;

    virtual void SetUp() {
        storage = NULL;
        read_cache = NULL;
        log = NULL;

        container_helper = new ContainerTestHelper(ContainerStorageCacheTest::TEST_DATA_SIZE,
            ContainerStorageCacheTest::TEST_DATA_COUNT);
        ASSERT_TRUE(container_helper->SetUp());

        idle_detector = new IdleDetector();
        ASSERT_TRUE(idle_detector);
        EXPECT_CALL(system, idle_detector()).WillRepeatedly(Return(idle_detector));
        EXPECT_CALL(system, info_store()).WillRepeatedly(Return(&info_store));
        EXPECT_CALL(system, chunk_index()).WillRepeatedly(Return(&chunk_index));
        EXPECT_CALL(chunk_index, ChangePinningState(_,_,_)).WillRepeatedly(Return(LOOKUP_FOUND));

        log = new Log();
        ASSERT_TRUE(log->SetOption("filename", "work/log"));
        ASSERT_TRUE(log->SetOption("max-log-size", "1M"));
        ASSERT_TRUE(log->SetOption("info.type", "sqlite-disk-btree"));
        ASSERT_TRUE(log->SetOption("info.filename", "work/log-info"));
        ASSERT_TRUE(log->SetOption("info.max-item-count", "16"));
        ASSERT_TRUE(log->Start(StartContext(), &system));
        EXPECT_CALL(system, log()).WillRepeatedly(Return(log));

        storage = NULL;
        read_cache = NULL;

        this->storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
        ASSERT_TRUE(this->storage);
        ASSERT_NO_FATAL_FAILURE(SetDefaultStorageOptions(storage));
        ASSERT_TRUE(this->storage->Start(StartContext(), &system));
        ASSERT_TRUE(this->storage->Run());

        this->read_cache = this->storage->GetReadCache();
        ASSERT_TRUE(this->read_cache);
    }

    void SetDefaultStorageOptions(Storage* storage) {
        ASSERT_TRUE(storage->SetOption("filename", "work/container-data-1"));
        ASSERT_TRUE(storage->SetOption("filename", "work/container-data-2"));
        ASSERT_TRUE(storage->SetOption("meta-data", "static-disk-hash"));
        ASSERT_TRUE(storage->SetOption("meta-data.page-size", "2K"));
        ASSERT_TRUE(storage->SetOption("meta-data.size", "4M"));
        ASSERT_TRUE(storage->SetOption("meta-data.filename", "work/container-metadata"));
        ASSERT_TRUE(storage->SetOption("size", "1G"));
        ASSERT_TRUE(storage->SetOption("read-cache-size", "4"));

        ASSERT_TRUE(storage->SetOption("gc", "greedy"));
        ASSERT_TRUE(storage->SetOption("gc.type","sqlite-disk-btree"));
        ASSERT_TRUE(storage->SetOption("gc.filename", "work/merge-candidates"));
        ASSERT_TRUE(storage->SetOption("gc.max-item-count", "64"));
        ASSERT_TRUE(storage->SetOption("alloc", "memory-bitmap"));
        ASSERT_TRUE(storage->SetOption("alloc.type","sqlite-disk-btree"));
        ASSERT_TRUE(storage->SetOption("alloc.filename", "work/container-bitmap"));
        ASSERT_TRUE(storage->SetOption("alloc.max-item-count", "2K"));
    }

    virtual void TearDown() {
        if (storage) {
            ASSERT_TRUE(storage->Close());
            storage = NULL;
            read_cache = NULL;
        }

        if (log) {
            ASSERT_TRUE(log->Close());
            log = NULL;
        }

        if (idle_detector) {
            ASSERT_TRUE(idle_detector->Close());
            delete idle_detector;
            idle_detector = NULL;
        }

        if (container_helper) {
            delete container_helper;
            container_helper = NULL;
        }
    }
};

TEST_F(ContainerStorageCacheTest, Simple) {
    CacheEntry cache_entry;

    lookup_result lr = read_cache->GetCache(1, &cache_entry);
    ASSERT_EQ(LOOKUP_NOT_FOUND, lr);
    ASSERT_TRUE(cache_entry.is_set());
    Container container(1, storage->GetContainerSize(), false);

    ASSERT_TRUE(read_cache->CopyToReadCache(container, &cache_entry));

    lr = read_cache->GetCache(1, &cache_entry);
    ASSERT_EQ(LOOKUP_FOUND, lr);
}

TEST_F(ContainerStorageCacheTest, SimpleWithReplace) {
    CacheEntry cache_entry;

    lookup_result lr;
    for (int i = 0; i < 10; i++) {
        lr = read_cache->GetCache(i, &cache_entry);
        ASSERT_EQ(LOOKUP_NOT_FOUND, lr);
        ASSERT_TRUE(cache_entry.is_set());

        Container container(i, storage->GetContainerSize(), false);

        ASSERT_TRUE(read_cache->CopyToReadCache(container, &cache_entry));
    }

    lr = read_cache->GetCache(1, &cache_entry);
    ASSERT_EQ(LOOKUP_NOT_FOUND, lr);
    ASSERT_TRUE(cache_entry.is_set());
    cache_entry.lock()->ReleaseLock();
}

TEST_F(ContainerStorageCacheTest, GetCache) {
    CacheEntry cache_entry;

    lookup_result lr = read_cache->GetCache(1, &cache_entry);
    ASSERT_EQ(LOOKUP_NOT_FOUND, lr);
    ASSERT_TRUE(cache_entry.is_set());
    Container container(1, storage->GetContainerSize(), false);
    ASSERT_TRUE(read_cache->CopyToReadCache(container, &cache_entry));

    const Container* cache_container = NULL;
    lr = read_cache->CheckCache(1, &cache_container, false, true, &cache_entry);
    ASSERT_EQ(LOOKUP_FOUND, lr);
    ASSERT_TRUE(cache_entry.is_set());
    cache_entry.lock()->ReleaseLock();

    lr = read_cache->CheckCache(1, &cache_container, true, true, &cache_entry);
    ASSERT_EQ(LOOKUP_FOUND, lr);
    ASSERT_TRUE(cache_entry.is_set());
    cache_entry.lock()->ReleaseLock();
}

TEST_F(ContainerStorageCacheTest, CheckCacheWithUpdate) {
    CacheEntry cache_entry;

    const Container* cache_container = NULL;
    lookup_result lr = read_cache->CheckCache(1, &cache_container, false, true, &cache_entry);
    ASSERT_EQ(LOOKUP_NOT_FOUND, lr);
    ASSERT_TRUE(cache_entry.is_set());

    Container container(1, storage->GetContainerSize(), false);
    ASSERT_TRUE(read_cache->CopyToReadCache(container, &cache_entry));

    lr = read_cache->CheckCache(1, &cache_container, true, true, &cache_entry);
    ASSERT_EQ(LOOKUP_FOUND, lr);
    ASSERT_TRUE(cache_entry.is_set());
    cache_entry.lock()->ReleaseLock();
}

TEST_F(ContainerStorageCacheTest, CheckCacheWithoutUpdate) {
    CacheEntry cache_entry;

    const Container* cache_container = NULL;
    lookup_result lr = read_cache->CheckCache(1, &cache_container, true, true, &cache_entry);
    ASSERT_EQ(LOOKUP_NOT_FOUND, lr);
    ASSERT_FALSE(cache_entry.is_set());
}

}
}
