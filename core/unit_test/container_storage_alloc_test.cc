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

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

#include <string>
#include <list>
#include <vector>
#include <map>

#include <gtest/gtest.h>

#include "dedupv1.pb.h"

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
#include <core/container_storage_alloc.h>
#include <base/option.h>
#include <base/threadpool.h>
#include "storage_test.h"
#include <test_util/log_assert.h>

#include <core/dedup_system.h>
#include "dedup_system_test.h"
#include <test/container_storage_mock.h>
#include <test/storage_mock.h>
#include <test/log_mock.h>
#include <test/chunk_index_mock.h>

LOGGER("ContainerStorageAllocatorTest");

using std::map;
using std::vector;
using dedupv1::base::strutil::ToString;
using testing::Return;
using testing::_;
using dedupv1::base::Option;

namespace dedupv1 {
namespace chunkstore {

class MemoryBitmapContainerStorageAllocatorTestFriend {
public:
    MemoryBitmapContainerStorageAllocator* alloc_;

    MemoryBitmapContainerStorageAllocatorTestFriend(MemoryBitmapContainerStorageAllocator* alloc) {
        alloc_ = alloc;
    }

    int GetNextFile() {
        return alloc_->GetNextFile();
    }
};

class MemoryBitmapAllocatorTest  : public testing::TestWithParam<const char*> {
protected:
    static size_t const CONTAINER_SIZE = 1024 * 1024;
    static size_t const TEST_DATA_SIZE = 256 * 1024;

    USE_LOGGING_EXPECTATION();

    MemoryBitmapContainerStorageAllocator* alloc;

    ContainerStorage* storage;
    DedupSystem* system;
    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;

    byte test_data[16][TEST_DATA_SIZE];
    uint64_t test_adress[164];
    uint64_t test_fp[16];

    virtual void SetUp() {
        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());

        system = NULL;
        storage = NULL;
        alloc = NULL;

        int fd = open("/dev/urandom", O_RDONLY);
        for (size_t i = 0; i < 16; i++) {
            ASSERT_EQ(read(fd, test_data[i], TEST_DATA_SIZE), (ssize_t) TEST_DATA_SIZE);
            test_fp[i] = i + 1;
            test_adress[i] = 0;
        }
        close(fd);
    }

    void CreateSystem(const std::string& configuration) {
        system = dedupv1::DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
        ASSERT_TRUE(system);

        storage = dynamic_cast<ContainerStorage*>(system->storage());
        alloc = dynamic_cast<MemoryBitmapContainerStorageAllocator*>(storage->allocator());

        DEBUG("Created system");
    }

    void Restart() {
        if (system) {
            ASSERT_TRUE(system->Close());
        }
        system = NULL;
        alloc = NULL;
        storage = NULL;

        system = dedupv1::DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true);
        ASSERT_TRUE(system);

        storage = dynamic_cast<ContainerStorage*>(system->storage());
        ASSERT_TRUE(storage);

        alloc = dynamic_cast<MemoryBitmapContainerStorageAllocator*>(storage->allocator());
        ASSERT_TRUE(alloc);
    }

    virtual void TearDown() {
        if (system) {
            ASSERT_TRUE(system->Close());
        }
        system = NULL;
        alloc = NULL;
        storage = NULL;
    }

    void FillDefaultContainer(Container* container, int begin, int count) {
        for (int i = 0; i < count; i++) {
            // Use small items to avoid an overflow
            ASSERT_TRUE(container->AddItem((byte *) &test_fp[begin + i],
                  sizeof(test_fp[begin + i]),
                  (byte *) test_data[begin + i], (size_t) 16 * 1024,
                  true, NULL))
            << "Add item " << begin + i << " failed";
        }
    }
};

INSTANTIATE_TEST_CASE_P(MemoryBitmapAllocator,
    MemoryBitmapAllocatorTest,
    ::testing::Values(
        "data/dedupv1_test.conf"));

/**
 * This test case tests that the allocator does not try to access an invalid file index
 */
TEST_P(MemoryBitmapAllocatorTest, GetNextFile) {
    CreateSystem(GetParam());

    MemoryBitmapContainerStorageAllocatorTestFriend alloc_friend(alloc);

    for (int i = 0; i < (100 * storage->GetFileCount()); i++) {
        ASSERT_EQ(i % storage->GetFileCount(), alloc_friend.GetNextFile());
    }
}

/**
 * This test case tests that the allocator assigned an address at the
 * end of a file when there are no free container places.
 */
TEST_P(MemoryBitmapAllocatorTest, OnCommit) {
    CreateSystem(GetParam());

    Container c;
    c.Init(0, CONTAINER_SIZE);
    FillDefaultContainer(&c, 0, 12 );

    uint64_t free_areas = alloc->free_count();

    for (int i = 0; i < 1000; i++) {
        ContainerStorageAddressData address_data;
        address_data.set_file_index(-2); // set illegal values
        address_data.set_file_offset(-2); // set illegal values
        ASSERT_EQ(ALLOC_OK, alloc->OnNewContainer(c, true, &address_data));

        Option<bool> b = alloc->IsAddressFree(address_data);
        ASSERT_TRUE(b.valid());
        ASSERT_FALSE(b.value());
    }

    ASSERT_EQ(free_areas - 1000, alloc->free_count());

    DEBUG("Restart");
    ASSERT_NO_FATAL_FAILURE(Restart());

    ASSERT_EQ(free_areas - 1000, alloc->free_count());
}

/**
 * Tests if the allocator is handling out container places for merges, even if
 * normal allocators already fail
 */
TEST_P(MemoryBitmapAllocatorTest, OnContainerForMerge) {
    CreateSystem(GetParam());

    Container c;
    c.Init(0, CONTAINER_SIZE);
    FillDefaultContainer(&c, 0, 12 );

    ContainerStorageAddressData address_data;
    address_data.set_file_index(-2); // set illegal values
    address_data.set_file_offset(-2); // set illegal values
    enum alloc_result ar = alloc->OnNewContainer(c, true, &address_data);
    while (ar == ALLOC_OK) {
        address_data.set_file_index(-2); // set illegal values
        address_data.set_file_offset(-2); // set illegal values
        ar = alloc->OnNewContainer(c, true, &address_data);
    }
    ASSERT_EQ(ALLOC_FULL, ar);
    ASSERT_GE(alloc->free_count(), 0);

    address_data.set_file_index(-2); // set illegal values
    address_data.set_file_offset(-2); // set illegal values
    ar = alloc->OnNewContainer(c, false, &address_data);
    ASSERT_EQ(ALLOC_OK, ar);
}

TEST_P(MemoryBitmapAllocatorTest, Overflow) {
    CreateSystem(GetParam());

    Container c;
    c.Init(0, CONTAINER_SIZE);
    FillDefaultContainer(&c, 0, 12 );

    map<int, ContainerStorageAddressData> address_map;
    int i = 0;
    for (int i = 0; i < 128; i++) {
        ContainerStorageAddressData address_data;
        address_data.set_file_index(-2); // set illegal values
        address_data.set_file_offset(-2); // set illegal values
        ASSERT_EQ(ALLOC_OK, alloc->OnNewContainer(c, true, &address_data));

        address_map[i] = address_data;

        Option<bool> b = alloc->IsAddressFree(address_data);
        ASSERT_TRUE(b.valid());
        ASSERT_FALSE(b.value());

        DEBUG("Free count " << alloc->free_count());
    }

    for (int i = 0; i < 32; i++) {
        ASSERT_TRUE(alloc->FreeAddress(address_map[i], false));

        DEBUG("Free count " << alloc->free_count());
    }

    for (; alloc->free_count() > 4; i++) { // not all places will be allocated to new containers
        ContainerStorageAddressData address_data;
        address_data.set_file_index(-2); // set illegal values
        address_data.set_file_offset(-2); // set illegal values
        ASSERT_EQ(ALLOC_OK, alloc->OnNewContainer(c, true, &address_data));

        address_map[i] = address_data;

        Option<bool> b = alloc->IsAddressFree(address_data);
        ASSERT_TRUE(b.valid());
        ASSERT_FALSE(b.value());

        uint64_t max_file_size = storage->size() / storage->GetFileCount(); // assuming here a uniform distribution
        ASSERT_LT(address_data.file_offset(), max_file_size);

        DEBUG("Free count " << alloc->free_count());
    }
}

TEST_P(MemoryBitmapAllocatorTest, OnCommitAndFree) {
    CreateSystem(GetParam());

    Container c;
    c.Init(0, CONTAINER_SIZE);
    FillDefaultContainer(&c, 0, 12 );

    map<int, ContainerStorageAddressData> address_map;
    uint64_t free_areas = alloc->free_count();
    for (int i = 0; i < 1000; i++) {
        ContainerStorageAddressData address_data;
        address_data.set_file_index(-2); // set illegal values
        address_data.set_file_offset(-2); // set illegal values
        ASSERT_EQ(ALLOC_OK, alloc->OnNewContainer(c, true, &address_data));

        address_map[i] = address_data;

        Option<bool> b = alloc->IsAddressFree(address_data);
        ASSERT_TRUE(b.valid());
        ASSERT_FALSE(b.value());
    }
    ASSERT_EQ(free_areas - 1000, alloc->free_count());

    Restart();

    ASSERT_EQ(free_areas - 1000, alloc->free_count());

    for (int i = 0; i < 1000; i++) {
        ASSERT_TRUE(alloc->FreeAddress(address_map[i], false));
        Option<bool> b = alloc->IsAddressFree(address_map[i]);
        ASSERT_TRUE(b.valid());
        ASSERT_TRUE(b.value());
    }
    ASSERT_EQ(free_areas, alloc->free_count());

    Restart();

    ASSERT_EQ(free_areas, alloc->free_count());
}

}
}
