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

#include <gtest/gtest.h>

#include <core/dedup.h>
#include <base/logging.h>
#include <core/log_consumer.h>
#include <core/log.h>
#include <core/chunk_index.h>
#include <core/block_mapping.h>
#include <base/memory.h>
#include <core/garbage_collector.h>
#include <base/strutil.h>
#include <base/locks.h>
#include <base/strutil.h>
#include <base/index.h>
#include <core/container.h>
#include <core/storage.h>
#include <base/strutil.h>
#include <base/crc32.h>
#include <core/container_storage.h>
#include <core/container_storage_gc.h>
#include <base/thread.h>
#include <base/runnable.h>

#include "dedupv1.pb.h"

#include "storage_test.h"
#include <test_util/log_assert.h>
#include "dedup_system_test.h"
#include "container_test_helper.h"
#include <test/container_storage_mock.h>
#include <test/storage_mock.h>

LOGGER("ContainerGCIntegrationTest");

using std::pair;
using std::make_pair;
using std::list;
using testing::Return;
using testing::_;

using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::make_list;
using dedupv1::base::make_bytestring;
using dedupv1::chunkstore::GreedyContainerGCStrategy;
using dedupv1::chunkstore::Container;
using dedupv1::base::strutil::ToString;
using dedupv1::base::PersistentIndex;
using dedupv1::base::IndexCursor;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::chunkstore::STORAGE_ADDRESS_COMMITED;
using dedupv1::StartContext;
using dedupv1::StopContext;
using dedupv1::DedupSystem;
using dedupv1::chunkstore::ContainerStorage;

class GreedyContainerGCStrategyIntegrationTest : public testing::TestWithParam<const char*> {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;

    DedupSystem* system;
    DedupSystem* crashed_system;

    GreedyContainerGCStrategy* container_gc;
    ContainerStorage* storage;

    ContainerTestHelper* container_test_helper;

    virtual void SetUp() {
        container_test_helper = NULL;
        crashed_system = NULL;
        container_gc = NULL;
        storage = NULL;

        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());

        system = dedupv1::DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
        ASSERT_TRUE(system);

        storage = dynamic_cast<ContainerStorage*>(system->storage());
        ASSERT_TRUE(storage);
        container_gc = dynamic_cast<GreedyContainerGCStrategy*>(storage->container_gc());
        ASSERT_TRUE(container_gc);

        container_test_helper = new ContainerTestHelper(64 * 1024, 16);
        ASSERT_TRUE(container_test_helper);
        ASSERT_TRUE(container_test_helper->SetUp());
    }

    void Crash() {
        ASSERT_TRUE(crashed_system == NULL);
        system->ClearData();
        crashed_system = system;
        system = NULL;
        storage = NULL;
        container_gc = NULL;

        system =  dedupv1::DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true, true, true);

        storage = dynamic_cast<ContainerStorage*>(system->storage());
        ASSERT_TRUE(storage);
        container_gc = dynamic_cast<GreedyContainerGCStrategy*>(storage->container_gc());
        ASSERT_TRUE(container_gc);
    }

    void Restart() {
        ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
        ASSERT_TRUE(system->Close());
        system = NULL;
        storage = NULL;
        container_gc = NULL;

        system = dedupv1::DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true, false, false);

        storage = dynamic_cast<ContainerStorage*>(system->storage());
        ASSERT_TRUE(storage);
        container_gc = dynamic_cast<GreedyContainerGCStrategy*>(storage->container_gc());
        ASSERT_TRUE(container_gc);

    }

    virtual void TearDown() {
        if (system) {
            ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
            ASSERT_TRUE(system->Close());
            storage = NULL;
            container_gc = NULL;
        }
        if (crashed_system) {
            ASSERT_TRUE(crashed_system->Close());
            crashed_system = NULL;
        }
        if (container_test_helper) {
            delete container_test_helper;
            container_test_helper = NULL;
        }
    }
};

bool ContainerRead(ContainerStorage* storage, uint64_t container_id) {
    DEBUG("Start read thread");
    for (int j = 0; j < 4; j++) {
        storage->GetReadCache()->ClearCache();
        for (int i = 0; i < 4; i++) {
            Container c;
            c.Init(i, storage->GetContainerSize());
            CHECK(storage->ReadContainer(&c), "Failed to read container");
            DEBUG("Read container: " << c.DebugString());
        }
    }
    DEBUG("Stop read thread");
    return true;
}

bool DeleteItem(ContainerStorage* storage, ContainerTestHelper* container_test_helper) {
    DEBUG("Start delete thread");

    for (int i = 0; i < 16; i++) {
        CHECK(storage->DeleteChunk(container_test_helper->data_address(i),
                container_test_helper->fingerprint(i).data(),
                container_test_helper->fingerprint(i).size(), NO_EC),
            "Failed to delete from container");
        sleep(1);
    }

    DEBUG("Stop delete thread");
    return true;
}

bool Merge(GreedyContainerGCStrategy* gc) {
    for (int i = 0; i < 16; i++) {
        sleep(0);
        CHECK(gc->OnStoragePressure(), "Check the gc");
    }
    return true;
}

TEST_P(GreedyContainerGCStrategyIntegrationTest, MergeWithReading) {
    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 0, 16));

    ASSERT_TRUE(storage->Flush(NO_EC));
    storage->GetReadCache()->ClearCache();

    Thread<bool> t(NewRunnable(&ContainerRead, storage, container_test_helper->data_address(1)), "read");
    ASSERT_TRUE(t.Start());

    Thread<bool> t2(NewRunnable(&DeleteItem, storage, container_test_helper), "delete");
    ASSERT_TRUE(t2.Start());

    Thread<bool> t3(NewRunnable(&Merge, container_gc), "merge");
    ASSERT_TRUE(t3.Start());

    sleep(2);

    ASSERT_TRUE(storage->Flush(NO_EC));
    storage->GetReadCache()->ClearCache();

    sleep(10);

    ASSERT_TRUE(t.Join(NULL));
    ASSERT_TRUE(t2.Join(NULL));
    ASSERT_TRUE(t3.Join(NULL));
}

INSTANTIATE_TEST_CASE_P(GreedyContainerGCStrategy,
    GreedyContainerGCStrategyIntegrationTest,
    ::testing::Values(
        "data/dedupv1_test.conf",
        "data/dedupv1_leveldb_test.conf"));
