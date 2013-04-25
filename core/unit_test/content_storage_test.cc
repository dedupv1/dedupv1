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
#include <map>
#include <string>
#include <list>

#include <gtest/gtest.h>

#include <core/dedup.h>
#include <base/locks.h>

#include <core/log_consumer.h>
#include <base/disk_hash_index.h>
#include <base/index.h>
#include <core/chunk_mapping.h>
#include <core/fingerprinter.h>
#include <base/index.h>
#include <base/hash_index.h>
#include <core/container_storage.h>
#include <base/crc32.h>
#include <core/storage.h>
#include <core/dedup_system.h>
#include <core/content_storage.h>
#include <core/block_index.h>
#include <core/session.h>

#include "dedup_system_test.h"
#include <test_util/log_assert.h>
#include "filter_chain_test_util.h"

namespace dedupv1 {
namespace contentstorage {

#define BLOCK_SIZE 64 * 1024

LOGGER("ContentStorageTest");

class ContentStorageTest : public testing::TestWithParam<const char*> {
protected:
    USE_LOGGING_EXPECTATION();

    DedupSystem* system;
    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;

    byte test_data[4][BLOCK_SIZE];
    uint64_t test_adress[4];
    uint64_t test_fp[4];

    Chunker* chunker;

    virtual void SetUp() {
        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());

        system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
        ASSERT_TRUE(system);

        chunker = Chunker::Factory().Create("static");
        ASSERT_TRUE(chunker);
        ASSERT_TRUE(chunker->Start());

        memset(test_data[0], 1, BLOCK_SIZE);
        memset(test_data[1], 2, BLOCK_SIZE);
        memset(test_data[2], 3, BLOCK_SIZE);
        memset(test_data[3], 4, BLOCK_SIZE);

        test_fp[0] = 1;
        test_fp[1] = 2;
        test_fp[2] = 3;
        test_fp[3] = 4;

        memset(test_adress, 0, 4 * sizeof(uint64_t));

    }

    void WriteTestData(Session* session) {
        for (int i = 0; i < 4; i++) {
            ASSERT_TRUE(system->block_locks()->WriteLock(10 + i, LOCK_LOCATION_INFO));
            Request request(REQUEST_WRITE, 10 + i, 0, system->block_size(), test_data[i], system->block_size());
            ASSERT_TRUE(system->content_storage()->WriteBlock(session, &request, NULL, false, NO_EC));
        }
    }

    void ReadTestData(Session* session) {
        int i = 0;

        byte result[4][BLOCK_SIZE];
        memset(result, 0, 4 * BLOCK_SIZE);

        for (i = 0; i < 4; i++) {
            Request request(REQUEST_READ, 10 + i, 0, system->block_size(), result[i], system->block_size());
            ASSERT_TRUE(system->content_storage()->ReadBlock(session, &request, NULL, NO_EC));
        }

        for (i = 0; i < 4; i++) {
            ASSERT_TRUE(memcmp(test_data[i], result[i], BLOCK_SIZE) == 0) << "Compare 1 error";
        }
    }

    virtual void TearDown() {
        if (chunker) {
            delete chunker;
            chunker = NULL;
        }
        if (system) {
            ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
            delete system;
        }
    }
};

INSTANTIATE_TEST_CASE_P(ContentStorage,
    ContentStorageTest,
    ::testing::Values(
        "data/dedupv1_test.conf"));

TEST_P(ContentStorageTest, Start) {
    ASSERT_EQ(system->block_size(), (size_t) BLOCK_SIZE);
}


TEST_P(ContentStorageTest, BasicReadWrite) {
    Session* session = new Session();
    ASSERT_TRUE(session->Init(system->GetVolume(0)));

    DEBUG("Write data");
    WriteTestData(session);

    DEBUG("Read data");
    ReadTestData(session);

    DEBUG("Shutdown");
    delete session;
}

}
}
