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
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <string>
#include <list>
#include <tr1/tuple>

#include <json/json.h>

#include <gtest/gtest.h>

#include <core/dedup.h>
#include <base/locks.h>

#include <core/dedup_system.h>
#include <core/chunk_store.h>
#include <core/log_consumer.h>
#include <core/log.h>
#include <test_util/log_assert.h>
#include "container_storage_test_helper.h"
#include <test/dedup_system_mock.h>
#include <test/chunk_index_mock.h>

using std::string;
using dedupv1::log::Log;
using std::tr1::make_tuple;
using testing::Return;
using testing::_;
using dedupv1::base::LOOKUP_FOUND;

namespace dedupv1 {
namespace chunkstore {

class ChunkStoreTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    ChunkStore* chunk_store;

    dedupv1::MemoryInfoStore info_store;
    Log* log;
    IdleDetector* idle_detector;
    MockDedupSystem system;
    MockChunkIndex chunk_index;

    virtual void SetUp() {
        idle_detector = NULL;
        log = NULL;
        chunk_store = NULL;

        idle_detector = new IdleDetector();
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

        chunk_store = new ChunkStore();
        ASSERT_TRUE(chunk_store->Init("container-storage"));
        SetDefaultStorageOptions(chunk_store, make_tuple(4, 0, false, 4));
    }

    virtual void TearDown() {
        if (chunk_store) {
            delete chunk_store;
        }

        if (log) {
            delete log;
        }

        if (idle_detector) {
            delete idle_detector;
        }

    }
};

TEST_F(ChunkStoreTest, Create) {
    // Do nothing
}

TEST_F(ChunkStoreTest, Start) {
    dedupv1::StartContext start_context;
    ASSERT_TRUE(chunk_store->Start(start_context, &system));
}

TEST_F(ChunkStoreTest, PrintLockStatistics) {
    dedupv1::StartContext start_context;
    ASSERT_TRUE(chunk_store->Start(start_context, &system));

    string s = chunk_store->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_F(ChunkStoreTest, PrintStatistics) {
    dedupv1::StartContext start_context;
    ASSERT_TRUE(chunk_store->Start(start_context, &system));

    string s = chunk_store->PrintStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_F(ChunkStoreTest, PrintTrace) {
    dedupv1::StartContext start_context;
    ASSERT_TRUE(chunk_store->Start(start_context, &system));

    string s = chunk_store->PrintTrace();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_F(ChunkStoreTest, PrintProfile) {
    dedupv1::StartContext start_context;
    ASSERT_TRUE(chunk_store->Start(start_context, &system));

    string s = chunk_store->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

}
}
