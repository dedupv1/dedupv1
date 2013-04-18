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
#include <tbb/atomic.h>

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
#include <core/container_storage_gc.h>
#include <core/fingerprinter.h>
#include <base/thread.h>
#include <base/runnable.h>

#include "storage_test.h"
#include "container_test_helper.h"
#include <test_util/log_assert.h>
#include <test/dedup_system_mock.h>
#include <test/chunk_index_mock.h>

using std::string;
using std::pair;
using dedupv1::base::crc;
using dedupv1::base::strutil::ToString;
using dedupv1::Fingerprinter;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::PUT_OK;
using dedupv1::base::lookup_result;
using dedupv1::base::ThreadUtil;
using dedupv1::log::Log;
using dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START;
using dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using testing::Return;
using testing::_;
using ::std::tr1::tuple;
LOGGER("ContainerStorageTest");

namespace dedupv1 {
namespace chunkstore {

/**
 * Test cases about the container storage system
 */
class ContainerStorageTest : public testing::TestWithParam<tuple<const char*, int> > {
public:
    static const size_t TEST_DATA_SIZE = 128 * 1024;
    static const size_t TEST_DATA_COUNT = 64;
protected:
    USE_LOGGING_EXPECTATION();

    ContainerStorage* storage;
    ContainerStorage* crashed_storage;
    Log* log;
    IdleDetector* idle_detector;
    dedupv1::MemoryInfoStore info_store;
    MockDedupSystem system;
    MockChunkIndex chunk_index;

    ContainerTestHelper* container_helper;

    virtual void SetUp() {
        storage = NULL;
        idle_detector = NULL;
        log = NULL;
        crashed_storage = NULL;

        container_helper = new ContainerTestHelper(ContainerStorageTest::TEST_DATA_SIZE,
            ContainerStorageTest::TEST_DATA_COUNT);
        ASSERT_TRUE(container_helper->SetUp());

        idle_detector = new IdleDetector();
        ASSERT_TRUE(idle_detector);
        EXPECT_CALL(system, idle_detector()).WillRepeatedly(Return(idle_detector));
        EXPECT_CALL(system, info_store()).WillRepeatedly(Return(&info_store));
        EXPECT_CALL(system, chunk_index()).WillRepeatedly(Return(&chunk_index));

        EXPECT_CALL(chunk_index, ChangePinningState(_,_,_)).WillRepeatedly(Return(LOOKUP_FOUND));

        log = new Log();
        ASSERT_TRUE(log);
        ASSERT_TRUE(log->Init());
        ASSERT_TRUE(log->SetOption("filename", "work/log"));
        ASSERT_TRUE(log->SetOption("max-log-size", "1M"));
        ASSERT_TRUE(log->SetOption("info.type", "sqlite-disk-btree"));
        ASSERT_TRUE(log->SetOption("info.filename", "work/log-info"));
        ASSERT_TRUE(log->SetOption("info.max-item-count", "16"));
        ASSERT_TRUE(log->Start(StartContext(), &system));
        EXPECT_CALL(system, log()).WillRepeatedly(Return(log));

        storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
        ASSERT_TRUE(storage);
        SetDefaultStorageOptions(storage);

    }

    void SetDefaultStorageOptions(Storage* storage) {

        string use_compression(std::tr1::get<0>(GetParam()));
        int explicit_file_size = std::tr1::get<1>(GetParam());

        ASSERT_TRUE(storage->SetOption("filename", "work/container-data-1"));
        if (explicit_file_size >= 1) {
            ASSERT_TRUE(storage->SetOption("filesize", "512M"));
        }
        ASSERT_TRUE(storage->SetOption("filename", "work/container-data-2"));
        if (explicit_file_size >= 2) {
            ASSERT_TRUE(storage->SetOption("filesize", "512M"));
        }
        ASSERT_TRUE(storage->SetOption("meta-data", "tc-disk-btree"));
        ASSERT_TRUE(storage->SetOption("meta-data.filename", "work/container-metadata"));
        ASSERT_TRUE(storage->SetOption("container-size", "512K"));
        ASSERT_TRUE(storage->SetOption("size", "1G"));
        ASSERT_TRUE(storage->SetOption("gc", "greedy"));
        ASSERT_TRUE(storage->SetOption("gc.type","tc-disk-btree"));
        ASSERT_TRUE(storage->SetOption("gc.filename", "work/merge-candidates"));
        ASSERT_TRUE(storage->SetOption("alloc", "memory-bitmap"));
        ASSERT_TRUE(storage->SetOption("alloc.type","tc-disk-btree"));
        ASSERT_TRUE(storage->SetOption("alloc.filename", "work/container-bitmap"));

        if (!use_compression.empty()) {
            ASSERT_TRUE(storage->SetOption("compression", use_compression));
        }
    }

    void WriteTestData(StorageSession* session) {
        ASSERT_TRUE(session);
        ASSERT_TRUE(container_helper->WriteDefaultData(session, NULL, 0, TEST_DATA_COUNT));
    }

    void DeleteTestData(StorageSession* session) {
        ASSERT_TRUE(session);
        for (size_t i = 0; i < TEST_DATA_COUNT; i++) {
            ASSERT_TRUE(session->Delete(container_helper->data_address(i),
                    container_helper->fingerprint(i).data()
                    , container_helper->fingerprint(i).size(), NO_EC))
            << "Delete " << i << " failed";
        }
    }

    void ReadDeletedTestData(StorageSession* session) {
        ASSERT_TRUE(session);
        size_t i = 0;

        byte* result = new byte[TEST_DATA_SIZE];
        memset(result, 0, TEST_DATA_SIZE);

        for (i = 0; i < TEST_DATA_COUNT; i++) {
            size_t result_size = TEST_DATA_SIZE;
            ASSERT_FALSE(session->Read(container_helper->data_address(i), container_helper->fingerprint(i).data()
                    , container_helper->fingerprint(i).size(), result, &result_size, NO_EC))
            << "Found data that should be deleted: key " << Fingerprinter::DebugString(container_helper->fingerprint(i));
        }

        delete[] result;
    }

    void CrashAndRestart() {
        storage->ClearData();
        crashed_storage = storage;

        storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
        ASSERT_TRUE(storage);
        SetDefaultStorageOptions(storage);

        StartContext start_context;
        start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
        ASSERT_TRUE(storage->Start(start_context, &system));
        ASSERT_TRUE(log->PerformDirtyReplay());
        ASSERT_TRUE(storage->Run());
    }

    void Restart() {
        storage->Close();

        storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
        ASSERT_TRUE(storage);
        SetDefaultStorageOptions(storage);

        StartContext start_context;
        start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
        ASSERT_TRUE(storage->Start(start_context, &system));
        ASSERT_TRUE(log->PerformDirtyReplay());
    }

    void ReadTestData(StorageSession* session) {
        ASSERT_TRUE(session);
        size_t i = 0;

        byte result[TEST_DATA_SIZE];
        memset(result, 0, TEST_DATA_SIZE);

        size_t result_size;
        result_size = TEST_DATA_SIZE;

        for (i = 0; i < TEST_DATA_COUNT; i++) {
            memset(result, 0, TEST_DATA_SIZE);
            result_size = TEST_DATA_SIZE;

            ASSERT_TRUE(session->Read(container_helper->data_address(i), container_helper->fingerprint(i).data()
                    , container_helper->fingerprint(i).size(), result, &result_size, NO_EC)) << "Read " << i << " failed";
            ASSERT_TRUE(result_size == TEST_DATA_SIZE) << "Read " << i << " error";
            ASSERT_TRUE(memcmp(container_helper->data(i), result, result_size) == 0) << "Compare " << i << " error";
        }
    }

    virtual void TearDown() {
        if (storage) {
            ASSERT_TRUE(storage->Close());
            storage = NULL;
        }
        if (crashed_storage) {
            crashed_storage->Close();
            crashed_storage = NULL;
        }
        if (log) {
            ASSERT_TRUE(log->Close());
            log = NULL;
        }

        if (container_helper) {
            delete container_helper;
            container_helper = NULL;
        }

        if (idle_detector) {
            ASSERT_TRUE(idle_detector->Close());
            delete idle_detector;
            idle_detector = NULL;
        }
    }
};

TEST_P(ContainerStorageTest, Create) {
    // do nothing
}

TEST_P(ContainerStorageTest, Start) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
}

TEST_P(ContainerStorageTest, Run) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
}

TEST_P(ContainerStorageTest, SimpleReopen) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create(
                                                  "container-storage"));
    ASSERT_TRUE(storage);

    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());
}

TEST_P(ContainerStorageTest, SimpleReadWrite) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* session = storage->CreateSession();

    WriteTestData(session);
    ReadTestData(session);

    session->Close();
    session = NULL;
}

/**
 * Simple test where we read the data twice.
 * Additionally we also check if the cache was hit. In particular, we want to test if a read
 * using a sessin adds the container to the read cache.
 */
TEST_P(ContainerStorageTest, SimpleReread) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* session = storage->CreateSession();

    WriteTestData(session);

    ContainerStorageReadCache* read_cache = storage->GetReadCache();
    ASSERT_TRUE(read_cache->ClearCache());

    uint32_t cache_hits_before = read_cache->stats().cache_hits_;

    ReadTestData(session);

    uint32_t cache_hits_after1 = read_cache->stats().cache_hits_;
    ReadTestData(session);

    uint32_t cache_hits_after2 = read_cache->stats().cache_hits_;
    EXPECT_GT(cache_hits_after1, cache_hits_before) << "We should observe cache hits during the read: " << read_cache->PrintStatistics();
    EXPECT_GT(cache_hits_after2, cache_hits_after1) << "We should observe cache hits during the re-read: " << read_cache->PrintStatistics();
    EXPECT_GT(cache_hits_after2 - cache_hits_after1, cache_hits_after1 - cache_hits_before) << "We should see more cache hits in the re-read" <<
    " then in the first: " << read_cache->PrintStatistics();
    session->Close();
    session = NULL;
}

TEST_P(ContainerStorageTest, SimpleCrash) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Writing data");

    StorageSession* session = storage->CreateSession();
    WriteTestData(session);
    session->Close();
    session = NULL;
    storage->Flush(NO_EC);

    DEBUG("Crashing");
    CrashAndRestart();

    DEBUG("Reading data");
    session = storage->CreateSession();
    ReadTestData(session);
    session->Close();
    session = NULL;

    DEBUG("Closing data");
}

TEST_P(ContainerStorageTest, CrashedDuringBGLogReplay) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Writing data");

    StorageSession* session = storage->CreateSession();
    WriteTestData(session);
    session->Close();
    storage->Flush(NO_EC);

    ASSERT_TRUE(log->ReplayStart(EVENT_REPLAY_MODE_REPLAY_BG, true));
    dedupv1::log::log_replay_result result = dedupv1::log::LOG_REPLAY_OK;
    uint64_t replay_log_id = 0;
    while (result == dedupv1::log::LOG_REPLAY_OK) {
        replay_log_id = 0;
        result = log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, &replay_log_id, NULL);
    }
    DEBUG("Crashing");
    storage->ClearData();
    storage->Close();
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);
    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Reading data");
    session = storage->CreateSession();
    ReadTestData(session);
    session->Close();

    DEBUG("Closing data");
}

TEST_P(ContainerStorageTest, CrashedDuringCrashLogReplay) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Writing data");

    StorageSession* session = storage->CreateSession();
    WriteTestData(session);
    session->Close();
    storage->Flush(NO_EC);

    DEBUG("Crashing");
    storage->ClearData();
    storage->Close();
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // We simulate a log replay where the last container commit event
    // is replayed, but the system crashes before the replay stopped.
    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(log->PerformDirtyReplay());

    DEBUG("Crashing");
    storage->ClearData();
    storage->Close();
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Reading data");
    session = storage->CreateSession();
    ReadTestData(session);
    session->Close();

    DEBUG("Closing data");
}

TEST_P(ContainerStorageTest, Delete) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Key not found").Repeatedly();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* session = storage->CreateSession();

    WriteTestData(session); // data is in write cache

    // here we check the COW system
    pair<lookup_result, ContainerStorageAddressData> address_result =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(address_result.first, LOOKUP_FOUND);

    DeleteTestData(session);
    ReadDeletedTestData(session);

    pair<lookup_result, ContainerStorageAddressData> address_result2 =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(address_result2.first, LOOKUP_FOUND);

    ASSERT_FALSE(address_result.second.file_index() == address_result2.second.file_index() &&
        address_result.second.file_offset() && address_result.second.file_offset())
    << "Container hasn't changed position after deletion";

    session->Close();
}

TEST_P(ContainerStorageTest, DeleteBeforeRun) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Key not found").Repeatedly();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* session = storage->CreateSession();

    WriteTestData(session); // data is in write cache

    // here we check the COW system
    pair<lookup_result, ContainerStorageAddressData> address_result =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(address_result.first, LOOKUP_FOUND);

    session->Close();
    session = NULL;
    Restart();

    session = storage->CreateSession();
    DeleteTestData(session);
    session->Close();
    session = NULL;

    ASSERT_TRUE(storage->Run());
    session = storage->CreateSession();
    ReadDeletedTestData(session);

    pair<lookup_result, ContainerStorageAddressData> address_result2 =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(address_result2.first, LOOKUP_FOUND);

    ASSERT_FALSE(address_result.second.file_index() == address_result2.second.file_index() &&
        address_result.second.file_offset() && address_result.second.file_offset())
    << "Container hasn't changed position after deletion";

    session->Close();
    session = NULL;
}

TEST_P(ContainerStorageTest, DeleteAfterClose) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Key not found").Repeatedly();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* session = storage->CreateSession();

    WriteTestData(session);

    ASSERT_TRUE(session->Close());
    storage->Close();
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create(
                                                  "container-storage"));
    ASSERT_TRUE(storage);
    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());
    session = storage->CreateSession();
    DeleteTestData(session); // data should not in read or write cache
    ReadDeletedTestData(session);

    session->Close();
    session = NULL;
}

TEST_P(ContainerStorageTest, DeleteAfterFlush) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Key not found").Repeatedly();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* session = storage->CreateSession();

    WriteTestData(session);

    ASSERT_TRUE(session->Close());
    ASSERT_TRUE(storage->Flush(NO_EC)); // data is in read cache

    session = storage->CreateSession();
    DeleteTestData(session);
    ReadDeletedTestData(session);

    session->Close();
    session = NULL;
}

TEST_P(ContainerStorageTest, WriteFull) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Container storage full").Repeatedly();
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("Write.*failed").Repeatedly();

    int explicit_file_size = std::tr1::get<1>(GetParam());
    if (explicit_file_size) {
        INFO("Skip test");
        return;
    }
    ASSERT_TRUE(storage->SetOption("size", "32M"));
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* session = storage->CreateSession();
    ASSERT_TRUE(container_helper->WriteDefaultData(session, NULL, 0, TEST_DATA_COUNT));
    ASSERT_TRUE(container_helper->WriteDefaultData(session, NULL, 0, TEST_DATA_COUNT));
    ASSERT_FALSE(container_helper->WriteDefaultData(session, NULL, 0, TEST_DATA_COUNT));
    // System should be full now

    // Delete a bit out of it
    // Actually, this are move operations. It tests if move operations are also possible
    // if the system is full
    ASSERT_TRUE(session->Delete(container_helper->data_address(4),
            container_helper->fingerprint(4).data(),
            container_helper->fingerprint(4).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(3),
            container_helper->fingerprint(3).data(),
            container_helper->fingerprint(3).size(), NO_EC));

    ASSERT_TRUE(session->Delete(container_helper->data_address(1),
            container_helper->fingerprint(1).data(),
            container_helper->fingerprint(1).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(0),
            container_helper->fingerprint(0).data(),
            container_helper->fingerprint(0).size(), NO_EC));

    ASSERT_TRUE(session->Delete(container_helper->data_address(8),
            container_helper->fingerprint(8).data(),
            container_helper->fingerprint(8).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(9),
            container_helper->fingerprint(9).data(),
            container_helper->fingerprint(9).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(12),
            container_helper->fingerprint(12).data(),
            container_helper->fingerprint(12).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(13),
            container_helper->fingerprint(13).data(),
            container_helper->fingerprint(13).size(), NO_EC));

    sleep(5);
    ASSERT_TRUE(session->Close());
    session = NULL;

    ASSERT_TRUE(storage->Flush(NO_EC));

    bool aborted = false;
    bool result = storage->TryMergeContainer(container_helper->data_address(2), container_helper->data_address(13), &aborted);
    ASSERT_TRUE(result);
    ASSERT_FALSE(aborted);
}

bool ReadAndCheckContainer(ContainerStorage* storage, tbb::atomic<bool>* stop_flag,
                           ContainerTestHelper* container_helper) {

    byte* data_buffer = new byte[512 * 1024];
    size_t data_size = 512 * 1024;
    bool failed = false;

    while (!(*stop_flag) && !failed) {
        StorageSession* session = storage->CreateSession();

        data_size = 512 * 1024;
        bool r = session->Read(container_helper->data_address(14), container_helper->fingerprint(14).data(), container_helper->fingerprint(14).size(), data_buffer, &data_size, NO_EC);
        if (!r) {
            failed = true;
        }

        session->Close();
        session = NULL;
    }
    delete[] data_buffer;
    return !failed;
}

bool LookupAndCheckContainer(ContainerStorage* storage, tbb::atomic<bool>* stop_flag, uint64_t container_id) {
    while (!(*stop_flag)) {
        pair<lookup_result, ContainerStorageAddressData> container_address =
            storage->LookupContainerAddressWait(container_id, NULL, false);
        CHECK(container_address.first == LOOKUP_FOUND, "Failed to lookup container address: " << container_id);
        TRACE("Found address: " << container_address.second.DebugString());
    }
    return true;
}

TEST_P(ContainerStorageTest, Extend) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, 0, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    ASSERT_TRUE(storage->Flush(NO_EC));

    DEBUG(storage->PrintStatistics());

    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("size", "2G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(StartContext::FORCE);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(log->PerformDirtyReplay());
    ASSERT_TRUE(storage->Run());

    DEBUG(storage->PrintStatistics());

    s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, TEST_DATA_COUNT / 2, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    ASSERT_TRUE(storage->Flush(NO_EC));

    DEBUG(storage->PrintStatistics());
}

TEST_P(ContainerStorageTest, RestartMissingFile) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, 0, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    DEBUG(storage->PrintStatistics());

    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    ASSERT_TRUE(storage->SetOption("filename.clear", "true"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-2"));
    // container-data-1 is missing

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

TEST_P(ContainerStorageTest, RestartWrongFileOrder) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, 0, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    DEBUG(storage->PrintStatistics());

    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    ASSERT_TRUE(storage->SetOption("filename.clear", "true"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-2"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-1"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

TEST_P(ContainerStorageTest, RestartChangeContainerSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, 0, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    DEBUG(storage->PrintStatistics());

    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    ASSERT_TRUE(storage->SetOption("container-size", "2M"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

TEST_P(ContainerStorageTest, ExtendWithoutForce) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, 0, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    DEBUG(storage->PrintStatistics());

    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("size", "2G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

TEST_P(ContainerStorageTest, DoubleExtend) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, 0, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    ASSERT_TRUE(storage->Flush(NO_EC));

    DEBUG(storage->PrintStatistics());

    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("size", "2G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(StartContext::FORCE);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(log->PerformDirtyReplay());
    ASSERT_TRUE(storage->Run());

    ASSERT_TRUE(storage->Close());

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // 1. extend
    ASSERT_TRUE(storage->SetOption("size", "4G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));
    // 2. extend
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-5"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-6"));

    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(StartContext::FORCE);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(log->PerformDirtyReplay());
    ASSERT_TRUE(storage->Run());

    DEBUG(storage->PrintStatistics());

    s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, TEST_DATA_COUNT / 2, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    ASSERT_TRUE(storage->Flush(NO_EC));

    DEBUG(storage->PrintStatistics());
}

TEST_P(ContainerStorageTest, ExtendWithExplicitSize) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, 0, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    DEBUG(storage->PrintStatistics());

    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("size", "2560M"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filesize", "1G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(StartContext::FORCE);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(log->PerformDirtyReplay());
    ASSERT_TRUE(storage->Run());

    DEBUG(storage->PrintStatistics());

    s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, TEST_DATA_COUNT / 2, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    ASSERT_TRUE(storage->Flush(NO_EC));

    DEBUG(storage->PrintStatistics());
}

TEST_P(ContainerStorageTest, ExtendWithIllegalExplicitSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, 0, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("size", "2560M"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filesize", "1G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));
    ASSERT_TRUE(storage->SetOption("filesize", "1G"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(StartContext::FORCE);
    ASSERT_FALSE(storage->Start(start_context, &system)) << "Should fail because we didn't change the total size";
}

TEST_P(ContainerStorageTest, ExtendWithoutChangingSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, 0, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;

    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(StartContext::FORCE);
    ASSERT_FALSE(storage->Start(start_context, &system)) << "Should fail because we didn't change the total size";
}

TEST_P(ContainerStorageTest, IllegalExplicitSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->SetOption("filesize", "1G"));
    ASSERT_FALSE(storage->Start(StartContext(), &system));
}

TEST_P(ContainerStorageTest, IllegalExplicitSize2) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->SetOption("filesize", "1023M"));
    ASSERT_FALSE(storage->Start(StartContext(), &system));
}

TEST_P(ContainerStorageTest, IllegalSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->SetOption("size", "1023M"));
    ASSERT_FALSE(storage->Start(StartContext(), &system));
}

TEST_P(ContainerStorageTest, ChangeExplcitSizeOfExistingFile) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, 0, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;
    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // change explicit size of existing file
    ASSERT_TRUE(storage->SetOption("size", "1536M"));
    ASSERT_TRUE(storage->SetOption("filesize", "1G"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

TEST_P(ContainerStorageTest, ChangeExplcitSizeOfExistingFileWithForce) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    StorageSession* s = storage->CreateSession();
    container_helper->WriteDefaultData(s, NULL, 0, TEST_DATA_COUNT / 2);
    s->Close();
    s = NULL;
    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // change explicit size of existing file
    ASSERT_TRUE(storage->SetOption("size", "1536M"));
    ASSERT_TRUE(storage->SetOption("filesize", "1G"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(StartContext::FORCE);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

/**
 * Tests if the merged of a chain of containers happens without race conditions when the container
 * container is read in a paralell thread
 * This test might be flaky as we are trying to hit a race condition.
 */
TEST_P(ContainerStorageTest, ReadDuringMerge) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    struct StorageSession* session = storage->CreateSession();

    WriteTestData(session);

    ASSERT_TRUE(session->Delete(container_helper->data_address(4),
            container_helper->fingerprint(4).data(),
            container_helper->fingerprint(4).size(), NO_EC));

    ASSERT_TRUE(session->Delete(container_helper->data_address(1),
            container_helper->fingerprint(1).data(),
            container_helper->fingerprint(1).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(0),
            container_helper->fingerprint(0).data(),
            container_helper->fingerprint(0).size(), NO_EC));

    ASSERT_TRUE(session->Delete(container_helper->data_address(8),
            container_helper->fingerprint(8).data(),
            container_helper->fingerprint(8).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(9),
            container_helper->fingerprint(9).data(),
            container_helper->fingerprint(9).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(12),
            container_helper->fingerprint(12).data(),
            container_helper->fingerprint(12).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(13),
            container_helper->fingerprint(13).data(),
            container_helper->fingerprint(13).size(), NO_EC));

    ASSERT_TRUE(storage->Flush(NO_EC));

    bool aborted = false;
    ASSERT_TRUE(storage->TryMergeContainer(container_helper->data_address(0), container_helper->data_address(3), &aborted));
    ASSERT_FALSE(aborted);

    aborted = false;
    ASSERT_TRUE(storage->TryMergeContainer(container_helper->data_address(10), container_helper->data_address(14), &aborted));
    ASSERT_FALSE(aborted);

    ASSERT_TRUE(session->Delete(container_helper->data_address(2),
            container_helper->fingerprint(2).data(),
            container_helper->fingerprint(2).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(10),
            container_helper->fingerprint(10).data(),
            container_helper->fingerprint(10).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(11),
            container_helper->fingerprint(11).data(),
            container_helper->fingerprint(11).size(), NO_EC));
    EXPECT_TRUE(session->Close());
    session = NULL;

    tbb::atomic<bool> stop_flag;
    stop_flag = false;
    Thread<bool> read_thread(NewRunnable(&ReadAndCheckContainer,
                                 storage,
                                 &stop_flag,
                                 container_helper), "lookup thread");
    ASSERT_TRUE(read_thread.Start());

    ThreadUtil::Sleep(rand() % 1500, ThreadUtil::MILLISECONDS);

    int i = 0;
    do {
        aborted = false;
        i++;
        ASSERT_TRUE(storage->TryMergeContainer(container_helper->data_address(0), 4, &aborted));
    } while (aborted && (i < 30));

    ASSERT_FALSE(aborted);

    stop_flag = true;

    bool result = false;
    ASSERT_TRUE(read_thread.Join(&result));
    ASSERT_TRUE(result);
}

/**
 * Tests if the merged of a chain of containers happens without race conditions when the container
 * address is lookuped up in between.
 * This test might be flaky as we are trying to hit a race condition.
 */
TEST_P(ContainerStorageTest, LookupDuringMerge) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    struct StorageSession* session = storage->CreateSession();

    WriteTestData(session);

    ASSERT_TRUE(session->Delete(container_helper->data_address(4),
            container_helper->fingerprint(4).data(),
            container_helper->fingerprint(4).size(), NO_EC));

    ASSERT_TRUE(session->Delete(container_helper->data_address(1),
            container_helper->fingerprint(1).data(),
            container_helper->fingerprint(1).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(0),
            container_helper->fingerprint(0).data(),
            container_helper->fingerprint(0).size(), NO_EC));

    ASSERT_TRUE(session->Delete(container_helper->data_address(8),
            container_helper->fingerprint(8).data(),
            container_helper->fingerprint(8).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(9),
            container_helper->fingerprint(9).data(),
            container_helper->fingerprint(9).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(12),
            container_helper->fingerprint(12).data(),
            container_helper->fingerprint(12).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(13),
            container_helper->fingerprint(13).data(),
            container_helper->fingerprint(13).size(), NO_EC));

    ASSERT_TRUE(storage->Flush(NO_EC));

    bool aborted = false;
    ASSERT_TRUE(storage->TryMergeContainer(container_helper->data_address(0), container_helper->data_address(3), &aborted));
    ASSERT_FALSE(aborted);

    aborted = false;
    ASSERT_TRUE(storage->TryMergeContainer(container_helper->data_address(10), container_helper->data_address(14), &aborted));
    ASSERT_FALSE(aborted);

    ASSERT_TRUE(session->Delete(container_helper->data_address(2),
            container_helper->fingerprint(2).data(),
            container_helper->fingerprint(2).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(10),
            container_helper->fingerprint(10).data(),
            container_helper->fingerprint(10).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(11),
            container_helper->fingerprint(11).data(),
            container_helper->fingerprint(11).size(), NO_EC));
    EXPECT_TRUE(session->Close());
    session = NULL;

    tbb::atomic<bool> stop_flag;
    stop_flag = false;
    Thread<bool> lookup_thread(NewRunnable(&LookupAndCheckContainer, storage, &stop_flag, container_helper->data_address(14)), "lookup thread");
    ASSERT_TRUE(lookup_thread.Start());

    aborted = false;
    ASSERT_TRUE(storage->TryMergeContainer(container_helper->data_address(0), 4, &aborted));
    ASSERT_FALSE(aborted);

    stop_flag = true;

    bool result = false;
    ASSERT_TRUE(lookup_thread.Join(&result));
    ASSERT_TRUE(result);
}

TEST_P(ContainerStorageTest, DeleteAfterMerge) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    struct StorageSession* session = storage->CreateSession();

    WriteTestData(session);

    ASSERT_TRUE(session->Delete(container_helper->data_address(4),
            container_helper->fingerprint(4).data(),
            container_helper->fingerprint(4).size(), NO_EC));

    ASSERT_TRUE(session->Delete(container_helper->data_address(1),
            container_helper->fingerprint(1).data(),
            container_helper->fingerprint(1).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(0),
            container_helper->fingerprint(0).data(),
            container_helper->fingerprint(0).size(), NO_EC));

    ASSERT_TRUE(storage->Flush(NO_EC));

    bool aborted = false;
    ASSERT_TRUE(storage->TryMergeContainer(container_helper->data_address(0), container_helper->data_address(3), &aborted));
    ASSERT_FALSE(aborted);

    pair<lookup_result, ContainerStorageAddressData> old_address0 =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(old_address0.first, LOOKUP_FOUND);
    pair<lookup_result, ContainerStorageAddressData> old_address3 =
        storage->LookupContainerAddress(container_helper->data_address(3), NULL, false);
    ASSERT_EQ(old_address3.first, LOOKUP_FOUND);

    ASSERT_TRUE(session->Delete(container_helper->data_address(3),
            container_helper->fingerprint(3).data(),
            container_helper->fingerprint(3).size(), NO_EC));
    EXPECT_TRUE(session->Close());
    session = NULL;

    // here we test the COW property
    pair<lookup_result, ContainerStorageAddressData> new_address0 =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(new_address0.first, LOOKUP_FOUND);
    pair<lookup_result, ContainerStorageAddressData> new_address3 =
        storage->LookupContainerAddress(container_helper->data_address(3), NULL, false);
    ASSERT_EQ(new_address3.first, LOOKUP_FOUND);

    ASSERT_FALSE(old_address0.second.file_index() == new_address0.second.file_index() &&
        old_address0.second.file_offset() == new_address0.second.file_offset()) <<
    "container of item 0 hasn't changed during merge";
    ASSERT_FALSE(old_address3.second.file_index() == new_address3.second.file_index() &&
        old_address3.second.file_offset() == new_address3.second.file_offset()) <<
    "container of item 3 hasn't changed during merge";
    ASSERT_TRUE(new_address0.second.file_index() == new_address3.second.file_index() &&
        new_address0.second.file_offset() == new_address3.second.file_offset()) <<
    "address of item 0 and item 3 should be the same after the merge";
}

TEST_P(ContainerStorageTest, NextContainerIDAfterClose) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    StorageSession* session = storage->CreateSession();

    WriteTestData(session);
    ReadTestData(session);

    uint64_t container_id = storage->GetLastGivenContainerId();
    ASSERT_GT(container_id, static_cast<uint64_t>(2));

    ASSERT_TRUE(session->Close());
    session = NULL;

    ASSERT_TRUE(storage->Close());
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));

    uint64_t new_container_id = storage->GetLastGivenContainerId();
    ASSERT_EQ(container_id, new_container_id) << "last given container id not restored after close";
}

TEST_P(ContainerStorageTest, NextContainerIDAfterCrash) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    StorageSession* session = storage->CreateSession();

    WriteTestData(session);
    ReadTestData(session);

    uint64_t container_id = storage->GetLastGivenContainerId();

    session->Close();
    session = NULL;
    storage->Close();
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create(
                                                  "container-storage"));
    ASSERT_TRUE(storage);
    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));

    storage->SetLastGivenContainerId(0);

    ASSERT_TRUE(log->PerformDirtyReplay());

    uint64_t new_container_id = storage->GetLastGivenContainerId();
    ASSERT_EQ(container_id, new_container_id) << "last given container id not restored after close";
    ASSERT_TRUE(container_id > 0);
}

TEST_P(ContainerStorageTest, SessionClose) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    StorageSession* session = storage->CreateSession();

    WriteTestData(session);
    ReadTestData(session);

    session->Close();
    session = NULL;

    StorageSession* session2 = storage->CreateSession();
    ReadTestData(session2);
    session2->Close();
    session2 = NULL;
}

TEST_P(ContainerStorageTest, CommitOnFlush) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    StorageSession* session = storage->CreateSession();
    WriteTestData(session);
    ReadTestData(session);

    ASSERT_TRUE(session->Close());

    ASSERT_TRUE(storage->Flush(NO_EC));

    StorageSession* session2 = storage->CreateSession();

    ReadTestData(session2);
    ASSERT_TRUE(session2->Close());
}

TEST_P(ContainerStorageTest, CommitOnStorageClose) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    StorageSession* session = storage->CreateSession();

    WriteTestData(session);
    ReadTestData(session);

    ASSERT_TRUE(session->Close());
    ASSERT_TRUE(storage->Close());

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create(
                                                  "container-storage"));
    ASSERT_TRUE(storage);
    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());

    session = storage->CreateSession();
    ASSERT_TRUE(session);

    ReadTestData(session);

    ASSERT_TRUE(session->Close());
}

TEST_P(ContainerStorageTest, IsCommited) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    StorageSession* session = storage->CreateSession();
    ASSERT_TRUE(session);

    WriteTestData(session);

    ASSERT_EQ(STORAGE_ADDRESS_COMMITED, storage->IsCommitted(1));

    ASSERT_EQ(STORAGE_ADDRESS_NOT_COMMITED, storage->IsCommitted(500));

    ASSERT_TRUE(session->Close());
}

TEST_P(ContainerStorageTest, IsCommittedOnFlush) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    struct StorageSession* session = storage->CreateSession();
    ASSERT_TRUE(session);

    ASSERT_TRUE(session->WriteNew(container_helper->fingerprint(0).data(),
            container_helper->fingerprint(0).size(), container_helper->data(0), TEST_DATA_SIZE, true, container_helper->mutable_data_address(0), NO_EC))
    << "Write " << 0 << " failed";
    ASSERT_EQ(STORAGE_ADDRESS_NOT_COMMITED, storage->IsCommitted(1)) << "Container shouldn't be committed before flush";

    ASSERT_TRUE(storage->Flush(NO_EC));
    ASSERT_EQ(STORAGE_ADDRESS_COMMITED, storage->IsCommitted(1)) << "Container should be comitted after flush";

    ASSERT_TRUE(session->Close());
}

TEST_P(ContainerStorageTest, IsCommittedWaitOnFlush) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    struct StorageSession* session = storage->CreateSession();
    ASSERT_TRUE(session);

    ASSERT_TRUE(session->WriteNew(container_helper->fingerprint(0).data(),
            container_helper->fingerprint(0).size(), container_helper->data(0), TEST_DATA_SIZE, true, container_helper->mutable_data_address(0), NO_EC))
    << "Write " << 0 << " failed";
    ASSERT_EQ(STORAGE_ADDRESS_COMMITED, storage->IsCommittedWait(1)) << "Container should be comitted after IsCommittedWait";

    ASSERT_TRUE(session->Close());
}

TEST_P(ContainerStorageTest, Merge) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    struct StorageSession* session = storage->CreateSession();

    WriteTestData(session);

    ASSERT_TRUE(session->Delete(container_helper->data_address(4),
            container_helper->fingerprint(4).data(),
            container_helper->fingerprint(4).size(), NO_EC));

    ASSERT_TRUE(session->Delete(container_helper->data_address(1),
            container_helper->fingerprint(1).data(),
            container_helper->fingerprint(1).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(0),
            container_helper->fingerprint(0).data(),
            container_helper->fingerprint(0).size(), NO_EC));

    ASSERT_TRUE(storage->Flush(NO_EC));

    pair<lookup_result, ContainerStorageAddressData> old_address0 =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(old_address0.first, LOOKUP_FOUND);
    pair<lookup_result, ContainerStorageAddressData> old_address3 =
        storage->LookupContainerAddress(container_helper->data_address(3), NULL, false);
    ASSERT_EQ(old_address3.first, LOOKUP_FOUND);

    bool aborted = false;
    ASSERT_TRUE(storage->TryMergeContainer(container_helper->data_address(0), container_helper->data_address(3), &aborted));
    ASSERT_FALSE(aborted);

    // here we test the COW property
    pair<lookup_result, ContainerStorageAddressData> new_address0 =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(new_address0.first, LOOKUP_FOUND);
    pair<lookup_result, ContainerStorageAddressData> new_address3 =
        storage->LookupContainerAddress(container_helper->data_address(3), NULL, false);
    ASSERT_EQ(new_address3.first, LOOKUP_FOUND);

    ASSERT_FALSE(old_address0.second.file_index() == new_address0.second.file_index() &&
        old_address0.second.file_offset() == new_address0.second.file_offset()) <<
    "container of item 0 hasn't changed during merge";
    ASSERT_FALSE(old_address3.second.file_index() == new_address3.second.file_index() &&
        old_address3.second.file_offset() == new_address3.second.file_offset()) <<
    "container of item 3 hasn't changed during merge";
    ASSERT_TRUE(new_address0.second.file_index() == new_address3.second.file_index() &&
        new_address0.second.file_offset() == new_address3.second.file_offset()) <<
    "address of item 0 and item 3 should be the same after the merge";
    int i = 0;

    byte result[2][TEST_DATA_SIZE];
    memset(result, 0, 2 * TEST_DATA_SIZE);

    size_t result_size[2];
    result_size[0] = TEST_DATA_SIZE;
    result_size[1] = TEST_DATA_SIZE;

    for (i = 0; i < 2; i++) {
        ASSERT_TRUE(session->Read(container_helper->data_address(i + 2),
                container_helper->fingerprint(i + 2).data(),
                container_helper->fingerprint(i + 2).size(), result[i], &result_size[i], NO_EC)) << "Read " << (i + 2) << " failed";
        DEBUG("Read CRC " << (i + 2) << " - " << crc(result[i], result_size[i]));
    }

    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(result_size[i] == TEST_DATA_SIZE) << "Read " << i << " error";
        ASSERT_TRUE(memcmp(container_helper->data(i + 2), result[i], result_size[i]) == 0) << "Compare " << (i + 2) << " error";
    }

    EXPECT_TRUE(session->Close());
    session = NULL;
}

/**
 * This unit tests verify the behavior of the merge operations during a crash.
 */
TEST_P(ContainerStorageTest, MergeWithCrash) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    struct StorageSession* session = storage->CreateSession();

    WriteTestData(session);

    ASSERT_TRUE(session->Delete(container_helper->data_address(4),
            container_helper->fingerprint(4).data(),
            container_helper->fingerprint(4).size(), NO_EC));

    ASSERT_TRUE(session->Delete(container_helper->data_address(1),
            container_helper->fingerprint(1).data(),
            container_helper->fingerprint(1).size(), NO_EC));
    ASSERT_TRUE(session->Delete(container_helper->data_address(0),
            container_helper->fingerprint(0).data(),
            container_helper->fingerprint(0).size(), NO_EC));

    ASSERT_TRUE(storage->Flush(NO_EC));

    pair<lookup_result, ContainerStorageAddressData> old_address0 =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(old_address0.first, LOOKUP_FOUND);
    pair<lookup_result, ContainerStorageAddressData> old_address3 =
        storage->LookupContainerAddress(container_helper->data_address(3), NULL, false);
    ASSERT_EQ(old_address3.first, LOOKUP_FOUND);

    bool aborted = false;
    ASSERT_TRUE(storage->TryMergeContainer(container_helper->data_address(0), container_helper->data_address(3), &aborted));
    ASSERT_FALSE(aborted);

    // here we test the COW property
    pair<lookup_result, ContainerStorageAddressData> new_address0 =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(new_address0.first, LOOKUP_FOUND);
    pair<lookup_result, ContainerStorageAddressData> new_address3 =
        storage->LookupContainerAddress(container_helper->data_address(3), NULL, false);
    ASSERT_EQ(new_address3.first, LOOKUP_FOUND);

    ASSERT_FALSE(old_address0.second.file_index() == new_address0.second.file_index() &&
        old_address0.second.file_offset() == new_address0.second.file_offset()) <<
    "container of item 0 hasn't changed during merge";
    ASSERT_FALSE(old_address3.second.file_index() == new_address3.second.file_index() &&
        old_address3.second.file_offset() == new_address3.second.file_offset()) <<
    "container of item 3 hasn't changed during merge";
    ASSERT_TRUE(new_address0.second.file_index() == new_address3.second.file_index() &&
        new_address0.second.file_offset() == new_address3.second.file_offset()) <<
    "address of item 0 and item 3 should be the same after the merge";

    EXPECT_TRUE(session->Close());
    session = NULL;

    // introduce a invalid state. This simulates the state when the system crashes
    // during the LogAck update routine.
    uint64_t container_id = container_helper->data_address(3);
    ASSERT_EQ(PUT_OK, storage->meta_data_index()->Put(&container_id, sizeof(container_id), old_address3.second));

    ASSERT_NO_FATAL_FAILURE(Restart());

    // verify data
    int i = 0;

    byte result[2][TEST_DATA_SIZE];
    memset(result, 0, 2 * TEST_DATA_SIZE);

    size_t result_size[2];
    result_size[0] = TEST_DATA_SIZE;
    result_size[1] = TEST_DATA_SIZE;

    session = storage->CreateSession();
    for (i = 0; i < 2; i++) {
        EXPECT_TRUE(session->Read(container_helper->data_address(i + 2),
                container_helper->fingerprint(i + 2).data(),
                container_helper->fingerprint(i + 2).size(), result[i], &result_size[i], NO_EC)) << "Read " << (i + 2) << " failed";
        DEBUG("Read CRC " << (i + 2) << " - " << crc(result[i], result_size[i]));
    }

    for (int i = 0; i < 2; i++) {
        EXPECT_TRUE(result_size[i] == TEST_DATA_SIZE) << "Read " << i << " error";
        EXPECT_TRUE(memcmp(container_helper->data(i + 2), result[i], result_size[i]) == 0) << "Compare " << (i + 2) << " error";
    }
    EXPECT_TRUE(session->Close());
    session = NULL;
}

TEST_P(ContainerStorageTest, MergeWithSameContainerId) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("merge").Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    struct StorageSession* session = storage->CreateSession();

    WriteTestData(session);

    ASSERT_TRUE(session->Delete(container_helper->data_address(0),
            container_helper->fingerprint(0).data(),
            container_helper->fingerprint(0).size(), NO_EC));
    EXPECT_TRUE(session->Close());
    session = NULL;

    ASSERT_TRUE(storage->Flush(NO_EC));

    bool aborted = false;
    ASSERT_FALSE(storage->TryMergeContainer(container_helper->data_address(0), container_helper->data_address(0), &aborted));
    ASSERT_FALSE(aborted);
}

TEST_P(ContainerStorageTest, MergeWithSameContainerLock) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Search lock pair");

    uint64_t container_id1 = 1;
    uint64_t container_id2 = 2;
    dedupv1::base::ReadWriteLock* lock1 = storage->GetContainerLock(container_id1);
    dedupv1::base::ReadWriteLock* lock2 = NULL;
    while (lock1 != lock2) {
        container_id2++;
        lock2 = storage->GetContainerLock(container_id2);
    }
    // Here we have found a lock pair

    byte buffer[1024];
    memset(buffer, 0, 1024);
    uint64_t key1 = 1;
    uint64_t key2 = 2;
    Container container1;
    ASSERT_TRUE(container1.Init(container_id1, storage->GetContainerSize()));
    ASSERT_TRUE(container1.AddItem(reinterpret_cast<const byte*>(&key1), sizeof(key1), buffer, 1024, true, NULL));
    Container container2;
    ASSERT_TRUE(container2.Init(container_id2, storage->GetContainerSize()));
    ASSERT_TRUE(container2.AddItem(reinterpret_cast<const byte*>(&key2), sizeof(key2), buffer, 1024, true, NULL));

    ContainerStorageAddressData address1;
    ContainerStorageAddressData address2;
    ASSERT_TRUE(storage->allocator()->OnNewContainer(container1, true, &address1));
    ASSERT_TRUE(storage->allocator()->OnNewContainer(container2, true, &address2));

    DEBUG("Write container");
    ASSERT_TRUE(storage->CommitContainer(&container1, address1));
    ASSERT_TRUE(storage->CommitContainer(&container2, address2));

    GreedyContainerGCStrategy* gc = static_cast<GreedyContainerGCStrategy*>(storage->GetGarbageCollection());
    ASSERT_TRUE(gc);

    uint64_t bucket = 0;
    gc->merge_candidates()->Delete(&bucket, sizeof(bucket));

    DEBUG("Merge container");
    bool aborted = false;
    ASSERT_TRUE(storage->TryMergeContainer(container_id1, container_id2, &aborted));
    ASSERT_FALSE(aborted);
}

TEST_P(ContainerStorageTest, WriteReadRead) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    StorageSession* session = storage->CreateSession();

    WriteTestData(session);
    ReadTestData(session);

    session->Close();
    session = NULL;

    storage->Close();
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create(
                                                  "container-storage"));
    ASSERT_TRUE(storage);
    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());

    session = storage->CreateSession();

    ReadTestData(session);

    ASSERT_TRUE(session->Close());
}

TEST_P(ContainerStorageTest, Timeout) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    if (storage->HasCommitTimeout() == false) {
        return;
    }
    StorageSession* session = storage->CreateSession();
    WriteTestData(session);
    session->Close();
    session = NULL;

    sleep(2 * storage->GetTimeoutSeconds());

    for (size_t i = 0; i < TEST_DATA_COUNT; i++) {
        ASSERT_EQ(storage->IsCommitted(container_helper->data_address(i)), STORAGE_ADDRESS_COMMITED);
    }
}

TEST_P(ContainerStorageTest, ReadContainer) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    StorageSession* session = storage->CreateSession();
    WriteTestData(session);

    ASSERT_TRUE(session->Close());
    session = NULL;

    ASSERT_TRUE(storage->Flush(NO_EC)); // data is no committed

    for (int i = 0; i < TEST_DATA_COUNT; i++) {
        Container container;
        container.Init(container_helper->data_address(i), storage->GetContainerSize());

        enum lookup_result r = storage->ReadContainer(&container);
        ASSERT_EQ(r, LOOKUP_FOUND);

        ContainerItem* item = container.FindItem(container_helper->fingerprint(i).data(), container_helper->fingerprint(i).size());
        ASSERT_TRUE(item);
    }
}

TEST_P(ContainerStorageTest, ReadContainerWithCache) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    StorageSession* session = storage->CreateSession();
    WriteTestData(session);
    ASSERT_TRUE(session->Close());
    ASSERT_TRUE(storage->Flush(NO_EC)); // data is now committed

    for (int i = 0; i < TEST_DATA_COUNT; i++) {
        Container container;
        ASSERT_TRUE(container.Init(container_helper->data_address(i), storage->GetContainerSize()));

        enum lookup_result r = storage->ReadContainerWithCache(
            &container);
        ASSERT_EQ(LOOKUP_FOUND, r) << "container " << container.DebugString();

        ContainerItem* item = container.FindItem(container_helper->fingerprint(i).data(), container_helper->fingerprint(i).size());
        ASSERT_TRUE(item);
    }
}

INSTANTIATE_TEST_CASE_P(ContainerStorage,
    StorageTest,
    ::testing::Values("container-storage;filename=work/container-data;meta-data=static-disk-hash;meta-data.page-size=2K;meta-data.size=4M;meta-data.filename=work/container-meta"));

INSTANTIATE_TEST_CASE_P(ContainerStorage,
    ContainerStorageTest,
    ::testing::Combine(
        ::testing::Values("", "deflate", "bz2", "lz4", "snappy"),
        ::testing::Values(0, 1, 2)));

}
}
