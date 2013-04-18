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

#include <gtest/gtest.h>

#include <core/dedup.h>
#include <base/strutil.h>

#include <base/locks.h>
#include <base/logging.h>
#include <core/log_consumer.h>
#include <core/log.h>
#include <core/chunk_index.h>
#include <core/block_mapping.h>
#include <base/memory.h>
#include <core/garbage_collector.h>
#include <core/usage_count_garbage_collector.h>
#include <base/protobuf_util.h>
#include <base/strutil.h>

#include "block_mapping_test.h"
#include <test_util/log_assert.h>

#include <test/log_mock.h>
#include <test/chunk_index_mock.h>
#include <test/dedup_system_mock.h>
#include <test/container_storage_mock.h>
#include <test/storage_mock.h>
#include "container_test_helper.h"
#include "dedup_system_test.h"

using std::map;
using std::set;
using std::string;
using std::list;
using std::multimap;
using std::make_pair;
using std::pair;
using testing::Return;
using testing::_;
using dedupv1::base::make_bytestring;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::blockindex::BlockMappingTest;
using dedupv1::chunkstore::STORAGE_ADDRESS_COMMITED;
using dedupv1::base::ScopedArray;
using dedupv1::Fingerprinter;
using dedupv1::base::PersistentIndex;
using dedupv1::base::IndexCursor;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::lookup_result;
using dedupv1::chunkindex::ChunkIndex;
using dedupv1::log::LogReplayContext;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::log::EVENT_TYPE_LOG_EMPTY;
using dedupv1::log::EVENT_REPLAY_MODE_DIRECT;
using dedupv1::log::event_type;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_DELETED;
using dedupv1::blockindex::BlockIndex;
using dedupv1::log::LogConsumer;
using dedupv1::log::Log;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::MessageEquals;

LOGGER("GarbageCollectorTest");

namespace dedupv1 {
namespace gc {

class LastEntryLogConsumer : public LogConsumer {
public:
    enum event_type last_event_type;
    LogEventData last_event_value;
    uint64_t last_log_id;

    LastEntryLogConsumer() {
        last_log_id = 0;
        last_event_type = dedupv1::log::EVENT_TYPE_NONE;
    }

    virtual bool LogReplay(enum event_type event_type, const LogEventData& event_value,
                           const LogReplayContext& context) {

        DEBUG("Log event: " << event_value.ShortDebugString());

        if (event_type != dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN) {
            return true;
        }
        if (MessageEquals(last_event_value, event_value)) {
            // this is the re-replay
            return true;
        }
        last_event_type = event_type;
        last_event_value = event_value;
        last_log_id = context.log_id();
        DEBUG("Replay event " << Log::GetReplayModeName(context.replay_mode()) << " - " << Log::GetEventTypeName(event_type));
        return true;
    }

    void Clear() {
    }
};

/**
 * This class tests the garbage collector, but in contrast to the
 * UsageCountGarbageCollectorTest class, we here use the full suite. So it is
 * more an integration test as usual unit test.
 */
class UsageCountGarbageCollectorIntegrationTest : public testing::TestWithParam<const char*> {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;

    DedupSystem* system;
    DedupSystem* crashed_system;

    UsageCountGarbageCollector* gc;
    BlockIndex* block_index;

    ContainerTestHelper* container_test_helper;

    virtual void SetUp() {
        container_test_helper = NULL;
        crashed_system = NULL;
        gc = NULL;
        block_index = NULL;

        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());

        system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
        ASSERT_TRUE(system);

        gc = dynamic_cast<UsageCountGarbageCollector*>(
            system->garbage_collector());
        ASSERT_TRUE(gc);
        block_index = system->block_index();
        ASSERT_TRUE(block_index);

        container_test_helper = new ContainerTestHelper(64 * 1024, 16);
        ASSERT_TRUE(container_test_helper);
        ASSERT_TRUE(container_test_helper->SetUp());
    }

    void Crash() {
        ASSERT_TRUE(crashed_system == NULL);
        system->ClearData();
        crashed_system = system;
        system = NULL;
        block_index = NULL;
        gc = NULL;

        system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true, true);

        gc = dynamic_cast<UsageCountGarbageCollector*>(system->garbage_collector());
        ASSERT_TRUE(gc);
        block_index = system->block_index();
        ASSERT_TRUE(block_index);
    }

    void Restart() {
        ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
        ASSERT_TRUE(system->Close());
        system = NULL;
        block_index = NULL;
        gc = NULL;

        system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true);

        gc = dynamic_cast<UsageCountGarbageCollector*>(system->garbage_collector());
        ASSERT_TRUE(gc);
        block_index = system->block_index();
        ASSERT_TRUE(block_index);
    }

    virtual void TearDown() {
        if (system) {
            ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
            ASSERT_TRUE(system->Close());
            gc = NULL;
            block_index = NULL;
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

TEST_P(UsageCountGarbageCollectorIntegrationTest, FailedBlockMappingWrite) {
    EXPECT_LOGGING(dedupv1::test::INFO).Matches("Current event has already been processed.*").Repeatedly();

    LastEntryLogConsumer c;
    ASSERT_TRUE(system->log()->RegisterConsumer("c", &c));
    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 0, 16));
    ASSERT_TRUE(system->storage()->Flush(NO_EC));
    container_test_helper->LoadContainerDataIntoChunkIndex(system);

    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));
    BlockMapping m1(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillBlockMapping(&m1));
    m1.set_version(m1.version() + 1);
    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));
    ASSERT_TRUE(system->log()->WaitUntilDirectReplayQueueEmpty(0));

    LogEventData written_event_value = c.last_event_value;
    DEBUG("Event " << written_event_value.ShortDebugString());
    uint64_t written_event_log_id = c.last_log_id;
    EXPECT_TRUE(written_event_value.has_block_mapping_written_event());

    EXPECT_TRUE(system->log()->UnregisterConsumer("c"));

    Restart();

    BlockMappingWriteFailedEventData event_data;
    event_data.mutable_mapping_pair()->CopyFrom(written_event_value.block_mapping_written_event().mapping_pair());
    event_data.set_write_event_log_id(written_event_log_id);
    ASSERT_TRUE(system->log()->CommitEvent(dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED, &event_data, NULL, NULL, NO_EC));

    Restart();

    INFO("Replay all");

    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    gc->no_gc_candidates_during_last_try_ = false;
    ASSERT_TRUE(gc->StartProcessing());
    do {
        sleep(4);
    } while (!gc->no_gc_candidates_during_last_try_);
    ASSERT_TRUE(gc->StopProcessing());

    ChunkMapping cm2(container_test_helper->fingerprint(0));
    lookup_result lr = system->chunk_index()->Lookup(&cm2, false, NO_EC);
    EXPECT_EQ(LOOKUP_NOT_FOUND, lr) << "Fingerprint should not be in chunk index anymore: " << cm2.DebugString();
}

/**
 * This unit test aim to create a "No Chunk Mapping Found" message after a log replay.
 * This may have causes the issue #37 and #86.
 *
 * First step:
 * - Write a block mapping with a +1 on a chunk c
 * - Write another block mapping with a +1 on a chunk c
 * - Write a block mapping with a -1 on a chunk c
 * - Force the gc to execute the last log entry twice.
 * - Write another block mapping with a -1 on a chunk c.
 *
 * The gc should be detect that the 3. entry has been executed twice. The after the log replay usage
 * count should be 1 and not 0. At the end the usage counter should be zero.
 */
TEST_P(UsageCountGarbageCollectorIntegrationTest, NoChunkMappingFoundAfterLogReplay) {
    EXPECT_LOGGING(dedupv1::test::INFO).Matches("Current event has already been processed.*").Repeatedly();

    LastEntryLogConsumer c;
    ASSERT_TRUE(system->log()->RegisterConsumer("c", &c));
    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 0, 16));
    ASSERT_TRUE(system->storage()->Flush(NO_EC));
    container_test_helper->LoadContainerDataIntoChunkIndex(system);

    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));
    BlockMapping m1(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillBlockMapping(&m1));
    m1.set_version(m1.version() + 1);
    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    BlockMapping orig2(1, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig2, NO_EC));
    BlockMapping m2(1, 64 * 1024);
    container_test_helper->FillBlockMapping(&m2);
    m2.set_version(m2.version() + 1);
    ASSERT_TRUE(block_index->StoreBlock(orig2, m2, NO_EC));

    BlockMapping m3(1, 64 * 1024);
    ASSERT_TRUE(m3.CopyFrom(orig2));
    m3.set_version(m3.version() + 2);
    ASSERT_TRUE(block_index->StoreBlock(m2, m3, NO_EC));

    INFO("Replay all");

    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());
    ASSERT_TRUE(gc->StartProcessing());
    sleep(8);
    ASSERT_TRUE(gc->StopProcessing());

    INFO("Re-replay the last event");
    EXPECT_TRUE(system->log()->UnregisterConsumer("c"));
    LogReplayContext replay_context(dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG, c.last_log_id);
    ASSERT_TRUE(system->log()->PublishEvent(replay_context,
            c.last_event_type, c.last_event_value));
    ASSERT_TRUE(system->log()->RegisterConsumer("c", &c));

    // here we trigger the gc
    INFO("Replay all");
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    ChunkMapping cm(container_test_helper->fingerprint(0));
    DEBUG("chunk mapping " << cm.DebugString());
    ASSERT_EQ(cm.fingerprint_size(), sizeof(uint64_t));
    lookup_result lr = system->chunk_index()->Lookup(&cm, false, NO_EC);
    EXPECT_EQ(lr, LOOKUP_FOUND);
    EXPECT_EQ(cm.usage_count(), 1);

    BlockMapping m4(0, 64 * 1024);
    ASSERT_TRUE(m4.CopyFrom(orig));
    m4.set_version(m4.version() + 2);
    ASSERT_TRUE(block_index->StoreBlock(m1, m4, NO_EC));

    INFO("Replay all");
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());
    gc->no_gc_candidates_during_last_try_ = false;
    ASSERT_TRUE(gc->StartProcessing());
    do {
        sleep(4);
    } while (!gc->no_gc_candidates_during_last_try_);
    ASSERT_TRUE(gc->StopProcessing());

    EXPECT_TRUE(system->log()->UnregisterConsumer("c"));

    ChunkMapping cm2(container_test_helper->fingerprint(0));
    lr = system->chunk_index()->Lookup(&cm2, false, NO_EC);
    EXPECT_EQ(LOOKUP_NOT_FOUND, lr) << "Fingerprint should not be in chunk index anymore";
}

TEST_P(UsageCountGarbageCollectorIntegrationTest, InCombatChunk) {
    byte buffer[64 * 1024];
    memset(buffer, 0, 64 * 1024);

    DedupVolume* volume = system->GetVolume(0);
    ASSERT_TRUE(volume);

    DEBUG("Write version 1");
    memset(buffer, 0x07, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    ASSERT_TRUE(system->storage()->Flush(NO_EC));
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    DEBUG("Write version 2");
    memset(buffer, 0, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    INFO("Replay all");
    ASSERT_TRUE(system->storage()->Flush(NO_EC));
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    DEBUG("Write version 3");
    memset(buffer, 0x07, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    uint64_t item_count = system->chunk_index()->GetPersistentCount();

    // here we trigger the gc
    ASSERT_TRUE(gc->StartProcessing());
    sleep(8);
    ASSERT_TRUE(gc->StopProcessing());

    // Check that there are not delete operations
    uint64_t item_count2 = system->chunk_index()->GetPersistentCount();
    ASSERT_EQ(item_count, item_count2);
}

/**
 * This unit tests checks if the recheck of the usage count of a gc candidate
 * is correctly done.
 */
TEST_P(UsageCountGarbageCollectorIntegrationTest, UCRecheck) {
    byte buffer[64 * 1024];
    memset(buffer, 0, 64 * 1024);

    DedupVolume* volume = system->GetVolume(0);
    ASSERT_TRUE(volume);

    DEBUG("Write version 1");
    memset(buffer, 0x07, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    DEBUG("Write version 2");
    memset(buffer, 0, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    DEBUG("Write version 3");
    memset(buffer, 0x07, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    Restart();

    INFO("Replay all");
    ASSERT_TRUE(system->storage()->Flush(NO_EC));
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    // here we trigger the gc
    ASSERT_TRUE(gc->StartProcessing());
    sleep(8);
    ASSERT_TRUE(gc->StopProcessing());

    volume = system->GetVolume(0);
    ASSERT_TRUE(volume);

    // Check that there are no wrong delete operations
    DEBUG("Read");
    memset(buffer, 0x0, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_READ, 0, 4 * 1024, buffer, NO_EC));
}

TEST_P(UsageCountGarbageCollectorIntegrationTest, InCombatChunkRestart) {
    byte buffer[64 * 1024];
    memset(buffer, 0, 64 * 1024);

    DedupVolume* volume = system->GetVolume(0);
    ASSERT_TRUE(volume);

    DEBUG("Write version 1");
    memset(buffer, 0x07, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    DEBUG("Write version 2");
    memset(buffer, 0, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    DEBUG("Write version 3");
    memset(buffer, 0x07, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    Restart();

    INFO("Replay parts of it");
    ASSERT_TRUE(system->storage()->Flush(NO_EC));
    ASSERT_TRUE(system->log()->Replay(dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL));
    ASSERT_TRUE(system->log()->Replay(dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL));
    ASSERT_TRUE(system->log()->Replay(dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL));
    ASSERT_TRUE(system->log()->Replay(dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL));
    ASSERT_TRUE(system->log()->Replay(dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL));
    ASSERT_TRUE(system->log()->Replay(dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL));

    // here we trigger the gc
    ASSERT_TRUE(gc->StartProcessing());
    sleep(8);
    ASSERT_TRUE(gc->StopProcessing());

    // Check that there are no wrong delete operations
    DEBUG("Read");

    volume = system->GetVolume(0);
    ASSERT_TRUE(volume);

    memset(buffer, 0x0, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_READ, 0, 4 * 1024, buffer, NO_EC));
}

TEST_P(UsageCountGarbageCollectorIntegrationTest, InCombatChunkRestartReplay) {
    byte buffer[64 * 1024];
    memset(buffer, 0, 64 * 1024);

    DedupVolume* volume = system->GetVolume(0);
    ASSERT_TRUE(volume);

    DEBUG("Write version 1");
    memset(buffer, 0x07, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    DEBUG("Write version 2");
    memset(buffer, 0, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    DEBUG("Write version 3");
    memset(buffer, 0x07, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    DEBUG("Write version 4");
    memset(buffer, 0, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 4 * 1024, buffer, NO_EC));

    Restart();

    INFO("Replay all");
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    // here we trigger the gc
    ASSERT_TRUE(gc->StartProcessing());
    sleep(8);
    ASSERT_TRUE(gc->StopProcessing());

    // Check that there are no wrong delete operations
    DEBUG("Read");

    volume = system->GetVolume(0);
    ASSERT_TRUE(volume);

    memset(buffer, 0x0, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_READ, 0, 4 * 1024, buffer, NO_EC));
}

/**
 * Given the following situation:
 * - A block mapping m_1 used a new chunk fp_1. The fp_1 is added to a new container c_1. m_1 is added to the volatile block store.
 * - The block mapping m_1 is overwritten and does not reference fp_1 anymore. I call this mapping m_1'. m_1' is committed at that time.
 * - c_1 is committed and the now commitable m_1 is also committed.
 */
TEST_P(UsageCountGarbageCollectorIntegrationTest, OutrunnedBlockMapping) {
    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 0, 1)); // only one fingerprint

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));
    BlockMapping m1(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m1, 0));

    ChunkMapping chunk_mapping(container_test_helper->fingerprint(0));
    chunk_mapping.set_data_address(container_test_helper->data_address(0));
    ASSERT_TRUE(system->chunk_index()->Put(chunk_mapping, NO_EC));

    m1.set_version(m1.version() + 1);
    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(m2.CopyFrom(orig));
    m2.set_version(m2.version() + 2);
    ASSERT_TRUE(block_index->StoreBlock(m1, m2, NO_EC));

    INFO("Replay all");
    ASSERT_TRUE(system->storage()->Flush(NO_EC));
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    BlockMapping check_mapping(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &check_mapping, NO_EC));
    // is it the same when we ignore the event log id
    ASSERT_GT(check_mapping.event_log_id(), 0);
    check_mapping.set_event_log_id(0);
    ASSERT_TRUE(m2.Equals(check_mapping)) << m2.DebugString() << std::endl << std::endl << check_mapping.DebugString();

    ChunkMapping check_chunk_mapping(container_test_helper->fingerprint(0));
    ASSERT_TRUE(system->chunk_index()->Lookup(&check_chunk_mapping, false, NO_EC));
    ASSERT_EQ(check_chunk_mapping.usage_count(), 0) << "The usage count should be zero";
    ASSERT_EQ(0, block_index->volatile_blocks()->GetContainerCount());
    ASSERT_EQ(0, block_index->volatile_blocks()->GetBlockCount());

    INFO("Restart");
    Restart();

    BlockMapping check_mapping2(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &check_mapping2, NO_EC));
    // is it the same when we ignore the event log id
    ASSERT_GT(check_mapping2.event_log_id(), 0);
    check_mapping2.set_event_log_id(0);
    ASSERT_TRUE(m2.Equals(check_mapping2)) << m2.DebugString() << std::endl << std::endl << check_mapping2.DebugString();
}

/**
 * Given the following situation:
 * - A block mapping m_1 used a new chunk fp_1. The fp_1 is added to a new container c_1. m_1 is added to the volatile block store.
 * - The block mapping m_1 is overwritten and does not reference fp_1 anymore. I call this mapping m_1'. m_1' is committed at that time.
 * - c_1 is committed and the now commitable m_1 is also committed.
 */
TEST_P(UsageCountGarbageCollectorIntegrationTest, OutrunnedBlockMappingChain) {

    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 1, 2)); // only two fingerprint
    ASSERT_TRUE(system->storage()->Flush(NO_EC));

    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 0, 1)); // only one fingerprint

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));
    BlockMapping m1(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m1, 0));

    ChunkMapping chunk_mapping(container_test_helper->fingerprint(0));
    chunk_mapping.set_data_address(container_test_helper->data_address(0));
    ASSERT_TRUE(system->chunk_index()->Put(chunk_mapping, NO_EC));

    m1.set_version(m1.version() + 1);
    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m2, 1));
    m2.set_version(2);
    ASSERT_TRUE(block_index->StoreBlock(m1, m2, NO_EC));

    BlockMapping m3(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m3, 2));
    m3.set_version(3);
    ASSERT_TRUE(block_index->StoreBlock(m2, m3, NO_EC));

    ASSERT_TRUE(system->storage()->Flush(NO_EC));
    ASSERT_EQ(0, block_index->volatile_blocks()->GetContainerCount());
    ASSERT_EQ(0, block_index->volatile_blocks()->GetBlockCount());

    INFO("Restart");
    Restart();
    // replay all
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    // check
    for (int i = 0; i < 2; i++) {
        ChunkMapping check_chunk_mapping(container_test_helper->fingerprint(i));
        ASSERT_TRUE(system->chunk_index()->Lookup(&check_chunk_mapping, false, NO_EC));
        ASSERT_EQ(check_chunk_mapping.usage_count(), 0) << "The usage count should be zero: " << check_chunk_mapping.DebugString();
    }
    ChunkMapping check_chunk_mapping(container_test_helper->fingerprint(2));
    ASSERT_TRUE(system->chunk_index()->Lookup(&check_chunk_mapping, false, NO_EC));
    ASSERT_EQ(check_chunk_mapping.usage_count(), 11) << "The usage count should be zero: " << check_chunk_mapping.DebugString();

    BlockMapping check_mapping(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &check_mapping, NO_EC));
    INFO("Result: " << check_mapping.DebugString());
    // is it the same when we ignore the event log id
    ASSERT_GT(check_mapping.event_log_id(), 0) << check_mapping.DebugString();
    check_mapping.set_event_log_id(0);
    ASSERT_TRUE(m3.Equals(check_mapping)) << m3.DebugString() << std::endl << std::endl << check_mapping.DebugString();
}

/**
 * Given the following situation:
 * - A block mapping m_1 used a new chunk fp_1. The fp_1 is added to a new container c_1. m_1 is added to the volatile block store.
 * - The block mapping m_1 is overwritten and does not reference fp_1 anymore. I call this mapping m_1'. m_1' is committed at that time.
 * - c_1 is committed and the now comittable m_1 is also committed.
 */
TEST_P(UsageCountGarbageCollectorIntegrationTest, OutrunnedBlockMappingChainCrash) {
    EXPECT_LOGGING(dedupv1::test::WARN).Logger("ContainerStorageWriteCache").Repeatedly();
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Missing container for import").Times(0, 2);
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Mapping has open containers that cannot be recovered").Repeatedly();
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Found no entry for chunk mapping").Repeatedly();

    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 1, 2)); // only two fingerprint
    ASSERT_TRUE(system->storage()->Flush(NO_EC));

    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 0, 1)); // only one fingerprint

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));
    BlockMapping m1(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m1, 0));

    ChunkMapping chunk_mapping(container_test_helper->fingerprint(0));
    chunk_mapping.set_data_address(container_test_helper->data_address(0));
    ASSERT_TRUE(system->chunk_index()->Put(chunk_mapping, NO_EC));

    m1.set_version(m1.version() + 1);
    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m2, 1));
    m2.set_version(2);
    ASSERT_TRUE(block_index->StoreBlock(m1, m2, NO_EC));

    BlockMapping m3(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m3, 2));
    m3.set_version(3);
    ASSERT_TRUE(block_index->StoreBlock(m2, m3, NO_EC));

    INFO("Crash and Restart");
    ASSERT_NO_FATAL_FAILURE(Crash());
    ASSERT_TRUE(system);
    ASSERT_TRUE(gc);
    ASSERT_TRUE(block_index);
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    for (int i = 0; i <= 2; i++) {
        ChunkMapping check_chunk_mapping(container_test_helper->fingerprint(i));
        ASSERT_TRUE(system->chunk_index()->Lookup(&check_chunk_mapping, false, NO_EC));
        ASSERT_EQ(check_chunk_mapping.usage_count(), 0) << "The usage count should be zero: " << check_chunk_mapping.DebugString();
    }

    BlockMapping check_mapping(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &check_mapping, NO_EC));
    check_mapping.set_event_log_id(0);

    BlockMapping final_mapping(0, 64 * 1024);
    ASSERT_TRUE(final_mapping.CopyFrom(orig));
    final_mapping.set_version(m3.version());

    ASSERT_TRUE(check_mapping.Equals(final_mapping)) << check_mapping.DebugString() << " => " << final_mapping.DebugString();
}

TEST_P(UsageCountGarbageCollectorIntegrationTest, OutrunnedBlockMappingChainCrashPartialFlush) {
    EXPECT_LOGGING(dedupv1::test::WARN).Logger("ContainerStorageWriteCache").Repeatedly();
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Missing container for import").Times(0, 2);
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Mapping has open containers that cannot be recovered").Repeatedly();
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Found no entry for chunk mapping").Repeatedly();

    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 1, 2)); // only two fingerprint
    ASSERT_TRUE(system->storage()->Flush(NO_EC));

    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 0, 1)); // only one fingerprint

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));
    BlockMapping m1(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m1, 1));

    ChunkMapping chunk_mapping(container_test_helper->fingerprint(0));
    chunk_mapping.set_data_address(container_test_helper->data_address(0));
    ASSERT_TRUE(system->chunk_index()->Put(chunk_mapping, NO_EC));

    m1.set_version(m1.version() + 1);
    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m2, 0));
    m2.set_version(2);
    ASSERT_TRUE(block_index->StoreBlock(m1, m2, NO_EC));

    BlockMapping m3(0, 64 * 1024);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m3, 2));
    m3.set_version(3);
    ASSERT_TRUE(block_index->StoreBlock(m2, m3, NO_EC));

    INFO("Crash and Restart");
    ASSERT_NO_FATAL_FAILURE(Crash());
    ASSERT_TRUE(system);
    ASSERT_TRUE(gc);
    ASSERT_TRUE(block_index);
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    ChunkMapping check_chunk_mapping1(container_test_helper->fingerprint(0));
    ASSERT_TRUE(system->chunk_index()->Lookup(&check_chunk_mapping1, false, NO_EC));
    ASSERT_EQ(check_chunk_mapping1.usage_count(), 0) << "The usage count should be zero: " << check_chunk_mapping1.DebugString();

    ChunkMapping check_chunk_mapping2(container_test_helper->fingerprint(1));
    ASSERT_TRUE(system->chunk_index()->Lookup(&check_chunk_mapping2, false, NO_EC));
    ASSERT_EQ(check_chunk_mapping2.usage_count(), 11) << "The usage count should be zero: " << check_chunk_mapping2.DebugString();

    ChunkMapping check_chunk_mapping3(container_test_helper->fingerprint(2));
    ASSERT_TRUE(system->chunk_index()->Lookup(&check_chunk_mapping3, false, NO_EC));
    ASSERT_EQ(check_chunk_mapping3.usage_count(), 0) << "The usage count should be zero: " << check_chunk_mapping3.DebugString();

    BlockMapping check_mapping(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &check_mapping, NO_EC));
    check_mapping.set_event_log_id(0);

    BlockMapping final_mapping(0, 64 * 1024);
    ASSERT_TRUE(final_mapping.CopyFrom(m1));
    final_mapping.set_version(m3.version());

    ASSERT_TRUE(check_mapping.Equals(final_mapping)) << check_mapping.DebugString() << " => " << final_mapping.DebugString();
}

INSTANTIATE_TEST_CASE_P(UsageCountGarbageCollector,
    UsageCountGarbageCollectorIntegrationTest,
    ::testing::Values(
        "data/dedupv1_test.conf",
        "data/dedupv1_leveldb_test.conf"));

}
}
