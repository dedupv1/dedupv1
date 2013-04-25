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

#include "block_mapping_test.h"

#include "dedupv1.pb.h"

#include <core/dedup.h>
#include <base/locks.h>

#include <core/log_consumer.h>
#include <core/filter.h>
#include <core/chunk_mapping.h>
#include <core/block_index.h>
#include <base/disk_hash_index.h>
#include <base/logging.h>
#include <core/log.h>
#include <core/dedup_system.h>
#include <core/chunk_store.h>
#include <core/container_storage.h>
#include <core/chunk_index.h>
#include <core/block_mapping_pair.h>

#include "block_mapping_test.h"
#include "dedup_system_test.h"
#include "container_test_helper.h"
#include <test_util/log_assert.h>

LOGGER("BlockIndexTest");

using std::string;
using dedupv1::base::DELETE_NOT_FOUND;
using dedupv1::base::DELETE_OK;
using dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG;
using dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START;
using dedupv1::log::LogConsumer;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN;
using dedupv1::log::EVENT_TYPE_CONTAINER_COMMITED;
using dedupv1::chunkstore::ContainerStorage;
using dedupv1::base::PersistentIndex;
using dedupv1::blockindex::BlockIndex;

namespace dedupv1 {
namespace blockindex {

/**
 * Tests mainly focused on the block index
 * However, these are not classical unit test as also the correct integration with the other component is validated.
 */
class BlockIndexTest : public testing::TestWithParam<const char*> {
protected:
    USE_LOGGING_EXPECTATION();

    DedupSystem* system;
    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;

    ContainerTestHelper* container_test_helper;

    virtual void SetUp() {
        system = NULL;
        container_test_helper = NULL;

        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());

        container_test_helper = new ContainerTestHelper(64 * 1024, 16);
        ASSERT_TRUE(container_test_helper);
        ASSERT_TRUE(container_test_helper->SetUp());
    }

    virtual void TearDown() {
        if (system) {
            ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
            delete system;
            system = NULL;
        }
        delete container_test_helper;
    }
};

INSTANTIATE_TEST_CASE_P(BlockIndex,
    BlockIndexTest,
    ::testing::Values(
        "data/dedupv1_test.conf", // 0
        "data/dedupv1_sqlite_test.conf" // 2
        ));

TEST_P(BlockIndexTest, Init) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, false);
    ASSERT_TRUE(system);
    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);
}

TEST_P(BlockIndexTest, DoubleStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);
    BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);
    ASSERT_FALSE(block_index->Start(StartContext(), system)) << "Second start should fail";
}

TEST_P(BlockIndexTest, ConfigurationAfterStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);
    BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);
    ASSERT_FALSE(block_index->SetOption("import-thread-count", "4")) << "Set options after start should fail";
}

TEST_P(BlockIndexTest, StartWithoutSystem) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, false);
    ASSERT_TRUE(system);
    BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);
    ASSERT_FALSE(block_index->Start(StartContext(), NULL)) << "Start without system should fail";
}

TEST_P(BlockIndexTest, IsSoftLimitReachedBeforeStart) {
    BlockIndex* block_index = new BlockIndex();
    ASSERT_TRUE(block_index);
    EXPECT_FALSE(block_index->IsSoftLimitReached());
    delete block_index;
}

TEST_P(BlockIndexTest, IsHardLimitReachedBeforeStart) {
    BlockIndex* block_index = new BlockIndex();
    ASSERT_TRUE(block_index);
    EXPECT_FALSE(block_index->IsHardLimitReached());
    delete block_index;
}

TEST_P(BlockIndexTest, GetActiveBlockCountBeforeStart) {
    BlockIndex* block_index = new BlockIndex();
    ASSERT_TRUE(block_index);
    EXPECT_EQ(0, block_index->GetActiveBlockCount());
    delete block_index;
}

TEST_P(BlockIndexTest, Start) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);
    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);
}

TEST_P(BlockIndexTest, IllegalSoftLimitHardLimit) {
    EXPECT_LOGGING(dedupv1::test::WARN).Once();

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, false);
    ASSERT_TRUE(system);
    BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);
    ASSERT_TRUE(block_index->SetOption("max-auxiliary-size", "32K"));
    ASSERT_TRUE(block_index->SetOption("auxiliary-size-hard-limit", "32K"));

    ASSERT_TRUE(system->Start(StartContext(), &info_store, &tp)) << "Start should fail";
}

TEST_P(BlockIndexTest, ReadWrite) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    DEBUG("Write data");
    container_test_helper->WriteDefaultData(system, 0, 16);
    ASSERT_TRUE(system->storage()->Flush(NO_EC));

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping m1(0, 64 * 1024);
    container_test_helper->FillBlockMapping(&m1);
    ASSERT_TRUE(m1.Check());

    DEBUG("Store block");
    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(m2.CopyFrom(m1));
    m2.set_version(m2.version() + 1);
    ASSERT_TRUE(block_index->StoreBlock(m1, m2, NO_EC));

    sleep(2);

    DEBUG("Read block");
    BlockMapping m3(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m3, NO_EC));

    // is it the same when we ignore the event log id
    ASSERT_GT(m3.event_log_id(), 0);
    m3.set_event_log_id(0);
    ASSERT_TRUE(m2.Equals(m3)) << "m2 " << m2.DebugString() << ", m3 " << m3.DebugString();
}

TEST_P(BlockIndexTest, ReadWriteAfterClose) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 0, 16));

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    ASSERT_TRUE(container_test_helper->FillBlockMapping(&m1));

    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
    delete system;
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true);
    ASSERT_TRUE(system);

    block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m2, NO_EC));

    // is it the same when we ignore the event log id
    ASSERT_GT(m2.event_log_id(), 0);
    m2.set_event_log_id(0);
    ASSERT_TRUE(m1.Equals(m2)) << m1.DebugString() << std::endl << std::endl << m2.DebugString();
}

TEST_P(BlockIndexTest, ReadWriteWithFullyCommittedData) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    container_test_helper->WriteDefaultData(system, 0, 16);

    ASSERT_TRUE(system->log()->UnregisterConsumer("chunk-index"));

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    container_test_helper->FillBlockMapping(&m1);

    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m2, NO_EC));
    // is it the same when we ignore the event log id
    m2.set_event_log_id(0);
    ASSERT_TRUE(m1.Equals(m2)) << m1.DebugString() << std::endl << std::endl << m2.DebugString();

    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
    delete system;
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true);
    ASSERT_TRUE(system);

    block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping m3(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m3, NO_EC));
    // is it the same when we ignore the event log id
    ASSERT_GT(m3.event_log_id(), 0);
    m3.set_event_log_id(0);
    ASSERT_TRUE(m1.Equals(m3)) << m1.DebugString() << std::endl << std::endl << m3.DebugString();
}

/**
 * This test ensures a correct behavor when all containers have correctly been written, but the system crashes
 * directly after the commit. Especially this tests the situation where the COMMIT event is written, but the meta data index
 * is not updated
 */
TEST_P(BlockIndexTest, ReadWriteWithCrashAfterCommit) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    container_test_helper->WriteDefaultData(system, 0, 16);

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    container_test_helper->FillBlockMapping(&m1);

    DEBUG("Write block");
    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    DEBUG("Flush storage");
    ASSERT_TRUE(system->chunk_store()->Flush(NO_EC));

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m2, NO_EC));
    // is it the same when we ignore the event log id
    m2.set_event_log_id(0);
    ASSERT_TRUE(m1.Equals(m2)) << m1.DebugString() << std::endl << std::endl << m2.DebugString();

    // delete the metadata of the last container
    uint64_t container_id = 2;
    DEBUG("Delete metadata of container id " << container_id);
    ContainerStorage* container_storage = dynamic_cast<ContainerStorage*>(system->storage());
    ASSERT_TRUE(container_storage);
    PersistentIndex* container_meta_data_index = container_storage->meta_data_index();
    ASSERT_TRUE(container_meta_data_index);
    ASSERT_TRUE(container_meta_data_index->Delete(&container_id, sizeof(container_id)));

    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
    delete system;
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true);
    ASSERT_TRUE(system);

    block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping m3(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m3, NO_EC));
    // is it the same when we ignore the event log id
    ASSERT_GT(m3.event_log_id(), 0);
    m3.set_event_log_id(0);
    ASSERT_TRUE(m1.Equals(m3)) << m1.DebugString() << std::endl << std::endl << m3.DebugString();
}

/**
 * This test ensures a correct behavior when all containers have correctly been written in a startup before.
 */
TEST_P(BlockIndexTest, ReadWriteWithPrecommittedData) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    DEBUG("Write data");
    container_test_helper->WriteDefaultData(system, 0, 16);

    DEBUG("Restart");
    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
    delete system;

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true, true);
    ASSERT_TRUE(system);

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    container_test_helper->FillBlockMapping(&m1);

    DEBUG("Write block");
    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m2, NO_EC));
    // is it the same when we ignore the event log id
    m2.set_event_log_id(0);
    ASSERT_TRUE(m1.Equals(m2)) << m1.DebugString() << std::endl << std::endl << m2.DebugString();

    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
    delete system;

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true, false, true);
    ASSERT_TRUE(system);

    block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping m3(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m3, NO_EC));
    // is it the same when we ignore the event log id
    ASSERT_GT(m3.event_log_id(), 0);
    m3.set_event_log_id(0);
    ASSERT_TRUE(m1.Equals(m3)) << m1.DebugString() << std::endl << std::endl << m3.DebugString();
}

/**
 * This test ensures a correct behavior when the data on which a block mapping relies
 * fails to be written.
 */
TEST_P(BlockIndexTest, ReadWriteWithFailedCommittedData) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Failed to commit container").Times(0, 1);
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Missing container for import").Times(0, 1);

    string config = GetParam();
    config += ";storage.container-size=4M";
    system = DedupSystemTest::CreateDefaultSystem(config, &info_store, &tp);
    ASSERT_TRUE(system);

    ContainerStorage* storage = dynamic_cast<ContainerStorage*>(system->storage());
    ASSERT_TRUE(storage);

    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 0, 8));
    ASSERT_TRUE(storage->Flush(NO_EC));
    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 8, 8));

    ASSERT_TRUE(system->log()->UnregisterConsumer("chunk-index"));
    ASSERT_TRUE(system->log()->UnregisterConsumer("gc"));

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m1, 9));

    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    BlockMapping m3(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m3, NO_EC));
    m3.set_event_log_id(0);
    ASSERT_TRUE(m1.Equals(m3)) << m1.DebugString() << std::endl << std::endl << m3.DebugString();

    ASSERT_TRUE(storage->FailWriteCacheContainer(container_test_helper->data_address(9)));

    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
    delete system;
    system = NULL;
    storage = NULL;

    system = DedupSystemTest::CreateDefaultSystem(config, &info_store, &tp, true, true);
    ASSERT_TRUE(system);

    block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping m4(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m4, NO_EC));
    m4.set_event_log_id(0);

    BlockMapping final_mapping(0, 64 * 1024);
    ASSERT_TRUE(final_mapping.CopyFrom(orig));
    final_mapping.set_version(m1.version());
    ASSERT_TRUE(m4.Equals(final_mapping)) << final_mapping.DebugString() << std::endl << std::endl << m4.DebugString();
}

/**
 * This test ensures a correct behavior when the data on which a block mapping relies
 * fails to be written. Similar to ReadWriteWithFailedCommittedData2, but
 * with a newer version that is not failing.
 */
TEST_P(BlockIndexTest, ReadWriteWithFailedCommittedData2) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Failed to commit container").Times(0, 1);
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Missing container for import").Times(0, 1);

    string config = GetParam();
    config += ";storage.container-size=4M";
    system = DedupSystemTest::CreateDefaultSystem(config, &info_store, &tp);
    ASSERT_TRUE(system);

    ContainerStorage* storage = dynamic_cast<ContainerStorage*>(system->storage());
    ASSERT_TRUE(storage);

    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 0, 8));
    ASSERT_TRUE(storage->Flush(NO_EC));
    ASSERT_TRUE(container_test_helper->WriteDefaultData(system, 8, 8));

    ASSERT_TRUE(system->log()->UnregisterConsumer("chunk-index"));
    ASSERT_TRUE(system->log()->UnregisterConsumer("gc"));

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m1, 9));

    BlockMapping m2(0, 64 * 1024);
    m2.set_version(m2.version() + 2);
    ASSERT_TRUE(container_test_helper->FillSameBlockMapping(&m2, 1));

    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));
    ASSERT_TRUE(block_index->StoreBlock(m1, m2, NO_EC));

    BlockMapping m3(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m3, NO_EC));
    m3.set_event_log_id(0);
    ASSERT_TRUE(m2.Equals(m3)) << m2.DebugString() << std::endl << std::endl << m3.DebugString();

    ASSERT_TRUE(storage->FailWriteCacheContainer(container_test_helper->data_address(9)));

    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
    delete system;
    system = NULL;
    storage = NULL;

    system = DedupSystemTest::CreateDefaultSystem(config, &info_store, &tp, true, true);
    ASSERT_TRUE(system);

    block_index = system->block_index();
    ASSERT_TRUE(block_index);

    /*
     * Here we changed the expected behavior at one point.
     * If a new version of a block (v'') can be written completly, but an old version of the block
     * fails (v'), the old bahvior was to fall back on the original version (v). The new
     * behavior is to skip v' and mark v'' as valid.
     */
    BlockMapping m4(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m4, NO_EC));
    m4.set_event_log_id(0);
    ASSERT_TRUE(m4.Equals(m2)) << m2.DebugString() << std::endl << std::endl << m4.DebugString();
}

TEST_P(BlockIndexTest, ReadWriteAfterCloseWithoutCommit) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    BlockMappingTest::FillTestBlockMapping(&m1, 1);

    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    DedupSystem* system_backup = system;
    system_backup->ClearData();
    system = NULL;

    INFO("Opening system after 'crash'");
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true, true, true);
    ASSERT_TRUE(system);

    block_index = system->block_index();
    ASSERT_TRUE(block_index);

    DEBUG("Replaying the log");

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo( NULL, &m2, NO_EC));

    DEBUG("Block Mapping: Orig: " << orig.DebugString());
    DEBUG("Block Mapping: Change: " << m1.DebugString());
    DEBUG("Block Mapping: After Crash: " << m2.DebugString());

    m2.set_event_log_id(0);

    BlockMapping final_mapping(0, 64 * 1024);
    ASSERT_TRUE(final_mapping.CopyFrom(orig));
    final_mapping.set_version(m1.version());
    ASSERT_TRUE(m2.Equals(final_mapping)) << m2.DebugString() << std::endl << std::endl << final_mapping.DebugString();

    delete system_backup;
    system_backup = NULL;
}

TEST_P(BlockIndexTest, ReadWriteAfterCloseWithCommit) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    container_test_helper->WriteDefaultData(system, 0, 16);

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    ASSERT_TRUE(system->log()->UnregisterConsumer("chunk-index"));
    ASSERT_TRUE(system->log()->UnregisterConsumer("container-storage"));
    ASSERT_TRUE(system->log()->UnregisterConsumer("gc"));

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    container_test_helper->FillBlockMapping(&m1);

    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));
    ASSERT_TRUE(system->storage()->Flush(NO_EC));

    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));

    delete system;
    system = NULL;

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true);
    ASSERT_TRUE(system);

    block_index = system->block_index();
    ASSERT_TRUE(block_index);

    // Removed because of interferences with test
    ASSERT_TRUE(system->log()->UnregisterConsumer("chunk-index"));
    ASSERT_TRUE(system->log()->UnregisterConsumer("container-storage"));
    ASSERT_TRUE(system->log()->UnregisterConsumer("gc"));

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m2, NO_EC));
    // is it the same when we ignore the event log id
    ASSERT_GT(m2.event_log_id(), 0);
    m2.set_event_log_id(0);
    ASSERT_TRUE(m1.Equals(m2)) << m1.DebugString() << std::endl << std::endl << m2.DebugString();
}

class BlockIndexTestLogConsumer : public LogConsumer {
public:
    int count;
    BlockMappingPair mapping_pair;
    BlockMapping modified;

    BlockIndexTestLogConsumer()
        : mapping_pair(64 * 1024), modified(64 * 1024) {
        count = 0;
    }

    virtual bool LogReplay(dedupv1::log::event_type event_type,
                           const LogEventData& event_value,
                           const dedupv1::log::LogReplayContext& context) {
        if (event_type == EVENT_TYPE_BLOCK_MAPPING_WRITTEN) {
            this->count++;
            BlockMappingWrittenEventData event_data = event_value.block_mapping_written_event();
            CHECK(event_data.has_mapping_pair(), "Event data has no block mapping");

            CHECK(mapping_pair.CopyFrom(event_data.mapping_pair()), "Failed to copy mapping pair");
            modified = mapping_pair.GetModifiedBlockMapping(context.log_id());
        }
        DEBUG("Event " << dedupv1::log::Log::GetEventTypeName(event_type) << ", replay " << context.replay_mode());
        return true;
    }
};

TEST_P(BlockIndexTest, BlockUpdateLogging) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    container_test_helper->WriteDefaultData(system, 0, 16);
    container_test_helper->LoadContainerDataIntoChunkIndex(system);

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockIndexTestLogConsumer lc;
    ASSERT_TRUE(system->log()->RegisterConsumer("bi_test", &lc));

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(1);
    container_test_helper->FillBlockMapping(&m1);

    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    INFO("Flush");
    ASSERT_TRUE(system->chunk_store()->Flush(NO_EC));

    INFO("Replay");
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode());

    ASSERT_TRUE(system->log()->UnregisterConsumer("bi_test"));

    ASSERT_EQ(2, lc.count) << "Wrong number of events logged and replayed";

    BlockMapping& logged_mapping(lc.modified);
    logged_mapping.set_event_log_id(0); // ignore the event log id
    ASSERT_TRUE(logged_mapping.Equals(m1)) << "logged mapping " << logged_mapping.DebugString() <<
    ", original mapping " << m1.DebugString();
}

TEST_P(BlockIndexTest, PartiallyWrittenBlock) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    container_test_helper->WriteDefaultData(system, 0, 16);

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    BlockMapping m1(BlockMapping::ILLEGAL_BLOCK_ID, orig.block_size());
    ASSERT_TRUE(m1.CopyFrom(orig));
    m1.set_version(m1.version() + 1);
    ASSERT_TRUE(container_test_helper->Append(&m1, 0, 0, 4761));
    ASSERT_TRUE(container_test_helper->Append(&m1, 4761, 1, 12334));
    // here we write only some bytes of the block

    DEBUG(m1.DebugString());

    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
    delete system;
    system = NULL;
    block_index = NULL;

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true, true);
    ASSERT_TRUE(system);

    block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m2, NO_EC));
    // is it the same when we ignore the event log id
    ASSERT_GT(m2.event_log_id(), 0);
    m2.set_event_log_id(0);
    ASSERT_TRUE(m1.Equals(m2)) << "should be " << m1.DebugString() << ", is " << m2.DebugString();
}

TEST_P(BlockIndexTest, PartiallyWrittenBlockDirtyReplay) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Still").Times(0, 3);

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    container_test_helper->WriteDefaultData(system, 0, 16);

    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    BlockMapping m1(BlockMapping::ILLEGAL_BLOCK_ID, orig.block_size());
    ASSERT_TRUE(m1.CopyFrom(orig));
    ASSERT_TRUE(container_test_helper->Append(&m1, 0, 0, 4761));
    ASSERT_TRUE(container_test_helper->Append(&m1, 4761, 1, 12334));
    m1.set_version(m1.version() + 1);
    // here we write only some bytes of the block

    DEBUG(m1.DebugString());

    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    // only fast shutdown mode
    delete system;
    system = NULL;
    block_index = NULL;

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true, true);
    ASSERT_TRUE(system);

    block_index = system->block_index();
    ASSERT_TRUE(block_index);

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m2, NO_EC));
    // is it the same when we ignore the event log id
    ASSERT_GT(m2.event_log_id(), 0);
    m2.set_event_log_id(0);
    ASSERT_TRUE(m1.Equals(m2)) << "should be " << m1.DebugString() << ", is " << m2.DebugString();
}

TEST_P(BlockIndexTest, DeleteWithoutData) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    container_test_helper->WriteDefaultData(system, 0, 16);

    ASSERT_TRUE(system->log()->UnregisterConsumer("chunk-index"));

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);

    ASSERT_EQ(block_index->DeleteBlockInfo(0, NO_EC), DELETE_NOT_FOUND);
}

TEST_P(BlockIndexTest, DeleteWithData) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    container_test_helper->WriteDefaultData(system, 0, 16);
    ASSERT_TRUE(system->chunk_store()->Flush(NO_EC));

    ASSERT_TRUE(system->log()->UnregisterConsumer("chunk-index"));

    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    ASSERT_TRUE(block_index);
    BlockMapping orig(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &orig, NO_EC));

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    container_test_helper->FillBlockMapping(&m1);

    // write a block mapping (it will be in the auxiliary block store)
    ASSERT_TRUE(block_index->StoreBlock(orig, m1, NO_EC));

    // delete the block mapping
    ASSERT_EQ(block_index->DeleteBlockInfo(0, NO_EC), DELETE_OK);

    BlockMapping m2(0, 64 * 1024);
    ASSERT_TRUE(block_index->ReadBlockInfo(NULL, &m2, NO_EC));
    ASSERT_TRUE(orig.Equals(m2)) << orig.DebugString() << std::endl << std::endl << m2.DebugString();
}

}
}
