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
#include <core/block_mapping_pair.h>
#include <base/memory.h>
#include <core/garbage_collector.h>
#include <core/usage_count_garbage_collector.h>

#include "block_mapping_test.h"
#include <test_util/log_assert.h>

#include <test/log_mock.h>
#include <test/chunk_index_mock.h>
#include <test/block_index_mock.h>
#include <test/dedup_system_mock.h>
#include <test/container_storage_mock.h>
#include <test/storage_mock.h>
#include <base/threadpool.h>

using std::map;
using std::string;
using std::list;
using std::multimap;
using std::make_pair;
using std::pair;
using testing::Return;
using testing::_;
using dedupv1::base::make_bytestring;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingPair;
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
using dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG;
using dedupv1::log::event_type;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_DELETED;
using dedupv1::base::Threadpool;
using dedupv1::base::strutil::ToString;

LOGGER("GarbageCollectorTest");

namespace dedupv1 {
namespace gc {

class UsageCountGarbageCollectorTest : public testing::TestWithParam<int> {
protected:
    USE_LOGGING_EXPECTATION();

    MockDedupSystem system;
    MockLog log;
    ChunkIndex* chunk_index;
    MockBlockIndex block_index;
    UsageCountGarbageCollector* gc;
    MockContainerStorage storage;
    MockStorageSession storage_session;
    MemoryInfoStore info_store;
    IdleDetector idle_detector;
    Threadpool tp;

    // It is NULL, I know it
    ContentStorage* content_storage;

    virtual void SetUp() {
        content_storage = NULL;
        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());

        EXPECT_CALL(system, log()).WillRepeatedly(Return(&log));
        EXPECT_CALL(system, block_index()).WillRepeatedly(Return(&block_index));
        EXPECT_CALL(system, storage()).WillRepeatedly(Return(&storage));
        EXPECT_CALL(system, info_store()).WillRepeatedly(Return(&info_store));
        EXPECT_CALL(system, idle_detector()).WillRepeatedly(Return(&idle_detector));
        EXPECT_CALL(system, block_size()).WillRepeatedly(Return(64 * 1024));

        EXPECT_CALL(system, content_storage()).WillRepeatedly(Return(content_storage));
        EXPECT_CALL(storage, CreateSession()).WillRepeatedly(Return(&storage_session));
        EXPECT_CALL(storage, IsCommitted(_)).WillRepeatedly(Return(dedupv1::chunkstore::STORAGE_ADDRESS_COMMITED));
        EXPECT_CALL(storage_session, Close()).WillRepeatedly(Return(true));
        EXPECT_CALL(log, RegisterConsumer("gc",_)).WillRepeatedly(Return(true));
        EXPECT_CALL(log, UnregisterConsumer("gc")).WillRepeatedly(Return(true));
        EXPECT_CALL(log, RegisterConsumer("chunk-index",_)).WillRepeatedly(Return(true));
        EXPECT_CALL(log, UnregisterConsumer("chunk-index")).WillRepeatedly(Return(true));
        system.set_threadpool(&tp);

        chunk_index = new ChunkIndex();
        ASSERT_TRUE(chunk_index->Init());
        ASSERT_TRUE(chunk_index->SetOption("persistent","static-disk-hash"));
        ASSERT_TRUE(chunk_index->SetOption("persistent.page-size","4K"));
        ASSERT_TRUE(chunk_index->SetOption("persistent.size","4M"));
        ASSERT_TRUE(chunk_index->SetOption("persistent.filename","work/chunk-index"));
        ASSERT_TRUE(chunk_index->SetOption("persistent.write-cache","true"));
        ASSERT_TRUE(chunk_index->SetOption("persistent.write-cache.bucket-count","8K"));
        ASSERT_TRUE(chunk_index->SetOption("persistent.write-cache.max-page-count","8K"));
        ASSERT_TRUE(chunk_index->Start(StartContext(), &system));

        EXPECT_CALL(system, chunk_index()).WillRepeatedly(Return(chunk_index));

        gc = new UsageCountGarbageCollector();
        ASSERT_TRUE(gc);
    }

    virtual void TearDown() {
        if (gc) {
            ASSERT_TRUE(gc->Close());
        }
        if (chunk_index) {
            ASSERT_TRUE(chunk_index->Close());
        }
    }

    void SetDefaultOptions(GarbageCollector* gc) {
        ASSERT_TRUE(gc->SetOption("type", "sqlite-disk-btree"));
        if (GetParam() == 1) {
            ASSERT_TRUE(gc->SetOption("filename", "work/gc_candidte_info"));
        } else {
            for (int i = 0; i < GetParam(); i++) {
                ASSERT_TRUE(gc->SetOption("filename", "work/gc_candidte_info" + ToString(i + 1)));
            }
        }
        ASSERT_TRUE(gc->SetOption("max-item-count", "4M"));
    }

    static void ReplayWrittenLogEntry(UsageCountGarbageCollector * gc,
                                      const LogReplayContext& replay_context,
                                      const BlockMapping& previous_block_mapping,
                                      const BlockMapping& updated_block_mapping) {
        BlockMappingWrittenEventData event_data;

        BlockMappingPair mapping_pair(updated_block_mapping.block_size());
        ASSERT_TRUE(mapping_pair.CopyFrom(previous_block_mapping, updated_block_mapping));
        ASSERT_TRUE(mapping_pair.SerializeTo(event_data.mutable_mapping_pair()));

        LogEventData event_value;
        event_value.mutable_block_mapping_written_event()->MergeFrom(event_data);

        ASSERT_TRUE(gc->LogReplay(EVENT_TYPE_BLOCK_MAPPING_WRITTEN, event_value, replay_context));
    }

    static void ReplayDeletedLogEntry(UsageCountGarbageCollector* gc,
                                      const LogReplayContext& replay_context,
                                      const BlockMapping& previous_block_mapping) {
        BlockMappingDeletedEventData event_data;
        ASSERT_TRUE(previous_block_mapping.SerializeTo(event_data.mutable_original_block_mapping(), true, false));

        LogEventData event_value;
        event_value.mutable_block_mapping_deleted_event()->MergeFrom(event_data);

        ASSERT_TRUE(gc->LogReplay(EVENT_TYPE_BLOCK_MAPPING_DELETED, event_value, replay_context));
    }

    static void PerformDiff(UsageCountGarbageCollector* gc,
                            const BlockMapping& original_block_mapping,
                            const BlockMapping& modified_block_mapping,
                            map<bytestring, pair<int, uint64_t> >* diff) {
        ASSERT_TRUE(gc->Diff(original_block_mapping, modified_block_mapping, diff));
    }
};

INSTANTIATE_TEST_CASE_P(UsageCountGarbageCollector,
    UsageCountGarbageCollectorTest,
    ::testing::Values(1, 2, 4));

TEST_P(UsageCountGarbageCollectorTest, Init) {
    // do nothing
}

TEST_P(UsageCountGarbageCollectorTest, StartWithoutConfig) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_FALSE(gc->Start(StartContext(), &system));
}

TEST_P(UsageCountGarbageCollectorTest, Start) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));
}

TEST_P(UsageCountGarbageCollectorTest, StartIdleStop) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));
    ASSERT_TRUE(gc->Run());
    sleep(2);

    DEBUG("Idle start");

    gc->IdleStart();

    DEBUG("Sleep");
    sleep(11);

    DEBUG("Stop");
    ASSERT_TRUE(gc->Stop(StopContext::FastStopContext()));
}

TEST_P(UsageCountGarbageCollectorTest, DoubleStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));
    ASSERT_FALSE(gc->Start(StartContext(), &system)) << "The second start should fail";
}

TEST_P(UsageCountGarbageCollectorTest, StopWithoutRun) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));
    ASSERT_TRUE(gc->Stop(StopContext::FastStopContext()));
}

TEST_P(UsageCountGarbageCollectorTest, StopWithoutStart) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Stop(StopContext::FastStopContext()));
}

TEST_P(UsageCountGarbageCollectorTest, RunAndStop) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));
    ASSERT_TRUE(gc->Run());
    ASSERT_TRUE(gc->Stop(StopContext::FastStopContext()));
}

TEST_P(UsageCountGarbageCollectorTest, RunAndFastStop) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));
    ASSERT_TRUE(gc->Run());
    ASSERT_TRUE(gc->Stop(dedupv1::StopContext(dedupv1::StopContext::FAST)));
}

TEST_P(UsageCountGarbageCollectorTest, RunWithoutStop) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));
    ASSERT_TRUE(gc->Run());
}

TEST_P(UsageCountGarbageCollectorTest, RunWithoutStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_FALSE(gc->Run());
}

TEST_P(UsageCountGarbageCollectorTest, DifferenceSimple) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));

    BlockMapping m1(0, 64 * 1024);
    BlockMappingTest::FillTestBlockMapping(&m1, 0);

    BlockMapping m2(64 * 1024);
    ASSERT_TRUE(m2.CopyFrom(m1));
    size_t offset = 1234;
    size_t size = 10244;
    BlockMappingTest::Append(&m2, offset, 20, size, 10);

    map<bytestring, pair<int, uint64_t> > diff;
    PerformDiff(gc, m1, m2, &diff);
    ASSERT_EQ(diff.size(), 1U);
    ASSERT_EQ(diff[BlockMappingTest::FingerprintString(20)].first, 1);
}

TEST_P(UsageCountGarbageCollectorTest, DifferenceDoubleFP) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));

    BlockMapping m1(0, 64 * 1024);
    BlockMappingTest::FillTestBlockMapping(&m1, 0);

    BlockMapping m2(64 * 1024);
    ASSERT_TRUE(m2.CopyFrom(m1));
    size_t offset = 1234;
    size_t size = 10244;
    BlockMappingTest::Append(&m2, offset, 7, size, 10);

    map<bytestring, pair<int, uint64_t> > diff;
    PerformDiff(gc, m1, m2, &diff);
    ASSERT_EQ(diff.size(), 1U);
    ASSERT_EQ(diff[BlockMappingTest::FingerprintString(7)].first, 1);
}

TEST_P(UsageCountGarbageCollectorTest, DifferenceDelete) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));

    BlockMapping m1(0, 64 * 1024);
    BlockMappingTest::FillTestBlockMapping(&m1, 0);

    BlockMapping m2(64 * 1024);
    ASSERT_TRUE(m2.CopyFrom(m1));
    size_t offset = 5000;
    size_t size = 10244;
    BlockMappingTest::Append(&m2, offset, 20, size, 10);

    map<bytestring, pair<int, uint64_t> > diff;
    PerformDiff(gc, m1, m2, &diff);

    ASSERT_EQ(diff[BlockMappingTest::FingerprintString(20)].first, 1);
    ASSERT_EQ(diff[BlockMappingTest::FingerprintString(1)].first, -1);
    ASSERT_EQ(diff.size(), 2U);
}

TEST_P(UsageCountGarbageCollectorTest, DifferenceDeleteMultiple) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));

    BlockMapping m1(0, 64 * 1024);
    BlockMappingTest::FillTestBlockMapping(&m1, 0);

    BlockMapping m2(64 * 1024);
    ASSERT_TRUE(m2.CopyFrom(m1));
    size_t offset = 6456;
    size_t size = 23456;
    BlockMappingTest::Append(&m2, offset, 20, size, 10);

    map<bytestring, pair<int, uint64_t> > diff;
    PerformDiff(gc, m1, m2, &diff);

    ASSERT_EQ(diff[BlockMappingTest::FingerprintString(20)].first, 1);
    ASSERT_EQ(diff[BlockMappingTest::FingerprintString(2)].first, -1);
    ASSERT_EQ(diff[BlockMappingTest::FingerprintString(3)].first, -1);
    ASSERT_EQ(diff[BlockMappingTest::FingerprintString(4)].first, -1);
    ASSERT_EQ(diff.size(), 4U);
}

TEST_P(UsageCountGarbageCollectorTest, DifferenceDeleteNone) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));

    BlockMapping m1(0, 64 * 1024);
    BlockMappingTest::FillTestBlockMapping(&m1, 0);

    BlockMapping m2(64 * 1024);
    ASSERT_TRUE(m2.CopyFrom(m1));
    size_t offset = 22000;
    size_t size = 1000;
    BlockMappingTest::Append(&m2, offset, 20, size, 10);

    DEBUG(m1.DebugString());
    DEBUG(m2.DebugString());

    map<bytestring, pair<int, uint64_t> > diff;
    PerformDiff(gc, m1, m2, &diff);

    map<bytestring, pair<int, uint64_t> >::iterator j;
    for (j = diff.begin(); j != diff.end(); j++) {
        DEBUG("" << Fingerprinter::DebugString(j->first) << "-" << j->second.first)
    }

    ASSERT_EQ(diff[BlockMappingTest::FingerprintString(20)].first, 1);
    ASSERT_EQ(diff[BlockMappingTest::FingerprintString(4)].first, 1);
    ASSERT_EQ(diff.size(), 2U);
}

TEST_P(UsageCountGarbageCollectorTest, ProcessBlockMappingDirect) {
    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));

    uint64_t container_id = 10;
    BlockMapping orig(0, 64 * 1024);

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    BlockMappingTest::FillTestBlockMapping(&m1, container_id);

    LogReplayContext replay_context(EVENT_REPLAY_MODE_DIRECT, 1);
    ReplayWrittenLogEntry(gc, replay_context, orig, m1);
}

TEST_P(UsageCountGarbageCollectorTest, ProcessBlockMappingWrittenReplayCommitted) {
    EXPECT_CALL(storage, IsCommitted(_)).WillRepeatedly(Return(STORAGE_ADDRESS_COMMITED));

    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));

    uint64_t container_id = 10;
    BlockMapping orig(0, 64 * 1024);

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    BlockMappingTest::FillTestBlockMapping(&m1, container_id);

    list<BlockMappingItem>::iterator i;
    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item->ConvertTo(&chunk));

        chunk.set_usage_count(10);
        ASSERT_TRUE(chunk_index->PutPersistentIndex(chunk, false, false, NO_EC));
    }
    LogReplayContext replay_context(EVENT_REPLAY_MODE_REPLAY_BG, 1);
    ReplayWrittenLogEntry(gc, replay_context, orig, m1);

    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));
        ASSERT_TRUE(chunk_index->Lookup(&chunk, false, NO_EC));
        ASSERT_EQ(11U, chunk.usage_count());
    }
}

TEST_P(UsageCountGarbageCollectorTest, ProcessBlockMappingDeletedReplay) {
    EXPECT_CALL(storage, IsCommitted(_)).WillRepeatedly(Return(STORAGE_ADDRESS_COMMITED));

    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));

    uint64_t container_id = 10;
    BlockMapping orig(0, 64 * 1024);

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    BlockMappingTest::FillTestBlockMapping(&m1, container_id);

    list<BlockMappingItem>::iterator i;
    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));

        chunk.set_usage_count(10);
        ASSERT_TRUE(chunk_index->PutPersistentIndex(chunk, false, false, NO_EC));
    }
    LogReplayContext replay_context(EVENT_REPLAY_MODE_REPLAY_BG, 1);
    ReplayDeletedLogEntry(gc, replay_context, m1);

    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));
        ASSERT_TRUE(chunk_index->Lookup(&chunk, false, NO_EC));
        ASSERT_EQ(chunk.usage_count(), 9U);
    }
}

TEST_P(UsageCountGarbageCollectorTest, ProcessBlockMappingWrittenWithUpdatedMapping) {
    EXPECT_CALL(storage, IsCommitted(_)).WillRepeatedly(Return(STORAGE_ADDRESS_COMMITED));

    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));

    uint64_t container_id = 10;
    BlockMapping m1(0, 64 * 1024);
    BlockMappingTest::FillTestBlockMapping(&m1, container_id);

    BlockMapping m2(64 * 1024);
    ASSERT_TRUE(m2.CopyFrom(m1));
    size_t offset = 6456;
    size_t size = 23456;
    BlockMappingTest::Append(&m2, offset, 20, size, 10);
    m1.set_version(m1.version() + 1); // TODO (dmeister) m1? m2?

    // Init chunk index
    list<BlockMappingItem>::iterator i;
    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));

        chunk.set_usage_count(1);
        ASSERT_TRUE(chunk_index->PutPersistentIndex(chunk, false, false, NO_EC));
    }
    for (i = m2.items().begin(); i != m2.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));

        chunk.set_usage_count(1);
        ASSERT_TRUE(chunk_index->PutPersistentIndex(chunk, false, false, NO_EC));
    }

    LogReplayContext replay_context(EVENT_REPLAY_MODE_REPLAY_BG, 1);
    ReplayWrittenLogEntry(gc, replay_context, m1, m2);

    // Check chunk index usage count
    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));
        ASSERT_TRUE(chunk_index->Lookup(&chunk, false, NO_EC));

        byte short_fp = chunk.fingerprint()[0];
        if (short_fp == 2 || short_fp == 3 || short_fp == 4) {
            ASSERT_EQ(chunk.usage_count(), 0U);
        } else {
            ASSERT_EQ(chunk.usage_count(), 1U);
        }
    }

    // Check gc candidates
    PersistentIndex* index = gc->candidate_info();
    ASSERT_TRUE(index);
    dedupv1::base::IndexIterator* cur = index->CreateIterator();
    ASSERT_TRUE(cur);

    map<bytestring, bool> found;
    GarbageCollectionCandidateData candidate_data;
    enum lookup_result lr = cur->Next(NULL, NULL, &candidate_data);
    while (lr == LOOKUP_FOUND) {
        DEBUG("Process: " << candidate_data.ShortDebugString());
        for (int i = 0; i < candidate_data.item_size(); i++) {
            bytestring bs = make_bytestring(candidate_data.item(i).fp());
            found[bs] = true;
        }
        lr = cur->Next(NULL, NULL, &candidate_data);
    }
    ASSERT_TRUE(lr != LOOKUP_ERROR);
    delete cur;

    ASSERT_EQ(found.size(), 3U);
    ASSERT_TRUE(found[BlockMappingTest::FingerprintString(2)]);
    ASSERT_TRUE(found[BlockMappingTest::FingerprintString(3)]);
    ASSERT_TRUE(found[BlockMappingTest::FingerprintString(4)]);
}

TEST_P(UsageCountGarbageCollectorTest, TriggerByIdleStart) {
    uint64_t container_id = 10;
    EXPECT_CALL(storage_session, Delete(container_id, _, _, _)).WillRepeatedly(Return(true));

    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));
    ASSERT_TRUE(gc->Run());

    BlockMapping orig(0, 64 * 1024);

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    BlockMappingTest::FillTestBlockMapping(&m1, container_id);

    list<BlockMappingItem>::iterator i;
    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));

        chunk.set_usage_count(0);
        chunk.set_data_address(container_id);

        ASSERT_TRUE(chunk_index->PutPersistentIndex(chunk, false, false, NO_EC));

        multimap<uint64_t, ChunkMapping> gc_chunks;
        gc_chunks.insert(make_pair(container_id, chunk));
        ASSERT_TRUE(gc->PutGCCandidates(gc_chunks, false));
    }

    // here we assume that the log is empty
    ASSERT_TRUE(chunk_index->in_combats().Clear());
    gc->IdleStart();
    sleep(4);

    // Check chunk index usage count
    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));
        ASSERT_EQ(LOOKUP_NOT_FOUND, chunk_index->Lookup(&chunk, false, NO_EC));
    }

    // Check gc candidates
    PersistentIndex* index = gc->candidate_info();
    ASSERT_TRUE(index);
    ASSERT_EQ(0, index->GetItemCount());
}

TEST_P(UsageCountGarbageCollectorTest, TriggerByIdleStartLogReplayed) {
    uint64_t container_id = 10;
    EXPECT_CALL(storage_session, Delete(container_id, _, _, _)).WillRepeatedly(Return(true));
    EXPECT_CALL(log, IsReplaying()).WillRepeatedly(Return(false));

    ASSERT_TRUE(system.idle_detector()->Start());
    ASSERT_TRUE(system.idle_detector()->Run());
    ASSERT_TRUE(system.idle_detector()->ForceIdle(true));

    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));
    ASSERT_TRUE(gc->Run());

    BlockMapping orig(0, 64 * 1024);

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    BlockMappingTest::FillTestBlockMapping(&m1, container_id);

    list<BlockMappingItem>::iterator i;
    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));

        chunk.set_usage_count(0);
        chunk.set_data_address(container_id);

        ASSERT_TRUE(chunk_index->PutPersistentIndex(chunk, false, false, NO_EC));

        multimap<uint64_t, ChunkMapping> gc_chunks;
        gc_chunks.insert(make_pair(container_id, chunk));
        ASSERT_TRUE(gc->PutGCCandidates(gc_chunks, false));
    }
    // here we assume that the log is empty
    ASSERT_TRUE(chunk_index->in_combats().Clear());
    gc->IdleStart();
    sleep(4);
    ASSERT_TRUE(system.idle_detector()->Stop(StopContext()));

    // Check chunk index usage count
    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));
        ASSERT_EQ(LOOKUP_NOT_FOUND, chunk_index->Lookup(&chunk, false, NO_EC));
    }

    // Check gc candidates
    PersistentIndex* index = gc->candidate_info();
    ASSERT_TRUE(index);
    ASSERT_EQ(0, index->GetItemCount());
}

TEST_P(UsageCountGarbageCollectorTest, TriggerByStartProcessing) {
    uint64_t container_id = 10;
    EXPECT_CALL(storage_session, Delete(container_id, _, _, _)).WillRepeatedly(Return(true));

    SetDefaultOptions(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &system));
    ASSERT_TRUE(gc->Run());

    BlockMapping orig(0, 64 * 1024);

    BlockMapping m1(0, 64 * 1024);
    m1.set_version(m1.version() + 1);
    BlockMappingTest::FillTestBlockMapping(&m1, container_id);

    list<BlockMappingItem>::iterator i;
    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));

        chunk.set_usage_count(0);
        chunk.set_data_address(container_id);

        ASSERT_TRUE(chunk_index->PutPersistentIndex(chunk, false, false, NO_EC));

        multimap<uint64_t, ChunkMapping> gc_chunks;
        gc_chunks.insert(make_pair(container_id, chunk));
        ASSERT_TRUE(gc->PutGCCandidates(gc_chunks, false));
    }

    // here we assume that the log is empty
    ASSERT_TRUE(chunk_index->in_combats().Clear());
    ASSERT_TRUE(gc->StartProcessing());
    sleep(4);

    // Check chunk index usage count
    for (i = m1.items().begin(); i != m1.items().end(); i++) {
        ChunkMapping chunk;
        BlockMappingItem* item = (&*i);
        ASSERT_TRUE(item);
        ASSERT_TRUE(item->ConvertTo(&chunk));
        ASSERT_EQ(LOOKUP_NOT_FOUND, chunk_index->Lookup(&chunk, false, NO_EC));
    }

    // Check gc candidates
    PersistentIndex* index = gc->candidate_info();
    ASSERT_TRUE(index);
    ASSERT_EQ(0, index->GetItemCount());
}

}
}
