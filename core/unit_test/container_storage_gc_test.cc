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
#include <base/locks.h>
#include <base/strutil.h>
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

#include "dedupv1.pb.h"

#include "storage_test.h"
#include <test_util/log_assert.h>

#include <test/container_storage_mock.h>
#include <test/storage_mock.h>

LOGGER("ContainerGCTest");

using std::pair;
using std::make_pair;
using std::list;
using testing::Return;
using testing::_;

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

class GreedyContainerGCStrategyTest : public testing::TestWithParam<int> {
protected:
    static size_t const CONTAINER_SIZE = 512 * 1024;
    static size_t const TEST_DATA_SIZE = 256 * 1024;

    USE_LOGGING_EXPECTATION();

    GreedyContainerGCStrategy* gc;
    MockContainerStorage storage;
    MockStorageSession storage_session;

    byte test_data[16][TEST_DATA_SIZE];
    uint64_t test_adress[164];
    uint64_t test_fp[16];

    virtual void SetUp() {
        EXPECT_CALL(storage, CreateSession()).WillRepeatedly(Return(&storage_session));
        EXPECT_CALL(storage_session, Close()).WillRepeatedly(Return(true));

        storage.SetOption("container-size", ToString(CONTAINER_SIZE));

        gc = new GreedyContainerGCStrategy();
        ASSERT_TRUE(gc);
        ASSERT_TRUE(gc->Init());

        int fd = open("/dev/urandom", O_RDONLY);
        for (size_t i = 0; i < 16; i++) {
            size_t j = 0;
            while (j < TEST_DATA_SIZE) {
                size_t r = read(fd, test_data[i] + j, TEST_DATA_SIZE - j);
                j += r;
            }
            test_fp[i] = i + 1;
            test_adress[i] = 0;
        }
        close(fd);
    }

    virtual void TearDown() {
        if (gc) {
            ASSERT_TRUE(gc->Close());
        }
    }

    void SetDefaultConfig(GreedyContainerGCStrategy* gc) {
        ASSERT_TRUE(gc->SetOption("type", "sqlite-disk-btree"));
        ASSERT_TRUE(gc->SetOption("max-item-count", "4M"));

        if (GetParam() == 1) {
            ASSERT_TRUE(gc->SetOption("filename", "work/merge-candidates"));
        } else {
            for (int i = 0; i < GetParam(); i++) {
                ASSERT_TRUE(gc->SetOption("filename", "work/merge-candidates-" + ToString(i + 1)));
            }
        }
        ASSERT_TRUE(gc->SetOption("eviction-timeout", "0")); // we deactivate the eviction system
    }

    void FillDefaultContainer(Container* container, int begin, int count) {
        for (int i = 0; i < count; i++) {
            // Use small items to avoid an overflow
            ASSERT_TRUE(container->AddItem((byte *) &test_fp[begin + i], sizeof(test_fp[begin + i]), (byte *) test_data[begin + i], (size_t) 16 * 1024, true, NULL))
            << "Add item " << begin + i << " failed";
        }
    }
};

INSTANTIATE_TEST_CASE_P(GreedyContainerGCStrategy,
    GreedyContainerGCStrategyTest,
    ::testing::Values(1, 2, 4));

TEST_P(GreedyContainerGCStrategyTest, Init) {
    // do nothing
}

TEST_P(GreedyContainerGCStrategyTest, StartWithoutConfig) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_FALSE(gc->Start(StartContext(), &storage)) << "A start without a config should fail";
}

TEST_P(GreedyContainerGCStrategyTest, Start) {
    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));
}

TEST_P(GreedyContainerGCStrategyTest, OnRead) {
    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));

    Container c;
    ASSERT_TRUE(c.Init(0, CONTAINER_SIZE));
    FillDefaultContainer(&c, 0, 4 );

    ASSERT_TRUE(gc->OnRead(c, (void *) &test_fp[2], sizeof(test_fp[2])));
}

TEST_P(GreedyContainerGCStrategyTest, OnCommitFullContainer) {
    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));

    Container c;
    ASSERT_TRUE(c.Init(0, CONTAINER_SIZE));
    FillDefaultContainer(&c, 0, 12 );

    ContainerCommittedEventData data;
    data.set_container_id(c.primary_id());
    data.set_active_data_size(c.active_data_size());
    data.set_item_count(c.item_count());

    ASSERT_TRUE(gc->OnCommit(data));

    PersistentIndex* mc = gc->merge_candidates();
    ASSERT_TRUE(mc);
    ASSERT_EQ(0, mc->GetItemCount());
}

TEST_P(GreedyContainerGCStrategyTest, OnCommitEmptyContainer) {
    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));

    Container c;
    ASSERT_TRUE(c.Init(0, CONTAINER_SIZE));
    FillDefaultContainer(&c, 0, 1);

    ContainerCommittedEventData data;
    data.set_container_id(c.primary_id());
    data.set_active_data_size(c.active_data_size());
    data.set_item_count(c.item_count());
    ASSERT_TRUE(gc->OnCommit(data));

    PersistentIndex* mc = gc->merge_candidates();
    ASSERT_TRUE(mc);

    ContainerGreedyGCCandidateData candidate_data;
    uint64_t bucket = gc->GetBucket(c.active_data_size());
    ASSERT_EQ(LOOKUP_FOUND, mc->Lookup(&bucket, sizeof(bucket), &candidate_data));
    ASSERT_EQ(candidate_data.item_size(), 1);
    ASSERT_EQ(candidate_data.item(0).address(), 0U);
}

TEST_P(GreedyContainerGCStrategyTest, OnCommitEmptyContainerWithExistingBucket) {
    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));

    Container c;
    c.Init(0, CONTAINER_SIZE);
    FillDefaultContainer(&c, 0, 2);

    ContainerCommittedEventData data1;
    data1.set_container_id(c.primary_id());
    data1.set_active_data_size(c.active_data_size());
    data1.set_item_count(c.item_count());
    ASSERT_TRUE(gc->OnCommit(data1));

    Container c2;
    ASSERT_TRUE(c2.Init(1, CONTAINER_SIZE));
    FillDefaultContainer(&c2, 0, 2);

    ContainerCommittedEventData data2;
    data2.set_container_id(c2.primary_id());
    data2.set_active_data_size(c2.active_data_size());
    data2.set_item_count(c2.item_count());
    ASSERT_TRUE(gc->OnCommit(data2));

    PersistentIndex* mc = gc->merge_candidates();
    ASSERT_TRUE(mc);

    ContainerGreedyGCCandidateData candidate_data;
    uint64_t bucket = gc->GetBucket(c.active_data_size());
    ASSERT_EQ(LOOKUP_FOUND, mc->Lookup(&bucket, sizeof(bucket), &candidate_data));
    ASSERT_EQ(candidate_data.item_size(), 2);
    ASSERT_EQ(candidate_data.item(0).address(), 0U);
    ASSERT_EQ(candidate_data.item(1).address(), 1U);
}

TEST_P(GreedyContainerGCStrategyTest, OnDeleteFull) {
    EXPECT_CALL(storage, IsCommitted(_)).WillRepeatedly(Return(STORAGE_ADDRESS_COMMITED));

    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));

    Container c;
    c.Init(0, CONTAINER_SIZE);
    FillDefaultContainer(&c, 0, 12);

    uint32_t old_active_data_size = c.active_data_size();
    uint32_t old_item_count = c.item_count();
    c.DeleteItem((byte *) &test_fp[2], sizeof(test_fp[2]));

    ContainerMoveEventData event_data;
    event_data.set_container_id(c.primary_id());
    event_data.set_old_active_data_size(old_active_data_size);
    event_data.set_active_data_size(c.active_data_size());
    event_data.set_item_count(c.item_count());
    event_data.set_old_item_count(old_item_count);

    ASSERT_TRUE(gc->OnMove(event_data));

    old_active_data_size = c.active_data_size();
    old_item_count = c.item_count();

    c.DeleteItem((byte *) &test_fp[0], sizeof(test_fp[0]));

    event_data.Clear();
    event_data.set_container_id(c.primary_id());
    event_data.set_old_active_data_size(old_active_data_size);
    event_data.set_active_data_size(c.active_data_size());
    event_data.set_item_count(c.item_count());
    event_data.set_old_item_count(old_item_count);

    ASSERT_TRUE(gc->OnMove(event_data));

    PersistentIndex* mc = gc->merge_candidates();
    ASSERT_TRUE(mc);
    ASSERT_EQ(0, mc->GetItemCount());
}

TEST_P(GreedyContainerGCStrategyTest, OnDeleteHalfFull) {
    EXPECT_CALL(storage, IsCommitted(_)).WillRepeatedly(Return(STORAGE_ADDRESS_COMMITED));

    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));

    Container c;
    ASSERT_TRUE(c.Init(0, CONTAINER_SIZE));
    FillDefaultContainer(&c, 0, 4 );

    uint32_t old_active_data_size = c.active_data_size();
    uint32_t old_item_count = c.item_count();
    ASSERT_TRUE(c.DeleteItem((byte *) &test_fp[2], sizeof(test_fp[2])));

    ContainerMoveEventData event_data;
    event_data.set_container_id(c.primary_id());
    event_data.set_old_active_data_size(old_active_data_size);
    event_data.set_active_data_size(c.active_data_size());
    event_data.set_item_count(c.item_count());
    event_data.set_old_item_count(old_item_count);

    ASSERT_TRUE(gc->OnMove(event_data));

    old_active_data_size = c.active_data_size();
    old_item_count = c.item_count();

    ASSERT_TRUE(c.DeleteItem((byte *) &test_fp[0], sizeof(test_fp[0])));

    event_data.Clear();
    event_data.set_container_id(c.primary_id());
    event_data.set_old_active_data_size(old_active_data_size);
    event_data.set_active_data_size(c.active_data_size());
    event_data.set_item_count(c.item_count());
    event_data.set_old_item_count(old_item_count);
    ASSERT_TRUE(gc->OnMove(event_data));

    PersistentIndex* mc = gc->merge_candidates();
    ASSERT_TRUE(mc);

    ContainerGreedyGCCandidateData candidate_data;
    uint64_t bucket = gc->GetBucket(c.active_data_size());
    ASSERT_EQ(LOOKUP_FOUND, mc->Lookup(&bucket, sizeof(bucket), &candidate_data));
    ASSERT_EQ(candidate_data.item_size(), 1);
    ASSERT_EQ(candidate_data.item(0).address(), 0U);
}

TEST_P(GreedyContainerGCStrategyTest, OnIdleNoCandidates) {
    EXPECT_CALL(storage, TryMergeContainer(_,_,_)).Times(0);

    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));

    ASSERT_TRUE(gc->OnIdle());
}

TEST_P(GreedyContainerGCStrategyTest, OnIdleOneCandidates) {
    ContainerStorageAddressData address0;
    address0.set_file_index(1);
    address0.set_file_offset(0);
    EXPECT_CALL(storage, LookupContainerAddress(0, _, _)).WillRepeatedly(Return(make_pair(LOOKUP_FOUND, address0)));

    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));

    Container c;
    c.Init(0, CONTAINER_SIZE);
    FillDefaultContainer(&c, 0, 2);

    ContainerCommittedEventData data;
    data.set_container_id(c.primary_id());
    data.set_active_data_size(c.active_data_size());
    data.set_item_count(c.item_count());
    ASSERT_TRUE(gc->OnCommit(data));

    ASSERT_TRUE(gc->OnIdle());

    // check if all candidate buckets are gone
    PersistentIndex* mc = gc->merge_candidates();
    ASSERT_TRUE(mc);

    ContainerGreedyGCCandidateData candidate_data;
    uint64_t bucket = gc->GetBucket(c.active_data_size());
    ASSERT_EQ(LOOKUP_FOUND, mc->Lookup(&bucket, sizeof(bucket), &candidate_data));
    ASSERT_EQ(candidate_data.item_size(), 1);
}

TEST_P(GreedyContainerGCStrategyTest, OnIdleTwoCandidates) {
    ContainerStorageAddressData address0;
    address0.set_file_index(1);
    address0.set_file_offset(0);
    ContainerStorageAddressData address1;
    address1.set_file_index(2);
    address1.set_file_offset(0);
    EXPECT_CALL(storage, TryMergeContainer(0, 1, _)).Times(1).WillOnce(Return(true));
    EXPECT_CALL(storage, LookupContainerAddress(0, _, _)).WillRepeatedly(Return(make_pair(LOOKUP_FOUND, address0)));
    EXPECT_CALL(storage, LookupContainerAddress(1, _, _)).WillRepeatedly(Return(make_pair(LOOKUP_FOUND, address1)));

    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));

    Container c;
    c.Init(0, CONTAINER_SIZE);
    FillDefaultContainer(&c, 0, 2);
    ContainerCommittedEventData data;
    data.set_container_id(c.primary_id());
    data.set_active_data_size(c.active_data_size());
    data.set_item_count(c.item_count());
    ASSERT_TRUE(gc->OnCommit(data));

    Container c2;
    c2.Init(1, CONTAINER_SIZE);
    FillDefaultContainer(&c2, 0, 2);
    ContainerCommittedEventData data2;
    data2.set_container_id(c2.primary_id());
    data2.set_active_data_size(c2.active_data_size());
    data2.set_item_count(c2.item_count());
    ASSERT_TRUE(gc->OnCommit(data2));

    ASSERT_TRUE(gc->OnIdle());

    // check if all candidate buckets are gone
    PersistentIndex* mc = gc->merge_candidates();
    ASSERT_TRUE(mc);
    ASSERT_EQ(0, mc->GetItemCount());
}

TEST_P(GreedyContainerGCStrategyTest, OnIdleThreeCandidatesinSingleBucket) {
    EXPECT_CALL(storage, TryMergeContainer(_,_,_)).Times(1).WillOnce(Return(true));
    ContainerStorageAddressData address0;
    address0.set_file_index(1);
    address0.set_file_offset(0);
    ContainerStorageAddressData address1;
    address1.set_file_index(2);
    address1.set_file_offset(0);
    ContainerStorageAddressData address2;
    address2.set_file_index(3);
    address2.set_file_offset(0);
    EXPECT_CALL(storage, LookupContainerAddress(2, _, _)).WillRepeatedly(Return(make_pair(LOOKUP_FOUND, address2)));
    EXPECT_CALL(storage, LookupContainerAddress(1, _, _)).WillRepeatedly(Return(make_pair(LOOKUP_FOUND, address1)));
    EXPECT_CALL(storage, LookupContainerAddress(0, _, _)).WillRepeatedly(Return(make_pair(LOOKUP_FOUND, address0)));

    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));

    Container c;
    c.Init(0, CONTAINER_SIZE);
    FillDefaultContainer(&c, 0, 2);
    ContainerCommittedEventData data;
    data.set_container_id(c.primary_id());
    data.set_active_data_size(c.active_data_size());
    data.set_item_count(c.item_count());
    ASSERT_TRUE(gc->OnCommit(data));

    Container c2;
    c2.Init(1, CONTAINER_SIZE);
    FillDefaultContainer(&c2, 0, 2);
    ContainerCommittedEventData data2;
    data2.set_container_id(c2.primary_id());
    data2.set_active_data_size(c2.active_data_size());
    data2.set_item_count(c2.item_count());
    ASSERT_TRUE(gc->OnCommit(data2));

    Container c3;
    c3.Init(2, CONTAINER_SIZE);
    FillDefaultContainer(&c3, 0, 2);
    ContainerCommittedEventData data3;
    data3.set_container_id(c3.primary_id());
    data3.set_active_data_size(c3.active_data_size());
    data3.set_item_count(c3.item_count());
    ASSERT_TRUE(gc->OnCommit(data3));

    ASSERT_TRUE(gc->OnIdle());

    PersistentIndex* mc = gc->merge_candidates();
    ASSERT_TRUE(mc);

    ContainerGreedyGCCandidateData candidate_data;
    uint64_t bucket = gc->GetBucket(c3.active_data_size());
    ASSERT_EQ(LOOKUP_FOUND, mc->Lookup(&bucket, sizeof(bucket), &candidate_data));
    ASSERT_EQ(candidate_data.item_size(), 1);
}

TEST_P(GreedyContainerGCStrategyTest, OnIdleThreeCandidatesinTwoBucket) {
    EXPECT_CALL(storage, TryDeleteContainer(_,_)).Times(1).WillOnce(Return(true));
    EXPECT_CALL(storage, TryMergeContainer(_,_,_)).Times(1).WillOnce(Return(true));

    ContainerStorageAddressData address2;
    address2.set_file_index(1);
    address2.set_file_offset(0);
    ContainerStorageAddressData address1;
    address1.set_file_index(2);
    address1.set_file_offset(0);
    ContainerStorageAddressData address0;
    address0.set_file_index(3);
    address0.set_file_offset(0);
    EXPECT_CALL(storage, LookupContainerAddress(0, _, _)).WillRepeatedly(Return(make_pair(LOOKUP_FOUND, address0)));
    EXPECT_CALL(storage, LookupContainerAddress(1, _, _)).WillRepeatedly(Return(make_pair(LOOKUP_FOUND, address1)));
    EXPECT_CALL(storage, LookupContainerAddress(2, _, _)).WillRepeatedly(Return(make_pair(LOOKUP_FOUND, address2)));

    SetDefaultConfig(gc);
    ASSERT_TRUE(gc->Start(StartContext(), &storage));

    Container c;
    c.Init(0, CONTAINER_SIZE);
    FillDefaultContainer(&c, 0, 0);
    ContainerCommittedEventData data;
    data.set_container_id(c.primary_id());
    data.set_active_data_size(c.active_data_size());
    data.set_item_count(c.item_count());
    ASSERT_TRUE(gc->OnCommit(data));

    Container c2;
    c2.Init(1, CONTAINER_SIZE);
    FillDefaultContainer(&c2, 0, 2);
    ContainerCommittedEventData data2;
    data2.set_container_id(c2.primary_id());
    data2.set_active_data_size(c2.active_data_size());
    data2.set_item_count(c2.item_count());
    ASSERT_TRUE(gc->OnCommit(data2));

    Container c3;
    c3.Init(2, CONTAINER_SIZE);
    FillDefaultContainer(&c3, 0, 2);
    ContainerCommittedEventData data3;
    data3.set_container_id(c3.primary_id());
    data3.set_active_data_size(c3.active_data_size());
    data3.set_item_count(c3.item_count());
    ASSERT_TRUE(gc->OnCommit(data3));

    ASSERT_TRUE(gc->OnIdle());

    PersistentIndex* mc = gc->merge_candidates();
    ASSERT_TRUE(mc);

    ContainerGreedyGCCandidateData candidate_data;
    uint64_t bucket = gc->GetBucket(c2.active_data_size());
    ASSERT_EQ(LOOKUP_FOUND, mc->Lookup(&bucket, sizeof(bucket), &candidate_data));
    ASSERT_EQ(candidate_data.item_size(), 2);

    // Now delete the other two
    ASSERT_TRUE(gc->OnIdle());

    mc = gc->merge_candidates();
    ASSERT_EQ(0, mc->GetItemCount());
}
