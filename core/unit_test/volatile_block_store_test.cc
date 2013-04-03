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

#include <list>
#include <set>
#include <algorithm>

#include <gtest/gtest.h>

#include <core/dedup.h>
#include <base/locks.h>

#include <core/session.h>
#include <core/chunk_store.h>
#include <core/chunker.h>
#include <core/block_mapping.h>
#include <core/volatile_block_store.h>
#include <core/dedup_system.h>

#include "block_mapping_test.h"
#include <test_util/log_assert.h>

using std::list;
using std::set;

namespace dedupv1 {
namespace blockindex {

class VolatileBlockStorageTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1::blockindex::VolatileBlockStore* volatile_blocks;

    virtual void SetUp() {

        volatile_blocks = new dedupv1::blockindex::VolatileBlockStore();
        ASSERT_TRUE(volatile_blocks);
    }

    virtual void TearDown() {
        if (volatile_blocks) {
            volatile_blocks->Clear();
            delete volatile_blocks;
            volatile_blocks = NULL;
        }
    }
};

TEST_F(VolatileBlockStorageTest, Create) {
    ASSERT_TRUE(volatile_blocks);
}

TEST_F(VolatileBlockStorageTest, SimpleAddBlock) {
    uint64_t container_id = 1;

    BlockMapping orig(container_id, 64 * 1024);
    BlockMapping m(container_id, 64 * 1024);
    BlockMappingTest::FillTestBlockMapping(&m);

    set<uint64_t> l;
    l.insert(container_id);

    ASSERT_TRUE(volatile_blocks->AddBlock(orig, m, NULL, l, 0, NULL));

    std::multimap<uint64_t, UncommitedBlockEntry>::iterator i = volatile_blocks->uncommited_block_map_.find(m.block_id());
    ASSERT_TRUE(i != volatile_blocks->uncommited_block_map_.end());
    ASSERT_TRUE(i->second.modified_mapping().Equals(m));
    ASSERT_TRUE(i->second.original_mapping().Equals(orig));
    ASSERT_EQ(i->second.open_container_count(), 1U);

    std::map<uint64_t, UncommitedContainerEntry>::iterator j = volatile_blocks->uncommited_container_map_.find(container_id);
    ASSERT_TRUE(j != volatile_blocks->uncommited_container_map_.end()) << "Found no matching container entry";
    std::list< std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::const_iterator k = std::find(
        j->second.block_list().begin(), j->second.block_list().end(), i);
    ASSERT_TRUE(k != j->second.block_list().end()) << "Block list and container list are not connected correctly";

}

TEST_F(VolatileBlockStorageTest, AddBlockTwoContainers) {
    uint64_t container_id[2];
    container_id[0] = 1;
    container_id[1] = 2;

    BlockMapping orig(container_id[0], 64 * 1024);
    BlockMapping m(container_id[0], 64 * 1024);
    BlockMappingTest::FillTestBlockMapping(&m);

    set<uint64_t> l;
    l.insert(container_id[0]);
    l.insert(container_id[1]);

    ASSERT_TRUE(volatile_blocks->AddBlock(orig, m, NULL, l, 0, NULL));

    std::multimap<uint64_t, UncommitedBlockEntry>::iterator bi = volatile_blocks->uncommited_block_map_.find(m.block_id());
    ASSERT_TRUE(bi != volatile_blocks->uncommited_block_map_.end());
    ASSERT_TRUE(bi->second.modified_mapping().Equals(m));
    ASSERT_TRUE(bi->second.original_mapping().Equals(orig));
    ASSERT_EQ(bi->second.open_container_count(), 2U);

    for (int i = 0; i < 2; i++) {
        std::map<uint64_t, UncommitedContainerEntry>::iterator j = volatile_blocks->uncommited_container_map_.find(container_id[i]);
        ASSERT_TRUE(j != volatile_blocks->uncommited_container_map_.end()) << "Found no matching container entry";
        std::list< std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::const_iterator k = std::find(
            j->second.block_list().begin(), j->second.block_list().end(), bi);
        ASSERT_TRUE(k != j->second.block_list().end()) << "Block list and container list are not connected correctly";
    }
}

TEST_F(VolatileBlockStorageTest, AddTwoBlockTwoContainers) {
    uint64_t container_id[2];
    container_id[0] = 1;
    container_id[1] = 2;

    BlockMapping** mappings = new BlockMapping *[2];
    for (int j = 0; j < 2; j++) {
        mappings[j] = new BlockMapping(container_id[0], 64 * 1024);
        BlockMappingTest::FillTestBlockMapping(mappings[j]);

        set<uint64_t> l;
        l.insert(container_id[0]);
        l.insert(container_id[1]);

        BlockMapping orig(container_id[0], 64 * 1024);
        ASSERT_TRUE(volatile_blocks->AddBlock(orig, *mappings[j], NULL, l, 0, NULL));
    }

    for (int j = 0; j < 2; j++) {
        std::multimap<uint64_t, UncommitedBlockEntry>::iterator i = volatile_blocks->uncommited_block_map_.find(mappings[j]->block_id());
        ASSERT_TRUE(i != volatile_blocks->uncommited_block_map_.end());
        ASSERT_TRUE(i->second.modified_mapping().Equals(*mappings[j]));
        ASSERT_EQ(i->second.open_container_count(), 2U);

        for (int k = 0; k < 2; k++) {
            std::map<uint64_t, UncommitedContainerEntry>::iterator l = volatile_blocks->uncommited_container_map_.find(container_id[k]);
            ASSERT_TRUE(l != volatile_blocks->uncommited_container_map_.end()) << "Found no matching container entry";
            std::list< std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::const_iterator m = std::find(
                l->second.block_list().begin(), l->second.block_list().end(), i);
            ASSERT_TRUE(m != l->second.block_list().end()) << "Block list and container list are not connected correctly";
        }
    }
    for (int j = 0; j < 2; j++) {
        delete mappings[j];
    }
    delete[] mappings;
}

class VolatileBlockStorageTestCommitCallback : public VolatileBlockCommitCallback {
public:
    int counter_;
    int fail_;
    uint64_t last_commit_event_log_id_;

    VolatileBlockStorageTestCommitCallback() {
        counter_ = 0;
        fail_ = 0;
    }

    bool CommitVolatileBlock(const BlockMapping& o, const BlockMapping& m, const google::protobuf::Message* extra_message, int64_t event_log_id, bool direct) {
        counter_++;
        last_commit_event_log_id_ = event_log_id;
        return true;
    }

    bool FailVolatileBlock(const BlockMapping& o, const BlockMapping& m, const google::protobuf::Message* extra_message, int64_t event_log_id) {
        fail_++;
        return true;
    }
};

TEST_F(VolatileBlockStorageTest, SimpleCommit) {
    uint64_t container_id[2];
    container_id[0] = 1;
    container_id[1] = 2;

    BlockMapping orig(container_id[0], 64 * 1024);
    BlockMapping m(container_id[0],64 * 1024);
    BlockMappingTest::FillTestBlockMapping(&m);

    set<uint64_t> l;
    l.insert(container_id[0]);
    l.insert(container_id[1]);

    uint64_t block_write_log_id = 10;
    ASSERT_TRUE(volatile_blocks->AddBlock(orig, m, NULL, l, block_write_log_id, NULL));

    VolatileBlockStorageTestCommitCallback c;

    ASSERT_TRUE(volatile_blocks->Commit(1, &c));
    ASSERT_EQ(c.counter_, 0);

    ASSERT_TRUE(volatile_blocks->Commit(2, &c));
    ASSERT_EQ(c.counter_, 1);
    ASSERT_EQ(c.last_commit_event_log_id_, block_write_log_id);
}

}
}
