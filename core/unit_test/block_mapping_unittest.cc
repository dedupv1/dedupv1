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

#include <test/block_mapping_test.h>

#include <base/logging.h>

#include <gtest/gtest.h>
#include <core/block_mapping.h>

using std::pair;

LOGGER("BlockMappingTest");

namespace dedupv1 {
namespace blockindex {

INSTANTIATE_TEST_CASE_P(BlockMapping,
    BlockMappingTest,
    ::testing::Values(
        pair<int, int>(1024, 656),
        pair<int, int>(1235, 1),
        pair<int, int>(7889, 6430),
        pair<int, int>(12349, 12345),
        pair<int, int>(44056, 9234),
        pair<int, int>(51667, 123)
        ));

TEST_F(BlockMappingTest, Init) {
    BlockMapping m(BLOCKSIZE_64K);
    ASSERT_TRUE(m.block_id() == BlockMapping::ILLEGAL_BLOCK_ID);
    ASSERT_EQ(m.block_size(), BLOCKSIZE_64K);
    ASSERT_EQ(m.item_count(), 1U);
    ASSERT_TRUE(m.Check());
}

TEST_F(BlockMappingTest, Acquire) {
    BlockMapping m(1, BLOCKSIZE_64K);
    ASSERT_EQ(m.block_id(), 1U);
    ASSERT_EQ(m.block_size(), BLOCKSIZE_64K);
    ASSERT_EQ(m.item_count(), 1U);
    ASSERT_TRUE(m.Check());
}

TEST_F(BlockMappingTest, DefaultBlockMapping) {
    BlockMapping m(0, BLOCKSIZE_64K);

    BlockMappingTest::FillDefaultBlockMapping(&m);
    ASSERT_TRUE(m.Check());
}

TEST_F(BlockMappingTest, NormalBlockMapping) {
    BlockMapping m(0, BLOCKSIZE_64K);

    BlockMappingTest::FillTestBlockMapping(&m);
    ASSERT_TRUE(m.Check());
}

TEST_F(BlockMappingTest, LargeNormalBlockMapping) {
    BlockMapping m(0, BLOCKSIZE_256K);

    BlockMappingTest::FillTestLargeMapping(&m);
    ASSERT_TRUE(m.Check());
}

TEST_F(BlockMappingTest, SimpleAppend) {
    BlockMappingItem item(0, 1024);
    BlockMapping m(1, BLOCKSIZE_64K);
    ASSERT_TRUE(m.Append(0, item));
    ASSERT_TRUE(m.Check());
}

TEST_F(BlockMappingTest, Equals) {
    BlockMapping m1(0, BLOCKSIZE_64K);
    BlockMappingTest::FillTestBlockMapping(&m1);

    BlockMapping m2(0, BLOCKSIZE_64K);
    BlockMappingTest::FillTestBlockMapping(&m2);

    ASSERT_TRUE(m1.Equals(m2));
}

TEST_F(BlockMappingTest, MergeParts) {
    BlockMapping m1(0, BLOCKSIZE_256K);
    BlockMappingTest::FillTestLargeMapping(&m1);

    BlockMapping m2(0, BLOCKSIZE_256K);
    ASSERT_TRUE(m2.FillEmptyBlockMapping());

    DEBUG("Merge from position 4012 to offset 1238, 12312 bytes:");
    ASSERT_TRUE(m2.MergePartsFrom(m1, 4012, 1238, 12312));

    DEBUG("Source: " << m1.DebugString());
    DEBUG("Result: " << m2.DebugString());
    ASSERT_TRUE(m2.Check()) << "Check failed: " << m2.DebugString();
}

TEST_F(BlockMappingTest, FullMergeParts) {
    BlockMapping m1(0, BLOCKSIZE_256K);
    BlockMappingTest::FillTestLargeMapping(&m1);

    BlockMapping m2(0, BLOCKSIZE_256K);
    ASSERT_TRUE(m2.FillEmptyBlockMapping());

    DEBUG("Merge from position 4012 to offset 1238, 12312 bytes:");
    ASSERT_TRUE(m2.MergePartsFrom(m1, 0, 0, m1.block_size()));

    DEBUG("Source: " << m1.DebugString());
    DEBUG("Result: " << m2.DebugString());
    ASSERT_TRUE(m2.Check()) << "Check failed: " << m2.DebugString();
    ASSERT_TRUE(m1.Equals(m2)) << m1.DebugString() << " != " << m2.DebugString();
}

TEST_F(BlockMappingTest, Serialize) {
    BlockMapping m1(0, BLOCKSIZE_64K);
    BlockMapping m2(0, BLOCKSIZE_64K);

    BlockMappingTest::FillTestBlockMapping(&m1);

    BlockMappingData value;
    ASSERT_TRUE(m1.SerializeTo(&value, true, true));
    ASSERT_TRUE(m2.UnserializeFrom(value, true));

    DEBUG("before: " << m1.DebugString());
    DEBUG("after: " << m2.DebugString());

    ASSERT_TRUE(m1.Equals(m2));
}

TEST_F(BlockMappingTest, SerializeWithoutChecksum) {
    BlockMapping m1(0, BLOCKSIZE_64K);
    BlockMapping m2(0, BLOCKSIZE_64K);

    BlockMappingTest::FillTestBlockMapping(&m1);

    BlockMappingData value;
    ASSERT_TRUE(m1.SerializeTo(&value, true, false));
    ASSERT_TRUE(m2.UnserializeFrom(value, true));

    DEBUG("before: " << m1.DebugString());
    DEBUG("after: " << m2.DebugString());

    ASSERT_TRUE(m1.Equals(m2));
}

TEST_F(BlockMappingTest, CreateEmptyMapping) {
    BlockMapping m(1, BLOCKSIZE_64K);
    ASSERT_TRUE(m.FillEmptyBlockMapping());
    ASSERT_TRUE(m.Check());
}

TEST_F(BlockMappingTest, CreateLargeEmptyMapping) {
    BlockMapping m(1, BLOCKSIZE_256K);
    BlockMappingItem item(0, m.block_size());
    ASSERT_TRUE(m.FillEmptyBlockMapping());
    ASSERT_TRUE(m.Check());
}

}
}
