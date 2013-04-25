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

#include <core/chunk.h>
#include <core/chunk_mapping.h>
#include <core/storage.h>
#include <test_util/log_assert.h>

using dedupv1::chunkstore::Storage;

namespace dedupv1 {
namespace chunkindex {

class ChunkMappingTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(ChunkMappingTest, Init) {
    ChunkMapping m;

    ASSERT_EQ(m.data_address(), Storage::ILLEGAL_STORAGE_ADDRESS);
    ASSERT_TRUE(m.chunk() == NULL);
    ASSERT_EQ(m.fingerprint_size(), (size_t) 0);
}

TEST_F(ChunkMappingTest, SerializeWithoutUsageCount) {
    uint64_t fp = 1;
    ChunkMapping m1((byte *) &fp, sizeof(fp));
    m1.set_data_address(10);

    ChunkMappingData value;

    ASSERT_TRUE(m1.SerializeTo(&value));

    ChunkMapping m2;
    ASSERT_TRUE(m2.UnserializeFrom(value, false));
    ASSERT_EQ(m2.data_address(), m1.data_address());
    ASSERT_EQ(m2.usage_count(), (size_t) 0);
}

TEST_F(ChunkMappingTest, SerializeWithUsageCount) {
    uint64_t fp = 1;
    ChunkMapping m1((byte *) &fp, sizeof(fp));
    m1.set_data_address(10);
    m1.set_usage_count(10);

    ChunkMappingData value;
    ASSERT_TRUE(m1.SerializeTo(&value));

    ChunkMapping m2;
    ASSERT_TRUE(m2.UnserializeFrom(value, false));
    ASSERT_EQ(m2.data_address(), m1.data_address());
    ASSERT_EQ(m2.usage_count(), (unsigned int) 10);
}

TEST_F(ChunkMappingTest, InitWithFP) {
    uint64_t fp = 1;
    ChunkMapping m((byte *) &fp, sizeof(fp));

    ASSERT_EQ(m.data_address(), Storage::ILLEGAL_STORAGE_ADDRESS);
    ASSERT_TRUE(m.chunk() == NULL);
    ASSERT_EQ(m.fingerprint_size(), sizeof(fp));
}

TEST_F(ChunkMappingTest, InitWitChunk) {
    uint64_t fp = 1;

    Chunk c(8 * 1024);
    ChunkMapping m((byte *) &fp, sizeof(fp));
    ASSERT_TRUE(m.Init(&c));

    ASSERT_EQ(m.data_address(), Storage::ILLEGAL_STORAGE_ADDRESS);
    ASSERT_TRUE(m.chunk() == &c);
    ASSERT_EQ(m.fingerprint_size(), sizeof(fp));
    ASSERT_EQ(m.chunk()->size(), 8U * 1024);
}

TEST_F(ChunkMappingTest, Copy) {
    uint64_t fp = 1;

    Chunk c(8 * 1024);
    ChunkMapping m((byte *) &fp, sizeof(fp));
    ASSERT_TRUE(m.Init(&c));

    ChunkMapping m2 = m;

    ASSERT_EQ(m2.data_address(), Storage::ILLEGAL_STORAGE_ADDRESS);
    ASSERT_TRUE(m2.chunk() == &c);
    ASSERT_EQ(m2.fingerprint_size(), sizeof(fp));
    ASSERT_EQ(m2.chunk()->size(), 8U * 1024);
}

}
}
