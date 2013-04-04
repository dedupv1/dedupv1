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

#include <base/base.h>
#include <base/memchunk.h>
#include <test_util/log_assert.h>

using dedupv1::base::Memchunk;

class MemChunkTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    byte buffer[16 * 1024];
    Memchunk* mc;

    virtual void SetUp() {
        mc = NULL;
        for (int i = 0; i < 16; i++) {
            memset(buffer + (i * 1024), i, 1024);
        }
    }

    virtual void TearDown() {
        if (mc) {
            delete mc;
            mc = NULL;
        }
    }
};

TEST_F(MemChunkTest, CreateWithSize) {
    mc = new Memchunk(1024);
    ASSERT_TRUE(mc);
    ASSERT_EQ(mc->size(), 1024U);
    ASSERT_EQ(memcmp(mc->value(), buffer, 1024),0);
}

TEST_F(MemChunkTest, CreateWithoutSize) {
    mc = new Memchunk(0);
    ASSERT_TRUE(mc);
    ASSERT_EQ(mc->size(), 0U);
    ASSERT_TRUE(mc->value() == NULL);
}

TEST_F(MemChunkTest, CloseWithoutFree) {
    mc = new Memchunk(NULL, 1024, false);
    ASSERT_TRUE(mc);
    ASSERT_EQ(mc->size(), 1024U);

    byte* data = (byte *) mc->value();
    ASSERT_TRUE(data);
    delete mc;
    mc = NULL;

    ASSERT_EQ(memcmp(data, buffer, 1024),0);
    delete[] data;
}

TEST_F(MemChunkTest, Realloc) {
    mc = new Memchunk(1024);
    ASSERT_TRUE(mc);
    memset(mc->value(), 1, 1024);

    ASSERT_TRUE(mc->Realloc(2048));
    ASSERT_EQ(mc->size(), 2048U);
    byte* data = (byte *) mc->value();
    ASSERT_EQ(memcmp(data, buffer + 1024, 1024),0);
    ASSERT_EQ(memcmp(data + 1024, buffer, 1024),0);

    ASSERT_TRUE(mc->Realloc(2048));
    ASSERT_EQ(mc->size(), 2048U);
    data = (byte *) mc->value();
    ASSERT_EQ(memcmp(data, buffer + 1024, 1024),0);
    ASSERT_EQ(memcmp(data + 1024, buffer, 1024),0);

    ASSERT_TRUE(mc->Realloc(512));
    ASSERT_EQ(mc->size(), 512U);
    data = (byte *) mc->value();
    ASSERT_EQ(memcmp(data, buffer + 1024, 512),0);
}

TEST_F(MemChunkTest, ReallocWithoutSize) {
    mc = new Memchunk(0);
    ASSERT_TRUE(mc);
    mc->Realloc(1024);
    ASSERT_EQ(mc->size(), 1024U);
    ASSERT_EQ(memcmp(mc->value(), buffer, 1024),0);
}

TEST_F(MemChunkTest, Checksum) {
    mc = new Memchunk(1024);
    ASSERT_TRUE(mc);
    memset(mc->value(), 1, 1024);

    ASSERT_GT(mc->checksum().size(), 0);
}

TEST_F(MemChunkTest, NewAsCopy) {
    mc = Memchunk::NewAsCopy(buffer + 1024, 1024);
    ASSERT_TRUE(mc);
    ASSERT_EQ(mc->size(), 1024);

    ASSERT_EQ(memcmp(mc->value(), buffer + 1024, 1024), 0);
}
