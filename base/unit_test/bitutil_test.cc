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

#include <base/bitutil.h>
#include <base/logging.h>
#include <test_util/log_assert.h>

LOGGER("BitUtilTest");

using dedupv1::base::bit_clear;
using dedupv1::base::bit_test;
using dedupv1::base::bit_set;
using dedupv1::base::bits;

/**
 * Tests that the bit manipulation functions work as expected
 */
class BitUtilTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(BitUtilTest, BitTest) {
    uint32_t data = 0;
    for (int i = 0; i < 32; i++) {
        ASSERT_FALSE(bit_test(data, i));
    }
}

TEST_F(BitUtilTest, BitSetAndClear) {
    uint32_t data = 0;
    bit_set(&data, 10);
    ASSERT_TRUE(bit_test(data, 10));

    bit_clear(&data, 10);
    ASSERT_FALSE(bit_test(data, 10));
}

TEST_F(BitUtilTest, BitSetAndClear64) {
    uint64_t data = 0;
    bit_set(&data, 45);

    ASSERT_TRUE(bit_test(data, 45));
    for (int i = 0; i < sizeof(data) * 8; i++) {
        if (i != 45) {
            ASSERT_FALSE(bit_test(data, i));
        }
    }
    bit_clear(&data, 45);
    ASSERT_FALSE(bit_test(data, 45));
}

TEST_F(BitUtilTest, Bits) {
    ASSERT_EQ(0, bits(0));
    ASSERT_EQ(0, bits(1));
    ASSERT_EQ(1, bits(2));
    ASSERT_EQ(2, bits(3));
    ASSERT_EQ(2, bits(4));
    ASSERT_EQ(3, bits(5));
    ASSERT_EQ(9, bits(512));
    ASSERT_EQ(10, bits(768));
    ASSERT_EQ(10, bits(1024));
}
