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
#include <tbb/atomic.h>

#include <gtest/gtest.h>

#include <base/base.h>

#include <test_util/log_assert.h>

using tbb::atomic;

/**
 * Tests atomic operations
 */
class AtomicTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(AtomicTest, Init) {
    atomic<uint32_t> v;
    v = 2;
    ASSERT_EQ(v, 2);
}

/**
 * Verifies that the assignment to an atomic variable works
 */
TEST_F(AtomicTest, Set) {
    atomic<uint32_t> v;
    v = 2;
    v = 3;
    ASSERT_EQ(v,3);
}

/**
 * Verifies that the assignment also works without an initial assignment
 */
TEST_F(AtomicTest, SetWithoutInit) {
    atomic<uint32_t> v;
    v = 3;
    ASSERT_EQ(v, 3);
}

/**
 * Verifies that an atomic add works
 */
TEST_F(AtomicTest, Add) {
    atomic<uint32_t> v;
    v = 3;
    uint32_t old_v = v.fetch_and_add(2);
    ASSERT_EQ(old_v, 3);
    ASSERT_EQ(v,5);
}

/**
 * Verifies that an atomic increment works as expected
 */
TEST_F(AtomicTest, Inc) {
    atomic<uint32_t> v;

    v = 3;
    uint32_t old_v = v.fetch_and_increment();
    ASSERT_EQ(old_v, 3);
    ASSERT_EQ(v, 4);
}
