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

#include <base/thread.h>
#include <base/runnable.h>

#include <base/barrier.h>

#include <test/log_assert.h>

using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::Barrier;

/**
 * Tests for the barrier class
 */
class BarrierTest : public testing::Test {
private:
    USE_LOGGING_EXPECTATION();
public:
    static bool WaitForBarrier(Barrier* b) {
        return b->Wait();
    }
};

TEST_F(BarrierTest, Nothing) {
    Barrier b(1);
}

TEST_F(BarrierTest, One) {
    Barrier b(1);

    Thread<bool> t1(NewRunnable(&BarrierTest::WaitForBarrier, &b), "barrier-test");

    ASSERT_TRUE(t1.Start());
    bool t1_result = false;
    ASSERT_TRUE(t1.Join(&t1_result));
    ASSERT_TRUE(t1_result);
}

TEST_F(BarrierTest, Two) {
    Barrier b(2);

    Thread<bool> t1(NewRunnable(&BarrierTest::WaitForBarrier, &b), "barrier-test");
    ASSERT_TRUE(t1.Start());

    Thread<bool> t2(NewRunnable(&BarrierTest::WaitForBarrier, &b), "barrier-test");
    ASSERT_TRUE(t2.Start());

    bool t1_result = false;
    ASSERT_TRUE(t1.Join(&t1_result));
    ASSERT_TRUE(t1_result);

    bool t2_result = false;
    ASSERT_TRUE(t2.Join(&t2_result));
    ASSERT_TRUE(t2_result);
}
