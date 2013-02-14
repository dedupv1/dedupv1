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

#include <base/handover_store.h>
#include <base/timer.h>
#include <test/log_assert.h>

using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::HandoverStore;
using dedupv1::base::timed_bool;
using dedupv1::base::TIMED_FALSE;
using dedupv1::base::TIMED_TRUE;
using dedupv1::base::TIMED_TIMEOUT;
using dedupv1::base::Walltimer;
/**
 * Tests for the HandoverStore class
 */
class HandoverStoreTest : public testing::Test {
private:
    USE_LOGGING_EXPECTATION();
public:
};

TEST_F(HandoverStoreTest, Nothing) {
    HandoverStore<int> store;
}

TEST_F(HandoverStoreTest, Simple) {
    HandoverStore<int> store;

    int value = 0;
    timed_bool tb = store.Get(&value, 5);
    ASSERT_EQ(tb, TIMED_TIMEOUT);

    tb = store.Put(10, 1);
    ASSERT_EQ(tb, TIMED_TRUE);

    tb = store.Get(&value, 1);
    ASSERT_EQ(tb, TIMED_TRUE);
    ASSERT_EQ(value, 10);
}

timed_bool PutValue(HandoverStore<int>* hs, int value, int delay) {
    sleep(delay);
    return hs->Put(value, 10);
}

TEST_F(HandoverStoreTest, Compex2) {
    HandoverStore<int> store;

    timed_bool tb = store.Put(10, 1);
    ASSERT_EQ(tb, TIMED_TRUE);

    Thread<timed_bool> t1(NewRunnable(&PutValue, &store, 10, 2), "hs-test");
    ASSERT_TRUE(t1.Start());

    sleep(15);

    Walltimer t;
    int value = 0;
    tb = store.Get(&value, 5);
    ASSERT_EQ(tb, TIMED_TRUE);
    ASSERT_EQ(value, 10);
    ASSERT_LE(t.GetTime(), 3000.0);

    timed_bool t1_result;
    ASSERT_TRUE(t1.Join(&t1_result));
    ASSERT_EQ(t1_result, TIMED_TIMEOUT);
}

TEST_F(HandoverStoreTest, Compex) {
    HandoverStore<int> store;

    Thread<timed_bool> t1(NewRunnable(&PutValue, &store, 10, 2), "hs-test");
    ASSERT_TRUE(t1.Start());

    Walltimer t;
    int value = 0;
    timed_bool tb = store.Get(&value, 5);
    ASSERT_EQ(tb, TIMED_TRUE);
    ASSERT_EQ(value, 10);
    ASSERT_LE(t.GetTime(), 3000.0);

    timed_bool t1_result;
    ASSERT_TRUE(t1.Join(&t1_result));
    ASSERT_TRUE(t1_result);
}
