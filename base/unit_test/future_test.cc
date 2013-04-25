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

#include <base/future.h>
#include <base/option.h>
#include <test_util/log_assert.h>

using dedupv1::base::Option;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::Future;

/**
 * Tests for the future class
 */
class FutureTest : public testing::Test {
private:
    USE_LOGGING_EXPECTATION();
};

/**
 * Do nothing (only tests construction)
 */
TEST_F(FutureTest, Nothing) {
    Future<int>* f = new Future<int>();
    ASSERT_TRUE(f);

    delete f;
}

/**
 * Tests a normal get
 */
TEST_F(FutureTest, Get) {
    Future<int>* f = new Future<int>();
    ASSERT_TRUE(f);

    ASSERT_FALSE(f->is_value_set());

    ASSERT_TRUE(f->Set(10));

    int i = 0;
    ASSERT_TRUE(f->Get(&i));
    ASSERT_EQ(10, i);

    delete f;
}

/**
 * Tests the wait timeout method
 */
TEST_F(FutureTest, Timeout) {
    Future<int>* f = new Future<int>();
    ASSERT_TRUE(f);

    Option<bool> b = f->WaitTimeout(5);
    ASSERT_TRUE(b.valid());
    ASSERT_FALSE(b.value());

    delete f;
}

/**
 * Tests the abort method
 */
TEST_F(FutureTest, Abort) {
    Future<int>* f = new Future<int>();
    ASSERT_TRUE(f);

    ASSERT_TRUE(f->Abort());

    ASSERT_TRUE(f->is_abort());

    delete f;
}

/**
 * Reference counting is hard
 */
TEST_F(FutureTest, RefCount) {
    Future<int>* f = new Future<int>();
    ASSERT_TRUE(f);

    Future<int>* f2 = f->AddRef();
    ASSERT_TRUE(f2);

    delete f;
    ASSERT_FALSE(f2->is_abort()); // there we only test that the test is not seg faulting.
    delete f2;
}
