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

#include <base/semaphore.h>
#include <base/timer.h>

#include <test_util/log_assert.h>

using dedupv1::base::Semaphore;
using dedupv1::base::Walltimer;

class SemaphoreTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Semaphore* s;

    virtual void SetUp() {
        s = new Semaphore(0);
        ASSERT_TRUE(s);
    }

    virtual void TearDown() {
        delete s;
        s = NULL;
    }
};

TEST_F(SemaphoreTest, Create) {

}

TEST_F(SemaphoreTest, WaitPost) {
    ASSERT_TRUE(s->Post());
    ASSERT_TRUE(s->Wait());
    ASSERT_TRUE(s->Post());
}

TEST_F(SemaphoreTest, TryWait) {
    bool locked = false;

    ASSERT_TRUE(s->TryWait(&locked));
    ASSERT_FALSE(locked);

    ASSERT_TRUE(s->Post());
    ASSERT_TRUE(s->TryWait(&locked));
    ASSERT_TRUE(locked);

    ASSERT_TRUE(s->Post());
}
