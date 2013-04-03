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

#include <base/cache_strategy.h>
#include <test_util/log_assert.h>

using dedupv1::base::LRUCacheStrategy;

class LRUCacheStrategyTest  : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    LRUCacheStrategy<int> cache;

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
};

TEST_F(LRUCacheStrategyTest, OneTouch) {
    ASSERT_EQ(cache.size(), 0U);
    ASSERT_TRUE(cache.Touch(0));
    ASSERT_EQ(cache.size(), 1U);
    int i = -1;
    ASSERT_TRUE(cache.Replace(&i));
    ASSERT_EQ(i, 0);
}

TEST_F(LRUCacheStrategyTest, Touch) {
    ASSERT_TRUE(cache.Touch(0));
    ASSERT_TRUE(cache.Touch(1));
    ASSERT_TRUE(cache.Touch(2));
    ASSERT_TRUE(cache.Touch(4));
    ASSERT_TRUE(cache.Touch(0));
    int i = -1;
    ASSERT_TRUE(cache.Replace(&i));
    ASSERT_EQ(i, 1);
}
