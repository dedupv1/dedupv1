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
#include <base/sliding_average.h>
#include <test_util/log_assert.h>

using dedupv1::base::SlidingAverage;
using dedupv1::base::SimpleSlidingAverage;

class SlidingAverageTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(SlidingAverageTest, Init) {
    SlidingAverage sa(4);
    ASSERT_EQ(sa.GetAverage(4), 0.0);
}

TEST_F(SlidingAverageTest, Easy) {
    SlidingAverage sa(4);

    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(sa.Add(i, 4.0));
    }
    for (int i = 4; i < 16; i++) {
        ASSERT_TRUE(sa.Add(i, 4.0));
        ASSERT_EQ(sa.GetAverage(i), 4.0);
    }
}

TEST_F(SlidingAverageTest, AverageWithHoles) {
    SlidingAverage sa(4);

    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(sa.Add(i, 4.0));
    }

    sa.Add(20, 16);
    ASSERT_EQ(sa.GetAverage(20), 4.0);
}

TEST_F(SlidingAverageTest, AddPartial) {
    SlidingAverage sa(4);

    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(sa.Add(i, 4.0));
    }

    sa.Add(20, 8);
    sa.Add(20, 8);
    ASSERT_EQ(sa.GetAverage(20), 4.0);
}

class SimpleSlidingAverageTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(SimpleSlidingAverageTest, Init) {
    SimpleSlidingAverage sa(4);
    ASSERT_EQ(sa.GetAverage(), 0.0);
}

TEST_F(SimpleSlidingAverageTest, Easy) {
    SimpleSlidingAverage sa(4);

    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(sa.Add(4.0));
    }
    for (int i = 4; i < 16; i++) {
        ASSERT_TRUE(sa.Add(4.0));
        ASSERT_EQ(sa.GetAverage(), 4.0);
    }
}

TEST_F(SimpleSlidingAverageTest, AverageWithHoles) {
    SimpleSlidingAverage sa(4);

    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(sa.Add(4.0));
    }
    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(sa.Add(8.0));
    }
    ASSERT_EQ(sa.GetAverage(), 8.0);
}
