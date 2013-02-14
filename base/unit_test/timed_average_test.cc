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
#include <base/timed_average.h>
#include <test/log_assert.h>
#include <pthread.h>

using dedupv1::base::TimedAverage;

class TimedAverageTest: public testing::Test {
    protected:
        USE_LOGGING_EXPECTATION();
};

TEST_F(TimedAverageTest, Init)
{
    TimedAverage<uint16_t, 60> ta;
    ASSERT_EQ(ta.GetAverage(), 0.0);
}

TEST_F(TimedAverageTest, OneValueOuttimed)
{
    TimedAverage<uint16_t, 1> ta;
    ta.Set(5);
    sleep(1);
    ASSERT_EQ(ta.GetAverage(), 5.0);
}

TEST_F(TimedAverageTest, BigValOuttimed)
{
    TimedAverage<uint16_t, 1> ta;
    ta.Set(1000);
    ta.Set(1);
    sleep(1);
    ASSERT_EQ(ta.GetAverage(), 1.0);
}

TEST_F(TimedAverageTest, SeveralValues)
{
    TimedAverage<uint16_t, 5> ta;
    ta.Set(1000);
    sleep(1);
    ta.Set(500);
    sleep(1);
    ta.Set(750);
    sleep(1);
    ta.Set(600);
    sleep(1);
    ta.Set(900);
    sleep(1);
    double average = ta.GetAverage();
    ASSERT_NEAR(average, 750.0, 250.0);
}

TEST_F(TimedAverageTest, IncValue)
{
    TimedAverage<uint16_t, 1> ta;
    ta.Set(5);
    ta.Inc();
    ASSERT_EQ(ta.GetValue(), 6.0);
    sleep(1);
    ASSERT_EQ(ta.GetAverage(), 6.0);
}

TEST_F(TimedAverageTest, DecValue)
{
    TimedAverage<uint16_t, 1> ta;
    ta.Set(5);
    ta.Dec();
    ASSERT_EQ(ta.GetValue(), 4.0);
    sleep(1);
    ASSERT_EQ(ta.GetAverage(), 4.0);
}
