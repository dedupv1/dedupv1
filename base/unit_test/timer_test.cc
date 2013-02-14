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

#include <base/timer.h>
#include <test/log_assert.h>

using dedupv1::base::Walltimer;

class TimerTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(TimerTest, Simple) {
    Walltimer t;

    sleep(5);
    double d = t.GetTime();

    ASSERT_GE(d, 5000.0 - 10);
}

TEST_F(TimerTest, Multiple) {
    Walltimer t;

    sleep(1);
    double d = t.GetTime();
    ASSERT_GE(d, 1000.0 - 10) << "1st call failed";

    sleep(2);
    d = t.GetTime();
    ASSERT_GE(d, 2000.0 - 10) << "2nd call failed";

    sleep(1);
    d = t.GetTime();
    ASSERT_GE(d, 1000.0 - 10) << "3rd call failed";
}
