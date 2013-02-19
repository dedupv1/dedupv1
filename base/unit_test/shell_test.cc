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

#include <base/shell.h>
#include <test/log_assert.h>
#include <base/logging.h>

using dedupv1::base::Option;
using std::pair;

LOGGER("ShellTest");

class ShellTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(ShellTest, Simple) {
    Option<pair<int, bytestring> > b = dedupv1::base::RunUntilCompletion("echo abc");
    ASSERT_TRUE(b.valid());
    ASSERT_EQ(b.value().second[0], 'a');
    ASSERT_EQ(b.value().second[1], 'b');
    ASSERT_EQ(b.value().second[2], 'c');
    ASSERT_EQ(b.value().second[3], '\n');
}

TEST_F(ShellTest, NonExisting) {
    Option<pair<int, bytestring> > b = dedupv1::base::RunUntilCompletion("foobarXZY");
    ASSERT_TRUE(b.valid());
    ASSERT_FALSE(b.value().first == 0);
}

TEST_F(ShellTest, ProcessError) {
    Option<pair<int, bytestring> > b = dedupv1::base::RunUntilCompletion("ls -l /xzya");
    ASSERT_TRUE(b.valid());
    ASSERT_FALSE(b.value().first == 0);
}