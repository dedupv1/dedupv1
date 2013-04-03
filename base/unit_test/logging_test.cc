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
#include <base/fileutil.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <test_util/log_assert.h>

LOGGER("LoggingTest");

using dedupv1::base::strutil::Contains;

bool CheckFunc(bool v) {
    CHECK(v, "Check failed");
    return true;
}

int CheckReturnFunc(bool v, int v2) {
    CHECK_RETURN(v, v2, "Check failed");
    return 0;
}

class LoggingTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(LoggingTest, Check) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("Check failed").Once();

    ASSERT_TRUE(CheckFunc(true));
    ASSERT_FALSE(CheckFunc(false));
}

TEST_F(LoggingTest, CheckReturn) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("Check failed").Once();

    ASSERT_EQ(CheckReturnFunc(true, 10), 0);
    ASSERT_EQ(CheckReturnFunc(false, 10), 10);
}

TEST_F(LoggingTest, FileBasename) {
    ASSERT_STREQ(file_basename("a/b"), "b");
    ASSERT_STREQ(file_basename("b"), "b");
    ASSERT_STREQ(file_basename(""), "");
}
