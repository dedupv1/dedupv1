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

#include <test/log_assert.h>
#include <base/logging.h>

#include <gtest/gtest.h>

using dedupv1::test::Level;
using dedupv1::test::ERROR;
using dedupv1::test::WARN;

LOGGER("LogAssertTest");
MAKE_LOGGER(test_logger, "Test");

class LogAssertTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
};

TEST_F(LogAssertTest, Default) {
    __log_expect__set__.SkipReporting();
    ASSERT_TRUE(__log_expect__set__.Check());
}

TEST_F(LogAssertTest, DefaultWithError) {
    ERROR_LOGGER(test_logger, "Test message");

    __log_expect__set__.SkipReporting();
    ASSERT_FALSE(__log_expect__set__.Check());
}

TEST_F(LogAssertTest, NoDefaultWithError) {
    EXPECT_LOGGING(ERROR).Once();

    ERROR_LOGGER(test_logger, "Test message");

    __log_expect__set__.SkipReporting();
    ASSERT_TRUE(__log_expect__set__.Check());
}

TEST_F(LogAssertTest, Never) {
    EXPECT_LOGGING(ERROR).Never();

    __log_expect__set__.SkipReporting();
    ASSERT_TRUE(__log_expect__set__.Check());
}

TEST_F(LogAssertTest, NeverWithError) {
    EXPECT_LOGGING(ERROR).Never();

    ERROR_LOGGER(test_logger, "Test message");

    __log_expect__set__.SkipReporting();
    ASSERT_FALSE(__log_expect__set__.Check());
}

TEST_F(LogAssertTest, Regex) {
    EXPECT_LOGGING("Container Mismatch").Once().Level(WARN);

    __log_expect__set__.SkipReporting();
    ASSERT_FALSE(__log_expect__set__.Check()) << "Container mismatch not logged";
}

TEST_F(LogAssertTest, RegexWithMessage) {
    EXPECT_LOGGING("Container Mismatch.*").Once().Level(WARN);

    WARNING_LOGGER(test_logger, "Container Mismatch xy");

    __log_expect__set__.SkipReporting();
    ASSERT_TRUE(__log_expect__set__.Check()) << "Container mismatch is logged";
}
