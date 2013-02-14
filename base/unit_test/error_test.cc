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

#include <base/error.h>
#include <test/log_assert.h>

using dedupv1::base::ErrorContext;

class ErrorContextTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(ErrorContextTest, Simple) {
    ErrorContext ec;
    ASSERT_FALSE(ec.is_fatal());
    ASSERT_FALSE(ec.is_full());
    ASSERT_FALSE(ec.has_checksum_error());

    ASSERT_GT(ec.DebugString().size(), 0);
}

TEST_F(ErrorContextTest, AllSet) {
    ErrorContext ec;
    ec.set_fatal();
    ASSERT_TRUE(ec.is_fatal());

    ec.set_checksum_error();
    ASSERT_TRUE(ec.has_checksum_error());

    ec.set_full();
    ASSERT_TRUE(ec.is_full());

    ASSERT_GT(ec.DebugString().size(), 0);
}
