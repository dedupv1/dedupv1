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

#include <test/log_assert.h>
#include "dedupv1d.pb.h"
#include "dedupv1d_user.h"

namespace dedupv1d {

class Dedupv1dUserTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(Dedupv1dUserTest, Ctor) {
    Dedupv1dUser user(true);

    ASSERT_EQ("", user.name());

    UserInfoData data;
    data.set_user_name("admin1");

    ASSERT_TRUE(user.ParseFrom(data));
    ASSERT_EQ("admin1", user.name());
}

TEST_F(Dedupv1dUserTest, SerializeParse) {
    Dedupv1dUser user(true);
    ASSERT_TRUE(user.SetOption("name", "admin1"));
    ASSERT_EQ("admin1", user.name());

    UserInfoData data;
    ASSERT_TRUE(user.SerializeTo(&data));

    Dedupv1dUser user2(true);
    ASSERT_TRUE(user2.ParseFrom(data));
    ASSERT_EQ("admin1", user2.name());
}

TEST_F(Dedupv1dUserTest, IllegalName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    Dedupv1dUser user(true);

    ASSERT_FALSE(user.SetOption( "name", ""));
    ASSERT_FALSE(user.SetOption( "name", "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123")); // 513
    ASSERT_FALSE(user.SetOption( "name", "U\u00f6ser"));
    ASSERT_FALSE(user.SetOption( "name", "U+ser"));
    ASSERT_FALSE(user.SetOption( "name", "U$ser"));
    ASSERT_FALSE(user.SetOption( "name", "U\u0040ser"));
    ASSERT_FALSE(user.SetOption( "name", "U ser"));
    ASSERT_TRUE(user.SetOption( "name", "a"));
    ASSERT_TRUE(user.SetOption( "name", "This.is_my-2nd:User"));
    ASSERT_TRUE(user.SetOption( "name", "Default_iqn.2001-04.com.example:storage:diskarrays-sn-a8675309"));
    ASSERT_TRUE(user.SetOption( "name", "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012")); // 512
}

}
