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

#include <test_util/log_assert.h>
#include "dedupv1d.pb.h"
#include "dedupv1d_target.h"

namespace dedupv1d {

class Dedupv1dTargetTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(Dedupv1dTargetTest, Ctor) {
    Dedupv1dTarget target(true);

    ASSERT_EQ("", target.name());

    TargetInfoData data;
    data.set_target_name("iqn.2010.05:example");

    ASSERT_TRUE(target.ParseFrom(data));
    ASSERT_EQ("iqn.2010.05:example", target.name());
}

TEST_F(Dedupv1dTargetTest, SerializeParse) {
    Dedupv1dTarget target(true);
    ASSERT_TRUE(target.SetOption("tid", "2"));
    ASSERT_TRUE(target.SetOption("name", "iqn.2010.05:example"));
    ASSERT_EQ("iqn.2010.05:example", target.name());
    ASSERT_EQ(2, target.tid());

    TargetInfoData data;
    ASSERT_TRUE(target.SerializeTo(&data));

    Dedupv1dTarget target2(true);
    ASSERT_TRUE(target2.ParseFrom(data));
    ASSERT_EQ("iqn.2010.05:example", target2.name());
    ASSERT_EQ(2, target2.tid());
}

TEST_F(Dedupv1dTargetTest, IllegalName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    Dedupv1dTarget target(true);

    ASSERT_FALSE(target.SetOption( "name", ""));
    ASSERT_FALSE(target.SetOption( "name", "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234")); // 224
    ASSERT_FALSE(target.SetOption( "name", "tar\u00f6get"));
    ASSERT_FALSE(target.SetOption( "name", "tar+get"));
    ASSERT_FALSE(target.SetOption( "name", "tar$get"));
    ASSERT_FALSE(target.SetOption( "name", "tar\u0040get"));
    ASSERT_FALSE(target.SetOption( "name", "tar get"));
    ASSERT_FALSE(target.SetOption( "name", "tar_get"));
    ASSERT_FALSE(target.SetOption( "name", "Target"));
    ASSERT_TRUE(target.SetOption( "name", "a"));
    ASSERT_TRUE(target.SetOption( "name", "this.ismy2ndtarget"));
    ASSERT_TRUE(target.SetOption( "name", "thisismy-2ndtarget"));
    ASSERT_TRUE(target.SetOption( "name", "thisismy2nd:target"));
    ASSERT_TRUE(target.SetOption( "name", "this.ismy-2nd:target"));
    ASSERT_TRUE(target.SetOption( "name", "iqn.2001-04.com.example:storage:diskarrays-sn-a8675309"));
    ASSERT_TRUE(target.SetOption( "name", "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123")); // 223
}

}
