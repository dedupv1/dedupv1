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
#include "dedupv1d_group.h"

namespace dedupv1d {

class Dedupv1dGroupTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(Dedupv1dGroupTest, Ctor) {
    Dedupv1dGroup group(true);

    ASSERT_EQ("", group.name());

    GroupInfoData data;
    data.set_group_name("backup");

    ASSERT_TRUE(group.ParseFrom(data));
    ASSERT_EQ("backup", group.name());
}

TEST_F(Dedupv1dGroupTest, SerializeParse) {
    Dedupv1dGroup group(true);
    ASSERT_TRUE(group.SetOption("name", "backup"));
    ASSERT_EQ("backup", group.name());

    GroupInfoData data;
    ASSERT_TRUE(group.SerializeTo(&data));

    Dedupv1dGroup group2(true);
    ASSERT_TRUE(group2.ParseFrom(data));
    ASSERT_EQ("backup", group2.name());
}

TEST_F(Dedupv1dGroupTest, SerializeParseWithInitiator) {
    Dedupv1dGroup group(true);
    ASSERT_TRUE(group.SetOption("name", "backup"));
    ASSERT_TRUE(group.SetOption("initiator", "iqn.2010"));
    ASSERT_EQ("backup", group.name());

    GroupInfoData data;
    ASSERT_TRUE(group.SerializeTo(&data));

    Dedupv1dGroup group2(true);
    ASSERT_TRUE(group2.ParseFrom(data));
    ASSERT_EQ("backup", group2.name());
    ASSERT_EQ(*group2.initiator_pattern().begin(), "iqn.2010");

    ASSERT_TRUE(group2.AddInitiatorPattern("iqn.2011"));
    ASSERT_EQ(group2.initiator_pattern().size(), 2);

    ASSERT_TRUE(group2.RemoveInitiatorPattern("iqn.2010"));
    ASSERT_EQ(group2.initiator_pattern().size(), 1);
}

TEST_F(Dedupv1dGroupTest, IllegalName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    Dedupv1dGroup group(true);

    ASSERT_FALSE(group.SetOption( "name", ""));
    ASSERT_FALSE(group.SetOption( "name", "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123")); // 513
    ASSERT_FALSE(group.SetOption( "name", "Gr\u00f6oup"));
    ASSERT_FALSE(group.SetOption( "name", "Gr+oup"));
    ASSERT_FALSE(group.SetOption( "name", "Gr$oup"));
    ASSERT_FALSE(group.SetOption( "name", "Gr@oup"));
    ASSERT_FALSE(group.SetOption( "name", "Gr oup"));
    ASSERT_TRUE(group.SetOption( "name", "a"));
    ASSERT_TRUE(group.SetOption( "name", "This.is_my-2nd:Group"));
    ASSERT_TRUE(group.SetOption( "name", "Default_iqn.2001-04.com.example:storage:diskarrays-sn-a8675309"));
    ASSERT_TRUE(group.SetOption( "name", "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012")); // 512
}

}
