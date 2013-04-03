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

#include <base/uuid.h>
#include <base/logging.h>
#include <test_util/log_assert.h>

LOGGER("UUIDTest");

using dedupv1::base::UUID;
using std::string;
using dedupv1::base::Option;

class UUIDTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(UUIDTest, Init) {
    UUID uuid;
    ASSERT_TRUE(uuid.IsNull());
}

TEST_F(UUIDTest, Generate) {
    UUID uuid = UUID::Generate();

    ASSERT_FALSE(uuid.IsNull());
}

TEST_F(UUIDTest, Copy) {
    UUID uuid = UUID::Generate();
    UUID uuid2 = uuid;

    ASSERT_TRUE(uuid.Equals(uuid2));

    UUID uuid3;
    uuid3.CopyFrom(uuid);

    ASSERT_TRUE(uuid.Equals(uuid2));

    UUID uuid4 = UUID::Generate();
    ASSERT_FALSE(uuid.Equals(uuid4));
}

TEST_F(UUIDTest, String) {
    UUID uuid = UUID::Generate();

    string s = uuid.ToString();

    Option<UUID> uuid2 = UUID::FromString(s);
    ASSERT_TRUE(uuid2.valid());
    ASSERT_TRUE(uuid2.value().Equals(uuid));

    Option<UUID> uuid3 = UUID::FromString("891hjk12h3");
    ASSERT_FALSE(uuid3.valid());
}
