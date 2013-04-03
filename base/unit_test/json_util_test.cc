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
#include <base/json_util.h>
#include <test_util/log_assert.h>

using dedupv1::base::AsJson;
using dedupv1::base::Option;

class JsonUtilTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
};

TEST_F(JsonUtilTest, NoJson) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(1);

    Option<Json::Value> o = AsJson("{[\"a\",\"b\",\"c\",]}");
    ASSERT_FALSE(o.valid());
}

TEST_F(JsonUtilTest, SimpleJson) {
    Option<Json::Value> o = AsJson("{\"a\": [\"a\",\"b\",\"c\"]}");

    ASSERT_TRUE(o.valid());
    Json::Value root = o.value();
    ASSERT_TRUE(root.isObject());
    ASSERT_TRUE(root.isMember("a"));
    ASSERT_TRUE(root["a"].isArray());
}
