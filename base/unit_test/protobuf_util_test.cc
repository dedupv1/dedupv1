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

#include <string>
#include <base/protobuf_util.h>
#include "dedupv1_base.pb.h"
#include <test_util/log_assert.h>
#include <base/logging.h>
#include <base/strutil.h>

using std::string;
using dedupv1::base::SerializeSizedMessageToString;
using dedupv1::base::SerializeSizedMessageCached;
using dedupv1::base::SerializeSizedMessage;
using dedupv1::base::ParseSizedMessage;
using dedupv1::base::Option;

LOGGER("ProtobufTest");

/**
 * Tests for the protobuf utilites functions
 */
class ProtobufUtilTest : public testing::TestWithParam< ::std::tr1::tuple<bool, bool> > {
protected:
    USE_LOGGING_EXPECTATION();

    bool src_checksum;
    bool des_checksum;

    virtual void SetUp() {
        src_checksum = ::std::tr1::get<0>(GetParam());
        des_checksum = ::std::tr1::get<1>(GetParam());
    }
};

TEST_P(ProtobufUtilTest, EmptyMessage) {
    FixedIndexMetaData data;

    byte buffer[1024];
    Option<size_t> r = SerializeSizedMessage(data, buffer, 1024, src_checksum);
    ASSERT_TRUE(r.valid());

    FixedIndexMetaData data2;
    ASSERT_TRUE(ParseSizedMessage(&data2, buffer, 1024, des_checksum).valid());
}

TEST_P(ProtobufUtilTest, FilledMessage) {
    FixedIndexMetaData data;
    data.set_file_count(10);
    data.set_size(1024);
    data.set_width(4);

    byte buffer[1024];
    Option<size_t> r = SerializeSizedMessage(data, buffer, 1024, src_checksum);
    ASSERT_TRUE(r.valid());

    FixedIndexMetaData data2;
    ASSERT_TRUE(ParseSizedMessage(&data2, buffer, 1024, des_checksum).valid());
    ASSERT_EQ(data.width(), data2.width());
    ASSERT_EQ(data.size(), data2.size());
    ASSERT_EQ(data.file_count(), data2.file_count());
}

TEST_P(ProtobufUtilTest, FilledCachedMessage) {
    FixedIndexMetaData data;
    data.set_file_count(10);
    data.set_size(1024);
    data.set_width(4);

    byte buffer[1024];
    data.ByteSize();
    Option<size_t> r = SerializeSizedMessageCached(data, buffer, 1024, src_checksum);
    ASSERT_TRUE(r.valid());

    FixedIndexMetaData data2;
    ASSERT_TRUE(ParseSizedMessage(&data2, buffer, 1024, des_checksum).valid());
    ASSERT_EQ(data.width(), data2.width());
    ASSERT_EQ(data.size(), data2.size());
    ASSERT_EQ(data.file_count(), data2.file_count());
}

TEST_P(ProtobufUtilTest, FilledToLargeMessage) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    FixedIndexMetaData data;
    data.set_file_count(10);
    data.set_size(1024);
    data.set_width(4);

    byte buffer[1024];
    data.ByteSize();
    Option<size_t> r = SerializeSizedMessageCached(data, buffer, 8, src_checksum);
    ASSERT_FALSE(r.valid());
}

TEST_P(ProtobufUtilTest, StringVersion) {
    FixedIndexMetaData data;
    data.set_file_count(10);
    data.set_size(1024);
    data.set_width(4);

    byte buffer[1024];
    data.ByteSize();
    Option<size_t> r = SerializeSizedMessageCached(data, buffer, 1024, src_checksum);
    ASSERT_TRUE(r.valid());

    string s;
    ASSERT_TRUE(SerializeSizedMessageToString(data, &s, src_checksum));
    ASSERT_EQ(r.value(), s.size());
    ASSERT_TRUE(memcmp(buffer, s.data(), r.value()) == 0);
}

INSTANTIATE_TEST_CASE_P(ProtobufUtil,
    ProtobufUtilTest,
    ::testing::Combine(
        ::testing::Bool(),
        ::testing::Bool()
        ));
