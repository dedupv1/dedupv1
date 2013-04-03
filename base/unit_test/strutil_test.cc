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
#include <base/strutil.h>
#include <test_util/log_assert.h>
#include <base/logging.h>

#include <vector>
#include <string>

using std::string;
using std::vector;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::FormatStorageUnit;
using dedupv1::base::strutil::Index;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::IsNumeric;
using dedupv1::base::strutil::Trim;
using dedupv1::base::strutil::Split;
using dedupv1::base::strutil::Join;
using dedupv1::base::strutil::FormatLargeNumber;
using dedupv1::base::strutil::FromHexString;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::EndsWith;
using dedupv1::base::strutil::IsPrintable;
using dedupv1::base::strutil::FriendlySubstr;

LOGGER("StrUtilTest");

class StrUtilTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(StrUtilTest, Atob) {
    ASSERT_TRUE(To<bool>("true").valid());
    ASSERT_TRUE(To<bool>("true").value());

    ASSERT_TRUE(To<bool>("false").valid());
    ASSERT_FALSE(To<bool>("false").value());
}

TEST_F(StrUtilTest, AtobIllegalValue) {
    ASSERT_FALSE(To<bool>("bla").valid());
}

TEST_F(StrUtilTest, Atosu) {
    ASSERT_EQ(1024L, ToStorageUnit("1K").value());
    ASSERT_EQ(1024L, ToStorageUnit("1k").value());
    ASSERT_EQ(1024L * 1024L, ToStorageUnit("1M").value());
    ASSERT_EQ(1024L * 1024L, ToStorageUnit("1m").value());
    ASSERT_EQ(1024L * 1024 * 1024, ToStorageUnit("1G").value());
    ASSERT_EQ(1024L * 1024 * 1024, ToStorageUnit("1g").value());
    ASSERT_EQ(1024L * 1024 * 1024 * 1024, ToStorageUnit("1T").value());
    ASSERT_EQ(1024L * 1024 * 1024 * 1024, ToStorageUnit("1t").value());
}

TEST_F(StrUtilTest, AtosuOtherValues) {
    ASSERT_EQ(2048L, ToStorageUnit("2K").value());
    ASSERT_EQ(16L * 1024, ToStorageUnit("16k").value());
    ASSERT_EQ(64L * 1024 * 1024, ToStorageUnit("64M").value());
    ASSERT_EQ(7L * 1024 * 1024 * 1024, ToStorageUnit("7G").value());
}

TEST_F(StrUtilTest, IllegalAtosuValues) {
    ASSERT_FALSE(ToStorageUnit("2Kblkasd").valid());
    ASSERT_FALSE(ToStorageUnit("asdasd16k").valid());
    ASSERT_FALSE(ToStorageUnit("value").valid());
}

TEST_F(StrUtilTest, FormatOtherValues) {
    ASSERT_EQ(FormatStorageUnit(2048L), "2K");
    ASSERT_EQ(FormatStorageUnit(64L * 1024 * 1024), "64M");
    ASSERT_EQ(FormatStorageUnit(7L * 1024 * 1024 * 1024), "7G");
    ASSERT_EQ(FormatStorageUnit(-536870912), "-512M");
}

TEST_F(StrUtilTest, FormatNegativeStorageValue) {
    ASSERT_EQ(FormatStorageUnit(-536870912), "-512M");
}

TEST_F(StrUtilTest, StrindexSimple) {
    ASSERT_EQ(6, Index("Hello World", "Wo").value());
    ASSERT_FALSE(Index("Hello World", "Bla").valid());
}

TEST_F(StrUtilTest, StrindexIdentical) {
    ASSERT_EQ(0, Index("Ok", "Ok").value());
}

TEST_F(StrUtilTest, To) {
    ASSERT_EQ(-16, To<int32_t>("-16").value());
    ASSERT_EQ(16, To<byte>("16").value());
    ASSERT_FALSE(To<byte>("16hello").valid());

    ASSERT_TRUE(To<int8_t>("13").valid());
    ASSERT_EQ(13, To<int8_t>("13").value());
}

TEST_F(StrUtilTest, IsNumeric) {
    ASSERT_TRUE(IsNumeric("1001238"));
    ASSERT_TRUE(IsNumeric("1"));

    ASSERT_FALSE(IsNumeric("1a"));
    ASSERT_FALSE(IsNumeric("a1"));
    ASSERT_FALSE(IsNumeric("9a"));
    ASSERT_FALSE(IsNumeric("asdasdasd"));
}

TEST_F(StrUtilTest, Trim) {
    ASSERT_EQ(Trim("   Hello"), "Hello");
    ASSERT_EQ(Trim("   Hello        "), "Hello");
    ASSERT_EQ(Trim("Hello   "), "Hello");
    ASSERT_EQ(Trim("Hello"), "Hello");
}

TEST_F(StrUtilTest, TrimEmpty) {
    ASSERT_EQ(Trim("      "), "");
    ASSERT_EQ(Trim(""), "");
}

TEST_F(StrUtilTest, TrimUname) {
    string s = "Linux dedupv1 2.6.28-15-scst #49 SMP Tue Dec 22 13:27:16 CET 2009 x86_64 GNU/Linux\n";
    string s2 = "Linux dedupv1 2.6.28-15-scst #49 SMP Tue Dec 22 13:27:16 CET 2009 x86_64 GNU/Linux";

    ASSERT_EQ(Trim(s), s2);
}

TEST_F(StrUtilTest, Split) {
    string a;
    string b;

    ASSERT_TRUE(Split("filename=work/tc_test_data","=", &a, &b));
    ASSERT_EQ(a, "filename");
    ASSERT_EQ(b, "work/tc_test_data");
}

TEST_F(StrUtilTest, SplitAndJoin) {

    string str = "a;b;c;d;e,f";
    vector<string> components;
    ASSERT_TRUE(Split(str, ";", &components));

    string str2 = Join(components.begin(), components.end(), ";");
    ASSERT_EQ(str, str2);
}

TEST_F(StrUtilTest, FormatLargeNumber) {
    ASSERT_EQ(FormatLargeNumber(0), "0");
    ASSERT_EQ(FormatLargeNumber(123), "123");
    ASSERT_EQ(FormatLargeNumber(123123), "123,123");
    ASSERT_EQ(FormatLargeNumber(1231111), "1,231,111");
}

TEST_F(StrUtilTest, Hex) {
    string s = ToHexString(255);
    ASSERT_EQ("ff", s);
    ASSERT_EQ(255, FromHexString<int>(s));
}

TEST_F(StrUtilTest, EndsWith) {
    ASSERT_FALSE(EndsWith("test", "trash"));
    ASSERT_TRUE(EndsWith("test", "test"));
    ASSERT_TRUE(EndsWith("test", "st"));
    ASSERT_FALSE(EndsWith("test", "te"));
    ASSERT_FALSE(EndsWith("test", "es"));
}

TEST_F(StrUtilTest, ToStringBool) {
    ASSERT_EQ(ToString(true), "true");
    ASSERT_EQ(ToString(false), "false");

    ASSERT_EQ(ToString(To<bool>("true").value()), "true");
    ASSERT_EQ(ToString(To<bool>("false").value()), "false");
}

TEST_F(StrUtilTest, IsPrintable) {
    ASSERT_TRUE(IsPrintable("asb"));
    ASSERT_TRUE(IsPrintable("abs /:\"+#?-"));
    ASSERT_FALSE(IsPrintable("abs \t"));
    ASSERT_FALSE(IsPrintable("asb\basdb")); // backspace in it
}

TEST_F(StrUtilTest, FriendlySubstr) {
    ASSERT_EQ("Hello", FriendlySubstr("Hello World", 0, 5));
    ASSERT_EQ("Hello World", FriendlySubstr("Hello World", 0, 20));
    ASSERT_EQ("Hello World", FriendlySubstr("Hello World", 0, 11, "..."));
    ASSERT_EQ("Hello Worl...", FriendlySubstr("Hello World", 0, 10, "..."));
}
