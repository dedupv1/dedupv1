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

#include <base/base64.h>
#include <base/strutil.h>
#include <test_util/log_assert.h>

using std::string;
using dedupv1::base::ToBase64;
using dedupv1::base::FromBase64;
using dedupv1::base::make_bytestring;
using dedupv1::base::strutil::ToHexString;

class Base64Test : public testing::Test {
private:
    USE_LOGGING_EXPECTATION();
};

TEST_F(Base64Test, Encode) {
    // an example (modified from German Wikipedia
    string s = "Polyfon zwitschernd aßen Maexchens Voegel Rueben, Joghurt und Quark";
    bytestring bs = make_bytestring(reinterpret_cast<const byte*>(s.data()), s.size());

    string encoded = ToBase64(bs);
    bytestring bs2 = FromBase64(encoded);

    ASSERT_EQ(bs.size(), bs2.size());
    ASSERT_TRUE(bs == bs2) << ToHexString(bs.data(), bs.size()) << ", " << ToHexString(bs2.data(), bs2.size());
}
