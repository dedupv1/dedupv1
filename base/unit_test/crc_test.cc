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
#include <base/crc32.h>
#include <test/log_assert.h>

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>

using std::string;
using dedupv1::base::crc;
using dedupv1::base::CRC;

/**
 * Tests the crc calculation class
 */
class CRCTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(CRCTest, CalcAndCompare) {
    byte buffer[2][1024];
    string crc_value[2];

    for (int i = 0; i < 2; i++) {
        memset(buffer[i], i, 1024);
    }
    for (int i = 0; i < 2; i++) {
        crc_value[i] = crc(buffer[i], 1024, 16);
    }
    for (int i = 0; i < 2; i++) {
        string crc2 = crc(buffer[i], 1024, 16);
        ASSERT_EQ(crc2, crc_value[i]);

    }
    ASSERT_NE(crc_value[0], crc_value[1]);
}

TEST_F(CRCTest, ClassCalcAndCompare) {
    byte buffer[2][1024];
    string crc_value[2];

    for (int i = 0; i < 2; i++) {
        memset(buffer[i], i, 1024);
    }
    for (int i = 0; i < 2; i++) {
        CRC crc;
        crc.Update(buffer[i], 1024);
        crc_value[i] = crc.GetValue(16);
    }

    for (int i = 0; i < 2; i++) {
        CRC crc;
        crc.Update(buffer[i], 1024);
        string crc2 = crc.GetValue(16);
        ASSERT_EQ(crc2, crc_value[i]);

    }
    ASSERT_NE(crc_value[0], crc_value[1]);
}

TEST_F(CRCTest, ClassGetValueLow) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    byte buffer[1024];
    memset(buffer, 1, 1024);

    CRC crc;
    crc.Update(buffer, 1024);
    ASSERT_EQ(crc.GetValue(4).size(), 0U) << "value size is too low to return valid result";
}

TEST_F(CRCTest, ClassGetValueHigh) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    byte buffer[1024];
    memset(buffer, 1, 1024);

    CRC crc;
    crc.Update(buffer, 1024);
    ASSERT_EQ(crc.GetValue(1024).size(), 0U) << "value size is too large to return a valid result";
}

TEST_F(CRCTest, PiecewiseUpdate) {
    byte buffer[2048];
    memset(buffer, 2, 2048);
    CRC crc;
    crc.Update(buffer, 2048);

    std::string checksum_whole = crc.GetValue(8);

    CRC crc2;
    crc2.Update(buffer, 1024);
    crc2.Update(buffer + 1024, 1024);
    std::string checksum_piecewise = crc2.GetValue(8);

    EXPECT_EQ(checksum_whole, checksum_piecewise);
}
