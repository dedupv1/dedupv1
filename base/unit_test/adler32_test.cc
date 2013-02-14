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
#include <base/adler32.h>
#include <test/log_assert.h>

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>

using std::string;
using dedupv1::base::AdlerChecksum;

/**
 * Tests the adler32 checksumclass
 */
class Adler32Test : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(Adler32Test, CalcAndCompare) {
    byte buffer[2][1024];
    uint32_t crc_value[2];

    for (int i = 0; i < 2; i++) {
        memset(buffer[i], i, 1024);
    }
    for (int i = 0; i < 2; i++) {
        AdlerChecksum adler32;
        crc_value[i] = adler32.Digest(buffer[i], 1024);
    }
    for (int i = 0; i < 2; i++) {
        AdlerChecksum adler32;
        uint32_t crc2 = adler32.Digest(buffer[i], 1024);
        ASSERT_EQ(crc2, crc_value[i]);

    }
    ASSERT_NE(crc_value[0], crc_value[1]);
}

TEST_F(Adler32Test, PiecewiseUpdate) {
    byte buffer[2048];
    memset(buffer, 2, 2048);
    AdlerChecksum adler32;
    adler32.Update(buffer, 2048);

    uint32_t checksum_whole = adler32.checksum();

    AdlerChecksum adler32_2;
    adler32_2.Update(buffer, 1024);
    adler32_2.Update(buffer + 1024, 1024);
    uint32_t checksum_piecewise = adler32_2.checksum();

    EXPECT_EQ(checksum_whole, checksum_piecewise);
}
