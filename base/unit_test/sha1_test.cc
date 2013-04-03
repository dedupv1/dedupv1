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

#include <base/sha1.h>
#include <base/logging.h>
#include <test_util/log_assert.h>

LOGGER("Sha1Test");

using std::string;

class Sha1Test : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(Sha1Test, Calculate) {
    const char* sentence = "Franz jagt im komplett verwahrlosten Taxi quer durch Bayern";
    string r = dedupv1::base::sha1(sentence, strlen(sentence));
    ASSERT_EQ("68ac906495480a3404beee4874ed853a037a7a8f", r);
}
