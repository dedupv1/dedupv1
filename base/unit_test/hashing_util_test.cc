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

#include <base/hashing_util.h>
#include <base/logging.h>
#include <test/log_assert.h>

LOGGER("HashingUtilTest");

using std::string;

namespace dedupv1 {
namespace base {

typedef uint64_t (*hash_func)(const void*, size_t);

class HashingUtilTest : public testing::TestWithParam<hash_func> {
protected:
    USE_LOGGING_EXPECTATION();

    hash_func f;

    virtual void SetUp() {
        f = GetParam();
        ASSERT_TRUE(f);
    }

    virtual void TearDown() {
    }
};

TEST_P(HashingUtilTest, Empty) {
    uint64_t data = 0;
    uint64_t h = f(&data, 0);
    UNUSED(h);
}

TEST_P(HashingUtilTest, String) {
    string s = "dedupv1-4-test";
    uint64_t h = f(s.data(), s.size());
    UNUSED(h);
}

TEST_P(HashingUtilTest, Distribution) {
    size_t data_size = 1000;
    uint64_t data[data_size];
    for (size_t i = 0; i < data_size; i++) {
        data[i] = i;
    }
    uint64_t values[data_size];
    uint32_t bucket[4];
    bucket[0] = bucket[1] = bucket[2] = bucket[3] = 0;

    for (size_t i = 0; i < data_size; i++) {
        values[i] = f(&data[i], sizeof(data[i]));
        // DEBUG("" << data[i] << " => " << values[i] << " => " << (values[i] % 4));
        bucket[values[i] % 4]++;
    }

    for (int i = 0; i < 4; i++) {
        DEBUG("" << i << " => " << bucket[i]);
    }
}

INSTANTIATE_TEST_CASE_P(HashingUtil,
    HashingUtilTest,
    ::testing::Values(bj_hash));

}
}
