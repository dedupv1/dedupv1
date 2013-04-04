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

#include <base/bloom_set.h>
#include <base/logging.h>
#include <test_util/log_assert.h>

#include <tbb/tick_count.h>

using dedupv1::base::BloomSet;

LOGGER("BloomSetTest");

/**
 * Tests that the bloom set
 */
class BloomSetTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    BloomSet* bloom_set;

    virtual void SetUp() {
        bloom_set = new BloomSet(16 * 1024, 4);
        ASSERT_TRUE(bloom_set);
        ASSERT_TRUE(bloom_set->Init());
    }

    virtual void TearDown() {
        delete bloom_set;
    }
};

TEST_F(BloomSetTest, Adding) {
    uint64_t i = 0;
    for (i = 0; i < 1024; i++) {
        ASSERT_TRUE(bloom_set->Put((byte *) &i, sizeof(i)));
    }
}

TEST_F(BloomSetTest, ExistingTesting) {
    uint64_t i = 0;
    for (i = 0; i < 1024; i++) {
        ASSERT_TRUE(bloom_set->Put((byte *) &i, sizeof(i)));
    }

    for (i = 0; i < 1024; i++) {
        ASSERT_EQ(bloom_set->Contains((byte *) &i, sizeof(i)), dedupv1::base::LOOKUP_FOUND);
    }
}

TEST_F(BloomSetTest, NotExistingTesting) {
    uint64_t i = 0;
    for (i = 0; i < 1024; i++) {
        ASSERT_TRUE(bloom_set->Put((byte *) &i, sizeof(i)));
    }

    int failures = 0;
    for (i = 0; i < 1024; i++) {
        uint64_t value = 1024 + (i * 2);
        if (bloom_set->Contains((byte *) &value, sizeof(value)) != dedupv1::base::LOOKUP_NOT_FOUND) {
            failures++;
        }
    }
    ASSERT_LE(failures, 8);
}

TEST_F(BloomSetTest, CapacityConstructor) {
    delete bloom_set;
    bloom_set = NULL;

    bloom_set = BloomSet::NewOptimizedBloomSet(1024 * 1024, 0.01);
    ASSERT_TRUE(bloom_set);

    INFO("k " << (int) bloom_set->hash_count() << ", bits " << bloom_set->size());
}

TEST_F(BloomSetTest, Performance) {
    delete bloom_set;
    bloom_set = NULL;
    bloom_set = new BloomSet(1024 * 1024, 4);
    ASSERT_TRUE(bloom_set);
    ASSERT_TRUE(bloom_set->Init());

    tbb::tick_count start = tbb::tick_count::now();
    uint32_t count = 1024 * 1024;
    for (uint64_t i = 0; i < count; i++) {
        ASSERT_TRUE(bloom_set->Put((byte *) &i, sizeof(i)));
    }
    tbb::tick_count end = tbb::tick_count::now();

    INFO("" << (end - start).seconds() * 1000 << "ms");
}
