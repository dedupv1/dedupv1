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

#include <base/hash_index.h>
#include <base/index.h>

#include <test_util/log_assert.h>

#include "index_test.h"

#include <gtest/gtest.h>

namespace dedupv1 {
namespace base {

class HashIndexTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Index* index;

    byte buffer[8192];
    int buffer_size;

    virtual void SetUp() {

        index = Index::Factory().Create("mem-chained-hash");
        ASSERT_TRUE(index);

        ASSERT_TRUE(index->SetOption("buckets", "1K"));
        ASSERT_TRUE(index->SetOption("sub-buckets", "1K"));
        ASSERT_TRUE(index->Start(StartContext()));

        buffer_size = 8192;
        memset(buffer, 0, buffer_size);
    }

    virtual void TearDown() {
        if (index) {
          delete index;
        }
    }
};

INSTANTIATE_TEST_CASE_P(HashIndex,
    IndexTest,
    ::testing::Values(
        "mem-chained-hash;buckets=1K;sub-buckets=1K",
        "mem-chained-hash;buckets=1K;sub-buckets=2K",
        "mem-chained-hash;buckets=1K;sub-buckets=4K",
        "mem-chained-hash;buckets=2K;sub-buckets=4K",
        "mem-chained-hash;buckets=2K;sub-buckets=8K"
        ));

}
}
