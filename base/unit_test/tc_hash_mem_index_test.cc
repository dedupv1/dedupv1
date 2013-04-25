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

#include <string>
#include <list>

#include <gtest/gtest.h>

#include <base/base.h>
#include <base/locks.h>
#include <base/tc_hash_index.h>
#include <base/index.h>
#include <base/logging.h>

#include "dedupv1_base.pb.h"

#include "index_test.h"
#include <test_util/log_assert.h>

LOGGER("TCHashMemIndexTest");

namespace dedupv1 {
namespace base {

class TCHashMemIndexTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Index* index_;

    byte buffer_[8192];
    int buffer_size_;

    virtual void SetUp() {
        index_ = NULL;

        buffer_size_ = 8192;
        memset(buffer_, 0, buffer_size_);
    }

    virtual void TearDown() {
        if (index_) {
            delete index_;
        }

    }

    bool ReadWrite(int n) {
        for (int i = 0; i < n; i++) {
            uint64_t key_value = i;

            IntData put_value;
            put_value.set_i(i);

            CHECK(index_->Put(&key_value, sizeof(key_value), put_value), "Put failed");

        }

        for (int i = 0; i < n; i++) {
            uint64_t key_value = i;
            uint64_t value = i;

            IntData get_value;
            CHECK(index_->Lookup(&key_value, sizeof(key_value), &get_value), "Lookup failed");
            CHECK(value == get_value.i(), "Comparison failed");
            return true;
        }
        return true;
    }
};

INSTANTIATE_TEST_CASE_P(TCMemHashIndex,
    IndexTest,
    ::testing::Values("tc-mem-hash;bucket-count=1K",
        "tc-mem-hash;bucket-count=16K"
        ));

TEST_F(TCHashMemIndexTest, StartWithBucketCountZero) {
    index_ = Index::Factory().Create("tc-mem-hash");
    ASSERT_TRUE(index_->Start(StartContext()));
    ASSERT_TRUE(ReadWrite(16));
}

}
}

