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
#include <list>

#include <base/base.h>
#include <base/locks.h>
#include <base/index.h>
#include <base/tc_hash_index.h>

#include "index_test.h"
#include <test_util/log_assert.h>

using std::string;

namespace dedupv1 {
namespace base {

class TCHashIndexTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Index* index_;

    byte buffer_[8192];
    int buffer_size_;

    virtual void SetUp() {
        index_ = Index::Factory().Create("tc-disk-hash");
        ASSERT_TRUE(index_);

        ASSERT_TRUE(index_->SetOption("filename", "work/tc_test_data"));

        dedupv1::StartContext start_context;
        ASSERT_TRUE(index_->Start(start_context));

        buffer_size_ = 8192;
        memset(buffer_, 0, buffer_size_);
    }

    virtual void TearDown() {
        if (index_) {
            delete index_;
        }

    }
};

INSTANTIATE_TEST_CASE_P(TCHashIndex,
    IndexTest,
    ::testing::Values(string("tc-disk-hash;filename=work/tc_test_data1;filename=work/tc_test_data2")
        ));

}
}

