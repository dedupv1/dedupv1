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

#include <gtest/gtest.h>

#include <base/index.h>
#include <base/tc_btree_index.h>
#include "index_test.h"
#include <test_util/log_assert.h>
#include <base/logging.h>

#include <tcbdb.h>

#include <map>

using std::map;

LOGGER("TCBTreeIndexTest");

namespace dedupv1 {
namespace base {

class TCBTreeIndexTest : public testing::TestWithParam<std::string> {
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
        delete index_;
        index_ = NULL;
    }
};

INSTANTIATE_TEST_CASE_P(TCBTreeIndex,
    IndexTest,
    ::testing::Values("tc-disk-btree;filename=work/data/tc_test_data",
        "tc-disk-btree;filename=work/tc_test_data;compression=deflate",
        "tc-disk-btree;filename=work/tc_test_data;mem-mapped-size=1024",
        "tc-disk-btree;filename=work/tc_test_data;defrag=10",
        "tc-disk-btree;filename=work/tc_test_data1;filename=work/tc_test_data2"
        ));

INSTANTIATE_TEST_CASE_P(TCBTreeIndex,
    TCBTreeIndexTest,
    ::testing::Values("tc-disk-btree;filename=work/tc_test_data",
        "tc-disk-btree;filename=work/tc_test_data;compression=deflate",
        "tc-disk-btree;filename=work/tc_test_data;mem-mapped-size=1024",
        "tc-disk-btree;filename=work/tc_test_data;defrag=10",
        "tc-disk-btree;filename=work/tc_test_data1;filename=work/tc_test_data2"
        ));

TEST_F(TCBTreeIndexTest, GetBTree) {
    index_ = Index::Factory().Create("tc-disk-btree");
    ASSERT_TRUE(index_);

    ASSERT_TRUE(index_->SetOption("filename", "work/hash_test_data1"));
    ASSERT_TRUE(index_->SetOption("filename", "work/hash_test_data2"));
    ASSERT_TRUE(index_->SetOption("filename", "work/hash_test_data3"));
    ASSERT_TRUE(index_->SetOption("filename", "work/hash_test_data4"));
    ASSERT_TRUE(index_->Start(StartContext()));

    TCBTreeIndex* btree = dynamic_cast<TCBTreeIndex*>(index_);
    ASSERT_TRUE(btree);

    uint64_t id[100];
    TCBDB* db[100];
    for (int i = 0; i < 100; i++) {
        id[i] = i;
        db[i] = NULL;
    }
    map<int64_t, int> bucket;
    for (int i = 0; i < 100; i++) {
        db[i] = btree->GetBTree(&id[i], sizeof(id[i])).first;

        bucket[reinterpret_cast<int64_t>(db[i])]++;
    }

    map<int64_t, int>::iterator j;
    for (j = bucket.begin(); j != bucket.end(); j++) {
        DEBUG("" << j->first << " => " << j->second);
    }
}

}
}

