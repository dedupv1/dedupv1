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
#include <base/tc_fixed_index.h>
#include <base/logging.h>
#include "index_test.h"
#include <test_util/log_assert.h>

#include "dedupv1_base.pb.h"

#include <tcfdb.h>

LOGGER("TCFixedIndexTest");

using std::string;
using std::map;

namespace dedupv1 {
namespace base {

class TCFixedIndexTest : public testing::TestWithParam<std::string> {
protected:
    USE_LOGGING_EXPECTATION();

    TCFixedIndex* index_;

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
            index_ = NULL;
        }
    }
};

INSTANTIATE_TEST_CASE_P(TCFixedIndex,
    IndexTest,
    ::testing::Values("tc-disk-fixed;filename=work/tc_test_data;width=8K;size=128M",
        "tc-disk-fixed;filename=work/tc_test_data1;filename=work/tc_test_data2;filename=work/tc_test_data3;filename=work/tc_test_data4;size=1G"
        ));

INSTANTIATE_TEST_CASE_P(TCFixedIndex,
    TCFixedIndexTest,
    ::testing::Values("tc-disk-fixed;filename=work/tc_test_data;width=8K;size=128M",
        "tc-disk-fixed;filename=work/tc_test_data1;filename=work/tc_test_data2;filename=work/tc_test_data3;filename=work/tc_test_data4;size=1G"
        ));

TEST_P(TCFixedIndexTest, MaxId) {
    index_ = dynamic_cast<TCFixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index_);

    ASSERT_TRUE(index_->SetOption("size", "1G"));
    ASSERT_TRUE(index_->SetOption("width", "4K"));
    ASSERT_TRUE(index_->Start(StartContext()));

    ASSERT_EQ(index_->GetMaxId(), -1);

    int64_t id = 17;
    IntData value;
    value.set_i(id);
    ASSERT_TRUE(index_->Put(&id, sizeof(id), value));
    ASSERT_EQ(index_->GetMaxId(), 17);

    id = 1023;
    value.set_i(id);
    ASSERT_TRUE(index_->Put(&id, sizeof(id), value));
    ASSERT_EQ(index_->GetMaxId(), 1023);
}

TEST_P(TCFixedIndexTest, MaxId2) {
    index_ = dynamic_cast<TCFixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index_);

    ASSERT_TRUE(index_->Start(StartContext()));

    for (int64_t id = 0; id < 128; id++) {
        IntData value;
        value.set_i(id);
        ASSERT_TRUE(index_->Put(&id, sizeof(id), value));

        int max_id = index_->GetMaxId();
        enum lookup_result r = index_->Lookup(&max_id, sizeof(max_id), NULL);
        ASSERT_EQ(r, LOOKUP_FOUND);

        ASSERT_EQ(index_->GetMaxId(), max_id);

        int next_id = max_id + 1;
        r = index_->Lookup(&next_id, sizeof(next_id), NULL);
        ASSERT_EQ(r, LOOKUP_NOT_FOUND);

        ASSERT_EQ(index_->GetMaxId(), max_id);
    }
}

TEST_P(TCFixedIndexTest, LimitId) {
    index_ = dynamic_cast<TCFixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index_);

    ASSERT_TRUE(index_->SetOption("size", "16M"));
    ASSERT_TRUE(index_->SetOption("width", "4K"));
    ASSERT_TRUE(index_->Start(StartContext()));

    int64_t id = index_->GetLimitId();
    IntData value;
    value.set_i(id);
    ASSERT_TRUE(index_->Put(&id, sizeof(id), value));
    ASSERT_EQ(index_->GetMaxId(), id);
}

TEST_F(TCFixedIndexTest, GetDB) {
    index_ = dynamic_cast<TCFixedIndex*>(Index::Factory().Create("tc-disk-fixed"));
    ASSERT_TRUE(index_);
    ASSERT_TRUE(index_->SetOption("filename", "work/tc_test_data1"));
    ASSERT_TRUE(index_->SetOption("filename", "work/tc_test_data2"));
    ASSERT_TRUE(index_->SetOption("size", "1G"));
    ASSERT_TRUE(index_->SetOption("width", "4K"));
    ASSERT_TRUE(index_->Start(StartContext()));

    map<uint64_t, int> db_map;
    for (int i = 0; i < 1000; i++) {
        int64_t id = i;
        TCFDB* db = NULL;
        int64_t db_id = 0;
        ASSERT_TRUE(index_->GetDB(id, &db, &db_id));
        db_map[reinterpret_cast<int64_t>(db)]++;
        DEBUG("" << i << " => " << reinterpret_cast<int64_t>(db));
    }
}

}
}

