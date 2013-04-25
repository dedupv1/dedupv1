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
#include <base/fixed_index.h>
#include <base/logging.h>
#include <base/fileutil.h>

#include "dedupv1_base.pb.h"

#include "index_test.h"

#include <test_util/log_assert.h>

using std::string;
using std::map;

LOGGER("FixedIndexTest");

namespace dedupv1 {
namespace base {

class FixedIndexTest : public testing::TestWithParam<std::string> {
protected:
    USE_LOGGING_EXPECTATION();

    FixedIndex* index;

    byte buffer[8192];
    int buffer_size;

    virtual void SetUp() {
        index = NULL;

        buffer_size = 8192;
        memset(buffer, 0, buffer_size);
    }

    virtual void TearDown() {
        if (index) {
            delete index;
            index = NULL;
        }
    }
};

INSTANTIATE_TEST_CASE_P(FixedIndex,
    IndexTest,
    ::testing::Values("disk-fixed;filename=work/data/tc_test_data;size=64M",
        "disk-fixed;filename=work/tc_test_data;width=8K;size=128M",
        "disk-fixed;filename=work/tc_test_data1;filename=work/tc_test_data2;width=8K;size=128M",
        "disk-fixed;filename=work/tc_test_data1;filename=work/tc_test_data2;filename=work/tc_test_data3;filename=work/tc_test_data4;size=128M"
        ));

INSTANTIATE_TEST_CASE_P(FixedIndex,
    FixedIndexTest,
    ::testing::Values("disk-fixed;filename=work/tc_test_data;size=64M",
        "disk-fixed;filename=work/tc_test_data;width=8K;size=64M",
        "disk-fixed;filename=work/tc_test_data;size=128M",
        "disk-fixed;filename=work/tc_test_data1;filename=work/tc_test_data2;size=128M",
        "disk-fixed;filename=work/tc_test_data1;filename=work/tc_test_data2;filename=work/tc_test_data3;filename=work/tc_test_data4;size=128M"
        ));

TEST_F(FixedIndexTest, IllegalFile) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    index = dynamic_cast<FixedIndex*>(Index::Factory().Create("disk-fixed"));
    ASSERT_TRUE(index);

    ASSERT_TRUE(index->SetOption("filename", "illegal-dir/hash_test_data"));
    ASSERT_FALSE(index->Start(StartContext()));
}

TEST_P(FixedIndexTest, OpenWithChangedSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(2);

    index = dynamic_cast<FixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->SetOption("size", "16M"));
    ASSERT_TRUE(index->SetOption("width", "4K"));
    ASSERT_TRUE(index->Start(StartContext()));
    delete index;
    index = NULL;

    index = dynamic_cast<FixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->SetOption("size", "32M"));
    ASSERT_TRUE(index->SetOption("width", "4K"));
    ASSERT_FALSE(index->Start(StartContext()));
}

TEST_P(FixedIndexTest, OpenWithChangedWidth) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(2);

    index = dynamic_cast<FixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->SetOption("size", "16M"));
    ASSERT_TRUE(index->SetOption("width", "2K"));
    ASSERT_TRUE(index->Start(StartContext()));
    delete index;
    index = NULL;

    index = dynamic_cast<FixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->SetOption("size", "32M"));
    ASSERT_TRUE(index->SetOption("width", "4K"));
    ASSERT_FALSE(index->Start(StartContext()));
}

TEST_P(FixedIndexTest, CheckIfEmpty) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    index = dynamic_cast<FixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->Start(StartContext()));

    uint64_t i = 0;
    for (;; i++) {
        IntData data;
        enum lookup_result lr = index->Lookup(&i, sizeof(i), &data);
        ASSERT_NE(dedupv1::base::LOOKUP_FOUND, lr);
        if (lr == dedupv1::base::LOOKUP_ERROR) {
            break;
        }
    }
}

TEST_P(FixedIndexTest, CheckIfEmptyPartlyWritten) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    index = dynamic_cast<FixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->Start(StartContext()));

    uint64_t i = 0;
    for (i = 0; i < 1024; i++) {
        IntData data;
        data.set_i(42);
        ASSERT_EQ(dedupv1::base::PUT_OK, index->Put(&i, sizeof(i), data));
    }
    for (;; i++) {
        IntData data;
        enum lookup_result lr = index->Lookup(&i, sizeof(i), &data);
        ASSERT_NE(dedupv1::base::LOOKUP_FOUND, lr);
        if (lr == dedupv1::base::LOOKUP_ERROR) {
            break;
        }
    }
    for (i = 0; i < 1024; i++) {
        IntData data;
        enum lookup_result lr = index->Lookup(&i, sizeof(i), &data);
        ASSERT_EQ(dedupv1::base::LOOKUP_FOUND, lr);
    }
}

TEST_P(FixedIndexTest, OpenWithChangedFileFileCount) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(2);

    index = dynamic_cast<FixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->SetOption("size", "24M"));
    ASSERT_TRUE(index->SetOption("width", "2K"));
    ASSERT_TRUE(index->Start(StartContext()));
    delete index;
    index = NULL;

    index = dynamic_cast<FixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->SetOption("size", "24M"));
    ASSERT_TRUE(index->SetOption("width", "2K"));
    ASSERT_TRUE(index->SetOption("filename", "/tmp/a"));
    ASSERT_TRUE(index->SetOption("filename", "/tmp/b"));
    ASSERT_FALSE(index->Start(StartContext()));
}

TEST_P(FixedIndexTest, LimitId) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once().Matches("id exceeds");

    index = dynamic_cast<FixedIndex*>(IndexTest::CreateIndex(GetParam()));
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->SetOption("size", "16M"));
    ASSERT_TRUE(index->SetOption("width", "4K"));
    ASSERT_TRUE(index->Start(StartContext()));

    int64_t id = index->GetLimitId();
    IntData data;
    data.set_i(id);
    ASSERT_TRUE(index->Put(&id, sizeof(id), data));

    int64_t next_id = index->GetLimitId() + 1;
    IntData next_data;
    next_data.set_i(next_id);
    ASSERT_FALSE(index->Put(&next_id, sizeof(next_id), next_data)) <<
    "Put should fail because the id should exceed the limit";
}

TEST_F(FixedIndexTest, GetFile) {
    index = dynamic_cast<FixedIndex*>(Index::Factory().Create("disk-fixed"));
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->SetOption("filename", "work/tc_test_data1"));
    ASSERT_TRUE(index->SetOption("filename", "work/tc_test_data2"));
    ASSERT_TRUE(index->SetOption("size", "1G"));
    ASSERT_TRUE(index->SetOption("width", "4K"));
    ASSERT_TRUE(index->Start(StartContext()));

    map<uint64_t, int> file_map;
    for (int i = 0; i < 1000; i++) {
        int64_t id = i;
        dedupv1::base::File* file = NULL;
        int64_t file_id = 0;
        ASSERT_TRUE(index->GetFile(id, &file, &file_id));
        file_map[reinterpret_cast<int64_t>(file)]++;
        DEBUG("" << i << " => " << reinterpret_cast<int64_t>(file));
    }
}

}
}
