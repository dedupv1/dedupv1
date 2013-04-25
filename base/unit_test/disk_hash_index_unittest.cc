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
#include <map>

#include <gtest/gtest.h>

#include <base/index.h>
#include <base/disk_hash_index.h>
#include <base/option.h>
#include <base/fileutil.h>
#include <base/strutil.h>
#include <base/protobuf_util.h>
#include "index_test.h"
#include <test_util/log_assert.h>
#include <base/logging.h>
#include "dedupv1_base.pb.h"

using std::string;
using std::map;
using dedupv1::base::strutil::FromHexString;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::SerializeSizedMessage;
LOGGER("DiskHashIndexTest");

namespace dedupv1 {
namespace base {

class DiskHashIndexTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Index* index;

    virtual void SetUp() {
        index = NULL;
    }

    virtual void TearDown() {
        if (index) {
            delete index;
        }
    }
};

INSTANTIATE_TEST_CASE_P(DiskHashIndex,
    IndexTest,
    ::testing::Values(
        // 4 files
        "static-disk-hash;max-key-size=8;max-value-size=8;page-size=8K;size=32M;filename=work/data/hash_test_data1;filename=work/hash_test_data2;filename=work/hash_test_data3;filename=work/hash_test_data4",
        // Sync = false
        "static-disk-hash;max-key-size=8;max-value-size=8;page-size=8K;size=32M;filename=work/data/hash_test_data1;filename=work/hash_test_data2;filename=work/hash_test_data3;filename=work/hash_test_data4;sync=true",
        // Not much space, but enough
        "static-disk-hash;max-key-size=8;max-value-size=8;page-size=8K;size=512K;filename=work/data/hash_test_data1",
        // Overflow
        "static-disk-hash;max-key-size=8;max-value-size=8;page-size=8K;size=64K;filename=work/data/hash_test_data1;overflow-area=sqlite-disk-btree;overflow-area.max-item-count=1K;overflow-area.filename=work/tc_test_overflow_data",
        // Transactions (custom files)
        "static-disk-hash;max-key-size=8;max-value-size=8;page-size=8K;size=32M;filename=work/data/hash_test_data1;transactions.filename=work/hash_test_trans1;transactions.filename=work/hash_test_trans2",
        // Write-back cache
        "static-disk-hash;max-key-size=8;max-value-size=8;page-size=8K;size=32M;filename=work/data/hash_test_data;write-cache=true;write-cache.bucket-count=1K;write-cache.max-page-count=128"
        ))
;

TEST_F(DiskHashIndexTest, CorrectItemCount)
{
    string
        config =
        "static-disk-hash;max-key-size=8;max-value-size=8;page-size=8K;size=32M;filename=work/hash_test_data1;transactions.filename=work/hash_test_trans1;transactions.filename=work/hash_test_trans2";
    index = IndexTest::CreateIndex(config);
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->Start(StartContext()));

    for (int i = 0; i < 32; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        IntData value;
        value.set_i(i);
        ASSERT_EQ(index->Put(key, sizeof(key_value), value), PUT_OK) << "Put " << i << " failed";
    }

    ASSERT_EQ(32, index->GetItemCount());

    for (int i = 16; i < 24; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        ASSERT_EQ(index->Delete(key, sizeof(key_value)), DELETE_OK) << "Delete " << i << " failed";
    }

    ASSERT_EQ(24, index->GetItemCount());
}

TEST_F(DiskHashIndexTest, RecoverItemCount) {
    string config = "static-disk-hash;max-key-size=8;max-value-size=8;page-size=8K;size=32M;filename=work/hash_test_data1;transactions.filename=work/hash_test_trans1;transactions.filename=work/hash_test_trans2";
    index = IndexTest::CreateIndex(config);
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->Start(StartContext()));

    for (int i = 0; i < 32; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        IntData value;
        value.set_i(i);
        ASSERT_EQ(index->Put(key, sizeof(key_value), value), PUT_OK) << "Put " << i << " failed";
    }

    // crash
    DiskHashIndex* dhi = dynamic_cast<DiskHashIndex*>(index);
    dhi->item_count_ = 0;
    dhi->version_counter_ = 0;
    delete index;
    index = NULL;
    dhi = NULL;
    index = IndexTest::CreateIndex(config);
    ASSERT_TRUE(index);
    ASSERT_TRUE(index->Start(StartContext()));

    ASSERT_EQ(32, index->GetItemCount());
}

TEST_F(DiskHashIndexTest, TransactionsWithoutFilename) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    index = Index::Factory().Create("static-disk-hash");
    ASSERT_TRUE(index);

    ASSERT_TRUE(index->SetOption("filename", "work/hash_test_data1"));
    ASSERT_TRUE(index->SetOption("filename", "work/hash_test_data2"));
    ASSERT_TRUE(index->SetOption("filename", "work/hash_test_data3"));
    ASSERT_TRUE(index->SetOption("filename", "work/hash_test_data4"));
    ASSERT_TRUE(index->SetOption("size", "256M"));
    ASSERT_TRUE(index->SetOption("page-size", "4K"));
    ASSERT_TRUE(index->SetOption("max-key-size", "8"));
    ASSERT_TRUE(index->SetOption("max-value-size", "8"));
    ASSERT_TRUE(index->SetOption("transactions.area-size", "1024")); // disabled auto config
    ASSERT_FALSE(index->Start(StartContext()));
}

TEST_F(DiskHashIndexTest, GetFileSequential) {
    index = Index::Factory().Create("static-disk-hash");
    ASSERT_TRUE(index);

    ASSERT_TRUE(index->SetOption("filename", "work/hash_test_data1"));
    ASSERT_TRUE(index->SetOption("filename", "work/hash_test_data2"));
    ASSERT_TRUE(index->SetOption("filename", "work/hash_test_data3"));
    ASSERT_TRUE(index->SetOption("filename", "work/hash_test_data4"));
    ASSERT_TRUE(index->SetOption("size", "256M"));
    ASSERT_TRUE(index->SetOption("page-size", "4K"));
    ASSERT_TRUE(index->SetOption("max-key-size", "8"));
    ASSERT_TRUE(index->SetOption("max-value-size", "8"));
    ASSERT_TRUE(index->Start(StartContext()));

    DiskHashIndex* hash_index = dynamic_cast<DiskHashIndex*>(index);
    ASSERT_TRUE(hash_index);

    uint64_t id[100];
    unsigned int bucket_id[100];
    unsigned int file_id[100];
    unsigned int lock_id[100];

    int i = 0;
    for (i = 0; i < 100; i++) {
        id[i] = i;
        bucket_id[i] = 0;
        file_id[i] = 0;
        lock_id[i] = 0;
    }
    map<unsigned int, int> bucket;
    for (i = 0; i < 100; i++) {
        bucket_id[i] = hash_index->GetBucket(&id[i], sizeof(id[i]));
        hash_index->GetFileIndex(bucket_id[i],
            &file_id[i],
            &lock_id[i]);
        bucket[file_id[i]]++;
    }

    map<unsigned int, int>::iterator j;
    for (j = bucket.begin(); j != bucket.end(); j++) {
        DEBUG("" << j->first << " => file " << j->second);
    }
}

}
}
