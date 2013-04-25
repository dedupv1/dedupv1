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

#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <map>

#include <gtest/gtest.h>

#include <base/index.h>
#include <base/disk_hash_index.h>
#include <base/disk_hash_index_transaction.h>
#include <base/logging.h>
#include <base/protobuf_util.h>
#include "index_test.h"
#include <test_util/log_assert.h>

using std::map;
using dedupv1::base::internal::DiskHashIndexTransaction;
using dedupv1::base::internal::DiskHashIndexTransactionSystem;
using dedupv1::base::internal::DiskHashPage;
using dedupv1::base::SerializeSizedMessage;
using dedupv1::base::LOOKUP_FOUND;

LOGGER("DiskHashIndexTransactionTest");

namespace dedupv1 {
namespace base {

class DiskHashIndexTransactionTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    DiskHashIndex* index;
    DiskHashIndexTransactionSystem* trans_system;

    byte* buffer;
    size_t buffer_size;

    uint64_t key1;
    uint64_t key2;
    IntData value;
    uint64_t bucket_id;

    void Restart() {
        if (index) {
            ASSERT_TRUE(index);
            delete index;
        }

        CreateIndex();
    }

    void CreateIndex() {
        Index* new_index = Index::Factory().Create("static-disk-hash");
        ASSERT_TRUE(new_index);

        ASSERT_TRUE(new_index->SetOption("filename", "work/hash_test_data"));
        ASSERT_TRUE(new_index->SetOption("size", "256M"));
        ASSERT_TRUE(new_index->SetOption("page-size", "4K"));
        ASSERT_TRUE(new_index->SetOption("max-key-size", "8"));
        ASSERT_TRUE(new_index->SetOption("max-value-size", "8"));
        ASSERT_TRUE(new_index->SetOption("transactions.filename", "work/hash_test_trans"));

        index = dynamic_cast<DiskHashIndex*>(new_index);
        ASSERT_TRUE(index);
        ASSERT_TRUE(index->Start(StartContext()));
        trans_system = index->transaction_system();
        ASSERT_TRUE(trans_system);
    }

    virtual void SetUp() {
        buffer = NULL;
        buffer_size = 0;

        key1 = 0;
        key2 = 1;
        value.set_i(42);

        CreateIndex();

        bucket_id = index->GetBucket(&key1, sizeof(key1));
        // find a second key in the same bucket
        while (bucket_id != index->GetBucket(&key2, sizeof(key2))) {
            key2++;
        }
        buffer = new byte[index->page_size()];
        memset(buffer, 0, index->page_size());
        buffer_size = index->page_size();
    }

    virtual void TearDown() {
        if (index) {
            delete index;
            trans_system = NULL;
        }
        if (buffer) {
            delete[] buffer;
            buffer = NULL;
        }
    }

    void CheckKey(uint64_t key) {
        IntData check_value;
        ASSERT_EQ(index->Lookup(&key, sizeof(key), &check_value), LOOKUP_FOUND);
        ASSERT_EQ(value.i(), check_value.i());
    }
};

TEST_F(DiskHashIndexTransactionTest, Init) {
    // do nothing
}

TEST_F(DiskHashIndexTransactionTest, StartWithEmptyRecovery) {
    Restart();
}

TEST_F(DiskHashIndexTransactionTest, NormalCommitWithRecovery) {
    DiskHashPage page(index, bucket_id, buffer, buffer_size);
    ASSERT_TRUE(page.Update(&key1, sizeof(key1), value));

    {
        DiskHashIndexTransaction trans(trans_system, page);
        index->version_counter_ = 1;
        // change page data and update buffer
        ASSERT_TRUE(page.Update(&key2, sizeof(key2), value));
        ASSERT_TRUE(trans.Start(0, page));
        ASSERT_TRUE(trans.Commit());
    }

    index->item_count_ = 0;
    index->version_counter_ = 0;
    Restart();

    CheckKey(key1);
    CheckKey(key2);
    ASSERT_EQ(index->GetItemCount(), 2);
}

TEST_F(DiskHashIndexTransactionTest, AbortBeforeStart) {
    // we have the scope here to ensure that the transaction is aborted here
    {
        DiskHashPage page(index, bucket_id, buffer, buffer_size);
        ASSERT_TRUE(page.Update(&key1, sizeof(key1), value));

        DiskHashIndexTransaction trans(trans_system, page);
    }
    // abort

    Restart();

    // we cannot say anything about key1 and key2 but the system should recover
}

TEST_F(DiskHashIndexTransactionTest, AbortAfterStart) {
    // we have the scope here to ensure that the transaction is aborted here
    {
        DiskHashPage page(index, bucket_id, buffer, buffer_size);
        ASSERT_TRUE(page.Update(&key1, sizeof(key1), value));

        DiskHashIndexTransaction trans(trans_system, page);

        // change page data and update buffer
        ASSERT_TRUE(page.Update(&key2, sizeof(key2), value));
        ASSERT_TRUE(page.SerializeToBuffer());

        ASSERT_TRUE(trans.Start(0, page));
    }
    // crash

    Restart();

    CheckKey(key1);
    CheckKey(key2);
}

TEST_F(DiskHashIndexTransactionTest, ScrambleTransactionData) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once().Matches("parse").Logger("Protobuf");
    EXPECT_LOGGING(dedupv1::test::ERROR).Once().Matches("parse").Logger("File");
    EXPECT_LOGGING("Failed to read transaction page data").Level(dedupv1::test::WARN).Once();

    DiskHashPage page(index, bucket_id, buffer, buffer_size);
    ASSERT_TRUE(page.Update(&key1, sizeof(key1), value));

    {
        {
        }
        DiskHashIndexTransaction trans(trans_system, page);

        // change page data and update buffer
        ASSERT_TRUE(page.Update(&key2, sizeof(key2), value));
        ASSERT_TRUE(page.SerializeToBuffer());

        ASSERT_TRUE(trans.Start(0, page));

        ASSERT_TRUE(trans.Commit());
    }
    off_t transaction_area_file_offset = trans_system->transaction_area_offset(bucket_id);
    off_t transaction_area_file_size = trans_system->page_size();

    // crash
    delete index;
    index = NULL;

    File* transaction_file = File::Open("work/hash_test_trans", O_RDWR | O_LARGEFILE, 0);
    ASSERT_TRUE(transaction_file);

    // destroy the data
    byte scample_buffer[transaction_area_file_size];
    memset(scample_buffer, 17, transaction_area_file_size);
    transaction_file->Write(transaction_area_file_offset, scample_buffer, transaction_area_file_size);
    delete transaction_file;
    transaction_file = NULL;

    Restart();
}

}
}
