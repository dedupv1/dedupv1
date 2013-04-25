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
#include "index_test.h"

#include <base/index.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/fixed_index.h>
#include <base/thread.h>
#include <base/runnable.h>
#include "dedupv1_base.pb.h"
#include <base/protobuf_util.h>

#include <json/json.h>

#include <string>
#include <vector>
#include <set>
#include <tr1/tuple>

using std::set;
using std::tr1::tuple;
using std::string;
using std::vector;
using dedupv1::base::strutil::Split;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::Thread;
using dedupv1::base::Runnable;
using google::protobuf::Message;
using std::tr1::make_tuple;

LOGGER("IndexTest");

#define SKIP_IF_FIXED_INDEX(x) if (dynamic_cast<FixedIndex*>(x)) { INFO("Skipping test for fixed index"); return; }

#define SKIP_UNLESS_PUT_IF_ABSENT_SUPPORTED(index) if (!index->HasCapability(PUT_IF_ABSENT)) { INFO("Skipping test for index"); return; }

#define INDEX_TEST_OP_COUNT (1024 * 4)

namespace dedupv1 {
namespace base {

void IndexTest::SetUp() {
    config = GetParam();

    index = CreateIndex(config);
    ASSERT_TRUE(index) << "Failed to create index";

    buffer_size = 8192;
    memset(buffer, 0, buffer_size);
}

void IndexTest::TearDown() {
    if (index) {
        delete index;
    }
}

bool IndexTest::Write(Index* index, int start, int end) {
    for (int i = start; i < end; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        IntData value;
        value.set_i(i);
        CHECK(index->Put(key, sizeof(key_value), value) == PUT_OK, "Put " << i << " failed");
    }
    return true;
}

bool IndexTest::BatchWrite(Index* index, int start, int end) {
    int batch_size = 8;
    for (int i = start; i < end; i += batch_size) {

        vector<tuple<bytestring, const google::protobuf::Message*> > data;
        for (int j = i; j < (i + batch_size) && j < end; j++) {
            uint64_t key_value = j;
            byte* key = (byte *) &key_value;

            IntData* value = new IntData();
            value->set_i(j);

            DEBUG("Put " << j << " = " << ToHexString(key, sizeof(key_value)));

            data.push_back(make_tuple(dedupv1::base::make_bytestring(key, sizeof(key_value)), value));
        }

        CHECK(index->PutBatch(data) == PUT_OK, "Put " << i << " failed");

        vector<tuple<bytestring, const google::protobuf::Message*> >::iterator k;
        for (k = data.begin(); k != data.end(); k++) {
            delete std::tr1::get<1>(*k);
        }

    }
    return true;
}

bool IndexTest::Read(Index* index, int start, int end) {
    for (int i = start; i < end; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;
        uint64_t value = i;

        IntData get_value;
        CHECK(index->Lookup(key, sizeof(key_value), &get_value) == LOOKUP_FOUND, "Lookup failed: key " << i);
        CHECK(value == get_value.i(), "Comparison failed");
    }
    return true;
}

void IndexTest::Restart() {
    if (index) {
        delete index;
    }
    index = CreateIndex(config);
    ASSERT_TRUE(index) << "Failed to create index";
    ASSERT_TRUE(index->Start(StartContext()));
}

Index* IndexTest::CreateIndex(string config_option) {
    vector<string> options;
    CHECK_RETURN(Split(config_option, ";", &options), NULL, "Failed to split: " << config_option);

    Index* index = Index::Factory().Create(options[0]);
    CHECK_RETURN(index, NULL, "Failed to create index type: " << options[0]);

    for (size_t i = 1; i < options.size(); i++) {
        if (options[i].empty()) {
            continue;
        }
        string option_name;
        string option;
        CHECK_RETURN(Split(options[i], "=", &option_name, &option), NULL, "Failed to split " << options[i]);
        CHECK_RETURN(index->SetOption(option_name, option), NULL, "Failed set option: " << options[i]);
    }
    return index;
}

/**
 * Tests if the index can be created and closed without errors
 */
TEST_P(IndexTest, Create) {
}

/**
 * Tests if the index can be started
 */
TEST_P(IndexTest, Start) {
    ASSERT_TRUE(index->Start(StartContext()));
}

TEST_P(IndexTest, Restart) {
    ASSERT_TRUE(index->Start(StartContext()));
    ASSERT_NO_FATAL_FAILURE(Restart());
}

/**
 * Check that Clear can be called on an memory index before the start
 */
TEST_P(IndexTest, ClearWithoutStart) {
    if (index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    MemoryIndex* mi = index->AsMemoryIndex();
    ASSERT_TRUE(mi->Clear());
}

/**
 * Checks on an persistent index that the first start fails when the create mode is not set
 */
TEST_P(IndexTest, StartWithoutCreate) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();
    dedupv1::StartContext start_context(dedupv1::StartContext::NON_CREATE);
    ASSERT_FALSE(index->Start(start_context)) << "First start without create flag should fail";
}

/**
 * Checks that a second start on a persistent index fails when the create mode is set
 */
TEST_P(IndexTest, StartWithoutCreateAsSecondStart) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }

    // normal start
    ASSERT_TRUE(index->Start(StartContext()));
    delete index;
    index = NULL;

    index = CreateIndex(config);
    dedupv1::StartContext start_context(dedupv1::StartContext::NON_CREATE);
    ASSERT_TRUE(index->Start(start_context)) << "First start without create flag should fail";
}

TEST_P(IndexTest, StartWithDefaultFilemode) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }

    // normal start
    ASSERT_TRUE(index->Start(StartContext()));

    dedupv1::FileMode file_mode;
    dedupv1::FileMode dir_mode(true);

    vector<string> options;
    ASSERT_TRUE(Split(GetParam(), ";", &options));

    for (vector<string>::iterator i = options.begin(); i != options.end(); i++) {
        if (i == options.begin()) {
            continue;
        }
        string option_name;
        string option;
        ASSERT_TRUE(Split(*i, "=", &option_name, &option)) << *i;

        if (dedupv1::base::strutil::EndsWith(option_name, "filename")) {
            struct stat file_stat;
            memset(&file_stat, 0, sizeof(file_stat));
            ASSERT_TRUE(File::Stat(option, &file_stat));

            if ((file_stat.st_mode & S_IFDIR) == 0) {
                ASSERT_EQ(file_mode.mode(), file_stat.st_mode & 07777) <<
                option << " has wrong mode";
            } else {
                ASSERT_EQ(dir_mode.mode(), file_stat.st_mode & 07777) <<
                option << " has wrong mode (directory)";
            }
        }
    }
}

TEST_P(IndexTest, StartWithCustomFilemode) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }

    // normal start
    StartContext start_context;
    int file_mode =  S_IRUSR | S_IWUSR | S_IRGRP;
    int dir_mode = S_IRUSR | S_IWUSR | S_IXUSR;
    start_context.set_file_mode(dedupv1::FileMode::Create(-1, false, file_mode));
    start_context.set_dir_mode(dedupv1::FileMode::Create(-1, false, dir_mode));

    ASSERT_TRUE(index->Start(start_context));

    vector<string> options;
    ASSERT_TRUE(Split(GetParam(), ";", &options));

    for (vector<string>::iterator i = options.begin(); i != options.end(); i++) {
        if (i == options.begin()) {
            continue;
        }
        string option_name;
        string option;
        ASSERT_TRUE(Split(*i, "=", &option_name, &option)) << *i;

        if (dedupv1::base::strutil::EndsWith(option_name, "filename")) {
            struct stat file_stat;
            memset(&file_stat, 0, sizeof(file_stat));
            ASSERT_TRUE(File::Stat(option, &file_stat));

            if ((file_stat.st_mode & S_IFDIR) == 0) {
                ASSERT_EQ(file_mode, file_stat.st_mode & 07777);
            } else {
                ASSERT_EQ(dir_mode, file_stat.st_mode & 07777);
            }
        }
    }
}

/**
 * Checks that the estimated max item count of an persistent index is not 0.
 */
TEST_P(IndexTest, GetEstimatedMaxItemCount) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    ASSERT_TRUE(index->Start(StartContext()));

    PersistentIndex* pi = index->AsPersistentIndex();
    uint64_t max_item_count = pi->GetEstimatedMaxItemCount();
    DEBUG(max_item_count);
    ASSERT_GT(max_item_count, 0);
}

/**
 * Checks that LOOKUP_NOT_FOUND is returned when a key is not existing
 */
TEST_P(IndexTest, LookupWithoutData) {
    ASSERT_TRUE(index->Start(StartContext()));
    uint64_t key_value = 1;
    byte* key = (byte *) &key_value;

    IntData value;
    enum lookup_result result = index->Lookup(key, sizeof(key_value), &value);
    ASSERT_EQ(result, LOOKUP_NOT_FOUND) << "Index shouldn't find anything";
}

TEST_P(IndexTest, WriteRead) {
    ASSERT_TRUE(index->Start(StartContext()));
    uint64_t key_value = 1;
    byte* key = (byte *) &key_value;

    IntData value;
    value.set_i(1);
    ASSERT_TRUE(index->Put(key, sizeof(key_value), value));

    IntData get_value;
    ASSERT_EQ(index->Lookup(key, sizeof(key_value), &get_value), LOOKUP_FOUND);
    ASSERT_EQ(value.i(), get_value.i());
}

TEST_P(IndexTest, WriteOverwrite) {
    ASSERT_TRUE(index->Start(StartContext()));
    uint64_t key_value = 1;
    byte* key = (byte *) &key_value;

    IntData value;
    value.set_i(1);
    ASSERT_TRUE(index->Put(key, sizeof(key_value), value));

    IntData value2;
    value2.set_i(2);
    ASSERT_TRUE(index->Put(key, sizeof(key_value), value2));

    IntData get_value;
    ASSERT_EQ(index->Lookup(key, sizeof(key_value), &get_value), LOOKUP_FOUND);
    ASSERT_EQ(value2.i(), get_value.i());
}

TEST_P(IndexTest, WriteClear) {
    if (index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    MemoryIndex* mi = index->AsMemoryIndex();

    ASSERT_TRUE(mi->Start(StartContext()));
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        IntData value;
        value.set_i(i);
        ASSERT_EQ(mi->Put(key, sizeof(key_value), value), PUT_OK) << "Put " << i << " failed";
    }
    ASSERT_EQ(mi->GetItemCount(), INDEX_TEST_OP_COUNT);

    ASSERT_TRUE(mi->Clear());
    ASSERT_EQ(mi->GetItemCount(), 0);
}

TEST_P(IndexTest, CompareAndSwap) {
    if (!index->HasCapability(COMPARE_AND_SWAP)) {
        return;
    }
    ASSERT_TRUE(index->Start(StartContext()));

    uint64_t key_value = 2;
    byte* key = (byte *) &key_value;

    IntData value;
    value.set_i(key_value);
    ASSERT_EQ(index->Put(key, sizeof(key_value), value), PUT_OK) << "Initial put failed";

    IntData value2;
    value2.set_i(3);

    IntData result_message;
    ASSERT_EQ(index->CompareAndSwap(key, sizeof(key_value), value2, value, &result_message), PUT_OK);
    ASSERT_TRUE(dedupv1::base::MessageEquals(result_message, value2));

    IntData another_value;
    another_value.set_i(17);
    IntData yet_another_value;
    yet_another_value.set_i(18);

    ASSERT_EQ(index->CompareAndSwap(key, sizeof(key_value), another_value, yet_another_value, &result_message), PUT_KEEP);
    ASSERT_TRUE(dedupv1::base::MessageEquals(result_message, value2));

    ASSERT_EQ(index->CompareAndSwap(key, sizeof(key_value), another_value, value2, &result_message), PUT_OK);
    ASSERT_TRUE(dedupv1::base::MessageEquals(result_message, another_value));
}

TEST_P(IndexTest, MultipleWriteRead) {
    ASSERT_TRUE(index->Start(StartContext()));
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        IntData value;
        value.set_i(i);
        ASSERT_EQ(index->Put(key, sizeof(key_value), value), PUT_OK) << "Put " << i << " failed";
    }

    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;
        uint64_t value = i;

        IntData get_value;
        ASSERT_EQ(index->Lookup(key, sizeof(key_value), &get_value), LOOKUP_FOUND);
        ASSERT_EQ(value, get_value.i());
    }
}

TEST_P(IndexTest, BatchedMultiThreadedWriteRead) {
    SKIP_IF_FIXED_INDEX(index);

    tbb::tick_count start = tbb::tick_count::now();
    ASSERT_TRUE(index->Start(StartContext()));
    INFO("Init time: " << (tbb::tick_count::now() - start).seconds() << "s");

    Thread<bool> thread1(NewRunnable(&IndexTest::BatchWrite, index, 0, INDEX_TEST_OP_COUNT / 4), "write 1");
    Thread<bool> thread2(NewRunnable(&IndexTest::BatchWrite, index, INDEX_TEST_OP_COUNT / 4, INDEX_TEST_OP_COUNT / 2), "write 2");
    Thread<bool> thread3(NewRunnable(&IndexTest::BatchWrite, index, INDEX_TEST_OP_COUNT / 2, 3 * (INDEX_TEST_OP_COUNT / 4)), "write 3");
    Thread<bool> thread4(NewRunnable(&IndexTest::BatchWrite, index, 3 * (INDEX_TEST_OP_COUNT / 4), INDEX_TEST_OP_COUNT), "write 4");
    start = tbb::tick_count::now();
    ASSERT_TRUE(thread1.Start());
    ASSERT_TRUE(thread2.Start());
    ASSERT_TRUE(thread3.Start());
    ASSERT_TRUE(thread4.Start());
    bool r1 = false;
    bool r2 = false;
    bool r3 = false;
    bool r4 = false;
    ASSERT_TRUE(thread1.Join(&r1));
    ASSERT_TRUE(thread2.Join(&r2));
    ASSERT_TRUE(thread3.Join(&r3));
    ASSERT_TRUE(thread4.Join(&r4));
    ASSERT_TRUE(r1 && r2 && r3 && r4);
    INFO("Write time: " << (tbb::tick_count::now() - start).seconds() << "s");

    Thread<bool> thread_read1(NewRunnable(&IndexTest::Read, index, 0, INDEX_TEST_OP_COUNT / 2), "read 1");
    Thread<bool> thread_read2(NewRunnable(&IndexTest::Read, index, INDEX_TEST_OP_COUNT / 2, INDEX_TEST_OP_COUNT), "read 2");
    start = tbb::tick_count::now();
    ASSERT_TRUE(thread_read1.Start());
    ASSERT_TRUE(thread_read2.Start());
    ASSERT_TRUE(thread_read1.Join(&r1));
    ASSERT_TRUE(thread_read2.Join(&r2));
    ASSERT_TRUE(r1 && r2);
    INFO("Read time: " << (tbb::tick_count::now() - start).seconds() << "s");

    if (!index->IsPersistent()) {
        return;
    }
    delete index;
    index = NULL;

    index = CreateIndex(config);
    dedupv1::StartContext start_context(dedupv1::StartContext::NON_CREATE);
    ASSERT_TRUE(index->Start(start_context));

    start = tbb::tick_count::now();
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;
        uint64_t value = i;

        IntData get_value;
        ASSERT_EQ(index->Lookup(key, sizeof(key_value), &get_value), LOOKUP_FOUND);
        ASSERT_EQ(value, get_value.i());
    }
    INFO("Read time (single threaded): " << (tbb::tick_count::now() - start).seconds() << "s");
}

TEST_P(IndexTest, MultiThreadedWriteRead) {
    tbb::tick_count start = tbb::tick_count::now();
    ASSERT_TRUE(index->Start(StartContext()));
    INFO("Init time: " << (tbb::tick_count::now() - start).seconds() << "s");

    Thread<bool> thread1(NewRunnable(&IndexTest::Write, index, 0, INDEX_TEST_OP_COUNT / 4), "write 1");
    Thread<bool> thread2(NewRunnable(&IndexTest::Write, index, INDEX_TEST_OP_COUNT / 4, INDEX_TEST_OP_COUNT / 2), "write 2");
    Thread<bool> thread3(NewRunnable(&IndexTest::Write, index, INDEX_TEST_OP_COUNT / 2, 3 * (INDEX_TEST_OP_COUNT / 4)), "write 3");
    Thread<bool> thread4(NewRunnable(&IndexTest::Write, index, 3 * (INDEX_TEST_OP_COUNT / 4), INDEX_TEST_OP_COUNT), "write 4");
    start = tbb::tick_count::now();
    ASSERT_TRUE(thread1.Start());
    ASSERT_TRUE(thread2.Start());
    ASSERT_TRUE(thread3.Start());
    ASSERT_TRUE(thread4.Start());
    bool r1 = false;
    bool r2 = false;
    bool r3 = false;
    bool r4 = false;
    ASSERT_TRUE(thread1.Join(&r1));
    ASSERT_TRUE(thread2.Join(&r2));
    ASSERT_TRUE(thread3.Join(&r3));
    ASSERT_TRUE(thread4.Join(&r4));
    ASSERT_TRUE(r1 && r2 && r3 && r4);
    INFO("Write time: " << (tbb::tick_count::now() - start).seconds() << "s");

    Thread<bool> thread_read1(NewRunnable(&IndexTest::Read, index, 0, INDEX_TEST_OP_COUNT / 2), "read 1");
    Thread<bool> thread_read2(NewRunnable(&IndexTest::Read, index, INDEX_TEST_OP_COUNT / 2, INDEX_TEST_OP_COUNT), "read 2");
    start = tbb::tick_count::now();
    ASSERT_TRUE(thread_read1.Start());
    ASSERT_TRUE(thread_read2.Start());
    ASSERT_TRUE(thread_read1.Join(&r1));
    ASSERT_TRUE(thread_read2.Join(&r2));
    ASSERT_TRUE(r1 && r2);
    INFO("Read time: " << (tbb::tick_count::now() - start).seconds() << "s");

    if (!index->IsPersistent()) {
        return;
    }
    delete index;
    index = NULL;

    index = CreateIndex(config);
    dedupv1::StartContext start_context(dedupv1::StartContext::NON_CREATE);
    ASSERT_TRUE(index->Start(start_context));

    start = tbb::tick_count::now();
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;
        uint64_t value = i;

        IntData get_value;
        ASSERT_EQ(index->Lookup(key, sizeof(key_value), &get_value), LOOKUP_FOUND);
        ASSERT_EQ(value, get_value.i());
    }
    INFO("Read time (single threaded): " << (tbb::tick_count::now() - start).seconds() << "s");
}

/**
 * Checks that the item count of a persistent index with the item count capability is correct
 */
TEST_P(IndexTest, ItemCountOnRestart) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    if (!index->HasCapability(dedupv1::base::PERSISTENT_ITEM_COUNT)) {
        return;
    }
    ASSERT_TRUE(index->Start(StartContext()));
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        IntData value;
        value.set_i(i);
        ASSERT_EQ(index->Put(key, sizeof(key_value), value), PUT_OK) << "Put " << i << " failed";
    }
    ASSERT_EQ(INDEX_TEST_OP_COUNT, index->GetItemCount());
    Restart();
    ASSERT_EQ(INDEX_TEST_OP_COUNT, index->GetItemCount());
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;
        uint64_t value = i;

        IntData get_value;
        ASSERT_EQ(index->Lookup(key, sizeof(key_value), &get_value), LOOKUP_FOUND);
        ASSERT_EQ(value, get_value.i());
    }
}

/**
 * Checks the delete method
 */
TEST_P(IndexTest, Delete) {
    ASSERT_TRUE(index->Start(StartContext()));
    uint64_t key_value = 1;
    byte* key = (byte *) &key_value;

    IntData value;
    value.set_i(1);
    ASSERT_EQ(index->Put(key, sizeof(key_value), value), PUT_OK);

    enum delete_result r = index->Delete(key, sizeof(key_value));
    ASSERT_TRUE(r == DELETE_OK);

    IntData get_value;
    ASSERT_EQ(index->Lookup(key, sizeof(key_value), &get_value), LOOKUP_NOT_FOUND);

    if (index->HasCapability(RETURNS_DELETE_NOT_FOUND)) {
        r = index->Delete(key, sizeof(key_value));
        ASSERT_TRUE(r == DELETE_NOT_FOUND);
    }
}

TEST_P(IndexTest, MultipleWriteReadDelete) {
    tbb::tick_count start = tbb::tick_count::now();
    ASSERT_TRUE(index->Start(StartContext()));
    INFO("Init time: " << (tbb::tick_count::now() - start).seconds() << "s");

    DEBUG("Insert data")
    start = tbb::tick_count::now();
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        IntData value;
        value.set_i(i);
        ASSERT_EQ(index->Put(key, sizeof(key_value), value), PUT_OK) << "Put " << i << " failed";
    }
    INFO("Insert time: " << (tbb::tick_count::now() - start).seconds() << "s");

    DEBUG("Delete data");
    start = tbb::tick_count::now();
    for (int i = 0; i < INDEX_TEST_OP_COUNT / 2; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        ASSERT_EQ(index->Delete(key, sizeof(key_value)), DELETE_OK) << "Delete " << i << " failed";
    }
    INFO("Delete time: " << (tbb::tick_count::now() - start).seconds() << "s");

    DEBUG("Read data");
    start = tbb::tick_count::now();
    for (int i = 0; i < INDEX_TEST_OP_COUNT / 2; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        ASSERT_EQ(index->Lookup(key, sizeof(key_value), NULL), LOOKUP_NOT_FOUND);
    }

    for (int i = INDEX_TEST_OP_COUNT / 2; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;
        uint64_t value = i;

        IntData get_value;
        ASSERT_EQ(index->Lookup(key, sizeof(key_value), &get_value), LOOKUP_FOUND);
        ASSERT_EQ(value, get_value.i());
    }
    INFO("Read time: " << (tbb::tick_count::now() - start).seconds() << "s");

    DEBUG("Overwrite data")
    start = tbb::tick_count::now();
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        IntData value;
        value.set_i(i + 1);
        ASSERT_EQ(index->Put(key, sizeof(key_value), value), PUT_OK) << "Put " << i << " failed";
    }

    DEBUG("Read all");
    start = tbb::tick_count::now();
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;
        ASSERT_EQ(index->Lookup(key, sizeof(key_value), NULL), LOOKUP_FOUND);
    }
    INFO("Read all time: " << (tbb::tick_count::now() - start).seconds() << "s");

    DEBUG("Delete all");
    start = tbb::tick_count::now();
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        ASSERT_EQ(index->Delete(key, sizeof(key_value)), DELETE_OK) << "Delete " << i << " failed";
    }
    INFO("Delete all time: " << (tbb::tick_count::now() - start).seconds() << "s");

    DEBUG("Read all");
    start = tbb::tick_count::now();
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;
        ASSERT_EQ(index->Lookup(key, sizeof(key_value), NULL), LOOKUP_NOT_FOUND);
    }
    INFO("Read all time: " << (tbb::tick_count::now() - start).seconds() << "s");
}

TEST_P(IndexTest, DeleteNotFound) {
    SKIP_IF_FIXED_INDEX(index);

    bool has_cap = index->HasCapability(RETURNS_DELETE_NOT_FOUND);

    ASSERT_TRUE(index->Start(StartContext()));
    uint64_t key_value = 1;
    byte* key = (byte *) &key_value;

    enum delete_result r = index->Delete(key, sizeof(key_value));
    if (has_cap) {
        ASSERT_TRUE(r == DELETE_NOT_FOUND);
    } else {
        // if the index has not the capability, it should return DELETE_OK.
        // It should not be an error
        ASSERT_TRUE(r == DELETE_OK);
    }
}

TEST_P(IndexTest, PutIfAbsent) {
    SKIP_UNLESS_PUT_IF_ABSENT_SUPPORTED(index);

    ASSERT_TRUE(index->Start(StartContext()));
    uint64_t key_value = 1;
    byte* key = (byte *) &key_value;

    IntData value;
    value.set_i(1);

    // 1. Put
    ASSERT_TRUE(index->PutIfAbsent(key, sizeof(key_value), value));

    IntData get_value;
    ASSERT_EQ(index->Lookup(key, sizeof(key_value), &get_value), LOOKUP_FOUND);
    ASSERT_EQ(value.i(), get_value.i());

    // 2. Put
    IntData second_value;
    second_value.set_i(2);

    ASSERT_EQ(index->PutIfAbsent(key, sizeof(key_value), second_value), PUT_KEEP);

    ASSERT_EQ(index->Lookup(key, sizeof(key_value), &get_value), LOOKUP_FOUND);
    ASSERT_EQ(value.i(), get_value.i()) << "Value has still if the first value";
}

TEST_P(IndexTest, PrintLockStatistics) {
    ASSERT_TRUE(index->Start(StartContext()));

    string s = index->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse: " << reader.getFormatedErrorMessages() << ", data " << s;
}

TEST_P(IndexTest, PrintTrace) {
    ASSERT_TRUE(index->Start(StartContext()));

    string s = index->PrintTrace();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse: " << reader.getFormatedErrorMessages() << ", data " << s;
}

TEST_P(IndexTest, PrintProfile) {
    ASSERT_TRUE(index->Start(StartContext()));

    string s = index->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse: " << reader.getFormatedErrorMessages() << ", data " << s;
}

TEST_P(IndexTest, CursorBeforeStarted) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    PersistentIndex* pi = index->AsPersistentIndex();
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(0, 1);

    if (!pi->SupportsCursor()) {
        // if an index doesn't support the cursor we can ignore this test
        return;
    }
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(0, 1);

    ASSERT_TRUE(pi->CreateCursor() == NULL);
}

TEST_P(IndexTest, Cursor) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    PersistentIndex* pi = index->AsPersistentIndex();
    ASSERT_TRUE(pi->Start(StartContext()));
    if (pi->SupportsCursor() == false) {
        EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
        ASSERT_TRUE(pi->CreateCursor() == NULL);
        return;
    }

    IndexCursor* cursor = pi->CreateCursor();
    ASSERT_TRUE(cursor);
    ASSERT_EQ(cursor->First(), LOOKUP_NOT_FOUND);
    ASSERT_FALSE(cursor->IsValidPosition());

    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key_value = i;
        byte* key = (byte *) &key_value;

        IntData value;
        value.set_i(i);

        ASSERT_EQ(pi->Put(key, sizeof(key_value), value), PUT_OK) << "Put " << i << " failed";
    }
    ASSERT_EQ(cursor->First(), LOOKUP_FOUND);
    ASSERT_TRUE(cursor->IsValidPosition());

    set<uint64_t> result_set;
    for (int i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        uint64_t key;
        size_t key_size = sizeof(key);
        IntData value;

        ASSERT_TRUE(cursor->Get(&key, &key_size, &value));

        ASSERT_TRUE(result_set.find(key) == result_set.end()); // nothing is doubled
        result_set.insert(key);
        ASSERT_EQ(value.i(), key);

        DEBUG("Found " << key);

        if (i < INDEX_TEST_OP_COUNT - 1) {
            ASSERT_EQ(cursor->Next(), LOOKUP_FOUND);
            ASSERT_TRUE(cursor->IsValidPosition());
        } else {
            ASSERT_EQ(cursor->Next(), LOOKUP_NOT_FOUND);
            ASSERT_FALSE(cursor->IsValidPosition());
        }
    }

    ASSERT_EQ(cursor->First(), LOOKUP_FOUND);
    ASSERT_TRUE(cursor->IsValidPosition());

    IntData value;
    value.set_i(10);
    ASSERT_TRUE(cursor->Put(value));

    ASSERT_EQ(cursor->First(), LOOKUP_FOUND);
    ASSERT_TRUE(cursor->IsValidPosition());

    IntData get_value;
    ASSERT_TRUE(cursor->Get(NULL, NULL, &get_value));
    ASSERT_EQ(get_value.i(), 10);

    int ic = pi->GetItemCount();

    ASSERT_TRUE(cursor->Remove());
    ASSERT_EQ(pi->GetItemCount(), ic - 1);

    ASSERT_TRUE(cursor->Get(NULL, NULL, &get_value));
    ASSERT_FALSE(get_value.i() == 10) << "The cursor has not moved to the next position after the Remove() call";

    delete cursor;
}

TEST_P(IndexTest, Iterator) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    PersistentIndex* pi = index->AsPersistentIndex();
    ASSERT_TRUE(pi->Start(StartContext()));
    // for all indexes that support an iterator
    set<uint64_t> s;
    for (uint64_t i = 0; i < INDEX_TEST_OP_COUNT; i++) {
        s.insert(i);
        IntData value;
        value.set_i(i);
        ASSERT_EQ(pi->Put(&i, sizeof(i), value), PUT_OK);
    }

    IndexIterator* it = pi->CreateIterator();
    ASSERT_TRUE(it);
    enum lookup_result r = LOOKUP_FOUND;
    while (r == LOOKUP_FOUND) {
        uint64_t k;
        IntData v;
        size_t k_size = sizeof(k);
        r = it->Next(&k, &k_size, &v);
        ASSERT_TRUE(r != LOOKUP_ERROR);
        if (r == LOOKUP_FOUND) {
            ASSERT_EQ(k_size, sizeof(k));

            DEBUG("Found " << k);

            ASSERT_EQ(k, v.i());
            s.erase(k);
        }
    }
    ASSERT_EQ(s.size(), 0);
    delete it;
}

TEST_P(IndexTest, IteratorBeforeStart) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    PersistentIndex* pi = index->AsPersistentIndex();
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(0, 1);

    ASSERT_TRUE(pi->CreateIterator() == NULL);
}

TEST_P(IndexTest, IteratorEmptyIndex) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    PersistentIndex* pi = index->AsPersistentIndex();
    ASSERT_TRUE(pi->Start(StartContext()));

    IndexIterator* it = pi->CreateIterator();
    ASSERT_TRUE(it);

    uint64_t k;
    IntData v;
    size_t k_size = sizeof(k);
    enum lookup_result r = it->Next(&k, &k_size, &v);
    ASSERT_EQ(r, LOOKUP_NOT_FOUND);

    delete it;
}

/**
 * Checks that the iterator is returned an error if the index has been modified concurrently.
 */
TEST_P(IndexTest, IteratorConcurrentModification) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    PersistentIndex* pi = index->AsPersistentIndex();
    ASSERT_TRUE(pi->Start(StartContext()));
    // for all indexes that support an iterator

    EXPECT_LOGGING(dedupv1::test::ERROR).Once().Matches("Concurrent modification error");

    for (uint64_t i = 0; i < 100; i++) {
        IntData value;
        value.set_i(i);
        ASSERT_TRUE(pi->Put(&i, sizeof(i), value) != PUT_ERROR);
    }

    IndexIterator* it = pi->CreateIterator();
    ASSERT_TRUE(it);

    uint64_t k;
    IntData v;
    size_t k_size = sizeof(k);
    enum lookup_result r = it->Next(&k, &k_size, &v);
    ASSERT_EQ(r, LOOKUP_FOUND);

    uint64_t i = 11;
    IntData new_value;
    new_value.set_i(i + 1);
    ASSERT_TRUE(pi->Put(&i, sizeof(i), new_value) != PUT_ERROR);

    k = 0;
    k_size = sizeof(k);
    r = it->Next(&k, &k_size, &v);
    ASSERT_EQ(r, LOOKUP_ERROR);

    delete it;
}

/**
 * Checks that the iterator correctly returns LOOKUP_NOT_FOUND on an empty index
 */
TEST_P(IndexTest, IteratorEmpty) {
    if (!index->IsPersistent()) {
        // if an index is not persistent the create flag is meaning less
        return;
    }
    PersistentIndex* pi = index->AsPersistentIndex();
    ASSERT_TRUE(pi->Start(StartContext()));
    IndexIterator* it = pi->CreateIterator();
    ASSERT_TRUE(it);

    uint64_t k;
    IntData v;
    size_t k_size = sizeof(k);
    enum lookup_result r = it->Next(&k, &k_size, &v);
    ASSERT_EQ(r, LOOKUP_NOT_FOUND);

    delete it;
}

}
}
