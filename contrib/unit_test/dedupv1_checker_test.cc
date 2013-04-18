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

#include <map>
#include <vector>
#include <string>
#include <iostream>

#include <dedupv1.pb.h>
#include <gtest/gtest.h>

#include <dedupv1_checker.h>
#include <dedupv1_replayer.h>

#include <core/chunk_index.h>
#include <core/chunk_mapping.h>
#include <core/dedup_system.h>
#include <core/dedupv1_scsi.h>
#include <base/logging.h>
#include <base/memory.h>
#include <core/storage.h>
#include <base/strutil.h>
#include <base/index.h>
#include "dedupv1d.h"
#include <test_util/log_assert.h>

using std::map;
using std::string;
using std::vector;
using dedupv1::scsi::SCSI_OK;
using dedupv1::chunkindex::ChunkIndex;
using dedupv1::gc::GarbageCollector;
using dedupv1::base::Index;
using dedupv1::base::PersistentIndex;
using dedupv1::base::IndexIterator;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::strutil::ToHexString;
using dedupv1d::Dedupv1d;
using dedupv1::contrib::check::Dedupv1Checker;

using namespace dedupv1;
LOGGER("Dedupv1CheckerTest");

class Dedupv1CheckerTest : public testing::TestWithParam<uint32_t> {
protected:
    USE_LOGGING_EXPECTATION();

    Dedupv1d* system;
    uint32_t passes;

    virtual void SetUp() {
        system = NULL;
    }

    virtual void TearDown() {
        if (system) {
            ASSERT_TRUE(system->Close());
        }
    }
};

TEST_P(Dedupv1CheckerTest, Init)
{
    passes = GetParam();
    Dedupv1Checker checker(false, false);
    ASSERT_TRUE(checker.set_passes(passes));
}

TEST_P(Dedupv1CheckerTest, CheckWithUnreplayedLog)
{
    passes = GetParam();
    // write some data to the system
    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));
    ASSERT_TRUE(system->Run());
    FILE* file = fopen("data/random","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    DedupVolume* volume = system->dedup_system()->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));
    fclose(file);
    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext::FastStopContext()));
    ASSERT_TRUE(system->Stop());
    ASSERT_TRUE(system->Close());
    system = NULL;

    Dedupv1Checker checker(false, true);
    checker.set_passes(passes);
    EXPECT_TRUE(checker.Initialize("data/dedupv1_test.conf"));
    EXPECT_TRUE(checker.Check());
    uint64_t number_of_chunks = checker.dedupv1d()->dedup_system()->chunk_index()->GetPersistentCount();
    ASSERT_EQ(checker.get_all_pass_processed_chunks(), number_of_chunks);
    ASSERT_EQ(checker.get_all_pass_skipped_chunks(), number_of_chunks * (checker.passes() - 1));
    EXPECT_TRUE(checker.Close());
}

TEST_P(Dedupv1CheckerTest, Check)
{
    passes = GetParam();

    // write some data to the system
    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));
    ASSERT_TRUE(system->Run());
    FILE* file = fopen("data/random","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    DedupVolume* volume = system->dedup_system()->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));
    fclose(file);
    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext::FastStopContext()));
    ASSERT_TRUE(system->Stop());
    ASSERT_TRUE(system->Close());
    system = NULL;

    // replay
    dedupv1::contrib::replay::Dedupv1Replayer replayer;
    ASSERT_TRUE(replayer.Initialize("data/dedupv1_test.conf"));
    ASSERT_TRUE(replayer.Replay());
    ASSERT_TRUE(replayer.Close());

    Dedupv1Checker checker(false, false);
    checker.set_passes(passes);
    EXPECT_TRUE(checker.Initialize("data/dedupv1_test.conf"));
    EXPECT_TRUE(checker.Check());
    uint64_t number_of_chunks = checker.dedupv1d()->dedup_system()->chunk_index()->GetPersistentCount();
    ASSERT_EQ(checker.get_all_pass_processed_chunks(), number_of_chunks);
    ASSERT_EQ(checker.get_all_pass_skipped_chunks(), number_of_chunks * (checker.passes() - 1));
    ASSERT_EQ(0, checker.reported_errors());
    EXPECT_TRUE(checker.Close());
}

TEST_P(Dedupv1CheckerTest, CheckWithChunkDataAddressError)
{
    passes = GetParam();
    EXPECT_LOGGING(dedupv1::test::WARN).Repeatedly();

    // write some data to the system
    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));
    ASSERT_TRUE(system->Run());
    FILE* file = fopen("data/random","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    DedupVolume* volume = system->dedup_system()->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));
    fclose(file);
    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext()));
    ASSERT_TRUE(system->Stop());
    ASSERT_TRUE(system->Close());
    system = NULL;

    // replay
    dedupv1::contrib::replay::Dedupv1Replayer replayer;
    ASSERT_TRUE(replayer.Initialize("data/dedupv1_test.conf"));
    ASSERT_TRUE(replayer.Replay());
    ASSERT_TRUE(replayer.Close());

    // open to introduce an error
    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(system->Start(start_context));

    dedupv1::chunkindex::ChunkIndex* chunk_index = system->dedup_system()->chunk_index();

    // get a fingerprint
    dedupv1::base::IndexIterator* i = chunk_index->CreatePersistentIterator();
    ASSERT_TRUE(i);
    size_t fp_size = 20;
    byte fp[fp_size];
    ChunkMappingData chunk_data;
    ASSERT_EQ(dedupv1::base::LOOKUP_FOUND, i->Next(fp, &fp_size, &chunk_data));
    delete i;

    chunk_data.set_data_address(0); // a wrong data address
    dedupv1::base::PersistentIndex* persitent_chunk_index =
        chunk_index->persistent_index();
    ASSERT_EQ(persitent_chunk_index->Put(fp, fp_size, chunk_data), dedupv1::base::PUT_OK);

    ASSERT_TRUE(system->Close());
    system = NULL;

    // replay 2
    dedupv1::contrib::replay::Dedupv1Replayer replayer2;
    ASSERT_TRUE(replayer2.Initialize("data/dedupv1_test.conf"));
    ASSERT_TRUE(replayer2.Replay());
    ASSERT_TRUE(replayer2.Close());

    // check
    Dedupv1Checker checker(false, false);
    checker.set_passes(passes);
    EXPECT_TRUE(checker.Initialize("data/dedupv1_test.conf"));
    EXPECT_TRUE(checker.Check());
    uint64_t number_of_chunks = checker.dedupv1d()->dedup_system()->chunk_index()->GetPersistentCount();
    ASSERT_EQ(checker.get_all_pass_processed_chunks(), number_of_chunks);
    ASSERT_EQ(checker.get_all_pass_skipped_chunks(), number_of_chunks * (checker.passes() - 1));
    ASSERT_GT(checker.reported_errors(), 0);
    EXPECT_TRUE(checker.Close());
}

/**
 * Tests if the dedupv1 checker can finds a "Unused chunk is not gc candidate" error
 */
TEST_P(Dedupv1CheckerTest, CheckWithNoGCCandidateError)
{
    passes = GetParam();
    EXPECT_LOGGING(dedupv1::test::WARN).Repeatedly();

    // write some data to the system
    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));
    ASSERT_TRUE(system->dedup_system()->garbage_collector()->PauseProcessing());
    ASSERT_TRUE(system->Run());
    FILE* file = fopen("data/random","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    DedupVolume* volume = system->dedup_system()->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));

    // overwrite with zeros
    memset(buffer, 0, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));

    fclose(file);
    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext()));
    ASSERT_TRUE(system->Stop());
    ASSERT_TRUE(system->Close());
    system = NULL;

    // replay
    dedupv1::contrib::replay::Dedupv1Replayer replayer;
    ASSERT_TRUE(replayer.PauseGC());
    ASSERT_TRUE(replayer.Initialize("data/dedupv1_test.conf"));
    ASSERT_TRUE(replayer.Replay());
    ASSERT_TRUE(replayer.Close());

    // open to introduce an error
    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(system->Start(start_context));

    dedupv1::base::PersistentIndex* candidate_info = system->dedup_system()->garbage_collector()->candidate_info();
    ASSERT_TRUE(candidate_info);
    dedupv1::base::IndexIterator* i = candidate_info->CreateIterator();
    ASSERT_TRUE(i);
    size_t key_size = 1024;
    byte key[1024];
    ASSERT_EQ(dedupv1::base::LOOKUP_FOUND, i->Next(key, &key_size, NULL));
    delete i;
    candidate_info->Delete(key, key_size);
    ASSERT_TRUE(system->Close());
    system = NULL;

    // replay 2
    dedupv1::contrib::replay::Dedupv1Replayer replayer2;
    ASSERT_TRUE(replayer2.PauseGC());
    ASSERT_TRUE(replayer2.Initialize("data/dedupv1_test.conf"));
    ASSERT_TRUE(replayer2.Replay());
    ASSERT_TRUE(replayer2.Close());

    // check
    Dedupv1Checker checker(false, false);
    checker.set_passes(passes);
    EXPECT_TRUE(checker.Initialize("data/dedupv1_test.conf"));
    EXPECT_TRUE(checker.Check());
    uint64_t number_of_chunks = checker.dedupv1d()->dedup_system()->chunk_index()->GetPersistentCount();
    ASSERT_EQ(checker.get_all_pass_processed_chunks(), number_of_chunks);
    ASSERT_EQ(checker.get_all_pass_skipped_chunks(), number_of_chunks * (checker.passes() - 1));
    ASSERT_GT(checker.reported_errors(), 0);
    EXPECT_TRUE(checker.Close());
}

/**
 * Tests if dedupv1 check can repair if the data address of a chunk is incorrect
 */
TEST_P(Dedupv1CheckerTest, RepairWithChunkDataAddressError)
{
    passes = GetParam();
    EXPECT_LOGGING(dedupv1::test::WARN).Repeatedly();

    // write some data to the system
    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));
    ASSERT_TRUE(system->Run());
    FILE* file = fopen("data/random","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    DedupVolume* volume = system->dedup_system()->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));
    fclose(file);
    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext()));
    ASSERT_TRUE(system->Stop());
    ASSERT_TRUE(system->Close());
    system = NULL;

    // replay
    dedupv1::contrib::replay::Dedupv1Replayer replayer;
    ASSERT_TRUE(replayer.Initialize("data/dedupv1_test.conf"));
    ASSERT_TRUE(replayer.Replay());
    ASSERT_TRUE(replayer.Close());

    // open to introduce an error
    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(system->Start(start_context));

    dedupv1::chunkindex::ChunkIndex* chunk_index = system->dedup_system()->chunk_index();

    // get a fingerprint
    dedupv1::base::IndexIterator* i = chunk_index->CreatePersistentIterator();
    ASSERT_TRUE(i);
    size_t fp_size = 20;
    byte fp[fp_size];
    ChunkMappingData chunk_data;
    ASSERT_EQ(dedupv1::base::LOOKUP_FOUND, i->Next(fp, &fp_size, &chunk_data));
    delete i;

    chunk_data.set_data_address(0); // a wrong data address
    dedupv1::base::PersistentIndex* persitent_chunk_index =
        chunk_index->persistent_index();
    ASSERT_EQ(dedupv1::base::PUT_OK, persitent_chunk_index->Put(fp, fp_size, chunk_data));

    ASSERT_TRUE(system->Close());
    system = NULL;

    // replay 2
    dedupv1::contrib::replay::Dedupv1Replayer replayer2;
    ASSERT_TRUE(replayer2.Initialize("data/dedupv1_test.conf"));
    ASSERT_TRUE(replayer2.Replay());
    ASSERT_TRUE(replayer2.Close());

    // repair
    Dedupv1Checker checker(false, true);
    checker.set_passes(passes);
    EXPECT_TRUE(checker.Initialize("data/dedupv1_test.conf"));
    EXPECT_TRUE(checker.Check());
    uint64_t number_of_chunks = checker.dedupv1d()->dedup_system()->chunk_index()->GetPersistentCount();
    ASSERT_EQ(checker.get_all_pass_processed_chunks(), number_of_chunks);
    ASSERT_EQ(checker.get_all_pass_skipped_chunks(), number_of_chunks * (checker.passes() - 1));
    ASSERT_GT(checker.fixed_errors(), 0);
    ASSERT_EQ(checker.fixed_errors(), checker.reported_errors());
    EXPECT_TRUE(checker.Close());
}

/**
 * Tests if the dedupv1 checker can repair "Wrong usage count" error
 *
 * This test handles usage counts increased and decreased by 1 and a usage_count bigger than 2^32 in chunk index.
 *
 * TODO(fermat): set up a way to to test what happens if the number of times a chunk is used is more ten 2^32 bigger then the usage count
 * TODO(fermat): check what happens if there are more then 2^8 chunks in one prefix
 * TODO(fermat): build a mock for the indices and the system
 */
TEST_P(Dedupv1CheckerTest, RepairWithUsageCountError)
{
    passes = GetParam();
    EXPECT_LOGGING(dedupv1::test::WARN).Repeatedly();

    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));

    if (system->dedup_system()->garbage_collector()->gc_concept() != GarbageCollector::USAGE_COUNT) {
        // skip this test
        ASSERT_TRUE(system->Close());
        return;
    }

    ASSERT_TRUE(system->Run());
    FILE* file = fopen("data/random","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    DedupVolume* volume = system->dedup_system()->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));

    fclose(file);
    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext()));
    ASSERT_TRUE(system->Stop());
    ASSERT_TRUE(system->Close());
    system = NULL;

    // replay
    dedupv1::contrib::replay::Dedupv1Replayer replayer;
    ASSERT_TRUE(replayer.Initialize("data/dedupv1_test.conf"));
    ASSERT_TRUE(replayer.Replay());
    ASSERT_TRUE(replayer.Close());

    // open to introduce an error
    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(system->Start(start_context));

    dedupv1::chunkindex::ChunkIndex* chunk_index = system->dedup_system()->chunk_index();
    ASSERT_LE(3, chunk_index->GetPersistentCount());

    dedupv1::base::IndexIterator* i = chunk_index->CreatePersistentIterator();
    ASSERT_TRUE(i);
    size_t fp_size = 20;
    byte fp_increased[fp_size], fp_decreased[fp_size], fp_extrem_high[fp_size];
    ChunkMappingData chunk_data_decreased, chunk_data_increased, chunk_data_extrem_high;
    ASSERT_EQ(dedupv1::base::LOOKUP_FOUND, i->Next(fp_decreased, &fp_size, &chunk_data_decreased));
    ASSERT_EQ(dedupv1::base::LOOKUP_FOUND, i->Next(fp_increased, &fp_size, &chunk_data_increased));
    ASSERT_EQ(dedupv1::base::LOOKUP_FOUND, i->Next(fp_extrem_high, &fp_size, &chunk_data_extrem_high));
    delete i;

    uint64_t extrem_usage_count = 1;
    extrem_usage_count <<= 40;
    chunk_data_decreased.set_usage_count(chunk_data_decreased.usage_count() - 1);
    chunk_data_increased.set_usage_count(chunk_data_increased.usage_count() + 1);
    chunk_data_extrem_high.set_usage_count(extrem_usage_count);

    dedupv1::base::PersistentIndex* persitent_chunk_index =
        chunk_index->persistent_index();

    ASSERT_EQ(dedupv1::base::PUT_OK,
        persitent_chunk_index->Put(fp_increased, fp_size, chunk_data_increased));
    ASSERT_EQ(dedupv1::base::PUT_OK,
        persitent_chunk_index->Put(fp_decreased, fp_size, chunk_data_decreased));
    ASSERT_EQ(dedupv1::base::PUT_OK,
        persitent_chunk_index->Put(fp_extrem_high, fp_size, chunk_data_extrem_high));

    ASSERT_TRUE(system->Close());
    system = NULL;

    // replay 2
    dedupv1::contrib::replay::Dedupv1Replayer replayer2;
    ASSERT_TRUE(replayer2.Initialize("data/dedupv1_test.conf"));
    ASSERT_TRUE(replayer2.Replay());
    ASSERT_TRUE(replayer2.Close());

    // repair
    Dedupv1Checker checker(false, true);
    ASSERT_TRUE(checker.set_passes(passes));
    EXPECT_TRUE(checker.Initialize("data/dedupv1_test.conf"));
    EXPECT_TRUE(checker.Check());
    uint64_t number_of_chunks = checker.dedupv1d()->dedup_system()->chunk_index()->GetPersistentCount();
    ASSERT_EQ(checker.get_all_pass_processed_chunks(), number_of_chunks);
    ASSERT_EQ(checker.get_all_pass_skipped_chunks(), number_of_chunks * (checker.passes() - 1));
    ASSERT_GT(checker.fixed_errors(), 0);
    ASSERT_EQ(checker.fixed_errors(), checker.reported_errors());
    EXPECT_TRUE(checker.Close());

}

/**
 * Tests if the dedupv1 checker can repair a "Unused chunk is not gc candidate" error
 */
TEST_P(Dedupv1CheckerTest, RepairWithNoGCCandidateError)
{
    passes = GetParam();
    EXPECT_LOGGING(dedupv1::test::WARN).Repeatedly();

    // write some data to the system
    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));

    if (system->dedup_system()->garbage_collector()->gc_concept() != GarbageCollector::USAGE_COUNT) {
        // skip this test
        ASSERT_TRUE(system->Close());
        return;
    }

    ASSERT_TRUE(system->Run());
    FILE* file = fopen("data/random","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    DedupVolume* volume = system->dedup_system()->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));

    // overwrite with zeros
    memset(buffer, 0, 64 * 1024);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));

    fclose(file);
    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext()));
    ASSERT_TRUE(system->Stop());
    ASSERT_TRUE(system->Close());
    system = NULL;

    INFO("Stopped dedupv1d");

    // replay
    dedupv1::contrib::replay::Dedupv1Replayer replayer;
    ASSERT_TRUE(replayer.Initialize("data/dedupv1_test.conf"));

    // Here I pause the gc to avoid that the item is processed before the error is injected.
    ASSERT_TRUE(replayer.PauseGC());
    ASSERT_TRUE(replayer.Replay());
    ASSERT_TRUE(replayer.Close());

    INFO("Replay finished");

    // open to introduce an error
    system = new Dedupv1d();
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->Init());
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(system->Start(start_context));

    dedupv1::base::PersistentIndex* candidate_info = system->dedup_system()->garbage_collector()->candidate_info();
    ASSERT_TRUE(candidate_info);
    dedupv1::base::IndexIterator* i = candidate_info->CreateIterator();
    ASSERT_TRUE(i);

    GarbageCollectionCandidateData candidate_data;
    size_t key_size = 1024;
    byte key[1024];
    lookup_result lr = i->Next(key, &key_size, &candidate_data);
    ASSERT_NE(dedupv1::base::LOOKUP_ERROR, lr);

    bool process_with_test = lr == dedupv1::base::LOOKUP_FOUND;
    // when we deleted the gc candidate earlier, we cannot delete it

    delete i;

    if (process_with_test) {
        DEBUG("Delete " << dedupv1::base::strutil::ToHexString(key, key_size) << " as gc candidate: " <<
            candidate_data.ShortDebugString());
        dedupv1::base::delete_result dr = candidate_info->Delete(key, key_size);
        EXPECT_EQ(dedupv1::base::DELETE_OK, dr);
    }
    ASSERT_TRUE(system->Close());
    system = NULL;

    INFO("Error injected");

    if (!process_with_test) {
        return;
    }

    INFO("Perform check");

    // repair
    Dedupv1Checker checker(false, true);
    checker.set_passes(passes);
    EXPECT_TRUE(checker.Initialize("data/dedupv1_test.conf"));
    EXPECT_TRUE(checker.Check());
    uint64_t number_of_chunks = checker.dedupv1d()->dedup_system()->chunk_index()->GetPersistentCount();
    EXPECT_EQ(checker.get_all_pass_processed_chunks(), number_of_chunks);
    EXPECT_EQ(checker.get_all_pass_skipped_chunks(), number_of_chunks * (checker.passes() - 1));
    EXPECT_GT(checker.fixed_errors(), 0);
    EXPECT_EQ(checker.fixed_errors(), checker.reported_errors());
    EXPECT_TRUE(checker.Close());
}

INSTANTIATE_TEST_CASE_P(Dedupv1Checker,
    Dedupv1CheckerTest,
    ::testing::Values(0U, 1U, 2U, 3U, 4U))
// passes
;
