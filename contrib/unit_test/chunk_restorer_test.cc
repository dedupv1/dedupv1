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
#include <chunk_index_restorer.h>

#include <core/chunk_index.h>
#include <core/chunk_mapping.h>
#include <core/dedup_system.h>
#include <core/dedupv1_scsi.h>
#include <core/fingerprinter.h>
#include <base/logging.h>
#include <base/memory.h>
#include <core/storage.h>
#include <base/strutil.h>
#include "dedupv1d.h"
#include <test_util/log_assert.h>

using std::map;
using std::string;
using std::vector;
using dedupv1::scsi::SCSI_OK;
using dedupv1::chunkindex::ChunkIndex;
using dedupv1::base::Index;
using dedupv1::base::PersistentIndex;
using dedupv1::base::IndexIterator;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::strutil::ToHexString;
using dedupv1d::Dedupv1d;

using namespace dedupv1;

LOGGER("ChunkRestorerTest");

class ChunkIndexRestorerTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Dedupv1d* system;

    dedupv1::contrib::restorer::ChunkIndexRestorer restorer;

    virtual void SetUp(){
        system = NULL;
    }

    virtual void TearDown() {
        EXPECT_TRUE(restorer.Close());
        if (system) {
            ASSERT_TRUE(system->Close());
        }
    }
};

TEST_F(ChunkIndexRestorerTest, Init) {
}

TEST_F(ChunkIndexRestorerTest, ChunkIndexRestorerRestore) {
    system = new Dedupv1d();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));
    ASSERT_TRUE(system->Run());
    INFO("Write data");
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

    // TODO (dmeister): Get the filename directly from the config file
    unlink("work/chunk-index");

    INFO("Restore data");
    // Start restorer with an empty chunk index. Restore the chunk index.
    ASSERT_TRUE(restorer.InitializeStorageAndChunkIndex("data/dedupv1_test.conf"));
    ASSERT_TRUE(restorer.RestoreChunkIndexFromContainerStorage());

    // Close down the restorer
    restorer.Close();
}

TEST_F(ChunkIndexRestorerTest, ChunkIndexRestorerFastShutdown) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Still").Repeatedly();

    system = new Dedupv1d();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));
    ASSERT_TRUE(system->Run());
    INFO("Write data");
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

    // TODO (dmeister): Get the filename directly from the config file
    unlink("work/chunk-index");

    INFO("Restore data");
    // Start restorer with an empty chunk index. Restore the chunk index.
    ASSERT_TRUE(restorer.InitializeStorageAndChunkIndex("data/dedupv1_test.conf"));
    ASSERT_TRUE(restorer.RestoreChunkIndexFromContainerStorage());

    // Close down the restorer
    restorer.Close();
}

TEST_F(ChunkIndexRestorerTest, ChunkIndexRestorerUsageCount) {
    system = new Dedupv1d();
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

    INFO("Before looking up mappings");
    // Restart System. The mark the system as dirty and enforce a log replay. That means that
    // after the start the usage count has to be correct

    dedupv1::StartContext start_context;
    start_context.set_create(dedupv1::StartContext::NON_CREATE);
    system = new Dedupv1d();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(start_context));

    // here we need the replay to that the usage counter is updated
    ASSERT_TRUE(system->dedup_system()->log()->PerformFullReplayBackgroundMode());

    // Store all chunk mappings in the vector to check them later
    ChunkIndex* chunk_index = system->dedup_system()->chunk_index();
    ASSERT_TRUE(chunk_index != NULL);
    ChunkMappingData chunk_mapping_data;
    size_t key_size = 20; // TODO(craupach) get the key size
    byte key[20];

    IndexIterator* iter = chunk_index->CreatePersistentIterator();
    ASSERT_TRUE(iter != NULL);
    map<string, uint64_t> reference_map;

    while (iter->Next(key, &key_size, &chunk_mapping_data) == LOOKUP_FOUND) {
        string fp;
        fp.assign(reinterpret_cast<char*>(key), key_size);

        INFO("Before usage count: fp " << ToHexString(fp.data(), fp.size()) <<
            ", usage count " << chunk_mapping_data.usage_count());

        reference_map[fp] = chunk_mapping_data.usage_count();
    }
    delete iter;
    ASSERT_GT(reference_map.size(), 0); // At least one chunk mapping present

    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext::WritebackStopContext()));
    ASSERT_TRUE(system->Stop());
    ASSERT_TRUE(system->Close());
    system = NULL;

    // TODO (dmeister): Get the filename directly from the config file
    unlink("work/chunk-index");

    INFO("Restore");
    // Start restorer with an empty chunk index. Restore the chunk index.
    ASSERT_TRUE(restorer.InitializeStorageAndChunkIndex("data/dedupv1_test.conf"));
    ASSERT_TRUE(restorer.RestoreChunkIndexFromContainerStorage());

    // Close down the restorer
    ASSERT_TRUE(restorer.Close());
    // Open yet another dedup system
    system = new Dedupv1d();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(start_context));
    chunk_index = system->dedup_system()->chunk_index();
    ASSERT_TRUE(chunk_index);
    // Test for all keys in the reference vector if they are present
    for (map<string, uint64_t>::iterator i = reference_map.begin(); i != reference_map.end(); i++) {
        int usage_count = i->second;

        ChunkMapping mapping(reinterpret_cast<const byte*>(i->first.data()), i->first.size());
        lookup_result result = chunk_index->Lookup(&mapping, false, NO_EC);
        EXPECT_EQ(result, LOOKUP_FOUND);

        INFO("Before usage count: fp " << ToHexString(i->first.data(), i->first.size()) <<
            ", should usage count " << usage_count <<
            ", actual usage count " << mapping.usage_count());

        EXPECT_EQ(usage_count, mapping.usage_count());
    }
}

TEST_F(ChunkIndexRestorerTest, ChunkIndexRestorerEmptyFingerprint) {
    system = new Dedupv1d();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));
    ASSERT_TRUE(system->Run());

    byte buffer[64 * 1024];
    memset(buffer, 0, 64 * 1024); // zero the written data

    DedupVolume* volume = system->dedup_system()->GetVolume(0);
    ASSERT_TRUE(volume);
    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));

    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext()));
    ASSERT_TRUE(system->Stop());
    ASSERT_TRUE(system->Close());
    system = NULL;

    // TODO (dmeister): Get the filename directly from the config file
    unlink("work/chunk-index");

    INFO("Restore");
    // Start restorer with an empty chunk index. Restore the chunk index.
    ASSERT_TRUE(restorer.InitializeStorageAndChunkIndex("data/dedupv1_test.conf"));
    ASSERT_TRUE(restorer.RestoreChunkIndexFromContainerStorage());

    // Close down the restorer
    ASSERT_TRUE(restorer.Close());
    // if the restore finished, without an error we are happy

}
