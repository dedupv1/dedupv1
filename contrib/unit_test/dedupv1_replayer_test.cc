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

LOGGER("Dedupv1ReplayerTest");

class Dedupv1ReplayerTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Dedupv1d* system;

    virtual void SetUp(){
        system = NULL;
    }

    virtual void TearDown() {
        if (system) {
            ASSERT_TRUE(system->Close());
        }
    }
};

TEST_F(Dedupv1ReplayerTest, Init) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("System not initialized").Once();
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    dedupv1::contrib::replay::Dedupv1Replayer replayer;
    ASSERT_FALSE(replayer.Initialize("data/dedupv1_test.conf")) <<
    "There is no system that can be replayed";
    ASSERT_TRUE(replayer.Close());
}

TEST_F(Dedupv1ReplayerTest, Replay) {

    // write some data to the system
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
    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext::FastStopContext()));
    ASSERT_TRUE(system->Stop());
    ASSERT_TRUE(system->Close());
    system = NULL;

    // replay
    dedupv1::contrib::replay::Dedupv1Replayer replayer;
    ASSERT_TRUE(replayer.Initialize("data/dedupv1_test.conf"));
    ASSERT_TRUE(replayer.Replay());
    ASSERT_TRUE(replayer.Close());
}
