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

#include "filter_test.h"
#include "dedup_system_test.h"
#include <gtest/gtest.h>
#include <core/filter.h>
#include <core/dedup_system.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <base/memory.h>
#include <core/storage.h>
#include <core/dedupv1_scsi.h>
#include <base/thread.h>
#include <base/runnable.h>

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <string>
#include <vector>
#include <cryptopp/cryptlib.h>
#include <cryptopp/rng.h>
#include <json/json.h>
#include <tr1/tuple>

using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using std::tr1::tuple;
using std::tr1::make_tuple;
using std::string;
using std::vector;
using dedupv1::base::strutil::Split;
using dedupv1::scsi::SCSI_OK;
using dedupv1::base::ScopedArray;
using dedupv1::MemoryPersistentStatistics;
using std::string;
using dedupv1::DedupSystemTest;
using namespace CryptoPP;

LOGGER("BlockIndexFilterTest");

namespace dedupv1 {
namespace filter {

INSTANTIATE_TEST_CASE_P(BlockIndexFilter,
    FilterTest,
    ::testing::Values("block-index-filter",
      "block-index-filter;block-chunk-cache=true"));

INSTANTIATE_TEST_CASE_P(BlockIndexFilter,
    DedupSystemTest,
    ::testing::Values("data/dedupv1_blc_test.conf"));

class BlockIndexFilterTest : public testing::Test  {
protected:
    USE_LOGGING_EXPECTATION();

    DedupSystem* system;
    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;

    virtual void SetUp() {
        system = NULL;

        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());
    }

    virtual void TearDown() {
        if (system) {
            ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
            ASSERT_TRUE(system->Close());
        }
    }
};

/**
 * This test checks if the block index filter works correctly with regards to the garbage collection.
 * Especially it checks if it is ok for the gc that we have a FILTER_STRONG_MAYBE result without that
 * the chunk being in the auxiliary index (or cache if #392 is applied) which may be the case after
 * a replay.
 */
TEST_F(BlockIndexFilterTest, OverwriteAfterReplay) {
    system = DedupSystemTest::CreateDefaultSystem("data/dedupv1_test.conf", &info_store, &tp);
    ASSERT_TRUE(system);

    ASSERT_TRUE(system->filter_chain()->GetFilterByName("block-index-filter") != NULL) <<
    "block index filter not configured";

    int size = 16 * 1024 * 1024;
    int requests = size / system->block_size();

    byte* buffer = new byte[size];
    ScopedArray<byte> scoped_buffer(buffer); // scope frees buffer

    LC_RNG rng(1024);
    rng.GenerateBlock(buffer, size);

    byte* result = new byte[size];
    ScopedArray<byte> scoped_result(result); // scope frees buffer
    memset(result, 0, size);

    DedupVolume* volume = system->GetVolume(0);
    ASSERT_TRUE(volume);
    for (int i = 0; i < requests; i++) {
        ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, i * system->block_size(), system->block_size(), buffer + (i * system->block_size()), NO_EC));
    }
    volume = NULL;
    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode(true));
    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
    ASSERT_TRUE(system->Close());
    system = NULL;

    system = DedupSystemTest::CreateDefaultSystem("data/dedupv1_test.conf", &info_store, &tp, true, true);
    ASSERT_TRUE(system);

    volume = system->GetVolume(0);
    ASSERT_TRUE(volume);
    for (int i = 0; i < requests; i++) {
        // copy the first half of the request to the second half
        byte* request_buffer = buffer + (i * system->block_size());
        memcpy(request_buffer + (system->block_size() / 2), request_buffer, (system->block_size() / 2));

        ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, i * system->block_size(), system->block_size(), request_buffer, NO_EC));
    }
    volume = NULL;

    ASSERT_TRUE(system->log()->PerformFullReplayBackgroundMode(true));
}

}
}
