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

#include "dedup_system_test.h"

#include <core/dedup_system.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <base/memory.h>
#include <core/dedupv1_scsi.h>
#include <base/barrier.h>

#include <tr1/tuple>

#include <json/json.h>

LOGGER("DedupSystem");

using std::tr1::tuple;
using std::tr1::make_tuple;
using std::pair;
using std::make_pair;
using std::string;
using std::vector;
using dedupv1::scsi::SCSI_OK;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::Barrier;

namespace dedupv1 {

TEST_F(DedupSystemTest, StartWithoutConfig) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    system = new DedupSystem();
    ASSERT_FALSE(system->Start(StartContext(), &info_store, &tp)) << "System should not start without configuration";
}

TEST_F(DedupSystemTest, DoubleStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    system = new DedupSystem();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));

    ASSERT_TRUE(system->Start(StartContext(), &info_store, &tp));
    ASSERT_FALSE(system->Start(StartContext(), &info_store, &tp));
}

TEST_F(DedupSystemTest, DoubleRun) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    system = new DedupSystem();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));

    ASSERT_TRUE(system->Start(StartContext(), &info_store, &tp));
    ASSERT_TRUE(system->Run());
    ASSERT_FALSE(system->Run());
}

TEST_F(DedupSystemTest, RunWithoutStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    system = new DedupSystem();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));

    ASSERT_FALSE(system->Run());
}

TEST_F(DedupSystemTest, MakeReadRequestWithOffsetInEmptySystem) {
    system = new DedupSystem();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(StartContext(), &info_store, &tp));
    ASSERT_TRUE(system->Run());

    byte* buffer = new byte[4096];
    memset(buffer, 0, 4096);

    byte* result = new byte[4096];
    memset(result, 0, 4096);

    DedupVolume* volume = system->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_READ, 4096, 4096, result, NO_EC));
    for (int i = 0; i < 4096; i++) {
        char error_buffer[100];
        sprintf(error_buffer, "Contents are not the same at index %d",i);
        ASSERT_EQ(result[i], buffer[i]) << error_buffer;
    }
    delete[] buffer;
    delete[] result;
}

/**
 * Unit test that tests if there is not container mismatch when the same
 * chunk is written multiple times in the same request.
 *
 * As long there is not Log assertion mechanism, the output of the unit test
 * has to be checked manually.
 */
TEST_F(DedupSystemTest, ContainerMismatchSameRequest) {
    system = new DedupSystem();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(StartContext(), &info_store, &tp));
    ASSERT_TRUE(system->Run());

    size_t buffer_size = system->block_size();
    byte* buffer = new byte[buffer_size];

    DedupVolume* volume = system->GetVolume(0);
    ASSERT_TRUE(volume);

    for (int i = 0; i < 128; i++) {
        memset(buffer, i, buffer_size);
        ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, i * buffer_size, buffer_size, buffer, NO_EC));
    }
    delete[] buffer;
    if (system) {
        ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
        delete system;
    }
    system = NULL;
}

bool MakeSameRequestRunner(tuple<DedupSystem*, Barrier*, int> args) {
    DedupSystem* system = std::tr1::get<0>(args);
    Barrier* barrier = std::tr1::get<1>(args);
    int thread_id = std::tr1::get<2>(args);
    CHECK(system, "System not set");
    CHECK(barrier, "Barrier not set");

    size_t buffer_size = system->block_size();
    byte* buffer = new byte[buffer_size];
    CHECK(buffer != NULL, "Failed to alloc buffer");

    DedupVolume* volume = system->GetVolume(0);
    CHECK(volume, "Failed to find volume");

    for (int i = 0; i < 256; i++) {
        memset(buffer, i, buffer_size);

        barrier->Wait();

        uint64_t block_id = (thread_id * 256) + i;
        CHECK(volume->MakeRequest(REQUEST_WRITE, block_id * system->block_size(), buffer_size, buffer, NO_EC),
            "Failed to make write request");
    }
    delete[] buffer;
    return true;
}

/**
 * Unit test that tests if there is not container mismatch when the same
 * chunk is written multiple times by different threads at the same time
 *
 * As long there is not Log assertion mechanism, the output of the unit test
 * has to be checked manually.
 */
TEST_F(DedupSystemTest, ContainerMismatchMultipleThreads) {
    system = new DedupSystem();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(StartContext(), &info_store, &tp));
    ASSERT_TRUE(system->Run());

    Barrier barrier(2);

    Thread<bool> t1(NewRunnable(MakeSameRequestRunner, make_tuple(system, &barrier, 0)), "write thread 1");
    Thread<bool> t2(NewRunnable(MakeSameRequestRunner, make_tuple(system, &barrier, 1)), "write thread 2");

    ASSERT_TRUE(t1.Start());
    ASSERT_TRUE(t2.Start());

    bool r = false;
    ASSERT_TRUE(t1.Join(&r));
    ASSERT_TRUE(r) << "thread 1 exited with an error";
    ASSERT_TRUE(t2.Join(&r));
    ASSERT_TRUE(r) << "thread 2 exited with an error";
}

}
