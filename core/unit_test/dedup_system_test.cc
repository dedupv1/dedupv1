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
using namespace CryptoPP;

LOGGER("DedupSystemTest");

namespace dedupv1 {

void DedupSystemTest::SetUp() {
    system = NULL;

    ASSERT_TRUE(tp.SetOption("size", "8"));
    ASSERT_TRUE(tp.Start());
}

void DedupSystemTest::TearDown() {
    if (system) {
        ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
        ASSERT_TRUE(system->Close());
    }
}

DedupSystem* DedupSystemTest::CreateDefaultSystem(string config_option,
                                                  dedupv1::InfoStore* info_store,
                                                  dedupv1::base::Threadpool* tp,
                                                  bool start,
                                                  bool restart,
                                                  bool crashed,
                                                  bool dirty,
                                                  bool full_replay) {
    CHECK_RETURN(info_store, NULL, "Info store not set");
    CHECK_RETURN(tp != NULL, NULL, "Threadpool not set");

    vector<string> options;
    CHECK_RETURN(Split(config_option, ";", &options), NULL, "Failed to split: " << config_option);

    DedupSystem* system = new DedupSystem();
    CHECK_RETURN(system, NULL, "Cannot create system");
    CHECK_RETURN(system->Init(), NULL, "Failed to init system");
    CHECK_RETURN(system->LoadOptions(options[0]), NULL, "Cannot load options");

    for (size_t i = 1; i < options.size(); i++) {
        string option_name;
        string option;
        CHECK_RETURN(Split(options[i], "=", &option_name, &option), NULL, "Failed to split " << options[i]);
        CHECK_RETURN(system->SetOption(option_name, option), NULL, "Failed set option: " << options[i]);
    }

    if (start) {
        StartContext start_context;
        if (dirty) {
            start_context.set_dirty(StartContext::DIRTY);
        }
        if (crashed) {
            start_context.set_crashed(true);
        }
        if (restart) {
            start_context.set_create(StartContext::NON_CREATE);
        }
        CHECK_RETURN(system->Start(start_context, info_store, tp), NULL, "Cannot start system");
        if (dirty) {
            system->log()->PerformDirtyReplay();
        }
        if (full_replay) {
            CHECK_RETURN(system->log()->PerformFullReplayBackgroundMode(), NULL, "Failed to replay log");
        }
        CHECK_RETURN(system->Run(), NULL, "Cannot run system");
    }
    return system;
}

TEST_P(DedupSystemTest, LoadConfig) {
    string p = GetParam();
    system = CreateDefaultSystem(p, &info_store, &tp);
    ASSERT_TRUE(system) << "Could not create default system with options: " << p;
    ASSERT_TRUE(system->volume_info() != NULL);
}

TEST_P(DedupSystemTest, PrintStatistics) {
    string p = GetParam();
    system = CreateDefaultSystem(p, &info_store, &tp);
    ASSERT_TRUE(system);

    string s = system->PrintStatistics();
    ASSERT_TRUE(s.size() > 0);

    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) << "Failed to parse: " << reader.getFormatedErrorMessages() << ": " << s;
}

TEST_P(DedupSystemTest, PrintProfile) {
    string p = GetParam();
    system = CreateDefaultSystem(p, &info_store, &tp);
    ASSERT_TRUE(system);

    string s = system->PrintProfile();
    ASSERT_TRUE(s.size() > 0);

    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) << "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_P(DedupSystemTest, PrintLockStatistics) {
    string p = GetParam();
    system = CreateDefaultSystem(p, &info_store, &tp);
    ASSERT_TRUE(system);

    string s = system->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) << "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_P(DedupSystemTest, PrintTrace) {
    string p = GetParam();
    system = CreateDefaultSystem(p, &info_store, &tp);
    ASSERT_TRUE(system);

    string s = system->PrintTrace();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) << "Failed to parse configuration: " << "trace " << s << "error " << reader.getFormatedErrorMessages();
}

static Json::Value ToJson(const string& s) {
    Json::Value root;
    Json::Reader reader;
    reader.parse( s, root );
    return root;
}

TEST_P(DedupSystemTest, PersistentStatistics) {
    system = CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    FILE* file = fopen("data/rabin-test","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    byte result[64 * 1024];
    memset(result, 0, 64 * 1024);

    DedupVolume* volume = system->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));
    ASSERT_TRUE(volume->MakeRequest(REQUEST_READ, 0, 64 * 1024, result, NO_EC));
    for (int i = 0; i < 64 * 1024; i++) {
        ASSERT_EQ(result[i], buffer[i]) << "Contents are not the same at index " << i;
    }
    ASSERT_TRUE(memcmp(result, buffer, 64 * 1024) == 0) << "Written and readed contents is the same";

    fclose(file);

    sleep(10);
    ASSERT_TRUE(system->storage()->Flush(NO_EC));
    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));

    Json::Value s1 = ToJson(system->PrintStatistics());
    s1["block index"]["auxiliary index fill ratio"] = 0;
    s1["block index"]["index fill ratio"] = 0;
    s1["chunk index"]["auxiliary index fill ratio"] = 0;
    s1["chunk index"]["index item count"] = 0;
    s1["chunk index"]["index fill ratio"] = 0;
    s1["log"]["fill ratio"] = 0;
    s1["idle"]["idle time"] = 0;
    s1["chunk store"]["storage"]["allocator"]["file"] = 0;

    MemoryPersistentStatistics mps;
    ASSERT_TRUE(system->PersistStatistics("dedup", &mps));
    if (system) {
        ASSERT_TRUE(system->Close());
        system = NULL;
    }

    system = CreateDefaultSystem(GetParam(), &info_store, &tp, true, true);
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->RestoreStatistics("dedup", &mps));

    Json::Value s2 = ToJson(system->PrintStatistics());
    s2["block index"]["auxiliary index fill ratio"] = 0;
    s2["block index"]["index fill ratio"] = 0;
    s2["chunk index"]["auxiliary index fill ratio"] = 0;
    s2["chunk index"]["index item count"] = 0;
    s2["chunk index"]["index fill ratio"] = 0;
    s2["log"]["fill ratio"] = 0;
    s2["idle"]["idle time"] = 0;
    s2["chunk store"]["storage"]["allocator"]["file"] = 0;

    ASSERT_EQ(s1, s2) << "Statistics should be persistent";
}

TEST_P(DedupSystemTest, MakeRequest) {
    system = CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    FILE* file = fopen("data/rabin-test","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    byte result[64 * 1024];
    memset(result, 0, 64 * 1024);

    DedupVolume* volume = system->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));
    ASSERT_TRUE(volume->MakeRequest(REQUEST_READ, 0, 64 * 1024, result, NO_EC));
    for (int i = 0; i < 64 * 1024; i++) {
        char error_buffer[100];
        sprintf(error_buffer, "Contents are not the same at index %d",i);
        ASSERT_EQ(result[i], buffer[i]) << error_buffer;
    }
    ASSERT_TRUE(memcmp(result, buffer, 64 * 1024) == 0) << "Writen and readed contents is the same";

    fclose(file);
}

TEST_P(DedupSystemTest, MakeLargeRequest) {
    system = CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

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
    for (int i = 0; i < requests; i++) {
        ASSERT_TRUE(volume->MakeRequest(REQUEST_READ, i * system->block_size(), system->block_size(), result + (i * system->block_size()), NO_EC));
    }
    for (int i = 0; i < size; i++) {
        char error_buffer[100];
        sprintf(error_buffer, "Contents are not the same at index %d",i);
        ASSERT_EQ(result[i], buffer[i]) << error_buffer;
    }

    DEBUG(system->PrintProfile());
}

bool PrintStatisticsLoop(DedupSystem* system, bool* stop_flag) {
    while (!(*stop_flag)) {
        system->PrintStatistics();
        dedupv1::base::ThreadUtil::Yield();
    }
    return true;
}

TEST_P(DedupSystemTest, StatisticsDuringStartup) {
    system = CreateDefaultSystem(GetParam(), &info_store, &tp, false);
    ASSERT_TRUE(system);

    bool stop_flag = false;
    Thread<bool> stats_thread(NewRunnable(&PrintStatisticsLoop, system, &stop_flag), "stats");
    ASSERT_TRUE(stats_thread.Start());

    EXPECT_TRUE(system->Start(StartContext(), &info_store, &tp));

    stop_flag = true;
    ASSERT_TRUE(stats_thread.Join(NULL));
    // it is ok if the done crash
}

TEST_P(DedupSystemTest, MakeReallyLargeRequest) {
    system = CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    int size = 128 * 1024 * 1024;
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
    dedupv1::base::ErrorContext ec;
    for (int i = 0; i < requests; i++) {
        ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE,
                i * system->block_size(),
                system->block_size(),
                buffer + (i * system->block_size()),
                &ec)) << "Write " << i << " failed";
    }

    for (int i = 0; i < requests; i++) {
        ASSERT_TRUE(volume->MakeRequest(REQUEST_READ,
                i * system->block_size(),
                system->block_size(),
                result + (i * system->block_size()),
                &ec)) << "Read " << i << " failed";
    }

    for (int i = 0; i < size; i++) {
        char error_buffer[100];
        sprintf(error_buffer, "Contents are not the same at index %d",i);
        ASSERT_EQ(result[i], buffer[i]) << error_buffer;
    }
    DEBUG(system->PrintProfile());
    DEBUG(system->PrintLockStatistics());
}

TEST_P(DedupSystemTest, OverwriteRequests) {
    system = CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

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

    DEBUG("Write 1");
    for (int i = 0; i < requests; i++) {
        ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, i * system->block_size(), system->block_size(), buffer + (i * system->block_size()), NO_EC));
    }

    memset(buffer, 1, size);

    // now overwrite
    DEBUG("Write 2");
    for (int i = 0; i < requests; i++) {
        ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, i * system->block_size(), system->block_size(), buffer + (i * system->block_size()), NO_EC));
    }

    DEBUG("Read");
    for (int i = 0; i < requests; i++) {
        ASSERT_TRUE(volume->MakeRequest(REQUEST_READ, i * system->block_size(), system->block_size(), result + (i * system->block_size()), NO_EC));
    }

    for (int i = 0; i < size; i++) {
        char error_buffer[100];
        sprintf(error_buffer, "Contents are not the same at index %d",i);
        ASSERT_EQ(result[i], buffer[i]) << error_buffer;
    }
}

bool MakeOverwriteRequest(tuple<DedupSystem*, int, int, bool, bool> t) {
    DedupSystem* system = std::tr1::get<0>(t);
    int thread_id = std::tr1::get<1>(t);
    int request_count = std::tr1::get<2>(t);
    bool overwrite = std::tr1::get<3>(t);
    bool zero_data = std::tr1::get<4>(t);

    byte* buffer = new byte[system->block_size()];
    ScopedArray<byte> scoped_buffer(buffer);
    if (zero_data) {
      memset(buffer, 0, system->block_size());
    } else {
      LC_RNG rng(1024);
      rng.GenerateBlock(buffer, system->block_size());
    }

    DedupVolume* volume = system->GetVolume(0);
    CHECK(volume, "Failed to find volume");

    for (int i = 0; i < request_count; i++) {
        memset(buffer, i % 8, system->block_size());
        if (overwrite) {
            CHECK(volume->MakeRequest(REQUEST_WRITE, thread_id * system->block_size(), system->block_size(), buffer, NO_EC),
                "Write failed");
        } else {
            CHECK(volume->MakeRequest(REQUEST_WRITE, ((request_count * thread_id) + i) * system->block_size(), system->block_size(), buffer, NO_EC),
                "Write failed");
        }
    }
    return true;

}

TEST_P(DedupSystemTest, StrictOverwriteRequests) {
    system = CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    int requests = 128;
    int thread_count = 4;
    // scope frees buffer

    Thread<bool>** threads = new Thread<bool>*[thread_count];
    for (int i = 0; i < thread_count; i++) {
        threads[i] = new Thread<bool>(
            NewRunnable(&MakeOverwriteRequest, make_tuple(system, i,
                requests, true, false)),"write");
    }
    tbb::tick_count start = tbb::tick_count::now();
    for (int i = 0; i < thread_count; i++) {
        ASSERT_TRUE(threads[i]->Start());
    }
    for (int i = 0; i < thread_count; i++) {
        bool result = false;
        ASSERT_TRUE(threads[i]->Join(&result));
        ASSERT_TRUE(result);
    }
    system->chunk_store()->Flush(NO_EC);
    tbb::tick_count write_end_time = tbb::tick_count::now();
    double overwrite_time = (write_end_time - start).seconds();
    INFO("Overwrite time: " << overwrite_time);

    DEBUG(system->block_index()->PrintProfile());
    DEBUG(system->block_index()->PrintTrace());

    for (int i = 0; i < thread_count; i++) {
        delete threads[i];
        threads[i] = NULL;
    }
    delete[] threads;
    threads = NULL;
}

TEST_P(DedupSystemTest, StrictOverwriteZeroDataRequests) {
    system = CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    int requests = 128;
    int thread_count = 4;
    // scope frees buffer

    Thread<bool>** threads = new Thread<bool>*[thread_count];
    for (int i = 0; i < thread_count; i++) {
        threads[i] = new Thread<bool>(
            NewRunnable(&MakeOverwriteRequest, make_tuple(system, i,
                requests, true, true)),"write");
    }
    tbb::tick_count start = tbb::tick_count::now();
    for (int i = 0; i < thread_count; i++) {
        ASSERT_TRUE(threads[i]->Start());
    }
    for (int i = 0; i < thread_count; i++) {
        bool result = false;
        ASSERT_TRUE(threads[i]->Join(&result));
        ASSERT_TRUE(result);
    }
    system->chunk_store()->Flush(NO_EC);
    tbb::tick_count write_end_time = tbb::tick_count::now();
    double overwrite_time = (write_end_time - start).seconds();
    INFO("Overwrite time: " << overwrite_time);

    DEBUG(system->block_index()->PrintProfile());
    DEBUG(system->block_index()->PrintTrace());

    for (int i = 0; i < thread_count; i++) {
        delete threads[i];
        threads[i] = NULL;
    }
    delete[] threads;
    threads = NULL;
}

TEST_P(DedupSystemTest, StrictNoOverwriteRequests) {
    system = CreateDefaultSystem(GetParam(), &info_store, &tp);
    ASSERT_TRUE(system);

    int requests = 128;
    int thread_count = 4;
    // scope frees buffer

    Thread<bool>** threads = new Thread<bool>*[thread_count];
    for (int i = 0; i < thread_count; i++) {
        threads[i] = new Thread<bool>(
            NewRunnable(&MakeOverwriteRequest, make_tuple(system, i,
                requests, false, false)),"write");
    }
    tbb::tick_count start = tbb::tick_count::now();
    for (int i = 0; i < thread_count; i++) {
        ASSERT_TRUE(threads[i]->Start());
    }
    for (int i = 0; i < thread_count; i++) {
        bool result = false;
        ASSERT_TRUE(threads[i]->Join(&result));
        ASSERT_TRUE(result);
    }
    system->chunk_store()->Flush(NO_EC);
    tbb::tick_count write_end_time = tbb::tick_count::now();
    double overwrite_time = (write_end_time - start).seconds();
    INFO("No overwrite time: " << overwrite_time);

    DEBUG(system->block_index()->PrintProfile());
    DEBUG(system->block_index()->PrintTrace());

    for (int i = 0; i < thread_count; i++) {
        delete threads[i];
        threads[i] = NULL;
    }
    delete[] threads;
    threads = NULL;
}

}
