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
#include <gtest/gtest-param-test.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <list>
#include <string>
#include <re2/re2.h>

#include <core/dedupv1_scsi.h>
#include <core/log_consumer.h>
#include <base/locks.h>
#include <core/dedup_volume.h>
#include <core/dedup_system.h>
#include <base/strutil.h>
#include <core/fingerprinter.h>
#include <base/logging.h>
#include <core/log.h>
#include <base/thread.h>
#include <base/crc32.h>
#include <core/chunk_index.h>
#include <core/block_index.h>
#include <core/garbage_collector.h>
#include <core/container_storage.h>
#include <core/chunk_store.h>
#include <test_util/log_assert.h>

#include <cryptopp/cryptlib.h>
#include <cryptopp/rng.h>

#include "dedupv1d.h"
#include "dedupv1d_volume.h"
#include "scst_handle.h"
#include "log_replayer.h"
#include "dedupv1d_volume_info.h"

#include "port_util.h"

using std::string;
using std::vector;
using std::stringstream;
using dedupv1::base::strutil::Split;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::ToString;
using dedupv1::base::Runnable;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::scsi::SCSI_OK;
using dedupv1::base::crc;
using dedupv1d::Dedupv1dVolume;
using dedupv1d::CommandHandler;
using dedupv1::REQUEST_WRITE;
using dedupv1::REQUEST_READ;
using dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START;
using dedupv1::chunkstore::ContainerStorage;
using namespace CryptoPP;

LOGGER("Dedupv1dExecutionTest");

#define LARGE_SIZE (32 * 1024 * 1024)

static string dedupv1d_test_read(dedupv1d::Dedupv1d* ds);
static string dedupv1d_test_write(dedupv1d::Dedupv1d* ds, int seed, uint64_t offset);
static string dedupv1d_test_write_2vol(dedupv1d::Dedupv1d* ds, int seed, uint64_t offset);
static std::list<std::string> dedupv1d_test_write_large(dedupv1d::Dedupv1d* ds,
                                                        int seed,
                                                        uint64_t offset);
static std::list<std::string> dedupv1d_test_read_large(dedupv1d::Dedupv1d* ds);

class Dedupv1dExecutionTest : public testing::TestWithParam<const char*> {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1d::Dedupv1d* ds;

    virtual void SetUp() {
        ds = NULL;
    }

    virtual void TearDown() {
        if (ds) {
            ASSERT_TRUE(ds->Close());
            ds = NULL;
        }
    }

    dedupv1d::Dedupv1d* Create(string config) {
        vector<string> options;
        CHECK_RETURN(Split(config, ";", &options), NULL, "Failed to split: " << config);

        dedupv1d::Dedupv1d* system = new dedupv1d::Dedupv1d();
        CHECK_RETURN(system, NULL, "Cannot create system");
        CHECK_RETURN(system->Init(), NULL, "Failed to init system");
        CHECK_RETURN(system->LoadOptions(options[0]), NULL, "Cannot load options");

        for (size_t i = 1; i < options.size(); i++) {
            string option_name;
            string option;
            CHECK_RETURN(Split(options[i], "=", &option_name, &option), NULL, "Failed to split " << options[i]);
            CHECK_RETURN(system->SetOption(option_name, option), NULL, "Failed set option: " << options[i]);
        }
        CHECK_RETURN(system->SetOption("monitor.port", PortUtil::getNextPort()), NULL, "");
        return system;
    }

    template<class RT> RT RunThread(Runnable<RT>* r) {
        Thread<RT> t(r,"runner");
        t.Start();
        sleep(2);
        RT result;
        t.Join(&result);
        return result;
    }

    bool WriteWriteRead() {
        Thread<bool> t(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
        t.Start();
        sleep(2);

        string write_result = RunThread(NewRunnable(dedupv1d_test_write, ds, 12, 0UL));
        CHECK(write_result.size() > 0,"Write thread error");

        string write_result2 = RunThread(NewRunnable(dedupv1d_test_write, ds, 11, 0UL));
        CHECK(write_result2.size() > 0,"Write thread error");

        string read_result = RunThread(NewRunnable(dedupv1d_test_read, ds));
        CHECK(read_result.size() > 0,"Write thread error");

        CHECK(ds->Shutdown(dedupv1::StopContext::FastStopContext()), "Failed to shutdown dedupv1");
        CHECK(t.Join(NULL), "Failed to join run thread");

        CHECK(write_result2 == read_result,"Data is not the same");
        return true;
    }

    bool WriteCloseRead() {

        Thread<bool> t_run(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
        t_run.Start();
        sleep(2);

        string write_result = RunThread(NewRunnable(dedupv1d_test_write, ds, 10, 0UL));
        CHECK(write_result.size() > 0,"Write thread error");

        CHECK(ds->Shutdown(dedupv1::StopContext::FastStopContext()), "Failed to shutdown dedupv1");
        CHECK(t_run.Join(NULL), "Failed to join run thread");

        ds->Close();
        ds = NULL;

        dedupv1::StartContext start_context2(dedupv1::StartContext::NON_CREATE);
        start_context2.set_dirty(dedupv1::StartContext::DIRTY);
        ds = Create(GetParam());
        CHECK(ds, "Failed to create application");
        CHECK(ds->Start(start_context2), "Cannot start application");
        CHECK(ds->Run(), "Cannot run application");

        Thread<bool> t_run2(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait), "runner");
        t_run2.Start();
        sleep(2);

        string read_result = RunThread(NewRunnable(dedupv1d_test_read, ds));
        CHECK(read_result.size() > 0,"Write thread error");

        CHECK(ds->Shutdown(dedupv1::StopContext::FastStopContext()), "Failed to shutdown dedupv1");
        CHECK(t_run2.Join(NULL), "Failed to join run thread");

        CHECK(write_result == read_result,"Data is not the same");
        return true;
    }

    bool WriteRead() {
        Thread<bool> t(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
        t.Start();
        sleep(2);

        string write_result = RunThread(NewRunnable(dedupv1d_test_write, ds, 9, 0UL));
        CHECK(write_result.size() > 0,"Write thread error");

        string read_result = RunThread(NewRunnable(dedupv1d_test_read, ds));
        CHECK(read_result.size() > 0,"Write thread error");

        CHECK(write_result == read_result, "");

        sleep(2);

        CHECK(ds->Shutdown(dedupv1::StopContext::FastStopContext()), "Failed to shutdown dedupv1");
        CHECK(t.Join(NULL), "Failed to join run thread");

        return true;
    }

    bool ReadWriteLarge() {
        Thread<bool> t(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
        t.Start();
        sleep(2);

        std::list<std::string> fp_write =
            RunThread(NewRunnable(dedupv1d_test_write_large, ds, 8, 0UL));
        CHECK(fp_write.size() > 1, "Write thread error");
        DEBUG("Finished writing");
        sleep(2);

        std::list<std::string> fp_read = RunThread(NewRunnable(dedupv1d_test_read_large, ds));
        CHECK(fp_read.size() > 1, "Read thread error");
        DEBUG("Finished reading");

        CHECK(ds->Shutdown(dedupv1::StopContext::FastStopContext()), "Failed to shutdown dedupv1");
        CHECK(t.Join(NULL), "Failed to join run thread");

        CHECK(fp_write.back() == fp_read.back(), "Data is not identical");
        return true;
    }

    bool Write2Volumes() {
        Thread<bool> t(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
        t.Start();
        sleep(2);

        string write_result = RunThread(NewRunnable(dedupv1d_test_write_2vol, ds, 7, 0UL));
        CHECK(write_result.size() > 0,"Write thread error");

        CHECK(ds->Shutdown(dedupv1::StopContext::FastStopContext()), "Failed to shutdown dedupv1");
        CHECK(t.Join(NULL), "Failed to join run thread");
        return true;
    }
};

TEST_P(Dedupv1dExecutionTest, PrintStatistics) {
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    string s = ds->PrintStatistics();
    ASSERT_TRUE(s.size() > 0);
}

TEST_F(Dedupv1dExecutionTest, StatsVolumeSummary) {
    ds = Create("data/dedupv1_test.conf;volume.id=1;volume.logical-size=1G");
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    ASSERT_TRUE(Write2Volumes());

    uint64_t cumulative_scsi_command_count = 0;
    uint64_t cumulative_sector_read_count = 0;
    uint64_t cumulative_sector_write_count = 0;
    uint64_t cumulative_retry_count = 0;
    double cumulative_write_throughput = 0.0;
    double cumulative_read_throughput = 0.0;

    Dedupv1dVolume* v = NULL;
    CommandHandler::Statistics* s = NULL;
    for (int i = 0; i <= 1; i++) {
        v = ds->volume_info()->FindVolume(i, NULL);
        s = v->command_handler()->stats();
        cumulative_scsi_command_count += s->scsi_command_count_;
        cumulative_sector_read_count += s->sector_read_count_;
        cumulative_sector_write_count += s->sector_write_count_;
        cumulative_retry_count += s->retry_count_;
        cumulative_write_throughput += s->average_write_throughput();
        cumulative_read_throughput += s->average_read_throughput();
    }

    const string stats = ds->volume_info()->PrintStatisticSummary();
    ASSERT_TRUE(RE2::PartialMatch(stats, "\"cumulative scsi command count\": " + ToString(cumulative_scsi_command_count)));
    ASSERT_TRUE(RE2::PartialMatch(stats, "\"cumulative sector read count\": " + ToString(cumulative_sector_read_count)));
    ASSERT_TRUE(RE2::PartialMatch(stats, "\"cumulative sector write count\": " + ToString(cumulative_sector_read_count)));
    ASSERT_TRUE(RE2::PartialMatch(stats, "\"cumulative retry count\": " + ToString(cumulative_retry_count)));
}

TEST_F(Dedupv1dExecutionTest, Uptime) {
    ds = Create("data/dedupv1_test.conf");
    ASSERT_TRUE(ds);

    double t = ds->uptime();
    double t2 = 0.0;
    for (int i = 0; i < 10; i++) {
        sleep(2);
        t2 = ds->uptime();
        ASSERT_GT(t2, t) << "Uptime has not incremented";
        t = t2;
    }
}

TEST_F(Dedupv1dExecutionTest, Servicetime) {
    ds = Create("data/dedupv1_test.conf");
    ASSERT_TRUE(ds);

    ASSERT_EQ(-1.0, ds->servicetime()) << "Service Time before initialisation has not been -1";

    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";

    double t = ds->servicetime();
    double t2 = 0.0;
    for (int i = 0; i < 10; i++) {
        sleep(2);
        t2 = ds->servicetime();
        ASSERT_GT(t2, t) << "Servicetime has not incremented";
        t = t2;
    }

    ASSERT_TRUE(ds->Close());
    ds = NULL;

    sleep(1);

    ds = Create("data/dedupv1_test.conf");
    ASSERT_TRUE(ds);

    ASSERT_EQ(-1.0, ds->servicetime()) << "Service Time before initialisation after restart has not been -1";

    dedupv1::StartContext start_context;
    start_context.set_create(dedupv1::StartContext::NON_CREATE);
    ASSERT_TRUE(ds->Start(start_context)) << "Cannot start application after restart";

    t2 = ds->servicetime();
    ASSERT_GT(t2, t) << "Servicetime has not incremented after restart";
}

TEST_P(Dedupv1dExecutionTest, PrintProfile) {
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    string s = ds->PrintProfile();
    ASSERT_TRUE(s.size() > 0);
}

TEST_P(Dedupv1dExecutionTest, PrintLockStatistics) {
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    string s = ds->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
}

TEST_P(Dedupv1dExecutionTest, SimpleWriteRead) {
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    ASSERT_TRUE(WriteRead());
}

TEST_P(Dedupv1dExecutionTest, SimpleWriteCloseRead) {
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    ASSERT_TRUE(WriteCloseRead());
}

TEST_P(Dedupv1dExecutionTest, SimpleWriteWriteRead) {
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    ASSERT_TRUE(WriteWriteRead());
}

TEST_P(Dedupv1dExecutionTest, WriteOffsetWriteRead) {
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
    t.Start();
    sleep(2);

    string write_result = RunThread(NewRunnable(dedupv1d_test_write, ds, 6, 0UL));
    ASSERT_TRUE(write_result.size() > 0) << "Write thread error";

    string write_result2 = RunThread(NewRunnable(dedupv1d_test_write, ds, 6,
            4UL * ds->dedup_system()->block_size()));
    ASSERT_TRUE(write_result2.size() > 0) << "Write thread error";

    string read_result = RunThread(NewRunnable(dedupv1d_test_read, ds));
    ASSERT_TRUE(read_result.size() > 0) << "Write thread error";

    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext())) << "Failed to shutdown dedupv1";
    ASSERT_TRUE(t.Join(NULL)) << "Failed to join run thread";

    ASSERT_TRUE(write_result == read_result) << "Data is not the same";
}

TEST_P(Dedupv1dExecutionTest, ReadWriteLarge) {
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    ASSERT_TRUE(ReadWriteLarge());
}

TEST_P(Dedupv1dExecutionTest, ReadWriteLargeWithLogReplay) {
    // we have the processing and the log replay in parallel on a single disk. This might give problems with long running requests, but
    // here I do not care.
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Long running request.*").Repeatedly();

    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());
    ASSERT_TRUE(ds->log_replayer()->Resume()) <<  "Cannot resume log replayer";

    ASSERT_TRUE(ReadWriteLarge());
}

TEST_P(Dedupv1dExecutionTest, WriteOverwriteWhileIdle) {
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
    t.Start();

    dedupv1::base::ThreadUtil::Sleep(2, dedupv1::base::ThreadUtil::SECONDS);

    std::list<std::string> fp_write =
        RunThread(NewRunnable(dedupv1d_test_write_large, ds, 5, 0UL));
    ASSERT_TRUE(fp_write.size() > 0) << "Write thread error";

    dedupv1::base::ThreadUtil::Sleep(2, dedupv1::base::ThreadUtil::SECONDS);

    ASSERT_TRUE(ds->dedup_system()->idle_detector()->ForceIdle(true));

    for (int i = 0; i < 16; i++) {
        fp_write = RunThread(NewRunnable(dedupv1d_test_write_large, ds, 4, 0UL));
        ASSERT_TRUE(fp_write.size() > 0) << "Write thread error";

        dedupv1::base::ThreadUtil::Sleep(4, dedupv1::base::ThreadUtil::SECONDS);
    }

    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext())) << "Failed to shutdown dedupv1";
    ASSERT_TRUE(t.Join(NULL)) <<  "Failed to join run thread";
    ASSERT_TRUE(ds->Close());
    ds = NULL;

    dedupv1::StartContext start_context2(dedupv1::StartContext::NON_CREATE);
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(start_context2)) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t_run2(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
    ASSERT_TRUE(t_run2.Start());

    std::list<std::string> fp_read2 = RunThread(NewRunnable(dedupv1d_test_read_large,ds));
    ASSERT_TRUE(fp_read2.size() > 0) << "Read thread error";
    ASSERT_TRUE(fp_read2 == fp_write) << "Data is not identical";

    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext())) << "Failed to shutdown dedupv1";
    ASSERT_TRUE(t_run2.Join(NULL));
}

TEST_P(Dedupv1dExecutionTest, WriteReadCloseRead) {
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
    t.Start();

    sleep(2);

    std::list<std::string> fp_write1 =
        RunThread(NewRunnable(dedupv1d_test_write_large, ds, 3, 0UL));
    ASSERT_TRUE(fp_write1.size() > 0) << "Write thread error";

    sleep(2);

    std::list<std::string> fp_read1 = RunThread(NewRunnable(dedupv1d_test_read_large, ds));
    ASSERT_TRUE(fp_read1.size() > 0) << "Read thread error";

    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext())) << "Failed to shutdown dedupv1";
    ASSERT_TRUE(t.Join(NULL)) <<  "Failed to join run thread";

    ASSERT_TRUE(fp_write1 == fp_read1);

    ASSERT_TRUE(ds->Close());
    ds = NULL;

    dedupv1::StartContext start_context2(dedupv1::StartContext::NON_CREATE);
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(start_context2)) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t_run2(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
    ASSERT_TRUE(t_run2.Start());

    std::list<std::string> fp_read2 = RunThread(NewRunnable(dedupv1d_test_read_large,ds));
    ASSERT_TRUE(fp_read2.size() > 0) << "Read thread error";

    ASSERT_TRUE(fp_read1 == fp_read2) << "Data is not identical";

    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext())) << "Failed to shutdown dedupv1";
    ASSERT_TRUE(t_run2.Join(NULL));
}

TEST_P(Dedupv1dExecutionTest, WriteReadCloseReadWriteRead) {
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
    t.Start();

    sleep(2);

    std::list<std::string> fp_write1 =
        RunThread(NewRunnable(dedupv1d_test_write_large,ds, 2, 0UL));
    ASSERT_TRUE(fp_write1.size() > 0) << "Write thread error";
    INFO("Write 1 finished");

    sleep(2);

    std::list<std::string> fp_read1 = RunThread(NewRunnable(dedupv1d_test_read_large,ds));
    ASSERT_TRUE(fp_read1.size() > 0) << "Read thread error";
    INFO("Read 1 finished");

    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext())) << "Failed to shutdown dedupv1";
    ASSERT_TRUE(t.Join(NULL)) <<  "Failed to join run thread";

    ASSERT_TRUE(fp_write1 == fp_read1);

    ds->Close();
    ds = NULL;

    dedupv1::StartContext start_context2(dedupv1::StartContext::NON_CREATE,
                                         dedupv1::StartContext::DIRTY,
                                         dedupv1::StartContext::NO_FORCE);
    ds = Create(GetParam());
    ASSERT_TRUE(ds);
    ASSERT_TRUE(ds->Start(start_context2)) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t_run2(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
    t_run2.Start();

    std::list<std::string> fp_read2 = RunThread(NewRunnable(dedupv1d_test_read_large,ds));
    ASSERT_TRUE(fp_read2.size() > 0) << "Read thread error";
    INFO("Read 2 finished");

    ASSERT_TRUE(fp_read2 == fp_read1);

    std::list<std::string> fp_write2 = RunThread(NewRunnable(dedupv1d_test_write_large, ds, 1, 0UL));
    ASSERT_TRUE(fp_write2.size() > 0) << "Write thread error";
    INFO("Write 2 finished");

    std::list<std::string> fp_read3 = RunThread(NewRunnable(dedupv1d_test_read_large, ds));
    ASSERT_TRUE(fp_read3.size() > 0) << "Read thread error";
    INFO("Read 3 finished");

    ASSERT_TRUE(fp_write2 == fp_read3) << "Data is not identical";

    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext())) << "Failed to shutdown dedupv1";
    ASSERT_TRUE(t_run2.Join(NULL)) <<  "Failed to join run thread";
}

string dedupv1d_test_write(dedupv1d::Dedupv1d* ds, int seed, uint64_t offset) {
    Dedupv1dVolume* volume = NULL;
    volume = ds->volume_info()->FindVolume(0, NULL);
    CHECK_RETURN(volume, "", "Volume not set");

    LC_RNG rng(seed);
    byte buffer[ds->dedup_system()->block_size()];
    rng.GenerateBlock(buffer, ds->dedup_system()->block_size());

    CHECK_RETURN(volume->MakeRequest(REQUEST_WRITE,
            offset, ds->dedup_system()->block_size(), buffer, NO_EC),
        "",
        "Cannot write");
    return crc(buffer, ds->dedup_system()->block_size());
}

string dedupv1d_test_write_2vol(dedupv1d::Dedupv1d* ds, int seed, uint64_t offset) {
    LC_RNG rng(seed);
    byte buffer[ds->dedup_system()->block_size()];
    rng.GenerateBlock(buffer, ds->dedup_system()->block_size());

    Dedupv1dVolume* volume = NULL;
    for (int i = 0; i <= 1; i++) {
        volume = ds->volume_info()->FindVolume(i, NULL);
        CHECK_RETURN(volume, "", "Volume not set (" + ToString(i) + ")");
        CHECK_RETURN(
            volume->MakeRequest(REQUEST_WRITE,
                offset,
                ds->dedup_system()->block_size(),
                buffer,
                NO_EC),
            "", "Cannot write");
    }

    return crc(buffer, ds->dedup_system()->block_size());
}

string dedupv1d_test_read(dedupv1d::Dedupv1d* ds) {
    Dedupv1dVolume* volume = NULL;
    volume = ds->volume_info()->FindVolume(0, NULL);
    CHECK_RETURN(volume, "READ", "Volume not set");

    byte result[ds->dedup_system()->block_size()];
    memset(result, 0, ds->dedup_system()->block_size());
    CHECK_RETURN(volume->MakeRequest(REQUEST_READ, 0,
            ds->dedup_system()->block_size(), result, NO_EC), "READ", "Cannot read");
    return crc(result, ds->dedup_system()->block_size());
}

std::list<std::string> dedupv1d_test_write_large(dedupv1d::Dedupv1d* ds, int seed, uint64_t offset) {
    int i = 0;
    byte buffer[ds->dedup_system()->block_size()];
    long bytes = 0;
    int r = 0;
    std::list<std::string> fp_list;

    Dedupv1dVolume* volume = NULL;
    volume = ds->volume_info()->FindVolume(0, NULL);

    bytes = LARGE_SIZE;
    LC_RNG rng(seed);

    i = bytes;
    while (i > 0) {
        rng.GenerateBlock(buffer, ds->dedup_system()->block_size());
        r = ds->dedup_system()->block_size();
        if (r > i) {
            r = i;
        }
        string crc_value = crc(buffer, r);
        fp_list.push_back(crc_value);

        DEBUG("Write offset " << (offset + bytes - i) << ", size " << r << ", data " << crc_value);
        CHECK_GOTO(volume->MakeRequest(REQUEST_WRITE, offset + bytes - i, r, buffer, NO_EC), "Cannot write");
        i -= r;
    }
    INFO("Write finished");
    return fp_list;
error:
    fp_list.push_back("WRITE ERROR");
    return fp_list;
}

std::list<std::string> dedupv1d_test_read_large(dedupv1d::Dedupv1d* ds) {
    int i = 0;
    byte buffer[ds->dedup_system()->block_size()];
    long bytes = 0;
    int r = 0;
    std::list<std::string> fp_list;

    Dedupv1dVolume* volume = NULL;
    volume = ds->volume_info()->FindVolume(0, NULL);

    bytes = LARGE_SIZE;

    i = bytes;
    while (i > 0) {
        r = ds->dedup_system()->block_size();
        if (r > i) {
            r = i;
        }
        uint64_t offset = bytes - i;
        CHECK_GOTO(volume->MakeRequest(REQUEST_READ, offset, r, buffer, NO_EC),
            "Cannot read data: offset " << offset << ", size " << r);

        string crc_value = crc(buffer, r);
        DEBUG("Read offset " << offset << ", size " << r << ", data " << crc_value);
        fp_list.push_back(crc_value);
        i -= r;
    }
    INFO("Read finished");
    return fp_list;
error:
    fp_list.push_back("READ ERROR");
    return fp_list;
}

INSTANTIATE_TEST_CASE_P(Dedupv1dExecution,
    Dedupv1dExecutionTest,
    ::testing::Values("data/dedupv1_test.conf",
        "data/dedupv1_test.conf;storage.compression=lz4",
        "data/dedupv1_test.conf;storage.compression=snappy",
        "data/dedupv1_sqlite_test.conf",
        "data/dedupv1_leveldb_test.conf",
        "data/dedupv1_sampling_test.conf",
        "data/dedupv1_test.conf;chunking.avg-chunk-size=16K;chunking.min-chunk-size=4K;chunking.max-chunk-size=64K",
        "data/dedupv1_test.conf;chunking.avg-chunk-size=4K;chunking.min-chunk-size=1K;chunking.max-chunk-size=16K"));
