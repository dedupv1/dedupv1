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
#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <list>
#include <string>

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

#include "dedupv1d.h"
#include "dedupv1d_volume.h"
#include "scst_handle.h"
#include "log_replayer.h"
#include "dedupv1d_volume_info.h"
#include "monitor_helper.h"
#include "port_util.h"

using std::string;
using std::vector;
using std::list;
using std::pair;
using std::make_pair;
using dedupv1::base::Option;
using dedupv1::base::strutil::Split;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::Runnable;
using dedupv1::base::Thread;
using dedupv1::base::ThreadUtil;
using dedupv1::base::NewRunnable;
using dedupv1::scsi::SCSI_OK;
using dedupv1::base::crc;
using dedupv1d::Dedupv1dVolume;
using dedupv1d::Dedupv1dVolumeInfo;
using dedupv1::REQUEST_WRITE;
using dedupv1::REQUEST_READ;
using dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START;
using dedupv1::chunkstore::ContainerStorage;
using testing::ContainsRegex;

LOGGER("Dedupv1dVolumeInfoIntegrationTest");

class Dedupv1dVolumeInfoIntegrationTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1d::Dedupv1d* ds;

    virtual void SetUp() {
        ds = Create("data/dedupv1_test.conf");
        ASSERT_TRUE(ds);
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
};

bool Write(dedupv1d::Dedupv1d* ds, uint32_t volume_id) {
    byte buffer[64 * 1024];
    long bytes = 0;
    int r = 0;

    Dedupv1dVolume* volume = ds->volume_info()->FindVolume(volume_id, NULL);

    int random_file = open("/dev/urandom", O_RDONLY);
    bytes = 64 * 1024;

    int i = bytes;
    while (i > 0) {
        r = read(random_file, buffer, 64 * 1024);
        if (r > i) {
            r = i;
        }
        uint64_t offset = bytes - i;
        DEBUG("Write offset " << offset << ", size " << r);
        CHECK(volume->MakeRequest(REQUEST_WRITE, bytes - i, r, buffer, NO_EC), "Cannot write");
        i -= r;
    }
    INFO("Write finished");
    close(random_file);
    return true;
}

/**
 * Tests for a bug where the chunking data is not visible in the volume monitor
 */
TEST_F(Dedupv1dVolumeInfoIntegrationTest, AttachWithOwnChunking) {
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("logical-size", "1G"));
    options.push_back( make_pair("chunking", "rabin"));
    options.push_back( make_pair("chunking.min-chunk-size", "2048"));
    options.push_back( make_pair("chunking.avg-chunk-size", "8192"));

    Dedupv1dVolumeInfo* volume_info = ds->volume_info();
    ASSERT_TRUE(volume_info->AttachVolume(options));

    Dedupv1dVolume* volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);
    ASSERT_TRUE(volume->volume()->chunker() != NULL);

    list< pair< string, string> > params;
    MonitorClient client(ds->monitor()->port(), "volume", params);
    Option<string> s = client.Get();
    ASSERT_TRUE(s.valid());
    INFO(s.value());
    ASSERT_THAT(s.value(), ContainsRegex(".*rabin.*"));

    volume->ChangeMaintenanceMode(true);

    options.clear();
    options.push_back( make_pair("chunking", "rabin"));
    options.push_back( make_pair("chunking.min-chunk-size", "2048"));
    options.push_back( make_pair("chunking.avg-chunk-size", "8192"));
    volume_info->ChangeOptions(1, options);

    s = client.Get();
    ASSERT_TRUE(s.valid());
    INFO(s.value());
    ASSERT_THAT(s.value(), ContainsRegex(".*rabin.*"));

    volume->ChangeMaintenanceMode(false);

    ASSERT_TRUE(volume->volume()->chunker() != NULL);

    s = client.Get();
    ASSERT_TRUE(s.valid());
    INFO(s.value());
    ASSERT_THAT(s.value(), ContainsRegex(".*rabin.*"));
}

TEST_F(Dedupv1dVolumeInfoIntegrationTest, WriteDetach) {
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    ThreadUtil::Sleep(1);

    ASSERT_TRUE(ds->log_replayer()->Stop(dedupv1::StopContext())); // stop the log replayer. We manually replay the log.

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(ds->volume_info()->AttachVolume(options));

    ASSERT_TRUE(dedupv1::base::Thread<bool>::RunThread(NewRunnable(&Write, ds, 1U)));

    ASSERT_TRUE(ds->volume_info()->DetachVolume(1));

    ASSERT_TRUE(ds->dedup_system()->log()->PerformFullReplayBackgroundMode());

    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext()));
}

TEST_F(Dedupv1dVolumeInfoIntegrationTest, WriteDetachWithClose) {
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(ds->volume_info()->AttachVolume(options));

    ASSERT_TRUE(dedupv1::base::Thread<bool>::RunThread(NewRunnable(&Write, ds, 1U)));

    ASSERT_TRUE(ds->volume_info()->DetachVolume(1));

    ASSERT_TRUE(ds->Close());

    ds = Create("data/dedupv1_test.conf");
    ASSERT_TRUE(ds);
    dedupv1::StartContext start_context;
    start_context.set_create(dedupv1::StartContext::NON_CREATE);
    start_context.set_dirty(dedupv1::StartContext::DIRTY);
    start_context.set_crashed(true);
    ASSERT_TRUE(ds->Start(start_context)) << "Cannot start application";
    ASSERT_TRUE(ds->Run());
}

