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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <list>
#include <string>

#include <json/json.h>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <core/dedup_volume.h>
#include <core/dedup_system.h>
#include <base/strutil.h>
#include <base/fileutil.h>
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
#include "dedupv1d_volume_info.h"
#include "scst_handle.h"

#include "port_util.h"
#include "log_replayer.h"

using std::string;
using dedupv1::base::File;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::chunkstore::ContainerStorage;

LOGGER("Dedupv1dTest");

namespace dedupv1d {

class Dedupv1dTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Dedupv1d* ds;

    virtual void SetUp() {
        ds = new dedupv1d::Dedupv1d();
    }

    virtual void TearDown() {
        if (ds) {
            ASSERT_TRUE(ds->Close());
            ds = NULL;
        }
    }
};

TEST_F(Dedupv1dTest, Create) {
    // no nothing
}

TEST_F(Dedupv1dTest, Start) {
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort().c_str()));
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
}

TEST_F(Dedupv1dTest, TwoInstances) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("lock").Once();

    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort().c_str()));
    ASSERT_TRUE(ds->SetOption("daemon.lockfile", "work/lock"));
    ASSERT_TRUE(ds->OpenLockfile());
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";

    dedupv1d::Dedupv1d* ds2 = new dedupv1d::Dedupv1d();
    ASSERT_TRUE(ds2->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds2->SetOption("monitor.port", PortUtil::getNextPort().c_str()));
    ASSERT_TRUE(ds2->SetOption("daemon.lockfile", "work/lock"));
    ASSERT_FALSE(ds2->OpenLockfile());
    ASSERT_TRUE(ds2->Close());
}

TEST_F(Dedupv1dTest, StopWithoutStart) {
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort().c_str()));

    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext()));
    ASSERT_TRUE(ds->Stop());
}

/**
 * Restart with the same config
 */
TEST_F(Dedupv1dTest, Restart) {
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort().c_str()));
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    ASSERT_TRUE(ds->Close());
    ds = NULL;

    ds = new dedupv1d::Dedupv1d();
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort().c_str()));

    dedupv1::StartContext start_context;
    start_context.set_force(dedupv1::StartContext::NO_FORCE);
    start_context.set_create(dedupv1::StartContext::NON_CREATE);
    ASSERT_TRUE(ds->Start(start_context)) << "Cannot start application";
}

TEST_F(Dedupv1dTest, RestartWithOtherConfig) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("Configuration changed").Once();

    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort().c_str()));
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    ASSERT_TRUE(ds->Close());
    ds = NULL;

    ds = new dedupv1d::Dedupv1d();
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_minimal_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort().c_str()));
    dedupv1::StartContext start_context;
    start_context.set_force(dedupv1::StartContext::NO_FORCE);
    start_context.set_create(dedupv1::StartContext::NON_CREATE);
    ASSERT_FALSE(ds->Start(start_context)) << "System should not start with another config";
}

/**
 * Restarts the daemon with another but configurable configuration
 */
TEST_F(Dedupv1dTest, RestartWithOtherConfigButForce) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Configuration changed").Once();

    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort().c_str()));
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    ASSERT_TRUE(ds->Close());
    ds = NULL;

    ds = new dedupv1d::Dedupv1d();
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_minimal_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort().c_str()));
    dedupv1::StartContext start_context;
    start_context.set_force(dedupv1::StartContext::FORCE);
    start_context.set_create(dedupv1::StartContext::NON_CREATE);
    ASSERT_TRUE(ds->Start(start_context));

}

TEST_F(Dedupv1dTest, StartWithoutCreateFlag) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    dedupv1::StartContext start_context(dedupv1::StartContext::NON_CREATE /* no create flag */);
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort().c_str()));
    ASSERT_FALSE(ds->Start(start_context)) << "Start should fail";
}

TEST_F(Dedupv1dTest, DirtyFlagAfterNormalClose) {
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort()));
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
    ASSERT_TRUE(t.Start());
    sleep(2);
    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext())) << "Failed to shutdown dedupv1";
    ASSERT_TRUE(t.Join(NULL)) <<  "Failed to join run thread";
    ASSERT_TRUE(ds->Close());
    ds = NULL;

    dedupv1::StartContext start_context2(dedupv1::StartContext::NON_CREATE);
    ds = new dedupv1d::Dedupv1d();
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort()));
    ASSERT_TRUE(ds->Start(start_context2)) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t_run2(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait),"runner");
    ASSERT_TRUE(t_run2.Start());
    sleep(2);
    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext())) << "Failed to shutdown dedupv1";
    ASSERT_TRUE(t_run2.Join(NULL));

    ASSERT_TRUE(ds->start_context().dirty()) << "The system should be marked as dirty";
}

TEST_F(Dedupv1dTest, DirtyFlagAfterCrashDestroyedDirtyfile) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    dedupv1::StartContext start_context;
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort()));
    ASSERT_TRUE(ds->Start(start_context)) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t_run(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait), "runner");
    t_run.Start();
    sleep(2);
    INFO("Crash simulation");
    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext())) << "Failed to shutdown dedupv1";
    ASSERT_TRUE(t_run.Join(NULL)) <<  "Failed to join run thread";
    ASSERT_TRUE(ds->Stop()) << "Failed to stop dedupv1";

    string dirtyfile = ds->daemon_dirtyfile();

    ASSERT_TRUE(ds->Close());
    ds = NULL;

    // this is pretty much like the system has crashed
    // during the update of the dirty file
    ASSERT_TRUE(File::CopyFile(dirtyfile, dirtyfile + ".tmp"));
    ASSERT_TRUE(File::Truncate(dirtyfile, 7));

    dedupv1::StartContext start_context2(dedupv1::StartContext::NON_CREATE); // the dirty flag should be recovered by the dedupv1d
    ds = new dedupv1d::Dedupv1d();
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort()));
    ASSERT_TRUE(ds->Start(start_context2)) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t_run2(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait), "runner");
    t_run2.Start();
    sleep(2);
    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext())) << "Failed to shutdown dedupv1";
    ASSERT_TRUE(t_run2.Join(NULL)) <<  "Failed to join run thread";

    ASSERT_TRUE(ds->start_context().dirty()) << "The system should be marked as dirty";
}

TEST_F(Dedupv1dTest, StartWithIllegalFile) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_FALSE(ds->LoadOptions("data/dedupv1_test_not_existing.conf")) << "Should fail";
    ASSERT_FALSE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
}

TEST_F(Dedupv1dTest, DoubleStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort()));

    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_FALSE(ds->Start(dedupv1::StartContext())) << "The 2nd start should fail";
}

TEST_F(Dedupv1dTest, CheckVolumes) {
    ASSERT_TRUE(ds->LoadOptions( "data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort()));
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";

    ASSERT_EQ(ds->volume_info()->GetVolumes(NULL).value().size(), 1U);
    Dedupv1dVolume* volume = NULL;

    volume = ds->volume_info()->FindVolume(0, NULL);
    ASSERT_TRUE(volume);
    ASSERT_EQ(volume->id(), 0U);
    ASSERT_TRUE(volume->device_name().size() > 0U);
    ASSERT_EQ(volume->logical_size(), ToStorageUnit("1G").value());
    ASSERT_EQ(volume->state(), Dedupv1dVolume::DEDUPV1D_VOLUME_STATE_STARTED);
}

TEST_F(Dedupv1dTest, Wait) {
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort()));
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait), "runner");
    t.Start();
    sleep(2);

    INFO("Stopping system");
    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext()));

    bool run_result = false;
    t.Join(&run_result);
    ASSERT_TRUE(run_result);

    ASSERT_TRUE(ds->Stop()) << "Cannot stop dedupv1";
}

TEST_F(Dedupv1dTest, PrintStatistics) {
    ASSERT_TRUE(ds->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption("monitor.port", PortUtil::getNextPort()));
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    ASSERT_TRUE(ds->Run());

    Thread<bool> t(NewRunnable(ds, &dedupv1d::Dedupv1d::Wait), "runner");
    t.Start();
    sleep(2);

    string content = ds->PrintStatistics();

    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( content, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages() << "\n" << content;

    INFO("Stopping system");
    ASSERT_TRUE(ds->Shutdown(dedupv1::StopContext::FastStopContext()));

    bool run_result = false;
    t.Join(&run_result);
    ASSERT_TRUE(run_result);

    ASSERT_TRUE(ds->Stop()) << "Cannot stop dedupv1";
}

}
