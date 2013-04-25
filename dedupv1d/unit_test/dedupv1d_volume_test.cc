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

#include "dedupv1d.h"
#include "dedupv1d_volume.h"
#include "scst_handle.h"
#include "command_handler.h"

#include <core/dedup_volume.h>
#include <base/strutil.h>
#include <core/dedup_system.h>
#include <base/logging.h>

#include <test_util/log_assert.h>
#include <json/json.h>

LOGGER("Dedupv1dVolumeTest");

using std::string;
using dedupv1::DedupSystem;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::Contains;

namespace dedupv1d {

class Dedupv1dVolumeTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Dedupv1dVolume* dsv;

    DedupSystem* dedup_system;
    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;

    virtual void SetUp() {
        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());

        dsv = new Dedupv1dVolume(true);
        ASSERT_TRUE(dsv);

        dedup_system = new DedupSystem();
        ASSERT_TRUE(dedup_system->LoadOptions("data/dedupsystem.conf"));
        ASSERT_TRUE(dedup_system->Start(dedupv1::StartContext(), &info_store, &tp));
        ASSERT_TRUE(dedup_system->Run());
    }

    virtual void TearDown() {
        if (dsv) {
            ASSERT_TRUE(dsv->Close());
            dsv = NULL;
        }
        if (dedup_system) {
            ASSERT_TRUE(dedup_system->Close());
            dedup_system = NULL;
        }
    }
};

TEST_F(Dedupv1dVolumeTest, Create) {
    // no nothing
}

TEST_F(Dedupv1dVolumeTest, StartWithOutConfig) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_FALSE(dsv->Start(dedup_system)) << "Start without config should fail because e.g. the id is not set";
}

TEST_F(Dedupv1dVolumeTest, RunWithoutStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_FALSE(dsv->Run());
}

TEST_F(Dedupv1dVolumeTest, StartWithMinimalConfig) {
    ASSERT_TRUE(dsv->SetOption( "id", "0"));
    ASSERT_TRUE(dsv->SetOption( "logical-size", "1G"));
    ASSERT_TRUE(dsv->Start( dedup_system));

    ASSERT_EQ(dsv->state(), Dedupv1dVolume::DEDUPV1D_VOLUME_STATE_STARTED);
    ASSERT_EQ(dsv->id(), 0U);
    ASSERT_EQ(dsv->logical_size(), ToStorageUnit("1G").value());
    ASSERT_EQ(dsv->device_name(), "dedupv1-0");
    ASSERT_EQ(dsv->groups().size(), 0U);
}

TEST_F(Dedupv1dVolumeTest, StartWithGB) {
    ASSERT_TRUE(dsv->SetOption( "id", "0"));
    ASSERT_TRUE(dsv->SetOption( "logical-size", "16GB"));
    ASSERT_TRUE(dsv->Start( dedup_system));

    ASSERT_EQ(dsv->state(), Dedupv1dVolume::DEDUPV1D_VOLUME_STATE_STARTED);
    ASSERT_EQ(dsv->id(), 0U);
    ASSERT_EQ(dsv->logical_size(), ToStorageUnit("16G").value());
    ASSERT_EQ(dsv->device_name(), "dedupv1-0");
    ASSERT_EQ(dsv->groups().size(), 0U);
}

TEST_F(Dedupv1dVolumeTest, StartWithIllegalSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(dsv->SetOption("id", "0"));
    dsv->SetOption("logical-size", "17168"); // this call might fail or success, but Start should fail
    EXPECT_FALSE(dsv->Start(dedup_system));
}

TEST_F(Dedupv1dVolumeTest, DoubleStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(dsv->SetOption( "id", "0"));
    ASSERT_TRUE(dsv->SetOption( "logical-size", "1G"));
    ASSERT_TRUE(dsv->Start( dedup_system));
    ASSERT_FALSE(dsv->Start(dedup_system)) << "2nd start should fail";
}

TEST_F(Dedupv1dVolumeTest, StartWithFullConfig) {
    ASSERT_TRUE(dsv->SetOption( "id", "0"));
    ASSERT_TRUE(dsv->SetOption( "logical-size", "1G"));
    ASSERT_TRUE(dsv->SetOption( "device-name", "dedupv1-test"));
    ASSERT_TRUE(dsv->SetOption( "group", "backup:0"));
    ASSERT_TRUE(dsv->SetOption( "threads", "16"));
    ASSERT_TRUE(dsv->Start(dedup_system));

    ASSERT_EQ(dsv->volume()->GetId(), 0U);
    ASSERT_EQ(dsv->volume()->GetLogicalSize(), ToStorageUnit("1G").value());
    ASSERT_EQ(dsv->command_thread_count(), 16U);
    ASSERT_EQ(dsv->handle()->device_name(), "dedupv1-test");
    ASSERT_EQ(dsv->groups().size(), 1U);
    ASSERT_EQ(dsv->groups()[0].first, "backup");
    ASSERT_EQ(dsv->groups()[0].second, 0);
}

TEST_F(Dedupv1dVolumeTest, StartWithTarget) {
    ASSERT_TRUE(dsv->SetOption( "id", "0"));
    ASSERT_TRUE(dsv->SetOption( "logical-size", "1G"));
    ASSERT_TRUE(dsv->SetOption( "device-name", "dedupv1-test"));
    ASSERT_TRUE(dsv->SetOption( "group", "backup:0"));
    ASSERT_TRUE(dsv->SetOption( "target", "iqn.2010.05:example:0"));
    ASSERT_TRUE(dsv->SetOption( "threads", "16"));
    ASSERT_TRUE(dsv->Start(dedup_system));

    ASSERT_EQ(dsv->volume()->GetId(), 0U);
    ASSERT_EQ(dsv->volume()->GetLogicalSize(), ToStorageUnit("1G").value());
    ASSERT_EQ(dsv->command_thread_count(), 16U);
    ASSERT_EQ(dsv->handle()->device_name(), "dedupv1-test");
    ASSERT_EQ(dsv->targets().size(), 1U);
    ASSERT_EQ(dsv->targets()[0].first, "iqn.2010.05:example");
    ASSERT_EQ(dsv->targets()[0].second, 0);
}

TEST_F(Dedupv1dVolumeTest, RunWithFullConfig) {
    ASSERT_TRUE(dsv->SetOption( "id", "0"));
    ASSERT_TRUE(dsv->SetOption( "logical-size", "1G"));
    ASSERT_TRUE(dsv->SetOption( "device-name", "dedupv1-test"));
    ASSERT_TRUE(dsv->SetOption( "group", "backup:0"));
    ASSERT_TRUE(dsv->SetOption( "threads", "16"));
    ASSERT_TRUE(dsv->Start(dedup_system));
    ASSERT_TRUE(dsv->Run());

    ASSERT_EQ(dsv->state(), Dedupv1dVolume::DEDUPV1D_VOLUME_STATE_RUNNING);
    ASSERT_EQ(dsv->handle()->state(), ScstHandle::SCST_HANDLE_STATE_STARTED);
    ASSERT_EQ(dsv->command_handler()->IsStarted(), true);
    ASSERT_EQ(dsv->volume()->GetId(), 0U);
    ASSERT_EQ(dsv->volume()->GetLogicalSize(), ToStorageUnit("1G").value());
    ASSERT_EQ(dsv->command_thread_count(), 16U);
    ASSERT_EQ(dsv->handle()->device_name(), "dedupv1-test");
    ASSERT_EQ(dsv->groups().size(), 1U);
    ASSERT_EQ(dsv->groups()[0].first, "backup");
    ASSERT_EQ(dsv->groups()[0].second, 0);
}

TEST_F(Dedupv1dVolumeTest, IllegalGroups) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_FALSE(dsv->SetOption( "group", "backup"));
    ASSERT_FALSE(dsv->SetOption( "group", "backup:"));
    ASSERT_FALSE(dsv->SetOption( "group", "backup:asd"));
    ASSERT_FALSE(dsv->SetOption( "group", ":asd"));
}

TEST_F(Dedupv1dVolumeTest, IllegalTarget) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_FALSE(dsv->SetOption( "target", "backup"));
    ASSERT_FALSE(dsv->SetOption( "target", "backup:"));
    ASSERT_FALSE(dsv->SetOption( "target", "backup:asd"));
    ASSERT_FALSE(dsv->SetOption( "target", ":asd"));
}

TEST_F(Dedupv1dVolumeTest, IllegalName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_FALSE(dsv->SetOption( "device-name", ""));
    ASSERT_FALSE(dsv->SetOption( "device-name", "1234567890123456789012345678901234567890123456789")); // 49
    ASSERT_FALSE(dsv->SetOption( "device-name", "Vol\u00f6ume"));
    ASSERT_FALSE(dsv->SetOption( "device-name", "Vol+ume"));
    ASSERT_FALSE(dsv->SetOption( "device-name", "Vol$ume"));
    ASSERT_FALSE(dsv->SetOption( "device-name", "Vol\u0040ume"));
    ASSERT_FALSE(dsv->SetOption( "device-name", "Vol ume"));
    ASSERT_FALSE(dsv->SetOption( "device-name", "Vol:ume"));
    ASSERT_TRUE(dsv->SetOption( "device-name", "a"));
    ASSERT_TRUE(dsv->SetOption( "device-name", "This.is_my-2nd-Volume"));
    ASSERT_TRUE(dsv->SetOption( "device-name", "123456789012345678901234567890123456789012345678")); // 48
}

TEST_F(Dedupv1dVolumeTest, MultipeGroups) {
    ASSERT_TRUE(dsv->SetOption( "id", "0"));
    ASSERT_TRUE(dsv->SetOption( "logical-size", "1G"));
    ASSERT_TRUE(dsv->SetOption( "device-name", "dedupv1-test"));
    ASSERT_TRUE(dsv->SetOption( "group", "backup:0"));
    ASSERT_TRUE(dsv->SetOption( "group", "backup2:0"));
    ASSERT_TRUE(dsv->SetOption( "threads", "16"));
    ASSERT_TRUE(dsv->Start(dedup_system));

    ASSERT_EQ(dsv->state(), Dedupv1dVolume::DEDUPV1D_VOLUME_STATE_STARTED);
    ASSERT_EQ(dsv->volume()->GetId(), 0U);
    ASSERT_EQ(dsv->volume()->GetLogicalSize(), ToStorageUnit("1G").value());
    ASSERT_EQ(dsv->command_thread_count(), 16U);
    ASSERT_EQ(dsv->handle()->device_name(), "dedupv1-test");
    ASSERT_EQ(dsv->groups().size(), 2U);
    ASSERT_EQ(dsv->groups()[0].first, "backup");
    ASSERT_EQ(dsv->groups()[0].second, 0);
    ASSERT_EQ(dsv->groups()[1].first, "backup2");
    ASSERT_EQ(dsv->groups()[1].second, 0);
}

TEST_F(Dedupv1dVolumeTest, MultipeTargets) {
    ASSERT_TRUE(dsv->SetOption( "id", "0"));
    ASSERT_TRUE(dsv->SetOption( "logical-size", "1G"));
    ASSERT_TRUE(dsv->SetOption( "device-name", "dedupv1-test"));
    ASSERT_TRUE(dsv->SetOption( "target", "backup:0"));
    ASSERT_TRUE(dsv->SetOption( "target", "backup2:0"));
    ASSERT_TRUE(dsv->SetOption( "threads", "16"));
    ASSERT_TRUE(dsv->Start(dedup_system));

    ASSERT_EQ(dsv->state(), Dedupv1dVolume::DEDUPV1D_VOLUME_STATE_STARTED);
    ASSERT_EQ(dsv->targets().size(), 2U);
    ASSERT_EQ(dsv->targets()[0].first, "backup");
    ASSERT_EQ(dsv->targets()[0].second, 0);
    ASSERT_EQ(dsv->targets()[1].first, "backup2");
    ASSERT_EQ(dsv->targets()[1].second, 0);
}

TEST_F(Dedupv1dVolumeTest, EmptyGroup) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_FALSE(dsv->SetOption( "group", "")) << "Adding an empty group should fail";
}

TEST_F(Dedupv1dVolumeTest, EmptyTarget) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_FALSE(dsv->SetOption( "target", "")) << "Adding an empty target should fail";
}

TEST_F(Dedupv1dVolumeTest, DoubleGroup) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_TRUE(dsv->SetOption( "group", "backup:0"));
    ASSERT_FALSE(dsv->SetOption( "group", "backup:0")) << "Adding the same group twice should fail";
}

TEST_F(Dedupv1dVolumeTest, DoubleTarget) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_TRUE(dsv->SetOption( "target", "backup:0"));
    ASSERT_FALSE(dsv->SetOption( "target", "backup:0")) << "Adding the same target twice should fail";
}

TEST_F(Dedupv1dVolumeTest, DefaultDevicename) {
    ASSERT_TRUE(Contains(dsv->device_name(),"[")) << "Dummy device name before id is set: " << dsv->device_name();
    ASSERT_TRUE(dsv->SetOption( "id", "0"));
    ASSERT_EQ(dsv->device_name(), "dedupv1-0");
    ASSERT_TRUE(dsv->SetOption( "device-name", "dedupv1-test"));
    ASSERT_EQ(dsv->device_name(), "dedupv1-test");
}

TEST_F(Dedupv1dVolumeTest, Split) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    string group;
    uint64_t lun;

    ASSERT_TRUE(Dedupv1dVolume::SplitGroupOption("Default:0", &group, &lun));
    ASSERT_EQ(group, "Default");
    ASSERT_EQ(lun, 0);

    ASSERT_TRUE(Dedupv1dVolume::SplitGroupOption("Group:14", &group, &lun));
    ASSERT_EQ(group, "Group");
    ASSERT_EQ(lun, 14);

    ASSERT_FALSE(Dedupv1dVolume::SplitGroupOption("Group", &group, &lun));
    ASSERT_FALSE(Dedupv1dVolume::SplitGroupOption("Group:", &group, &lun));
    ASSERT_FALSE(Dedupv1dVolume::SplitGroupOption(":Group", &group, &lun));
    ASSERT_FALSE(Dedupv1dVolume::SplitGroupOption("", &group, &lun));

    ASSERT_TRUE(Dedupv1dVolume::SplitGroupOption("Group:0:0", &group, &lun));
    ASSERT_EQ(group, "Group:0") << "Split should use right-most colon as lun seperator";
    ASSERT_EQ(lun, 0);
}

/**
 * Test case for the reported issue in issue #40
 */
TEST_F(Dedupv1dVolumeTest, GroupnameWithColon) {
    ASSERT_TRUE(dsv->SetOption( "id", "0"));
    ASSERT_TRUE(dsv->SetOption( "logical-size", "500G"));
    ASSERT_TRUE(dsv->SetOption( "device-name", "Montag"));
    ASSERT_TRUE(dsv->SetOption( "group","TapeMO_ign.2010-12.example.com:tape.mo:0"));

    ASSERT_TRUE(dsv->Start( dedup_system));

    ASSERT_EQ(dsv->state(), Dedupv1dVolume::DEDUPV1D_VOLUME_STATE_STARTED);
    ASSERT_EQ(dsv->groups().size(), 1U);
    ASSERT_EQ(dsv->groups()[0].first, "TapeMO_ign.2010-12.example.com:tape.mo");
    ASSERT_EQ(dsv->groups()[0].second, 0);
}

TEST_F(Dedupv1dVolumeTest, PrintStatistics) {
    ASSERT_TRUE(dsv->SetOption( "id", "0"));
    ASSERT_TRUE(dsv->SetOption( "logical-size", "500G"));
    ASSERT_TRUE(dsv->SetOption( "device-name", "Montag"));
    ASSERT_TRUE(dsv->SetOption( "group","TapeMO_ign.2010-12.example.com:tape.mo:0"));

    ASSERT_TRUE(dsv->Start( dedup_system));

    string content = dsv->PrintStatistics();

    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( content, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages() << "\n" << content;
}

}
