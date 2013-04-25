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

#include <json/json.h>

#include <core/dedup_volume.h>
#include <core/dedup_system.h>
#include <base/strutil.h>
#include <base/http_client.h>

#include <test_util/log_assert.h>

#include "dedupv1d.h"
#include "dedupv1d_volume.h"
#include "scst_handle.h"
#include "port_util.h"
#include "volume_monitor.h"
#include "monitor.h"
#include "monitor_helper.h"
#include "dedupv1d_volume_info.h"
#include "dedupv1d_volume.h"
#include "monitor_test.h"
#include <test_util/json_test_util.h>

using std::string;
using std::pair;
using std::list;
using dedupv1::base::HttpResult;
using dedupv1::base::strutil::Contains;
using dedupv1::base::Option;
using dedupv1::testing::IsJson;

namespace dedupv1d {
namespace monitor {

class VolumeMonitorTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1d::Dedupv1d* ds;
    MonitorSystem* m;

    virtual void SetUp() {
        ds = new dedupv1d::Dedupv1d();
        m = ds->monitor();

        ASSERT_TRUE(ds->LoadOptions( "data/dedupv1_test.conf"));
        ASSERT_TRUE(ds->SetOption( "monitor.port", PortUtil::getNextPort().c_str()));
        ASSERT_TRUE(ds->SetOption( "monitor.volume", "false")); // remove default volume monitor

        ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
        ASSERT_TRUE(ds->Run());
        ASSERT_TRUE(m->Add("volume", new VolumeMonitorAdapter(ds)));
    }

    virtual void TearDown() {
        delete ds;
        ds = NULL;
        m = NULL;
    }
};

INSTANTIATE_TEST_CASE_P(VolumeMonitor,
    MonitorAdapterTest,
    ::testing::Values("volume"));

TEST_F(VolumeMonitorTest, AttachVolume) {
    list < pair<string, string> > options;
    options.push_back( pair<string, string>("op", "attach"));
    options.push_back( pair<string, string>("id", "4"));
    options.push_back( pair<string, string>("group", "Default:1"));
    options.push_back( pair<string, string>("device-name", "dedupv1-4-test"));
    options.push_back( pair<string, string>("logical-size", "1G"));

    MonitorClient r(m->port(), "volume", options);
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_FALSE(Contains(content.value(), "ERROR"));
    ASSERT_TRUE(Contains(content.value(), "dedupv1-4-test"));

    Dedupv1dVolume* volume = ds->volume_info()->FindVolume(4, NULL);
    ASSERT_TRUE(volume);
    ASSERT_EQ(volume->device_name(), "dedupv1-4-test");

    ASSERT_TRUE(content.value().size() > 0);
    ASSERT_TRUE(IsJson(content.value()));
}

TEST_F(VolumeMonitorTest, DetachVolume) {
    list < pair<string, string> > attach_options;
    attach_options.push_back( pair<string, string>("id", "2"));
    attach_options.push_back( pair<string, string>("group", "Default:1"));
    attach_options.push_back( pair<string, string>("logical-size", "1G"));
    ASSERT_TRUE(ds->volume_info()->AttachVolume(attach_options));
    Dedupv1dVolume* volume = ds->volume_info()->FindVolume(2, NULL);
    ASSERT_TRUE(volume);

    list < pair<string, string> > request_options;
    request_options.push_back( pair<string, string>("op", "rmfromgroup"));
    request_options.push_back( pair<string, string>("id", "2"));
    request_options.push_back( pair<string, string>("group", "Default"));
    MonitorClient r(m->port(), "volume", request_options);
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_FALSE(Contains(content.value(), "ERROR"));

    request_options.clear();
    request_options.push_back( pair<string, string>("op", "detach"));
    request_options.push_back( pair<string, string>("id", "2"));
    MonitorClient r2(m->port(), "volume", request_options);

    content = r2.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_FALSE(Contains(content.value(), "ERROR"));

    volume = ds->volume_info()->FindVolume(2, NULL);
    ASSERT_FALSE(volume);

    ASSERT_TRUE(content.value().size() > 0);
    ASSERT_TRUE(IsJson(content.value()));
}

}
}
