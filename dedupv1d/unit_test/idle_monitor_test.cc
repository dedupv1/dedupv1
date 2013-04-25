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
#include "port_util.h"
#include "stats_monitor.h"
#include "monitor.h"
#include "monitor_test.h"
#include "monitor_helper.h"
#include <core/dedup_volume.h>
#include <core/dedup_system.h>
#include <base/strutil.h>
#include <base/http_client.h>
#include <test_util/log_assert.h>

#include <json/json.h>

using std::string;
using std::pair;
using std::list;
using dedupv1::base::HttpResult;
using dedupv1::base::strutil::Contains;
using dedupv1::base::Option;

namespace dedupv1d {
namespace monitor {

class IdleMonitorTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1d::Dedupv1d* ds;
    MonitorSystem* m;

    virtual void SetUp() {
        ds = new dedupv1d::Dedupv1d();
        m = ds->monitor();

        ASSERT_TRUE(ds->LoadOptions( "data/dedupv1_test.conf"));
        ASSERT_TRUE(ds->SetOption( "monitor.port", PortUtil::getNextPort().c_str()));

        ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
        ASSERT_TRUE(ds->Run());
    }

    virtual void TearDown() {
        ds->Close();
        ds = NULL;
        m = NULL;
    }
};

TEST_F(IdleMonitorTest, ForceIdle) {
    list < pair<string, string> > options;
    options.push_back( pair<string, string>("force-idle", "true"));

    MonitorClient r(m->port(), "idle", options);
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_FALSE(Contains(content.value(), "ERROR"));

    sleep(2);

    ASSERT_TRUE(ds->dedup_system()->idle_detector()->IsIdle());

    options.clear();
    options.push_back( pair<string, string>("force-busy", "true"));

    MonitorClient r2(m->port(), "idle", options);
    content = r2.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_FALSE(Contains(content.value(), "ERROR"));

    sleep(2);

    ASSERT_FALSE(ds->dedup_system()->idle_detector()->IsIdle());
}

TEST_F(IdleMonitorTest, TickInterval) {
    list < pair<string, string> > options;
    options.push_back( pair<string, string>("change-idle-tick-interval", "1"));

    MonitorClient r(m->port(), "idle", options);
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_FALSE(Contains(content.value(), "ERROR"));

    ASSERT_EQ(ds->dedup_system()->idle_detector()->GetIdleTickInterval(), 1);
}

INSTANTIATE_TEST_CASE_P(IdleMonitor,
    MonitorAdapterTest,
    ::testing::Values("idle"));

}
}
