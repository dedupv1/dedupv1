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
#include <core/dedup_volume.h>
#include <core/dedup_system.h>
#include <base/strutil.h>
#include "port_util.h"
#include "logging_monitor.h"
#include "monitor.h"
#include <base/http_client.h>
#include <test/log_assert.h>
#include "monitor_helper.h"
#include <json/json.h>
#include "monitor_test.h"

using std::string;
using std::list;
using std::pair;
using std::make_pair;
using dedupv1::base::HttpResult;
using dedupv1::base::strutil::Contains;
using dedupv1::base::Option;

namespace dedupv1d {
namespace monitor {

class LoggingMonitorTest : public MonitorAdapterTest {
protected:
};

TEST_F(LoggingMonitorTest, LogWarning) {
    EXPECT_LOGGING(dedupv1::test::WARN).Logger("Utils").Matches("Hello").Once();

    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";

    list< pair< string, string> > params;
    params.push_back(make_pair("message", "Hello"));
    params.push_back(make_pair("logger", "Utils"));
    params.push_back(make_pair("level", "WARNING"));

    MonitorClient client(m->port(), "logging", params);
    ASSERT_TRUE(client.Get().valid());
}

TEST_F(LoggingMonitorTest, LogError) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Logger("Utils").Matches("Hello").Once();

    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";

    list< pair< string, string> > params;
    params.push_back(make_pair("message", "Hello"));
    params.push_back(make_pair("logger", "Utils"));
    params.push_back(make_pair("level", "ERROR"));

    MonitorClient client(m->port(), "logging", params);
    ASSERT_TRUE(client.Get().valid());
}

TEST_F(LoggingMonitorTest, LogInfo) {
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";

    list< pair< string, string> > params;
    params.push_back(make_pair("message", "Hello"));
    params.push_back(make_pair("logger", "Utils"));
    params.push_back(make_pair("level", "INFO"));

    MonitorClient client(m->port(), "logging", params);
    ASSERT_TRUE(client.Get().valid());
}

TEST_F(LoggingMonitorTest, LogDebug) {
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";

    list< pair< string, string> > params;
    params.push_back(make_pair("message", "Hello"));
    params.push_back(make_pair("logger", "Utils"));
    params.push_back(make_pair("level", "DEBUG"));

    MonitorClient client(m->port(), "logging", params);
    ASSERT_TRUE(client.Get().valid());
}

INSTANTIATE_TEST_CASE_P(LoggingMonitor,
    MonitorAdapterTest,
    ::testing::Values("logging"));

}
}
