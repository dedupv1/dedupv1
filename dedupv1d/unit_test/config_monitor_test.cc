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

#include <vector>
#include <string>

#include <core/dedup_volume.h>
#include <core/dedup_system.h>
#include <base/http_client.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <test_util/log_assert.h>
#include <test_util/json_test_util.h>

#include "dedupv1d.h"
#include "dedupv1d_volume.h"
#include "scst_handle.h"
#include "port_util.h"
#include "config_monitor.h"
#include "monitor.h"
#include "monitor_helper.h"
#include "monitor_test.h"
#include <json/json.h>

using std::string;
using std::pair;
using std::list;
using std::vector;
using dedupv1::base::HttpResult;
using dedupv1::base::strutil::Contains;
using dedupv1::base::Option;
using dedupv1::testing::IsJson;

LOGGER("ConfigMonitorTest");

namespace dedupv1d {
namespace monitor {

class ConfigMonitorTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1d::Dedupv1d* ds;
    MonitorSystem* m;

    virtual void SetUp() {
        ds = new dedupv1d::Dedupv1d();
        m = ds->monitor();
        ASSERT_TRUE(m);

        ASSERT_TRUE(ds->LoadOptions( "data/dedupv1_test.conf"));
        ASSERT_TRUE(ds->SetOption( "monitor.port", PortUtil::getNextPort().c_str()));
        ASSERT_TRUE(ds->SetOption( "monitor.config", "false")); // remove default config monitor

        ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
        ASSERT_TRUE(m->Add("config", new ConfigMonitorAdapter(ds)));
    }

    virtual void TearDown() {
        if (ds) {
            ASSERT_TRUE(ds->Close());
            ds = NULL;
            m = NULL;
        }
    }
};

INSTANTIATE_TEST_CASE_P(ConfigMonitor,
    MonitorAdapterTest,
    ::testing::Values("config"));

TEST_F(ConfigMonitorTest, ReadMonitor) {
    sleep(2);

    MonitorClient r(m->port(), "config");
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_TRUE(content.value().size() > 0);

    DEBUG("content: " << content.value());
    ASSERT_TRUE(Contains(content.value(), "filename")) << "Monitor output should contain the text: " << content.value();

    ASSERT_TRUE(IsJson(content.value()));
}

}
}
