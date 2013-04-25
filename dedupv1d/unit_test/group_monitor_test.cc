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

#include <base/strutil.h>
#include <base/threadpool.h>
#include <base/socket.h>
#include <base/http_client.h>
#include <base/option.h>

#include <test_util/log_assert.h>
#include <test_util/json_test_util.h>

#include "monitor_test.h"
#include "dedupv1d.h"
#include "dedupv1d_group.h"
#include "scst_handle.h"
#include "port_util.h"
#include "group_monitor.h"
#include "monitor.h"
#include "monitor_helper.h"
#include "dedupv1d_group_info.h"

using std::string;
using std::pair;
using std::list;
using dedupv1::base::HttpResult;
using dedupv1::base::strutil::Contains;
using dedupv1::base::Option;
using dedupv1::testing::IsJson;

namespace dedupv1d {
namespace monitor {

class GroupMonitorTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    static const std::string kDefaultGroupName;
    static const std::string kPreconfiguredGroupName;

    dedupv1d::Dedupv1d* ds;
    MonitorSystem* m;

    virtual void SetUp() {
        ds = new dedupv1d::Dedupv1d();
        m = ds->monitor();
        ASSERT_TRUE(m);

        ASSERT_TRUE(ds->LoadOptions( "data/dedupv1_test.conf"));
        ASSERT_TRUE(ds->SetOption( "monitor.port", PortUtil::getNextPort().c_str()));
        ASSERT_TRUE(ds->SetOption( "monitor.group", "false")); // remove default volume monitor

        ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
        ASSERT_TRUE(ds->Run());
        ASSERT_TRUE(m->Add("group", new GroupMonitorAdapter(ds)));
    }

    virtual void TearDown() {
        if (ds) {
            delete ds;
            ds = NULL;
            m = NULL;
        }
    }
};

const std::string GroupMonitorTest::kDefaultGroupName = "backup";
const std::string GroupMonitorTest::kPreconfiguredGroupName = "Default";

INSTANTIATE_TEST_CASE_P(GroupMonitor,
    MonitorAdapterTest,
    ::testing::Values("group"));

TEST_F(GroupMonitorTest, ReadMonitor) {
    sleep(2);

    MonitorClient r(m->port(), "group");
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_TRUE(Contains(content.value(), kPreconfiguredGroupName)) << "Monitor output should contain the text: " << content.value();

    ASSERT_TRUE(content.value().size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse(content.value(), root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_F(GroupMonitorTest, AddGroup) {
    list < pair<string, string> > options;
    options.push_back( pair<string, string>("op", "add"));
    options.push_back( pair<string, string>("name", kDefaultGroupName));

    MonitorClient r(m->port(), "group", options);
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_TRUE(content.value().size() > 0);
    ASSERT_FALSE(Contains(content.value(), "ERROR"));
    ASSERT_TRUE(Contains(content.value(), kDefaultGroupName));

    Option<Dedupv1dGroup> group = ds->group_info()->FindGroup(kDefaultGroupName);
    ASSERT_TRUE(group.valid());
    ASSERT_EQ(group.value().name(), kDefaultGroupName);

    ASSERT_TRUE(IsJson(content.value()));
}

TEST_F(GroupMonitorTest, RemoveGroup) {
    list < pair<string, string> > attach_options;
    attach_options.push_back( pair<string, string>("name", kDefaultGroupName));
    ASSERT_TRUE(ds->group_info()->AddGroup(attach_options));

    list < pair<string, string> > request_options;
    request_options.push_back( pair<string, string>("op", "remove"));
    request_options.push_back( pair<string, string>("name", kDefaultGroupName));
    MonitorClient r(m->port(), "group", request_options);
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_TRUE(content.value().size() > 0);
    ASSERT_FALSE(Contains(content.value(), "ERROR"));

    ASSERT_FALSE(ds->group_info()->FindGroup(kDefaultGroupName).valid());

    ASSERT_TRUE(IsJson(content.value()));
}

}
}
