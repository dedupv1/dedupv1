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
#include <base/http_client.h>
#include <base/option.h>
#include <test_util/log_assert.h>

#include "dedupv1d.h"
#include "dedupv1d_target.h"
#include "scst_handle.h"
#include "port_util.h"
#include "target_monitor.h"
#include "monitor.h"
#include "monitor_helper.h"
#include "dedupv1d_target_info.h"
#include "monitor_test.h"
#include <test_util/json_test_util.h>

using std::string;
using std::pair;
using std::list;
using std::make_pair;
using dedupv1::base::HttpResult;
using dedupv1::base::strutil::Contains;
using dedupv1::base::Option;
using dedupv1::testing::IsJson;

namespace dedupv1d {
namespace monitor {

class TargetMonitorTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    static const std::string kDefaultTargetName;
    static const std::string kPreconfiguredTargetName;

    dedupv1d::Dedupv1d* ds;
    MonitorSystem* m;

    virtual void SetUp() {
        ds = new dedupv1d::Dedupv1d();
        m = ds->monitor();
        ASSERT_TRUE(m);

        ASSERT_TRUE(ds->LoadOptions( "data/dedupv1_test.conf"));
        ASSERT_TRUE(ds->SetOption( "monitor.port", PortUtil::getNextPort().c_str()));
        ASSERT_TRUE(ds->SetOption( "monitor.target", "false")); // remove default volume monitor

        ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
        ASSERT_TRUE(ds->Run());
        ASSERT_TRUE(m->Add("target", new TargetMonitorAdapter(ds)));
    }

    virtual void TearDown() {
        if (ds) {
            delete ds;
            ds = NULL;
            m = NULL;
        }
    }
};

const std::string TargetMonitorTest::kDefaultTargetName = "iqn.2010.05:example";
const std::string TargetMonitorTest::kPreconfiguredTargetName = "iqn.2010.05:preconf";

INSTANTIATE_TEST_CASE_P(TargetMonitor,
    MonitorAdapterTest,
    ::testing::Values("target"));

TEST_F(TargetMonitorTest, ReadMonitor) {
    sleep(2);

    MonitorClient r(m->port(), "target");
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_TRUE(content.value().size() > 0);
    ASSERT_TRUE(Contains(content.value(), kPreconfiguredTargetName)) << "Monitor output should contain the text: " << content.value();

    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse(content.value(), root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_F(TargetMonitorTest, AddTarget) {
    list < pair<string, string> > options;
    options.push_back( make_pair("op", "add"));
    options.push_back( make_pair("tid", "3"));
    options.push_back( make_pair("name", kDefaultTargetName));
    options.push_back( make_pair("param.QueuedCommands", "16"));

    MonitorClient r(m->port(), "target", options);
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_TRUE(content.value().size() > 0);
    ASSERT_FALSE(Contains(content.value(), "ERROR"));
    ASSERT_TRUE(Contains(content.value(), kDefaultTargetName));

    Option<Dedupv1dTarget> target = ds->target_info()->FindTargetByName(kDefaultTargetName);
    ASSERT_TRUE(target.valid());
    ASSERT_EQ(target.value().name(), kDefaultTargetName);

    ASSERT_TRUE(IsJson(content.value()));
}

TEST_F(TargetMonitorTest, ChangeTargetParams) {
    list < pair<string, string> > attach_options;
    attach_options.push_back( make_pair("tid", "3"));
    attach_options.push_back( make_pair("name", kDefaultTargetName));
    ASSERT_TRUE(ds->target_info()->AddTarget(attach_options));

    Option<Dedupv1dTarget> target = ds->target_info()->FindTargetByName(kDefaultTargetName);
    ASSERT_TRUE(target.valid());
    ASSERT_FALSE(target.value().param("QueuedCommands").valid());

    list < pair<string, string> > request_options;
    request_options.push_back( make_pair("op", "change-param"));
    request_options.push_back( make_pair("tid", "3"));
    request_options.push_back( make_pair("param.QueuedCommands", "16"));
    MonitorClient r(m->port(), "target", request_options);

    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_TRUE(content.value().size() > 0);
    ASSERT_FALSE(Contains(content.value(), "ERROR"));
    ASSERT_TRUE(IsJson(content.value()));

    target = ds->target_info()->FindTargetByName(kDefaultTargetName);
    ASSERT_TRUE(target.valid());
    ASSERT_TRUE(target.value().param("QueuedCommands").valid());
    ASSERT_EQ(target.value().param("QueuedCommands").value(), "16");

    request_options.clear();
    request_options.push_back( make_pair("op", "change-param"));
    request_options.push_back( make_pair("tid", "3"));
    request_options.push_back( make_pair("param.QueuedCommands", "8"));
    MonitorClient r2(m->port(), "target", request_options);

    content = r2.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_TRUE(content.value().size() > 0);
    ASSERT_FALSE(Contains(content.value(), "ERROR"));
    ASSERT_TRUE(IsJson(content.value()));

    target = ds->target_info()->FindTargetByName(kDefaultTargetName);
    ASSERT_TRUE(target.valid());
    ASSERT_TRUE(target.value().param("QueuedCommands").valid());
    ASSERT_EQ(target.value().param("QueuedCommands").value(), "8");
}

TEST_F(TargetMonitorTest, RemoveTarget) {
    list < pair<string, string> > attach_options;
    attach_options.push_back( make_pair("tid", "3"));
    attach_options.push_back( make_pair("name", kDefaultTargetName));
    ASSERT_TRUE(ds->target_info()->AddTarget(attach_options));

    list < pair<string, string> > request_options;
    request_options.push_back( make_pair("op", "remove"));
    request_options.push_back( make_pair("tid", "3"));
    MonitorClient r(m->port(), "target", request_options);

    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_TRUE(content.value().size() > 0);
    ASSERT_FALSE(Contains(content.value(), "ERROR"));

    ASSERT_FALSE(ds->target_info()->FindTargetByName(kDefaultTargetName).valid());

    ASSERT_TRUE(IsJson(content.value()));
}

}
}
