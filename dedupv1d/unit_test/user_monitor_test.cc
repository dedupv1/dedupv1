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
#include "monitor_test.h"
#include "dedupv1d.h"
#include "dedupv1d_user.h"
#include "scst_handle.h"
#include "port_util.h"
#include "user_monitor.h"
#include "monitor.h"
#include "monitor_helper.h"
#include "dedupv1d_user_info.h"
#include <test_util/json_test_util.h>

using std::string;
using std::pair;
using std::list;
using std::make_pair;
using dedupv1::base::HttpResult;
using dedupv1::base::strutil::Contains;
using dedupv1::base::Option;
using dedupv1::testing::IsJson;
using dedupv1d::Dedupv1dUser;

namespace dedupv1d {
namespace monitor {

class UserMonitorTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    static const std::string kDefaultUserName;
    static const std::string kPreconfiguredUserName;

    dedupv1d::Dedupv1d* ds;
    MonitorSystem* m;

    virtual void SetUp() {
        ds = new dedupv1d::Dedupv1d();
        m = ds->monitor();
        ASSERT_TRUE(m);

        ASSERT_TRUE(ds->LoadOptions( "data/dedupv1_test.conf"));
        ASSERT_TRUE(ds->SetOption( "monitor.port", PortUtil::getNextPort().c_str()));
        ASSERT_TRUE(ds->SetOption( "monitor.user", "false")); // remove default volume monitor

        ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
        ASSERT_TRUE(ds->Run());
        ASSERT_TRUE(m->Add("user", new UserMonitorAdapter(ds)));
    }

    virtual void TearDown() {
        if (ds) {
            ASSERT_TRUE(ds->Close());
            ds = NULL;
            m = NULL;
        }
    }
};

const std::string UserMonitorTest::kDefaultUserName = "admin2";
const std::string UserMonitorTest::kPreconfiguredUserName = "admin1";

INSTANTIATE_TEST_CASE_P(UserMonitor,
    MonitorAdapterTest,
    ::testing::Values("user"));

TEST_F(UserMonitorTest, ReadMonitor) {
    sleep(2);

    MonitorClient r(m->port(), "user");
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_TRUE(Contains(content.value(), kPreconfiguredUserName)) << "Monitor output should contain the text: " << content.value();

    ASSERT_TRUE(content.value().size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( content.value(), root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_F(UserMonitorTest, AddUser) {
    list < pair<string, string> > options;
    options.push_back( make_pair("op", "add"));
    options.push_back( make_pair("name", kDefaultUserName));
    options.push_back( make_pair("secret-hash",
            Dedupv1dUser::EncodePassword(kDefaultUserName + kDefaultUserName)));

    MonitorClient r(m->port(), "user", options);
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_FALSE(Contains(content.value(), "ERROR"));
    ASSERT_TRUE(Contains(content.value(), kDefaultUserName));

    Option<Dedupv1dUser> user = ds->user_info()->FindUser(kDefaultUserName);
    ASSERT_TRUE(user.valid());
    ASSERT_EQ(user.value().name(), kDefaultUserName);

    ASSERT_TRUE(content.value().size() > 0);
    ASSERT_TRUE(IsJson(content.value()));
}

TEST_F(UserMonitorTest, RemoveUser) {
    list < pair<string, string> > attach_options;
    attach_options.push_back( make_pair("name", kDefaultUserName));
    attach_options.push_back( make_pair("secret-hash",
            Dedupv1dUser::EncodePassword(kDefaultUserName + kDefaultUserName)));
    ASSERT_TRUE(ds->user_info()->AddUser(attach_options));

    list < pair<string, string> > request_options;
    request_options.push_back( make_pair("op", "remove"));
    request_options.push_back( make_pair("name", kDefaultUserName));
    MonitorClient r(m->port(), "user", request_options);
    Option<string> content = r.Get();
    ASSERT_TRUE(content.valid());
    ASSERT_FALSE(Contains(content.value(), "ERROR"));

    ASSERT_FALSE(ds->user_info()->FindUser(kDefaultUserName).valid());

    ASSERT_TRUE(content.value().size() > 0);
    ASSERT_TRUE(IsJson(content.value()));
}

}
}
