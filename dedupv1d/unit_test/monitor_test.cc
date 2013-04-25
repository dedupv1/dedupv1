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

#include "monitor_test.h"
#include "monitor_helper.h"
#include "monitor.h"
#include "port_util.h"
#include <base/strutil.h>
#include <base/http_client.h>
#include "default_monitor.h"
#include <test_util/log_assert.h>
#include <json/json.h>
#include <base/logging.h>
#include <base/thread.h>
#include <base/runnable.h>
#include <test_util/json_test_util.h>
#include <re2/re2.h>

using dedupv1::testing::IsJson;

using std::string;
using std::vector;
using dedupv1::base::strutil::Split;
using dedupv1::base::HttpResult;
using dedupv1::base::strutil::Contains;
using dedupv1::base::Option;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using std::make_pair;

namespace dedupv1d {
namespace monitor {

LOGGER("MonitorTest");

void MonitorAdapterTest::SetUp() {
    ds = new dedupv1d::Dedupv1d();
    m = ds->monitor();
    ASSERT_TRUE(m);

    ASSERT_TRUE(ds->LoadOptions( "data/dedupv1_test.conf"));
    ASSERT_TRUE(ds->SetOption( "monitor.port", PortUtil::getNextPort().c_str()));
}

void MonitorAdapterTest::ParseParams() {
    vector<string> options;
    ASSERT_TRUE(Split(GetParam(), ";", &options));

    monitor_name = options[0];

    for (size_t i = 1; i < options.size(); i++) {
        string option_name;
        string option;
        ASSERT_TRUE(Split(options[i], "=", &option_name, &option));
        params.push_back(make_pair(option_name, option));
    }
}

void MonitorAdapterTest::TearDown() {
    if (ds) {
        delete ds;
        ds = NULL;
    }
    m = NULL;
}

TEST_P(MonitorAdapterTest, Disable) {
    ParseParams();

    ASSERT_TRUE(ds->SetOption( "monitor." + monitor_name, "false")); // remove monitor
    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
}

bool CallMonitorLoop(MonitorClient* client, bool* stop_flag) {
    sleep(1);
    while (!(*stop_flag)) {
        Option<string> content = client->Get();

        CHECK(content.valid(), "Content not set");

        CHECK(content.value().size() > 0, "No content");
        DEBUG(content.value());
        CHECK(IsJson(content.value()), "Content is not JSON: " << content.value());
    }
    return true;
}

TEST_P(MonitorAdapterTest, ReadDuringStartup) {
    ParseParams();

    MonitorClient client(m->port(), monitor_name, params);

    bool stop_flag = false;
    Thread<bool> call_thread(NewRunnable(&CallMonitorLoop, &client, &stop_flag), "caller");
    ASSERT_TRUE(call_thread.Start());

    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    sleep(2);

    stop_flag = true;
    ASSERT_TRUE(call_thread.Join(NULL));
    // it is ok if the done crash
}

TEST_P(MonitorAdapterTest, ReadMonitor) {
    ParseParams();

    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Cannot find monitor").Times(0, 1);

    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    sleep(2);

    MonitorClient client(m->port(), monitor_name, params);
    Option<string> content = client.Get();

    ASSERT_TRUE(content.valid());

    ASSERT_TRUE(content.value().size() > 0);
    DEBUG(content.value());
    ASSERT_TRUE(IsJson(content.value())) << content.value();
}

TEST_P(MonitorAdapterTest, MonitorFormat) {
    ParseParams();

    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Cannot find monitor").Times(0, 1);

    ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
    sleep(2);

    MonitorClient client(m->port(), monitor_name, params);
    Option<string> content = client.Get();

    ASSERT_TRUE(content.valid());

    ASSERT_TRUE(content.value().size() > 0);
    DEBUG(content.value());
    ASSERT_TRUE(IsJson(content.value())) << content.value();

    ASSERT_FALSE(RE2::PartialMatch(content.value(), "B/s\"")) << content.value();
    ASSERT_FALSE(RE2::PartialMatch(content.value(), "\\d+ms\"")) << content.value();
    ASSERT_FALSE(RE2::PartialMatch(content.value(), "B\"")) << content.value();
}

static HttpResult* monitor_test_read_data(int c);
static HttpResult* monitor_test_read_wrong_data(int c);
static HttpResult* monitor_test_active_monitor(int c);

class MonitorTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    MonitorSystem* m;

    static int port;
    char port_str[16];

    virtual void SetUp() {
        m = new MonitorSystem();
        ASSERT_TRUE(m);

        port++;
        sprintf(port_str, "%d",port);
    }

    virtual void TearDown() {
        if (m) {
            delete m;
            m = NULL;
        }
    }
};

int MonitorTest::port = 8112;

TEST_F(MonitorTest, Create) {
    // no nothing
}

TEST_F(MonitorTest, StartWithoutAdaptors) {
    ASSERT_TRUE(m->SetOption("port", port_str));
    ASSERT_TRUE(m->Start(dedupv1::StartContext()));
}

TEST_F(MonitorTest, StartWithAutoPort) {
    ASSERT_TRUE(m->SetOption("port", port_str));
    ASSERT_TRUE(m->Start(dedupv1::StartContext()));

    MonitorSystem* m2 = new MonitorSystem();
    ASSERT_TRUE(m2->SetOption("port", port_str));
    ASSERT_TRUE(m2->SetOption("port", "auto"));
    ASSERT_TRUE(m2->Start(dedupv1::StartContext()));
    delete m2;
}

TEST_F(MonitorTest, StartWithHostAddress) {
    ASSERT_TRUE(m->SetOption("port", port_str));
    ASSERT_TRUE(m->SetOption("host", "localhost"));
    ASSERT_TRUE(m->Start(dedupv1::StartContext()));
}

class MonitorTestAdapter : public DefaultMonitorAdapter {
public:
    int result;
    string key;
    string value;
    string content;

    MonitorTestAdapter(string content = "test") {
        this->result = 0;
        this->key = "";
        this->value = "";
        this->content = content;
    }

    virtual string Monitor() {
        if (key.size() > 0) {
            return key + "=" + value;
        }
        return content;
    }

    virtual bool ParseParam(const string& key, const string& value) {
        this->key = key;
        this->value = value;
        return true;
    }
};

TEST_F(MonitorTest, AddRemove) {
    ASSERT_TRUE(m->Add("test", new MonitorTestAdapter()));
    ASSERT_EQ(1, m->monitor_count());

    ASSERT_TRUE(m->Remove("test"));
    ASSERT_EQ(0, m->monitor_count());
}

TEST_F(MonitorTest, NameDupliate) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    MonitorTestAdapter* mta = new MonitorTestAdapter();
    MonitorTestAdapter* mta2 = new MonitorTestAdapter();

    ASSERT_TRUE(m->Add("test", mta));
    ASSERT_EQ(1, m->monitor_count());

    ASSERT_FALSE(m->Add("test", mta2));
    ASSERT_EQ(1, m->monitor_count());

    ASSERT_TRUE(m->Remove("test"));
    ASSERT_EQ(0, m->monitor_count());

    delete mta2;
}

TEST_F(MonitorTest, TwoMonitorAdapter) {
    MonitorTestAdapter* mta = new MonitorTestAdapter();
    MonitorTestAdapter* mta2 = new MonitorTestAdapter();

    ASSERT_TRUE(m->Add("test", mta));
    ASSERT_EQ(1, m->monitor_count());

    ASSERT_TRUE(m->Add("test2", mta2));
    ASSERT_EQ(2, m->monitor_count());

    ASSERT_TRUE(m->Remove("test"));
    ASSERT_EQ(1, m->monitor_count());

    ASSERT_TRUE(m->Remove("test2"));
    ASSERT_EQ(0, m->monitor_count());
}

TEST_F(MonitorTest, Stop) {
    ASSERT_TRUE(m->SetOption("port", port_str));
    ASSERT_TRUE(m->Start(dedupv1::StartContext()));
    sleep(2);
    ASSERT_TRUE(m->Stop(dedupv1::StopContext::FastStopContext()));
    sleep(1);
}

TEST_F(MonitorTest, FastStop) {
    ASSERT_TRUE(m->SetOption("port", port_str));
    ASSERT_TRUE(m->Start(dedupv1::StartContext()));
    sleep(2);
    ASSERT_TRUE(m->Stop(dedupv1::StopContext::FastStopContext()));
    sleep(1);
}

TEST_F(MonitorTest, StartWithAdaptor) {
    MonitorTestAdapter* mta = new MonitorTestAdapter();
    ASSERT_TRUE(m->Add("test", mta));

    ASSERT_TRUE(m->SetOption("port", port_str));
    ASSERT_TRUE(m->Start(dedupv1::StartContext()));
    sleep(2);
}

HttpResult* monitor_test_read_data(int port) {
    string url = "http://localhost:" + dedupv1::base::strutil::ToString(port)  + "/test";
    HttpResult* result = HttpResult::GetUrl(url);
    return result;
}

TEST_F(MonitorTest, ReadMonitorData) {
    MonitorTestAdapter* mta = new MonitorTestAdapter();
    ASSERT_TRUE(m->Add("test", mta));

    ASSERT_TRUE(m->SetOption("port", port_str));
    ASSERT_TRUE(m->Start(dedupv1::StartContext()));
    sleep(2);

    Thread<HttpResult*> t(NewRunnable(monitor_test_read_data, port), "caller");
    ASSERT_TRUE(t.Start());

    HttpResult* result = NULL;
    ASSERT_TRUE(t.Join(&result));
    ASSERT_TRUE(result);
    ASSERT_STREQ("test",(char *) result->content());
    delete result;
}

TEST_F(MonitorTest, MultipleReadMonitorData) {
    MonitorTestAdapter* mta = new MonitorTestAdapter();
    ASSERT_TRUE(m->Add("test",mta));

    ASSERT_TRUE(m->SetOption("port", port_str));
    ASSERT_TRUE(m->Start(dedupv1::StartContext()));
    sleep(2);

    Thread<HttpResult*> t(NewRunnable(monitor_test_read_data, m->port()), "caller");
    ASSERT_TRUE(t.Start());

    HttpResult* result = NULL;
    ASSERT_TRUE(t.Join(&result));
    ASSERT_TRUE(result);
    ASSERT_STREQ("test", (const char *) result->content());
    if (result) {
        delete result;
        result = NULL;
    }

    Thread<HttpResult*> t2(NewRunnable(monitor_test_read_data, m->port()), "caller");
    ASSERT_TRUE(t2.Start());

    result = NULL;
    ASSERT_TRUE(t2.Join(&result));
    ASSERT_TRUE(result);
    ASSERT_STREQ("test", (const char *) result->content());
    if (result) {
        delete result;
        result = NULL;
    }
}

HttpResult* monitor_test_read_wrong_data(int port) {
    string url = "http://localhost:" + dedupv1::base::strutil::ToString(port)  + "/unknownmonitor";
    HttpResult* result = HttpResult::GetUrl(url);
    return result;
}

TEST_F(MonitorTest, ReadWrongMonitorData) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("unknownmonitor").Once();

    MonitorTestAdapter* mta = new MonitorTestAdapter();
    ASSERT_TRUE(m->Add("test",mta));

    ASSERT_TRUE(m->SetOption("port", port_str));
    ASSERT_TRUE(m->Start(dedupv1::StartContext()));
    sleep(2);

    Thread<HttpResult*> t(NewRunnable(monitor_test_read_wrong_data, m->port()), "caller");
    ASSERT_TRUE(t.Start());

    HttpResult* result = NULL;
    ASSERT_TRUE(t.Join(&result));
    ASSERT_TRUE(result);
    ASSERT_TRUE(Contains((const char *) result->content(), "Unknown monitor"));
    if (result) {
        delete result;
        result = NULL;
    }

    Thread<HttpResult*> t2(NewRunnable(monitor_test_read_data, m->port()), "caller");
    ASSERT_TRUE(t2.Start());

    result = NULL;
    ASSERT_TRUE(t2.Join(&result));
    ASSERT_TRUE(result);
    ASSERT_TRUE(Contains((const char *) result->content(), "test"));
    if (result) {
        delete result;
        result = NULL;
    }
}

HttpResult* monitor_test_active_monitor(int port) {
    string url = "http://localhost:" + dedupv1::base::strutil::ToString(port)  + "/test?key=value";
    HttpResult* result = HttpResult::GetUrl(url);
    return result;
}

TEST_F(MonitorTest, ActiveMonitor) {
    MonitorTestAdapter* mta = new MonitorTestAdapter();
    ASSERT_TRUE(m->Add("test",mta));

    ASSERT_TRUE(m->SetOption("port", port_str));
    ASSERT_TRUE(m->Start(dedupv1::StartContext()));
    sleep(2);

    Thread<HttpResult*> t(NewRunnable(monitor_test_active_monitor, m->port()), "caller");
    ASSERT_TRUE(t.Start());

    HttpResult* result = NULL;
    ASSERT_TRUE(t.Join(&result));
    ASSERT_TRUE(result);
    string content_text((const char *) result->content());
    ASSERT_TRUE(Contains(content_text, "key=value")) << content_text;
    delete result;

    ASSERT_EQ(mta->key, "key");
    ASSERT_EQ(mta->value, "value");
}

TEST_F(MonitorTest, RemoveAll) {
    MonitorTestAdapter* mta = new MonitorTestAdapter();
    ASSERT_TRUE(m->Add("test", mta));

}

}
}

