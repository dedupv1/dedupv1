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
#include "log_monitor.h"
#include "monitor.h"
#include "log_replayer.h"
#include <base/http_client.h>
#include <test_util/log_assert.h>
#include "monitor_test.h"
#include <json/json.h>
#include <test_util/json_test_util.h>

using std::string;
using dedupv1::base::HttpResult;
using dedupv1::base::strutil::Contains;
using dedupv1::testing::IsJson;

namespace dedupv1d {
namespace monitor {

static void* log_monitor_with_param(void* c);

class LogMonitorTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1d::Dedupv1d* ds;
    MonitorSystem* m;

    virtual void SetUp() {
        ds = new dedupv1d::Dedupv1d();
        m = ds->monitor();

        ASSERT_TRUE(ds->LoadOptions( "data/dedupv1_test.conf"));
        ASSERT_TRUE(ds->SetOption( "monitor.port",PortUtil::getNextPort().c_str()));
        ASSERT_TRUE(ds->SetOption( "monitor.log","false")); // remove default log monitor

        ASSERT_TRUE(ds->Start(dedupv1::StartContext())) << "Cannot start application";
        ASSERT_TRUE(ds->Run()) << "Failed to run dedupv1d";
        ASSERT_TRUE(m->Add("log", new LogMonitorAdapter(ds)));
    }

    virtual void TearDown() {
        delete ds;
        ds = NULL;
        m = NULL;
    }
};

INSTANTIATE_TEST_CASE_P(LogMonitor,
    MonitorAdapterTest,
    ::testing::Values("log"));

class MonitorCall {
public:
    int port_;
    std::string monitor_;
    std::string key_;
    std::string value_;

    MonitorCall(int port, const std::string& monitor, const std::string& key, const std::string& value) {
        this->port_ = port;
        this->monitor_ = monitor;
        this->key_ = key;
        this->value_ = value;
    }

    int port() const {
        return port_;
    }

    const std::string& monitor() const {
        return monitor_;
    }

    const std::string& key() const {
        return key_;
    }

    const std::string& value() const {
        return value_;
    }
};

void* log_monitor_with_param(void* c) {
    MonitorCall* mc = (MonitorCall *) c;
    std::string url = "http://localhost:" + dedupv1::base::strutil::ToString(mc->port()) + "/" +
                      mc->monitor();
    if (mc->key() != "") {
        url += "?" + mc->key() + "=" + mc->value();
    }
    HttpResult* result = HttpResult::GetUrl(url);
    return result;
}

TEST_F(LogMonitorTest, RunReplayer) {
    sleep(2);

    MonitorCall mc(m->port(), "log", "state", "resume");
    pthread_t t;
    pthread_create(&t, NULL, log_monitor_with_param, (void *) &mc);

    HttpResult* buffer = NULL;
    pthread_join(t, (void * *) &buffer);

    ASSERT_TRUE(buffer);
    string content((const char *) buffer->content());
    ASSERT_TRUE(Contains(content, "running")) << content;
    ASSERT_TRUE(content.size() > 0);
    ASSERT_TRUE(IsJson(content));

    if (buffer) {
        delete buffer;
    }
    ASSERT_EQ(ds->log_replayer()->state(), LogReplayer::LOG_REPLAYER_STATE_RUNNING);
}

TEST_F(LogMonitorTest, PauseReplayer) {
    sleep(2);
    ASSERT_TRUE(ds->log_replayer()->Resume());
    sleep(2);

    MonitorCall mc(m->port(), "log", "state", "pause");
    pthread_t t;
    pthread_create(&t, NULL, log_monitor_with_param, (void *) &mc);

    HttpResult* buffer = NULL;
    pthread_join(t, (void * *) &buffer);

    ASSERT_TRUE(buffer);
    string content((const char *) buffer->content());

    ASSERT_TRUE(Contains(content, "paused"));

    ASSERT_TRUE(content.size() > 0);
    ASSERT_TRUE(IsJson(content));

    if (buffer) {
        delete buffer;
    }
    ASSERT_EQ(ds->log_replayer()->state(), LogReplayer::LOG_REPLAYER_STATE_PAUSED);
}

}
}
