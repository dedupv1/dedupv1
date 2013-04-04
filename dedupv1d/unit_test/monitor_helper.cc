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

#include "monitor_helper.h"

#include <base/http_client.h>
#include <base/thread.h>
#include <base/runnable.h>
#include <base/strutil.h>
#include <base/logging.h>

using std::string;
using std::list;
using std::pair;
using dedupv1::base::HttpResult;
using dedupv1::base::strutil::ToString;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::Option;
using dedupv1::base::make_option;

LOGGER("MonitorClient");

MonitorClient::MonitorClient(int port, const string& monitor) {
    this->port = port;
    this->monitor = monitor;
}

MonitorClient::MonitorClient(int port, const string& monitor, const list< pair< string, string> >& params) {
    this->port = port;
    this->monitor = monitor;
    this->params = params;
}

Option<string> MonitorClient::PerformRequest() {
    string url = "http://localhost:" + ToString(this->port) + "/" + this->monitor;
    list< pair< string, string> >::iterator i;
    for (i = this->params.begin(); i != this->params.end(); i++) {
        pair< string, string> param_pair = *i;
        if (i == this->params.begin()) {
            url += "?";
        } else {
            url += "&";
        }
        url += param_pair.first + "=" + param_pair.second;
    }
    HttpResult* result = HttpResult::GetUrl(url);
    if (result == NULL) {
        return false;
    }
    if (result->content() == NULL) {
        delete result;
        return false;
    }
    string content((const char *) result->content());
    delete result;
    return make_option(content);
}

Option<string> MonitorClient::Get() {
    Thread<Option<string> > t(NewRunnable(this, &MonitorClient::PerformRequest), "monitor-client");
    CHECK(t.Start(), "Starting thread failed");
    Option<string> content;
    CHECK(t.Join(&content), "Joining thread failed");
    return content;
}
