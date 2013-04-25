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

// Definition for strnlen
#define __USE_GNU

#include "monitor.h"

#include <base/config.h>
#include <base/socket.h>
#include <base/threadpool.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/socket.h>
#include <sstream>

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <microhttpd.h>
#include <tcutil.h>

using std::string;
using std::stringstream;
using dedupv1::base::ScopedLock;
using dedupv1::base::strutil::IsPrintable;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::To;
using dedupv1::base::Socket;
using dedupv1::base::Option;
using dedupv1::base::Profile;
using dedupv1::base::ProfileTimer;

LOGGER("MonitorSystem");

namespace dedupv1d {
namespace monitor {

const int MonitorSystem::kDefaultMonitorPort = DEDUPV1_DEFAULT_MONITOR_PORT;

bool MonitorAdapterRequest::ParseParam(const std::string& key, const std::string& value) {
    return true;
}

string MonitorAdapter::GetContentType() {
    return "text/plain";
}

MonitorAdapter::MonitorAdapter() {
}

MonitorAdapter::~MonitorAdapter() {
}

MonitorAdapterRequest::MonitorAdapterRequest() {
    this->monitor_called_ = false;
}

MonitorAdapterRequest::~MonitorAdapterRequest() {
}

int MonitorAdapterRequest::PerformRequest(uint64_t pos, char* buf, uint32_t count) {
    if (this->monitor_called_ == false) {
        this->buffer_ = this->Monitor();
        this->monitor_called_ = true;
    }
    int len = count;
    if (pos + count > this->buffer_.size()) {
        len =  this->buffer_.size() - pos;
    }
    if (len > 0) {
        strncpy(buf, this->buffer_.c_str() + pos, count);
    }
    return len;
}

MonitorRequest::MonitorRequest(MonitorSystem* monitor_system, MonitorAdapterRequest* request) {
    this->monitor_system_ = monitor_system;
    this->request_ = request;
}

MonitorSystem::Statistics::Statistics() {
    call_count_ = 0;
}

MonitorSystem::MonitorSystem() {
    this->http_server_ = NULL;
    this->port_ = kDefaultMonitorPort;
    this->host_ = "127.0.0.1";
    this->port_auto_assign_ = false;
    this->state_ = MONITOR_STATE_CREATED;
    this->monitor_count_ = 0;

    MHD_set_panic_func(&MonitorSystem::MHDPanicHandler, NULL);
}

void MonitorSystem::MHDPanicHandler(void *cls, const char *file, unsigned int line, const char *reason) {
    stringstream sstr;
    sstr << "httpd panic: ";
    if (reason) {
        sstr << "reason: " << reason << ", ";
    }
    sstr << "file " << file << ", line " << line;
    ERROR(sstr.str());
}

void MonitorSystem::MHDLogHandler(void* cls, const char* fmt, va_list a) {
    ssize_t buffer_size = 1024;
    char* buffer = new char[buffer_size];
    vsnprintf(buffer, buffer_size, fmt, a);
    INFO(buffer);
    delete[] buffer;
}

bool MonitorSystem::Add(const string& name, MonitorAdapter* adapter) {
    CHECK(name.size() > 0, "Name not set");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot lock monitor");

    CHECK(this->FindAdapter(name) == 0, "Duplicate monitor name " << name);

    this->instances_[name] = adapter;
    monitor_count_++;

    CHECK(scoped_lock.ReleaseLock(), "Cannot unlock monitor");
    return true;
}

bool MonitorSystem::RemoveAll() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot lock monitor");

    std::map<std::string, MonitorAdapter*>::iterator i;
    for (i = this->instances_.begin(); i != this->instances_.end(); i++) {
        delete i->second;
    }
    this->instances_.clear();
    monitor_count_ = 0;
    CHECK(scoped_lock.ReleaseLock(), "Cannot unlock monitor");
    return true;
}

string MonitorSystem::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"monitor call count\": " << this->stats_.call_count_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string MonitorSystem::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"monitor time\": " << this->stats_.timing_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

bool MonitorSystem::Remove(const string& name) {
    CHECK(name.size() > 0, "Name not set");
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot lock monitor");

    std::map<std::string, MonitorAdapter*>::iterator i = instances_.find(name);
    CHECK(i != instances_.end(), "No monitor adapter " << name);

    delete i->second;
    instances_.erase(i);
    monitor_count_--;
    CHECK(scoped_lock.ReleaseLock(), "Cannot unlock monitor");
    return true;
}

MonitorAdapter* MonitorSystem::FindAdapter(const string& monitor_type) {
    std::map<std::string, MonitorAdapter*>::iterator i = instances_.find(monitor_type);

    if (i == instances_.end()) {
        return NULL;
    }
    return i->second;
}

static int monitor_request_send_response(struct MHD_Connection* connection, unsigned int status_code, const char* content) {
    int ret = 0;
    struct MHD_Response* response = MHD_create_response_from_data(strlen(content),(void *) content, false, true);
    MHD_add_response_header(response, "Content-Type", "text/plain");
    ret = MHD_queue_response(connection, status_code, response);
    MHD_destroy_response(response);
    return ret;
}

int MonitorRequest::KeyValueIteratorCallback(void *cls, enum MHD_ValueKind kind, const char *key, const char *value) {
    MonitorRequest* mr = reinterpret_cast<MonitorRequest*>(cls);
    CHECK_RETURN(mr, MHD_NO, "Monitor request not set");

    bool rc = true;
    if (kind == MHD_HEADER_KIND) {
        return MHD_YES;
    }

    string keystr;
    if (key) {
        keystr = string(key);
    }
    string valuestr;
    if (value) {
        valuestr = string(value);
    }

    if (keystr.empty() && valuestr.empty()) {
        return MHD_YES;
    }

    CHECK_RETURN(mr->request(), MHD_NO, "Request not set");
    CHECK_RETURN(IsPrintable(keystr), MHD_NO, "Parameter key contains non-printable characters: " <<
        ToHexString(keystr.data(), keystr.size()));
    CHECK_RETURN(IsPrintable(valuestr), MHD_NO, "Parameter value contains non-printable characters: " <<
        ToHexString(valuestr.data(), valuestr.size()) <<
        ", key " << keystr);
    rc = mr->request()->ParseParam(keystr, valuestr);
    if (rc == true) {
        return MHD_YES;
    }
    return MHD_NO;
}

int MonitorSystem::AccessCallback(void *cls, const struct sockaddr *addr, socklen_t addrlen) {
    CHECK_RETURN(addr, MHD_NO, "Addr not set");
    if (addr->sa_family != AF_INET) {
        return MHD_NO;
    }
    struct sockaddr_in* addr_in = (struct sockaddr_in *) addr;
    char buffer[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr_in->sin_addr, buffer, INET_ADDRSTRLEN);

    DEBUG("Monitor request from host " << buffer);
    string client_addr(buffer);
    if (client_addr != "127.0.0.1") {
        WARNING("Forbidden request from " << buffer);
        return MHD_NO;
    }
    return MHD_YES;
}

ssize_t MonitorRequest::RequestCallback(void *cls, uint64_t pos, char *buf, size_t max) {
    MonitorRequest* mr = reinterpret_cast<MonitorRequest*>(cls);
    CHECK_RETURN(mr, -1, "Monitor request not set");

    CHECK_RETURN(mr->lock_.AcquireLock(), -1, "Failed to acquire monitor lock");

    ProfileTimer timer(mr->monitor_system_->stats_.timing_);
    mr->monitor_system_->stats_.call_count_++;

    ssize_t ret = mr->request()->PerformRequest(pos, buf, max);

    if (ret < 0) {
        // error
        WARNING("Request failed");
        ret = MHD_CONTENT_READER_END_WITH_ERROR;
    } else if (ret == 0) {
        // Request finished
        ret = MHD_CONTENT_READER_END_OF_STREAM;
    }
    CHECK_RETURN(mr->lock_.ReleaseLock(), -1, "Failed to release monitor lock");
    return ret;
}

MonitorRequest::~MonitorRequest() {
    delete request_;
}

void MonitorRequest::RequestCallbackFree(void* cls) {
    MonitorRequest* mr = reinterpret_cast<MonitorRequest*>(cls);
    if (!mr) {
        WARNING("Monitor request not set");
        return;
    }
    delete mr;
    mr = NULL;
}

std::vector<std::string> MonitorSystem::GetMonitorNames() {
    std::vector<std::string> result;
    ScopedLock scoped_lock(&lock_);
    result.reserve(instances_.size());
    for (std::map<std::string, MonitorAdapter*>::iterator it = instances_.begin(); it != instances_.end(); it++) {
        result.push_back(it->first);
    }
    return result;
}

int MonitorSystem::RequestCallback(void* cls,struct MHD_Connection *connection, const char *url, const char *method, const char *version, const char *upload_data, size_t *upload_data_size, void **con_cls) {
    int ret = MHD_NO;
    MonitorSystem* m = reinterpret_cast<MonitorSystem*>(cls);
    if (m) {
        ret = m->DoRequestCallback(connection, url, method, version, upload_data, upload_data_size, con_cls);
    }
    if (ret == MHD_NO) {
        return monitor_request_send_response(connection, MHD_HTTP_OK, "{\"ERROR\": \"Internal Server Error\"}");
    }
    return ret;
}

int MonitorSystem::DoRequestCallback(struct MHD_Connection *connection, const char *url, const char *method, const char *version, const char *upload_data, size_t *upload_data_size, void **con_cls) {
    DCHECK_RETURN(connection, MHD_NO, "Connection not set");

    int ret = MHD_YES;
    const char* monitor_type = url + 1;
    if (state_ != MONITOR_STATE_STARTED) {
        return MHD_NO;
    }

    DEBUG("Monitor request: " << method << " " << url);

    ScopedLock scoped_lock(&lock_);
    CHECK_RETURN(scoped_lock.AcquireLock(), 0, "Monitor lock lock failed");
    MonitorAdapter* adapter = FindAdapter(monitor_type);
    if (adapter == NULL) {
        WARNING("Cannot find monitor: adapter " << monitor_type);
        return monitor_request_send_response(connection, MHD_HTTP_BAD_REQUEST, "{\"ERROR\": \"Unknown monitor\"}");
    }

    MonitorAdapterRequest* request = adapter->OpenRequest();
    CHECK_RETURN(request, MHD_NO, "Cannot open request");

    // the monitor request is closed by the RequestCallbackFree after
    // the request is processed. The request is usually
    // not processed when this method exists
    MonitorRequest* mr = new MonitorRequest(this, request);

    MHD_get_connection_values(connection, MHD_GET_ARGUMENT_KIND,
        &MonitorRequest::KeyValueIteratorCallback, mr);
    MHD_get_connection_values(connection, MHD_HEADER_KIND,
        &MonitorRequest::KeyValueIteratorCallback, mr);

    struct MHD_Response* response = MHD_create_response_from_callback(MHD_SIZE_UNKNOWN, 32 * 1024,
        &MonitorRequest::RequestCallback, mr,
        &MonitorRequest::RequestCallbackFree);
    CHECK_RETURN(scoped_lock.ReleaseLock(), MHD_NO, "Unlock monitor lock failed");

    if (response == NULL) {
        ERROR("Failed to create response");
        return MHD_NO;
    }

    string content_type = adapter->GetContentType();
    MHD_add_response_header(response, "Content-Type", content_type.c_str());
    ret = MHD_queue_response(connection, MHD_HTTP_OK, response);

    if (ret == MHD_NO) {
        ERROR("Queue response failed");
    }
    MHD_destroy_response(response);
    return ret;

}

bool MonitorSystem::Start(const dedupv1::StartContext& start_context) {
    CHECK(this->state_ == MONITOR_STATE_CREATED, "Illegal monitor state: " << this->state_);
    CHECK(this->port_ > 0, "Monitor socket port not set");

    this->state_ = MONITOR_STATE_STARTED;

    if (port_auto_assign_) {
        INFO("Start monitor with auto-assigned port");
        int first_port = this->port_;
        for (; port_ <= first_port + 256; port_++) {

            DEBUG("Test monitor port " << port_);
            if (!host_.empty()) {
                struct sockaddr_in daemon_ip_addr;
                memset(&daemon_ip_addr, 0, sizeof (struct sockaddr_in));
                daemon_ip_addr.sin_family = AF_INET;
                daemon_ip_addr.sin_port = htons(port_);

                inet_pton(AF_INET, host_.c_str(), &daemon_ip_addr.sin_addr);

                this->http_server_ = MHD_start_daemon(MHD_USE_SELECT_INTERNALLY, this->port_,
                    MonitorSystem::AccessCallback, this, &MonitorSystem::RequestCallback, this,
                    MHD_OPTION_EXTERNAL_LOGGER, &MonitorSystem::MHDLogHandler, NULL,
                    MHD_OPTION_SOCK_ADDR, &daemon_ip_addr,
                    MHD_OPTION_END);
            } else {
                this->http_server_ = MHD_start_daemon(MHD_USE_SELECT_INTERNALLY, this->port_,
                    MonitorSystem::AccessCallback, this, &MonitorSystem::RequestCallback, this,
                    MHD_OPTION_EXTERNAL_LOGGER, &MonitorSystem::MHDLogHandler, NULL,
                    MHD_OPTION_END);
            }
            if (this->http_server_) {
                break;
            }
        }
        if (this->http_server_) {
            INFO("Started monitor on port " << this->port_);
        } else {
            ERROR("Failed to find free port for monitor system");
            goto error;
        }
    } else {
        INFO("Start monitor on port " << this->port_);
        if (!host_.empty()) {
            struct sockaddr_in daemon_ip_addr;
            memset(&daemon_ip_addr, 0, sizeof (struct sockaddr_in));
            daemon_ip_addr.sin_family = AF_INET;
            daemon_ip_addr.sin_port = htons(port_);

            inet_pton(AF_INET, host_.c_str(), &daemon_ip_addr.sin_addr);
            this->http_server_ = MHD_start_daemon(MHD_USE_SELECT_INTERNALLY, this->port_,
                MonitorSystem::AccessCallback, this, &MonitorSystem::RequestCallback, this,
                MHD_OPTION_EXTERNAL_LOGGER, &MonitorSystem::MHDLogHandler, NULL,
                MHD_OPTION_SOCK_ADDR, &daemon_ip_addr,
                MHD_OPTION_END);
        } else {
            this->http_server_ = MHD_start_daemon(MHD_USE_SELECT_INTERNALLY, this->port_,
                MonitorSystem::AccessCallback, this, &MonitorSystem::RequestCallback, this,
                MHD_OPTION_EXTERNAL_LOGGER, &MonitorSystem::MHDLogHandler, NULL,
                MHD_OPTION_END);
        }
        CHECK_GOTO(this->http_server_, "Cannot start http monitor server: port " << this->port_);
    }
    return true;
error:
    this->state_ = MONITOR_STATE_FAILED;
    return false;
}

bool MonitorSystem::SetOption(const string& option_name, const string& option) {
    CHECK(this->state_ == MONITOR_STATE_CREATED, "Illegal monitor state");
    CHECK(option_name.size() > 0, "Option name not set");
    CHECK(option.size() > 0, "Option not set");

    if (option_name == "port") {
        if (option == "auto") {
            this->port_auto_assign_ = true;
            return true;
        }
        CHECK(To<int>(option).valid(), "Illegal option " << option);
        int port = To<int>(option).value();
        CHECK(port > 0 && port < 64 * 1024, "Illegal port " << port);
        this->port_ = port;
        return true;
    }
    if (option_name == "host") {
        if (option == "any") {
            host_.clear();
        }
        Option<sockaddr_in> addr = Socket::GetAddress(option);
        CHECK(addr.valid(), "Illegal option: " << option_name << "=" << option);
        host_ = option;
        return true;
    }
    ERROR("Unknown option " << option_name);
    return false;
}

bool MonitorSystem::Stop(const dedupv1::StopContext& stop_context) {
    CHECK(this, "Monitor not set");

    if (this->http_server_) {
        INFO("Stopping monitor");
        MHD_stop_daemon(this->http_server_);
        this->http_server_ = NULL;
    }
    this->state_ = MONITOR_STATE_STOPPED;
    DEBUG("Stopped monitor");
    return true;
}

MonitorSystem::~MonitorSystem() {
    DEBUG("Closing monitor");
    if (!this->Stop(dedupv1::StopContext::FastStopContext())) {
        WARNING("Cannot stop monitor server");
    }
    if (!this->RemoveAll()) {
        WARNING("Cannot remove monitor adapters");
    }
    MHD_set_panic_func(NULL, NULL);
}

}
}

