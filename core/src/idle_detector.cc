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

#include <core/idle_detector.h>

#include <vector>

#include <base/logging.h>
#include <base/runnable.h>
#include <base/strutil.h>
#include <base/thread.h>
#include <core/log.h>

using std::stringstream;
using std::string;
using std::map;
using std::vector;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::Join;
using dedupv1::base::NewRunnable;
using dedupv1::base::ScopedLock;
using dedupv1::base::TIMED_FALSE;
using dedupv1::base::TIMED_TIMEOUT;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::strutil::ToStringAsFixedDecimal;
using dedupv1::base::ThreadUtil;
using tbb::tick_count;

LOGGER("IdleDetector");

namespace dedupv1 {

IdleTickConsumer::IdleTickConsumer() {
}

IdleTickConsumer::~IdleTickConsumer() {
}

void IdleTickConsumer::IdleStart() {
}

void IdleTickConsumer::IdleTick() {
}

void IdleTickConsumer::IdleEnd() {
}

IdleDetector::IdleDetector()
    : idle_thread_(NewRunnable(this, &IdleDetector::IdleLoop), "idle"), sliding_througput_average_(30), sliding_latency_average_(30) {
    this->idle_tick_interval_ = 5;
    this->idle_check_interval_ = 1;
    max_average_throughput_ = 512 * 1024; // 512kb
    this->state_ = CREATED;
    idle_state_ = STATE_BUSY;
    notify_about_idle_end_ = false;
    forced_idle_ = false;
    forced_busy_ = false;
    this->last_tick_time_ = tbb::tick_count::now();
}

bool IdleDetector::SetOption(const string& option_name, const string& option) {
    if (option_name == "idle-throughput") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->max_average_throughput_ = ToStorageUnit(option).value();
        CHECK(this->idle_tick_interval_ > 0, "Idle throughput must be positive");
        return true;
    }
    if (option_name == "idle-tick-interval") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->idle_tick_interval_ = To<uint32_t>(option).value();
        CHECK(this->idle_tick_interval_ > 0, "Idle tick interval cannot be 0s");
        return true;
    }
    if (option_name == "idle-check-interval") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->idle_check_interval_ = To<uint32_t>(option).value();
        CHECK(this->idle_check_interval_ > 0, "Idle check interval cannot be 0s");
        return true;
    }
    return false;
}

bool IdleDetector::Start() {
    CHECK(this->state_ == CREATED, "Illegal state: " << this->state_);

    INFO("Starting idle detector");
    sliding_start_tick_ = tick_count::now();

    this->state_ = STARTED;
    return true;
}

bool IdleDetector::Run() {
    enum state old_state = this->state_.compare_and_swap(RUNNING, STARTED);
    CHECK(old_state == STARTED, "Illegal state: " << this->state_);

    DEBUG("Running idle detector");

    CHECK(this->idle_thread_.Start(), "Cannot start idle thread");
    return true;
}

bool IdleDetector::Stop(const dedupv1::StopContext& stop_context) {
    enum state old_state = this->state_.compare_and_swap(STOPPING, RUNNING);
    if (old_state != RUNNING) {
        return true;
    }
    INFO("Stopping idle detection");

    bool result = false;
    CHECK(this->idle_thread_.Join(&result), "Cannot join idle thread");
    if (!result) {
        WARNING("Idle thread finished with error");
    }
    DEBUG("Stopped idle detection");

    old_state = this->state_.compare_and_swap(STOPPED, STOPPING);
    CHECK(old_state == STOPPING, "Illegal state: " << this->state_);
    return true;
}

IdleDetector::~IdleDetector() {
    DEBUG("Closing idle detection");

    if (this->state_ == RUNNING) {
        if(!this->Stop(dedupv1::StopContext::FastStopContext())) {
          WARNING("Cannot stop idle thread");
      }
    }

    if (this->consumer_.size() > 0) {
        vector<string> consumer_names;
        map<string, IdleTickConsumer*>::iterator i;
        for (i = this->consumer_.begin(); i != this->consumer_.end(); i++) {
            consumer_names.push_back(i->first);
        }
        WARNING("Closing idle detector with consumers: [" << Join(consumer_names.begin(), consumer_names.end(), ", ") << "]");
    }
}

bool IdleDetector::ForceIdle(bool new_idle_value) {
    DEBUG("Force idle " << ToString(new_idle_value));
    forced_idle_ = new_idle_value;
    return true;
}

bool IdleDetector::ForceBusy(bool new_busy_value) {
    DEBUG("Force busy " << ToString(new_busy_value));
    forced_busy_ = new_busy_value;
    return true;
}

void IdleDetector::UpdateIdleState() {
    bool is_idle = false;

    tbb::spin_mutex::scoped_lock scoped_lock(this->sliding_data_lock_);
    uint32_t seconds_since_start = (tbb::tick_count::now() - sliding_start_tick_).seconds();
    double latency_average = sliding_latency_average_.GetAverage(seconds_since_start);
    double throughput_average = sliding_througput_average_.GetAverage(seconds_since_start);
    scoped_lock.release();

    if (latency_average < kMaxLatency && throughput_average < max_average_throughput_) {
        is_idle = true;
    }
    if (forced_idle_) {
        is_idle = true;
    }
    if (forced_busy_) {
        is_idle = false;
    }
    DEBUG("Update idle state: forced idle " << forced_idle_ <<
        ", force busy " << forced_busy_ <<
        ", average throughput " << throughput_average <<
        ", average latency " << latency_average <<
        ", updated idle state " << is_idle);

    if (is_idle) {
        idle_state old_idle_state = idle_state_.compare_and_swap(STATE_IDLE, STATE_BUSY);
        if (old_idle_state == STATE_BUSY) {
            INFO("Detected start of idle: throughput " << ToStringAsFixedDecimal(throughput_average, 0) << "B/s");
            this->PublishIdleStart();
            idle_start_time_ = tick_count::now();

        }
    } else {
        idle_state old_idle_state = idle_state_.compare_and_swap(STATE_BUSY, STATE_IDLE);
        if (old_idle_state == STATE_IDLE) {
            INFO("Detected end of idle: throughput " << ToStringAsFixedDecimal(throughput_average, 0) << "B/s");
            notify_about_idle_end_.compare_and_swap(true, false);
        }
    }
}

bool IdleDetector::IdleLoop() {
    DEBUG("Starting idle thread");
    while (this->state_ == RUNNING) {
        ThreadUtil::Sleep(idle_check_interval_);

        UpdateIdleState();

        if (IsIdle()) {
            tbb::tick_count current_time = tbb::tick_count::now();
            double d = (current_time - this->last_tick_time_).seconds();
            TRACE("Check last tick diff: " << d);
            if (d >= this->idle_tick_interval_) {
                TRACE("Still idle");
                this->PublishIdleTick();
                this->last_tick_time_ = current_time;
            }
        }

        bool should_notify = notify_about_idle_end_.compare_and_swap(false, true);
        if (should_notify) {
            this->PublishIdleEnd();
        }
    }
    DEBUG("Stopping idle thread");
    return true;
}

bool IdleDetector::OnRequestEnd(enum request_type rw, uint64_t request_index, uint64_t request_offset, uint64_t size, double request_latency) {
    tbb::spin_mutex::scoped_lock scoped_lock(this->sliding_data_lock_);
    uint32_t seconds_since_start = (tbb::tick_count::now() - sliding_start_tick_).seconds();
    sliding_latency_average_.Add(seconds_since_start, request_latency);
    sliding_througput_average_.Add(seconds_since_start, size);

    TRACE("Update data based on request: request size " << size << ", request latency " << request_latency <<
        ", latency average " << sliding_latency_average_.GetAverage(seconds_since_start) <<
        ", throughput average " << sliding_througput_average_.GetAverage(seconds_since_start));
    return true;
}

void IdleDetector::PublishIdleTick() {
    tbb::spin_mutex::scoped_lock l(this->lock_);

    DEBUG("Publish idle tick");
    map<string, IdleTickConsumer*>::iterator i;
    for (i = this->consumer_.begin(); i != this->consumer_.end(); i++) {
        i->second->IdleTick();
    }
}

void IdleDetector::PublishIdleStart() {
    tbb::spin_mutex::scoped_lock l(this->lock_);

    DEBUG("Publish idle start");
    map<string, IdleTickConsumer*>::iterator i;
    for (i = this->consumer_.begin(); i != this->consumer_.end(); i++) {
        i->second->IdleStart();
    }
}

void IdleDetector::PublishIdleEnd() {
    tbb::spin_mutex::scoped_lock l(this->lock_);

    DEBUG("Publish idle end");
    map<string, IdleTickConsumer*>::iterator i;
    for (i = this->consumer_.begin(); i != this->consumer_.end(); i++) {
        i->second->IdleEnd();
    }
}

Option<bool> IdleDetector::IsRegistered(const std::string& name) {
    tbb::spin_mutex::scoped_lock l(this->lock_);

    map<string, IdleTickConsumer*>::iterator i = this->consumer_.find(name);
    return make_option(i != this->consumer_.end());
}

bool IdleDetector::RegisterIdleConsumer(const string& name, IdleTickConsumer* consumer) {
    CHECK(consumer, "Consumer must be set");
    CHECK(name.size(), "Name must be set");

    DEBUG("Register consumer " << name);

    tbb::spin_mutex::scoped_lock l(this->lock_);
    map<string, IdleTickConsumer*>::iterator i = this->consumer_.find(name);
    CHECK(i == this->consumer_.end(), "Idle consumer already registered: name " << name);

    this->consumer_[name] = consumer;
    return true;
}

bool IdleDetector::UnregisterIdleConsumer(const string& name) {
    tbb::spin_mutex::scoped_lock l(this->lock_);

    DEBUG("Unregister consumer " << name);
    map<string, IdleTickConsumer*>::iterator i = this->consumer_.find(name);
    CHECK(i != this->consumer_.end(), "Unknown idle consumer: name " << name);

    this->consumer_.erase(i);
    return true;
}

bool IdleDetector::ChangeIdleTickInterval(uint32_t seconds) {
    tbb::spin_mutex::scoped_lock l(this->lock_);
    this->idle_tick_interval_ = seconds;
    return true;
}

string IdleDetector::PrintStatistics() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{";
    sstr << "\"idle time\":" << GetIdleTime() << ",";
    sstr << "\"forced idle\": " << ToString(static_cast<bool>(forced_idle_)) << ",";
    sstr << "\"forced busy\": " << ToString(static_cast<bool>(forced_busy_));
    sstr << "}";
    return sstr.str();

}

string IdleDetector::PrintTrace() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    tbb::spin_mutex::scoped_lock l(this->sliding_data_lock_);
    uint32_t seconds_since_start = (tbb::tick_count::now() - sliding_start_tick_).seconds();
    double sla = sliding_latency_average_.GetAverage(seconds_since_start);
    double sta = sliding_througput_average_.GetAverage(seconds_since_start);
    l.release();

    sstr << "{";
    sstr << "\"sliding latency average\": " << sla << ",";
    sstr << "\"sliding throughput average\": " << sta << "";
    sstr << "}";
    return sstr.str();
}

#ifdef DEDUPV1_CORE_TEST
void IdleDetector::ClearData() {
    this->Stop(StopContext::WritebackStopContext());
}
#endif

}

