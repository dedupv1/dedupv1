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

#include "log_replayer.h"

#include <unistd.h>

#include <string>
#include <vector>
#include <list>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <core/log.h>
#include <base/timer.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <base/option.h>

using std::string;
using dedupv1::base::NewRunnable;
using dedupv1::base::Thread;
using dedupv1::base::ThreadUtil;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::ScopedLock;
using dedupv1::base::Option;
using dedupv1::log::log_replay_result;
using dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG;
using dedupv1::log::LOG_REPLAY_ERROR;
using dedupv1::log::LOG_REPLAY_OK;
using dedupv1::log::LOG_REPLAY_NO_MORE_EVENTS;
using dedupv1::log::Log;
using dedupv1::IdleDetector;
using dedupv1::StopContext;

LOGGER("LogReplayer");

namespace dedupv1d {

LogReplayer::LogReplayer() :
    thread_(NewRunnable(this, &LogReplayer::Loop), "log bg") {
    this->log_ = NULL;
    this->idle_detector_ = NULL;
    this->thread_state_ = false;
    this->throttle_ = 0;
    this->nearly_full_throttle_ = 0;
    this->check_interval_ = 1;
    this->state_ = LOG_REPLAYER_STATE_CREATED;
    this->state_before_idle_ = LOG_REPLAYER_STATE_CREATED;
    this->is_replaying_ = false;
    this->max_area_size_replay_log_full_ = kDefaultMaxAreaSizeReplayLogFull_;
    this->max_area_size_replay_system_idle_ = kDefaultMaxAreaSizeReplaySystemIdle_;
}

bool LogReplayer::Loop() {
    if (!DoLoop()) {
        this->thread_state_ = false;
        CHECK(this->cond_.Broadcast(), "Broadcast failed");
        return false;
    } else {
        CHECK(this->cond_.Broadcast(), "Broadcast failed");
        return true;
    }
}

dedupv1::log::log_replay_result LogReplayer::Replay(uint32_t num_elements) {
    uint64_t replay_log_id = 0;
    enum log_replay_result replay_result = this->log_->Replay(EVENT_REPLAY_MODE_REPLAY_BG, num_elements,
        &replay_log_id, NULL);

    if (replay_result == LOG_REPLAY_ERROR) {
        WARNING("Error while replaying log event: replayed log id " << replay_log_id);
    } else if (replay_result == LOG_REPLAY_OK) {
        DEBUG("Replayed log event: log id " << replay_log_id);
    }
    return replay_result;
}

bool LogReplayer::TryStartReplay() {
    ScopedLock scoped_lock(&this->is_replaying_lock_);
    CHECK(scoped_lock.AcquireLock(), "Lock lock failed");

    if (!is_replaying_) {
        CHECK(this->log_->ReplayStart(EVENT_REPLAY_MODE_REPLAY_BG, false, true),
            "Cannot start log replay");
        is_replaying_ = true;
    }

    CHECK(scoped_lock.ReleaseLock(), "Lock unlock failed");
    return true;
}

bool LogReplayer::TryStopReplay() {
    ScopedLock scoped_lock(&this->is_replaying_lock_);
    CHECK(scoped_lock.AcquireLock(), "Lock lock failed");

    if (is_replaying_) {
        CHECK(this->log_->ReplayStop(EVENT_REPLAY_MODE_REPLAY_BG, true),
            "Cannot start log replay");
        is_replaying_ = false;
    }

    CHECK(scoped_lock.ReleaseLock(), "Lock unlock failed");
    return true;
}

bool LogReplayer::DoLoop() {
    DEBUG("Start log replayer thread");
    ScopedLock scoped_lock(&this->lock_);

    CHECK(scoped_lock.AcquireLock(), "Lock lock failed");
    bool is_running = (this->state_ == LOG_REPLAYER_STATE_RUNNING);
    bool is_paused = (this->state_ == LOG_REPLAYER_STATE_PAUSED);
    CHECK(scoped_lock.ReleaseLock(), "Lock unlock failed");

    while (is_running || is_paused) {
        log_replay_result replay_result = LOG_REPLAY_OK;

        if (this->log_->IsFull()) {
            CHECK(TryStartReplay(), "Cannot start log replay");

            // is nearly full
            replay_result = Replay(this->max_area_size_replay_log_full_);

            if (this->nearly_full_throttle_ > 0) {
                ThreadUtil::Sleep(nearly_full_throttle_, ThreadUtil::MILLISECONDS);
            }
        } else if (is_running) {
            replay_result = Replay(this->max_area_size_replay_system_idle_);

            if (this->throttle_ > 0) {
                ThreadUtil::Sleep(throttle_, ThreadUtil::MILLISECONDS);
            }
        } else {
            replay_result = LOG_REPLAY_NO_MORE_EVENTS;
        }

        if (replay_result == LOG_REPLAY_NO_MORE_EVENTS) {
            CHECK(TryStopReplay(), "Cannot stop log replay");

            ThreadUtil::Sleep(check_interval_, ThreadUtil::SECONDS);
        } else if (replay_result == LOG_REPLAY_ERROR) {
            ERROR("Log replay failed. Stopping log replay");
            return false;
        }

        CHECK(scoped_lock.AcquireLock(), "Lock lock failed");
        is_running = this->state_ == LOG_REPLAYER_STATE_RUNNING;
        is_paused = this->state_ == LOG_REPLAYER_STATE_PAUSED;
        CHECK(scoped_lock.ReleaseLock(), "Lock unlock failed");
    }

    // mark that the replay stopped
    CHECK(TryStopReplay(), "Cannot stop log replay");
    DEBUG("Exit log replayer thread");
    return true;
}

bool LogReplayer::SetOption(const string& option_name, const string& option) {
    CHECK(this->state_ == LOG_REPLAYER_STATE_CREATED, "Illegal state: " << state_);

    if (option_name == "throttle.default") {
        Option<bool> b = To<bool>(option);
        if (b.valid() && !b.value()) {
            // deactivate via throttle.default=false
            this->throttle_ = 0;
            return true;
        }
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->throttle_ = To<uint32_t>(option).value();
        return true;
    } else if (option_name == "area-size-system-idle") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        CHECK(ToStorageUnit(option).value() > 0, "Maximum max area size system idle has to be greater 0.");
        CHECK(ToStorageUnit(option).value() < UINT32_MAX, "Maximum area size system idle " << UINT64_MAX << ", illegal option " << option);
        this->max_area_size_replay_system_idle_ = ToStorageUnit(option).value();
        return true;
    } else if (option_name == "area-size-log-full") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        CHECK(ToStorageUnit(option).value() > 0, "Maximum max area size log full has to be greater 0.");
        CHECK(ToStorageUnit(option).value() < UINT32_MAX, "Maximum area size log full " << UINT64_MAX << ", illegal option " << option);
        this->max_area_size_replay_log_full_ = ToStorageUnit(option).value();
        return true;
    } else if (option_name == "throttle.nearly-full")        {
        Option<bool> b = To<bool>(option);
        if (b.valid() && !b.value()) {
            // deactivate via throttle.nearly-full=false
            this->nearly_full_throttle_ = 0;
            return true;
        }
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->nearly_full_throttle_ = To<uint32_t>(option).value();
        return true;
    }
    ERROR("Unknown option: " << option_name);
    return false;
}

bool LogReplayer::Start(Log* log, IdleDetector* idle_detector) {
    CHECK(log, "Log not set");
    CHECK(log->IsStarted(), "Log not started");
    CHECK(this->state_ == LOG_REPLAYER_STATE_CREATED, "Illegal state (CREATED)");

    this->log_ = log;
    this->idle_detector_ = idle_detector;

    if (nearly_full_throttle_ > throttle_) {
        WARNING("Nearly full throttling higher than default throttling: " << nearly_full_throttle_ << ", " << throttle_);
    }

    if (idle_detector) {
        CHECK(idle_detector->RegisterIdleConsumer("log replayer", this), "Failed to register idle tick consumer");
    }
    INFO("Starting log replayer");

    this->state_ = LOG_REPLAYER_STATE_STARTED;
    return true;
}

bool LogReplayer::Run() {
    CHECK(!this->thread_state_, "Log replayer thread already started");
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Lock lock failed");
    CHECK(this->state_ == LOG_REPLAYER_STATE_STARTED, "Illegal state of log player: " << state_);
    CHECK(scoped_lock.ReleaseLock(), "Unlock lock failed");
    this->thread_state_ = true;
    this->state_ = LOG_REPLAYER_STATE_PAUSED;

    DEBUG("Run log replayer");

    if (!this->thread_.Start()) {
        ERROR("Cannot start log replayer thread");
        this->state_ = LOG_REPLAYER_STATE_FAILED;
        this->thread_state_ = false;
        return false;
    }

    return true;
}

bool LogReplayer::Pause() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Lock lock failed");

    if (this->state_ == LOG_REPLAYER_STATE_PAUSED) {
        return true;
    }

    DEBUG("Pause log replayer");
    CHECK(this->state_ == LOG_REPLAYER_STATE_RUNNING, "Illegal state of log player");
    this->state_ = LOG_REPLAYER_STATE_PAUSED;
    CHECK(scoped_lock.ReleaseLock(), "Unlock lock failed");
    return true;
}

bool LogReplayer::Resume() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Lock lock failed");

    if (this->state_ == LOG_REPLAYER_STATE_RUNNING) {
        return true;
    }

    DEBUG("Resume log replayer");
    CHECK(this->state_ == LOG_REPLAYER_STATE_PAUSED, "Illegal state of log player");
    this->state_ = LOG_REPLAYER_STATE_RUNNING;
    CHECK(scoped_lock.ReleaseLock(), "Unlock lock failed");

    CHECK(TryStartReplay(), "Cannot start log replay");
    return true;
}

bool LogReplayer::Stop(const StopContext& stop_context) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Lock lock failed");

    // should unregister here to avoid getting idle notifications after stopped or while being stopped
    if (this->idle_detector_ && idle_detector_->IsRegistered("log replayer").value()) {
        CHECK(idle_detector_->UnregisterIdleConsumer("log replayer"), "Failed to unregister idle tick consumer");
    }
    if (!this->thread_state_) {
        return true;
    }

    INFO("Stopping log replay");
    this->state_ = LOG_REPLAYER_STATE_STOPPED;
    CHECK(scoped_lock.ReleaseLock(), "Unlock lock failed");

    CHECK(TryStopReplay(), "Cannot stop log replay");

    bool thread_result = false;
    CHECK(this->thread_.Join(&thread_result), "Failed to join log replayer thread");
    if (!thread_result) {
        WARNING("Log replayer thread exited with error");
    }
    CHECK(scoped_lock.AcquireLock(), "Lock lock failed");
    this->thread_state_ = false;
    CHECK(scoped_lock.ReleaseLock(), "Unlock lock failed");
    return true;
}

void LogReplayer::IdleStart() {
    ScopedLock scoped_lock(&this->lock_);
    if (!scoped_lock.AcquireLock()) {
        WARNING("Lock lock failed");
        return;
    }

    DEBUG("Detected start of idle period");
    if (this->state_ == LOG_REPLAYER_STATE_RUNNING || this->state_ == LOG_REPLAYER_STATE_PAUSED) {
        this->state_before_idle_ = this->state_;
        this->state_ = LOG_REPLAYER_STATE_RUNNING;
    }
    if (!scoped_lock.ReleaseLock()) {
        WARNING("Unlock lock failed");
    }

    if (!TryStartReplay()) {
        WARNING("Cannot stop log replay");
    }
}

void LogReplayer::IdleEnd() {
    ScopedLock scoped_lock(&this->lock_);
    if (!scoped_lock.AcquireLock()) {
        WARNING("Lock lock failed");
        return;
    }

    DEBUG("Detected end of idle period");
    if (this->state_ == LOG_REPLAYER_STATE_RUNNING && this->state_before_idle_ == LOG_REPLAYER_STATE_PAUSED) {
        this->state_ = this->state_before_idle_;
        this->state_before_idle_ = LOG_REPLAYER_STATE_CREATED;
    }

    if (!scoped_lock.ReleaseLock()) {
        WARNING("Unlock lock failed");
    }
}

LogReplayer::~LogReplayer() {
    DEBUG("Closing log replayer");

    if (this->thread_state_) {
        if(!this->Stop(dedupv1::StopContext::FastStopContext())) {
          WARNING("Failed to stop log replayer");
        }
    } else {
        // if runner was never called
        if (this->idle_detector_ && idle_detector_->IsRegistered("log replayer").value()) {
            if(!idle_detector_->UnregisterIdleConsumer("log replayer")) {
              WARNING("Failed to unregister idle tick consumer");
            }
        }
    }
}

const char* LogReplayer::state_name() {
    switch (this->state_) {
    case LOG_REPLAYER_STATE_CREATED:
        return "created";
    case LOG_REPLAYER_STATE_RUNNING:
        return "running";
    case LOG_REPLAYER_STATE_STARTED:
        return "started";
    case LOG_REPLAYER_STATE_PAUSED:
        return "paused";
    case LOG_REPLAYER_STATE_STOPPED:
        return "stopped";
    case LOG_REPLAYER_STATE_FAILED:
        return "failed";
    default:
        return "Unknown";
    }
    return NULL;
}

#ifdef DEDUPV1D_TEST
void LogReplayer::ClearData() {
    this->Stop(StopContext::FastStopContext());
}
#endif

}
