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

#ifndef LOG_REPLAYER_H__
#define LOG_REPLAYER_H__

#include <core/dedup.h>
#include <base/locks.h>
#include <base/thread.h>
#include <core/idle_detector.h>
#include <core/log.h>

namespace dedupv1d {

/**
 * The LogReplayer has the responsibility to replay log entries in background if any of the following
 * two conditions are true:
 * - The log is nearly full: Nearly full is usually seen as something like 70% of the log capacity
 * - The system is idle and the log replaying is activated
 *
 * If the system detects and idle period and if the replay during idle time is not deactivated using the configuration system,
 * the log replayer will replay event. When the idle period ends and the system has not been stopped before, the old state is
 * restored.
 *
 * TODO (dmeister) Adapt the replay speed to the amount of idle ticks.
 */
class LogReplayer: public dedupv1::IdleTickConsumer {
        DISALLOW_COPY_AND_ASSIGN(LogReplayer);
    public:
        /**
         * stats of the log replayer
         */
        enum log_replayer_state {
            LOG_REPLAYER_STATE_CREATED,//!< LOG_REPLAYER_STATE_CREATED
            LOG_REPLAYER_STATE_STARTED,
            LOG_REPLAYER_STATE_RUNNING,//!< LOG_REPLAYER_STATE_RUNNING
            LOG_REPLAYER_STATE_PAUSED, //!< LOG_REPLAYER_STATE_PAUSED
            LOG_REPLAYER_STATE_STOPPED,//!< LOG_REPLAYER_STATE_STOPPED
            LOG_REPLAYER_STATE_FAILED
        //!< LOG_REPLAYER_STATE_FAILED
        };
    private:

        /**
         * Default number of elements replayed at one if system is idle
         */
        static const uint32_t kDefaultMaxAreaSizeReplaySystemIdle_ = 4096;

        /**
         * Default number of elements replayed at one if system going full
         */
        static const uint32_t kDefaultMaxAreaSizeReplayLogFull_ = 4096;

        /**
         * Reference to the log.
         * Set during the startup
         */
        dedupv1::log::Log* log_;

        /**
         * Reference to the idle detector
         * Set during the startup, but maybe null if no idle system is used
         */
        dedupv1::IdleDetector* idle_detector_;

        /**
         * log replay background thread
         */
        dedupv1::base::Thread<bool> thread_;

        /**
         * State of the log replayer.
         */
        volatile enum log_replayer_state state_;

        /**
         * State of the log replayer thread.
         */
        volatile bool thread_state_;

        /**
         * Lock to protected shared variables of the log replayer, e.g. the stats
         */
        dedupv1::base::MutexLock lock_;

        /**
         * Condition that is fired if the log replayer thread stops.
         */
        dedupv1::base::Condition cond_;

        /**
         * Sleep time between in microseconds.
         * Default: 50ms
         */
        uint32_t throttle_;

        /**
         * Sleep time in microseconds when the log is nearly full
         * Default: 0ms
         */
        uint32_t nearly_full_throttle_;

        /**
         * Sleep time between checks
         * Default: 1s
         */
        uint32_t check_interval_;

        /**
         * Stores the state of the system before the idle period.
         * LOG_REPLAYER_STATE_CREATED denotes a non-set value
         */
        enum log_replayer_state state_before_idle_;

        /**
         * iff true, the log is currently replaying items because of one of several reasons:
         * idleness, unpauses log replay, log full.
         */
        bool is_replaying_;

        /**
         * lock to protect is_replaying_
         */
        dedupv1::base::MutexLock is_replaying_lock_;

        /**
         * Maxmimum Events to be replayed at once if system is idle
         */
        uint32_t max_area_size_replay_system_idle_;

        /**
         * Maxmimum Events to be replayed at once if log is going full
         */
        uint32_t max_area_size_replay_log_full_;

        /**
         * Loop method that is called in a background thread.
         * @return
         */
        bool Loop();

        /**
         * the actual loop contents
         */
        bool DoLoop();

        /**
         * replays a single log event.
         *
         * @param num_elements Number of Elements to replay
         */
        dedupv1::log::log_replay_result Replay(uint32_t num_elements = 1);

        /**
         * Tries to start the log replay.
         * Does nothing if the log is already being replayed
         */
        bool TryStartReplay();

        /**
         * Tries to stop the log replay.
         * Does nothing if the log is already being replayed
         */
        bool TryStopReplay();
    public:
        /**
         * Constructor.
         * @return
         */
        LogReplayer();

        /**
         * Destructor
         * @return
         */
        virtual ~LogReplayer();

        /**
         *
         * @param log
         * @param idle_detector reference to the idle detector. May be null. If a idle detector is given, the client has to ensure
         * that the detector lives longer than the log replayer.
         * @return
         */
        bool Start(dedupv1::log::Log* log, dedupv1::IdleDetector* idle_detector);

        /**
         * Runs the log replayer
         * @return
         */
        bool Run();

        /**
         * Stops the replayer
         * @param stop_context
         * @return
         */
        bool Stop(const dedupv1::StopContext& stop_context);

        /**
         * Pauses the log replayer.
         * @return
         */
        bool Pause();

        /**
         * Resumes the log replayer
         * @return
         */
        bool Resume();

        /**
         * Configures the log replayer
         *
         * Available options:
         * - throttle.default: uint32_t
         * - throttle.nearly-full: uint32_t
         * - area-size-system-idle: StorageUnit
         * - area-size-log-full: StorageUnit
         *
         * @param option_name
         * @param option
         * @return
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Returns the name of the current state
         * @return
         */
        const char* state_name();

        /**
         * Returns the log of the replayer
         * @return
         */
        inline dedupv1::log::Log* log();

        /**
         * Returns the state of the log replayer
         * @return
         */
        inline enum log_replayer_state state() const;

        /**
         * Called then the system is detected to be idle
         */
        void IdleStart();

        /**
         * Called when the system stopped to be idle
         */
        void IdleEnd();

        inline bool is_replaying() const;

        inline bool is_failed() const;

#ifdef DEDUPV1D_TEST
        void ClearData();
#endif
};

bool LogReplayer::is_failed() const {
    return state_ == LOG_REPLAYER_STATE_FAILED ||
            (state_ == LOG_REPLAYER_STATE_PAUSED && !thread_state_)  ||
            (state_ == LOG_REPLAYER_STATE_RUNNING && !thread_state_);
}

dedupv1::log::Log* LogReplayer::log() {
    return this->log_;
}

LogReplayer::log_replayer_state LogReplayer::state() const {
    return this->state_;
}

bool LogReplayer::is_replaying() const {
    return is_replaying_;
}

}

#endif  // LOG_REPLAYER_H__
