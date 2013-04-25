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

#ifndef IDLE_DETECTOR_H__
#define IDLE_DETECTOR_H__

#include <string>
#include <list>
#include <map>

#include <core/dedup.h>
#include <core/request.h>
#include <base/thread.h>
#include <core/log.h>
#include <base/option.h>
#include <base/sliding_average.h>

#include <tbb/tick_count.h>

namespace dedupv1 {

/**
 * Observer of the idle state.
 *
 * The observer is notified if an idle period has been detected (IdleStart),
 * during the idle period (IdleTick) and when the end of an idle period has been detected (IdleEnd).
 *
 * The interval between IdleTick calls might be changed, e.g. due to the number of requests.
 *
 * The class is designed to be overwritten.
 */
class IdleTickConsumer {
    public:
        /**
         * Constructor.
         * @return
         */
        IdleTickConsumer();

        /**
         * Destructor.
         * @return
         */
        virtual ~IdleTickConsumer();

        /**
         * Calls when an idle period has been detected.
         * Subclasses may overwrite this method.
         */
        virtual void IdleStart();

        /**
         * Subclasses may overwrite this method.
         */
        virtual void IdleTick();

        /**
         * Subclasses may overwrite this method.
         */
        virtual void IdleEnd();
};

/**
 * A very basic idle detection that assumes complete idleness for a given time.
 *
 * Thread safety: The idle detection can after the Start be used from
 * multiple threads.
 */
class IdleDetector : public dedupv1::StatisticProvider {
    DISALLOW_COPY_AND_ASSIGN(IdleDetector);

    /**
     * maximal amount of average latency in milliseconds of the last requests so that the system goes into idle mode.
     * Often additional tasks are performed when the system is idle, if the system is already under heavy load (which here usually indicates
     * a hardware issue) we do not want the increase the load anymore.
     */
    static const uint32_t kMaxLatency = 512;

    /**
     * Enumeration for the states of the idle detector
     */
    enum state {
        CREATED,     //!< CREATED
        STARTED,     //!< STARTED
        RUNNING,     //!< RUNNING
        STOPPING,    //!< STOPPING
        STOPPED,     //!< STOPPED
    };

    enum idle_state {
        STATE_BUSY,
        STATE_IDLE
    };

    /**
     * State of the idle detector
     */
    tbb::atomic<enum state> state_;

	/**
     * Idle state (Busy / Idle)
     */
    tbb::atomic<enum idle_state> idle_state_;

    /**
     * tick count where the an idle period starts the last time
     */
    tbb::tick_count idle_start_time_;

    /**
     * Time (in seconds) between idle ticks.
     * Default: 5 seconds
     */
    uint32_t idle_tick_interval_;

    /**
     * maximal throughput that is allowed while the system is idle.
     * The value is compared against a sliding average over the last 30 seconds
     */
    uint32_t max_average_throughput_;

    /**
     * interval in seconds between two checks if the idle state changed
     */
    uint32_t idle_check_interval_;

	/**
     * tick count of the last time a IdleTick was send
     */
    tbb::tick_count last_tick_time_;

	/**
     * idle thread
     */
    dedupv1::base::Thread<bool> idle_thread_;

	/**
     * if true, the system is marked as idle even if the system is usually to busy for it
     */
    tbb::atomic<bool> forced_idle_;

	/**
     * if true, the system is marked as busy even when there is no traffic at all.
     *  forced busy has a higher priority than forced idle
     */
    tbb::atomic<bool> forced_busy_;

    /**
     * does not protect the state
     */
    tbb::spin_mutex lock_;


    /**
     * map of all idle tick consumers mapped by the name
     * of the consumer
     */
    std::map<std::string, IdleTickConsumer*> consumer_;

    tbb::atomic<bool> notify_about_idle_end_;

	/**
     * Counter for the sliding average throughput calculation
     *
     * Protected by sliding_data_lock_
     */
    dedupv1::base::SlidingAverage sliding_througput_average_;

	/**
	 * Counter for the sliding average throughput calculation
     *
     * Protected by sliding_data_lock_
     */
    dedupv1::base::SlidingAverage sliding_latency_average_;

	/**
     * Lock to protect the sliding average calculation
     */
    tbb::spin_mutex sliding_data_lock_;

    /**
     * first tick set at the start of the idle detector.
     * The value is used to calculate the number of seconds since the start for the sliding average calculation
     *
     * Protected by sliding_data_lock_
     */
    tbb::tick_count sliding_start_tick_;

    /**
     * Update the idle state.
     */
    void UpdateIdleState();

    /**
     * Idle loop method that is executed in the idle thread.
     */
    bool IdleLoop();

    /**
     * Publishes an idle tick to all consumers
     *
     * Idle lock has to be acquired
     * @return
     */
    void PublishIdleTick();

    /**
     * Publishes an idle period detection to all consumers
     *
     * Idle lock has to be acquired
     * @return
     */
    void PublishIdleStart();

    /**
     * Publishes the end of an idle period to all consumers
     *
     * Idle lock has to be acquired
     * @return
     */
    void PublishIdleEnd();

    public:
    /**
     * Constructor
     * @return
     */
    IdleDetector();

    /**
     * Destructor
     * @return
     */
    virtual ~IdleDetector();

    /**
     * Configures the idle detector
     *
     * Available options:
     * - idle-throughput: StorageUnit
     * - idle-tick-interval: uint32_t
     * - idle-check-interval: uint32_t
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Starts the idle detector
     * @return true iff ok, otherwise an error has occurred
     */
    bool Start();

    /**
     * Runs the idle detector and starts its threads.
     * @return true iff ok, otherwise an error has occurred
     */
    bool Run();

    /**
     * Stops the idle detector and its threads
     * @return true iff ok, otherwise an error has occurred
     */
    bool Stop(const dedupv1::StopContext& stop_context);

    /**
     * Registers an idle tick consumer
     *
     * @param name
     * @param consumer
     * @return true iff ok, otherwise an error has occurred
     */
    bool RegisterIdleConsumer(const std::string& name, IdleTickConsumer* consumer);

    /**
     * Unregisters an idle tick consumer with a given name
     * @param name
     * @return true iff ok, otherwise an error has occurred
     */
    bool UnregisterIdleConsumer(const std::string& name);

    /**
     * Callback that is called when a request took place.
     *
     * @param rw is the request a read or a write request
     * @param request_index block id of the request
     * @param request_offset offset within the block
     * @param size size of the request within the block
     * @param replay_latency latency in ms needed to process the request.
     * @return true iff ok, otherwise an error has occurred
     */
    bool OnRequestEnd(enum request_type rw, uint64_t request_index, uint64_t request_offset, uint64_t size, double replay_latency);

    /**
     * checks if a idle tick consumer with the given name is registered.
     */
    dedupv1::base::Option<bool> IsRegistered(const std::string& name);

    /**
     * TODO (dmeister): ???
     * @return
     */
    inline uint32_t GetIdleTickInterval() const;

    /**
     * returns if the system is idle
     * @return
     */
    inline bool IsIdle() const;

    /**
     * return the number of seconds, the system has been declared to be idle.
     * return 0.0 if the system is busy.
     */
    inline double GetIdleTime();

    /**
     * Forces the detector to think that the system is idle
     * Mainly used for testing
     * @return
     */
    bool ForceIdle(bool new_idle_value);

    /**
     * Forces the detector to think that the system is busy
     * Mainly used for testing.
     * @return
     */
    bool ForceBusy(bool new_busy_value);

    /**
     * Changes the idle tick interval
     */
    bool ChangeIdleTickInterval(uint32_t seconds);

    /**
     * prints statistics about the idle state
     * @return
     */
    virtual std::string PrintStatistics();

    /**
     * prints trace statistics about the idle state
     * @return
     */
    virtual std::string PrintTrace();

    /**
     * return true iff forced idle is set
     */
    inline bool is_forced_idle() {
        return forced_idle_;
    }

    /**
     * return true iff forced busy is set
     */
    inline bool is_forced_busy() {
        return forced_busy_;
    }

#ifdef DEDUPV1_CORE_TEST
    void ClearData();
#endif
};

uint32_t IdleDetector::GetIdleTickInterval() const {
    return idle_tick_interval_;
}

double IdleDetector::GetIdleTime() {
    tbb::spin_mutex::scoped_lock l(this->lock_);
    if (IsIdle()) {
        double ms = (tbb::tick_count::now() - idle_start_time_).seconds();
        return ms;
    }
    return 0.0;
}

bool IdleDetector::IsIdle() const {
    return this->idle_state_ == STATE_IDLE;
}

}

#endif  // IDLE_DETECTOR_H__
