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
#ifndef SCHEDULER_H_
#define SCHEDULER_H_

#include <base/base.h>
#include <base/threadpool.h>
#include <base/locks.h>
#include <base/callback.h>
#include <base/semaphore.h>
#include <base/option.h>
#include <string>
#include <map>
#include <tr1/tuple>
#include <tbb/tick_count.h>

namespace dedupv1 {
namespace base {

/**
 * Options of a scheduling task.
 * While there is currently only a single option, we
 * use such a class because this makes the development of
 * future extensions easier.
 */
class ScheduleOptions {
    private:
        /**
         * interval between runs in seconds.
         */
        double interval_;
    public:
        /**
         * Constructor.
         * @return
         */
        ScheduleOptions();

        /**
         * Constructor.
         * @param interval
         * @return
         */
        ScheduleOptions(double interval);

        /**
         * returns the interval between scheduling runs in seconds.
         * @return
         */
        inline double interval() const;
};

double ScheduleOptions::interval() const {
    return interval_;
}

/**
 * Context of the execution of a scheduling context.
 */
class ScheduleContext {
    private:
        bool abort_;
    public:
        ScheduleContext(bool abort);

        /**
         * if set to true, the scheduling is aborted.
         *
         */
        inline bool abort() const;
};

bool ScheduleContext::abort() const {
    return abort_;
}

/**
 * A scheduled task
 */
class ScheduleTask {
    private:
        /**
         * name of the task
         */
        std::string name_;

        /**
         * options to use by the task
         */
        ScheduleOptions options_;

        /**
         * Callback of the task.
         */
        Callback1<bool, const ScheduleContext&>* callback_;

        /**
         * Semaphore
         * TODO (dmeister) ????
         */
        Semaphore* semaphore_;

        /**
         * tick of the last execution.
         */
        tbb::tick_count last_exec_tick_;
    public:
        /**
         * Constructor.
         * @return
         */
        ScheduleTask();

        const std::string& name() const {
            return name_;
        }

        ScheduleTask& set_name(const std::string& name) {
            name_ = name;
            return *this;
        }

        const ScheduleOptions& options() const {
            return options_;
        }

        ScheduleTask& set_options(const ScheduleOptions& options) {
            options_ = options;
            return *this;
        }

        Callback1<bool, const ScheduleContext&>* callback() {
            return callback_;
        }

        ScheduleTask& set_callback(Callback1<bool, const ScheduleContext&>* callback) {
            callback_ = callback;
            return *this;
        }

        Semaphore* semaphore() {
            return semaphore_;
        }

        ScheduleTask& set_semaphore(Semaphore* semaphore) {
            semaphore_ = semaphore;
            return *this;
        }

        tbb::tick_count last_exec_tick() const {
            return last_exec_tick_;
        }

        ScheduleTask& set_last_exec_tick(tbb::tick_count tick) {
            last_exec_tick_ = tick;
            return *this;
        }
};

/**
 * The scheduler is used to execute registered tasks in specific
 * intervals.
 * The scheduler is volatile. All state is lost during a shutdown.
 */
class Scheduler {
        DISALLOW_COPY_AND_ASSIGN(Scheduler);

        /**
         * Enumeration of states for the scheduler
         */
        enum state {
            INITED,
            STARTED,
            RUNNING,
            STOPPED
        };

        /**
         * state of the scheduler
         */
        volatile enum state state_;

        /**
         * Threadpool in which the tasks are executed
         */
        Threadpool* threadpool_;

        /**
         * Lock to protect the members
         * TODO (dmeister): Use a spin lock
         */
        MutexLock lock_;

        /**
         * map from the task name to the task
         */
        std::map<std::string, ScheduleTask> task_map_;

        /**
         * Scheduling thread
         */
        Thread<bool> schedule_thread_;

        /**
          * @return true iff ok, otherwise an error has occurred
         */
        bool ThreadRunner(ScheduleTask task);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool Runner();
    public:
        /**
         * Constructor.
         * @return
         */
        Scheduler();

        virtual ~Scheduler();

        /**
         * Starts the scheduler
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(Threadpool* tp);

        /**
         * Runs the scheduling thread.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Run();

        /**
         * Stops the scheduler
         * @return true iff ok, otherwise an error has occurred
         */
        bool Stop();

        /**
         * Checks if a task with the given name is scheduled
         */
        dedupv1::base::Option<bool> IsScheduled(const std::string& name);

        /**
         * Submits a new task into the scheduler
         * @param name name of the schedules task. Use for removal.
         * @param options options of the scheduling, e.g. the interval
         * @param callback Callback called if the task is due. The client
         * is responsible to ensure that every pointer or reference is valid
         * as long as the scheduled task is not removed from the scheduler. The
         * ownership of the callback is taken over by the scheduler.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool Submit(const std::string& name, const ScheduleOptions& options, Callback1<bool, const ScheduleContext&>* callback);

        /**
         * Removes a task with the given name
         * TODO (dmeister): What is the result if there is no such task?
         */
        bool Remove(const std::string& name);
};

}
}

#endif /* SCHEDULER_H_ */
