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

#include <base/scheduler.h>
#include <base/logging.h>
#include <set>

using std::string;
using std::set;
using std::map;
using dedupv1::base::NewRunnable;
using dedupv1::base::Runnable;
using dedupv1::base::ScopedLock;
using std::tr1::make_tuple;
using tbb::tick_count;
using dedupv1::base::Option;
using dedupv1::base::make_option;

LOGGER("Scheduler");

namespace dedupv1 {
namespace base {

ScheduleOptions::ScheduleOptions() {
    interval_ = 60;
}

ScheduleOptions::ScheduleOptions(double interval) {
    interval_ = interval;
}

ScheduleContext::ScheduleContext(bool abort) {
    abort_ = abort;
}

ScheduleTask::ScheduleTask() {
    callback_ = NULL;
    semaphore_ = NULL;
}

Scheduler::Scheduler() : schedule_thread_(NewRunnable(this, &Scheduler::Runner), "scheduler") {
    threadpool_ = NULL;
    state_ = INITED;
}

bool Scheduler::ThreadRunner(ScheduleTask task) {
    Callback1<bool, const ScheduleContext&>* callback = task.callback();
    Semaphore* semaphore = task.semaphore();
    DEBUG("Running callback " << task.name());

    ScheduleContext context(false);
    bool r = callback->Call(context);
    if (!r) {
        WARNING("Callback " << task.name() << " reported error");
    }
    DEBUG("Finished running callback " << task.name());
    CHECK(semaphore->Post(), "Failed to post task semaphore");
    return true;
}

bool Scheduler::Runner() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire scheduler lock");
    DEBUG("Starting scheduler thread");

    while (state_ == RUNNING) {
        tick_count now = tick_count::now();
        std::map<std::string, ScheduleTask >::iterator i;
        for (i = this->task_map_.begin(); i != task_map_.end(); i++) {
            string callback_name = i->first;
            Callback1<bool, const ScheduleContext&>* callback = i->second.callback();
            Semaphore* semaphore = i->second.semaphore();

            if ((now - i->second.last_exec_tick()).seconds() > i->second.options().interval()) {
                // due
                CHECK(callback, "Callback not set");
                CHECK(semaphore, "Semaphore not set");

                CHECK(semaphore->Wait(), "Failed to wait for task semaphore");

                ScheduleTask task = i->second;

                Runnable<bool>* runnable = NewRunnable(this, &Scheduler::ThreadRunner,
                    task);
                CHECK(runnable, "Failed to create runnable");
                Future<bool>* future = this->threadpool_->Submit(runnable);
                CHECK(future, "Failed to submit runnable");
                delete future; // we are not interested in the result of the execution here

                i->second.set_last_exec_tick(now);
            }
        }
        CHECK(scoped_lock.ReleaseLock(), "Failed to release scheduler lock");
        usleep(250 * 1000);
        CHECK(scoped_lock.AcquireLock(), "Failed to acquire scheduler lock");
    }
    DEBUG("Stopping scheduler thread");
    return true;
}

bool Scheduler::Start(Threadpool* tp) {
    CHECK(state_ == INITED, "Illegal state: " << state_);
    CHECK(tp, "Threadpool not set");
    CHECK(tp->IsStarted(), "Threadpool not started");

    DEBUG("Starting scheduler");
    this->threadpool_ = tp;
    state_ = STARTED;
    return true;
}

bool Scheduler::Run() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire scheduler lock");
    CHECK(state_ == STARTED, "Illegal state: " << state_);

    DEBUG("Running scheduler");
    CHECK(this->schedule_thread_.Start(),
        "Failed to start scheduler thread");

    state_ = RUNNING;
    CHECK(scoped_lock.ReleaseLock(), "Failed to release scheduler lock");
    return true;
}

Scheduler::~Scheduler() {
    DEBUG("Closing scheduler");
    if(!Stop()) {
      WARNING("Failed to stop scheduler");
    }
}

bool Scheduler::Stop() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire scheduler lock");
    if (state_ == RUNNING) {
        DEBUG("Stopping scheduler");
        state_ = STOPPED;
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release scheduler lock");

    if (schedule_thread_.IsJoinable()) {
        bool result = false;
        CHECK(schedule_thread_.Join(&result), "Failed to join scheduler thread");
        if (!result) {
            WARNING("Schedule thread existed with error");
        }
    }

    CHECK(scoped_lock.AcquireLock(), "Failed to acquire scheduler lock");
    std::map<std::string, ScheduleTask>::iterator i;
    for (i = this->task_map_.begin(); i != task_map_.end(); i++) {
        string callback_name = i->first;
        Callback1<bool, const ScheduleContext&>* callback = i->second.callback();
        CHECK(callback, "Callback not set");
        Semaphore* semaphore = i->second.semaphore();
        CHECK(semaphore, "Semaphore not set");

        CHECK(semaphore->Wait(), "Failed to wait for semaphore");

        ScheduleContext schedule_context(true);
        if (!callback->Call(schedule_context)) {
            WARNING("Callback " << callback_name << " reported error during abort");
        }
        CHECK(semaphore->Post(), "Failed to post task semaphore");
        delete callback;
        delete semaphore;
    }
    task_map_.clear();
    CHECK(scoped_lock.ReleaseLock(), "Failed to release scheduler lock");
    DEBUG("Stopped scheduler");
    return true;
}

Option<bool> Scheduler::IsScheduled(const std::string& name) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire scheduler lock");

    std::map<std::string, ScheduleTask>::iterator i;
    i = task_map_.find(name);
    bool b = i != task_map_.end();

    CHECK(scoped_lock.ReleaseLock(), "Failed to release scheduler lock");
    return make_option(b);
}

bool Scheduler::Submit(const std::string& name, const ScheduleOptions& options, Callback1<bool, const ScheduleContext&>* callback) {
    CHECK(callback, "Callback not set");
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire scheduler lock");
    CHECK(state_ == STARTED || state_ == RUNNING, "Illegal state: " << state_);

    DEBUG("Submit scheduled task: " << name);

    std::map<std::string, ScheduleTask>::iterator i;
    i = task_map_.find(name);
    CHECK(i == task_map_.end(), "Callback with name " << name << " already registered");

    Semaphore* semaphore = new Semaphore(1);
    task_map_[name].set_name(name).set_options(options).set_callback(callback).set_semaphore(semaphore).set_last_exec_tick(tick_count::now());

    if (!scoped_lock.ReleaseLock()) {
        // we do not want to have a reference if the call failed
        task_map_.erase(name);
        delete semaphore;
        ERROR("Failed to release scheduler lock");
        return false;
    }
    return true;
}

bool Scheduler::Remove(const std::string& name) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire scheduler lock");

    CHECK(state_ == STARTED || state_ == RUNNING, "Illegal state: " << state_);

    std::map<std::string, ScheduleTask>::iterator i;
    i = task_map_.find(name);
    if (i == task_map_.end()) {
        CHECK(scoped_lock.ReleaseLock(), "Failed to release scheduler lock");
        return true;
    }
    DEBUG("Remove callback with name " << name);
    Callback1<bool, const ScheduleContext&>* callback = i->second.callback();
    Semaphore* semaphore = i->second.semaphore();
    task_map_.erase(i);
    CHECK(scoped_lock.ReleaseLock(), "Failed to release scheduler lock");

    DEBUG("Waiting for last runs to finish");
    CHECK(semaphore->Wait(), "Failed to wait for task semaphore");
    // now we have the task for us and as it is removed from the task map no one
    // can acquire it
    CHECK(semaphore->Post(), "Failed to post task semaphore");
    delete callback;
    delete semaphore;
    return true;
}

}
}
