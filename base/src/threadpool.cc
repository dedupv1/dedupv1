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

#include <base/threadpool.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/runnable.h>
#include <base/thread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>

LOGGER("Threadpool");

using std::string;
using std::stringstream;
using std::make_pair;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;

namespace dedupv1 {
namespace base {

Threadpool::Threadpool(int count) {
    state_ = THREADPOOL_STATE_INIT;

    for (int i = 0; i < kPriorityCount; i++) {
        running_count_[i] = 0;
        queue_size_[i] = kDefaultQueueSize;
        thread_count_[i] = count;
    }
    next_task_id_ = 1;
    running_thread_count_ = 0;
    finished_thread_count_ = 0;
}

Threadpool::Statistics::Statistics() {
    for (int i = 0; i < kPriorityCount; i++) {
        submitted_task_count_[i] = 0;
        executed_task_count_[i] = 0;
        waiting_task_count_[i] = 0;
        caller_runs_count_[i] = 0;
        reject_count_[i] = 0;
    }
    busy_thread_count_ = 0;
}

bool Threadpool::SetOption(const string& option_name, const string& option) {
    if (option_name == "size") {
        CHECK(To<int>(option).valid(), "Illegal option " << option);
        int size = To<int> (option).value();
        CHECK(size > 0, "Illegal size value");
        this->thread_count_[HIGH_PRIORITY] = std::max(size / 2, 1);
        this->thread_count_[BACKGROUND_PRIORITY] = std::max(size / 2, 1);
        return true;
    }
    if (option_name == "high-priority-thread-count") {
        CHECK(To<int>(option).valid(), "Illegal option " << option);
        int size = To<int> (option).value();
        CHECK(size > 0, "Illegal size value");
        this->thread_count_[HIGH_PRIORITY] = size;
        return true;
    }
    if (option_name == "background-priority-thread-count") {
        CHECK(To<int>(option).valid(), "Illegal option " << option);
        int size = To<int> (option).value();
        CHECK(size > 0, "Illegal size value");
        this->thread_count_[BACKGROUND_PRIORITY] = size;
        return true;
    }
    if (option_name == "queue-size") {
        CHECK(To<int>(option).valid(), "Illegal option " << option);
        int size = To<int> (option).value();
        CHECK(size > 0, "Illegal size value");
        this->queue_size_[HIGH_PRIORITY] = size;
        this->queue_size_[BACKGROUND_PRIORITY] = size;
        return true;
    }
    if (option_name == "high-priority-queue-size") {
        CHECK(To<int>(option).valid(), "Illegal option " << option);
        int size = To<int> (option).value();
        CHECK(size > 0, "Illegal size value");
        this->queue_size_[HIGH_PRIORITY] = size;
        return true;
    }
    if (option_name == "background-priority-queue-size") {
        CHECK(To<int>(option).valid(), "Illegal option " << option);
        int size = To<int> (option).value();
        CHECK(size > 0, "Illegal size value");
        this->queue_size_[BACKGROUND_PRIORITY] = size;
        return true;
    }
    ERROR("Illegal option " << option);
    return false;
}

bool Threadpool::RunTask(const TaskData& task_data, priority prio) {
    TRACE("Execute task: task id " << task_data.task_id() <<
            ", busy thread count " << this->stats_.busy_thread_count_);
    // dequeue operation successful
    DCHECK(task_data.runnable(), "Runnable not set: " << task_data.task_id());

    // Execute
    stats_.waiting_task_count_[prio]--;
    this->running_count_[prio]++;
    this->stats_.busy_thread_count_++;
    bool retval = task_data.runnable()->Run();
    this->running_count_[prio]--;
    stats_.executed_task_count_[prio]++;
    this->stats_.busy_thread_count_--;

    if (task_data.future()) {
        if (!task_data.future()->Set(retval)) {
            WARNING("Failed to set future value: " << task_data.future());
        }
        if (!task_data.future()->Close()) {
            WARNING("Failed to close future: " << task_data.future());
        }
    }

    TRACE("Executed task: task id " << task_data.task_id() <<
            ", busy thread count " << this->stats_.busy_thread_count_);
    return true;
}

bool Threadpool::Runner(int thread_id, priority prio, int queue) {
    DEBUG("Start threadpool thread " << thread_id << ", prio " << prio);
    running_thread_count_++;

    // wait a bit until all threads are started, while avoiding the infinite wait of the
    // old barrier solution
    while (state_ == THREADPOOL_STATE_STARTING) {
        ThreadUtil::Sleep(50, ThreadUtil::MILLISECONDS);
    }

    tbb::concurrent_bounded_queue<TaskData>& queueRef(*task_queue_[prio][queue]);
    TaskData task_data;
    for(;;) {
        queueRef.pop(task_data);
        if (unlikely(task_data.task_id() == kSentinalTaskId)) {
            break;
        }
        RunTask(task_data, prio);
    }
    TRACE("Stop threadpool thread " << thread_id);
    running_thread_count_--;
    finished_thread_count_++;
    return true;
}

Threadpool::~Threadpool() {
    Stop();

    for (int i = 0; i < kPriorityCount; i++) {
        task_queue_[i].resize(thread_count_[i]);

        for (int j = 0; j < thread_count_[i]; j++) {
            delete task_queue_[i][j];
            task_queue_[i][j] = NULL;
        }
    }
}

bool Threadpool::Stop() {
    TRACE("Stopping threadpool");
    if (!(this->state_ == THREADPOOL_STATE_STARTED || state_ == THREADPOOL_STATE_STARTING)) {
        return true;
    }

    TRACE("Stopping threadpool");
    this->state_ = THREADPOOL_STATE_STOPPED;



    for (int j = 0; j < kPriorityCount; j++) {
        for (int i = 0; i < thread_count_[j]; i++) {
            TaskData task_data(kSentinalTaskId, NULL, NULL);

            // Try to push sentinal
            this->task_queue_[j][i]->push(task_data);
        }
    }

    for (int i = 0; i < threads_.size(); i++) {
        if (this->threads_[i]) {
            DEBUG("Stopping thread " << i);
            bool thread_result = false;
            if (this->threads_[i]->IsJoinable()) {
                TRACE("Joining thread " << i);
                CHECK(this->threads_[i]->Join(&thread_result), "Cannot join thread " << i);
                if (!thread_result) {
                    WARNING("Thread " << i << " finished with error");
                }
            }
            delete this->threads_[i];
            this->threads_[i] = NULL;
        }
    }
    threads_.clear();
    DEBUG("Stopped threadpool");

    // abort all open tasks
    for (int prio = 0; prio < kPriorityCount; prio++) {
        for (int j = 0; j < thread_count_[prio]; j++) {
            TaskData task_data;
            while (this->task_queue_[prio][j]->try_pop(task_data)) {
                WARNING("Abort task " << task_data.task_id());
                Runnable<bool>* r = task_data.runnable();
                Future<bool>* future = task_data.future();

                if (future) {
                    if (!future->Abort()) {
                        WARNING("Failed to about a task future");
                    }
                    if (!future->Close()) {
                        WARNING("Failed to close a task future");
                    }
                }
                if (r) {
                    r->Close();
                }
            }
        }
    }
    return true;
}

bool Threadpool::Start() {
    int i = 0;

    CHECK(this->state_ == THREADPOOL_STATE_INIT, "Threadpool already started");
    CHECK(this->thread_count_[HIGH_PRIORITY] > 0, "No thread pool size set");
    CHECK(this->thread_count_[BACKGROUND_PRIORITY] > 0, "No thread pool size set");

    uint32_t total_thread_count = thread_count_[HIGH_PRIORITY] + thread_count_[BACKGROUND_PRIORITY];
    this->threads_.resize(total_thread_count);
    for (i = 0; i < total_thread_count; i++) {
        this->threads_[i] = NULL;
    }

    DEBUG("Start thread pool");

    for (i = 0; i < kPriorityCount; i++) {
        task_queue_[i].resize(thread_count_[i]);
        for (int j = 0; j < thread_count_[i]; j++) {
            task_queue_[i][j] = new tbb::concurrent_bounded_queue<TaskData>();
            task_queue_[i][j]->set_capacity(queue_size_[i]);
        }
    }


    // Thread must be started after state is set
    this->state_ = THREADPOOL_STATE_STARTING;
    bool failed = false;
    int thread_id = 0;
    for (i = 0; i < kPriorityCount && !failed; i++) {
        for (int j = 0; j < thread_count_[i] && !failed; j++) {
            Threadpool* tp = this;
            priority prio = static_cast<priority>(i);
            Runnable<bool>* r = NewRunnable(tp, &Threadpool::Runner, thread_id, prio, j);
            CHECK(r, "Failed to create runnable: i " << thread_id);
            this->threads_[thread_id] = new Thread<bool> (r, "pool " + ToString(thread_id));
            if(!this->threads_[thread_id]->Start()) {
                ERROR("Failed to start threadpool thread " << thread_id);
                TaskData task_data(kSentinalTaskId, NULL, NULL);
                delete threads_[thread_id];
                threads_[thread_id] = NULL;
                failed = true;
            }
            thread_id++;
        }
    }

    if (failed) {
        ERROR("Failed to start all threads");
        state_ = THREADPOOL_STATE_STOPPED;

        for (int j = 0; j < kPriorityCount; j++) {
            for (int i = 0; i < thread_count_[j]; i++) {
                TaskData task_data(kSentinalTaskId, NULL, NULL);

                // Try to push sentinal
                this->task_queue_[j][i]->push(task_data);
            }
        }

        for (int i = 0; i < threads_.size(); i++) {
            if (this->threads_[i]) {
                DEBUG("Stopping thread " << i);
                bool thread_result = false;
                if (this->threads_[i]->IsJoinable()) {
                    TRACE("Joining thread " << i);
                    CHECK(this->threads_[i]->Join(&thread_result), "Cannot join thread " << i);
                    if (!thread_result) {
                        WARNING("Thread " << i << " finished with error");
                    }
                }
                delete this->threads_[i];
                this->threads_[i] = NULL;
            }
        }
        threads_.clear();
        return false;
    }
    // The start of all threads was successful, now we wait for all threads
    while(finished_thread_count_ == 0 && running_thread_count_ < total_thread_count) {
        // no thread failed, but not all threads are started
        ThreadUtil::Sleep(50, ThreadUtil::MILLISECONDS);
    }

    this->state_ = THREADPOOL_STATE_STARTED;
    TRACE("All threads started");
    return true;
}

bool Threadpool::IsStarted() {
    return this->state_ == THREADPOOL_STATE_STARTED;
}

Threadpool::TaskData::TaskData() {
    task_id_ = 0;
    future_ = NULL;
    runnable_ = NULL;
}

void Threadpool::CallerRuns(Runnable<bool>* r, priority prio, Future<bool>* future) {
    bool b = r->Run();
    stats_.submitted_task_count_[prio]++;
    stats_.executed_task_count_[prio]++;

    if (future) {
        if (!future->Set(b)) {
            WARNING("Failed to set future value: " << future);
        }
    }
}

bool Threadpool::DoSubmit(Runnable<bool>* r, priority prio, overflow_strategy overflow_method, Future<bool>* future) {
    DCHECK_RETURN(r, NULL, "Runnable not set");
    DCHECK_RETURN(prio < kPriorityCount, NULL, "Illegal priority: " << prio);
    CHECK_RETURN(this->state_ == THREADPOOL_STATE_STARTED, NULL, "Threadpool already stopped");

    uint64_t task_id = next_task_id_.fetch_and_increment();

    TRACE("Submit task: task id " << task_id << 
            ", priority " << GetPriorityName(prio) <<
            ", busy thread count " << this->stats_.busy_thread_count_ <<
            ", waiting task count " << stats_.waiting_task_count_[prio] <<
            ", queue size " << queue_size_[prio] <<
            ", thread count " << thread_count_[prio]);

    TaskData task_data(task_id, r, future);
    if (future) {
        // if there is a future, it may returned to the user
        future->AddRef();
    }
    stats_.waiting_task_count_[prio]++;
    int queue_index = task_id % thread_count_[prio];
    DCHECK(queue_index < task_queue_[prio].size(),
            "Illegal queue index: queue index " << queue_index <<
            ", task queue size " << task_queue_[prio].size());

    if (overflow_method == ACCEPT) {
        this->task_queue_[prio][queue_index]->push(task_data);
    } else {
        if (this->task_queue_[prio][queue_index]->try_push(task_data)) {
        } else {
            stats_.waiting_task_count_[prio]--;
            if (future) {
                future->Close();
            }
            if (overflow_method == CALLER_RUNS) {
                stats_.caller_runs_count_[prio]++;
                CallerRuns(r, prio, future);
                return true;
            } else if (overflow_method == REJECT) {
                stats_.reject_count_[prio]++;
                WARNING("Reject task (threadpool full)");
                return false;
            }
        }
    }

    stats_.submitted_task_count_[prio]++;
    return true;
}


bool Threadpool::SubmitNoFuture(Runnable<bool>* r,
        priority prio,
        overflow_strategy overflow_method) {
    return DoSubmit(r,
            prio,
            overflow_method, NULL);
}

Future<bool>* Threadpool::Submit(Runnable<bool>* r, priority prio, overflow_strategy overflow_method) {
    Future<bool>* future = new Future<bool> ();

    bool result = DoSubmit(r, prio, overflow_method, future);
    if (!result) {
        future->Close();
        return NULL;
    }
    return future;
}

string Threadpool::PrintProfile() {
    return "null";
}

std::string Threadpool::GetPriorityName(priority prio) {
    if (prio == HIGH_PRIORITY) {
        return "high priority";
    }
    if (prio == BACKGROUND_PRIORITY) {
        return "background priority";
    }
    return "invalid priority";
}

string Threadpool::PrintTrace() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(5);
    sstr << "{" << std::endl;
    sstr << "\"busy thread count\": " << stats_.busy_thread_count_ << "," << std::endl;
    for (int i = 0; i < kPriorityCount; i++) {
        if (i != 0) {
            sstr << "," << std::endl;
        }
        priority prio = static_cast<priority> (i);
        sstr << "\"" << GetPriorityName(prio) << "\": ";
        sstr << "{" << std::endl;
        sstr << "\"running count\": " << this->running_count(prio) << "," << std::endl;
        sstr << "\"submitted task count\": " << stats_.submitted_task_count_[prio] << "," << std::endl;
        sstr << "\"executed task count\": " << stats_.executed_task_count_[prio] << "," << std::endl;
        sstr << "\"waiting task count\": " << stats_.waiting_task_count_[prio] << "," << std::endl;
        sstr << "\"rejected task count\": " << stats_.reject_count_[prio] << "," << std::endl;
        sstr << "\"caller runs task count\": " << stats_.caller_runs_count_[prio] << std::endl;
        sstr << "}" << std::endl;
    }

    sstr << "}" << std::endl;
    return sstr.str();
}

}
}
