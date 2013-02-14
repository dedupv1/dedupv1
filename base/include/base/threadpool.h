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

#ifndef THREADPOOL_H__
#define THREADPOOL_H__

#include <string>
#include <vector>

#include <tbb/atomic.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/tick_count.h>

#include <base/base.h>
#include <base/locks.h>
#include <base/semaphore.h>
#include <base/future.h>
#include <base/runnable.h>
#include <base/thread.h>
#include <base/barrier.h>
#include <base/multi_signal_condition.h>
#include <base/timed_average.h>

namespace dedupv1 {
namespace base {

/**
 * Pool of thread for short running tasks.
 *
 * Should be similar to Java's Executor framework as described here:
 * http://download.oracle.com/javase/6/docs/api/java/util/concurrent/Executor.html
 *
 * Submitted tasks are placed in one of several queue depending on their
 * priority. The Threadpool starts this tasks one after another when a new free
 * thread is available. Here tasks with high priority are preferred, every few
 * high priority tasks a lower one is used.
 *
 * The client that submits task is responsible to ensure that every
 * pointer or reference enclosed by the runnable if valid
 * as long as the runnable is scheduled or executed.
 *
 * The threadpool should be used for short running tasks for which an own thread
 * would be overkill.
 *
 * The threadpool supports two priorities (high and background) and uses a kind of time slicing approach
 * to avoid starvation.
 *
 */
class Threadpool {
        DISALLOW_COPY_AND_ASSIGN(Threadpool);
    public:
        /**
         * Enumeration of states for the thread pool
         */
        enum threadpool_state {
            THREADPOOL_STATE_INIT, //!< THREADPOOL_STATE_INIT
            THREADPOOL_STATE_STARTING,
            THREADPOOL_STATE_STARTED, //!< THREADPOOL_STATE_STARTED
            THREADPOOL_STATE_STOPPED
        //!< THREADPOOL_STATE_STOPPED
        };

        enum overflow_strategy {
            /**
             * Default behavior. Add task to queue, but waits for it
             */
            ACCEPT,

            /**
             * Task in run in the calling thread.
             * This method should not be used when the calling thread still holds resources, the task needs. This might lead to a deadlock.
             */
            CALLER_RUNS,

            /**
             * Reject the execution
             */
            REJECT
        };

        /**
         * Priorities supported by the thread pool.
         *
         * If new priority classes are introduced the following things should be done:
         * - Update kPriorityCount
         * - Update GetPriorityName()
         * - Update Runner() so that the new priority task is correctly handled. Do not also not forget to adapt
         *   the max burst handling
         */
        enum priority {
            /**
             * high priority tasks that should be executed as fast as possible
             */
            HIGH_PRIORITY, BACKGROUND_PRIORITY
        };

        /**
         * Number of supported priorities
         */
        const static int kPriorityCount = 2;

        const static uint64_t kSentinalTaskId = -1;

        const static int kDefaultQueueSize = 4;
    private:
        /**
         * Structure combining data about a task
         */
        struct TaskData {
                /**
                 * Constructor
                 */
                inline TaskData(uint64_t task_id, Runnable<bool>* runnable, Future<bool>* future);

                /**
                 * Constructor
                 */
                TaskData();

                uint64_t task_id_;

                /**
                 * runnable of the task
                 */
                Runnable<bool>* runnable_;

                /**
                 * future of the task.
                 * Might be NULL
                 */
                Future<bool>* future_;

                uint64_t task_id() const {
                    return task_id_;
                }

                /**
                 * returns the future of the task.
                 * Might be NULL
                 */
                Future<bool>* future() const {
                    return future_;
                }

                /**
                 * returns the runnable of the task
                 */
                Runnable<bool>* runnable() const {
                    return runnable_;
                }
        };

        /**
         * state of the thread pool
         */
        volatile enum threadpool_state state_;

        /**
         * number of thread in the thread pool
         */
        uint16_t thread_count_[kPriorityCount];

        uint32_t queue_size_[kPriorityCount];

        /**
         * Threads of the thread pool
         */
        std::vector<Thread<bool>*> threads_;

        /**
         * queue of tasks that should be executed
         */
        std::vector<tbb::concurrent_bounded_queue<TaskData>* > task_queue_[kPriorityCount];

        /**
         * number of currently running threads in the threadpool
         * A thread is running if it has the runner method in its stack
         */
        tbb::atomic<int> running_thread_count_;

        /**
         * Number of started threads that are finished
         * Should be zero before the STOPPED state. Otherwise this indicates an error
         */
        tbb::atomic<int> finished_thread_count_;

        tbb::atomic<uint64_t> next_task_id_;

        /**
         * Statistics about the thread pool
         */
        struct Statistics {
                /**
                 * Constructor
                 */
                Statistics();

                /**
                 * number of submitted tasks with a given priority
                 */
                tbb::atomic<uint64_t> submitted_task_count_[kPriorityCount];

                tbb::atomic<uint64_t> waiting_task_count_[kPriorityCount];

                /**
                 * number of executed tasks with a given priority
                 */
                tbb::atomic<uint64_t> executed_task_count_[kPriorityCount];

                tbb::atomic<uint64_t> caller_runs_count_[kPriorityCount];

                /**
                 * number of rejected tasks with a given priority
                 */
                tbb::atomic<uint64_t> reject_count_[kPriorityCount];

                tbb::atomic<uint32_t> busy_thread_count_;
        };

        /**
         * Statistics
         */
        Statistics stats_;

        /**
         * number of tasks that are running at the given
         * moment.
         */
        tbb::atomic<int> running_count_[kPriorityCount];

        /**
         * Executes a task with the given task id
         */
        bool RunTask(const TaskData& task_data, priority prio);

        void CallerRuns(Runnable<bool>* r, priority prio, Future<bool>* future);

        bool DoSubmit(Runnable<bool>* r, priority prio, overflow_strategy overflow_method, Future<bool>* future);

        bool Runner(int thread_id, priority prio, int queue);
    public:
        /**
         * Constructor.
         * @return
         */
        Threadpool(int count = 0);

        /**
         * Destructor.
         */
        ~Threadpool();

        /**
         * configures the thread pool
         *
         * Available options:
         * - size: int
         * - starvation-prevention-factor: int
         *
         * @param option_name
         * @param option
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Starts the threadpool
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start();

        /**
         * Stops the threadpool
         * @return true iff ok, otherwise an error has occurred
         */
        bool Stop();

        /**
         * Submits a new task into the thread pool
         * @param r The taks to run
         * @param prio The priority of the task. Depending on this value it is integrated in the right queue.
         * @param overflow_method Decides what should happen, if there is no free thread left. ACCEPT means enqueue it, REJECT reject it and CALLER_RUNS means run it in the actual thread.
         * @return A Future which can be used to check if the task terminated. The inner bool is set by the result of the task.
         */
        Future<bool>* Submit(Runnable<bool>* r, priority prio = BACKGROUND_PRIORITY, overflow_strategy overflow_method =
                ACCEPT);

        bool SubmitNoFuture(Runnable<bool>* r, priority prio = BACKGROUND_PRIORITY, overflow_strategy overflow_method =
                ACCEPT);

        /**
         * Check if the thread pool has been started.
         * @return true iff the thread pool has been started.
         */
        bool IsStarted();

        /**
         * Number of currently running tasks of the given priority. Running tasks are tasks assigned
         * to a pool thread.
         */
        inline uint64_t running_count(priority prio) const;

        /**
         * Prints profile information about the threadpool
         */
        std::string PrintProfile();

        /**
         * Prints trace information about the threadpool
         */
        std::string PrintTrace();

        /**
         * returns a name for the given priority
         */
        static std::string GetPriorityName(priority prio);
};

Threadpool::TaskData::TaskData(uint64_t task_id, Runnable<bool>* r, Future<bool>* future) :
        task_id_(task_id), runnable_(r), future_(future) {
}

uint64_t Threadpool::running_count(priority prio) const {
    if (prio >= kPriorityCount) {
        return 0;
    }
    return running_count_[prio];
}

}
}

#endif  // THREADPOOL_H__
