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

#ifndef THREAD_H__
#define THREAD_H__

#include <pthread.h>
#ifdef __APPLE__
#include <sched.h>
#endif

#include <base/base.h>
#include <base/logging.h>
#include <base/locks.h>
#include <base/semaphore.h>
#include <base/runnable.h>
#include <base/strutil.h>

#include <string>
#include <tbb/spin_mutex.h>
#include <tbb/concurrent_hash_map.h>

namespace dedupv1 {
namespace base {


/**
 * \namespace dedupv1::internal
 * Namespace for classes that should be seen as implementation detail
 * for the related (public) class.
 *
 */
namespace internal {

extern LOGGER_CLASS kThreadLogger;

/**
 * Private thread implementation class.
 * Client should use Thread<T> directly
 */
class ThreadImpl {
        DISALLOW_COPY_AND_ASSIGN(ThreadImpl);

        tbb::spin_mutex lock_;

        /**
         * pthread thread
         */
        pthread_t t_;

        /**
         * Context pointer
         */
        void* context_;

        /**
         * Pointer to a pthread function.
         *
         * @param p
         */
        void*(*runner_)(void* p);

        bool IsStartedLocked();
    public:
        /**
         * Constructor
         * @param context
         * @param runner
         * @return
         */
        ThreadImpl(void* context, void*(*runner)(void*));

        /**
         * returns true iff the thread is started
         */
        bool IsStarted();

        /**
         *  starts the thread with the given priority
         */
        bool Start(int prio);

        /**
         * joins the thread and pass the thread return value to the
         * pointer if the pointer is given.
         */
        bool Join(void** return_value);

        /**
         * Detach the thread.
         */
        bool Detach();

        /**
         * Cancels a thread
         */
        bool Cancel();
};

/**
 * Private class that holds the result of a thread
 * execution.
 *
 * Should not be used directly by normal clients.
 */
template<class RT> class ThreadResult {
        /**
         * return value
         */
        RT value_;
    public:

        /**
         * Constructor
         * @param rt
         * @return
         */
        explicit ThreadResult(const RT& rt);

        /**
         * gets the return value of the thread.
         * @return
         */
        inline const RT& result();
};

template<class RT> ThreadResult<RT>::ThreadResult(const RT& rt) {
        this->value_ = rt;
}

/**
 * gets the return value of the thread.
 * @return
 */
template<class RT> const RT& ThreadResult<RT>::result() {
        return value_;
}

} // internal

/**
 * Wrapper around the pthread library.
 *
 * Each started thread MUST be joined as it is the case with non-detached
 * pthread threads.
 *
 * It should be used instead of pthreads for the following reason:
 * - Type safety
 * - Matching to the dedupv1 style guide
 * - Support to Runnable
 */
template<class RT> class Thread {
        /**
         * State of the thrad
         */
        enum ThreadState {
            CREATED,
            STARTED,
            FINISHED,
            JOINING,
            JOINED,

            /**
             * Starting the thread failed
             */
            FAILED
        };
        DISALLOW_COPY_AND_ASSIGN(Thread);
        /**
         * Pointer to the internal thread implementation
         */
        dedupv1::base::internal::ThreadImpl* impl_;

        /**
         * Pointer to a runnable
         */
        Runnable<RT>* runnable_;

        /**
         * Name of the string
         */
        std::string name_;

        /**
         * lock to protect the thread state
         */
        tbb::spin_mutex lock_;

        /**
         * state of the thread
         */
        tbb::atomic<enum ThreadState> state_;

        /**
         * priority of the thread
         */
        int prio_;

        static dedupv1::base::internal::ThreadResult<RT>* ThreadRunner(void* context);


    public:
        /**
         * Returns the name of the thread. If no name is set,
         * an empty string is returned.
         * @return
         */
        inline const std::string& name();

        /**
         * Sets the priority.
         *
         * Must be called before the thread is started.
         *
         * @param prio
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetPriority(int prio);

        /**
         * Constructs a new thread that executes the given runnable
         * after the start.
         *
         * The construction of a thread doesn't start it.
         *
         * @param runnable
         * @param name
         * @return
         */
        Thread(Runnable<RT>* runnable, const std::string& name);

        /**
         * Destructor.
         *
         * @return
         */
        ~Thread();

        /**
         * Starts the thread.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start();

        /**
         * Returns if the thread is started.
         *
         * @return true iff the thread is started, but not finished
         */
        bool IsStarted();

        /**
         * Returns if the thread is finished.
         *
         * @return true iff the thread is finished
         */
        bool IsFinished();

        /**
         * Returns true iff the thread failed, e.g. at the start.
         */
        bool IsFailed();

        /**
         * Returns true if the thread is joinable.
         * A thread is joinable if the thread has been started and has
         * not been joined before.
         */
        bool IsJoinable();

        /**
         * Joins the thread and sets the thread return value, if
         * the pointer is set.
         *
         * The call is blocking
         *
         * @param return_value
         * @return true iff ok, otherwise an error has occurred
         */
        bool Join(RT* return_value);

        /**
         * Cancels a thread
         *
         * This is the last-resort. Always.
         * The thread is canceled the moment it reached a
         * cancelation point and exits there.
         * No memory is freed, no locks are released.
         * Don't do this until it is the last resort, e.g. to stop the system
         */
        bool Cancel();

        /**
         * Detach the thread.
         */
        bool Detach();

        /**
         * Convenience method used to execute a runnable,
         * wait for its completion and return the result.
         *
         * The call is blocking.
         * @param r
         * @return
         */
        template<class RT2> static RT2 RunThread(Runnable<RT2>* r)  {
                Thread<RT2> t(r, "tmp");
                t.Start();
                RT2 result;
                t.Join(&result);
                return result;
        }
};

/**
 * Thread utility functions
 */
class ThreadUtil {
    public:
        enum TimeUnit {
            SECONDS,
            MILLISECONDS
        };

        /**
         * Yields the current thread.
         *
         * This means that the operating system is notified that
         * the current thread should be suspended and
         * append at the end of the running queue.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        static inline bool Yield();

        /**
         * Sleeps the current thread for a given number of seconds
         */
        static inline void Sleep(int time_units, TimeUnit time_unit = SECONDS);

        /**
         * returns the name of the thread with the given pthread id.
         * Returns the pid as hexadecimal number prefixed with 0x if no thread
         * name is registered. This usually means that the thread is not a controlled by
         * an instance of the Thread class.
         */
        static inline std::string GetThreadName(pthread_t pid);

        /**
         * returns the thread name of the current thread.
         * Returns the pid as hexadecimal number prefixed with 0x if no thread
         * name is registered. This usually means that the thread is not a controlled by
         * an instance of the Thread class.
         */
        static inline std::string GetCurrentThreadName();

        /**
         * Registers a name for the current thread
         */
        static void RegisterCurrentThread(const std::string& thread_name);

        /**
         * Unregisters the name of the current thread.
         */
        static void UnregisterCurrentThread();
    private:

        /**
         * Map from a pthread id to a registers thread name
         */
        static tbb::concurrent_hash_map<pthread_t, std::string> thread_names_;

};

std::string ThreadUtil::GetCurrentThreadName() {
    return GetThreadName(pthread_self());
}

std::string ThreadUtil::GetThreadName(pthread_t pid) {
    tbb::concurrent_hash_map<pthread_t, std::string>::const_accessor a;
    if (thread_names_.find(a, pid)) {
        return a->second;
    }
    return "0x" + dedupv1::base::strutil::ToHexString(&pid, sizeof(pid));
}

void ThreadUtil::Sleep(int time_units, TimeUnit time_unit) {
    if (time_unit == SECONDS) {
        while(time_units> 0) {
            time_units = sleep(time_units);
        }
    } else {
        usleep(time_units * 1000);
    }
}

bool ThreadUtil::Yield() {
#ifdef __APPLE__
    return sched_yield() == 0;
#else
    return pthread_yield() == 0;
#endif
}

template<class RT> dedupv1::base::internal::ThreadResult<RT>* Thread<RT>::ThreadRunner(void* context) {
        Thread<RT>* t = reinterpret_cast<Thread<RT>*>(context);
        std::string local_name = t->name();

        ThreadUtil::RegisterCurrentThread(local_name);

        Runnable<RT>* r = NULL;
        {
            tbb::spin_mutex::scoped_lock l(t->lock_);
            r = t->runnable_;
            t->runnable_ = NULL;
        }
        NESTED_LOG_CONTEXT(local_name);
        RT rt = r->Run();
        {
            tbb::spin_mutex::scoped_lock l(t->lock_);

            if (t->state_ == STARTED) {
                t->state_ = FINISHED;
            }
        }
        ThreadUtil::UnregisterCurrentThread();


        return new dedupv1::base::internal::ThreadResult<RT>(rt);
}

template<class RT> const std::string& Thread<RT>::name() {
        return name_;
}

template<class RT> bool Thread<RT>::SetPriority(int prio) {
        tbb::spin_mutex::scoped_lock l(lock_);
        if (this->state_ != CREATED) {
            ERROR_LOGGER(internal::kThreadLogger, "Illegal state: " << state_);
            return false;
        }
        prio_ = prio;
        return true;
}

template<class RT> Thread<RT>::Thread(Runnable<RT>* runnable, const std::string& name) {
        this->name_ = name;
        this->runnable_ = runnable;
        void* (*f)(void*) = (void* (*)(void*)) &Thread::ThreadRunner;
        this->impl_ = new dedupv1::base::internal::ThreadImpl(this, f);
        this->state_ = CREATED;
        this->prio_ = 0;
}

template<class RT> Thread<RT>::~Thread() {
        if (IsJoinable()) {
            Join(NULL);
        }
        {
            tbb::spin_mutex::scoped_lock l(lock_);
            if (this->runnable_) {
                Runnable<RT>* r = this->runnable_;
                //thread not yet started
                this->runnable_ = NULL;
                if (r) {
                    delete r;
                }
            }
        }
        if (this->impl_) {
            delete this->impl_;
            this->impl_ = NULL;
        }
}

template<class RT> bool Thread<RT>::Start() {
        tbb::spin_mutex::scoped_lock l(lock_);
        enum ThreadState old_state = state_;
        if (this->state_ == CREATED) {
            this->state_ = STARTED;
        }

        // let the real error be handled by the impl.
        // we can't do logging here.
        bool ret = this->impl_->Start(this->prio_);
        if (!ret) {
            // only switch state of failed when the old state was created
            // once a thread is started, it should not be set to failed.
            if (old_state == CREATED) {
                this->state_ = FAILED;
            }
        }
        return ret;
}

template<class RT> bool Thread<RT>::Cancel() {
    return this->impl_->Cancel();
}

template<class RT> bool Thread<RT>::Detach() {
    return this->impl_->Detach();
}

template<class RT> bool Thread<RT>::IsStarted() {
        tbb::spin_mutex::scoped_lock l(lock_);
        return (this->state_ == STARTED);
}

template<class RT> bool Thread<RT>::IsFinished() {
        tbb::spin_mutex::scoped_lock l(lock_);
        return (this->state_ == FINISHED);
}

template<class RT> bool Thread<RT>::IsFailed() {
        tbb::spin_mutex::scoped_lock l(lock_);
        return (this->state_ == FAILED);
}

template<class RT> bool Thread<RT>::IsJoinable() {
        tbb::spin_mutex::scoped_lock l(lock_);
        return (this->state_ == STARTED || state_ == FINISHED);
}

template<class RT> bool Thread<RT>::Join(RT* return_value) {
        tbb::spin_mutex::scoped_lock l(lock_);
        if (state_ == JOINED || state_ == JOINING || state_ == FAILED) {
            ERROR_LOGGER(internal::kThreadLogger, "Illegal state: " << state_);
            return false; // already joined
        }
        state_ = JOINING;
        l.release();

        dedupv1::base::internal::ThreadResult<RT>* tr = NULL;
        dedupv1::base::internal::ThreadResult<RT>** ptr = NULL;

        ptr = &tr;
        bool result = this->impl_->Join((void**)ptr);
        if (result && return_value && tr) {
            *return_value =  tr->result();
        }
        if (tr) {
            delete tr;
        }
        tbb::spin_mutex::scoped_lock l2(lock_);
        if (state_ != JOINING) {
            ERROR_LOGGER(internal::kThreadLogger, "Illegal state: " << state_);
            return false;
        }
        state_ = JOINED;
        return result;
}

}
}

#endif  // THREAD_H__


