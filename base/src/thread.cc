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

#include <base/base.h>
#include <base/thread.h>
#include <base/logging.h>

#ifdef HAS_PRCTL
#include <sys/prctl.h>
#endif

LOGGER("Thread");

namespace dedupv1 {
namespace base {
namespace internal {

ThreadImpl::ThreadImpl(void* context, void*(*runner)(void *)) { // NOLINT
    this->context_ = context;
    this->runner_ = runner;
    this->t_ = 0;
}

bool ThreadImpl::Start(int prio) {
    tbb::spin_mutex::scoped_lock l(lock_);
    CHECK(!IsStartedLocked(), "Thread already started");

    pthread_attr_t attr;
    int err = pthread_attr_init(&attr);
    if (err != 0) {
        ERROR("Failed to init attributes: " << strerror(err));
        return false;
    }
    if (prio > 0) {
        TRACE("Use priority " << prio);
        // see http://stackoverflow.com/questions/1704625/how-to-increase-the-priority-of-a-child-pthread-relative-to-the-parent-thread
        struct sched_param param;
        err = pthread_attr_setschedpolicy(&attr, SCHED_FIFO);
        if (err != 0) {
            ERROR("Failed to scheduling policy: " << strerror(err));
            return false;
        }
        err = pthread_attr_getschedparam(&attr, &param);
        if (err != 0) {
            ERROR("Failed to get scheduling params: " << strerror(err));
            return false;
        }
        param.sched_priority = sched_get_priority_max(SCHED_FIFO);
        if (prio < param.sched_priority) {
            param.sched_priority = prio;
        }
        err = pthread_attr_setschedparam(&attr, &param);
        if (err != 0) {
            ERROR("Failed to set scheduling params: " << strerror(err));
            return false;
        }
    }

    err = pthread_create(&this->t_, &attr, this->runner_, this->context_);
    if (err != 0) {
        ERROR("Failed to create thread: " << strerror(err));
        return false;
    }
    return true;
}

bool ThreadImpl::IsStartedLocked() {
    return this->t_ != 0;
}

bool ThreadImpl::IsStarted() {
    tbb::spin_mutex::scoped_lock l(lock_);
    return IsStartedLocked();
}

bool ThreadImpl::Cancel() {
    tbb::spin_mutex::scoped_lock l(lock_);
    CHECK(IsStartedLocked(), "Thread not started");

    int err = pthread_cancel(this->t_);
    if (err != 0) {
        ERROR("Failed to cancel the thread: " << strerror(err));
        return false;
    }
    return true;
}

bool ThreadImpl::Detach() {
    tbb::spin_mutex::scoped_lock l(lock_);
    CHECK(IsStartedLocked(), "Thread not started");

    int err = pthread_detach(this->t_);
    if (err != 0) {
        ERROR("Failed to detach the thread: " << strerror(err));
        return false;
    }
    return true;
}

bool ThreadImpl::Join(void** rt) {
    tbb::spin_mutex::scoped_lock l(lock_);
    CHECK(IsStartedLocked(), "Thread not started");

    int err = pthread_join(this->t_, rt);
    t_ = 0;
    if (err != 0) {
        ERROR("Failed to join thread: " << strerror(err));
        return false;
    }
    return true;
}

LOGGER_CLASS kThreadLogger = GET_LOGGER("Thread");

}

void ThreadUtil::RegisterCurrentThread(const std::string& thread_name) {
    thread_names_.insert(make_pair(pthread_self(), thread_name));
#ifdef HAS_PTHREAD_SETNAME_NP
    char buf[16];
    strncpy(buf, thread_name.c_str(), 16);
    buf[15] = 0;

#ifdef _DARWIN_C_SOURCE
    if (pthread_setname_np(buf) != 0) {
#else
    if (pthread_setname_np(pthread_self(), buf) != 0) {
#endif
        ERROR("Failed to set thread name: " << strerror(errno));
    }
#else
#ifdef HAS_PRCTL
    char buf[16];
    strncpy(buf, thread_name.c_str(), 16);
    buf[15] = 0;
    if (prctl(PR_SET_NAME, (unsigned long) &buf) != 0) {
        ERROR("Failed to set thread name: " << strerror(errno));
    }
#endif
#endif
}

void ThreadUtil::UnregisterCurrentThread() {
    thread_names_.erase(pthread_self());
}

tbb::concurrent_hash_map<pthread_t, std::string> ThreadUtil::thread_names_;

}
}
