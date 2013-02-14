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

#include <base/locks.h>

#include <string>
#include <sstream>

#include <pthread.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <stdio.h>
#include <sys/time.h>

#include <base/bitutil.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <base/thread.h>

LOGGER("Locks");

using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::ToHexString;
using std::string;
using std::stringstream;
using dedupv1::base::ThreadUtil;

#ifndef DEADLOCK_CHECK_TIMEOUT
#define DEADLOCK_CHECK_TIMEOUT 30
#endif

namespace dedupv1 {
namespace base {

/**
 * Adapted from "Advanced programming in the UNIX Environment", page 384.
 *
 * A major problem of this code is that uses the absolute system time. If
 * the system time is changed, and if the tsp is used to sleep a thread, the
 * thread may sleep much longer than necessary.
 */
static void maketimeout(struct timespec* tsp, long secs) {
    struct timeval now;
    gettimeofday(&now, NULL);
    tsp->tv_sec = now.tv_sec;
    tsp->tv_nsec = now.tv_usec * 1000;
    tsp->tv_sec += secs;
}

string DebugStringLockParam(LOCK_LOCATION_PARAM) {
    stringstream sstr;
    sstr << "[function " << function << ", file " << file_basename(file) << ", line " << line << "]";
    return sstr.str();
}

bool MutexLock::AcquireLockWithStatistics_(
    tbb::atomic<uint32_t>* free, tbb::atomic<uint32_t>* busy, const char* function, const char* file, int line) {
    int lockresult = pthread_mutex_trylock(&this->mutex);
    if (lockresult == EBUSY) {
        busy->fetch_and_increment();
#ifdef DEADLOCK_CHECK
        struct timespec timeout;
        maketimeout(&timeout, DEADLOCK_CHECK_TIMEOUT);
        lockresult = pthread_mutex_timedlock(&this->mutex, &timeout);
        if (lockresult == ETIMEDOUT) {
#ifdef NDEBUG
            WARNING("Deadlock possible: " << this->DebugString());
            lockresult = pthread_mutex_lock(&this->mutex);
#else
            ERROR("Deadlock possible: " << this->DebugString());
            return false;
#endif
        }
#else
        lockresult = pthread_mutex_lock(&this->mutex);
#endif
    } else if (lockresult == 0) {
        free->fetch_and_increment();
    }
    if (lockresult) {
        ERROR("Lock failed: " << strerror(lockresult) << ", " << this->DebugString());
        return false;
    }
    this->function = function;
    this->file = file;
    this->line = line;
    this->thread = pthread_self();
    return true;
}

bool MutexLock::ReleaseLock_(const char* function, const char* file, int line) {
    // While counterintuitively we must, we must reset the thread because of race conditions.
    // The thread value may be used for correctness purposes.
    this->thread = (pthread_t) 0;
    this->file = NULL;
    this->function = NULL;
    this->line = 0;

    int lockresult = pthread_mutex_unlock(&this->mutex);
    if (lockresult) {
        ERROR("Unlock failed: " << strerror(lockresult) << ", " << this->DebugString());
        return false;
    }
    return true;
}

bool MutexLock::IsHeld() {
    return this->thread == pthread_self();
}

string MutexLock::DebugString() {
    return "[thread " + ThreadUtil::GetThreadName(this->thread) +
           ", lock holder " + (this->function ? this->function : "") +
           ", file " + (this->file ? file_basename(this->file) : "") +
           ", line " + (this->line ? ToString(this->line) : "") + "]";
}

bool MutexLock::AcquireLock_(const char* function, const char* file, int line) {
    int lockresult = 0;

#ifdef DEADLOCK_CHECK
    struct timespec timeout;
    maketimeout(&timeout, DEADLOCK_CHECK_TIMEOUT);
    lockresult = pthread_mutex_timedlock(&this->mutex, &timeout);
    if (lockresult == ETIMEDOUT) {
        ERROR("Deadlock possible: " << this->DebugString());
        return false;
    }
#else
    lockresult = pthread_mutex_lock(&this->mutex);
#endif
    if (lockresult != 0) {
        ERROR("Lock failed: " << strerror(lockresult));
        return false;
    }
    this->function = function;
    this->file = file;
    this->line = line;
    this->thread = pthread_self();
    return true;
}

bool MutexLock::TryAcquireLock_(bool* locked, const char* function, const char* file, int line) {
    int lockresult = 0;
    lockresult = pthread_mutex_trylock(&this->mutex);
    if (lockresult == EBUSY) {
        *locked = false;
        return true;
    }
    if (lockresult) {
        ERROR("Unlock failed: " << strerror(lockresult));
        return false;
    }
    this->function = function;
    this->file = file;
    this->line = line;
    this->thread = pthread_self();
    *locked = true;
    return true;
}

MutexLock::MutexLock(enum Type type) {
    this->file = NULL;
    this->line = 0;
    this->function = 0;
    this->thread = 0;

    pthread_mutexattr_t attr;
    int err = pthread_mutexattr_init(&attr);
    if (unlikely(err)) {
        ERROR("Failed to init mutex attr: " << strerror(err));
        return;
    }
    int t = 0;
    switch (type) {
    case NORMAL:
        t = PTHREAD_MUTEX_NORMAL;
        break;
    case ERROR_CHECK:
        t = PTHREAD_MUTEX_ERRORCHECK;
        break;
    case OS_DEFAULT:
        t = PTHREAD_MUTEX_DEFAULT;
        break;
    case DEFAULT:
#ifdef NDEBUG
        t = PTHREAD_MUTEX_DEFAULT;
#else
        t = PTHREAD_MUTEX_ERRORCHECK;
#endif
        break;
    default:
        ERROR("Illegal mutex type: " << type);
        break;
    }
    err = pthread_mutexattr_settype(&attr, t);
    if (unlikely(err)) {
        ERROR("Failed to init mutex attr: " << strerror(err));
        return;
    }
    err = pthread_mutex_init(&this->mutex, &attr);
    if (unlikely(err)) {
        ERROR("Lock init failed: " << strerror(err));
    }
}

MutexLock::~MutexLock() {
    int lockresult = pthread_mutex_destroy(&this->mutex);
    if (lockresult) {
        ERROR("Failed to close mutex lock: " << strerror(lockresult) << ", " << this->DebugString());
    }
}

ReadWriteLock::ReadWriteLock() {
    this->function = NULL;
    this->file = NULL;
    this->line = 0;
    this->type = ' ';
    this->thread = 0;
    int lockresult = pthread_rwlock_init(&this->rw_lock, NULL);
    if (lockresult) {
        ERROR("Lock init failed: " << strerror(lockresult) << ", " << this->DebugString());
    }
}

ReadWriteLock::~ReadWriteLock() {
    int lockresult = pthread_rwlock_destroy(&this->rw_lock);
    if (lockresult) {
        ERROR("Lock close failed: " << strerror(lockresult));
    }
}

bool ReadWriteLock::ReleaseLock_(const char* function, const char* file, int line) {
    // While counterintuitively we must, we must reset the thread because of race conditions.
    // The thread value may be used for correctness purposes.
    this->thread = (pthread_t) 0;
    this->function = NULL;
    this->file = NULL;
    this->line = 0;
    this->type = ' ';
    int lockresult = pthread_rwlock_unlock(&this->rw_lock);
    if (lockresult) {
        ERROR("Unlock failed: " << strerror(lockresult) << ", " << this->DebugString());
        return false;
    }
    return true;
}

bool ReadWriteLock::AcquireReadLock_(const char* function, const char* file, int line) {
    int lockresult = 0;

#ifdef DEADLOCK_CHECK
    struct timespec timeout;
    maketimeout(&timeout, DEADLOCK_CHECK_TIMEOUT);
    lockresult = pthread_rwlock_timedrdlock(&this->rw_lock, &timeout);
    if (lockresult == ETIMEDOUT) {
#ifdef NDEBUG
        WARNING("Deadlock possible: " << this->DebugString());
        lockresult = pthread_rwlock_rdlock(&this->rw_lock);
#else
        ERROR("Deadlock possible: " << this->DebugString());
        return false;
#endif
    }
#else
    lockresult = pthread_rwlock_rdlock(&this->rw_lock);
#endif
    if (lockresult) {
        ERROR("Unlock failed: " << strerror(lockresult) << ", " << this->DebugString());
        return false;
    }
    this->function = function;
    this->file = file;
    this->line = line;
    this->type = 'r';
    this->thread = (pthread_t) 0;
    return true;
}

bool ReadWriteLock::TryAcquireReadLock_(bool* locked, const char* function, const char* file, int line) {
    int lockresult = 0;

    lockresult = pthread_rwlock_tryrdlock(&this->rw_lock);
    if (lockresult == EBUSY) {
        *locked = false;
        return true;
    }
    if (lockresult) {
        ERROR("Unlock failed: " << strerror(lockresult) << ", " << this->DebugString());
        return false;
    }
    this->function = function;
    this->file = file;
    this->line = line;
    this->type = 'r';
    this->thread = (pthread_t) 0;
    *locked = true;
    return true;
}

bool ReadWriteLock::AcquireWriteLock_(const char* function, const char* file, int line) {
    int lockresult = 0;

#ifdef DEADLOCK_CHECK
    struct timespec timeout;
    maketimeout(&timeout, DEADLOCK_CHECK_TIMEOUT);
    lockresult = pthread_rwlock_timedwrlock(&this->rw_lock, &timeout);
    if (lockresult == ETIMEDOUT) {
#ifdef NDEBUG
        WARNING("Deadlock possible: " << this->DebugString());
        lockresult = pthread_rwlock_wrlock(&this->rw_lock);
#else
        ERROR("Deadlock possible: " << this->DebugString());
        return false;
#endif
    }
#else
    lockresult = pthread_rwlock_wrlock(&this->rw_lock);
#endif
    if (lockresult) {
        ERROR("Lock failed: " << strerror(lockresult) << ", " << this->DebugString());
        return false;
    }
    this->function = function;
    this->file = file;
    this->line = line;
    this->type = 'w';
    this->thread = pthread_self();
    return true;
}

bool ReadWriteLock::TryAcquireWriteLock_(bool* locked, const char* function, const char* file, int line) {
    int lockresult = 0;

    lockresult = pthread_rwlock_trywrlock(&this->rw_lock);
    if (lockresult == EBUSY) {
        *locked = false;
        return true;
    }
    if (lockresult) {
        ERROR("Lock failed: " << strerror(lockresult) << ", " << this->DebugString());
        *locked = false;
        return false;
    }
    this->function = function;
    this->file = file;
    this->line = line;
    this->type = 'w';
    this->thread = pthread_self();
    *locked = true;
    return true;
}

string ReadWriteLock::DebugString() {
    return "[type " + ToString(this->type) +
           ", thread " + ThreadUtil::GetThreadName(this->thread) +
           ", lock holder " + (this->function ? this->function : "") +
           ", file " + (this->file ? file_basename(this->file) : "") +
           ", line " + ToString(this->line) + "]";
}

bool ReadWriteLock::AcquireReadLockWithStatistics_(
    tbb::atomic<uint32_t>* free,
    tbb::atomic<uint32_t>* busy,
    const char* function, const char* file, int line) {
    int lockresult = pthread_rwlock_tryrdlock(&this->rw_lock);

    if (lockresult == EBUSY) {
        busy->fetch_and_increment();
#ifdef DEADLOCK_CHECK
        struct timespec timeout;
        maketimeout(&timeout, DEADLOCK_CHECK_TIMEOUT);
        lockresult = pthread_rwlock_timedrdlock(&this->rw_lock, &timeout);
        if (lockresult == ETIMEDOUT) {
#ifdef NDEBUG
            WARNING("Deadlock possible: " << this->DebugString());
#else
            ERROR("Deadlock possible: " << this->DebugString());
            return false;
#endif
        }
#else
        lockresult = pthread_rwlock_rdlock(&this->rw_lock);

#endif
    } else if (lockresult == 0) {
        free->fetch_and_increment();
    }
    if (lockresult) {
        ERROR("Lock failed: " << strerror(lockresult) << ", " << this->DebugString());
        return false;
    }
    this->function = function;
    this->file = file;
    this->line = line;
    this->type = 'r';
    this->thread = (pthread_t) 0;
    return true;
}

bool ReadWriteLock::AcquireWriteLockWithStatistics_(
    tbb::atomic<uint32_t>* free,
    tbb::atomic<uint32_t>* busy,
    const char* function, const char* file, int line) {
    CHECK(free, "Free lock statistics not set");
    CHECK(busy, "Busy lock statistics not set");

    int lockresult = pthread_rwlock_trywrlock(&this->rw_lock);
    if (lockresult == EBUSY) {
        busy->fetch_and_increment();
#ifdef DEADLOCK_CHECK
        struct timespec timeout;
        maketimeout(&timeout, DEADLOCK_CHECK_TIMEOUT);
        lockresult = pthread_rwlock_timedwrlock(&this->rw_lock, &timeout);
        if (lockresult == ETIMEDOUT) {
            ERROR("Deadlock possible: " << this->DebugString());
            return false;
        }
#else
        lockresult = pthread_rwlock_wrlock(&this->rw_lock);
#endif
    } else if (lockresult == 0) {
        free->fetch_and_increment();
    }
    if (lockresult) {
        ERROR("Lock failed: " << strerror(lockresult) << ", " << this->DebugString());
        return false;
    }
    this->function = function;
    this->file = file;
    this->line = line;
    this->type = 'w';
    this->thread = pthread_self();
    return true;
}

bool ReadWriteLock::IsHeldForWrites() {
    return this->thread == pthread_self();
}

Condition::Condition() {
    valid_ = false;
    pthread_condattr_t attr;
    int err = pthread_condattr_init(&attr);
    if (err) {
        ERROR(strerror(err));
        return;
    }
#ifndef NO_PTHREAD_CONDATTR_SETCLOCK
    err = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    if (err) {
        ERROR(strerror(err));
        return;
    }
#endif
    err = pthread_cond_init(&this->condition, &attr);
    if (err) {
        ERROR(strerror(err));
        return;
    }
    valid_ = true;
    pthread_condattr_destroy(&attr);

}

bool Condition::ConditionWait_(MutexLock* lock,  const char* function, const char* file, int line) {
    CHECK(valid_, "Condition not valid");
    int err = pthread_cond_wait(&condition, &lock->mutex);
    if (err) {
        ERROR(strerror(err));
    }
    lock->function = function;
    lock->file = file;
    lock->line = line;
    lock->thread = pthread_self();
    return true;
}

enum timed_bool Condition::ConditionWaitTimeout_(MutexLock* lock, uint16_t secs, const char* function, const char* file, int line) {
    CHECK_RETURN(valid_, TIMED_FALSE, "Condition not valid");
    struct timespec timeout;
#ifndef NO_PTHREAD_CONDATTR_SETCLOCK
    clock_gettime(CLOCK_MONOTONIC, &timeout);
    timeout.tv_sec += secs;
#else
    maketimeout(&timeout, secs);
#endif
    int err = pthread_cond_timedwait(&condition, &lock->mutex, &timeout);
    if (err == ETIMEDOUT) {
        return TIMED_TIMEOUT;
    }
    if (err) {
        ERROR("Failed to wait for condition: error " << strerror(err));
        return TIMED_FALSE;
    }
    lock->function = function;
    lock->file = file;
    lock->line = line;
    lock->thread = pthread_self();
    return TIMED_TRUE;
}

bool Condition::Signal() {
    CHECK(valid_, "Condition not valid");
    int err = pthread_cond_signal(&condition);
    if (err) {
        ERROR("Failed to signal condition: error " << strerror(err));
        return false;
    }
    return true;
}

bool Condition::Broadcast() {
    CHECK(valid_, "Condition not valid");
    int err = pthread_cond_broadcast(&condition);
    if (err) {
        ERROR(strerror(err));
        return false;
    }
    return true;
}

Condition::~Condition() {
    if (valid_) {
        int err = pthread_cond_destroy(&condition);
        if (err) {
            ERROR(strerror(err));
        }
    }
}

}

}
