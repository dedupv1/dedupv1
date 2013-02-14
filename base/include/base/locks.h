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

#ifndef DEDUPV1_LOCKS_H__ // NOLINT
#define DEDUPV1_LOCKS_H__ // NOLINT

#include <string>
#include <vector>

#include <tbb/atomic.h>

#include <base/base.h>
#include <pthread.h>

namespace dedupv1 {
namespace base {

/**
 * Enumeration for timed operations, has an additional state to true/false for TIMEOUT
 */
enum timed_bool {
    /**
     * false
     */
    TIMED_FALSE = 0,

    /**
     * true
     */
    TIMED_TRUE = 1,

    /**
     * operations timed out
     */
    TIMED_TIMEOUT = 2
};

/**
 * While the correctness may relay on the thread value, the correctness
 * should not relay on the function, file, and line values. These values
 * are for debugging and support purposes only.
 *
 * The lock should not be copied or assigned, but we cannot forbid copy and
 * assignment as we want to use the lock inside STL containers.
 */
class MutexLock {
    public:
        /**
         * Type of the mutex.
         * In debug mode, the default is ERROR_CHECK. The
         * default in the release mode is inherited by the default of the OS.
         */
        enum Type {
            DEFAULT,      //!< DEFAULT
            OS_DEFAULT,
            ERROR_CHECK,
            NORMAL,       //!< NORMAL
        };

        /**
         * Constructor.
         * The lock is ready to be used after calling the constructor
         * @return
         */
        MutexLock(enum Type type = DEFAULT);

        /**
         * Destructor
         * @return
         */
        ~MutexLock();

        /**
         * Acquires the lock and maintains statistics information about the lock state.
         *
         * Use AcquireLockWithStatistics without the underscore tail.
         *
         * Note: We need here atomic operations.
         *
         * @param free
         * @param busy
         * @param function
         * @param file
         * @param line
         * @return
         */
        bool AcquireLockWithStatistics_(tbb::atomic<uint32_t>* free, tbb::atomic<uint32_t>* busy,
                const char* function, const char* file, int line);

        /**
         * Use AcquireLock without the underscore tail.
         *
         * @param function
         * @param file
         * @param line
         * @return
         */
        bool AcquireLock_(const char* function, const char* file, int line);

        /**
         * Use TryAcquireLock without the underscore tail.
         *
         * @param locked
         * @param function
         * @param file
         * @param line
         * @return
         */
        bool TryAcquireLock_(bool* locked, const char* function, const char* file, int line);

        /**
         * Use ReleaseLock without the underscore tail.
         *
         * @param function
         * @param file
         * @param line
         * @return
         */
        bool ReleaseLock_(const char* function, const char* file, int line);

        /**
         * returns true iff the lock is held by the current thread
         * @return
         */
        bool IsHeld();

        /**
         * print a developer-readable representation of the lock and its
         * state.
         *
         * @return
         */
        std::string DebugString();
    private:

        /**
         * Underlying pthread mutex
         */
        pthread_mutex_t mutex;

        /**
         * name of the function that currently holds the lock.
         * Set to NULL if the lock is not acquired.
         */
        const char* function;

        /**
         * name of the file of the function that currently holds the lock.
         * Set to NULL if lock is not acquired.
         */
        const char* file;

        /**
         * line in the file of the function that currently holds the lock.
         * Set to 0 if the lock is not acquired
         */
        int line;

        /**
         * thread (id) that currently holds the lock
         */
        pthread_t thread;

        friend class Condition;
        DISALLOW_COPY_AND_ASSIGN(MutexLock);
};

#define AcquireLockWithStatistics(free,busy) AcquireLockWithStatistics_(free, busy, __func__, __FILE__, __LINE__)
#define ReleaseLock() ReleaseLock_(__func__,__FILE__, __LINE__)
#define AcquireLock() AcquireLock_(__func__,__FILE__, __LINE__)
#define TryAcquireLock(locked) TryAcquireLock_(locked, __func__,__FILE__, __LINE__)

class MutexLockVector {
    public:

        MutexLockVector() {
        }

        bool Init(size_t s) {
            if (!locks_.empty()) {
                return false;
            }
            locks_.resize(s);
            for(size_t i = 0; i < s; i++) {
                locks_[i] = new MutexLock();
                if (!locks_[i]) {
                    return false;
                }
            }
            return true;
        }

        ~MutexLockVector() {
            for(size_t i = 0; i < locks_.size(); i++) {
                if (locks_[i]) {
                    delete locks_[i];
                    locks_[i] = NULL;
                }
            }
            locks_.clear();
        }

        MutexLock* Get(size_t i) {
            if (unlikely(i >= locks_.size())) {
                return NULL;
            }
            return locks_[i];
        }

        bool empty() {
            return locks_.empty();
        }

        size_t size() {
            return locks_.size();
        }
    private:

        std::vector<MutexLock*> locks_;

        DISALLOW_COPY_AND_ASSIGN(MutexLockVector);
};

/**
 * While the correctness may relay on the thread value, the correctness
 * should not relay on the function, file, and line values. These values
 * are for debugging and support purposes only.
 *
 * The lock should not be copied or assigned, but we cannot forbid copy and
 * assignment as we want to use the lock inside STL containers.
 */
class ReadWriteLock {
        /**
         * Underlying pthread rw lock
         */
        pthread_rwlock_t rw_lock;

        /**
         * name of the function that currently holds a write lock.
         * If the lock is not acquired or acquired in read lock mode,
         * the function is set to NULL:
         */
        const char* function;

        /**
         * file of the function that currently holds a write lock.
         * If the lock is not acquired or acquired in read lock mode,
         * the file is set to NULL:
         */
        const char* file;

        /**
         * line in the file of the function that currently holds a write lock.
         * If the lock is not acquired or acquired in read lock mode,
         * the line is set 0:
         */
        int line;

        /**
         * type in which the lock is currently held.
         * 'w' if the lock is held in write lock mode. 'r' if the lock
         * is held in read lock mode. ' ' if the lock is not held.
         */
        char type;

        /**
         * thread (id) that currently holds the (write) lock.
         */
        pthread_t thread;
    public:
        /**
         * Constructor
         * @return
         */
        ReadWriteLock();

        /**
         * Destructor
         * @return
         */
        ~ReadWriteLock();


        bool ReleaseLock_(const char* function, const char* file, int line);

        bool AcquireReadLock_(const char* function, const char* file, int line);
        bool TryAcquireReadLock_(bool* locked, const char* function, const char* file, int line);

        bool AcquireWriteLock_(const char* function, const char* file, int line);
        bool TryAcquireWriteLock_(bool* locked, const char* function, const char* file, int line);

        bool AcquireReadLockWithStatistics_(tbb::atomic<uint32_t>* free, tbb::atomic<uint32_t>* busy,
                const char* function, const char* file, int line);
        bool AcquireWriteLockWithStatistics_(tbb::atomic<uint32_t>* free, tbb::atomic<uint32_t>* busy,
                const char* function, const char* file, int line);

        std::string DebugString();

        /**
         * returns true iff the lock is held as write lock by the current thread.
         * @return
         */
        bool IsHeldForWrites();

        DISALLOW_COPY_AND_ASSIGN(ReadWriteLock);
};

#define LOCK_LOCATION_INFO  __func__, __FILE__, __LINE__
#define LOCK_LOCATION_PARAM const char* function, const char* file, int line
#define LOCK_LOCATION_DELEGATE function, file, line
#define AcquireReadLockWithStatistics(free,busy) AcquireReadLockWithStatistics_(free, busy, LOCK_LOCATION_INFO)
#define AcquireWriteLockWithStatistics(free,busy) AcquireWriteLockWithStatistics_(free, busy, LOCK_LOCATION_INFO)

#define AcquireReadLock() AcquireReadLock_(LOCK_LOCATION_INFO)
#define AcquireWriteLock() AcquireWriteLock_(LOCK_LOCATION_INFO)
#define TryAcquireReadLock(locked) TryAcquireReadLock_(locked, LOCK_LOCATION_INFO)
#define TryAcquireWriteLock(locked) TryAcquireWriteLock_(locked, LOCK_LOCATION_INFO)

std::string DebugStringLockParam(LOCK_LOCATION_PARAM);


class ReadWriteLockVector {
    public:
        bool Init(size_t s) {
            if (!locks_.empty()) {
                return false;
            }
            locks_.resize(s);
            for(size_t i = 0; i < s; i++) {
                locks_[i] = new ReadWriteLock();
                if (!locks_[i]) {
                    return false;
                }
            }
            return true;
        }

        ReadWriteLockVector() {
        }

        bool empty() {
            return locks_.empty();
        }

        size_t size() {
            return locks_.size();
        }

        ~ReadWriteLockVector() {
            for(size_t i = 0; i < locks_.size(); i++) {
                if (locks_[i]) {
                    delete locks_[i];
                    locks_[i] = NULL;
                }
            }
            locks_.clear();
        }

        ReadWriteLock* Get(size_t i) {
            if (unlikely(i >= locks_.size())) {
                return NULL;
            }
            return locks_[i];
        }
    private:

        std::vector<ReadWriteLock*> locks_;

        DISALLOW_COPY_AND_ASSIGN(ReadWriteLockVector);
};

/**
 * Condition variable.
 * A condition variable is used for sychonization between threads.
 * A number of threads wait until a condition if fulfilled and then wake up
 */
class Condition {
        /**
         * pthread condition
         */
        pthread_cond_t condition;

        bool valid_;
    public:
        /**
         * Constructor
         * @return
         */
        Condition();

        /**
         * Destructor
         * @return
         */
        ~Condition();

        /**
         * Wakes up a single waiting thread
         * @return
         */
        bool Signal();

        /**
         * Wakes up all waiting threads
         * @return
         */
        bool Broadcast();

        /**
         * Use ConditionWaitTimeout without the underscore tail.
         *
         * @param lock
         * @param secs
         * @param function
         * @param file
         * @param line
         * @return
         */
        enum timed_bool ConditionWaitTimeout_(MutexLock* lock, uint16_t secs,
                const char* function, const char* file, int line);

        /**
         * Use ConditionWait without the underscore tail.
         * @param lock
         * @param function
         * @param file
         * @param line
         * @return
         */
        bool ConditionWait_(MutexLock* lock, const char* function,
                const char* file, int line);

        DISALLOW_COPY_AND_ASSIGN(Condition);
};

#define ConditionWait(lock) ConditionWait_(lock, __func__,__FILE__, __LINE__)
#define ConditionWaitTimeout(lock, secs) ConditionWaitTimeout_(lock, secs, __func__,__FILE__, __LINE__)

/**
 * A scoped lock is a helper a mutex that ensures
 * that the lock is released once the scoped lock variable
 * leaves the scope.
 *
 * A scoped lock should only be used by a single thread, while there
 * can be multiple scoped lock for the same lock in different threads.
 *
 * The scoped lock is used to ensure error and exception safe code.
 *
 * This lock is similar to the concept of RIIT.
 * The scoped lock pattern is described in "POSA II".
 *
 * @sa http://en.wikipedia.org/wiki/Resource_Acquisition_Is_Initialization
 */
class ScopedLock {
        /**
         * Underlying lock that is managed by the scoped lock
         */
        MutexLock* lock;

        /**
         * true iff the lock is held using this lock. This
         * means also that this scoped lock is responsible for releasing
         * the lock.
         */
        volatile bool holds_it;
    public:
        /**
         * Constructor
         * @param lock lock to manage. The lock can be NULL. In that
         * case all acquire and release calls fail.
         *
         * @return
         */
        inline explicit ScopedLock(MutexLock* lock);

        /**
         * Destructor.
         * @return
         */
        inline ~ScopedLock();

        inline bool Set(MutexLock* lock);

        inline bool SetLocked(MutexLock* lock);

        /**
         * returns the underlying lock
         * @return
         */
        inline MutexLock* Get();

        inline bool AcquireLockWithStatistics_(tbb::atomic<uint32_t>* free, tbb::atomic<uint32_t>* busy,
                const char* function, const char* file, int line);

        inline bool AcquireLock_(const char* function, const char* file, int line);

        inline bool ReleaseLock_(const char* function, const char* file, int line);

        /**
         * returns true iff if the lock is held by the current thread.
         * It is not important if the lock was acquired by the scoped
         * lock or by other means.
         * @return
         */
        inline bool IsHeld();

        /**
         * returns a developer-readable representation of the
         * lock.
         *
         * @return
         */
        inline std::string DebugString();

        /**
         * Neutralize the scoped lock.
         * The caller is then responsible to releasing any lock held by this scoped lock.
         */
        inline void Unset();

        DISALLOW_COPY_AND_ASSIGN(ScopedLock);
};

/**
 * A scoped rw lock is a helper for a read-write mutex that ensures
 * that the lock is released once the scoped lock variable
 * leaves the scope.
 *
 * A scoped rw lock should only be used by a single thread, while there
 * can be multiple scoped rw lock for the same lock in different threads.
 *
 * The scoped rw lock is used to ensure error and exception safe code.
 *
 * This scoped rw lock is similar to the concept of RIIT.
 * The scoped lock pattern is described in "POSA II".
 *
 * @sa http://en.wikipedia.org/wiki/Resource_Acquisition_Is_Initialization
 */
class ScopedReadWriteLock {
        /**
         * Underlying lock
         */
        ReadWriteLock* lock;

        /**
         * true iff the lock is held using this lock. This
         * means also that this scoped lock is responsible for releasing
         * the lock.
         */
        volatile bool holds_it;
    public:
        inline explicit ScopedReadWriteLock(ReadWriteLock* lock);

        inline ~ScopedReadWriteLock();

        inline bool Set(ReadWriteLock* lock);

        inline bool SetLocked(ReadWriteLock* lock);

        inline ReadWriteLock* Get();

        inline bool AcquireReadLockWithStatistics_(tbb::atomic<uint32_t>* free, tbb::atomic<uint32_t>* busy,
                const char* function, const char* file, int line);

        inline bool AcquireReadLock_(const char* function, const char* file, int line);

        inline bool AcquireWriteLockWithStatistics_(tbb::atomic<uint32_t>* free, tbb::atomic<uint32_t>* busy,
                const char* function, const char* file, int line);

        inline bool AcquireWriteLock_(const char* function, const char* file, int line);

        inline bool TryAcquireWriteLock_(bool* locked, const char* function, const char* file, int line);
        inline bool TryAcquireReadLock_(bool* locked, const char* function, const char* file, int line);

        inline bool ReleaseLock_(const char* function, const char* file, int line);

        inline bool IsHeldForWrites();

        inline std::string DebugString();

        /**
         * Neutralize the scoped lock.
         * The caller is then responsible to releasing any lock held by this scoped lock.
         */
        inline void Unset();

        DISALLOW_COPY_AND_ASSIGN(ScopedReadWriteLock);
};

ScopedReadWriteLock::ScopedReadWriteLock(ReadWriteLock* lock) {
    this->lock = lock;
    this->holds_it = false;
}

ScopedReadWriteLock::~ScopedReadWriteLock() {
    if (holds_it && lock) {
        lock->ReleaseLock();
    }
}

void ScopedReadWriteLock::Unset() {
    holds_it = false;
    this->lock = NULL;
}

bool ScopedReadWriteLock::Set(ReadWriteLock* lock) {
    if (holds_it == true) return false;
    this->lock = lock;
    return true;
}

bool ScopedReadWriteLock::SetLocked(ReadWriteLock* lock) {
    if (holds_it == true) return false;
    this->lock = lock;
    this->holds_it = true;
    return true;
}

ReadWriteLock* ScopedReadWriteLock::Get() {
    return this->lock;
}

bool ScopedReadWriteLock::AcquireReadLockWithStatistics_(tbb::atomic<uint32_t>* free, tbb::atomic<uint32_t>* busy,
        const char* function, const char* file, int line) {
    if (!this->lock) return false;
    bool r = this->lock->AcquireReadLockWithStatistics_(free, busy, function, file, line);
    if (r) this->holds_it = true;
    return r;
}

bool ScopedReadWriteLock::TryAcquireReadLock_(bool* locked, const char* function, const char* file, int line) {
    if (!this->lock) return false;
    bool r = this->lock->TryAcquireReadLock_(locked, function, file, line);
    if (r && locked && *locked) this->holds_it = true;
    return r;
}

bool ScopedReadWriteLock::TryAcquireWriteLock_(bool* locked, const char* function, const char* file, int line) {
    if (!this->lock) return false;
    bool r = this->lock->TryAcquireWriteLock_(locked, function, file, line);
    if (r && locked && *locked) this->holds_it = true;
    return r;
}


inline bool ScopedReadWriteLock::AcquireReadLock_(const char* function, const char* file, int line) {
    if (!this->lock) return false;
    bool r = this->lock->AcquireReadLock_(function, file, line);
    if (r) this->holds_it = true;
    return r;
}

inline bool ScopedReadWriteLock::AcquireWriteLockWithStatistics_(tbb::atomic<uint32_t>* free, tbb::atomic<uint32_t>* busy,
        const char* function, const char* file, int line) {
    if (!this->lock) return false;
    bool r = this->lock->AcquireWriteLockWithStatistics_(free, busy, function, file, line);
    if (r) this->holds_it = true;
    return r;
}

inline bool ScopedReadWriteLock::AcquireWriteLock_(const char* function, const char* file, int line) {
    if (!this->lock) return false;
    bool r = this->lock->AcquireWriteLock_(function, file, line);
    if (r) this->holds_it = true;
    return r;
}

bool ScopedReadWriteLock::ReleaseLock_(const char* function, const char* file, int line) {
    if (!this->lock) return false;
    this->holds_it = false;
    return this->lock->ReleaseLock_(function, file, line);
}

bool ScopedReadWriteLock::IsHeldForWrites() {
    return (this->lock ? this->lock->IsHeldForWrites() : false);
}

std::string ScopedReadWriteLock::DebugString() {
    return (this->lock ? this->lock->DebugString() : "<lock not set>");
}

ScopedLock::ScopedLock(MutexLock* lock) {
    this->lock = lock;
    this->holds_it = false;
}

ScopedLock::~ScopedLock() {
    if (holds_it && lock) {
        lock->ReleaseLock();
    }
}

bool ScopedLock::Set(MutexLock* lock) {
    if (holds_it == true) return false;
    this->lock = lock;
    return true;
}

bool ScopedLock::SetLocked(MutexLock* lock) {
    if (holds_it == true) return false;
    this->lock = lock;
    this->holds_it = true;
    return true;
}

MutexLock* ScopedLock::Get() {
    return this->lock;
}

void ScopedLock::Unset() {
    holds_it = false;
    this->lock = NULL;
}

bool ScopedLock::AcquireLockWithStatistics_(tbb::atomic<uint32_t>* free, tbb::atomic<uint32_t>* busy,
        const char* function, const char* file, int line) {
    if (!this->lock) return false;
    bool r = this->lock->AcquireLockWithStatistics_(free, busy, function, file, line);
    if (r) this->holds_it = true;
    return r;
}

bool ScopedLock::AcquireLock_(const char* function, const char* file, int line) {
    if (!this->lock) return false;
    bool r = this->lock->AcquireLock_(function, file, line);
    if (r) this->holds_it = true;
    return r;
}

bool ScopedLock::ReleaseLock_(const char* function, const char* file, int line) {
    if (!this->lock) return false;
    this->holds_it = false;
    return this->lock->ReleaseLock_(function, file, line);
}

bool ScopedLock::IsHeld() {
    return (this->lock ? this->lock->IsHeld() : false);
}

std::string ScopedLock::DebugString() {
    return (this->lock ? this->lock->DebugString() : "<lock not set>");
}

}
}

#endif  // DEDUPV1_LOCKS_H__ NOLINT
