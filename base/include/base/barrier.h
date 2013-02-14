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
/**
 * @file barrier.h
 * Barrier 
 */

#ifndef BARRIER_H__
#define BARRIER_H__

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include <base/base.h>

#if defined(_POSIX_BARRIERS) && (_POSIX_BARRIERS - 20012L) >= 0
#else
#include <base/locks.h>
#endif

namespace dedupv1 {
namespace base {

/**
 * A barrier is synchronization construct in concurrent programming that
 * assures that all thread waiting in a barrier only proceed
 * when n threads are waiting in it.
 *
 * If POSIX barriers are available, we use them.
 * However, POSIX barriers are not available on on some platforms, e.g. on Mac.
 * We implement a basic barrier implementation.
 *
 * @sa http://en.wikipedia.org/wiki/Barrier_(computer_science)
 */
class Barrier {
        DISALLOW_COPY_AND_ASSIGN(Barrier);
    private:
#if defined(_POSIX_BARRIERS) && (_POSIX_BARRIERS - 20012L) >= 0
        /**
         * Posix barrier
         */
        pthread_barrier_t barr_;
#else
        /**
         * Condition flag for the alternative implementation
         */
        Condition condition_;

        /**
         * Lock to protected the count and current members
         */
        MutexLock lock_;

        /**
         * Number of threads to what the barrier is waiting.
         * If count is set to 2, the barrier is released
         * if 2 threads called Wait()
         */
        uint32_t count_;

        /**
         * Number of threads currently waiting in the barrier
         */
        uint32_t current_;
        
        bool fired_;
#endif

    public:
        /**
         * Creates a barrier in which count threads
         * should wait.
         *
         * @param count number of threads that wait in this barrier 
         * until the barrier is released
         * @return
         */
        explicit Barrier(uint32_t count);

        /**
         * Destructor
         * @return
         */
        ~Barrier();

        /**
         * Wait until count threads are waiting in the
         * barrier.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool Wait();
};

}
}

#endif  // BARRIER_H__
