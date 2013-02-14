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

#ifndef MULTI_SIGNAL_CONDITION_H__
#define MULTI_SIGNAL_CONDITION_H__

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include <base/base.h>
#include <base/locks.h>

namespace dedupv1 {
namespace base {

/**
 * A multi signal condition is a bit like a barrier, but only a single thread is waiting for n other threads
 * so signal that they reached a "barrier" construct.
 *
 * A multi signal condition is currently not reuseable.
 *
 */
class MultiSignalCondition {
        DISALLOW_COPY_AND_ASSIGN(MultiSignalCondition);
    private:
        /**
         * Condition flag
         */
        Condition condition_;

        /**
         * Lock to protected the count and current members
         */
        MutexLock lock_;

        /**
         * Number of threads to what the multi-signal condition is waiting.
         * If count is set to 2, the barrier is released
         * if 2 threads called Wait()
         */
        uint32_t count_;

        /**
         * Number of threads currently waiting in the multi-signal condition
         */
        uint32_t current_;

        bool signaled_;
    public:
        /**
         * Creates a multi-signal condition in which count threads
         * should wait.
         *
         * @param count number of threads that wait in this barrier
         * until the barrier is released
         * @return
         */
        explicit MultiSignalCondition(uint32_t count);

        /**
         * Destructor
         * @return
         */
        ~MultiSignalCondition();

        bool Signal();

        /**
         * Wait until count threads are waiting in the
         * multi-signal condition.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool Wait();
};

}
}

#endif  // BARRIER_H__
