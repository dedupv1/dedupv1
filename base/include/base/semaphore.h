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
#ifndef SEMAPHORE_H__
#define SEMAPHORE_H__

#include <base/base.h>
#include <semaphore.h>

namespace dedupv1 {
namespace base {

/**
 * Wrapper around a POSIX semaphore
 */
class Semaphore {
    private:
        DISALLOW_COPY_AND_ASSIGN(Semaphore);
        sem_t* sem_;
    public:
        /**
         * Creates a new semaphore with the given initial value
         * @param value
         * @return
         */
        explicit Semaphore(int value);

        /**
         * Destructor
         * @return
         */
        ~Semaphore();

        /**
         * Waits until the semaphore value is > 0.
         * This call might block without bounds which will lead to
         * a deadlock.
         *
         * Often the method TryWait and TimedWait are a better choice.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Wait();

        /**
         * Posts a new semaphore value so that the value is
         * incremented by 1. This call may unlock some
         * waiting threads.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool Post();

        /**
         * Tries to decrement the semaphore value.
         * If the value is <= 0, this method doesn't block
         * but sets the locked out value to false. If the semaphore value
         * was > 0, the locked out value is set to true.
         *
         * @param locked
         * @return true iff ok, otherwise an error has occurred.
         */
        bool TryWait(bool* locked);
};

}
}

#endif  // SEMAPHORE_H__
