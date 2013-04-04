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

#include <base/barrier.h>

#include <errno.h>

#include <base/logging.h>

LOGGER("Barrier");

namespace dedupv1 {
namespace base {

Barrier::Barrier(uint32_t count) {
#if defined(_POSIX_BARRIERS) && (_POSIX_BARRIERS - 20012L) >= 0
    int r = pthread_barrier_init(&barr_, NULL, count);
    if (r != 0) {
        WARNING("Barrier init failed: " << strerror(r));
    }
#else
    this->count_ = count;
    this->current_ = 0;
    this->fired_ = false;
#endif
}

Barrier::~Barrier() {
#if defined(_POSIX_BARRIERS) && (_POSIX_BARRIERS - 20012L) >= 0
    int r = pthread_barrier_destroy(&barr_);
    if (r != 0) {
        WARNING("Barrier destroy failed: " << strerror(r));
    }
#endif
}

bool Barrier::Wait() {
#if defined(_POSIX_BARRIERS) && (_POSIX_BARRIERS - 20012L) >= 0
    int r = pthread_barrier_wait(&barr_);
    CHECK(r == 0 || r == PTHREAD_BARRIER_SERIAL_THREAD, "Wait error: " << strerror(r));
#else
    CHECK(lock_.AcquireLock(), "Failed to acquire lock");
    if (fired_) {
        CHECK(lock_.ReleaseLock(), "Failed to release lock");
        ERROR("Failed wait for a non-ready barrier");
        return false;
    }
    this->current_++;

    while (true) {
        if (!fired_ && this->current_ == this->count_) {
            // We have the right number of threads, unlock all
            this->condition_.Broadcast();
            fired_ = true;
            this->current_--;
            if (current_ == 0) {
                // re-enable barrier
                fired_ = false;
            }
            break;
        } else if (fired_) {
            this->current_--;
            if (current_ == 0) {
                // re-enable barrier
                fired_ = false;
            }
            break;
        } else {
            // if there are open threads, wait
            CHECK(this->condition_.ConditionWait(&lock_), "Failed to wait for condition");
        }
    }
    CHECK(lock_.ReleaseLock(), "Failed to release lock");
#endif
    return true;
}

}

}
