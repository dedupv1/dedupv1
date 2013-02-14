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
#ifndef HANDOVER_STORE_H__
#define HANDOVER_STORE_H__

#include <base/base.h>
#include <base/semaphore.h>
#include <base/locks.h>

namespace dedupv1 {
namespace base {

/**
 * The handover store is used to handover an object between
 * to threads. In constrast to e.g. a concurrent queue the important
 * point is that the handover Put side does only unblock if an
 * partner gets the object. It is a synchonization point between
 * these two threads.
 *
 * It is usually used for flow-control in producer consumer situations
 * where the consumer might be slower than the producer. Here a
 * concurrent queue might become unlimited large. A handover store
 * slows the producer down to prevent that.
 */
template<class T> class HandoverStore {
    private:
        DISALLOW_COPY_AND_ASSIGN(HandoverStore);

        /**
         * Condition that is fired when the handover store is empty
         */
        dedupv1::base::Condition empty_condition_;

        /**
         * Condition that is fired when the handover store is filled.
         */
        dedupv1::base::Condition fill_condition_;

        /**
         * Lock to protect the buffer. The conditions also use
         * this lock.
         */
        dedupv1::base::MutexLock lock_;

        bool buffer_used_;

        /**
         * buffer element that is handed over
         */
        T buffer_;
    public:
        /**
         * Constructor.
         *
         * @param count
         * @return
         */
        HandoverStore() {
            buffer_used_ = false;
        }

        /**
         * Puts a new value to the handover store.
         *
         * The method blocks for at most the given time (in ms) if there
         * is no partner available to get the value.
         *
         * @param value
         * @param timeout s
         * @return
         */
        dedupv1::base::timed_bool Put(T value, uint32_t timeout) {
            if (!lock_.AcquireLock()) {
                return dedupv1::base::TIMED_FALSE;
            }
            while(buffer_used_) {
                dedupv1::base::timed_bool tb = empty_condition_.ConditionWaitTimeout(&lock_, timeout);
                if (tb == dedupv1::base::TIMED_FALSE) {
                    lock_.ReleaseLock();
                    return dedupv1::base::TIMED_FALSE;
                } else if (tb == dedupv1::base::TIMED_TIMEOUT && buffer_used_) {
                    if (!lock_.ReleaseLock()) {
                        return dedupv1::base::TIMED_FALSE;
                    }
                    return dedupv1::base::TIMED_TIMEOUT;
                }
            }
            // buffer_used == false
            this->buffer_ = value;
            buffer_used_ = true;
            if (!fill_condition_.Broadcast()) {
                lock_.ReleaseLock();
                return dedupv1::base::TIMED_FALSE;
            }
            if (!lock_.ReleaseLock()) {
                return dedupv1::base::TIMED_FALSE;
            }
            return dedupv1::base::TIMED_TRUE;
        }

        /**
         * Receives an object from the handover store.
         *
         * @param value
         * @param timeout milliseconds
         * @return
         */
        dedupv1::base::timed_bool Get(T* value, uint32_t timeout) {
            if (!lock_.AcquireLock()) {
                return dedupv1::base::TIMED_FALSE;
            }
            while(!buffer_used_) {
                dedupv1::base::timed_bool tb = fill_condition_.ConditionWaitTimeout(&lock_, timeout);
                if (tb == dedupv1::base::TIMED_FALSE) {
                    lock_.ReleaseLock();
                    return dedupv1::base::TIMED_FALSE;
                } else if (tb == dedupv1::base::TIMED_TIMEOUT && !buffer_used_) {
                    if (!lock_.ReleaseLock()) {
                        return dedupv1::base::TIMED_FALSE;
                    }
                    return dedupv1::base::TIMED_TIMEOUT;
                }
            }
            // buffer_used == true
            *value = this->buffer_;
            buffer_used_ = false;
            if (!empty_condition_.Broadcast()) {
                lock_.ReleaseLock();
                return dedupv1::base::TIMED_FALSE;
            }
            if (!lock_.ReleaseLock()) {
                return dedupv1::base::TIMED_FALSE;
            }
            return dedupv1::base::TIMED_TRUE;
        }
};

}
}

#endif  // HANDOVER_STORE_H__
