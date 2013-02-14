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

#ifndef PROTECTED_H__
#define PROTECTED_H__

#include <base/base.h>
#include <base/callback.h>

namespace dedupv1 {
namespace base {

/**
 * Protects a specific object by a lock against concurrent accesses.
 * All accesses to the protected value are done while holding this lock
 *
 * The class is inspired by the Atoms system of Clojure.
 *
 * The value to protect as the same size as an integral type, a trivial constructor/destructor, can be copied
 * by memcpy and compared by memcmp, it is better to use tbb::atomic<T>.
 *
 * Operators like * are not overwritten as they indicate a reference that we cannot provide here.
 *
 * @sa http://clojure.org/atoms
 */
template <class T> class Protected {
    private:
        /**
         * flag if the option value is set.
         *
         * All accesses to the protected value are done while holding this lock
         */
        tbb::spin_mutex lock_;

        /**
         * value to protect.
         *
         * All accesses to the protected value are done while holding this lock
         */
        T value_;
    public:

        /**
         * By default, the protected value is created using its default constructor
         * @return
         */
        Protected() {
        }

        /**
         * creates a new option.
         * Usually a helper method make_option should be used.
         * @param b
         * @param value
         * @return
         */
        Protected(const T& value) : value_(value) {
        }

        /**
         * return a copy of the value. No other thread can interfere with the copy.
         * @return
         */
        T Get() {
            tbb::spin_mutex::scoped_lock l(lock_);
            return value_;
        }

        /**
         * Sets the value that is protected. No other thread can interfere with the set operation
         */
        void Set(const T& value) {
            tbb::spin_mutex::scoped_lock l(lock_);
            value_ = value;
        }

        /**
         * Performs a atomic compare and swap operation on the protected value.
         *
         * To use this method the compare operator must be overwritten
         *
         * @sa http://en.wikipedia.org/wiki/Compare-and-swap
         */
        bool CompareAndSwap(const T& compare, const T& new_value) {
            tbb::spin_mutex::scoped_lock l(lock_);
            if (value_ == compare) {
                value_ = new_value;
                return true;
            }
            return false;
        }

        /**
         * Calls the callback to that the protected value is passed as a const reference under the protection.
         *
         * No other thread can interfere with the callback operation
         */
        void Protect(dedupv1::base::Callback1<void, const T&>* callback) {
            tbb::spin_mutex::scoped_lock l(lock_);
            callback->Call(value_);
        }

        /**
         * Calls the callback to that the protected value is passed as a mutable pointer under the protection.
         *
         * No other thread can interfere with the callback operation
         */
        void Protect(dedupv1::base::Callback1<void, T*>* callback) {
            tbb::spin_mutex::scoped_lock l(lock_);
            callback->Call(&value_);
        }
};

}
}

#endif // PROTECTED_H__
