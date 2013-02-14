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

#ifndef TIMED_AVERAGE_H__
#define TIMED_AVERAGE_H__

#include <tbb/spin_mutex.h>
#include <tbb/tick_count.h>
#include <queue>

#include <base/base.h>

namespace dedupv1 {
namespace base {

/**
 * TimedAverage allows to evaluate the average value of a variable over a time intervall.
 *
 * This class is thread-safe.
 *
 * @param T The type of the Variable (has to be some kind of number)
 * @param ET The evaluation time in seconds
 */
template<typename T, int ET> class TimedAverage {
    private:
        /**
         * Evaluation time in seconds
         */
        double eval_time_;

        /**
         * Mutex to allow thread safe access
         */
        tbb::spin_mutex mutex_;

        /**
         * Queue holding the timestamps when changes happened
         */
        std::queue<tbb::tick_count> queue_;

        /**
         * Queue with the values for the different changes
         */
        std::queue<T> values_;

        /**
         * The last value set before the first entry of the queues is legal
         */
        T first_val_;

        /**
         * The last entry set (used for Dec and Inc)
         */
        T last_set_val_;

    public:
        /**
         * Create a new TimedAverage
         */
        explicit TimedAverage() {
            this->eval_time_ = (double) ET;
            first_val_ = 0;
            last_set_val_ = 0;
        }

        /**
         * Set a new value
         *
         * @param value The new value of the variable
         */
        void Set(T value) {
            tbb::spin_mutex::scoped_lock scoped_lock(this->mutex_);
            tbb::tick_count now = tbb::tick_count::now();
            queue_.push(now);
            values_.push(value);
            last_set_val_ = value;
            while (((now - queue_.front()).seconds()) > eval_time_) {
                first_val_ = values_.front();
                values_.pop();
                queue_.pop();
            }
        }

        /**
         * increase the variable by one and add a new entry
         */
        void Inc() {
            tbb::spin_mutex::scoped_lock scoped_lock(this->mutex_);
            tbb::tick_count now = tbb::tick_count::now();
            last_set_val_++;
            queue_.push(now);
            values_.push(last_set_val_);
            while (((now - queue_.front()).seconds()) > eval_time_) {
                first_val_ = values_.front();
                values_.pop();
                queue_.pop();
            }
        }

        /**
         * Decrease the variable by one
         */
        void Dec() {
            tbb::spin_mutex::scoped_lock scoped_lock(this->mutex_);
            tbb::tick_count now = tbb::tick_count::now();
            last_set_val_--;
            queue_.push(now);
            values_.push(last_set_val_);
            while (((now - queue_.front()).seconds()) > eval_time_) {
                first_val_ = values_.front();
                values_.pop();
                queue_.pop();
            }
        }

        /**
         * Get the average of the variable over the last ET seconds
         *
         * @return the average value
         */
        double GetAverage() {
            tbb::spin_mutex::scoped_lock scoped_lock(this->mutex_);
            tbb::tick_count now = tbb::tick_count::now();
            while ((!queue_.empty()) && (((now - queue_.front()).seconds()) > eval_time_)) {
                first_val_ = values_.front();
                values_.pop();
                queue_.pop();
            }
            std::queue<tbb::tick_count> tmp_queue = queue_;
            std::queue<T> tmp_values = values_;
            double last_val = first_val_;
            scoped_lock.release();

            if (unlikely(tmp_queue.empty())) {
                // In this case the first val has been set before eval_time and not changed till now
                return (double) first_val_;
            }

            tbb::tick_count last_time = tmp_queue.front();
            tmp_queue.pop();
            double next_val = tmp_values.front();
            tmp_values.pop();
            double duration = this->eval_time_ - (now - last_time).seconds();
            double sum = duration * last_val;
            last_val = next_val;

            while (likely(!tmp_queue.empty())) {
                tbb::tick_count next_time = tmp_queue.front();
                tmp_queue.pop();
                next_val = tmp_values.front();
                tmp_values.pop();

                duration = (next_time - last_time).seconds();
                sum += duration * last_val;

                last_val = next_val;
                last_time = next_time;
            }

            duration = (now - last_time).seconds();
            sum += duration * ((double)last_val);

            return sum / eval_time_;
        }

        /**
         * get the actual value
         *
         * @return the actual value
         */
        T GetValue() {
            return last_set_val_;
        }
};

}
}

#endif  // TIMED_AVERAGE_H__
