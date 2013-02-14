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

#ifndef SLIDING_AVERAGE_H__
#define SLIDING_AVERAGE_H__

#include <map>
#include <queue>

#include <tbb/mutex.h>
#include <tbb/spin_mutex.h>
#include <tbb/concurrent_queue.h>
#include <tbb/atomic.h>

#include <base/base.h>

namespace dedupv1 {
namespace base {

/**
 * Calculates a sliding average over a specified window.
 * Not thread-safe.
 */
class SlidingAverage {
    private:
        int window_size;

        std::map<int, double> data;

        double sum;

    public:
        /**
         *
         * @param window_size > 0
         * @return
         */
        explicit SlidingAverage(int window_size);

        bool Add(int key, double value);
        double GetAverage(int current_key);
};

/**
 * A sliding average class with less functionality, but thread-safe.
 */
template<int WS> class TemplateSimpleSlidingAverage {
    private:
        tbb::spin_mutex mutex_;

        std::queue<uint64_t> queue_;

        uint64_t sum_;

    protected:
        int window_size_;

        TemplateSimpleSlidingAverage(int window_size) {
            sum_ = 0;
            window_size_ = window_size;
        }
    public:
        /**
         *
         * @param window_size > 0
         * @return
         */
        explicit TemplateSimpleSlidingAverage() {
            window_size_ = WS;
            sum_ = 0;
        }

        bool Add(uint64_t value) {
            tbb::spin_mutex::scoped_lock scoped_lock(this->mutex_);
            // lock is held

            this->sum_ += value;
            if (this->queue_.size() >= this->window_size_) {
                if (queue_.empty()) {
                    return false;
                }
                this->sum_ -= queue_.front();
                queue_.pop();
            }
            this->queue_.push(value);
            return true;
        }

        double GetAverage() {
            tbb::spin_mutex::scoped_lock scoped_lock(this->mutex_);
            // lock is held
            if (this->queue_.empty()) {
                return 0.0;
            }
            return 1.0 * this->sum_ / this->queue_.size();
        }
};

class SimpleSlidingAverage : public TemplateSimpleSlidingAverage<0> {
    public:
        explicit SimpleSlidingAverage(int window_size) : TemplateSimpleSlidingAverage<0>(window_size) {
        }
};

}
}

#endif  // SLIDING_AVERAGE_H__
