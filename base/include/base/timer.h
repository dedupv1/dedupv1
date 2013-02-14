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

#ifndef TIMER_H__
#define TIMER_H__

#include <math.h>
#include <time.h>
#include <sys/time.h>
#include <base/base.h>

namespace dedupv1 {
namespace base {

/**
 * Utility class that records the wallclock time between
 * multiple calls. Used for profiling.
 *
 * It is not safe to use Walltimer access threads as there might
 * be a CPU clock drift. For multi threaded timing consider using
 * tbb::tick_count.
 *
 */
class Walltimer {
    private:
#ifndef __APPLE__
        /**
         * The last time the timer was called
         */
        struct timespec last_;
#else
        double last_;
        long base_sec_;
        long base_usec_;
#endif
    public:
        /**
         * Creates a new Walltimer
         * @return
         */
        Walltimer();

        /**
         * Returns the duration in ms from the
         * creation of the object or the last call of this
         * method.
         *
         * @return
         */
        inline double GetTime();
};

double Walltimer::GetTime() {
#ifndef __APPLE__
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);

    double time_ = ((now.tv_sec * 1000) + (now.tv_nsec / 1000000)) -
            ((last_.tv_sec * 1000) + (last_.tv_nsec  / 1000000));
    this->last_ = now;
    return time_;
#else
    double mic, time;
    struct timeval tp;
    gettimeofday(&tp, NULL);
    time = static_cast<double>(tp.tv_sec - this->base_sec_);
    mic = static_cast<double>(tp.tv_usec - this->base_usec_);
    time = (time * 1000.0 + mic / 1000.0) - this->last_;
    this->last_ = time;
    return (time);
#endif
}

}
}

#endif  // TIMER_H__
