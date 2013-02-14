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

#ifndef PROFILE_H__
#define PROFILE_H__

#include <tbb/atomic.h>
#include <tbb/tick_count.h>

#include <base/base.h>
#include <base/timer.h>
#include <base/sliding_average.h>

namespace dedupv1 {
namespace base {

/**
 * Profile information in ms.
 */
class Profile {
        tbb::atomic<uint64_t> sum;
        tbb::atomic<uint64_t> count;
    public:
        /**
         * Constructor.
         * @return
         */
        Profile();

        /**
         * Destructor.
         * @return
         */
        ~Profile();

        inline uint64_t Add(Walltimer* t);
        inline uint64_t Add(uint64_t i);

        /**
         * Returns the sum over all added values
         */
        uint64_t GetSum() const;

        /**
         * Returns the average over all added values.
         * If there are not added values, 0 is returned.
         */
        double GetAverage() const;

        /**
         * Resets the profiling object.
         */
        void Reset();
};

/**
 * The idea of the profile timer is to make the usage of the Profile class easier.
 * The add the wallclock time of a scope, e.g. a method to a profile instance, all to be done is
 * to create an instance of the ProfileTimer connected with the profile instance and wait until the
 * ProfileTimer leaves scope.
 *
 * Note: Here we break the convention of not using non-const references for a cleaner code.
 */
class ProfileTimer {
    private:
        Walltimer t_;
        Profile& profile_;
        bool stopped_;
    public:
        /**
         * Creates a new profile time. When the object leaves the scope (or when the stop method) is called, the lifetime of the object
         * is added to the profile
         */
        inline ProfileTimer(dedupv1::base::Profile& profile);
        inline ~ProfileTimer();

        inline void stop();
};

void ProfileTimer::stop() {
    if (!stopped_) {
        profile_.Add(&t_);
        stopped_ = true;
    }
}

ProfileTimer::ProfileTimer(dedupv1::base::Profile& profile) :
    t_(), profile_(profile), stopped_(false) {
}

ProfileTimer::~ProfileTimer() {
    stop();
}

class SlidingAverageProfileTimer {
    private:
        tbb::tick_count start_;
        dedupv1::base::SimpleSlidingAverage& average_;
        bool stopped_;
    public:
        inline SlidingAverageProfileTimer(dedupv1::base::SimpleSlidingAverage& average);
        inline ~SlidingAverageProfileTimer();
        inline void stop();
};

SlidingAverageProfileTimer::SlidingAverageProfileTimer(dedupv1::base::SimpleSlidingAverage& average) :
    average_(average), stopped_(false) {
    start_ = tbb::tick_count::now();
}

SlidingAverageProfileTimer::~SlidingAverageProfileTimer() {
    stop();
}

void SlidingAverageProfileTimer::stop() {
    if (!stopped_) {
        tbb::tick_count end = tbb::tick_count::now();
        average_.Add((end - start_).seconds() * 1000);
        stopped_ = true;
    }
}

uint64_t Profile::Add(Walltimer* t) {
    double time = t->GetTime();
    return Add((uint64_t) time);
}

uint64_t Profile::Add(uint64_t v) {
    count.fetch_and_increment();
    sum.fetch_and_add(v);
    return v;
}

}
}

#endif  // PROFILE_H__
