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

#include <base/base.h>
#include <base/timer.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>

namespace dedupv1 {
namespace base {

Walltimer::Walltimer() {
#ifndef __APPLE__
    clock_gettime(CLOCK_MONOTONIC, &this->last_);
#else
    this->last_ = 0.0;
    double mic, time;
    struct timeval tp;

    gettimeofday(&tp, NULL);
    this->base_sec_ = tp.tv_sec;
    this->base_usec_ = tp.tv_usec;
    time = static_cast<double>(tp.tv_sec - this->base_sec_);
    mic = static_cast<double>(tp.tv_usec - this->base_usec_);
    time = (time * 1000.0 + mic / 1000.0) - this->last_;
    this->last_ = time;
#endif
}

}
}
