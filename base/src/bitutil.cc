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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include <base/base.h>
#include <base/bitutil.h>
#include <base/logging.h>

LOGGER("Bitutil");

namespace dedupv1 {
namespace base {

double log2(double v) {
    double l2 = log(2.0);
    double d = log(1.0 * v);
    return d / l2;
}

int bits(int value) {
    if (unlikely(value < 2)) {
        return 0;
    }
    return (sizeof(int) << 3) - __builtin_clz(value - 1);
}

size_t RoundUpFullBlocks(size_t s, size_t block_size) {
    for(int i = 1;; i = i << 1) {
        size_t t = block_size * i;
        if (s <= t) {
            return t;
        }
    }
    return 0;
}

}

}
