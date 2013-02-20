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
/**
 * @file bitutil.h
 * @brief Bit-manipulation helper functions
 */

#ifndef BITUTIL_H__
#define BITUTIL_H__

#include <stdint.h>

namespace dedupv1 {
namespace base {

/**
 * sets the n-th bit of x to 1
 *
 * @param x
 * @param n
 */
template <typename T> void bit_set(T* x, int n) { ((*x) |= (1ULL << (n))); }

/**
 * sets the n-th bit of x to 0.
 * @param x
 * @param n
 */
template <typename T> void bit_clear(T* x, int n) { ((*x) &= ~(1ULL << (n))); }

/**
 * tests if the n-th bit of x is set to 1.
 *
 * @param x
 * @param n
 * @return
 */
template <typename T> bool bit_test(const T& x, int n) { return ((x) & (1ULL << (n))); }

/**
 * Calculates how much bits are needed at least to encode the given number of values.
 * (So it really give the most significant set bit of value - 1)
 *
 * @param value
 * @return number bits necessary to represent the value.
 */
int bits(int value);

/**
 * log to the base 2
 */
double log2(double n);

size_t RoundUpFullBlocks(size_t s, size_t block_size);

}
}

#endif  // BITUTIL_H__
