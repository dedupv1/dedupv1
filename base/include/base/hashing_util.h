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
 * @file hashing_util.h
 * Contains different simple hash functions and similar
 * functions
 *
 * TODO (dmeister) Document what are the tradeoffs between the hash functions
 */

#ifndef HASHING_UTIL_H__
#define HASHING_UTIL_H__

#include <base/base.h>

namespace dedupv1 {
namespace base {

/**
 * From http://burtleburtle.net/bob/c/lookup3.c
 */
uint64_t bj_hash(const void *data, size_t data_size);

/**
 * Compares the two given byte arrays
 */
int raw_compare(const void* data1, size_t data1_size,
        const void* data2, size_t data2_size);

// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.
// @see http://code.google.com/p/smhasher/
void murmur_hash3_x86_32  ( const void * key, int len, uint32_t seed, void * out );

// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.
// @see http://code.google.com/p/smhasher/
void murmur_hash3_x86_128 ( const void * key, int len, uint32_t seed, void * out );

// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.
// @see http://code.google.com/p/smhasher/
void murmur_hash3_x64_128 ( const void * key, int len, uint32_t seed, void * out );


}
}

#endif  // HASHING_UTIL_H__
