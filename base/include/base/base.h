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
 * @file base.h
 * Some base definitions used by the complete dedupv1 system
 */
#ifndef BASE_H_
#define BASE_H_

#include "config.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>

#include <string>
#include <list>

#include <base/comp.h>

typedef uint8_t byte;
typedef std::basic_string<byte> bytestring;

/**
 * A macro to disallow the copy constructor and operator= functions
 * This should be used in the private: declarations for a class.
 *
 * In Google classes such a define is often called
 * DISALLOW_EVIL_METHODS or so
 */
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
        TypeName(const TypeName&);               \
        void operator=(const TypeName&)

// If there is no page size set, we set it to 4K
#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

// macro to avoid "unused" compile warnings
// from http://stackoverflow.com/questions/1486904/how-do-i-best-silence-a-warning-about-unused-variables
#define UNUSED(expr) do { (void)(expr); } while (0)

namespace dedupv1 {
	
/**
 * \namespace dedupv1::base
 * Namespace for all base classes and functions.
 * All members of the base namespace are not bound by the
 * dedupv1 project and are not directly related
 * to data deduplication.
 */
namespace base {

/**
 * creates a byte string from an array of bytes
 * @param v
 * @param s
 * @return
 */
bytestring make_bytestring(const byte* v, size_t s);

/**
 * Converts a char string (e.g. from protobuf) to
 * a byte string.
 *
 * @param s
 * @return
 */
bytestring make_bytestring(const std::string& s);

/**
 * creates a list from a single object
 * @param o
 * @return
 */
template<class T> std::list<T> make_list(const T& o) {
        std::list<T> l;
        l.push_back(o);
        return l;
}

/**
 * Registers the types for indexes.
 */
void RegisterDefaults();

}
}

/**
 * TODO (dmeister) Refactor
 * @param file_index
 * @param file_offset
 * @param file_count_bits
 * @return
 */
uint64_t make_multi_file_address(uint64_t file_index,
        uint64_t file_offset, unsigned int file_count_bits);

/**
 * Refactor
 * @param multi_file_adress
 * @param file_count_bits
 * @return
 */
unsigned int multi_file_get_file_index(uint64_t multi_file_adress,
        unsigned int file_count_bits);

/**
 * Refactor
 * @param multi_file_adress
 * @param file_count_bits
 * @return
 */
unsigned int multi_file_get_file_offset(uint64_t multi_file_adress,
        unsigned int file_count_bits);

// from http://stackoverflow.com/questions/1473921/optimizing-branching-by-re-ordering
#ifdef __GNUC__
#  define likely(x)   __builtin_expect((x), 1)
#  define unlikely(x) __builtin_expect((x), 0)
#else
#  define likely(x)   (x)
#  define unlikely(x) (x)
#endif

#endif /* BASE_H_ */
