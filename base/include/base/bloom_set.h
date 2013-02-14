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

#ifndef BLOOM_SET_H_
#define BLOOM_SET_H_

#include <base/base.h>
#include <base/index.h>
#include <base/locks.h>

namespace dedupv1 {
namespace base {

/**
 * Implementation of a bloom filter.
 *
 * We call the class bloom set to distinguish it from the bloom filter
 * deduplication filter.
 *
 * A bloom filter is a probabilistic data structure with set like operations.
 * It allows to add items and to test for membership. However, the membership test operations
 * are special. If the membership test failed, we can be sure that the search key is stored
 * in the bloom filter. If the membership test succeeds, there is a small probability that
 * the key isn't in the set, too.
 *
 * After n inserted object, a bloom filter with k hash functions and m bits of RAM
 * returns with a probability of (1-(1- 1 )kn)k m a false positive answer. This means that the
 * bloom filter states that the key is member of the set, but is is actually not the case.
 *
 * Bloom filters are developed by Bloom and published in "B. H. Bloom. Space/time trade-offs
 * in hash coding with allowable errors. Communications of the ACM, 1970.".
 *
 * \ingroup filterchain
 */
class BloomSet {
    private:
        /**
         * Bloom filter data
         */
        uint32_t* data_;

        /**
         * Size of the bloom filter in 32-bit words
         */
        uint64_t size_;

        /**
         * Number of hash functions to use
         */
        uint8_t k_;
        
        ReadWriteLock lock_;

        void Hash(uint32_t* results, const void* key, size_t key_size);
    public:
        /**
         * Constructor
         * @param size
         * @param hash_count
         * @return
         */
        BloomSet(uint32_t size, uint8_t hash_count);

        /**
         * Creates a bloom filter that given the capacity and the error rate
         * optimizes size and hash functions
         */
        static BloomSet* NewOptimizedBloomSet(uint64_t capacity, double error_rate);

        /**
         * Inits the bloom set
         * @return true iff ok, otherwise an error has occurred
         */
        bool Init();

        /**
         * Destructor of the bloom filter.
         * @return
         */
        ~BloomSet();

        /**
         * Checks if the given key is in the bloom set. By definition
         * of a bloom set, a LOOKUP_FOUND only means that it is
         * possible that the key is in the bloom set.
         *
         * @param key
         * @param key_size
         * @return
         */
        lookup_result Contains(const void* key, size_t key_size);

        /**
         * Puts a key into the bloom set
         * @param key
         * @param key_size
         * @return true iff ok, otherwise an error has occurred
         */
        bool Put(const void* key, size_t key_size);

        /**
         * Clears the bloom filter.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Clear();

        /**
         * returns the size of the bloom set in bits
         * @return
         */
        inline uint64_t size() const;

        /**
         * returns the size of the bloom set in bytes
         * @return
         */
        inline uint64_t byte_size() const;

        /**
         * returns the size of the bloom set in words.
         * A word here means 32-bit words.
         */
        inline uint64_t word_size() const;

        /**
         * returns the number of hash functions used
         * by this bloom set
         * @return
         */
        inline uint8_t hash_count() const;

        /**
         * returns the underlying data.
         * @return
         */
        inline const byte* data() const;

        /**
         * returns a mutable pointer to the underlying data.
         * This method is usually used to load the bloom set data from
         * persistent storage.
         *
         * @return
         */
        inline byte* mutable_data();
};

byte* BloomSet::mutable_data() {
    return reinterpret_cast<byte*>(data_);
}

const byte* BloomSet::data() const {
    return reinterpret_cast<const byte*>(data_);
}

uint64_t BloomSet::size() const {
    return size_;
}

uint64_t BloomSet::byte_size() const {
    return size() / sizeof(byte);
}

uint64_t BloomSet::word_size() const {
    return size() / sizeof(uint32_t);
}

uint8_t BloomSet::hash_count() const {
    return k_;
}

}
}

#endif /* BLOOM_SET_H_ */
