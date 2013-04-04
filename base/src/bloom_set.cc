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

#include <base/bloom_set.h>

#include <base/hashing_util.h>
#include <base/logging.h>
#include <base/bitutil.h>

#include <cmath>

LOGGER("BloomSet");

namespace dedupv1 {
namespace base {

BloomSet::BloomSet(uint32_t size, uint8_t hash_count) {
    data_ = NULL;
    size_ = size;
    k_ = hash_count;
}

BloomSet* BloomSet::NewOptimizedBloomSet(uint64_t capacity, double error_rate) {
    CHECK_RETURN(capacity > 0, NULL, "Illegal capacity");
    DCHECK_RETURN(error_rate > 0.0 && error_rate < 1.0, NULL, "Illegal error rate");
    // auto set up
    uint32_t hashes = (uint32_t) std::ceil(dedupv1::base::log2(1 / error_rate));
    uint32_t bits_per_hash = (uint32_t) std::ceil(
        (2 * capacity * std::abs(std::log(error_rate))) / (hashes * (std::pow(std::log(2), 2))));
    uint32_t bits = bits_per_hash * hashes;
    if ((bits % 32) != 0) {
        bits = 32 * ((bits / 32) + 1);
    }
    return new BloomSet(bits, hashes);
}

bool BloomSet::Init() {
    CHECK(data_ == NULL, "Bloom set already inited");
    CHECK(size_ % 32 == 0, "Illegal size: " << size_);
    CHECK(hash_count() > 0, "Illegal hash count " << hash_count());

    this->data_ = new uint32_t[word_size()];
    CHECK(this->data_, "Bloom filter allocation failed");
    memset(this->data_, 0, byte_size());

    return true;
}

BloomSet::~BloomSet() {
    if (data_) {
        delete[] data_;
        data_ = NULL;
    }
}

void BloomSet::Hash(uint32_t* results, const void* key, size_t key_size) {
    // We use Murmur hash as described in http://spyced.blogspot.com/2009/01/all-you-ever-wanted-to-know-about.html

    uint32_t* r = results;
    for (int i = 0; i < this->k_; i += 4) {
        murmur_hash3_x64_128(key, key_size, i, r);
        r += 4;
    }
    for (int i = 0; i < this->k_; i++) {
        results[i] %= size_;
    }
}

lookup_result BloomSet::Contains(const void* key, size_t key_size) {
    DCHECK_RETURN(data_ != NULL, LOOKUP_ERROR, "Bloom set not inited");
    DCHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    uint32_t hashes[k_ + 4];
    Hash(hashes, key, key_size);

    CHECK_RETURN(lock_.AcquireReadLock(), LOOKUP_ERROR, "Failed to acquire read lock");
    lookup_result r = LOOKUP_FOUND;
    for (int i = 0; i < this->k_; i++) {
        uint32_t hash = hashes[i];
        bool bittest = bit_test(this->data_[hash / 32], hash % 32);
        if (!bittest) {
            r = LOOKUP_NOT_FOUND;
            break;
        }
    }
    CHECK_RETURN(lock_.ReleaseLock(), LOOKUP_ERROR, "Failed to release read lock");
    return r;
}

bool BloomSet::Put(const void* key, size_t key_size) {
    DCHECK(data_ != NULL, "Bloom set not inited");
    DCHECK(key, "Key not set");

    uint32_t hashes[k_ + 4];
    Hash(hashes, key, key_size);

    CHECK_RETURN(lock_.AcquireWriteLock(), LOOKUP_ERROR, "Failed to acquire read lock");

    for (int i = 0; i < this->k_; i++) {
        uint32_t hash = hashes[i];
        size_t index = hash / 32;
        uint32_t* p = &(this->data_[index]);
        bit_set(p, hash % 32);
    }

    CHECK_RETURN(lock_.ReleaseLock(), LOOKUP_ERROR, "Failed to release read lock");
    return true;
}

bool BloomSet::Clear() {
    CHECK(data_ != NULL, "Bloom set not inited");
    CHECK_RETURN(lock_.AcquireWriteLock(), LOOKUP_ERROR, "Failed to acquire read lock");
    memset(this->data_, 0, byte_size());
    CHECK_RETURN(lock_.ReleaseLock(), LOOKUP_ERROR, "Failed to release read lock");
    return true;
}

}
}
