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
 * @file adler32.h
 * Adler-32 checksum
 */

#ifndef __DEDUPV1_ADLER32_H_
#define __DEDUPV1_ADLER32_H_

#include <base/base.h>
#include <base/profile.h>

#include <string>
#include <cryptopp/cryptlib.h>
#include <cryptopp/adler32.h>

namespace dedupv1 {
namespace base {

/**
 * Implementation of the 32-bit Adler checksum.
 *
 * Usually Adler-32 is faster than CRC32, but it has a weakness for short message
 * with a few hundred bytes.
 *
 * @sa http://www.cryptopp.com/benchmarks.html
 * @sa http://en.wikipedia.org/wiki/Adler-32
 */
class AdlerChecksum {
    private:
        /**
         * Instance of the Cryptopp implementation
         */
        CryptoPP::Adler32 adler32_;
    public:
        /**
         * Constructor.
         * @return
         */
        inline AdlerChecksum();

        /**
         * Updates the hash value with the given data.
         * Note that it should be equivalent for the final value
         * if a data block is updated with a single call or
         * split up into multiple update calls.
         *
         * @param data
         * @param data_size
         */
        inline void Update(const void* data, size_t data_size);

        /**
         * Returns the adler32 digest of the given data
         * TOOD (dmeister): Should be static?
         */
        inline uint32_t Digest(const void* data, size_t data_size);

        /**
         * Returns the raw checksum value.
         * @return
         */
        inline uint32_t checksum();

        DISALLOW_COPY_AND_ASSIGN(AdlerChecksum);
};

AdlerChecksum::AdlerChecksum() {
}

uint32_t AdlerChecksum::Digest(const void* data, size_t data_size) {
    uint32_t checksum;
    adler32_.CalculateDigest(reinterpret_cast<byte*>(&checksum), static_cast<const byte*>(data), data_size);
    return checksum;
}

void AdlerChecksum::Update(const void* data, size_t data_size) {
    adler32_.Update(static_cast<const byte*>(data), data_size);
}

uint32_t AdlerChecksum::checksum() {
    uint32_t checksum;
    adler32_.Final(reinterpret_cast<byte*>(&checksum));
    return checksum;
}

}
}

#endif /* __DEDUPV1_ADLER32_H_ */
