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

#ifndef FINGERPRINTER_H__
#define FINGERPRINTER_H__

#include <core/dedup.h>
#include <base/factory.h>

#include <map>
#include <string>

namespace dedupv1 {

/**
 * A fingerprinter is a type that is used to
 * calculate the fingerprint of chunk data.
 *
 * The choice fingerprint has small impact on the performance
 * and the general security of the deduplication.
 *
 * A single fingerprinter should not be used by
 * multiple threads in concurrently.
 */
class Fingerprinter {
    private:
        DISALLOW_COPY_AND_ASSIGN(Fingerprinter);

        /**
         * factory for fingerprinters.
         */
        static MetaFactory<Fingerprinter> factory_;
    public:

        /**
         * returns the fingerprinter factory
         * @return
         */
        static MetaFactory<Fingerprinter>& Factory();

        /**
         * returns the maximal allowed fingerprint size
         */
        static size_t const kMaxFingerprintSize = 64;

        /**
         * denotes the fingerprint of the empty data.
         * The empty data chunk is a special case
         */
        static byte const kEmptyDataFingerprint;

        /**
         * denotes the fingerprint size of the empty data.
         * The empty data chunk is a special case
         */
        static size_t const kEmptyDataFingerprintSize;

        /**
         * Constructor
         * @return
         */
        Fingerprinter();

        /**
         * Destructor
         * @return
         */
        virtual ~Fingerprinter();

        /**
         * Calculates the fingerprint of the given data.
         *
         * @param data data to calculate a fingerprint for
         * @param size size of the data byte buffer
         * @param fp byte buffer to hold the resulting fingerprint
         * @param fp_size Pointer to the size of the fp buffer. Set to the actual size of the fingerprint as result.
         */
        virtual bool Fingerprint(const byte* data, size_t size, byte* fp, size_t* fp_size) = 0;

        /**
         * Returns the fingerprint size in bytes.
         * @return
         */
        virtual size_t GetFingerprintSize() = 0;

        virtual std::string PrintLockStatistics();
        virtual std::string PrintProfile();
        virtual std::string PrintStatistics();

        static std::string DebugString(const void* fp, size_t fp_size);
        static std::string DebugString(const bytestring& fp);
        static std::string DebugString(const std::string& fp);

        /**
         * generate a fingerprint bytestring from the debug string
         * of a fingerprint. It is the inverse function of
         * DebugString
         * @param debug_string
         * @param fp
         * @return
         */
        static bool FromDebugString(const std::string& debug_string, bytestring* fp);

        /**
         * Checks if two fingerprint are equal
         * @param fp1
         * @param fp1_size
         * @param fp2
         * @param fp2_size
         * @return
         */
        inline static bool Equals(const byte* fp1, size_t fp1_size, const byte* fp2, size_t fp2_size);

        static bool SetEmptyDataFingerprint(byte* fp, size_t* fp_size);
        static bool IsEmptyDataFingerprint(const byte* fp, size_t fp_size);
        static bool IsEmptyDataFingerprint(const bytestring& fp);
};

inline bool Fingerprinter::Equals(const byte* fp1, size_t fp1_size, const byte* fp2, size_t fp2_size) {
    if (fp1_size != fp2_size) {
        return false;
    }
    return memcmp(fp1, fp2, fp1_size) == 0;
}

}

#endif  // FINGERPRINTER_H__
