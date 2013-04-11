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
 * @file crc32.h
 * CRC-32 checksum
 */
#ifndef __DEDUPV1_CRC32_H__ // NOLINT
#define __DEDUPV1_CRC32_H__ // NOLINT
#include <base/base.h>
#include <base/profile.h>
#include <base/logging.h>

#include <string>
#ifdef CRYPTO_CRC
#include <cryptopp/cryptlib.h>
#include <cryptopp/crc.h>
#else
#include <crcutil/interface.h>
#endif
namespace dedupv1 {
namespace base {

/**
 * Implementation of the 32-bit Cyclic-Redundancy-Check (CRC-32).
 *
 * @sa http://en.wikipedia.org/wiki/Cyclic_redundancy_check
 */
class CRC {
    public:
        /**
         * Constructor.
         * @return
         */
        inline CRC();

        /**
         * Destructor
         * @return
         */
        inline ~CRC();

        /**
         * Updates the hash value with the given data.
         * Note that it should be equivalent for the final value
         * if a data block is updated with a single call or
         * split up into multiple update calls.
         *
         * @param data
         * @param data_size
         * @return true iff ok, otherwise an error has occurred
         */
        inline bool Update(const void* data, size_t data_size);

        /**
         * Returns the CRC32 hash value as formatted
         * string.
         * TODO (dmeister): Returning an empty string is not a good solution for error handling. However
         * as long a crc_size between 9 and 99 is used there is no reason to fail
         * @param crc_size
         * @return crc string or an empty string if an error occurred
         */
        std::string GetValue(size_t crc_size = kStdSize);

        /**
         * Returns the raw CRC32 value.
         * @return
         */
        inline uint32_t GetRawValue();

        /**
         * Rest the CRC to calculate a new one.
         */
        inline void Reset();

        /**
         * Standard size of the crc string
         */
        static const size_t kStdSize = 8;

        /**
         * Minimal size of the crc string
         */
        static const size_t kMinSize = 8;

        /**
         * maximal size of the crc string
         */
        static const size_t kMaxSize = 99;

        DISALLOW_COPY_AND_ASSIGN(CRC);
    private:
#ifdef CRYPTO_CRC
        /**
         * Instance of cryptopp::CRC32, but we want to avoid
         * exporting cryptopp here.
         */
        CryptoPP::CRC32 crc_gen;
#else
        /**
         * Instance of crcutil implementation of crc
         */
        static crcutil_interface::CRC* crc_gen2;

        /**
         * Actual value of the calculated crc
         */
        crcutil_interface::UINT64 crc_value;
#endif

        /**
         * Logger to use by the crc32 class
         */
        static LOGGER_CLASS logger_;

};

#ifndef CRYPTO_CRC
class StaticCRCHolder {
    public:
        StaticCRCHolder() {
            crc = crcutil_interface::CRC::Create(0xEB31D82E, 0, 32, true, 0x1111, 0, 4,
                    crcutil_interface::CRC::IsSSE42Available(), NULL);
        }

        ~StaticCRCHolder() {
            crc->Delete();
        }

        crcutil_interface::CRC* getCRC() {
            return crc;
        }

    private:
        crcutil_interface::CRC* crc;
};
#endif

/**
 * Short function that calculates the CRC-32 value of the given
 * data.
 *
 * @param value
 * @param value_size
 * @param crc_size
 * @return
 */
inline std::string crc(const void* value, size_t value_size, size_t crc_size = dedupv1::base::CRC::kStdSize);

inline uint32_t crc_raw(const void* value, size_t value_size);

CRC::CRC() {
#ifndef CRYPTO_CRC
    crc_value = 0; // This is crc("")
#endif
}

CRC::~CRC() {
}

bool CRC::Update(const void* data, size_t data_size) {
#ifdef NDEBUG
    if(!data) {
        ERROR_LOGGER(CRC::logger_, "Data not set");
        return false;
    }
#endif
#ifdef CRYPTO_CRC
    crc_gen.Update(static_cast<const byte*>(data), data_size);
#else
    crc_gen2->Compute(data, data_size, &crc_value);
#endif
    return true;
}

uint32_t CRC::GetRawValue() {
#ifdef CRYPTO_CRC
    uint32_t crc_value;
    crc_gen.Final(reinterpret_cast<byte*>(&crc_value));
    return crc_value;
#else
    return static_cast<uint64_t>(crc_value);
#endif
}

void CRC::Reset() {
#ifdef CRYPTO_CRC
    crc_gen.Restart();
#else
    crc_value = 0;
#endif
}

std::string crc(const void* value, size_t value_size, size_t crc_size) {
    CRC crc_gen;
    crc_gen.Update(value, value_size);
    return crc_gen.GetValue(crc_size);
}

uint32_t crc_raw(const void* value, size_t value_size) {
    CRC crc_gen;
    crc_gen.Update(value, value_size);
    return crc_gen.GetRawValue();
}

}
}

#endif  // __DEDUPV1_CRC32_H__ NOLINT
