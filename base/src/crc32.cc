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
#include <cryptopp/cryptlib.h>
#include <cryptopp/crc.h>

#include <base/crc32.h>
#include <base/base.h>
#include <base/logging.h>

using CryptoPP::CRC32;
using std::string;

LOGGER("CRC32");

namespace dedupv1 {
namespace base {

LOGGER_CLASS CRC::logger_ = GET_LOGGER("CRC");

#ifndef CRYPTO_CRC
static StaticCRCHolder staticCRCHolder;
crcutil_interface::CRC* CRC::crc_gen2 = staticCRCHolder.getCRC();
#endif

string CRC::GetValue(size_t crc_size) {
    // Note: This function is based on work by
    // Eric Durbin, Kentucky Cancer Registry, University of Kentucky, October 14, 1998
    // published under public domain.

    uint32_t crc_calc_value; // value of CRC checksum for record
    uint32_t mask = 0x0000000FL; // Mask for converting hex string TODO (dmeister) Make it a constant
    const char *hex_string = "0123456789ABCDEF"; // TODO (dmeister) Make it a constant
    char buffer[16];

    // validate checksum length
    CHECK_RETURN(crc_size >= CRC::kMinSize, "", "Too small checksum");
    CHECK_RETURN(crc_size <= CRC::kMaxSize, "", "Too large checksum");

    crc_calc_value = GetRawValue();
    string s;
    // convert CRC Value to ASCII Hex String
    while (crc_size--) {
        snprintf(buffer, sizeof(buffer), "%c", *(hex_string + (crc_calc_value & mask)));
        s += buffer;
        crc_calc_value >>= 4; // right shift 4 bits
    }
    return s;
}

}
}
