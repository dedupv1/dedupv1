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

#include <core/dedup.h>

#include <core/fingerprinter.h>
#include <base/timer.h>
#include <base/logging.h>

#include <iostream>
#include <iomanip>
#include <string.h>
#include <stdio.h>

LOGGER("Fingerprinter");

using std::string;

namespace dedupv1 {

MetaFactory<Fingerprinter> Fingerprinter::factory_("Fingerprinter", "fingerprinter");

MetaFactory<Fingerprinter>& Fingerprinter::Factory() {
    return factory_;
}

byte const Fingerprinter::kEmptyDataFingerprint = static_cast<byte>(2);
size_t const Fingerprinter::kEmptyDataFingerprintSize = 1;

bool Fingerprinter::SetEmptyDataFingerprint(byte* fp, size_t* fp_size) {
    DCHECK(fp, "fp not set");
    DCHECK(fp_size, "fp size not set");

    *fp_size = kEmptyDataFingerprintSize;
    fp[0] = kEmptyDataFingerprint;
    return true;
}

bool Fingerprinter::IsEmptyDataFingerprint(const byte* fp, size_t fp_size) {
    DCHECK(fp, "fp not set");

    if (fp_size != kEmptyDataFingerprintSize) {
        return false;
    }
    if (fp[0] != kEmptyDataFingerprint) {
        return false;
    }
    return true;
}

bool Fingerprinter::IsEmptyDataFingerprint(const bytestring& fp) {
    if (fp.size() != kEmptyDataFingerprintSize) {
        return false;
    }
    if (fp[0] != kEmptyDataFingerprint) {
        return false;
    }
    return true;
}

Fingerprinter::Fingerprinter() {
}

Fingerprinter::~Fingerprinter() {
}

std::string Fingerprinter::DebugString(const void* fp, size_t fp_size) {
    CHECK_RETURN(fp, "ERROR", "fp not set");
    std::stringstream s;
    const byte* ba = static_cast<const byte*>(fp);
    unsigned int i = 0;
    for (i = 0; i < fp_size; i++) {
        char buf[10];
        snprintf(buf, sizeof(buf), "%02x", ba[i]);
        s << buf;
    }
    std::string value;
    s >> value;
    return value;
}

bool Fingerprinter::FromDebugString(const string& debug_string, bytestring* fp) {
    CHECK(fp, "fp not set");

    CHECK(debug_string.size() % 2 == 0, "Illegal fingerprint string");
    char buf[3];
    buf[2] = 0;

    for (size_t i = 0; i < debug_string.size(); i += 2) {
        buf[0] = debug_string[i];
        buf[1] = debug_string[i + 1];
        unsigned int v;
        sscanf(buf, "%02x", &v);
        char c = v;
        fp->append(reinterpret_cast<byte*>(&c), 1);
    }
    return true;
}

string Fingerprinter::DebugString(const bytestring& fp) {
    return Fingerprinter::DebugString(fp.data(), fp.size());
}

string Fingerprinter::DebugString(const string& fp) {
    return Fingerprinter::DebugString((const byte *) fp.c_str(), fp.size());
}

std::string Fingerprinter::PrintLockStatistics() {
    return "null";
}

std::string Fingerprinter::PrintProfile() {
    return "null";
}

std::string Fingerprinter::PrintStatistics() {
    return "null";
}

}
