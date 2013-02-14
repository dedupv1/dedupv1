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

#include <core/fingerprinter.h>
#include <core/crypto_fingerprinter.h>
#include <core/dedup.h>
#include <base/logging.h>
#include <base/timer.h>
#include <base/strutil.h>

#include <sstream>

#include <cryptopp/cryptlib.h>
#include <cryptopp/sha.h>

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/md5.h>

using CryptoPP::HashTransformation;
using std::stringstream;
using std::string;
using dedupv1::base::ProfileTimer;

LOGGER("CryptoFingerprinter");

namespace dedupv1 {

CryptoFingerprinter::CryptoFingerprinter(void* hash) {
    this->hash_ = hash;
}

CryptoFingerprinter::~CryptoFingerprinter() {
    if (hash_) {
        delete static_cast<HashTransformation*>(hash_);
    }
}

void CryptoFingerprinter::RegisterFingerprinter() {
    Fingerprinter::Factory().Register("sha1", &CryptoFingerprinter::CreateSHA1Fingerprinter);
    Fingerprinter::Factory().Register("sha256", &CryptoFingerprinter::CreateSHA256Fingerprinter);
    Fingerprinter::Factory().Register("sha512", &CryptoFingerprinter::CreateSHA512Fingerprinter);
    Fingerprinter::Factory().Register("md5", &CryptoFingerprinter::CreateMD5Fingerprinter);
}

Fingerprinter* CryptoFingerprinter::CreateSHA1Fingerprinter() {
    return new CryptoFingerprinter(new CryptoPP::SHA1());
}

Fingerprinter* CryptoFingerprinter::CreateSHA256Fingerprinter() {
    return new CryptoFingerprinter(new CryptoPP::SHA256());
}

Fingerprinter* CryptoFingerprinter::CreateSHA512Fingerprinter() {
    return new CryptoFingerprinter(new CryptoPP::SHA512());
}

Fingerprinter* CryptoFingerprinter::CreateMD5Fingerprinter() {
    return new CryptoFingerprinter(new CryptoPP::Weak1::MD5());
}

bool CryptoFingerprinter::Fingerprint(const byte* data, size_t size,
                                      unsigned char* fp, size_t* fp_size) {
    DCHECK(this->hash_, "Hash not set");

    *fp_size = static_cast<HashTransformation*>(this->hash_)->DigestSize();
    ProfileTimer timer(this->profile_);

    static_cast<HashTransformation*>(this->hash_)->CalculateDigest(fp, data, size);
    return true;
}

size_t CryptoFingerprinter::GetFingerprintSize() {
    DCHECK_RETURN(this->hash_, -1, "Hash not set");
    return static_cast<HashTransformation*>(this->hash_)->DigestSize();
}

string CryptoFingerprinter::PrintProfile() {
    stringstream sstr;
    sstr << this->profile_.GetSum() << std::endl;
    return sstr.str();
}

}
