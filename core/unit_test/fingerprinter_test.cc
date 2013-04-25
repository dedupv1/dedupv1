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

#include <gtest/gtest.h>
#include "fingerprinter_test.h"

#include <base/index.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <core/rabin_chunker.h>
#include <json/json.h>

#include <cryptopp/cryptlib.h>
#include <cryptopp/rng.h>

#include <string>
#include <vector>

using std::string;
using std::vector;
using namespace dedupv1;
using namespace CryptoPP;

LOGGER("FingerprinterTest");

namespace dedupv1 {

void FingerprinterTest::SetUp() {
    config = GetParam();

    fingerprinter = CreateFingerprinter(config);
    ASSERT_TRUE(fingerprinter) << "Failed to create fingerprinter";

    LC_RNG rng(1024);

    buffer_size = 1024 * 1024;
    buffer = new byte[buffer_size];
    rng.GenerateBlock(buffer, buffer_size);
}

void FingerprinterTest::TearDown() {
    if (buffer) {
        delete[] buffer;
    }
    if (fingerprinter) {
        delete fingerprinter;
    }
}

Fingerprinter* FingerprinterTest::CreateFingerprinter(string config_option) {
    Fingerprinter* fingerprinter = Fingerprinter::Factory().Create(config_option);
    CHECK_RETURN(fingerprinter, NULL, "Failed to create fingerprinter type: " << config_option);
    return fingerprinter;
}

TEST_P(FingerprinterTest, Create) {
}

TEST_P(FingerprinterTest, DigestFull) {
    byte fp1[Fingerprinter::kMaxFingerprintSize];
    byte fp2[Fingerprinter::kMaxFingerprintSize];

    size_t fp_size = Fingerprinter::kMaxFingerprintSize;

    ASSERT_TRUE(fingerprinter->Fingerprint(buffer, buffer_size, fp1, &fp_size));
    ASSERT_EQ(fp_size, fingerprinter->GetFingerprintSize());

    ASSERT_TRUE(fingerprinter->Fingerprint(buffer, buffer_size, fp2, &fp_size));
    ASSERT_EQ(fp_size, fingerprinter->GetFingerprintSize());

    ASSERT_TRUE(memcmp(fp1, fp2, fingerprinter->GetFingerprintSize()) == 0) << "Fingerprints for same data should not differ";
}

TEST_P(FingerprinterTest, PrintLockStatistics) {
    string s = fingerprinter->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_P(FingerprinterTest, PrintStatistics) {
    string s = fingerprinter->PrintStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_P(FingerprinterTest, PrintProfile) {
    string s = fingerprinter->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_P(FingerprinterTest, EmptyFingerprint) {
    byte* b = new byte[RabinChunker::kDefaultMaxChunkSize];
    memset(b, 0, RabinChunker::kDefaultMaxChunkSize);

    byte fp[fingerprinter->GetFingerprintSize()];
    size_t fp_size = fingerprinter->GetFingerprintSize();
    ASSERT_TRUE(fingerprinter->Fingerprint(b, RabinChunker::kDefaultMaxChunkSize, fp, &fp_size));

    DEBUG(Fingerprinter::DebugString(fp, fingerprinter->GetFingerprintSize()));

    delete[] b;
}

TEST(FingerprinterTest, DebugString) {
    byte fp[20];
    for (int i = 0; i < 20; i++) {
        fp[i] = i;
    }
    string fp_str = Fingerprinter::DebugString(fp, 20);
    bytestring new_fp;
    ASSERT_TRUE(Fingerprinter::FromDebugString(fp_str, &new_fp));
    ASSERT_EQ(new_fp.size(), 20);
}

}
