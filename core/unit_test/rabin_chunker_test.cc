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

#include <core/rabin_chunker.h>
#include <core/chunker.h>
#include <core/chunk_mapping.h>
#include <core/fingerprinter.h>
#include <core/chunk.h>
#include <base/logging.h>
#include <test_util/log_assert.h>

#include "chunker_test.h"

#include <gtest/gtest.h>
#include <stdio.h>

#ifndef NVALGRIND
#include <valgrind.h>
#endif

LOGGER("RabinChunkerTest");

using std::set;
using std::list;
using dedupv1::base::make_bytestring;

namespace dedupv1 {

class RabinChunkerTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    RabinChunker* chunker;

    virtual void SetUp() {
        chunker = dynamic_cast<RabinChunker*>(Chunker::Factory().Create("rabin"));
        ASSERT_TRUE(chunker);
        ASSERT_TRUE(chunker->Start());
    }

    virtual void TearDown() {
        if (chunker) {
            ASSERT_TRUE(chunker->Close());
            chunker = NULL;
        }
    }
};

TEST_F(RabinChunkerTest, ConfigAvgChunkSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    if (chunker) {
        ASSERT_TRUE(chunker->Close());
        chunker = NULL;
    }
    chunker = dynamic_cast<RabinChunker*>(Chunker::Factory().Create("rabin"));
    ASSERT_TRUE(chunker);

    ASSERT_TRUE(chunker->SetOption("avg-chunk-size", "4K"));
    ASSERT_TRUE(chunker->SetOption("avg-chunk-size", "8K"));
    ASSERT_TRUE(chunker->SetOption("avg-chunk-size", "16K"));
    ASSERT_FALSE(chunker->SetOption("avg-chunk-size", "3K"));
}

TEST_F(RabinChunkerTest, WrongMinimalChunkSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    if (chunker) {
        ASSERT_TRUE(chunker->Close());
        chunker = NULL;
    }
    chunker = dynamic_cast<RabinChunker*>(Chunker::Factory().Create("rabin"));
    ASSERT_TRUE(chunker);

    ASSERT_TRUE(chunker->SetOption("avg-chunk-size", "4K"));
    ASSERT_TRUE(chunker->SetOption("min-chunk-size", "8K"));
    ASSERT_FALSE(chunker->Start());
}

TEST_F(RabinChunkerTest, WrongMaximalChunkSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    if (chunker) {
        ASSERT_TRUE(chunker->Close());
        chunker = NULL;
    }
    chunker = dynamic_cast<RabinChunker*>(Chunker::Factory().Create("rabin"));
    ASSERT_TRUE(chunker);

    ASSERT_TRUE(chunker->SetOption("avg-chunk-size", "4K"));
    ASSERT_TRUE(chunker->SetOption("max-chunk-size", "2K"));
    ASSERT_FALSE(chunker->Start());
}

TEST_F(RabinChunkerTest, Create) {
    if (chunker) {
        ASSERT_TRUE(chunker->Close());
        chunker = NULL;
    }
    chunker = dynamic_cast<RabinChunker*>(Chunker::Factory().Create("rabin"));
    ASSERT_TRUE(chunker);
}

TEST_F(RabinChunkerTest, Start) {
}

TEST_F(RabinChunkerTest, ModTable) {
    EXPECT_EQ(chunker->t_[0], 0UL);
    EXPECT_EQ(chunker->t_[2], 9209141382100228870ULL);
    EXPECT_EQ(chunker->t_[100], 4267581128949538325ULL);
    EXPECT_EQ(chunker->t_[255], 14665969062442009581ULL);

    if (HasFailure()) {
        chunker->PrintTables();
    }
}

TEST_F(RabinChunkerTest, InvertTable) {
    EXPECT_EQ(chunker->u_[0], 0UL);
    EXPECT_EQ(chunker->u_[2], 7033709673330278438ULL);
    EXPECT_EQ(chunker->u_[100], 7551225361429087706ULL);
    EXPECT_EQ(chunker->u_[255], 1586646794406570246ULL);

    if (HasFailure()) {
        chunker->PrintTables();
    }
}

TEST_F(RabinChunkerTest, Simple) {
    int i = 0;
    RabinChunkerSession* sess1 =
        dynamic_cast<RabinChunkerSession*>(chunker->CreateSession());
    ASSERT_TRUE(sess1);

    ASSERT_TRUE(sess1->open_chunk_position() == 0);
    ASSERT_TRUE(sess1->fingerprint() == 0);

    for (i = 0; i < 256; i++) {
        sess1->UpdateWindowFingerprint(i);
    }
    uint64_t fp1 = sess1->fingerprint();
    sess1->Close();

    RabinChunkerSession* sess2 =
        dynamic_cast<RabinChunkerSession*>(chunker->CreateSession());
    ASSERT_TRUE(sess2);

    for (i = 0; i < 256; i++) {
        sess2->UpdateWindowFingerprint(i);
    }
    uint64_t fp2 = sess2->fingerprint();
    ASSERT_EQ(fp1, fp2);

    sess2->Close();
}

TEST_F(RabinChunkerTest, Rolling) {

    FILE* file = fopen("/dev/urandom", "r");
    ASSERT_TRUE(file);

    byte buffer[65536];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));
    fclose(file);
    file = NULL;

    RabinChunkerSession* sess1 =
        dynamic_cast<RabinChunkerSession*>(chunker->CreateSession());
    ASSERT_TRUE(sess1);
    ASSERT_TRUE(sess1->open_chunk_position() == 0);
    ASSERT_TRUE(sess1->fingerprint() == 0);

    for (int i = 0; i < 65536; i++) {
        sess1->UpdateWindowFingerprint(buffer[i]);

        if (i > RabinChunker::kDefaultWindowSize) {
            RabinChunkerSession* sess2 =
                dynamic_cast<RabinChunkerSession*>(chunker->CreateSession());
            ASSERT_TRUE(sess2);
            ASSERT_TRUE(sess2->open_chunk_position() == 0);
            ASSERT_TRUE(sess2->fingerprint() == 0);

            for (int k = i - RabinChunker::kDefaultWindowSize + 1; k <= i; k++) {
                sess2->UpdateFingerprint(buffer[k]);
            }
            ASSERT_EQ(sess1->fingerprint(), sess2->fingerprint()) << "Offset " << i;
            sess2->Close();
            sess2 = NULL;
        }

    }
    sess1->Close();
    sess1 = NULL;
}

TEST_F(RabinChunkerTest, Window) {
    int i = 0;
    RabinChunkerSession* sess1 =
        dynamic_cast<RabinChunkerSession*>(chunker->CreateSession());
    ASSERT_TRUE(sess1);

    ASSERT_TRUE(sess1->open_chunk_position() == 0);
    ASSERT_TRUE(sess1->fingerprint() == 0);

    for (i = 0; i < 64; i++) {
        DEBUG("" << i << " - " << sess1->fingerprint());
        sess1->UpdateWindowFingerprint(i);
    }
    uint64_t fp1 = sess1->fingerprint();

    for (i = 0; i < 64; i++) {
        DEBUG("" << i << " - " << sess1->fingerprint());
        sess1->UpdateWindowFingerprint(i);
    }
    uint64_t fp2 = sess1->fingerprint();
    ASSERT_EQ(fp1, fp2);

    sess1->Close();
}

TEST_F(RabinChunkerTest, SwitchingFingerprint) {
    Fingerprinter* fp = Fingerprinter::Factory().Create("sha1");
    ASSERT_TRUE(fp);

    byte buffer[65536];
    for (int i = 0; i < 65536; i++) {
        if (i % 2 == 0) {
            buffer[i] = 7;
        } else {
            buffer[i] = 3;
        }
    }

    ChunkerSession* sess1 = chunker->CreateSession();
    ASSERT_TRUE(sess1);

    list<Chunk*> chunks;
    ASSERT_TRUE(sess1->ChunkData(buffer, 0, 65536, true, &chunks));
    ASSERT_TRUE(sess1->Close());
    EXPECT_EQ(2, chunks.size());

    byte digest_session[20];
    set<bytestring> fps;
    for (list<Chunk*>::iterator i = chunks.begin(); i != chunks.end(); i++) {
        size_t fp_size = 20;
        Chunk* chunk = *i;
        EXPECT_TRUE(chunk);
        EXPECT_TRUE(fp->Fingerprint(chunk->data(), chunk->size(), digest_session, &fp_size));
        DEBUG("Chunk: " << Fingerprinter::DebugString(digest_session, 20));
        fps.insert(make_bytestring(digest_session, 20));
    }
    EXPECT_EQ(1, fps.size());
    for (list<Chunk*>::iterator i = chunks.begin(); i != chunks.end(); i++) {
        delete *i;
    }
    EXPECT_TRUE(fp->Close());
    fp = NULL;
}

TEST_F(RabinChunkerTest, Fingerprint) {
    Fingerprinter* fp = Fingerprinter::Factory().Create("sha1");
    ASSERT_TRUE(fp);

    FILE* file = fopen("data/rabin-test", "r");
    ASSERT_TRUE(file);
    ASSERT_EQ(0, ferror(file));

    list<Chunk*> chunks;

    byte buffer1[65536];
    ASSERT_EQ(65536, fread(buffer1, sizeof(byte), 65536, file));

    ChunkerSession* sess1 = chunker->CreateSession();
    ASSERT_TRUE(sess1);
    sess1->ChunkData(buffer1, 0, 65536, true, &chunks);
    sess1->Close();

    byte digest_session1[20];
    for (list<Chunk*>::iterator i = chunks.begin(); i != chunks.end(); i++) {
        size_t fp_size = 20;
        Chunk* chunk = *i;
        ASSERT_TRUE(chunk);
        fp->Fingerprint(chunk->data(), chunk->size(), digest_session1, &fp_size);
    }
    for (list<Chunk*>::iterator i = chunks.begin(); i != chunks.end(); i++) {
        delete *i;
    }
    chunks.clear();

    byte buffer2[65536];
    memset(buffer2, 0, 65536);
    memcpy(buffer2 + 16, buffer1 + 16, 65536 - 16);

    ChunkerSession* sess2 = chunker->CreateSession();
    ASSERT_TRUE(sess2);
    sess2->ChunkData(buffer2, 0, 65536, true, &chunks);
    sess2->Close();

    byte digest_session2[20];
    for (list<Chunk*>::iterator i = chunks.begin(); i != chunks.end(); i++) {
        size_t fp_size = 20;
        Chunk* chunk = *i;
        ASSERT_TRUE(chunk);
        fp->Fingerprint(chunk->data(), chunk->size(), digest_session2, &fp_size);
    }

    ASSERT_TRUE(memcmp(digest_session1, digest_session2, 20) == 0);

    fclose(file);

    for (list<Chunk*>::iterator i = chunks.begin(); i != chunks.end(); i++) {
        delete *i;

    }
    fp->Close();
}

TEST_F(RabinChunkerTest, Performance) {
#ifndef NVALGRIND
    if (unlikely(RUNNING_ON_VALGRIND)) {
        INFO("Skip this test because valgrind will would take too long...");
        return;
    }
#endif
    size_t data_size = 128 * 1024 * 1024;
    int repeat_count = 64;
    byte* data = new byte[data_size];
    ASSERT_TRUE(data);
    FILE* file = fopen("/dev/urandom","r");
    ASSERT_TRUE(file);
    ASSERT_EQ(65536, fread(data, sizeof(byte), 64 * 1024, file));

    size_t pos = 0;
    while (pos < data_size) {
        size_t size = 64 * 1024;
        if (data_size - pos < size) {
            size = data_size - pos;
        }
        memcpy(data + pos, data, size);
        pos += size;
    }

    list<Chunk*> chunks;

    tbb::tick_count start_time = tbb::tick_count::now();

    for (int i = 0; i < repeat_count; i++) {
        INFO("Repeat " << i);
        ChunkerSession* session = chunker->CreateSession();
        ASSERT_TRUE(session);

        pos = 0;
        while (pos < data_size) {
            size_t size = 256 * 1024;
            if (data_size - pos < size) {
                size = data_size - pos;
                ASSERT_TRUE(session->ChunkData(data + pos, 0, size, true, &chunks));
            } else {
                ASSERT_TRUE(session->ChunkData(data + pos, 0, size, false, &chunks));
            }
            pos += size;
        }
        ASSERT_TRUE(session->Close());
        session = NULL;

        list<Chunk*>::iterator ci;
        for (ci = chunks.begin(); ci != chunks.end(); ci++) {
            delete *ci;
        }
        chunks.clear();
    }

    tbb::tick_count end_time = tbb::tick_count::now();
    double diff = (end_time - start_time).seconds();
    double mbs = (repeat_count * data_size / (1024 * 1024)) / diff;
    INFO("Chunking Performance: " << mbs << " MB/s, time " << diff << " s");

#ifdef NDEBUG
    ASSERT_GE(mbs, 120.0);
#endif
    fclose(file);
    delete[] data;
}

INSTANTIATE_TEST_CASE_P(RabinChunker,
    ChunkerTest,
    ::testing::Values("rabin",
        "rabin;avg-chunk-size=4K;min-chunk-size=1K;max-chunk-size=16K",
        "rabin;avg-chunk-size=16K;min-chunk-size=4K;max-chunk-size=64K"));
}
