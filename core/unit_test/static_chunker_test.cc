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

#include <core/static_chunker.h>
#include <core/chunker.h>
#include <core/chunk_mapping.h>
#include <base/resource_management.h>
#include <core/chunk.h>
#include <test_util/log_assert.h>

#include <gtest/gtest.h>
#include <stdio.h>

#include "chunker_test.h"

using std::list;

namespace dedupv1 {
namespace contentstorage {

class StaticChunkerTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Chunker* chunker;

    virtual void SetUp() {
        chunker = Chunker::Factory().Create("static");
        ASSERT_TRUE(chunker);
        ASSERT_TRUE(chunker->Start());
    }

    virtual void TearDown() {
        if (chunker) {
            delete chunker;
            chunker = NULL;
        }
    }
};

INSTANTIATE_TEST_CASE_P(StaticChunker,
    ChunkerTest,
    ::testing::Values("static"));

TEST_F(StaticChunkerTest, Start) {

}

TEST_F(StaticChunkerTest, Chunk) {
    FILE* file = fopen("data/rabin-test","r");
    ASSERT_TRUE(file);

    list<Chunk*> chunks;

    byte buffer[65000]; // less than 2^16
    fread(buffer, sizeof(byte), 65000, file);

    ChunkerSession* session = chunker->CreateSession();
    ASSERT_TRUE(session);
    ASSERT_TRUE(session->ChunkData(buffer, 0, 65000, true, &chunks));
    delete session;

    int pos = 0;
    ASSERT_EQ(chunks.size(), 8U);

    list<Chunk*>::iterator i;
    size_t j = 0;
    for (i = chunks.begin(); i != chunks.end(); i++, j++) {
        Chunk* chunk = *i;
        ASSERT_TRUE(chunk);
        if (j < chunks.size() - 1) {
            ASSERT_EQ(chunk->size(), 8192U);
            ASSERT_TRUE(memcmp(chunk->data(), buffer + pos, chunk->size()) == 0);

            pos += chunk->size();
        } else {
            ASSERT_EQ(chunk->size(), 8192U - 536U);
            ASSERT_TRUE(memcmp(chunk->data(), buffer + pos, chunk->size()) == 0);
        }
    }
    fclose(file);

    for (i = chunks.begin()++; i != chunks.end(); i++) {
        delete *i;
    }
}

TEST_F(StaticChunkerTest, ChunkWithOffset) {
    FILE* file = fopen("data/rabin-test","r");
    ASSERT_TRUE(file);

    list<Chunk*> chunks;

    byte buffer[65000]; // less than 2^16
    fread(buffer, sizeof(byte), 65000, file);

    ChunkerSession* session = chunker->CreateSession();
    ASSERT_TRUE(session);
    ASSERT_TRUE(session->ChunkData(buffer, 1000, 65000, true, &chunks));
    delete session;

    ASSERT_EQ(chunks.size(), 9U);
    int pos = 0;

    list<Chunk*>::iterator i;
    size_t j = 0;
    for (i = chunks.begin(); i != chunks.end(); i++, j++) {
        Chunk* chunk = *i;
        if (j == 0) {
            ASSERT_EQ(chunk->size(), 8192U - 1000U);
            ASSERT_TRUE(memcmp(chunk->data(), buffer, chunk->size()) == 0);
            pos += chunk->size();
        } else if (j == chunks.size() - 1) {
            ASSERT_EQ(chunk->size(), 8192U - 536 - (8192 - 1000));
            ASSERT_TRUE(memcmp(chunk->data(), buffer + pos, chunk->size()) == 0);
        } else {
            ASSERT_EQ(chunk->size(), 8192U);
            ASSERT_TRUE(memcmp(chunk->data(), buffer + pos, chunk->size()) == 0);

            pos += chunk->size();
        }
    }

    fclose(file);

    for (i = chunks.begin()++; i != chunks.end(); i++) {
        delete *i;
    }
}

}
}
