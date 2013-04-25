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
#include "chunker_test.h"

#include <core/chunker.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/adler32.h>
#include <json/json.h>

#include <string>
#include <vector>

#ifndef NVALGRIND
#include <valgrind.h>
#endif

using std::string;
using std::vector;
using std::list;
using dedupv1::base::strutil::Split;
using dedupv1::base::AdlerChecksum;

LOGGER("ChunkerTest");

namespace dedupv1 {

void ChunkerTest::SetUp() {
    config = GetParam();

    chunker = CreateChunker(config);
    ASSERT_TRUE(chunker) << "Failed to create chunker";
}

void ChunkerTest::TearDown() {
    if (chunker) {
        ASSERT_TRUE(chunker->Close());
    }
}

Chunker* ChunkerTest::CreateChunker(string config_option) {
    vector<string> options;
    CHECK_RETURN(Split(config_option, ";", &options), NULL, "Failed to split: " << config_option);

    Chunker* chunker = Chunker::Factory().Create(options[0]);
    CHECK_RETURN(chunker, NULL, "Failed to create chunker type: " << options[0]);

    for (size_t i = 1; i < options.size(); i++) {
        string option_name;
        string option;
        CHECK_RETURN(Split(options[i], "=", &option_name, &option), NULL, "Failed to split " << options[i]);
        CHECK_RETURN(chunker->SetOption(option_name, option), NULL, "Failed set option: " << options[i]);
    }
    return chunker;
}

TEST_P(ChunkerTest, Create) {
}

TEST_P(ChunkerTest, PrintLockStatistics) {
    string s = chunker->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_P(ChunkerTest, PrintStatistics) {
    string s = chunker->PrintStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_P(ChunkerTest, PrintProfile) {
    string s = chunker->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_P(ChunkerTest, ZeroDataChunking) {
    ASSERT_TRUE(chunker->Start());

    size_t data_size = 8 * 1024 * 1024;
    byte* data = new byte[data_size];
    ASSERT_TRUE(data);
    memset(data, 0, data_size);

    AdlerChecksum checksum;
    checksum.Update(data, data_size);

    ChunkerSession* session = chunker->CreateSession();
    ASSERT_TRUE(session);

    list<Chunk*> chunks;
    size_t pos = 0;
    while (pos < data_size) {
        size_t size = 256 * 1024;
        if (data_size - pos < size) {
            size = data_size - pos;
        }
        bool last_call = (pos + size == data_size);
        ASSERT_TRUE(session->ChunkData(data + pos, 0, size, last_call, &chunks));
        pos += size;
    }
    ASSERT_TRUE(session->Close());
    session = NULL;

    AdlerChecksum checksum2;
    uint32_t size_sum = 0;
    list<Chunk*>::iterator ci;
    for (ci = chunks.begin(); ci != chunks.end(); ci++) {
        Chunk* chunk = *ci;
        ASSERT_TRUE(chunk);
        TRACE("Checksum chunk: " << chunk << ", size " << chunk->size());
        checksum2.Update(chunk->data(), chunk->size());
        size_sum += chunk->size();
        delete *ci;
    }
    chunks.clear();

    ASSERT_EQ(data_size, size_sum) << "Size mismatch";
    ASSERT_EQ(checksum.checksum(), checksum2.checksum()) << "Checksum mismatch";

    delete[] data;
}

TEST_P(ChunkerTest, BasicChunking) {
    ASSERT_TRUE(chunker->Start());

    size_t data_size = 8 * 1024 * 1024;
    byte* data = new byte[data_size];
    ASSERT_TRUE(data);
    FILE* file = fopen("/dev/urandom","r");
    ASSERT_TRUE(file);
    ASSERT_EQ(data_size, fread(data, sizeof(byte), data_size, file));

    AdlerChecksum checksum;
    checksum.Update(data, data_size);

    ChunkerSession* session = chunker->CreateSession();
    ASSERT_TRUE(session);

    list<Chunk*> chunks;
    size_t pos = 0;
    while (pos < data_size) {
        size_t size = 256 * 1024;
        if (data_size - pos < size) {
            size = data_size - pos;
        }
        bool last_call = (pos + size == data_size);
        EXPECT_TRUE(session->ChunkData(data + pos, 0, size, last_call, &chunks));
        pos += size;
    }
    EXPECT_TRUE(session->Close());
    session = NULL;

    AdlerChecksum checksum2;
    uint32_t size_sum = 0;
    list<Chunk*>::iterator ci;
    for (ci = chunks.begin(); ci != chunks.end(); ci++) {
        Chunk* chunk = *ci;
        ASSERT_TRUE(chunk);
        TRACE("Checksum chunk: " << chunk << ", size " << chunk->size());
        checksum2.Update(chunk->data(), chunk->size());
        size_sum += chunk->size();

        delete *ci;
    }
    chunks.clear();

    ASSERT_EQ(data_size, size_sum) << "Size mismatch";
    ASSERT_EQ(checksum.checksum(), checksum2.checksum()) << "Checksum mismatch";

    fclose(file);
    delete[] data;
}

}
