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

#ifndef CHUNKER_MOCK_H_
#define CHUNKER_MOCK_H_

#include <gmock/gmock.h>
#include <core/chunker.h>

class MockChunker : public dedupv1::Chunker {
public:
    MOCK_METHOD0(CreateSession, dedupv1::ChunkerSession*());
    MOCK_METHOD0(Start, bool());

    MOCK_METHOD0(GetMinChunkSize, size_t());
    MOCK_METHOD0(GetMaxChunkSize, size_t());
    MOCK_METHOD0(GetAvgChunkSize, size_t());

    MOCK_METHOD0(Close, bool());
};

class MockChunkerSession : public dedupv1::ChunkerSession {
  public:
  MOCK_METHOD0(Close, bool());

  MOCK_METHOD5(ChunkData, bool(const byte* data,
        unsigned int offset,
        unsigned int size,
        bool last_chunk_call,
        std::list<dedupv1::Chunk*>* chunks));

  MOCK_METHOD0(open_chunk_position, uint32_t());

  MOCK_METHOD3(GetOpenChunkData, bool(byte* data,
        unsigned int offset,
        unsigned int size));
};

#endif /* CHUNKER_MOCK_H_ */
