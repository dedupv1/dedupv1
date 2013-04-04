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

#include "null_chunker.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include <core/chunker.h>
#include <base/strutil.h>
#include <base/resource_management.h>
#include <core/chunk.h>
#include <base/logging.h>
#include <base/timer.h>

#include <sstream>

using std::string;
using std::stringstream;
using std::list;
using dedupv1::base::ProfileTimer;
using dedupv1::base::ResourceManagement;
using dedupv1::base::strutil::ToStorageUnit;

LOGGER("NullChunker");

namespace dedupv1 {

void NullChunker::RegisterChunker() {
    Chunker::Factory().Register("null-chunker", &NullChunker::CreateChunker);
}

Chunker* NullChunker::CreateChunker() {
    Chunker* c = new NullChunker();
    return c;
}

ChunkerSession* NullChunker::CreateSession() {
    return new NullChunkerSession();
}

NullChunkerSession::NullChunkerSession() {
}

NullChunker::NullChunker() {
}

NullChunker::~NullChunker() {
}

bool NullChunker::Start(ResourceManagement<Chunk>* cmc) {
    return true;
}

unsigned int NullChunkerSession::open_chunk_position() {
    return 0;
}

bool NullChunkerSession::GetOpenChunkData(byte* data, unsigned int offset, unsigned int size) {
    return true;
}

size_t NullChunker::GetMinChunkSize() {
    return 0;
}

size_t NullChunker::GetMaxChunkSize() {
    return 0;
}

size_t NullChunker::GetAvgChunkSize() {
    return 0;
}

bool NullChunkerSession::ChunkData(
    const byte* data, unsigned int request_offset,
    unsigned int size,
    bool last_chunk_call,
    list<Chunk*>* chunks) {
    return true;
}

bool NullChunkerSession::Clear() {
    return true;
}

}

