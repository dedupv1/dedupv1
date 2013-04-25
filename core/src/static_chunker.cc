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

LOGGER("StaticChunker");

namespace dedupv1 {

void StaticChunker::RegisterChunker() {
    Chunker::Factory().Register("static", &StaticChunker::CreateChunker);
}

Chunker* StaticChunker::CreateChunker() {
    Chunker* c = new StaticChunker();
    return c;
}

ChunkerSession* StaticChunker::CreateSession() {
    return new StaticChunkerSession(this);
}

StaticChunkerSession::StaticChunkerSession(StaticChunker* chunker) {
    this->chunker = chunker;
    current_chunk = new byte[chunker->avg_chunk_size_];
    if (current_chunk == NULL) {
        WARNING("Alloc for chunk buffer failed");
    }
    current_chunk_pos = 0;
}

StaticChunker::StaticChunker() {
    avg_chunk_size_ = Chunk::kDefaultAvgChunkSize;
}

StaticChunker::~StaticChunker() {
}

bool StaticChunker::Start() {
    DEBUG("Starting static chunker");
    return true;
}

bool StaticChunker::SetOption(const string& name, const string& data) {
    if (name == "avg-chunk-size") {
        CHECK(ToStorageUnit(data).valid(), "Illegal option " << data);
        this->avg_chunk_size_ = ToStorageUnit(data).value();
        CHECK(this->avg_chunk_size_ >= Chunk::kMinChunkSize, "Chunk size too small (min: " << Chunk::kMinChunkSize << ")");
        CHECK(this->avg_chunk_size_ <= Chunk::kMaxChunkSize, "Chunk size too large (max: " << Chunk::kMaxChunkSize << ")");
        return true;
    }
    return Chunker::SetOption(name, data);
}

string StaticChunker::PrintProfile() {
    stringstream sstr;
    sstr << this->profile_.GetSum() << std::endl;
    return sstr.str();
}

bool StaticChunkerSession::AcceptChunk(list<Chunk*>* chunks) {
    CHECK(chunks, "Chunks not set");

    Chunk* c = new Chunk(current_chunk_pos);
    memcpy(c->mutable_data(), current_chunk, current_chunk_pos);

    chunks->push_back(c);
    current_chunk_pos = 0;
    return true;
}

bool StaticChunkerSession::Close() {
    if (current_chunk) {
        delete [] current_chunk;
        current_chunk = NULL;
    }
    return ChunkerSession::Close();
}

unsigned int StaticChunkerSession::open_chunk_position() {
    return current_chunk_pos;
}

bool StaticChunkerSession::GetOpenChunkData(byte* data, unsigned int offset, unsigned int size) {
    if (current_chunk_pos < offset + size) {
        ERROR("Chunk buffer length to short for open data request");
        return false;
    }
    memcpy(data, &current_chunk[offset], size);
    return true;
}

size_t StaticChunker::GetMinChunkSize() {
    return this->avg_chunk_size_;
}

size_t StaticChunker::GetMaxChunkSize() {
    return this->avg_chunk_size_;
}

size_t StaticChunker::GetAvgChunkSize() {
    return this->avg_chunk_size_;
}

bool StaticChunkerSession::ChunkData(
    const byte* data,
    unsigned int request_offset,
    unsigned int size,
    bool last_chunk_call,
    list<Chunk*>* chunks) {
    ProfileTimer timer(this->chunker->profile_);
    unsigned int amount = 0;
    size_t pos = 0;

    request_offset = (request_offset % chunker->avg_chunk_size_);
    byte* byte_data = (byte *) data;
    while (size > 0) {
        amount = (chunker->avg_chunk_size_ - request_offset); // default case: amount = rest of current chunk
        if (size < amount) { // size to small for amount
            // limit amount
            amount = size;
        }
        // use amount
        memcpy(&current_chunk[current_chunk_pos],
            byte_data + pos, amount);
        current_chunk_pos += amount;

        AcceptChunk(chunks);
        request_offset = 0;

        pos += amount;
        size -= amount;
    }
    if (last_chunk_call && current_chunk_pos > 0) {
        AcceptChunk(chunks);
    }
    return true;
}

bool StaticChunkerSession::Clear() {
    current_chunk_pos = 0;
    return true;
}

}
