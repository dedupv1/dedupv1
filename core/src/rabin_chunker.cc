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
#include <base/base.h>
#include <base/logging.h>
#include <core/chunker.h>
#include <base/strutil.h>
#include <base/resource_management.h>
#include <core/chunk.h>
#include <base/bitutil.h>
#include <base/timer.h>
#include <base/crc32.h>
#include <core/fingerprinter.h>
#include "dedupv1_stats.pb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <stdint.h>
#include <sstream>

LOGGER("RabinChunker");

using std::string;
using std::stringstream;
using std::list;
using dedupv1::base::ProfileTimer;
using dedupv1::base::ResourceManagement;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::To;
using dedupv1::base::crc;
using CryptoPP::PolynomialMod2;

namespace dedupv1 {

namespace {

PolynomialMod2 MakePolynomialMod2(uint64_t l) {
    uint64_t l2 = htobe64(l);
    PolynomialMod2 p = PolynomialMod2(reinterpret_cast<const byte*>(&l2), sizeof(l));
    return p;
}

/**
 * Convert a polynomial to a 64-bit long value
 */
uint64_t ToLong(const PolynomialMod2& polynomial) {
    uint64_t l;
    polynomial.Encode(reinterpret_cast<byte*>(&l), sizeof(l));
    return l; // still big-endian
}

}

const PolynomialMod2 RabinChunker::kMostSignificantBit = PolynomialMod2::One() << 63;
const uint64_t RabinChunker::kDefaultPolynom = 9479294086943401663ULL; // &0xbfe6b8a5bf378d83LL;

void RabinChunker::RegisterChunker() {
    Chunker::Factory().Register("rabin", &RabinChunker::CreateChunker);
}

Chunker* RabinChunker::CreateChunker() {
    Chunker* c = new RabinChunker();
    return c;
}

ChunkerSession* RabinChunker::CreateSession() {
    RabinChunkerSession* s = new RabinChunkerSession(this);
    CHECK_RETURN(s, NULL, "Failed to alloc rabin chunker session");

    if (!s->IsValid()) {
        if (!s->Close()) {
            WARNING("Failed to close rabin chunker session");
        }
        return NULL;
    }
    return s;
}

bool RabinChunkerSession::IsValid() {
    return this->overflow_chunk_data_ && this->window_buffer_;
}

bool RabinChunkerSession::Close() {
    bool result = true;

    if (this->overflow_chunk_data_) {
        delete[] overflow_chunk_data_;
        overflow_chunk_data_ = NULL;
    }
    if (window_buffer_) {
        delete[] window_buffer_;
        window_buffer_ = NULL;
    }
    if (!ChunkerSession::Close()) {
        ERROR("Failed to close chunker session");
        result = false;
    }
    return result;
}

RabinChunkerSession::RabinChunkerSession(RabinChunker* chunker) {
    this->chunker_ = chunker;
    this->window_buffer_ = NULL;
    this->overflow_chunk_data_ = NULL;
    fingerprint_ = 0; /* current fingerprint */

    /*
     * cyclic buffer, needed to remove the oldest element of the buffer when a new element is processes.
     * Initially set to 0, simulated WINDOW_SIZE "0" values at the beginning
     */
    this->window_buffer_ = new byte[chunker->window_size_]; /* released in rabin_session_close */
    if (!window_buffer_) {
        WARNING("Alloc window buffer failed");
    } else {
        memset(window_buffer_, 0, chunker->window_size_);
        window_buffer_pos_ = -1;
    }
    /*
     * current chunk
     * the chunk cannot be larger than max_chunk bytes
     */
    overflow_chunk_data_ = new byte[chunker->max_chunk_];
    overflow_chunk_data_pos_ = 0;
}

RabinChunker::RabinChunker() {
    avg_chunk_ = Chunk::kDefaultAvgChunkSize; /* = 2^13 */
    min_chunk_ = RabinChunker::kDefaultMinChunkSize;
    max_chunk_ = RabinChunker::kDefaultMaxChunkSize;
    poly_ = PolynomialMod2(reinterpret_cast<const byte*>(&kDefaultPolynom), sizeof(kDefaultPolynom)); /* inreducable polynom */
    window_size_ = RabinChunker::kDefaultWindowSize; /* size of the sliding window */

    stats_.chunks_ = 0;
    stats_.size_forced_chunks_ = 0;
    stats_.close_forced_chunks_ = 0;
    memset(t_, 0, 256);
    memset(u_, 0, 256);
    breakmark_ = 0;
}

RabinChunker::Statistics::Statistics() {
    chunks_ = 0;
    size_forced_chunks_ = 0;
    close_forced_chunks_ = 0;
}

RabinChunker::~RabinChunker() {
}

bool RabinChunker::Start() {
    CHECK(this->min_chunk_ <= avg_chunk_, "Minimal chunk size larger than average chunk size");
    CHECK(this->avg_chunk_ <= max_chunk_, "Average chunk size larger than maximal chunk size");

    CHECK(poly_.IsIrreducible(), "Polynom is not irreducible:" << ToLong(poly_));

    unsigned int breakmark_bits = dedupv1::base::bits(avg_chunk_);
    breakmark_ = (unsigned long long) (pow(2.0, breakmark_bits) - 1); // breakmark mask to split chunks
    position_window_before_min_size_ = min_chunk_ - window_size_;
    CalculateModTable();
    CalculateInvertTable();
    return true;
}

void RabinChunker::PrintTables() {
    int i = 0;
    for (i = 0; i < 256; i++) {
        DEBUG("T[" << i << "d] = " << t_[i]);
    }
    for (i = 0; i < 256; i++) {
        DEBUG("U[" << i << "] = " << u_[i]);
    }
}

bool RabinChunker::SetOption(const string& name, const string& data) {
    CHECK(name.size() > 0, "Option name not set");
    CHECK(data.size() > 0, "Option value not set");

    if (name == "avg-chunk-size") {
        CHECK(ToStorageUnit(data).valid(), "Illegal option " << data);
        this->avg_chunk_ = ToStorageUnit(data).value();

        // enforce that chunk size has a value of 2^i
        int b = dedupv1::base::bits(this->avg_chunk_);

        CHECK(this->avg_chunk_ == pow(2, b), "Average chunk size must be a power of 2, e.g. 1024, 2048, ...");
        CHECK(this->avg_chunk_ >= Chunk::kMinChunkSize, "Chunk size too small (min: " << Chunk::kMinChunkSize << ")");
        CHECK(this->avg_chunk_ <= Chunk::kMaxChunkSize, "Chunk size too large (max: " << Chunk::kMaxChunkSize << ")");
        return true;
    } else if (name == "min-chunk-size") {
        CHECK(ToStorageUnit(data).valid(), "Illegal option " << data);
        this->min_chunk_ = ToStorageUnit(data).value();
        CHECK(this->min_chunk_ >= Chunk::kMinChunkSize, "Chunk size too small (min: " << Chunk::kMinChunkSize << ")");
        CHECK(this->min_chunk_ <= Chunk::kMaxChunkSize, "Chunk size too large (max: " << Chunk::kMaxChunkSize << ")");
        return true;
    } else if (name == "max-chunk-size") {
        CHECK(ToStorageUnit(data).valid(), "Illegal option " << data);
        this->max_chunk_ = ToStorageUnit(data).value();
        CHECK(this->max_chunk_ >= Chunk::kMinChunkSize, "Chunk size too small (min: " << Chunk::kMinChunkSize << ")");
        CHECK(this->max_chunk_ <= Chunk::kMaxChunkSize, "Chunk size too large (max: " << Chunk::kMaxChunkSize << ")");
        return true;
    } else if (name == "window-size") {
        CHECK(To<uint32_t>(data).valid(), "Illegal option " << data);
        this->window_size_ = To<uint32_t>(data).value();
        CHECK(this->max_chunk_ <= 128, "Window size too large");
        return true;
    } else if (name == "polynom") {
        string hex_polynom = data;
        bytestring polynom;
        CHECK(Fingerprinter::FromDebugString(hex_polynom, &polynom),
            "Failed to parse polynom: " << hex_polynom);
        CHECK(polynom.size() == sizeof(uint64_t), "Polynom has not 64-bit");

        PolynomialMod2 new_p = PolynomialMod2(polynom.data(), polynom.size());
        CHECK(new_p.IsIrreducible(), "Polynom is irreducible");
        poly_ = new_p;
    }
    return Chunker::SetOption(name, data);
}

void RabinChunker::CalculateModTable() {
    PolynomialMod2 T1 = kMostSignificantBit.Modulo(poly_);
    // for each possible byte
    for (int i = 0; i < 256; i++) {
        PolynomialMod2 j = MakePolynomialMod2(i);
        t_[i] = be64toh(ToLong((j * T1).Modulo(poly_)) | ToLong(j << 63));
    }
}

void RabinChunker::CalculateInvertTable() {
    uint64_t sizeshift = 1;
    for (int i = 1; i < window_size_; i++) {
        sizeshift = FingerprintAppendByte(sizeshift, 0);
    }
    PolynomialMod2 sizeshift_poly = MakePolynomialMod2(sizeshift);
    for (int i = 0; i < 256; i++) {
        PolynomialMod2 j = MakePolynomialMod2(i);
        u_[i] = be64toh(ToLong((j * sizeshift_poly).Modulo(poly_)));
    }
}

uint64_t RabinChunker::FingerprintAppendByte(uint64_t old_fingerprint, byte m) {
    return ((old_fingerprint << 8) | m) ^ t_[old_fingerprint >> kShift];
}

bool RabinChunker::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    RabinChunkerStatsData data;
    data.set_chunk_count(this->stats_.chunks_);
    data.set_size_forced_chunk_count(this->stats_.size_forced_chunks_);
    data.set_close_forced_chunk_count(this->stats_.close_forced_chunks_);
    CHECK(ps->Persist(prefix, data), "Failed to persist chunker stats");
    return true;
}

bool RabinChunker::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    RabinChunkerStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore chunker stats");
    this->stats_.chunks_ = data.chunk_count();
    this->stats_.size_forced_chunks_ = data.size_forced_chunk_count();
    this->stats_.close_forced_chunks_ = data.close_forced_chunk_count();
    return true;
}

string RabinChunker::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"chunks\": " << this->stats_.chunks_ << "," << std::endl;
    sstr << "\"forced chunks (size)\": " << this->stats_.size_forced_chunks_ << "," << std::endl;
    sstr << "\"forced chunks (close)\": " << this->stats_.close_forced_chunks_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string RabinChunker::PrintProfile() {
    stringstream sstr;
    sstr << this->stats_.time_.GetSum() << std::endl;
    return sstr.str();
}

bool RabinChunkerSession::AcceptChunk(list<Chunk*>* chunks, const byte* data, uint32_t size) {
    DCHECK(this->overflow_chunk_data_, "Current chunk not set");
    DCHECK(chunks, "chunks result not set");

    Chunk* c = new Chunk(overflow_chunk_data_pos_ + size);
    CHECK(c, "Chunk not acquired");
    DCHECK(c->data(), "Chunk data not set");
    DCHECK(this->overflow_chunk_data_pos_ + size > 0,
        "Current chunk has size zero: overflow chunk size " << overflow_chunk_data_pos_ <<
        ", size " << size);
    TRACE("Accept chunk: total size " << (overflow_chunk_data_pos_ + size) <<
        ", overflow chunk size " << overflow_chunk_data_pos_ <<
        ", direct size " << size);

    // The chunk data consists of the data from previous calls (overflow data) + the current data upto the offset "size"
    if (unlikely(overflow_chunk_data_pos_ > 0)) {
        memcpy(c->mutable_data(), this->overflow_chunk_data_, overflow_chunk_data_pos_);
    }
    if (likely(size > 0)) {
        memcpy(c->mutable_data() + overflow_chunk_data_pos_, data, size);
    }
    chunks->push_back(c);

    // reset
    this->overflow_chunk_data_pos_ = 0;
    this->fingerprint_ = 0;
    this->window_buffer_pos_ = -1;
    memset(window_buffer_, 0, chunker_->window_size_);
    this->chunker_->stats_.chunks_++;
    return true;
}

unsigned int RabinChunkerSession::open_chunk_position() {
    return overflow_chunk_data_pos_;
}

bool RabinChunkerSession::GetOpenChunkData(byte* data, unsigned int offset, unsigned int size) {
    if (overflow_chunk_data_pos_ < offset + size) {
        ERROR("Chunk buffer length to short for open data request");
        return false;
    }
    memcpy(data, &overflow_chunk_data_[offset], size);
    return true;
}

bool RabinChunkerSession::ChunkData(const byte* data,
                                    unsigned int request_offset,
                                    unsigned int size,
                                    bool last_chunk_call,
                                    list<Chunk*>* chunks) {
    // Note: The code in this method is (for dedupv1) highly optimized. Please benchmark it after changing it
    ProfileTimer timer(this->chunker_->stats_.time_);

    DCHECK(data, "Data not set");
    DCHECK(chunks, "Chunks not set");

    TRACE("Chunk data: data " << static_cast<const void*>(data) << ", request offset " << request_offset << ", size " << size);

    // for each position in the data stream block
    register const byte* current = data; // current data pointer, at the beginning at the beginning of the data set
    register const byte* chunk_data_end = data + size; // pointer denoting the end of the current data set
    register const byte* non_chunked_data = data; // pointer to the beginning of the area not already assigned to a finished chunk

    // until we reached the end
    while (unlikely(current < chunk_data_end)) {
        // we determine how much data should be processed maximally
        uint32_t todo = (chunk_data_end - current);
        unsigned int count_to_max = chunker_->max_chunk_ - overflow_chunk_data_pos_;
        if (likely(todo > count_to_max)) {
            // todo is higher than difference to the maximal chunk size. Only chunk until that size is reached
            todo = count_to_max;
        }

        // Check how much data should be skipped until the minimal chunk size is reached
        // There is no need to calculate rabin fingerprint the data before the minimal chunk size
        int32_t count_to_min = chunker_->position_window_before_min_size_ - overflow_chunk_data_pos_;
        if (unlikely(count_to_min < 0)) {
            count_to_min = 0;
        } else if (unlikely(count_to_min > todo)) {
            // even if the process all the data available we are not able to produce a chunk larger than the minimal size, skip it
            break;
        }

        TRACE("Total size " << size <<
            ", bytes to end " << static_cast<int>(chunk_data_end - current) <<
            ", todo size " << todo <<
            ", overflow chunk size " << overflow_chunk_data_pos_ <<
            ", count to max " << count_to_max <<
            ", count to min " << count_to_min);

        register const byte* end = current + todo; // end of current processing block
        current += count_to_min; // skip these

        for (; current < end; current++) {
            UpdateWindowFingerprint(*current);
            // breakmark is suffix of fingerprint and current chunk is larger then the minimal size
            register bool is_breakmark = (this->fingerprint_ & chunker_->breakmark_) == chunker_->breakmark_;
            if (unlikely(is_breakmark)) {
                current++; // move to next value, break is not moving current
                CHECK(AcceptChunk(chunks, non_chunked_data, current - non_chunked_data),
                    "Failed to accept chunk: reason breakmark");
                non_chunked_data = current;
                break;
            }
        }
        // here either the complete data is processed or the maximal size of a chunk is reached
        TRACE("Reached end inner loop: size " << size << ", todo size " << todo << ", count to max " << count_to_max);
        if (unlikely(current == end) && likely(current != non_chunked_data) && unlikely(todo == count_to_max)) {
            this->chunker_->stats_.size_forced_chunks_++;
            CHECK(AcceptChunk(chunks, non_chunked_data, current - non_chunked_data),
                "Failed to accept chunk: reason size" <<
                ", count to max " << count_to_max <<
                ", non chunked size " << static_cast<uint64_t>(current - non_chunked_data));
            non_chunked_data = current;
        }
    }

    // copy the non-processed data to the overflow buffer
    int overflow_data_len = static_cast<int>(chunk_data_end - non_chunked_data);
    if (overflow_data_len > 0) {
        TRACE("Add " << static_cast<int>(chunk_data_end - non_chunked_data) << " bytes to overflow chunk buffer");
        memcpy(overflow_chunk_data_ + overflow_chunk_data_pos_, non_chunked_data, overflow_data_len);
        overflow_chunk_data_pos_ += overflow_data_len;
    } else {
        TRACE("No data copied to overflow chunk buffer");
    }
    TRACE("Last call " << last_chunk_call << ", overflow chunk data pos " << overflow_chunk_data_pos_);
    if (last_chunk_call && this->overflow_chunk_data_pos_ > 0) {
        this->chunker_->stats_.close_forced_chunks_++;
        CHECK(AcceptChunk(chunks, NULL, 0), "Failed to accept chunk: reason chunking end");
    }
    return true;
}

RabinChunkerSession::~RabinChunkerSession() {
}

void RabinChunkerSession::UpdateFingerprint(byte c) {
    this->fingerprint_ = chunker_->FingerprintAppendByte(this->fingerprint_, c);
} // /

bool RabinChunkerSession::Clear() {
    fingerprint_ = 0; // current fingerprint
    window_buffer_pos_ = -1;
    overflow_chunk_data_pos_ = 0;
    return true;
}

size_t RabinChunker::GetMinChunkSize() {
    return this->min_chunk_;
}

size_t RabinChunker::GetMaxChunkSize() {
    return this->max_chunk_;
}

size_t RabinChunker::GetAvgChunkSize() {
    return this->avg_chunk_;
}

}
