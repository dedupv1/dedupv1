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

#ifndef CHUNK_MAPPING_H__
#define CHUNK_MAPPING_H__

#include <string>

#include <core/dedup.h>
#include <core/fingerprinter.h>
#include <core/chunk.h>
#include "dedupv1.pb.h"

namespace dedupv1 {
namespace chunkindex {

/**
 * Mapping from the fingerprint to the data chunk.
 *
 * The default copy constructor and assignment is ok.
 */
class ChunkMapping {
    private:
        /**
         * Fingerprint of the chunk
         */
        byte fp_[dedupv1::Fingerprinter::kMaxFingerprintSize];

        /**
         * Current size of the chunk fingerprint
         */
        size_t fp_size_;

        /**
         * set to true if the chunk is known.
         * It is used to check if the chunk was a known or a new chunk after the
         * data address is set. This value is interesting after the data address of a
         * new chunk is set
         */
        bool known_chunk_;

        /**
         * Data address of the chunk.
         * Set to Storage::ILLEGAL_DATA_ADDRESS if chunk is not in chunk store.
         */
        uint64_t data_address_;

        /**
         * Number of references to the chunk.
         * The value is usually stale as the garbage collector updates the usage counter in a lazy
         * fashion.
         * The usage count may be negative in outrun situations. See the unit test
         * GarbageCollectorIntegrationTest.OutrunnedBlockMapping for an example
         */
        int64_t usage_count_;

        /**
         * Event log id of the last change of the usage count
         */
        uint64_t usage_count_change_log_id_;

        /**
         * Event log id of the last change of usage count because of an invert for a failed write
         */
        uint64_t usage_count_failed_write_change_log_id_;

        /**
         * Reference to the chunk with the data.
         * Usually not set (NULL).
         */
        const Chunk* chunk_;

        /**
         * The block hint is an optional value that stores the block id of
         * the last block that used the chunk.
         *
         * This value is used by the BLC caching system.
         */
        uint64_t block_hint_;

        /**
         * true iff the block hint is set.
         */
        bool has_block_hint_;

        bool is_indexed_;

    public:
        /**
         * Constructor.
         * @return
         */
        ChunkMapping();

        /**
         * Constructor that should be usually used.
         *
         * @param fp
         * @param fp_size
         * @return
         */
        ChunkMapping(const byte* fp, size_t fp_size);

        /**
         * Constructor from a fingerprint string.
         * @param fp
         * @return
         */
        explicit ChunkMapping(const bytestring& fp);

        /**
         * Inits the chunk mapping from a chunk
         * @param chunk
         * @return true iff ok, otherwise an error has occurred
         */
        bool Init(const Chunk* chunk);

        /**
         * Serialize to an ChunkMappingData like used in persistent Index.
         *
         * @param data
         * @return true iff ok, otherwise an error has occurred
         */
        bool SerializeTo(ChunkMappingData* data) const;

        /**
         * Unserialize the chunk mapping from serialized data.
         *
         * @param data
         * @param preserve_chunk
         * @return true iff ok, otherwise an error has occurred
         */
        bool UnserializeFrom(const ChunkMappingData& data, bool preserve_chunk);

        /**
         * returns a mutable fingerprint
         * @return
         */
        inline byte* mutable_fingerprint();

        /**
         * returns the fingerprint
         * @return
         */
        inline const byte* fingerprint() const;

        /**
         * returns the fingerprint size
         * @return
         */
        inline size_t fingerprint_size() const;

        /**
         * return a mutable pointer to the fingerprint size
         * @return
         */
        inline size_t* mutable_fingerprint_size();

        /**
         * Returns a fingerprint as byte packed in a string.
         * The operation currently involves copying the fingerprint
         * multiple times.
         *
         * @return
         */
        inline const bytestring fingerprint_string() const;

        /**
         * returns true if the chunk is known.
         * @return
         */
        inline bool is_known_chunk() const;

        /**
         * sets the "known" state
         * @param t
         */
        inline ChunkMapping& set_known_chunk(bool t);

        /**
         * returns the current data address of the chunk
         * @return
         */
        inline uint64_t data_address() const;

        /**
         * sets the data address
         * @param data_address
         */
        inline ChunkMapping& set_data_address(uint64_t data_address);

        /**
         * returns the usage counter.
         * May be stale as the usage counter is update lazyly.
         * @return
         */
        inline int64_t usage_count() const;

        /**
         * sets the usage count
         * @param usage_count
         */
        inline ChunkMapping& set_usage_count(int64_t usage_count);

        /**
         * sets the fingerprint
         * @param fp
         * @return
         */
        inline bool set_fingerprint(const bytestring& fp);

        /**
         * sets the fingerprint size
         * @param fp_size
         */
        inline ChunkMapping& set_fingerprint_size(uint32_t fp_size);

        /**
         * returns a developer-readable representation of the chunk mapping.
         * @return
         */
        std::string DebugString() const;

        /**
         * returns the chunk
         * @return
         */
        inline const Chunk* chunk() const;

        /**
         * sets the chunk.
         * The caller is responsible that the chunk is not referenced by the chunk mapping
         * after the chunk has been released.
         *
         * @param chunk
         */
        inline ChunkMapping& set_chunk(Chunk* chunk);

        /**
         * returns the event log id if the last change of usage count because of an invert for a failed write
         * @return
         */
        inline uint64_t usage_count_change_log_id() const;

        /**
         * sets the event log id if the last change of usage count because of an invert for a failed write
         * @param log_id
         * @return
         */
        inline ChunkMapping& set_usage_count_change_log_id(uint64_t log_id);

        /**
         * returns the event log id of the last change of usage count because of an invert for a failed write
         * @return
         */
        inline uint64_t usage_count_failed_write_change_log_id() const;

        /**
         * sets the event log id of the last change of usage count because of an invert for a failed write
         * @param log_id
         * @return
         */
        inline ChunkMapping& set_usage_count_failed_write_change_log_id(uint64_t log_id);

        /**
         * returns true iff the block hint is set.
         */
        inline bool has_block_hint() const;

        /**
         * sets the block hint
         */
        inline void set_block_hint(uint64_t block_hint);

        /**
         * clears the block hint.
         * has_block_hint() is false afterwards.
         */
        inline void clear_block_hint();

        /**
         * Returns the block hint.
         * Assumes that has_block_hint is true.
         */
        inline uint64_t block_hint() const;

        inline void set_indexed(bool b);

        inline bool is_indexed() const;
};

void ChunkMapping::set_indexed(bool b) {
  is_indexed_ = b;
}

bool ChunkMapping::is_indexed() const {
  return is_indexed_;
}

bool ChunkMapping::has_block_hint() const {
    return has_block_hint_;
}

void ChunkMapping::set_block_hint(uint64_t block_hint) {
    has_block_hint_ = true;
    block_hint_ = block_hint;
}

void ChunkMapping::clear_block_hint() {
    has_block_hint_ = false;
    block_hint_ = -1;
}

uint64_t ChunkMapping::block_hint() const {
    return block_hint_;
}

bool ChunkMapping::set_fingerprint(const bytestring& fp) {
    if (fp.size() > dedupv1::Fingerprinter::kMaxFingerprintSize) {
        return false;
    }
    memcpy(this->fp_, fp.data(), fp.size());
    this->fp_size_ = fp.size();
    return true;
}

uint64_t ChunkMapping::usage_count_change_log_id() const {
    return this->usage_count_change_log_id_;
}

ChunkMapping& ChunkMapping::set_usage_count_change_log_id(uint64_t log_id) {
    this->usage_count_change_log_id_ = log_id;
    return *this;
}

uint64_t ChunkMapping::usage_count_failed_write_change_log_id() const {
    return this->usage_count_failed_write_change_log_id_;
}

ChunkMapping& ChunkMapping::set_usage_count_failed_write_change_log_id(uint64_t log_id) {
    this->usage_count_failed_write_change_log_id_ = log_id;
    return *this;
}

byte* ChunkMapping::mutable_fingerprint() {
    return this->fp_;
}

const byte* ChunkMapping::fingerprint() const {
    return fp_;
}

size_t ChunkMapping::fingerprint_size() const {
    return fp_size_;
}

size_t* ChunkMapping::mutable_fingerprint_size() {
    return &fp_size_;
}

const bytestring ChunkMapping::fingerprint_string() const {
    bytestring s;
    s.assign(fp_, fp_size_);
    return s;
}

bool ChunkMapping::is_known_chunk() const {
    return known_chunk_;
}

ChunkMapping& ChunkMapping::set_known_chunk(bool t) {
    this->known_chunk_ = t;
    return *this;
}

uint64_t ChunkMapping::data_address() const {
    return data_address_;
}

ChunkMapping& ChunkMapping::set_data_address(uint64_t data_address) {
    this->data_address_ = data_address;
    return *this;
}

ChunkMapping& ChunkMapping::set_usage_count(int64_t usage_count) {
    this->usage_count_ = usage_count;
    return *this;
}

ChunkMapping& ChunkMapping::set_fingerprint_size(uint32_t fp_size) {
    this->fp_size_ = fp_size;
    return *this;
}

int64_t ChunkMapping::usage_count() const {
    return usage_count_;
}

const Chunk* ChunkMapping::chunk() const {
    return chunk_;
}

ChunkMapping& ChunkMapping::set_chunk(Chunk* chunk) {
    this->chunk_ = chunk;
    return *this;
}

}
}

#endif  // CHUNK_MAPPING_H__
