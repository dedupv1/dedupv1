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

#ifndef BLOCK_MAPPING_H__
#define BLOCK_MAPPING_H__

#include <string>
#include <list>

#include <core/dedup.h>
#include <core/fingerprinter.h>
#include <core/chunk_mapping.h>

#include "dedupv1.pb.h"

#include <gtest/gtest_prod.h>

namespace dedupv1 {
namespace blockindex {

/**
 * Mapping of an block id to a portion of a chunk.
 *
 * Note: Copy constructor and assignment is ok here
 */
class BlockMappingItem {
    private:
        /**
         * Fingerprint of the block mapping item.
         */
        byte fp_[dedupv1::Fingerprinter::kMaxFingerprintSize];

        /**
         * Size of the fingerprint
         */
        size_t fp_size_;

        /**
         * data address (the container id if the container storage is used) if
         * the block mapping item has already an container id assigned.
         * However, an assigned container id does not guarantee that the
         * data is committed.
         */
        uint64_t data_address_;

        /**
         * Offset of the block mapping item data inside the chunk.
         */
        uint32_t chunk_offset_;

        /**
         * Size of the portion of a chunk that is used by this block mapping item.
         * The size is is less or equal to the size of the chunk.
         */
        uint32_t size_;

        /**
         * ????
         */
        bool item_chunk_in_use_;

    public:
        /**
         * Inits a block mapping item.
         *
         * @param chunk_offset Chunk offset of the item. This is not the offset inside the
         * block.
         * @param size Size of the item
         *
         */
        BlockMappingItem(unsigned int chunk_offset, unsigned int size);

        /**
         * Copy the data from the profobuf data structure into
         * the object.
         *
         * @param data
         * @return true iff ok, otherwise an error has occurred
         */
        bool CopyFrom(const BlockMappingItemData& data);

        /**
         * Copy the data from the other item object into the
         * object.
         *
         * We use a CopyFrom method instead a copy constructor to
         * enforce a more explicit coding style.
         *
         * @param item
         * @return true iff ok, otherwise an error has occurred
         */
        bool CopyFrom(const BlockMappingItem& item);

        /**
         * Checks if two block mapping items are equal.
         *
         * We use a Equals method instead of a = operator to
         * enforce a more explicit coding style.
         *
         * @param i2
         * @return true iff the this item and the provide are are the same.
         */
        bool Equals(const BlockMappingItem& i2) const;

        /**
         * Convert a single chunk to a block mapping item. The function
         * copies the fingerprint and sets the storage address.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Convert(const dedupv1::chunkindex::ChunkMapping& chunk_mapping);

        /**
         * Writes the data of a block mapping item into a chunk mapping.
         *
         * @param chunk_mapping
         * @return true iff ok, otherwise an error has occurred
         */
        bool ConvertTo(dedupv1::chunkindex::ChunkMapping* chunk_mapping) const;

        /**
         * Returns the fingerprint data.
         * @return
         */
        inline const byte* fingerprint() const;

        /**
         * Returns a mutable fingerprint.
         * @return
         */
        inline byte* mutable_fingerprint();

        /**
         * Returns a mutable pointer to the fingerprint size.
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
         * Returns the current size of the fingerprint.
         * @return
         */
        inline size_t fingerprint_size() const;

        /**
         * Sets the size of the current fingerprint.
         * @param new_size
         */
        inline void set_fingerprint_size(size_t new_size);

        /**
         * returns the current data address of the block
         * @return
         */
        inline uint64_t data_address() const;

        /**
         * Sets the current data address.
         * @param a
         */
        inline void set_data_address(uint64_t a);

        /**
         * returns the offset of the data area of the block mapping item within the chunk
         * @return
         */
        inline uint32_t chunk_offset() const;

        /**
         * sets the chunk offset.
         * @param co
         */
        inline void set_chunk_offset(uint32_t co);

        /**
         * returns the size of the data area of the block mapping item within the chunk
         * @return
         */
        inline uint32_t size() const;

        /**
         * sets the size
         * @param s
         */
        inline void set_size(uint32_t s);

        /**
         * TODO (dmeister): Needed???
         * @return
         */
        inline bool is_used();

        inline void set_is_used(bool u);

        /**
         * returns a developer-readable representation of the block mapping item.
         * Useful for logging.
         *
         * @return
         */
        std::string DebugString() const;

        friend class BlockMappingTest;
        FRIEND_TEST(BlockMappingTest, Append);
        FRIEND_TEST(BlockMappingTest, CreateEmptyMapping);
};

/**
 * A block mapping stores how the data of a block has been splitted up into
 * chunks and how the data can be reconstructed using chunk data.
 *
 * Note: Copy constructor and assignment is ok here
 */
class BlockMapping {
    public:
        /**
         * Constant that denotes an illegal block id.
         */
        static const uint64_t ILLEGAL_BLOCK_ID = -1;
    private:
        friend class BlockMappingTest;
        FRIEND_TEST(BlockMappingTest, Append);

        /**
         * Block id of the block mapping.
         */
        uint64_t block_id_;

        /**
         * Size of a block
         */
        size_t block_size_;

        /**
         * version counter. Is updated
         * very time the block mapping is changed.
         *
         * The version is usually incremented by 1 each time the block mapping is
         * updated, but if a block write from i to i+1 fails, a later block write would
         * go from i to i+2.
         */
        uint32_t version_counter_;

        /**
         * List of block mapping items that form the block mapping.
         */
        std::list<BlockMappingItem> items_;

        /**
         * checksum for the block to detect errors.
         */
        bytestring checksum_;

        /**
         * Id of the event related to the last persistent change
         * of the block mapping.
         *
         * We need a version and the log id because the log id
         * is not meaningful for the auxiliary index.
         */
        uint64_t event_log_id_;
    public:
        /**
         * Constructor that sets the block id and the block size.
         *
         * @param block_id
         * @param block_size
         * @return
         */
        BlockMapping(uint64_t block_id, size_t block_size);

        /**
         * Constructor that sets the block id to an illegal value.
         *
         * @param block_size
         * @return
         */
        explicit BlockMapping(size_t block_size);

        /**
         * Closes the block mapping.
         */
        ~BlockMapping();

        void Release();

        bool MergePartsFrom(const BlockMapping& src_block_mapping, uint32_t target_offset, uint32_t src_offset, uint32_t size);

        /**
         * Inserts a new block mapping item to a block mapping at a given offset.
         *
         * TODO (dmeister) The name append is not correct. Should be changed in the future.
         *
         * @param offset Offset of the item in the block. While this is not stored in the block mapping directly, it is
         *               important to correctly place the item in the mapping
         * @param item block mapping item to append (NotNull, Checked)
         *
         */
        bool Append(unsigned int offset, const BlockMappingItem& item);

        /**
         * Searches the last free block mapping item and copies the chunk data to it.
         */
        bool Merge(const dedupv1::chunkindex::ChunkMapping& chunk_mapping);

        /**
         * Prints a block mapping in an human readable format.
         */
        std::string DebugString() const;

        /**
         * Performs a simple check if the block mapping data is internally correct.
         * It checks if the sum of the item sizes is equal to the block size.
         * Can be used for debugging purposes.
         */
        bool Check() const;

        /**
         * Serializes the block mapping to a block mapping data message.
         *
         * If setBlockId is not set, the block id is not serialized to the block mapping data.
         * This is usually done, when the block mapping data is used as an index value, and the
         * block id is the key as it is done in the block index.
         *
         * If setChecksum is not set, the checksum will not be stored in the block mapping data
         * (often to safe space). This is usually done when there can be expected that the full checksum
         * has not additional value, e.g. the original block mapping in a block mapping written event.
         *
         * The client of UnserializeFrom has to check in a case by case basis, the checksum should
         * be existing for the given data source. However, all component should work
         * even when the checksum is not set (due to the downward compatibility)
         *
         * @param data data message to serialize to
         * @param setBlockId iff true, the block id is set in the serialized protobuf message
         * @param setChecksum iff true, the checksum of the block mapping is serialized
         * to the protobuf message.
         *
         * @return
         */
        bool SerializeTo(BlockMappingData* data, bool setBlockId, bool setChecksum) const;

        /**
         * Unserializes the block mapping from a block mapping data
         * message.
         *
         * @param data
         * @param checkBlockId
         * @return
         */
        bool UnserializeFrom(const BlockMappingData& data, bool checkBlockId);

        /**
         * TODO (dmeister): Difference between CopyFrom and
         * UnserializeFrom?
         *
         * Copies a block mapping from a source block mapping data
         * message to the current block mapping.
         *
         * @param data
         * @return
         */
        bool CopyFrom(const BlockMappingData& data);

        /**
         * Copies a block mapping from source to dest.
         *
         * @param src Source of the copy.
         */
        bool CopyFrom(const BlockMapping& src);

        /**
         * Tests is to block mapping items are equal.
         *
         * Two block mapping item are considered to be equal if
         * - the data address is the same
         * - the chunk offset is the same
         * - the size is the same.
         * - the fingerprint is the same
         */
        bool Equals(const BlockMapping& m2) const;

        /**
         * Truncates the block mapping at the given offset.
         *
         * @param offset offset to truncate at.
         *
         * TODO (dmeister) the function returns true if offset is larger than the block size. This should be considered a bug.
         */
        bool Cut(unsigned int offset);

        /**
         * sets the checksum value.
         *
         * @param new_checksum
         */
        inline void set_checksum(const bytestring& new_checksum);

        /**
         * returns the checksum if the checksum is set
         * @return
         */
        inline const bytestring& checksum() const;

        /**
         * returns a mutable pointer to the block checksum.
         * @return
         */
        inline bytestring* mutable_checksum();

        /**
        * Returns true if the checksum is set
        * @return
        */
        inline bool has_checksum() const;

        /**
         * Fills to block mapping with an empty mapping that represents a block filled with zeros.
         *
         * @return
         */
        bool FillEmptyBlockMapping();

        /**
         * Returns the block id
         * @return
         */
        inline uint64_t block_id() const;

        /**
         * Returns the block size
         * @return
         */
        inline size_t block_size() const;

        /**
         * Returns the block version.
         * @return
         */
        inline uint32_t version() const;

        /**
         * Sets the version.
         *
         * Usually the version should be set to the next higher value
         * if a block mapping is updated. The version is used
         * to detect an ordering between the block mappings. In
         * conjunction with the block locks, this version
         * ensures a total ordering between block mappings.
         * @param v
         */
        inline void set_version(uint32_t v);

        /**
         * Returns the event log id.
         * The event log id is the event log id after which a block mapping
         * becomes commitable (usually a CONTAINER COMMIT event).
         * It is used during the replay of BLOCK MAPPING DELETED
         * events to see if a currently stored block mapping
         * is situated before the block mapping deleted event
         * or after that.
         *
         * @return
         */
        inline uint64_t event_log_id() const;

        /**
         * sets the event log id.
         * @param log_id
         */
        inline void set_event_log_id(uint64_t log_id);

        /**
         * Returns the item count
         * @return
         */
        inline uint32_t item_count() const;

        /**
         * Returns the items (const version)
         * @return
         */
        inline const std::list<BlockMappingItem>& items() const;

        /**
         * Returns the items
         * @return
         */
        inline std::list<BlockMappingItem>& items();
};

uint64_t BlockMapping::block_id() const {
    return block_id_;
}

size_t BlockMapping::block_size() const {
    return block_size_;
}

uint32_t BlockMapping::version() const {
    return version_counter_;
}

void BlockMapping::set_version(uint32_t v) {
    this->version_counter_ = v;
}

uint64_t BlockMapping::event_log_id() const {
    return event_log_id_;
}

void BlockMapping::set_event_log_id(uint64_t log_id) {
    event_log_id_ = log_id;
}

uint32_t BlockMapping::item_count() const {
    return items_.size();
}

const std::list<BlockMappingItem>& BlockMapping::items() const {
    return items_;
}

std::list<BlockMappingItem>& BlockMapping::items() {
    return items_;
}

const byte* BlockMappingItem::fingerprint() const {
    return fp_;
}

byte* BlockMappingItem::mutable_fingerprint() {
    return fp_;
}

size_t* BlockMappingItem::mutable_fingerprint_size() {
    return &fp_size_;
}

const bytestring BlockMappingItem::fingerprint_string() const {
    bytestring s;
    s.assign(fp_, fp_size_);
    return s;
}

size_t BlockMappingItem::fingerprint_size() const {
    return fp_size_;
}

void BlockMappingItem::set_fingerprint_size(size_t new_size) {
    this->fp_size_ = new_size;
}

uint64_t BlockMappingItem::data_address() const {
    return data_address_;
}

void BlockMappingItem::set_data_address(uint64_t a) {
    this->data_address_ = a;
}

uint32_t BlockMappingItem::chunk_offset() const {
    return chunk_offset_;
}

void BlockMappingItem::set_chunk_offset(uint32_t co) {
    chunk_offset_ = co;
}

uint32_t BlockMappingItem::size() const {
    return size_;
}

void BlockMappingItem::set_size(uint32_t s) {
    size_ = s;
}

bool BlockMappingItem::is_used() {
    return item_chunk_in_use_;
}

void BlockMappingItem::set_is_used(bool u) {
    this->item_chunk_in_use_ = u;
}

const bytestring& BlockMapping::checksum() const {
    return checksum_;
}

bytestring* BlockMapping::mutable_checksum() {
    return &checksum_;
}

bool BlockMapping::has_checksum() const {
    return !checksum_.empty();
}

void BlockMapping::set_checksum(const bytestring& new_crc) {
    this->checksum_ = new_crc;
}

}
}

#endif  // BLOCK_MAPPING_H__
