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

#ifndef BLOCK_MAPPING_PAIR_H__
#define BLOCK_MAPPING_PAIR_H__

#include <string>
#include <list>

#include <core/dedup.h>
#include <core/fingerprinter.h>
#include <core/block_mapping.h>

#include "dedupv1.pb.h"

namespace dedupv1 {
namespace blockindex {

/**
 * Mapping of an block id to a portion of a chunk.
 *
 * Note: Copy constructor and assignment is ok here
 */
class BlockMappingPairItem {
    private:
        /**
         * Fingerprint of the block mapping pair item.
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

        int32_t usage_count_modifier_;

    public:
        /**
         * Inits a block mapping pair item.
         */
        BlockMappingPairItem();

        /**
         * Copy the data from the profobuf data structure into
         * the object.
         *
         * @param data
         * @return true iff ok, otherwise an error has occurred
         */
        bool CopyFrom(const BlockMappingPairItemData& data);

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
        bool CopyFrom(const BlockMappingPairItem& item);

        /**
         * Checks if two block mappingpair  items are equal.
         *
         * We use a Equals method instead of a = operator to
         * enforce a more explicit coding style.
         *
         * @param i2
         * @return true iff the this item and the provide are are the same.
         */
        bool Equals(const BlockMappingPairItem& i2) const;

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

        inline int32_t usage_count_modifier() const {
            return usage_count_modifier_;
        }

        inline void set_usage_count_modifier(int32_t ucm) {
            usage_count_modifier_ = ucm;
        }

        /**
         * returns a developer-readable representation of the block mapping pair item.
         * Useful for logging.
         *
         * @return
         */
        std::string DebugString() const;
};

/**
 * A block mapping pair stores how the data of a block has been splitted up into
 * chunks and how the data can be reconstructed using chunk data.
 *
 * Note: Copy constructor and assignment is ok here
 */
class BlockMappingPair {
    private:

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
        std::list<BlockMappingPairItem> items_;

    public:

        /**
         * Constructor that sets the block id to an illegal value.
         *
         * @param block_size
         * @return
         */
        explicit BlockMappingPair(size_t block_size);

        /**
         * Destructor
         */
        ~BlockMappingPair();

        bool CopyFrom(const BlockMapping& original_block_mapping, const BlockMapping& modified_block_mapping);

        /**
         * Prints a block mapping in an human readable format.
         */
        std::string DebugString() const;


        /**
         * Serializes the block mapping pair to a block mapping pair data message.
         * @return
         */
        bool SerializeTo(BlockMappingPairData* data) const;

        /**
         * Copies a block mapping pair from a source block mapping pair data
         * message to the current block mapping pair.
         *
         * @param data
         * @return
         */
        bool CopyFrom(const BlockMappingPairData& data);

        /**
         * Copies a block mapping pair from source to dest.
         *
         * @param src Source of the copy.
         */
        bool CopyFrom(const BlockMappingPair& src);

        BlockMapping GetModifiedBlockMapping(uint64_t event_log_id) const;

        std::map<bytestring, std::pair<int, uint64_t> > GetDiff() const;

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
         * Returns the item count
         * @return
         */
        inline uint32_t item_count() const;

        /**
         * Returns the items (const version)
         * @return
         */
        inline const std::list<BlockMappingPairItem>& items() const;

        /**
         * Returns the items
         * @return
         */
        inline std::list<BlockMappingPairItem>& items();
};

uint64_t BlockMappingPair::block_id() const {
    return block_id_;
}

size_t BlockMappingPair::block_size() const {
    return block_size_;
}

uint32_t BlockMappingPair::version() const {
    return version_counter_;
}

uint32_t BlockMappingPair::item_count() const {
    return items_.size();
}

const std::list<BlockMappingPairItem>& BlockMappingPair::items() const {
    return items_;
}

std::list<BlockMappingPairItem>& BlockMappingPair::items() {
    return items_;
}

const byte* BlockMappingPairItem::fingerprint() const {
    return fp_;
}

byte* BlockMappingPairItem::mutable_fingerprint() {
    return fp_;
}

size_t BlockMappingPairItem::fingerprint_size() const {
    return fp_size_;
}

void BlockMappingPairItem::set_fingerprint_size(size_t new_size) {
    this->fp_size_ = new_size;
}

uint64_t BlockMappingPairItem::data_address() const {
    return data_address_;
}

void BlockMappingPairItem::set_data_address(uint64_t a) {
    this->data_address_ = a;
}

uint32_t BlockMappingPairItem::chunk_offset() const {
    return chunk_offset_;
}

void BlockMappingPairItem::set_chunk_offset(uint32_t co) {
    chunk_offset_ = co;
}

uint32_t BlockMappingPairItem::size() const {
    return size_;
}

void BlockMappingPairItem::set_size(uint32_t s) {
    size_ = s;
}

}
}

#endif  // BLOCK_MAPPING_PAIR_H__
