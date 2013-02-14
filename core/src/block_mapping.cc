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

#include <core/dedup.h>
#include <base/locks.h>
#include <core/block_mapping.h>
#include <base/bitutil.h>
#include <base/strutil.h>
#include <core/chunk_mapping.h>
#include <base/hashing_util.h>
#include <base/logging.h>
#include <core/chunk.h>
#include <core/storage.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sstream>

#include "dedupv1.pb.h"

using std::string;
using std::list;
using dedupv1::chunkstore::Storage;
using dedupv1::Fingerprinter;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::raw_compare;
using dedupv1::base::strutil::ToHexString;

LOGGER("BlockMapping");

namespace dedupv1 {
namespace blockindex {

BlockMappingItem::BlockMappingItem(unsigned int offset, unsigned int size) {
    memset(this->fp_, 0, Fingerprinter::kMaxFingerprintSize);
    this->fp_size_ = 0;
    this->data_address_ = BlockMapping::ILLEGAL_BLOCK_ID; // address not set
    this->item_chunk_in_use_ = true;
    this->chunk_offset_ = offset;
    this->size_ = size;
}

bool BlockMappingItem::CopyFrom(const BlockMappingItemData& data) {
    CHECK(data.fp().size() <= (size_t) Fingerprinter::kMaxFingerprintSize, "Illegal fingerprint size");
    this->data_address_ = data.data_address();
    this->fp_size_ = data.fp().size();
    memcpy(this->fp_, data.fp().c_str(), this->fp_size_);
    this->chunk_offset_ = data.chunk_offset();
    this->size_ = data.size();
    return true;
}

void BlockMapping::Release() {
    this->block_id_ = BlockMapping::ILLEGAL_BLOCK_ID;
}

BlockMapping::BlockMapping(uint64_t block_id, size_t block_size) {
    this->block_id_ = block_id;
    this->block_size_ = block_size;
    this->version_counter_ = 0;
    BlockMappingItem item(0, block_size_);
    item.set_data_address(Storage::EMPTY_DATA_STORAGE_ADDRESS);
    item.set_fingerprint_size(1);
    item.mutable_fingerprint()[0] = 2;
    item.set_is_used(true);
    this->items_.push_back(item);
    this->event_log_id_ = 0;
}

BlockMapping::BlockMapping(size_t block_size) {
    this->block_id_ = BlockMapping::ILLEGAL_BLOCK_ID;
    this->block_size_ = block_size;
    this->version_counter_ = 0;
    BlockMappingItem item(0, block_size_);
    item.set_data_address(Storage::EMPTY_DATA_STORAGE_ADDRESS);
    item.set_fingerprint_size(1);
    item.mutable_fingerprint()[0] = 2;
    item.set_is_used(true);
    this->items_.push_back(item);
    this->event_log_id_ = 0;
}

BlockMapping::~BlockMapping() {
    this->items_.clear();
}

bool BlockMapping::MergePartsFrom(const BlockMapping& src_block_mapping, uint32_t target_offset, uint32_t src_offset, uint32_t size) {

    DEBUG("Merge parts: src " << src_block_mapping.DebugString() <<
            ", target " << this->DebugString() <<
            ", src offset " << src_offset <<
            ", target offset " << target_offset <<
            ", size " << size);

    if (block_size_ == src_block_mapping.block_size_ && src_offset == 0 && target_offset == 0 && size == block_size_) {
        // short cut if the complete block is merged
        items_ = src_block_mapping.items_;
    }

    list<BlockMappingItem>::const_iterator i;
    int start_pos = 0;
    int end_pos = 0;
    for (i = src_block_mapping.items().begin(); i != src_block_mapping.items().end() && size > 0; i++) {
        end_pos += i->size();
        if (end_pos > src_offset) { // if item reaches into the source copy range
            int item_offset = src_offset - start_pos; // offset within this item
            int item_size = i->size() - item_offset;
            if (item_size > size) {
                item_size = size;
            }
            TRACE("Start pos: " << start_pos <<
                    ", end pos " << end_pos <<
                    ", src offset " << src_offset <<
                    ", item offset " << item_offset <<
                    ", item size " << item_size <<
                    ", item " << i->DebugString());
            if (item_size > 0) {
                BlockMappingItem copy = *i;
                copy.set_chunk_offset(i->chunk_offset() + item_offset);
                copy.set_size(item_size);

                DEBUG("Append: target offset " << target_offset << ", item " << copy.DebugString());
                CHECK(this->Append(target_offset, copy), "Failed to append item: " << this->DebugString() <<
                        ", item " << copy.DebugString());

                src_offset += item_size;
                target_offset += item_size;
                size -= item_size;
            }
            start_pos = end_pos;
        }
    }
    return true;
}

bool BlockMapping::Append(unsigned int offset, const BlockMappingItem& item) {
    DCHECK(item.size() > 0, "Illegal block mapping item size: " << item.DebugString());
    unsigned int pos = 0;
    bool inserted = false;

    DCHECK(offset >= 0, "Offset cannot be negative");
    DCHECK(offset + item.size() <= this->block_size(),
            "Illegal request: offset " << offset <<
            ", item " << item.DebugString() <<
            ", block " << this->DebugString());

    TRACE("Append item " << item.DebugString() << ": offset " << offset << ", block " << this->DebugString());
    pos = 0;

    // Loop through list and maintain next and last item pointer
    list<BlockMappingItem>::iterator current;
    for (current = this->items().begin(); current != this->items().end(); ) {
        TRACE("merge new item " << item.DebugString() <<
                ", offset " << offset <<
                ", position " << pos <<
                ", current " << current->DebugString());
        if (pos < offset && (offset + item.size()) < (pos + current->size())) {
            // first case: new earlier and larger than current
            //        pos        pos + current->size
            // [------|------------|-----
            //          |       |
            //         offset   offset + item->size
            TRACE("first case: new later and smaller than current");

            // create copy item based on old values
            BlockMappingItem new_item(*current);
            int diff = (offset + item.size() - pos);
            new_item.set_chunk_offset(new_item.chunk_offset() + diff);
            TRACE("new item size " << new_item.size() << ", diff " << diff);
            new_item.set_size(new_item.size() - diff);

            // modify current item;
            current->set_size(offset - pos);

            // add new item
            list<BlockMappingItem>::iterator j = current;
            j++;
            this->items_.insert(j, item);

            // add copy
            this->items_.insert(j, new_item);
            return true;
        } else if (pos + current->size() <= offset || pos > offset + item.size()) {
            // second case: do nothing
            //        pos    pos + current->size
            // [------|------|-----------------------
            //                 |            |
            //                 offset        offset + item->size
            pos += current->size();
            TRACE("second case: do nothing");
            current++;
            continue;
            /* Case: Complete Overlapping */
        } else if (pos >= offset && pos + current->size() <= offset + item.size()) {
            // third case: complete overlap
            //        pos    pos + current->size
            // [------|------|-----------------------
            //      |            |
            //      offset        offset + item->size
            TRACE("third case: complete overlap");

            size_t current_size = current->size();
            if (!inserted) {
                *current = item;
                inserted = true;
                current++;
            } else {
                current = this->items_.erase(current);
            }
            pos += current_size;
            TRACE("Intermediate Result: " << DebugString());
            continue;
        } else if (pos >= offset) {
            // third case: left overlap
            //        pos
            // [------|------------------------------
            //      |
            //      offset
            TRACE("fourth case: left overlap");

            if (!inserted) {
                this->items_.insert(current, item);
            }

            int diff = (offset + item.size()) - pos;
            current->set_size(current->size() - diff);
            current->set_chunk_offset(current->chunk_offset() + diff);
            pos += current->size() + diff;
            return true;
        } else if (pos < offset) {
            // fifth case
            //        pos
            // [------|------------------------------
            //                |
            //              offset
            TRACE("fifth case: ");
            int diff = pos + current->size() - offset;
            size_t current_size = current->size();

            current->set_size(current->size() - diff);

            BlockMappingItem new_item(item);
            current++;
            this->items_.insert(current, item);

            pos += current_size;
            inserted = true;
            TRACE("Intermediate Result: " << DebugString());
        } else {
            ERROR("Should not happen");
            return false;
        }
    }
    CHECK(inserted, "Item not inserted: " <<
            "mapping " << this->DebugString() <<
            ", offset " << offset <<
            ", new item " << item.DebugString());
    return true;
}

bool BlockMappingItem::Convert(const ChunkMapping& chunk_mapping) {
    DCHECK(chunk_mapping.fingerprint_size() > 0, "No fingerprint set");

    this->fp_size_ = chunk_mapping.fingerprint_size();
    memcpy(this->fp_, chunk_mapping.fingerprint(), this->fp_size_);
    this->data_address_ = chunk_mapping.data_address();
    return true;
}

bool BlockMapping::Merge(const ChunkMapping& chunk_mapping) {
    // Process from the end (newest) items to the beginning an apply new
    // fingerprint from chunk_mapping
    CHECK(chunk_mapping.data_address() != Storage::ILLEGAL_STORAGE_ADDRESS,
            "Data address not set: " << chunk_mapping.DebugString());

    list<BlockMappingItem>::reverse_iterator i;
    for (i = this->items().rbegin(); i != this->items().rend(); i++) {
        if (i->fingerprint_size() == 0) {
            CHECK(i->Convert(chunk_mapping), "Mapping convertion failed");
            return true;
        }
    }
    return true;
}

bool BlockMapping::Cut(unsigned int offset) {
    unsigned int pos = 0;
    list<BlockMappingItem>::iterator i;
    for (i = this->items().begin(); i != this->items().end(); i++) {
        pos += i->size();

        if (pos > offset) {
            i->set_size(offset);
            i++;
            this->items().erase(i, this->items().end());
            return true;
        }
    }
    return true;
}

bool BlockMapping::Check() const {
    unsigned int count = 0;

    bool failed = false;
    // Checks if the sizes of the block mapping items add up to
    // the block size
    list<BlockMappingItem>::const_iterator i;
    for (i = this->items().begin(); i != this->items().end(); i++) {
        if (i->chunk_offset() > Chunk::kMaxChunkSize) {
            failed = true;
        }
        if (i->size() > Chunk::kMaxChunkSize) {
            failed = true;
        }
        if (i->size() == 0) {
            failed = true;
        }
        count += i->size();
    }
    if (this->block_size() != count) {
        failed = true;
    }
    if (failed) {
        ERROR("Invalid Block Mapping:" << this->DebugString());
    }
    return !failed;
}

bool BlockMapping::SerializeTo(BlockMappingData* data, bool setBlockId, bool setChecksum) const {
    DCHECK(data, "Data not set");
    if (setBlockId) {
        data->set_block_id(this->block_id());
    }
    data->set_version_counter(this->version());

    if (setChecksum && this->has_checksum()) {
        data->set_checksum(checksum_.data(), checksum_.size());
    }
    if (event_log_id_ > 0) {
        data->set_event_log_id(event_log_id_);
    }

    list<BlockMappingItem>::const_iterator i;
    for (i = this->items().begin(); i != this->items().end(); i++) {
        BlockMappingItemData* item_data = data->add_items();
        item_data->set_fp(i->fingerprint(), i->fingerprint_size());
        item_data->set_data_address(i->data_address());

        if (i->chunk_offset() > 0) {
            item_data->set_chunk_offset(i->chunk_offset());
        }
        item_data->set_size(i->size());
    }
    return true;
}

bool BlockMapping::UnserializeFrom(const BlockMappingData& data, bool checkBlockId) {
    DCHECK(!checkBlockId || !data.has_block_id() || this->block_id() == data.block_id(),
            "Illegal block id" << this->block_id());
    CHECK(this->CopyFrom(data), "cannot copy from data: " << data.ShortDebugString());
    return true;
}

bool BlockMapping::CopyFrom(const BlockMappingData& data) {
    if (data.has_block_id()) {
        this->block_id_ = data.block_id();
    }
    this->version_counter_ = data.version_counter();
    this->items_.clear();

    if (data.has_checksum()) {
        this->checksum_.assign(reinterpret_cast<const byte*>(data.checksum().data()),
                data.checksum().size());
    } else {
        this->checksum_.clear();
    }

    if (data.has_event_log_id()) {
        this->event_log_id_ = data.event_log_id();
    } else {
        this->event_log_id_ = 0;
    }

    // Read chunk data
    for (int i = 0; i < data.items_size(); i++) {
        const BlockMappingItemData& data_item(data.items(i));
        BlockMappingItem item(0, 0);
        CHECK(item.CopyFrom(data_item), "Cannot copy block mapping item");
        this->items_.push_back(item);
    }
    return true;
}

bool BlockMapping::CopyFrom(const BlockMapping& src) {
    DCHECK(src.block_id() != BlockMapping::ILLEGAL_BLOCK_ID,
            "Illegal block id: " << src.DebugString());
    DCHECK(this->block_id_ == ILLEGAL_BLOCK_ID || this->block_id_ == src.block_id(),
            "Block mapping already acquired: block id " << this->block_id_ <<
            ", src block " << src.DebugString());
    *this = src;
    return true;
}

std::string BlockMappingItem::DebugString() const {
    std::stringstream s;
    s << "[chunk offset " << this->chunk_offset() << ", size " << this->size() << ", fp ";
    if (this->fingerprint_size() > 0) {
        if (Fingerprinter::IsEmptyDataFingerprint(this->fingerprint(), this->fingerprint_size())) {
            s << "<empty>";
        } else {
            s << Fingerprinter::DebugString(this->fingerprint(), this->fingerprint_size());
        }
    } else {
        s << "(FP not set)";
    }
    if (this->data_address() == Storage::EMPTY_DATA_STORAGE_ADDRESS) {
        s << ", address <empty>";
    } else if (this->data_address() == Storage::ILLEGAL_STORAGE_ADDRESS) {
        s << ", address <illegal>";
    } else {
        s << ", address " << this->data_address();
    }
    s << "]";
    return s.str();
}

bool BlockMappingItem::ConvertTo(ChunkMapping* chunk_mapping) const {
    CHECK(chunk_mapping, "chunk mapping not set");

    CHECK(chunk_mapping->Init(NULL), "Failed to init chunk mapping");
    chunk_mapping->set_fingerprint(this->fingerprint_string());
    chunk_mapping->set_data_address(this->data_address());
    return true;
}

std::string BlockMapping::DebugString() const {
    unsigned int count = 0;
    std::stringstream s;
    s << "[block " << block_id() <<
            ", size " << block_size() <<
            ", item count " << item_count() <<
            ", version " << version() <<
            ", event log id " << event_log_id();

    if (has_checksum()) {
        s << ", checksum " << ToHexString(checksum_.data(), checksum_.size());
    }
    s << std::endl;

    list<BlockMappingItem>::const_iterator i;
    for (i = this->items().begin(); i != this->items().end(); i++) {
        s << "Item: offset " << count << ", " << i->DebugString() << std::endl;
        count += i->size();
    }
    s << "]";
    return s.str();
}

bool BlockMappingItem::Equals(const BlockMappingItem& i2) const {
    if (data_address_ != i2.data_address_) {
        return false;
    }
    if (chunk_offset_ != i2.chunk_offset_) {
        return false;
    }
    if (size_ != i2.size_) {
        return false;
    }
    if (raw_compare(fp_, fp_size_, i2.fp_, i2.fp_size_) != 0) {
        return false;
    }
    return true;
}

bool BlockMapping::Equals(const BlockMapping& m2) const {
    if ((this->block_id() != m2.block_id())
            || (this->block_size() != m2.block_size())
            || (this->item_count() != m2.item_count())
            || (this->version() != m2.version())
            || (this->event_log_id() != m2.event_log_id())) {
        return false;
    }

    list<BlockMappingItem>::const_iterator i1;
    list<BlockMappingItem>::const_iterator i2;
    for (i1 = this->items().begin(), i2 = m2.items().begin(); i1 != this->items().end(); i1++, i2++) {
        if (!i1->Equals(*i2)) {
            return false;
        }
    }
    return true;
}

bool BlockMapping::FillEmptyBlockMapping() {
    size_t block_size = this->block_size();
    size_t pos = 0;

    while (pos < block_size) {
        size_t size = block_size - pos;
        if (size > Chunk::kMaxChunkSize) {
            size = Chunk::kMaxChunkSize;
        }
        BlockMappingItem item(0, size);
        CHECK(Fingerprinter::SetEmptyDataFingerprint(item.mutable_fingerprint(), item.mutable_fingerprint_size()),
                "Failed to set empty data fp");
        item.set_data_address(Storage::EMPTY_DATA_STORAGE_ADDRESS);
        item.set_is_used(true);
        CHECK(this->Append(pos, item), "Failed to append empty block mapping");
        pos += size;
    }
    return true;
}

}
}
