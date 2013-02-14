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
#include <core/block_mapping_pair.h>
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
using std::map;
using std::pair;
using std::make_pair;
using std::list;
using dedupv1::chunkstore::Storage;
using dedupv1::Fingerprinter;
using dedupv1::base::raw_compare;
using dedupv1::base::strutil::ToHexString;

LOGGER("BlockMappingPair");

namespace dedupv1 {
namespace blockindex {

BlockMappingPairItem::BlockMappingPairItem() {
    memset(this->fp_, 0, Fingerprinter::kMaxFingerprintSize);
    this->fp_size_ = 0;
    this->data_address_ = BlockMapping::ILLEGAL_BLOCK_ID; // address not set
    this->chunk_offset_ = 0;
    this->size_ = 0;
    this->usage_count_modifier_ = 0;
}

bool BlockMappingPairItem::CopyFrom(const BlockMappingPairItemData& data) {
    CHECK(data.fp().size() <= (size_t) Fingerprinter::kMaxFingerprintSize, "Illegal fingerprint size");
    this->data_address_ = data.data_address();
    this->fp_size_ = data.fp().size();
    memcpy(this->fp_, data.fp().c_str(), this->fp_size_);
    this->chunk_offset_ = data.chunk_offset();
    this->size_ = data.size();
    this->usage_count_modifier_ = data.usage_count_modifier();
    return true;
}

BlockMappingPair::BlockMappingPair(size_t block_size) {
    this->block_id_ = BlockMapping::ILLEGAL_BLOCK_ID;
    this->block_size_ = block_size;
    this->version_counter_ = 0;
}

BlockMappingPair::~BlockMappingPair() {
    this->items_.clear();
}

bool BlockMappingPair::SerializeTo(BlockMappingPairData* data) const {
    DCHECK(data, "Data not set");
    data->set_block_id(this->block_id());
    data->set_version_counter(this->version());

    list<BlockMappingPairItem>::const_iterator i;
    for (i = this->items().begin(); i != this->items().end(); i++) {
        BlockMappingPairItemData* item_data = data->add_items();
        item_data->set_fp(i->fingerprint(), i->fingerprint_size());
        item_data->set_data_address(i->data_address());

        if (i->chunk_offset() > 0) {
            item_data->set_chunk_offset(i->chunk_offset());
        }
        item_data->set_size(i->size());

        if (i->usage_count_modifier() != 0) {
            item_data->set_usage_count_modifier(i->usage_count_modifier());
        }
    }
    return true;
}

BlockMapping BlockMappingPair::GetModifiedBlockMapping(uint64_t event_log_id) const {
    BlockMapping mapping(block_id(), block_size());
    mapping.set_version(version());
    mapping.set_event_log_id(event_log_id);
    mapping.items().clear();

    list<BlockMappingPairItem>::const_iterator i;
    for (i = this->items().begin(); i != this->items().end(); i++) {
        if (i->size() > 0) {
            BlockMappingItem item(i->chunk_offset(), i->size());
            item.set_fingerprint_size(i->fingerprint_size());
            memcpy(item.mutable_fingerprint(), i->fingerprint(), i->fingerprint_size());
            item.set_data_address(i->data_address());

            mapping.items().push_back(item);
        }
    }

    return mapping;
}

bool BlockMappingPair::CopyFrom(const BlockMapping& original_mapping, const BlockMapping& updated_mapping) {
    DCHECK(block_size_ == original_mapping.block_size(), "Illegal block size");
    DCHECK(block_size_ == updated_mapping.block_size(), "Illegal block size");
    DCHECK(original_mapping.block_id() == updated_mapping.block_id(), "Illegal block id");
    DCHECK(original_mapping.Check(), "Illegal original block mapping: " << original_mapping.DebugString());
    DCHECK(updated_mapping.Check(), "Illegal modified block mapping: " << updated_mapping.DebugString());

    TRACE("Update mapping pair: " << original_mapping.DebugString() << " => " << updated_mapping.DebugString());

    items_.clear();
    block_id_ = updated_mapping.block_id();
    version_counter_ = updated_mapping.version();

    map<bytestring, int32_t> uc_map;
    map<bytestring, uint64_t> container_map;

    list<BlockMappingItem>::const_iterator i;
    for (i = original_mapping.items().begin(); i != original_mapping.items().end(); i++) {
        if (i->size() == 0) {
            continue;
        }
        uc_map[i->fingerprint_string()]--;
        container_map[i->fingerprint_string()] = i->data_address();
    }
    for (i = updated_mapping.items().begin(); i != updated_mapping.items().end(); i++) {
        if (i->size() == 0) {
            continue;
        }
        uc_map[i->fingerprint_string()]++;
        container_map[i->fingerprint_string()] = i->data_address();
    }

    for (i = updated_mapping.items().begin(); i != updated_mapping.items().end(); i++) {
        if (i->size() == 0) {
            continue;
        }
        BlockMappingPairItem pair_item;
        pair_item.set_fingerprint_size(i->fingerprint_size());
        memcpy(pair_item.mutable_fingerprint(), i->fingerprint(), pair_item.fingerprint_size());

        pair_item.set_data_address(i->data_address());
        pair_item.set_chunk_offset(i->chunk_offset());
        pair_item.set_size(i->size());

        pair_item.set_usage_count_modifier(uc_map[i->fingerprint_string()]);
        items_.push_back(pair_item);

        uc_map.erase(i->fingerprint_string());
    }
    // process left over uc entires
    map<bytestring, int32_t>::const_iterator j;
    for(j = uc_map.begin(); j != uc_map.end(); j++) {
        if (Fingerprinter::IsEmptyDataFingerprint(j->first.data(), j->first.size())) {
            continue;
        }
        BlockMappingPairItem pair_item;
        pair_item.set_fingerprint_size(j->first.size());
        memcpy(pair_item.mutable_fingerprint(), j->first.data(), pair_item.fingerprint_size());

        pair_item.set_data_address(container_map[j->first]);
        pair_item.set_chunk_offset(0);
        pair_item.set_size(0);

        pair_item.set_usage_count_modifier(j->second);
        items_.push_back(pair_item);
    }
    return true;
}

map<bytestring, pair<int, uint64_t> > BlockMappingPair::GetDiff() const {
    map<bytestring, pair<int, uint64_t> > diff_result;
    map<bytestring, int> diff;
    map<bytestring, uint64_t> container_map;

    list<BlockMappingPairItem>::const_iterator i;
    for (i = this->items().begin(); i != this->items().end(); i++) {
        bytestring fp;
        fp.assign(i->fingerprint(), i->fingerprint_size());
        diff[fp] += i->usage_count_modifier(); 
        container_map[fp] = i->data_address();
    }
    map<bytestring, int>::iterator j;
    for (j = diff.begin(); j != diff.end(); j++) {
        if (j->second != 0) {
            diff_result[j->first] = make_pair(j->second, container_map[j->first]);
        }
    }
    return diff_result;
}

bool BlockMappingPair::CopyFrom(const BlockMappingPairData& data) {
    this->block_id_ = data.block_id();
    this->version_counter_ = data.version_counter();
    this->items_.clear();

    // Read chunk data
    for (int i = 0; i < data.items_size(); i++) {
        const BlockMappingPairItemData& data_item(data.items(i));
        BlockMappingPairItem item;
        CHECK(item.CopyFrom(data_item), 
                "Cannot copy block mapping pair item: " << data_item.ShortDebugString());
        this->items_.push_back(item);
    }
    return true;
}

std::string BlockMappingPairItem::DebugString() const {
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
    s << ", usage count modifier " << usage_count_modifier();
    s << "]";
    return s.str();
}

std::string BlockMappingPair::DebugString() const {
    unsigned int count = 0;
    std::stringstream s;
    s << "[block " << block_id() <<
            ", size " << block_size() <<
            ", item count " << item_count() <<
            ", version " << version() << std::endl;

    list<BlockMappingPairItem>::const_iterator i;
    for (i = this->items().begin(); i != this->items().end(); i++) {
        s << "Item: offset " << count << ", " << i->DebugString() << std::endl;
        count += i->size();
    }
    s << "]";
    return s.str();
}

}
}
