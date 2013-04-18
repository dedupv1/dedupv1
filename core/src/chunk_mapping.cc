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
#include <core/chunk_mapping.h>
#include <base/logging.h>
#include "dedupv1.pb.h"

#include <core/dedup.h>
#include <core/block_mapping.h>
#include <core/chunker.h>
#include <base/strutil.h>
#include <core/storage.h>
#include <core/chunk.h>

#include <sstream>

LOGGER("ChunkMapping");

using std::stringstream;
using std::string;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::chunkstore::Storage;
using dedupv1::Fingerprinter;
using dedupv1::base::strutil::ToString;

namespace dedupv1 {
namespace chunkindex {

ChunkMapping::ChunkMapping() {
    this->fp_size_ = 0;
    this->chunk_ = NULL;
    this->data_address_ = Storage::ILLEGAL_STORAGE_ADDRESS;
    this->known_chunk_ = false;
    this->usage_count_ = 0;
    this->usage_count_change_log_id_ = 0;
    this->usage_count_failed_write_change_log_id_ = 0;
    this->chunk_ = NULL;
    this->clear_block_hint();
    this->is_indexed_ = false;
}

ChunkMapping::ChunkMapping(const byte* fp, size_t fp_size) {
    this->fp_size_ = fp_size;
    memcpy(this->fp_, fp, fp_size);
    this->chunk_ = NULL;
    this->data_address_ = Storage::ILLEGAL_STORAGE_ADDRESS;
    this->known_chunk_ = false;
    this->usage_count_ = 0;
    this->usage_count_change_log_id_ = 0;
    this->usage_count_failed_write_change_log_id_ = 0;
    this->chunk_ = NULL;
    this->is_indexed_ = false;
    this->clear_block_hint();
}

ChunkMapping::ChunkMapping(const bytestring& fp) {
    this->fp_size_ = fp.size();
    memcpy(this->fp_, fp.data(), this->fp_size_);
    this->chunk_ = NULL;
    this->data_address_ = Storage::ILLEGAL_STORAGE_ADDRESS;
    this->known_chunk_ = false;
    this->usage_count_ = 0;
    this->usage_count_change_log_id_ = 0;
    this->usage_count_failed_write_change_log_id_ = 0;
    this->chunk_ = NULL;
    this->is_indexed_ = false;
    this->clear_block_hint();
}

bool ChunkMapping::Init(const Chunk* chunk) {
    this->chunk_ = chunk;
    this->data_address_ = Storage::ILLEGAL_STORAGE_ADDRESS;
    this->known_chunk_ = false;
    this->usage_count_ = 0;
    this->usage_count_change_log_id_ = 0;
    this->usage_count_failed_write_change_log_id_ = 0;
    return true;
}

bool ChunkMapping::SerializeTo(ChunkMappingData* data) const {
    CHECK(data, "Data not set");
    data->Clear();
    data->set_data_address(this->data_address());

    if (has_block_hint_) {
        data->set_last_block_hint(block_hint_);
    } else {
        data->clear_last_block_hint();
    }
    if (this->usage_count() != 0) {
        data->set_usage_count(this->usage_count());
    }
    if (this->usage_count_change_log_id() > 0) {
        data->set_usage_count_change_log_id(this->usage_count_change_log_id());
    }
    if (this->usage_count_failed_write_change_log_id() > 0) {
        data->set_usage_count_failed_write_change_log_id(this->usage_count_failed_write_change_log_id());
    }
    return true;
}

bool ChunkMapping::UnserializeFrom(const ChunkMappingData& data, bool preserve_chunk) {
    this->set_data_address(data.data_address());
    if (this->data_address() != Storage::ILLEGAL_STORAGE_ADDRESS) {
        this->known_chunk_ = true;
    }

    if (data.has_usage_count()) {
        this->usage_count_ = data.usage_count();
    } else {
        this->usage_count_ = 0;
    }

    if (data.has_last_block_hint()) {
        this->set_block_hint(data.last_block_hint());
    } else {
        this->clear_block_hint();
    }

    if (data.has_usage_count_change_log_id()) {
        this->usage_count_change_log_id_ = data.usage_count_change_log_id();
    } else {
        usage_count_change_log_id_ = 0;
    }
    if (data.has_usage_count_failed_write_change_log_id()) {
        this->usage_count_failed_write_change_log_id_ = data.usage_count_failed_write_change_log_id();
    } else {
        usage_count_failed_write_change_log_id_ = 0;
    }

    if (!preserve_chunk) {
        this->chunk_ = NULL;
    }

    return true;
}

string ChunkMapping::DebugString() const {
    stringstream s;
    s << "[" << Fingerprinter::DebugString(this->fp_, this->fp_size_);
    if (Fingerprinter::IsEmptyDataFingerprint(this->fp_, this->fp_size_)) {
        s << " <empty>";
    }
    if (this->data_address() == Storage::ILLEGAL_STORAGE_ADDRESS) {
        s << ", address <not set>";
    } else if (this->data_address() == Storage::EMPTY_DATA_STORAGE_ADDRESS) {
        s << ", address <empty>";
    } else {
        s << ", address " << this->data_address();
    }
    s << ", usage count " << this->usage_count();
    s << ", usage count change log id " << this->usage_count_change_log_id();
    s << ", usage count failed write change log id " << this->usage_count_failed_write_change_log_id();
    if (this->chunk() != NULL) {
        s << ", chunk size " << this->chunk()->size();
    } else {
        s << ", chunk size <not set>";
    }
    if (known_chunk_) {
        s << ", known chunk";
    }
    if (is_indexed_) {
      s << ", indexed";
    }
    s << "]";
    return s.str();
}

}
}
