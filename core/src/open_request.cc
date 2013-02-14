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
#include <core/block_mapping.h>
#include <core/open_request.h>
#include <base/logging.h>

using std::string;
using dedupv1::blockindex::BlockMapping;

LOGGER("OpenRequest");

namespace dedupv1 {

OpenRequest::OpenRequest(size_t block_size)
    : original_block_mapping_(block_size), updated_block_mapping_(block_size)  {
    block_id_ = BlockMapping::ILLEGAL_BLOCK_ID;
}

bool OpenRequest::CopyFrom(const BlockMapping& original_mapping,
                           const BlockMapping& updated_mapping) {
    DCHECK(original_mapping.block_id() == updated_mapping.block_id(),
        "Block id mismatch");
    CHECK(original_block_mapping_.CopyFrom(original_mapping),
        "Cannot copy block mapping: " << original_mapping.DebugString());
    CHECK(updated_block_mapping_.CopyFrom(updated_mapping),
        "Cannot copy block mapping: " << updated_mapping.DebugString());
    block_id_ = original_mapping.block_id();
    return true;
}

void OpenRequest::Release() {
    original_block_mapping_.Release();
    updated_block_mapping_.Release();
    block_id_ = BlockMapping::ILLEGAL_BLOCK_ID;
}

string OpenRequest::DebugString() const {
    return "[original " + this->original_block_mapping_.DebugString() +
           ", modified " + this->updated_block_mapping_.DebugString() + "]";
}

}
