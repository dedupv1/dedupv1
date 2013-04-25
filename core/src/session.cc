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
#include <core/session.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>

#include <vector>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/block_mapping.h>
#include <core/open_request.h>
#include <base/logging.h>
#include <core/log_consumer.h>
#include <core/block_index.h>
#include <core/chunk.h>
#include <core/chunker.h>
#include <core/chunk_mapping.h>
#include <core/storage.h>
#include <core/content_storage.h>

using std::list;
using std::string;
using std::vector;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::chunkstore::ChunkStore;
using dedupv1::filter::FilterChain;

LOGGER("Session");

namespace dedupv1 {

Session::Session() {
    chunker_session_ = NULL;
    fingerprinter_ = NULL;
    open_chunk_pos_ = 0;
    open_request_count_ = 0;
    open_request_start_ = 0;
    buffer_ = 0;
    buffer_size_ = 0;
}

bool Session::Init(uint32_t block_size, Chunker* chunker, Fingerprinter* fingerprinter,
                   const std::set<const dedupv1::filter::Filter*>& enabled_filters) {
    DCHECK(chunker, "chunker not set");
    DCHECK(fingerprinter, "Fingerprinter not set");

    uint32_t max_requests = (block_size / Chunk::kMaxChunkSize) + 1;

    CHECK(ContentStorage::InitEmptyFingerprint(chunker, fingerprinter, &empty_fp_),
        "Failed to calculate empty fingerprint");

    this->enabled_filters_ = enabled_filters;
    this->chunker_session_ = chunker->CreateSession();
    CHECK(this->chunker_session_, "Failed to create chunker session");

    this->fingerprinter_ = fingerprinter; // session takes over the ownership and closes it
    this->open_chunk_pos_ = 0;

    // Maximal possible number of blocks per chunk
    this->open_requests_.resize(max_requests);
    for (int i = 0; i < this->open_requests_.size(); i++) {
        this->open_requests_[i] = new OpenRequest(block_size);
        CHECK(this->open_requests_[i], "Cannot create new open request");
    }
    this->open_request_count_ = 0;
    this->open_request_start_ = 0;

    this->buffer_size_ = block_size;
    if (Chunk::kMaxChunkSize > block_size) {
        this->buffer_size_ = Chunk::kMaxChunkSize;
    }
    this->buffer_ = new byte[this->buffer_size_];
    CHECK(this->buffer_, "Alloc buffer failed");
    return true;
}

bool Session::Clear() {
    this->open_chunk_pos_ = 0;
    this->open_request_count_ = 0;
    this->open_request_start_ = 0;
    vector<OpenRequest*>::iterator j;
    for (j = this->open_requests_.begin(); j != this->open_requests_.end(); j++) {
        OpenRequest* r = *j;
        if (r) {
            r->Release();
        }
    }
    if (this->chunker_session_) {
        CHECK(this->chunker_session_->Clear(), "Failed to clear chunker session");
    }
    return true;
}

bool Session::Close() {
    if (chunker_session_) {
        CHECK(chunker_session_->Close(),
            "Failed to close session");
        chunker_session_ = NULL;
    }

    this->open_chunk_pos_ = 0;
    this->open_request_count_ = 0;
    this->open_request_start_ = 0;
    vector<OpenRequest*>::iterator j;
    for (j = this->open_requests_.begin(); j != this->open_requests_.end(); j++) {
        OpenRequest* r = *j;
        if (r) {
            delete r;
        }
    }
    this->open_requests_.clear();

    if (this->buffer_) {
        delete[] this->buffer_;
        this->buffer_ = NULL;
    }
    this->buffer_size_ = 0;

    if (this->fingerprinter_) {
        this->fingerprinter_->Close();
        this->fingerprinter_ = NULL;
    }
    delete this;
    return true;
}

OpenRequest* Session::GetRequest(unsigned int index) {
    return this->open_requests_[(this->open_request_start_ + index) % this->open_requests_.size()];
}

const OpenRequest* Session::GetRequest(unsigned int index) const {
    return this->open_requests_[(this->open_request_start_ + index) % this->open_requests_.size()];
}

bool Session::DeleteRequest(unsigned int index) {
    DCHECK(index <= this->open_request_count_, "Illegal index " << index);

    // Moving later requests
    for (unsigned int i = index + 1; i < this->open_request_count_; i++) {
        *this->open_requests_[(this->open_request_start_ + i - 1) % this->open_requests_.size()]
            = *this->open_requests_[(this->open_request_start_ + i) % this->open_requests_.size()];
    }
    this->open_requests_[(this->open_request_start_ + this->open_request_count_ - 1) % this->open_requests_.size()]->Release();
    this->open_request_count_--;
    return true;
}

bool Session::ClearRequests(unsigned int clear_count) {
    DCHECK(clear_count <= this->open_request_count_, "Illegal clear count");

    for (unsigned int i = 0; i < clear_count; i++) {
        this->open_requests_[(this->open_request_start_ + i) % this->open_requests_.size()]->Release();
    }
    this->open_request_count_ -= clear_count;
    this->open_request_start_ = (this->open_request_start_ + clear_count) % this->open_requests_.size();
    return true;
}

bool Session::AppendBlock(const BlockMapping& original_mapping,
                          const BlockMapping& updated_mapping) {
    OpenRequest* new_request = NULL;
    if (this->open_request_count_ > 0) {
        OpenRequest* last_request = this->GetRequest(this->open_request_count_ - 1);
        DCHECK(last_request, "New request not set");
        uint64_t last_block_id = last_request->block_id();
        if (last_block_id == original_mapping.block_id()) {
            return true;
        }
    }
    new_request = this->GetRequest(this->open_request_count_);
    DCHECK(new_request, "New request not set");
    CHECK(new_request->CopyFrom(original_mapping, updated_mapping),
        "Failed to copy block mapping " <<
        original_mapping.DebugString() << " => " << updated_mapping.DebugString());
    this->open_request_count_++;
    return true;
}

bool Session::AppendRequest(uint64_t block_id, unsigned int offset, const BlockMappingItem& request) {
    DCHECK(request.size() > 0, "Request size too small: " << request.DebugString());
    DCHECK(request.size() <= Chunk::kMaxChunkSize,
        "Request size too high: " << request.DebugString());
    DCHECK(request.chunk_offset() <= Chunk::kMaxChunkSize,
        "Request chunk offset too high: " << request.DebugString());
    DCHECK(request.size() + request.chunk_offset() <= Chunk::kMaxChunkSize,
        "Request length too high: " << request.DebugString());
    DCHECK(this->open_request_count_ != 0, "No open request");

    OpenRequest* open_request = this->GetRequest(this->open_request_count_ - 1);
    DCHECK(open_request, "Open request not set");
    DCHECK(open_request->block_id() == block_id, "Bad block id: " <<
        "block id " << block_id <<
        ", given block id " << open_request->block_id());

    CHECK(open_request->mutable_block_mapping()->Append(offset, request),
        "Block mapping append failed: " << open_request->mutable_block_mapping()->DebugString() <<
        ", offset " << offset <<
        ", item " << request.DebugString());
    return true;
}

Session::block_mapping_open_state Session::AppendIfOpen(BlockMapping* mapping) const {
    int pos = 0;
    for (uint32_t i = 0; i < this->open_request_count_; i++) {
        const OpenRequest* open_request = this->GetRequest(i);
        DCHECK_RETURN(open_request, BLOCK_MAPPING_OPEN_ERROR, "Open request not set");
        if (open_request->block_id() == mapping->block_id()) {
            list<BlockMappingItem>::const_iterator i;
            for (i = open_request->modified_block_mapping().items().begin(); i
                 != open_request->modified_block_mapping().items().end(); i++) {
                CHECK_RETURN(mapping->Append(pos, *i), BLOCK_MAPPING_OPEN_ERROR,
                    "Block mapping append failed");
                pos += i->size();
            }
            return BLOCK_MAPPING_IS_OPEN;
        }
    }
    return BLOCK_MAPPING_IS_NOT_OPEN;
}

bool Session::Lock() {
    CHECK(this->lock_.AcquireLock(), "Session lock failed");
    return true;
}

bool Session::Unlock() {
    CHECK(this->lock_.ReleaseLock(), "Session unlock failed");
    return true;
}

}
