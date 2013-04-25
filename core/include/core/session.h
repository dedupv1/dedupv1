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

#ifndef SESSION_H__
#define SESSION_H__

#include <vector>
#include <list>
#include <set>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/block_mapping.h>
#include <core/chunk_store.h>
#include <core/storage.h>
#include <base/resource_management.h>
#include <core/chunk.h>
#include <core/chunk_mapping.h>

namespace dedupv1 {

class OpenRequest;
class Fingerprinter;
class Chunker;
class ChunkerSession;

namespace filter {
class FilterChain;
class Filter;
}

/**
 * A session is common data storage for a series of requests
 * No two requests in a single session are allowed to overlap. However,
 * requests from different session can overlap (and for performance reasons should!).
 *
 * It is relatively expensive to create and free session, we therefore
 * use a session pool (SessionManagement) to save resources.
 */
class Session {
        DISALLOW_COPY_AND_ASSIGN(Session);

        /**
         * Reference to the chunker session used in the session
         */
        ChunkerSession* chunker_session_;

        /**
         * Reference to a fingerprinter used in this session
         */
        Fingerprinter* fingerprinter_;

        std::set<const dedupv1::filter::Filter*> enabled_filters_;

        /**
         * Number of bytes currently in the chunker.
         * This value is used to transfer this value from block request to block request.
         */
        uint32_t open_chunk_pos_;

        /**
         * This member stored a cyclic list of open requests.
         * An open request is an request whose processing (MakeRequest) is finished, but the chunking
         * could not finish right at the end of the request, the chunker session
         * contains some data of the request that is not processed yet. This can happen
         * when using the RabinChunker without block_awareness enabled. In such cases
         * we postpone to write the block mapping back to the index and save to mapping. If the data of the
         * mapping is assigned to a chunk, we block mapping processing is finished.
         *
         * Note: The open request list is implemented as cyclic buffer. So never access elements of the open_requests array
         * directly. Use Session::GetRequest instead.
         * Rationale:
         * - Normal array didn't worked out well because often the complete array contents except the last element should be deleted.
         * - I want to avoid a linked list at nearly all costs to avoid dynamic memory allocation (still true?)
         */
        std::vector<OpenRequest*> open_requests_;

        /**
         * Maximal number of allowed open requests.
         */
        uint32_t open_request_count_;

        /**
         * Current start of the open request cyclic buffer.
         */
        uint32_t open_request_start_;

        byte* buffer_;

        size_t buffer_size_;

        /**
         * Fingerprint of the chunk filled with zeros.
         */
        bytestring empty_fp_;

        /**
         * Session lock to avoid that a session is used by
         * more than one thread. Normally the session management
         * should be done by the SessionManagement class. And therefore
         * a session should never be used by more than one thread.
         * This lock is more a safety net.
         */
        dedupv1::base::MutexLock lock_;

    public:
        enum block_mapping_open_state {
            BLOCK_MAPPING_OPEN_ERROR,
            BLOCK_MAPPING_IS_OPEN,
            BLOCK_MAPPING_IS_NOT_OPEN,
        };

        /**
         * Constructs a new session
         * @return
         */
        Session();

        /**
         * Inits a new session.
         *
         * @param block_size
         * @param chunk_store
         * @param chunker_factory
         * @param fingerprinter
     * @return true iff ok, otherwise an error has occurred
         */
        bool Init(uint32_t block_size,
                Chunker* chunker_factory,
                Fingerprinter* fingerprinter,
                const std::set<const dedupv1::filter::Filter*>& enabled_filter);

        /**
         * Appends a block mapping to the open requests.
         *
         * @param original_mapping
         * @param updated_mapping
     * @return true iff ok, otherwise an error has occurred
         */
        bool AppendBlock(const dedupv1::blockindex::BlockMapping& original_mapping,
                const dedupv1::blockindex::BlockMapping& updated_mapping);

        /**
         * Appends a new block mapping item (related to a chunk) to the
         * matching open request.
         *
         * @param block_id
         * @param offset
         * @param request
     * @return true iff ok, otherwise an error has occurred
         */
        bool AppendRequest(uint64_t block_id, uint32_t offset,
                const dedupv1::blockindex::BlockMappingItem& request);

        /**
         * Clear the first clear_count requests and only keep
         * the open requests afterwards.
         *
         * Should not fail except for programming errors.
         *
         * @param clear_count
     * @return true iff ok, otherwise an error has occurred
         */
        bool ClearRequests(uint32_t clear_count);

        /**
         * Get the block mapping of the open request with the given index (index not block_id!).
         *
         * @param index
         * @return
         */
        OpenRequest* GetRequest(uint32_t index);

        /**
         * Get the block mapping of the open request with the given index (index not block_id!).
         *
         * Should not fail except for programming errors.
         *
         * @param index
         * @return
         */
        const OpenRequest* GetRequest(uint32_t index) const;

        inline bool is_filter_enabled(const dedupv1::filter::Filter* f) {
            return (enabled_filters_.find(f) != enabled_filters_.end());
        }

        /**
         * Delete the open request block mapping with the given index.
         *
         * Should not fail except for programming errors.
         *
         * @param index
     * @return true iff ok, otherwise an error has occurred
         */
        bool DeleteRequest(uint32_t index);

        /**
         * Closes the session and releases all it memory.
         * However, the chunker session should be closed before because
         * this might releases one or many new chunks that must be processed before
         * the complete session can be closed.
     * @return true iff ok, otherwise an error has occurred
         */
        bool Close();

        /**
         * Merges the open requests regarding the block with the given block mapping.
         *
         * @param mapping
         * @return
         */
        enum block_mapping_open_state AppendIfOpen(dedupv1::blockindex::BlockMapping* mapping) const;

        /**
         * Locks the session
     * @return true iff ok, otherwise an error has occurred
         */
        bool Lock();

        /**
         * Unlocks the session
     * @return true iff ok, otherwise an error has occurred
         */
        bool Unlock();

		/**
         * returns the chunker session
         */
        inline dedupv1::ChunkerSession* chunker_session();

		/**
		 * returns the number of open requests
         */
        inline uint32_t open_request_count();

		/**
		 * TODO(dmeister): ?? Used when ??
		 */
        inline void set_open_chunk_position(uint32_t new_pos);

        inline uint32_t open_chunk_position();

		/**
		 * returns the current fingerprinter
         */
        inline Fingerprinter* fingerprinter();

        inline byte* buffer();

        inline size_t buffer_size();

        inline const bytestring& empty_fp() const {
            return empty_fp_;
        }

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        bool Clear();
};

byte* Session::buffer() {
    return buffer_;
}

size_t Session::buffer_size() {
    return buffer_size_;
}

uint32_t Session::open_chunk_position() {
    return this->open_chunk_pos_;
}

void Session::set_open_chunk_position(uint32_t new_pos) {
    this->open_chunk_pos_ = new_pos;
}

dedupv1::ChunkerSession* Session::chunker_session() {
    return chunker_session_;
}

uint32_t Session::open_request_count() {
    return open_request_count_;
}

Fingerprinter* Session::fingerprinter() {
    return fingerprinter_;
}

}

#endif  // SESSION_H__
