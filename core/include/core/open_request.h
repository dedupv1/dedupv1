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

#ifndef OPEN_REQUEST_H__
#define OPEN_REQUEST_H__

#include <string>

#include <core/dedup.h>
#include <core/block_mapping.h>

namespace dedupv1 {

/**
 * The open requests is a data structure to store the original block mapping and
 * the modified block mapping of all block requests are not fully processed, e.g. because
 * some chunked data is still open.
 *
 * Note: Copy constructor and assignment is ok here
 */
class OpenRequest {
    private:
        /**
         * Block id of the open request.
         */
        uint64_t block_id_;

        /**
         * original block mapping.
         */
        dedupv1::blockindex::BlockMapping original_block_mapping_;

        /**
         * updated block mapping.
         */
        dedupv1::blockindex::BlockMapping updated_block_mapping_;
    public:
        /**
         * Constructor.
         *
         * @param block_size
         * @return
         */
        explicit OpenRequest(size_t block_size);

        /**
         * returns the block id
         * @return
         */
        inline uint64_t block_id() const;

        /**
         * Inits the system from the given block mapping.
         *
         * The block mapping is used
         * as original mapping and prototype for the modified mapping.
         *
         * @param original_mapping
         * @param updated_mapping
     * @return true iff ok, otherwise an error has occurred
         */
        bool CopyFrom(const dedupv1::blockindex::BlockMapping& original_mapping,
                const dedupv1::blockindex::BlockMapping& updated_mapping);

        /**
         * Returns a modifiable block mapping.
         * @return
         */
        inline dedupv1::blockindex::BlockMapping* mutable_block_mapping();

        /**
         * returns the modified block mapping.
         * @return
         */
        inline const dedupv1::blockindex::BlockMapping& modified_block_mapping() const;

        /**
         * returns the original unchanged block mapping.
         * @return
         */
        inline const dedupv1::blockindex::BlockMapping& original_block_mapping() const;

        /**
         * Releases the open request.
         */
        void Release();

        /**
         * returns a developer-readable representation of the open request.
         * @return
         */
        std::string DebugString() const;
};

uint64_t OpenRequest::block_id() const {
    return block_id_;
}

dedupv1::blockindex::BlockMapping* OpenRequest::mutable_block_mapping() {
    return &updated_block_mapping_;
}

const dedupv1::blockindex::BlockMapping& OpenRequest::modified_block_mapping() const {
    return updated_block_mapping_;
}

const dedupv1::blockindex::BlockMapping& OpenRequest::original_block_mapping() const {
    return original_block_mapping_;
}

}

#endif  // OPEN_REQUEST_H__
