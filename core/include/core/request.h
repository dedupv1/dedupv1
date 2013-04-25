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

#ifndef REQUEST_H__
#define REQUEST_H__

#include <string>

#include <core/dedup.h>
#include <tbb/tick_count.h>
#include <tbb/concurrent_unordered_map.h>

namespace dedupv1 {

/**
 * Request type
 */
enum request_type {
    REQUEST_READ,//!< REQUEST_READ
    REQUEST_WRITE//!< REQUEST_WRITE
};

#define REQUEST_STATS_START(rs, c) if (rs) { rs->Start(c); }
#define REQUEST_STATS_FINISH(rs, c) if (rs) { rs->Finish(c); }

/**
 * Statistics about the request.
 */
class RequestStatistics {
    public:
        enum ProfileComponent {
            TOTAL,
            WAITING,
            CHUNKING,
            FINGERPRINTING,
            FILTER_CHAIN,
            OPEN_REQUEST_HANDLING,
            BLOCK_STORING, // Part of OPEN_REQUEST_HANDLING
            PROCESSING
        };
    private:

        tbb::concurrent_unordered_map<enum ProfileComponent, tbb::tick_count> start_tick_map_;
        tbb::concurrent_unordered_map<enum ProfileComponent, tbb::atomic<uint64_t> > latency_map_;
    public:
        /**
         * Constructor.
         * @return
         */
        RequestStatistics();

        /**
         * Denotes the start of the execution of an component of the request
         * @param c
         */
        void Start(enum ProfileComponent c);

        /**
         * Marks the end of the execution of a component of the request
         * @param c
         */
        void Finish(enum ProfileComponent c);

        /**
         * returns the latency of the given component.
         * Returns 0 if the specified component as not been executed or finished.
         *
         * @param c
         * @return
         */
        uint64_t latency(enum ProfileComponent c);

        std::string DebugString();
};

/**
 * Class that requests a storage request
 *
 * Currently the offset and the size of a request must be a multiple of 512 byte.
 */
class Request {
    private:
        /**
         * Request type (read/write)
         */
        enum request_type request_type_;

        /**
         * block id of the index
         */
        uint64_t block_id_;

        /**
         * Request offset within the id
         */
        uint64_t offset_;

        /**
         * Request size within the id
         */
        uint64_t size_;

        /**
         * Buffer to read data from for writing or write data to while reading
         */
        byte* buffer_;

        /**
         * TODO (dmeister): Necessary
         */
        uint32_t block_size_;
    public:
        /**
         * Constructor.
         *
         * @param request_type
         * @param block_id
         * @param offset offset within the block
         * @param size size of the request
         * @param buffer regardless of the offset, the source/target should be copied to the beginning of the buffer.
         * @param block_size
         * @return
         */
        Request(enum request_type request_type,
                uint64_t block_id,
                uint64_t offset,
                uint64_t size,
                byte* buffer,
                uint32_t block_size);

        inline enum request_type request_type() const;
        inline uint64_t block_id() const;
        inline uint64_t offset() const;
        inline uint64_t size() const;
        inline byte* buffer();
        inline const byte* buffer() const;
        inline uint32_t block_size() const;

        bool IsValid() const;
        std::string DebugString();
};

inline enum request_type Request::request_type() const {
    return request_type_;
}

inline uint64_t Request::block_id() const {
    return block_id_;
}

inline uint64_t Request::offset() const {
    return offset_;
}

inline uint64_t Request::size() const {
    return size_;
}

inline byte* Request::buffer() {
    return buffer_;
}

inline const byte* Request::buffer() const {
    return buffer_;
}

inline uint32_t Request::block_size() const {
    return block_size_;
}

}

#endif  // REQUEST_H__
