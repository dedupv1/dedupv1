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

#ifndef CHUNK_H__
#define CHUNK_H__

#include <core/dedup.h>

namespace dedupv1 {

/**
 * A chunk of data.
 * The size attribute represents the actual size of the chunk. Size may lie between
 * kMinChunkSize and kMaxChunkSize.
 *
 * The client of chunk is responsible for allocating and freeing chunk instances.
 */
class Chunk {
    private:
        /**
         * Data buffer for the chunk
         */
        byte* data_;

        /**
         * Current size of the chunk
         */
        size_t size_;

    public:
        /**
         * Minimal chunk size. The minimal chunk size
         * if 1 due to chunks that are forcefully finished
         * at the end of requests
         */
        static size_t const kMinChunkSize = 1;

        /**
         * Default average chunk size
         */
        static size_t const kDefaultAvgChunkSize = 8192;

        /**
         * Compile time constant indicating the maximal allowed chunk size
         */
        static size_t const kMaxChunkSize = 65536;

        /**
         * Constructor
         * @return
         */
        Chunk(uint32_t size);

        /**
         * Destructor
         * @return
         */
        virtual ~Chunk();

        /**
         * size of the chunk
         * @return
         */
        inline size_t size() const;

        /**
         * returns the chunk data
         * @return
         */
        inline const byte* data() const;

        /**
         * returns a mutable data pointer
         * @return
         */
        inline byte* mutable_data();
        DISALLOW_COPY_AND_ASSIGN(Chunk);
};

size_t Chunk::size() const {
    return size_;
}

const byte* Chunk::data() const {
    return data_;
}

byte* Chunk::mutable_data() {
    return data_;
}

}

#endif  // CHUNK_H__
