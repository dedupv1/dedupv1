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

#ifndef COMPRESS_H__
#define COMPRESS_H__

#include <base/base.h>

namespace dedupv1 {
namespace base {

/**
 * A type for compression and decompressing data.
 *
 * Thread safety: The type can (after construction) used for compression and decompression
 * from multiple threads in parallel.
 */
class Compression {
    public:
        /**
         * enumerations for compression types.
         */
        enum CompressionType {
            COMPRESSION_ZLIB_1,//!< COMPRESSION_ZLIB_1
            COMPRESSION_ZLIB_3,//!< COMPRESSION_ZLIB_3
            COMPRESSION_ZLIB_9,//!< COMPRESSION_ZLIB_9
            COMPRESSION_BZ2,    //!< COMPRESSION_BZ2
            COMPRESSION_LZ4,
            COMPRESSION_SNAPPY //!< Snappy Compression
        };
    private:
        DISALLOW_COPY_AND_ASSIGN(Compression);

        /**
         * Type of the compression
         */
        CompressionType type_;
    public:
        /**
         * Returns the compression type of a compressor
         * @return
         */
        CompressionType GetCompressionType() const;

        /**
         * Constructor
         * @param type
         * @return
         */
        explicit Compression(enum CompressionType type);

        /**
         * Destructor
         * @return
         */
        virtual ~Compression();

        /**
         * Compresses the data given by src and stores the compression version in the dest memory.
         *
         * @param dest
         * @param dest_size
         * @param src
         * @param src_size
         * @return -1 denotes an error
         */
        virtual ssize_t Compress(void* dest, size_t dest_size, const void* src, size_t src_size) = 0;

        /**
         * Decompresses the data given by src and stores the
         * decompressed version in dest.
         *
         * @param dest
         * @param dest_size
         * @param src
         * @param src_size
         * @return -1 denotes an error
         */
        virtual ssize_t Decompress(void* dest, size_t dest_size, const void* src, size_t src_size) = 0;

        /**
         * Factory method to construct new compression objects.
         *
         * @param type type enum of the compression to create
         * @return a new compression instance or NULL if case of an error
         */
        static Compression* NewCompression(enum CompressionType type);
};

}
}

#endif  // COMPRESS_H__
