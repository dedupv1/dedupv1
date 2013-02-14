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

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "zlib.h"
#include "bzlib.h"
#include "snappy.h"
#include "lz4.h"

#include <base/compress.h>
#include <base/logging.h>
#include <base/sha1.h>

LOGGER("Compression");

using dedupv1::base::sha1;

namespace dedupv1 {
namespace base {

Compression::CompressionType Compression::GetCompressionType() const {
    return type_;
}

Compression::Compression(enum CompressionType type) {
    this->type_ = type;
}

Compression::~Compression() {
}

/**
 * Wrapper around the zlib compression
 */
class ZlibCompression : public Compression {
    int level_;
public:
    /**
     * Constructor
     * @param type
     * @param level
     * @return
     */
    ZlibCompression(enum CompressionType type, int level) : Compression(type) {
        this->level_ = level;
    }

    virtual ssize_t Compress(void* dest, size_t dest_size, const void* src, size_t src_size) {
        unsigned long new_dest_size = dest_size;
        int ret = compress2(static_cast<byte*>(dest),
            &new_dest_size,
            static_cast<const byte*>(src),
            src_size,
            this->level_);
        if (ret == Z_OK) {
            TRACE("Compressed " << src_size << " to " << new_dest_size);
            return new_dest_size;
        } else if (ret == Z_MEM_ERROR) {
            ERROR("Compression failed: Memory error");
            return -1;
        } else if (ret == Z_BUF_ERROR) {
            ERROR("Compression failed: Buffer too small: src size: " << src_size << ", dest size: " << dest_size);
            return -1;
        } else {
            ERROR("Unknown compression error: " << ret);
            return -1;
        }
    }

    virtual ssize_t Decompress(void* dest, size_t dest_size, const void* src, size_t src_size) {
        unsigned long new_dest_size = dest_size;
        int ret = uncompress(static_cast<byte*>(dest), &new_dest_size,
            static_cast<const byte*>(src), src_size);
        if (ret == Z_MEM_ERROR) {
            ERROR("Decompression failed: Memory error");
            return -1;
        } else if (ret == Z_BUF_ERROR) {
            ERROR("Decompression failed: Buffer too small: " << src_size << ", dest size: " << dest_size);
            return -1;
        }
        TRACE("Decompressed " << src_size << " to " << new_dest_size);
        return new_dest_size;
    }

    DISALLOW_COPY_AND_ASSIGN(ZlibCompression);
};

/**
 * Wrapper class around the bzip2 algorithm.
 * See http://www.bzip.org/ for details.
 */
class BZ2Compression : public Compression {

    /**
       *This specifies the block size for the bz2 algorithm. Takes values from 1 to 9 with 9 being best at compressing but
     * having the highest memory requirements.
     * */
    int block_size_;

    /**
     * This parameter determines when the bz2 algorithm switches from a standard sorting algorithm to a fallback algorithm when
     * presented by highly repetitive input. Default is 30.
     * */
    int work_factor_;
public:
    static const int kStdBlockSize = 4;
    static const int kStdWorkFactor = 30;

    /**
     * Constructor.
     *
     * @param type
     * @param block_size
     * @param work_factor
     * @return
     */
    BZ2Compression(enum CompressionType type, int block_size, int work_factor) : Compression(type),
        block_size_(block_size),
        work_factor_(work_factor) {
    }

    virtual ssize_t Compress(void* dest, size_t dest_size, const void* src, size_t src_size) {
        bz_stream stream;

        // Use standard malloc.
        stream.bzalloc = NULL;
        stream.opaque = NULL;
        stream.bzfree = NULL;

        // Initialize stream
        int result = BZ2_bzCompressInit(&stream, block_size_, 0, work_factor_);
        if (result != BZ_OK) {
            return -1;
        } else {
            // Initialize pointers to source and destination buffers.
            stream.next_in = const_cast<char*>(static_cast<const char*>(src));
            stream.avail_in = src_size;
            stream.next_out = static_cast<char*>(dest);
            stream.avail_out = dest_size;

            result = BZ2_bzCompress(&stream, BZ_RUN);

            if (result != BZ_RUN_OK) {
                return -1;
            } else {
                // Do the actual compression as long as there is data to compress.
                while (result != BZ_STREAM_END) {
                    result = BZ2_bzCompress(&stream, BZ_FINISH);
                    if (result != BZ_FINISH_OK && result != BZ_STREAM_END) {
                        // An error of some sort has occured.
                        return -1;
                    }
                }
                ssize_t compressed_size = dest_size - stream.avail_out;
                BZ2_bzCompressEnd(&stream);
                return compressed_size;
            }
        }
    }

    virtual ssize_t Decompress(void* dest, size_t dest_size, const void* src, size_t src_size) {
        bz_stream stream;

        // Use standard malloc.
        stream.bzalloc = NULL;
        stream.opaque = NULL;
        stream.bzfree = NULL;

        // Initialize pointers to compressed data and target buffer.
        stream.next_in = const_cast<char*>(static_cast<const char*>(src));
        stream.avail_in = src_size;
        stream.next_out = static_cast<char*>(dest);
        stream.avail_out = dest_size;

        int result = BZ2_bzDecompressInit(&stream, 0, 0);
        if (result != BZ_OK) {
            return -1;
        } else {
            // Do the actual decompression as long as there is data to decompress.
            while (result != BZ_STREAM_END) {
                result = BZ2_bzDecompress(&stream);
                if (result != BZ_OK && result != BZ_STREAM_END) {
                    BZ2_bzDecompressEnd(&stream);
                    return -1;
                }
            }
            ssize_t result = dest_size - stream.avail_out;
            BZ2_bzDecompressEnd(&stream);
            return result;
        }
    }

    DISALLOW_COPY_AND_ASSIGN(BZ2Compression);
};

/**
 * Wrapper around the lz4 compression
 */
class LZ4Compression : public Compression {
public:
    /**
     * Constructor
     * @param type
     * @return
     */
    LZ4Compression(enum CompressionType type) : Compression(type) {
    }

    virtual ssize_t Compress(void* dest, size_t dest_size, const void* src, size_t src_size) {

        size_t minimal_dest_size = src_size + std::max(static_cast<int>(src_size * 0.04), 8);
        if (dest_size < minimal_dest_size) {
            ERROR("Compression failed: Buffer too small: src size: " << src_size << ", dest size: " << dest_size << ", minimal dest size " << minimal_dest_size);
            return -1;
        }
        // there are no error codes. lz4 cannot fail
        return LZ4_compress(
                const_cast<char*>(static_cast<const char*>(src)),
                static_cast<char*>(dest), src_size);
    }

    virtual ssize_t Decompress(void* dest, size_t dest_size, const void* src, size_t src_size) {
        ssize_t r = LZ4_uncompress_unknownOutputSize(
                const_cast<char*>(static_cast<const char*>(src)),
                static_cast<char*>(dest), src_size, dest_size);
        if (r < 0) {
            return -1;
        }
        return r;
    }

    DISALLOW_COPY_AND_ASSIGN(LZ4Compression);
};

/**
 * Wrapper class around the snappy compression library
 * See http://code.google.com/p/snappy/ for details.
 */
class SnappyCompression : public Compression {
public:

    /**
     * Constructor.
     *
     * @param type
     * @return
     */
    SnappyCompression(enum CompressionType type) : Compression(type) {
    }

    virtual ssize_t Compress(void* dest, size_t dest_size, const void* src, size_t src_size) {
        DCHECK_RETURN(dest, -1, "Destination not set");
        DCHECK_RETURN(src, -1, "Src not set");

        CHECK_RETURN(snappy::MaxCompressedLength(src_size) <= dest_size, -1,
            "Compression failed: Buffer too small: src size: " << src_size << ", dest size: " << dest_size);

        size_t output_length;
        snappy::RawCompress(static_cast<const char*>(src),
            src_size,
            static_cast<char*>(dest),
            &output_length);
        return output_length;
    }

    virtual ssize_t Decompress(void* dest, size_t dest_size, const void* src, size_t src_size) {
        DCHECK_RETURN(dest, -1, "Destination not set");
        DCHECK_RETURN(src, -1, "Src not set");

        size_t l;
        CHECK_RETURN(snappy::GetUncompressedLength(static_cast<const char*>(src), src_size, &l), -1,
            "Failed to uncompress data. Cannot get uncompressed length");
        CHECK_RETURN(l <= dest_size, -1, "Uncompression failed: Buffer too small: src size: " << src_size <<
            ", dest size: " << dest_size <<
            ", uncompressed size " << l);

        CHECK_RETURN(snappy::RawUncompress(static_cast<const char*>(src), src_size,
                static_cast<char*>(dest)), -1,
            "Uncompression failed: Buffer too small: src size: " << src_size <<
            ", dest size: " << dest_size <<
            ", uncompressed size " << l);
        return l;
    }

    DISALLOW_COPY_AND_ASSIGN(SnappyCompression);
};

Compression* Compression::NewCompression(enum CompressionType type) {
    if (type == Compression::COMPRESSION_ZLIB_1) {
        return new ZlibCompression(type, 1);
    } else if (type == Compression::COMPRESSION_ZLIB_3) {
        return new ZlibCompression(type, 3);
    } else if (type == Compression::COMPRESSION_ZLIB_9) {
        return new ZlibCompression(type, 9);
    } else if (type == Compression::COMPRESSION_BZ2) {
        return new BZ2Compression(type, BZ2Compression::kStdBlockSize, BZ2Compression::kStdWorkFactor);
    } else if (type == Compression::COMPRESSION_LZ4) {
        return new LZ4Compression(COMPRESSION_LZ4);
    } else if (type == Compression::COMPRESSION_SNAPPY) {
        return new SnappyCompression(type);
    }
    return NULL;
}

}
}
