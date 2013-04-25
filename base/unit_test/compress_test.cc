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
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

#include <string>

#include <gtest/gtest.h>

#include <base/crc32.h>
#include <base/logging.h>
#include <base/compress.h>
#include <base/fileutil.h>
#include <test_util/log_assert.h>

using dedupv1::base::crc;
using std::string;
using dedupv1::base::File;
using dedupv1::base::Compression;
using dedupv1::base::Option;

LOGGER("CompressTest");

class CompressTest : public testing::TestWithParam< ::std::tr1::tuple<Compression::CompressionType, string> > {
protected:
    USE_LOGGING_EXPECTATION();

    Compression* comp;
    File* file;

    Compression::CompressionType type;
    string filename;

    virtual void SetUp() {
        comp = NULL;
        file = NULL;

        ::std::tr1::tuple< Compression::CompressionType, string>  parameters = GetParam();
        type = ::std::tr1::get<0>(parameters);
        filename = ::std::tr1::get<1>(parameters);
    }

    virtual void TearDown() {
        if (comp) {
            delete comp;
        }
        delete file;
    }
};

TEST_P(CompressTest, Create) {
    comp = Compression::NewCompression(type);
    ASSERT_TRUE(comp);
}

TEST_P(CompressTest, Use) {
    comp = Compression::NewCompression(type);
    ASSERT_TRUE(comp);

    file = File::Open(filename, O_RDONLY, 0);
    ASSERT_TRUE(file) << "Failed to open file " << filename;

    Option<off_t> fs = file->GetSize();
    ASSERT_TRUE(fs.valid());
    off_t file_size = fs.value();
    if (file_size > 64 * 1024) {
        file_size = 64 * 1024;
    }
    byte buffer[file_size];
    memset(buffer, 0, file_size);
    ASSERT_EQ(file->Read(0, buffer, file_size),(ssize_t) file_size);
    DEBUG("Readed " << file_size << " bytes from file " << filename);
    DEBUG("Contents: " << crc(buffer, file_size));

    byte compressed_data_buffer[2 * file_size];
    memset(compressed_data_buffer, 0, 2 * file_size);
    ssize_t compressed_size = comp->Compress(compressed_data_buffer, 2 * file_size, buffer, file_size);
    ASSERT_GT(compressed_size, 0) << "Compression should not fail";
    ASSERT_LE(compressed_size, file_size * 1.10);
    DEBUG("Compressed buffer after compress: " << crc(compressed_data_buffer, compressed_size));
    DEBUG("Compression from: " << file_size << " to: " << compressed_size);

    byte buffer2[file_size];
    memset(buffer2, 0, file_size);
    ssize_t uncompressed_size = comp->Decompress(buffer2, file_size, compressed_data_buffer, compressed_size);
    ASSERT_GT(uncompressed_size, 0) << "Decompression should not fail";
    DEBUG("Uncompressed contents: " << crc(buffer2, uncompressed_size));
    ASSERT_EQ(uncompressed_size, (ssize_t) file_size);
    ASSERT_TRUE(memcmp(buffer, buffer2, file_size) == 0);
}

INSTANTIATE_TEST_CASE_P(CompressionRun,
    CompressTest,
    ::testing::Combine(::testing::Values(Compression::COMPRESSION_ZLIB_1,Compression::COMPRESSION_ZLIB_3,
            Compression::COMPRESSION_ZLIB_9, Compression::COMPRESSION_BZ2, Compression::COMPRESSION_SNAPPY, Compression::COMPRESSION_LZ4),
        ::testing::Values("data/dedupv1_test.conf","data/compress_document.doc", "data/1mb-testdata", "data/1mb-zero")));
