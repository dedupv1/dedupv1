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
#include <stdlib.h>
#include <fcntl.h>

#include <string>
#include <list>

#include <gtest/gtest.h>

#include <core/dedup.h>
#include <base/locks.h>

#include <base/crc32.h>
#include <base/fileutil.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/compress.h>

#include <base/index.h>
#include <core/chunk_mapping.h>
#include <core/container.h>
#include <core/chunk.h>
#include <core/storage.h>
#include <test_util/log_assert.h>

LOGGER("ContainerTest");

using dedupv1::Fingerprinter;
using dedupv1::base::crc;

namespace dedupv1 {
namespace chunkstore {

class ContainerTest : public testing::Test {
public:
    static size_t const CONTAINER_SIZE = 512 * 1024;
    static size_t const TEST_DATA_SIZE = 256 * 1024;
    static size_t const TEST_DATA_COUNT = 8;
protected:
    USE_LOGGING_EXPECTATION();

    byte test_data[TEST_DATA_COUNT][TEST_DATA_SIZE];
    uint64_t test_adress[TEST_DATA_COUNT];
    uint64_t test_fp[TEST_DATA_COUNT];

    virtual void SetUp() {
        int fd = open("/dev/urandom",O_RDONLY);
        for (size_t i = 0; i < 4; i++) {
            size_t j = 0;
            while (j < TEST_DATA_SIZE) {
                size_t r = read(fd, test_data[i] + j, TEST_DATA_SIZE - j);
                j += r;
            }
            test_fp[i] = i + 1;
            test_adress[i] = 0;
        }
        close(fd);
        fd = open("/dev/zero",O_RDONLY);
        for (size_t i = 4; i < 8; i++) {
            size_t j = 0;
            while (j < TEST_DATA_SIZE) {
                size_t r = read(fd, test_data[i] + j, TEST_DATA_SIZE - j);
                j += r;
            }
            test_fp[i] = i + 1;
            test_adress[i] = 0;
        }
        close(fd);
    }

    void CompressionUniqueTest(dedupv1::base::Compression* comp) {
        ASSERT_TRUE(comp);

        Container container;
        container.Init(0, CONTAINER_SIZE);
        for (int i = 4; i < 8; i++) {
            // Use small items to avoid an overflow
            DEBUG("Add " << i << ", " << crc(test_data[i], 16 * 1024));
            ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, comp))
            << "Add item " << i << " failed";
        }

        for (int i = 4; i < 8; i++) {
            const ContainerItem* item = container.FindItem(&test_fp[i], sizeof(test_fp[i]));
            ASSERT_TRUE(item);
            ASSERT_EQ(item->raw_size(), (size_t) 16 * 1024);

            byte buffer[item->raw_size()];
            ASSERT_TRUE(container.CopyRawData(item, buffer, item->raw_size()));

            DEBUG("Get " << i << ", " << crc(buffer, 16 * 1024));
            ASSERT_TRUE(memcmp(buffer, test_data[i], item->raw_size()) == 0);
        }

        delete comp;
    }

    void CompressionRandomTest(dedupv1::base::Compression* comp) {
        ASSERT_TRUE(comp);

        Container container;
        container.Init(0, CONTAINER_SIZE);
        int count = 1;
        for (int i = 0; i < count; i++) {
            // Use small items to avoid an overflow
            DEBUG("Add " << i << ": " << crc(test_data[i], 16 * 1024));
            ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, comp))
            << "Add item " << i << " failed";
        }

        for (int i = 0; i < count; i++) {
            const ContainerItem* item = container.FindItem(&test_fp[i], sizeof(test_fp[i]));
            ASSERT_TRUE(item);
            ASSERT_EQ(item->raw_size(), (size_t) 16 * 1024);

            byte buffer[item->raw_size()];
            ASSERT_TRUE(container.CopyRawData(item, buffer, item->raw_size()));
            DEBUG("Get " << i << ": " << crc(buffer, 16 * 1024));
            ASSERT_TRUE(memcmp(buffer, test_data[i], item->raw_size()) == 0);
        }

        delete comp;
    }

    virtual void TearDown() {
    }
};

TEST_F(ContainerTest, AddItem) {
    Container container;
    container.Init(0, CONTAINER_SIZE);
    for (int i = 0; i < 4; i++) {
        size_t old_pos = container.data_position();
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]),
              (byte *) test_data[i],
              (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
        ASSERT_GT(container.data_position(), old_pos);
        ASSERT_EQ(container.item_count(), (uint32_t) i + 1);
    }

    for (int i = 0; i < 4; i++) {
        ContainerItem* item = container.FindItem(&test_fp[i], sizeof(test_fp[i]), false);
        ASSERT_TRUE(item);
    }
}

TEST_F(ContainerTest, AddItemRandomFingerprints) {
    Container container;
    container.Init(0, CONTAINER_SIZE);

    test_fp[0] = 123;
    test_fp[1] = 12;
    test_fp[2] = 215;
    test_fp[3] = 4;

    for (int i = 0; i < 4; i++) {
        size_t old_pos = container.data_position();
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]),
              (byte *) test_data[i],
              (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
        ASSERT_GT(container.data_position(), old_pos);
        ASSERT_EQ(container.item_count(), (uint32_t) i + 1);
    }

    for (int i = 0; i < 4; i++) {
        ContainerItem* item = container.FindItem(&test_fp[i], sizeof(test_fp[i]), false);
        ASSERT_TRUE(item);
    }
}

TEST_F(ContainerTest, NoLineFeedInDebugString) {
    Container container;
    container.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    for (int i = 0; i < 4; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    dedupv1::base::File* f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    ASSERT_TRUE(f);
    ASSERT_TRUE(container.StoreToFile(f, 0, true));
    ASSERT_TRUE(f->Close());
    f = NULL;

    std::string debugstring = container.DebugString();
    ASSERT_FALSE(dedupv1::base::strutil::Contains(debugstring, "\n"));
}

TEST_F(ContainerTest, SerializeContainer) {
    Container container;
    container.Init(0, CONTAINER_SIZE);
    for (int i = 0; i < 4; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    ASSERT_TRUE(container.SerializeMetadata(true));

    Container container2;
    container2.Init(0, CONTAINER_SIZE);

    // Transfer data
    memcpy(container2.mutable_data(), container.mutable_data(), CONTAINER_SIZE);

    container2.UnserializeMetadata(true);
    ASSERT_EQ(container2.item_count(), (uint32_t) 4);
    ASSERT_TRUE(container2.Equals(container)) << "Containers should be equal";
}

TEST_F(ContainerTest, CopyFrom) {
    Container container;
    container.Init(0, CONTAINER_SIZE);
    for (int i = 0; i < 4; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    Container container2;
    container2.Init(1, CONTAINER_SIZE);

    ASSERT_TRUE(container2.CopyFrom(container, true));
    ASSERT_TRUE(container2.Equals(container)) << "Containers should be equal";
}

TEST_F(ContainerTest, CompressRandom) {
    dedupv1::base::Compression* comp = dedupv1::base::Compression::NewCompression(dedupv1::base::Compression::COMPRESSION_ZLIB_1);
    ASSERT_NO_FATAL_FAILURE(CompressionRandomTest(comp));
}

TEST_F(ContainerTest, CompressUnique) {
    dedupv1::base::Compression* comp = dedupv1::base::Compression::NewCompression(dedupv1::base::Compression::COMPRESSION_ZLIB_1);
    ASSERT_NO_FATAL_FAILURE(CompressionUniqueTest(comp));
}

TEST_F(ContainerTest, CompressBZ2Random) {
    dedupv1::base::Compression* comp = dedupv1::base::Compression::NewCompression(dedupv1::base::Compression::COMPRESSION_BZ2);
    ASSERT_NO_FATAL_FAILURE(CompressionRandomTest(comp));
}

TEST_F(ContainerTest, CompressBZ2Unique) {
    dedupv1::base::Compression* comp = dedupv1::base::Compression::NewCompression(dedupv1::base::Compression::COMPRESSION_BZ2);
    ASSERT_NO_FATAL_FAILURE(CompressionUniqueTest(comp));
}

TEST_F(ContainerTest, CompressLZ4Random) {
    dedupv1::base::Compression* comp = dedupv1::base::Compression::NewCompression(dedupv1::base::Compression::COMPRESSION_LZ4);
    ASSERT_NO_FATAL_FAILURE(CompressionRandomTest(comp));
}

TEST_F(ContainerTest, CompressLZ4Unique) {
    dedupv1::base::Compression* comp = dedupv1::base::Compression::NewCompression(dedupv1::base::Compression::COMPRESSION_LZ4);
    ASSERT_NO_FATAL_FAILURE(CompressionUniqueTest(comp));
}

TEST_F(ContainerTest, CompressSnappyRandom) {
    dedupv1::base::Compression* comp = dedupv1::base::Compression::NewCompression(dedupv1::base::Compression::COMPRESSION_SNAPPY);
    ASSERT_NO_FATAL_FAILURE(CompressionRandomTest(comp));
}

TEST_F(ContainerTest, CompressSnappyUnique) {
    dedupv1::base::Compression* comp = dedupv1::base::Compression::NewCompression(dedupv1::base::Compression::COMPRESSION_SNAPPY);
    ASSERT_NO_FATAL_FAILURE(CompressionUniqueTest(comp));
}

TEST_F(ContainerTest, StoreAndLoad) {
    Container container;
    container.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    for (int i = 0; i < 4; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    dedupv1::base::File* f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    ASSERT_TRUE(f);
    ASSERT_TRUE(container.StoreToFile(f, 0, true));
    ASSERT_TRUE(f->Close());
    f = NULL;

    f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    ASSERT_TRUE(f);
    Container container2;
    container2.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    ASSERT_TRUE(container2.LoadFromFile(f, 0, true));
    ASSERT_TRUE(f->Close());
    f = NULL;

    ASSERT_TRUE(container.Equals(container2));
}

TEST_F(ContainerTest, AddAfterLoad) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    Container container;
    container.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    for (int i = 0; i < 3; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    dedupv1::base::File* f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    ASSERT_TRUE(f);
    ASSERT_TRUE(container.StoreToFile(f, 0, true));
    ASSERT_TRUE(f->Close());
    f = NULL;

    f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    ASSERT_TRUE(f);
    Container container2;
    container2.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    ASSERT_TRUE(container2.LoadFromFile(f, 0, true));

    ASSERT_FALSE(
        container2.AddItem((byte *) &test_fp[3], sizeof(test_fp[3]), (byte *) test_data[3], (size_t) 16 * 1024, true, NULL)) <<
    "It is not allowed to add items to an loaded container";
    EXPECT_TRUE(f->Close());

}

TEST_F(ContainerTest, AddAfterStore) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    Container container;
    container.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    for (int i = 0; i < 3; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    dedupv1::base::File* f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    ASSERT_TRUE(f);
    ASSERT_TRUE(container.StoreToFile(f, 0, true));
    ASSERT_TRUE(f->Close());
    f = NULL;

    // Add last
    ASSERT_FALSE(container.AddItem((byte *) &test_fp[3], sizeof(test_fp[3]), (byte *) test_data[3], (size_t) 16 * 1024, true, NULL));
}

TEST_F(ContainerTest, StoreMinimalChunks) {
    Container container;
    container.Init(Container::kLeastValidContainerId, Container::kDefaultContainerSize);

    size_t data_size = Chunk::kMinChunkSize;
    size_t key_size = Fingerprinter::kMaxFingerprintSize;
    int i = 0;
    while (container.IsFull(key_size, data_size) == false) {
        // Use small items to avoid an overflow
        byte fp[key_size];
        memset(fp, 0, key_size);
        ASSERT_TRUE(container.AddItem(fp, key_size, test_data[0], data_size, true, NULL));
        i++;
    }
    ASSERT_TRUE(container.SerializeMetadata(true));
    ASSERT_GT(i, 500) << "A normal container should take more than 1000 chunks";
}

TEST_F(ContainerTest, StoreMinimalCompressableChunks) {
    Container container;
    container.Init(Container::kLeastValidContainerId, Container::kDefaultContainerSize);

    size_t data_size = Container::kMinCompressedChunkSize;
    size_t key_size = Fingerprinter::kMaxFingerprintSize;
    int i = 0;
    while (container.IsFull(key_size, data_size) == false) {
        // Use small items to avoid an overflow
        byte fp[key_size];
        memset(fp, 0, key_size);
        ASSERT_TRUE(container.AddItem(fp, key_size, test_data[0], data_size, true, NULL));
        i++;
    }
    ASSERT_TRUE(container.SerializeMetadata(true));
    ASSERT_GT(i, 750) << "A normal container should take more than 1000 chunks";
}

TEST_F(ContainerTest, DeleteItem) {
    Container container;
    container.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    for (int i = 0; i < 4; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    uint32_t old_active_data_size = container.active_data_size();
    ASSERT_GT(old_active_data_size,  4U * 16 * 1024); // no compression for this test, please

    size_t old_item_count = container.item_count();
    ASSERT_TRUE(container.DeleteItem((byte *) &test_fp[2], sizeof(test_fp[2])));

    ASSERT_LE(container.active_data_size(), old_active_data_size - (16 * 1024));

    ASSERT_TRUE(container.FindItem((byte *) &test_fp[2], sizeof(test_fp[2])) == NULL);
    ASSERT_TRUE(container.FindItem((byte *) &test_fp[2], sizeof(test_fp[2]), true) != NULL);
    ASSERT_EQ(container.item_count(), old_item_count - 1);
}

TEST_F(ContainerTest, AddItemAfterDeleteItem) {
    Container container;
    container.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    for (int i = 0; i < 2; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL));
    }
    ASSERT_TRUE(container.DeleteItem((byte *) &test_fp[1], sizeof(test_fp[1])));
    for (int i = 2; i < 4; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL));
    }

    ASSERT_GT(container.active_data_size(), 3U * (16 * 1024));
    ASSERT_LE(container.active_data_size(), 3U * ((16 + 2) * 1024));

    ASSERT_TRUE(container.FindItem((byte *) &test_fp[1], sizeof(test_fp[1])) == NULL);
    ASSERT_TRUE(container.FindItem((byte *) &test_fp[1], sizeof(test_fp[1]), true) != NULL);
}

TEST_F(ContainerTest, ActiveDataSizeAfterStoreLoad) {
    Container container;
    container.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    DEBUG("Init : " << container.active_data_size())
    for (int i = 0; i < 4; i++) {
        DEBUG("After adding " << (i + 1) << ": " << container.active_data_size());
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }
    DEBUG("After adding all" << container.active_data_size());

    uint32_t old_active_data_size = container.active_data_size();
    ASSERT_GT(old_active_data_size,  4U * 16 * 1024); // no compression for this test, please

    dedupv1::base::File* f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    ASSERT_TRUE(f);
    ASSERT_TRUE(container.StoreToFile(f, 0, true));
    ASSERT_TRUE(f->Close());
    f = NULL;

    f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    ASSERT_TRUE(f);
    Container container2;
    container2.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    ASSERT_TRUE(container2.LoadFromFile(f, 0, true));
    ASSERT_TRUE(f->Close());
    f = NULL;

    DEBUG("After Load: " << container.active_data_size());
    ASSERT_EQ(container2.active_data_size(), old_active_data_size);
}

TEST_F(ContainerTest, DeleteItemAfterLoad) {
    Container container;
    container.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    for (int i = 0; i < 4; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    uint32_t old_active_data_size = container.active_data_size();
    ASSERT_GT(old_active_data_size,  4U * 16 * 1024); // no compression for this test, please

    ASSERT_TRUE(container.DeleteItem((byte *) &test_fp[2], sizeof(test_fp[2])));

    dedupv1::base::File* f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    ASSERT_TRUE(f);
    ASSERT_TRUE(container.StoreToFile(f, 0, true));
    ASSERT_TRUE(f->Close());
    f = NULL;

    f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    ASSERT_TRUE(f);
    Container container2;
    container2.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    ASSERT_TRUE(container2.LoadFromFile(f, 0, true));
    ASSERT_TRUE(f->Close());
    f = NULL;

    ASSERT_LT(container2.active_data_size(), old_active_data_size - (16 * 1024));
    ASSERT_GE(container.active_data_size(), old_active_data_size - ((16 + 2) * 1024));

    ASSERT_TRUE(container2.FindItem((byte *) &test_fp[2], sizeof(test_fp[2])) == NULL);
    ASSERT_TRUE(container2.FindItem((byte *) &test_fp[2], sizeof(test_fp[2]), true) != NULL);
}

TEST_F(ContainerTest, MergeContainer) {
    Container container1;
    container1.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    for (int i = 0; i < 2; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container1.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    Container container2;
    container2.Init(Container::kLeastValidContainerId + 1, CONTAINER_SIZE);
    for (int i = 2; i < 4; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container2.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    Container new_container;
    new_container.Init(Storage::ILLEGAL_STORAGE_ADDRESS, CONTAINER_SIZE);

    ASSERT_TRUE(new_container.MergeContainer(container1, container2));
    ASSERT_EQ(new_container.primary_id(), container1.primary_id());
    ASSERT_TRUE(new_container.HasId(container1.primary_id()));
    ASSERT_TRUE(new_container.HasId(container2.primary_id()));

    for (int i = 0; i < 4; i++) {
        ContainerItem* new_item = new_container.FindItem((byte *) &test_fp[i], sizeof(test_fp[i]));
        ASSERT_TRUE(new_item);

        ASSERT_EQ(new_item->raw_size(), 16 * 1024U);

        byte result[16 * 1024];
        memset(result, 0, 16 * 1024);
        ASSERT_TRUE(new_container.CopyRawData(new_item, result, 16 * 1024));
        ASSERT_TRUE(memcmp(result, (byte *) test_data[i], 16 * 1024) == 0);
    }
}

TEST_F(ContainerTest, MergeContainerSwitched) {
    Container container1;
    container1.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    for (int i = 0; i < 2; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container1.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    Container container2;
    container2.Init(Container::kLeastValidContainerId + 1, CONTAINER_SIZE);
    for (int i = 2; i < 4; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container2.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    Container new_container;
    new_container.Init(Storage::ILLEGAL_STORAGE_ADDRESS, CONTAINER_SIZE);

    ASSERT_TRUE(new_container.MergeContainer(container2, container1));
    ASSERT_EQ(new_container.primary_id(), container1.primary_id());
    ASSERT_TRUE(new_container.HasId(container1.primary_id()));
    ASSERT_TRUE(new_container.HasId(container2.primary_id()));

    for (int i = 0; i < 4; i++) {
        ContainerItem* new_item = new_container.FindItem((byte *) &test_fp[i], sizeof(test_fp[i]));
        ASSERT_TRUE(new_item);

        ASSERT_EQ(new_item->raw_size(), 16 * 1024U);

        byte result[16 * 1024];
        memset(result, 0, 16 * 1024);
        ASSERT_TRUE(new_container.CopyRawData(new_item, result, 16 * 1024));
        ASSERT_TRUE(memcmp(result, (byte *) test_data[i], 16 * 1024) == 0);
    }
}

TEST_F(ContainerTest, LoadOnlyMetaData) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    Container container;
    container.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    for (int i = 0; i < 3; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    dedupv1::base::File* f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    ASSERT_TRUE(f);
    ASSERT_TRUE(container.StoreToFile(f, 0, true));
    ASSERT_TRUE(f->Close());
    f = NULL;

    f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    ASSERT_TRUE(f);
    Container container2;
    container2.InitInMetadataOnlyMode(Container::kLeastValidContainerId, CONTAINER_SIZE);
    ASSERT_TRUE(container2.LoadFromFile(f, 0, true));

    ContainerItem* item = container2.FindItem((byte *) &test_fp[0], sizeof(test_fp[0]));
    ASSERT_TRUE(item) << "Should find the item";
    ASSERT_TRUE(container2.is_metadata_only());

    byte result[16 * 1024];
    memset(result, 0, 16 * 1024);
    ASSERT_FALSE(container2.CopyRawData(item, result, 16 * 1024)) << "Data access should fail in metadata mode";

    ASSERT_TRUE(f->Close());
    f = NULL;
}

TEST_F(ContainerTest, CopyFromMetaData) {
    Container container;
    container.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    for (int i = 0; i < 3; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }

    Container container2;
    container2.InitInMetadataOnlyMode(Container::kLeastValidContainerId, CONTAINER_SIZE);
    ASSERT_TRUE(container2.CopyFrom(container, true));

    ContainerItem* item = container2.FindItem((byte *) &test_fp[0], sizeof(test_fp[0]));
    ASSERT_TRUE(item) << "Should find the item";
    ASSERT_TRUE(container2.is_metadata_only());
}

TEST_F(ContainerTest, CommitTime) {
    Container container;
    container.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);

    for (int i = 0; i < 4; i++) {
        // Use small items to avoid an overflow
        ASSERT_TRUE(container.AddItem((byte *) &test_fp[i], sizeof(test_fp[i]), (byte *) test_data[i], (size_t) 16 * 1024, true, NULL))
        << "Add item " << i << " failed";
    }
    ASSERT_EQ(container.commit_time(), 0) << "Commit time should not be set now";

    dedupv1::base::File* f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    ASSERT_TRUE(f);
    ASSERT_TRUE(container.StoreToFile(f, 0, true));
    ASSERT_TRUE(f->Close());
    f = NULL;

    std::time_t t = container.commit_time();
    ASSERT_GT(t, 0) << "Commit time should be set now";

    f = dedupv1::base::File::Open("work/container", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    ASSERT_TRUE(f);
    Container container2;
    container2.Init(Container::kLeastValidContainerId, CONTAINER_SIZE);
    ASSERT_TRUE(container2.LoadFromFile(f, 0, true));
    ASSERT_EQ(container2.commit_time(), t) << "Commit time should be preserved";

    ASSERT_TRUE(f->Close());
    f = NULL;
}

}
}
