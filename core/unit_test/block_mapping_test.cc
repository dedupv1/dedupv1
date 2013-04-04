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

#include "block_mapping_test.h"

#include <base/logging.h>

#include <gtest/gtest.h>
#include <core/block_mapping.h>

LOGGER("BlockMappingTest");

using std::string;

namespace dedupv1 {
namespace blockindex {

const size_t BlockMappingTest::BLOCKSIZE_64K = 64U * 1024;
const size_t BlockMappingTest::BLOCKSIZE_256K = 256U * 1024;

BlockMappingTest::BlockMappingTest() {
}

void BlockMappingTest::SetUp() {
}

void BlockMappingTest::TearDown() {
}

bytestring BlockMappingTest::FingerprintString(byte fp) {
    byte f[20];
    memset(f, fp, 20);
    bytestring s;
    s.assign(f, 20);
    return s;
}

size_t BlockMappingTest::Append(BlockMapping* m, size_t offset, int i, size_t size, uint64_t address) {
    BlockMappingItem item(0, size);
    item.fp_size_ = 20;
    item.data_address_ = address;
    memset(item.fp_, i, 20);
    if (!m->Append(offset, item)) {
        return 0;
    }
    return offset + item.size_;
}

void BlockMappingTest::FillTestBlockMapping(BlockMapping* m,
                                            unsigned int address) {
    size_t offset = 0;
    int i = 0;

    offset = Append(m, offset, i, 6179, address);
    ASSERT_GT(offset, 0U) << "Append failed";
    i++;

    offset = Append(m, offset, i, 7821, address);
    ASSERT_GT(offset, 0U) << "Append failed";
    i++;

    offset = Append(m, offset, i, 4723, address);
    ASSERT_GT(offset, 0U) << "Append failed";
    i++;

    offset = Append(m, offset, i, 2799, address);
    ASSERT_GT(offset, 0U) << "Append failed";
    i++;

    offset = Append(m, offset, i, 4822, address);
    ASSERT_GT(offset, 0U) << "Append failed";
    i++;

    offset = Append(m, offset, i, 13060, address);
    ASSERT_GT(offset, 0U) << "Append failed";
    i++;

    offset = Append(m, offset, i, 5194, address);
    ASSERT_GT(offset, 0U) << "Append failed";
    i++;

    offset = Append(m, offset, i, 7200, address);
    ASSERT_GT(offset, 0U) << "Append failed";
    i++;

    offset = Append(m, offset, i, 4540, address);
    ASSERT_GT(offset, 0U) << "Append failed";
    i++;

    offset = Append(m, offset, i, 4083, address);
    ASSERT_GT(offset, 0U) << "Append failed";
    i++;

    offset = Append(m, offset, i, 5115, address);
    ASSERT_GT(offset, 0U) << "Append failed";
    i++;
}

void BlockMappingTest::FillTestLargeMapping(BlockMapping* m,
                                            unsigned int address) {
    size_t offset = 0;
    int i = 0;
    int j = 0;

    for (j = 0; j < 4; j++) {

        offset = Append(m, offset, i, 6179, address);
        ASSERT_GT(offset, 0U) << "Append failed";
        i++;

        offset = Append(m, offset, i, 7821, address);
        ASSERT_GT(offset, 0U) << "Append failed";
        i++;

        offset = Append(m, offset, i, 4723, address);
        ASSERT_GT(offset, 0U) << "Append failed";
        i++;

        offset = Append(m, offset, i, 2799, address);
        ASSERT_GT(offset, 0U) << "Append failed";
        i++;

        offset = Append(m, offset, i, 4822, address);
        ASSERT_GT(offset, 0U) << "Append failed";
        i++;

        offset = Append(m, offset, i, 13060, address);
        ASSERT_GT(offset, 0U) << "Append failed";
        i++;

        offset = Append(m, offset, i, 5194, address);
        ASSERT_GT(offset, 0U) << "Append failed";
        i++;

        offset = Append(m, offset, i, 7200, address);
        ASSERT_GT(offset, 0U) << "Append failed";
        i++;

        offset = Append(m, offset, i, 4540, address);
        ASSERT_GT(offset, 0U) << "Append failed";
        i++;

        offset = Append(m, offset, i, 4083, address);
        ASSERT_GT(offset, 0U) << "Append failed";
        i++;

        offset = Append(m, offset, i, 5115, address);
        ASSERT_GT(offset, 0U) << "Append failed";
        i++;
    }
}

void BlockMappingTest::FillDefaultBlockMapping(BlockMapping* m) {
    ASSERT_TRUE(m->FillEmptyBlockMapping());
    ASSERT_TRUE(m->Check());
}

TEST_P(BlockMappingTest, Append) {
    int offset = GetParam().first;
    int size = GetParam().second;

    BlockMapping m(1, BLOCKSIZE_64K);
    BlockMappingTest::FillTestBlockMapping(&m);

    BlockMappingItem item(0, size);
    item.set_data_address(32);
    ASSERT_TRUE(m.Append(offset, item));
    ASSERT_TRUE(m.Check());
    DEBUG("Result: " << m.DebugString());
}

TEST_F(BlockMappingTest, Size) {
    BlockMapping m(1, BLOCKSIZE_64K);
    BlockMappingTest::FillTestBlockMapping(&m);

    BlockMappingData data;
    ASSERT_TRUE(m.SerializeTo(&data, true, false));
    DEBUG("Data " << data.ShortDebugString());

    DEBUG("Data size " << data.ByteSize() << ", minimal data size " << (m.items().size() * 20));
}

}
}
