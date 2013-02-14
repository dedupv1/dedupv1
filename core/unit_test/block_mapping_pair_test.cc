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

#include <test/block_mapping_test.h>

#include <base/logging.h>

#include <gtest/gtest.h>
#include <core/block_mapping.h>
#include <core/block_mapping_pair.h>

LOGGER("BlockMappingTest");

using std::string;

namespace dedupv1 {
namespace blockindex {

class BlockMappingPairTest : public testing::Test {
    protected:
    static const size_t BLOCKSIZE_64K;
    static const size_t BLOCKSIZE_256K;

    USE_LOGGING_EXPECTATION();

    BlockMappingPairTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }
    
};

const size_t BlockMappingPairTest::BLOCKSIZE_64K = 64U * 1024;
const size_t BlockMappingPairTest::BLOCKSIZE_256K = 256U * 1024;

TEST_F(BlockMappingPairTest, Size) {
    BlockMapping m1(1, BLOCKSIZE_64K);
    BlockMappingTest::FillDefaultBlockMapping(&m1);

    BlockMapping m2(1, BLOCKSIZE_64K);
    BlockMappingTest::FillTestBlockMapping(&m2);

    BlockMappingPair mapping_pair(BLOCKSIZE_64K);
    ASSERT_TRUE(mapping_pair.CopyFrom(m1, m2));

    DEBUG("Mapping pair: " << mapping_pair.DebugString());

    BlockMappingPairData data;

    ASSERT_TRUE(mapping_pair.SerializeTo(&data));
    DEBUG("Data " << data.ShortDebugString());
    DEBUG("Data size " << data.ByteSize() << ", minimal data size " << (m2.items().size() * 20));

    ASSERT_GE(data.ByteSize(), m2.items().size() * 20) << "Encoding MUST be wrong";
    ASSERT_LE(data.ByteSize(), m2.items().size() * 20 * 2);
}

TEST_F(BlockMappingPairTest, GetMapping) {
    BlockMapping m1(1, BLOCKSIZE_64K);
    BlockMappingTest::FillDefaultBlockMapping(&m1);

    BlockMapping m2(1, BLOCKSIZE_64K);
    BlockMappingTest::FillTestBlockMapping(&m2);

    BlockMappingPair mapping_pair(BLOCKSIZE_64K);
    ASSERT_TRUE(mapping_pair.CopyFrom(m1, m2));

    DEBUG("Mapping pair: " << mapping_pair.DebugString());
    BlockMapping m3(mapping_pair.GetModifiedBlockMapping(0));

    ASSERT_TRUE(m3.Equals(m2)) << "modified mapping should be reconstructable: original " << m2.DebugString() << ", reconstructed " << m3.DebugString();
}

TEST_F(BlockMappingPairTest, GetMappingAfterSerialization) {
    BlockMapping m1(1, BLOCKSIZE_64K);
    BlockMappingTest::FillDefaultBlockMapping(&m1);

    BlockMapping m2(1, BLOCKSIZE_64K);
    BlockMappingTest::FillTestBlockMapping(&m2);

    BlockMappingPair mapping_pair(BLOCKSIZE_64K);
    ASSERT_TRUE(mapping_pair.CopyFrom(m1, m2));

    BlockMappingPairData data;
    ASSERT_TRUE(mapping_pair.SerializeTo(&data));

    BlockMappingPair mapping_pair2(BLOCKSIZE_64K);
    ASSERT_TRUE(mapping_pair2.CopyFrom(data));

    DEBUG("Mapping pair: " << mapping_pair.DebugString());
    BlockMapping m3(mapping_pair2.GetModifiedBlockMapping(0));

    ASSERT_TRUE(m3.Equals(m2)) << "modified mapping should be reconstructable: original " << m2.DebugString() << ", reconstructed " << m3.DebugString();
}

TEST_F(BlockMappingPairTest, GetDiff) {
    BlockMapping m1(1, BLOCKSIZE_64K);
    BlockMappingTest::FillDefaultBlockMapping(&m1);

    BlockMapping m2(1, BLOCKSIZE_64K);
    BlockMappingTest::FillTestBlockMapping(&m2);

    BlockMappingPair mapping_pair(BLOCKSIZE_64K);
    ASSERT_TRUE(mapping_pair.CopyFrom(m1, m2));

    DEBUG("Mapping pair: " << mapping_pair.DebugString());

    std::map<bytestring, std::pair<int, uint64_t> > diff = mapping_pair.GetDiff();
    for (std::map<bytestring, std::pair<int, uint64_t> >::iterator i = diff.begin(); i != diff.end(); i++) {
        if (Fingerprinter::IsEmptyDataFingerprint(i->first.data(), i->first.size())) {
            ASSERT_EQ(-1, i->second.first);
        } else {
            ASSERT_EQ(1, i->second.first);
        }
    }
}

}
}

