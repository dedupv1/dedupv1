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

#ifndef BLOCK_MAPPING_TEST_H_
#define BLOCK_MAPPING_TEST_H_

#include <gtest/gtest.h>

#include <core/block_mapping.h>
#include <test/log_assert.h>

#include <list>

namespace dedupv1 {
namespace blockindex {

class BlockMappingTest : public testing::TestWithParam< std::pair<int, int> > {
    protected:
    static const size_t BLOCKSIZE_64K;
    static const size_t BLOCKSIZE_256K;

    USE_LOGGING_EXPECTATION();

    BlockMappingTest();

    virtual void SetUp();

    virtual void TearDown();

    public:

    static size_t Append(BlockMapping* m,
            size_t offset, int i, size_t size, uint64_t address);
    static bytestring FingerprintString(byte i);

    static size_t Append(BlockMapping* m,
            size_t offset, byte fp, size_t size, size_t address);

    /**
    * Fills a block mapping with an example mapping.
    * Note that the client is responsible for storing data at the given address if the
    * SUT tries to read the data.
    *
    * @param m
    * @param address
    */
    static void FillTestBlockMapping(BlockMapping* m, unsigned int address = 1);

    /**
    * Fills a block mapping with a large example mapping.
    * Note that the client is responsible for storing data at the given address if the
    * SUT tries to read the data.
    *
    * @param m
    * @param address
    */
    static void FillTestLargeMapping(BlockMapping* m, unsigned int address = 1);

    /**
    * Fills a block mapping with an default or empty mapping.
    * Note that the client is responsible for storing data at the given address if the
    * SUT tries to read the data.
    *
    * @param m
    * @param address
    */
    static void FillDefaultBlockMapping(BlockMapping* m);
};

}
}

#endif /* BLOCK_MAPPING_TEST_H_ */
