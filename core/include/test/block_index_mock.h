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

#ifndef BLOCK_INDEX_MOCK_H_
#define BLOCK_INDEX_MOCK_H_

#include <gmock/gmock.h>
#include <core/block_index.h>

class MockBlockIndex : public dedupv1::blockindex::BlockIndex {
    private:
    DISALLOW_COPY_AND_ASSIGN(MockBlockIndex);
    public:

    // default constructor
    MockBlockIndex() {
    }

    MOCK_METHOD3(StoreBlock, bool(const dedupv1::blockindex::BlockMapping&, const dedupv1::blockindex::BlockMapping& updated_block_mapping, dedupv1::base::ErrorContext* ec));
    MOCK_METHOD3(ReadBlockInfo, read_result(dedupv1::Session*, dedupv1::blockindex::BlockMapping* block_mapping, dedupv1::base::ErrorContext* ec));
    MOCK_METHOD2(DeleteBlockInfo, dedupv1::base::delete_result(uint64_t, dedupv1::base::ErrorContext* ec));
};

#endif /* BLOCK_INDEX_MOCK_H_ */
