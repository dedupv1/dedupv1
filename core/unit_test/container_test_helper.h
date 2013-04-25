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

#ifndef CONTAINER_TEST_HELPER_H_
#define CONTAINER_TEST_HELPER_H_

#include <string>

#include <core/dedup.h>
#include <core/block_mapping.h>
#include <core/storage.h>
#include <core/dedup_system.h>

class ContainerTestHelper {
    private:
        size_t test_data_size_;
        size_t test_data_count_;

        byte* test_data_;
        uint64_t* addresses_;
        uint64_t* fp_;

        bool state_;
    public:
        ContainerTestHelper(size_t test_data_size, size_t test_data_count);
        ~ContainerTestHelper();

        byte* data(int i);
        uint64_t data_address(int i);
        uint64_t* mutable_data_address(int i);
        bytestring fingerprint(int i);

        bool SetUp();

        bool WriteDefaultData(dedupv1::chunkstore::Storage* storage, dedupv1::chunkindex::ChunkIndex* chunk_index, int offset, int count);
        bool WriteDefaultData(dedupv1::DedupSystem* system, int offset, int count);

        /**
         * Wills the block mapping data with the values written before.
         *
         * @param mapping
         * @return
         */
        bool FillBlockMapping(dedupv1::blockindex::BlockMapping* mapping);
        bool FillSameBlockMapping(dedupv1::blockindex::BlockMapping* m, int i);

        size_t Append(dedupv1::blockindex::BlockMapping* m, size_t offset, int i, size_t size);

        void  LoadContainerDataIntoChunkIndex(dedupv1::DedupSystem* system);

        size_t test_data_count() const {
            return test_data_count_;
        }
};

#endif /* CONTAINER_TEST_HELPER_H_ */
