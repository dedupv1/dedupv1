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

#ifndef CONTAINER_STORAGE_MOCK_H_
#define CONTAINER_STORAGE_MOCK_H_

#include <gmock/gmock.h>
#include <core/storage.h>
#include <core/container_storage.h>

/**
 * Mock of the container storage.
 */
class MockContainerStorage : public dedupv1::chunkstore::ContainerStorage {
    typedef std::pair<dedupv1::base::lookup_result, ContainerStorageAddressData> lookup_type;
    public:
    MOCK_METHOD0(Init, bool());

    MOCK_METHOD2(Start, bool(const dedupv1::StartContext& start_context, dedupv1::DedupSystem* system));
    MOCK_METHOD0(Stop, bool());

    MOCK_METHOD1(IsCommitted, dedupv1::chunkstore::storage_commit_state(uint64_t address));
    MOCK_METHOD3(LookupContainerAddress, lookup_type(uint64_t, dedupv1::base::ReadWriteLock**, bool));

    MOCK_METHOD1(Flush, bool(dedupv1::base::ErrorContext* ec));
    MOCK_METHOD4(DeleteChunk, bool(uint64_t address, const void* key, size_t key_size, dedupv1::base::ErrorContext* ec));
    MOCK_METHOD3(DeleteChunks, bool(uint64_t address, const std::list<bytestring>& list, dedupv1::base::ErrorContext* ec));
    MOCK_METHOD3(TryMergeContainer, bool(uint64_t address_1, uint64_t address_2, bool*));
    MOCK_METHOD2(TryDeleteContainer, bool(uint64_t address, bool*));

    MOCK_METHOD0(log, dedupv1::log::Log*());
};


#endif /* CONTAINER_STORAGE_MOCK_H_ */
