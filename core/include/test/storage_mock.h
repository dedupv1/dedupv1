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

#ifndef STORAGE_MOCK_H_
#define STORAGE_MOCK_H_

#include <gmock/gmock.h>
#include <core/storage.h>

/**
 * Mock for a storage session.
 */
class MockStorageSession : public dedupv1::chunkstore::StorageSession {
    public:
        MOCK_METHOD1(Sync, bool(dedupv1::base::ErrorContext* ec));
        MOCK_METHOD7(WriteNew, bool(const void* key, size_t key_size, const void* data,
                size_t data_size, bool is_indexed, uint64_t* address, dedupv1::base::ErrorContext* ec));
        MOCK_METHOD6(Read, bool(uint64_t address, const void* key, size_t key_size,
                void* data, size_t* data_size, dedupv1::base::ErrorContext* ec));
        MOCK_METHOD4(Delete, bool(uint64_t address, const void* key, size_t key_size, dedupv1::base::ErrorContext* ec));
        MOCK_METHOD0(Close, bool());
};

/**
 * Mock for a storage instance.
 */
class MockStorage : public dedupv1::chunkstore::Storage {
    public:
        MOCK_METHOD0(Init, bool());
        MOCK_METHOD2(SetOption, bool(const std::string& option_name, const std::string& option));

        MOCK_METHOD2(Start, bool(const dedupv1::StartContext& start_context, dedupv1::DedupSystem* system));
        MOCK_METHOD0(Stop, bool());
        MOCK_METHOD0(Close, bool());

        MOCK_METHOD0(CreateSession, dedupv1::chunkstore::StorageSession*());

        MOCK_METHOD1(IsCommittedWait, dedupv1::chunkstore::storage_commit_state(uint64_t address));
        MOCK_METHOD1(IsCommitted, dedupv1::chunkstore::storage_commit_state(uint64_t address));

        MOCK_METHOD1(Flush, bool(dedupv1::base::ErrorContext* ec));

        MOCK_METHOD0(GetActiveStorageDataSize, uint64_t());
};

#endif /* STORAGE_MOCK_H_ */
