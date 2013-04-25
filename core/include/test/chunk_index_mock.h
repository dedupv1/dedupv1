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

#ifndef CHUNK_INDEX_MOCK_H_
#define CHUNK_INDEX_MOCK_H_

#include <gmock/gmock.h>
#include <core/chunk_index.h>

class MockChunkIndex : public dedupv1::chunkindex::ChunkIndex {
    public:
        MOCK_METHOD2(SetOption, bool(const std::string&, const std::string& option));

        MOCK_METHOD2(Start, bool(dedupv1::DedupSystem* system, bool create));

        MOCK_METHOD0(Close, bool());
        MOCK_METHOD0(Sync, bool());

        MOCK_METHOD2(Delete, bool(const dedupv1::chunkindex::ChunkMapping& modified_mapping, dedupv1::base::ErrorContext* ec));
        MOCK_METHOD2(Lookup, dedupv1::base::lookup_result(dedupv1::chunkindex::ChunkMapping* modified_mapping, dedupv1::base::ErrorContext* ec));
        MOCK_METHOD2(Put, bool(const dedupv1::chunkindex::ChunkMapping& modified_mapping, dedupv1::base::ErrorContext* ec));
        MOCK_METHOD2(PutOverwrite, bool(const dedupv1::chunkindex::ChunkMapping& modified_mapping, dedupv1::base::ErrorContext* ec));

        MOCK_METHOD3(ChangePinningState, dedupv1::base::lookup_result(const void* key, size_t key_size, bool new_pin_state));
        MOCK_METHOD2(EnsurePersistent, dedupv1::base::put_result(const dedupv1::chunkindex::ChunkMapping& mapping, bool* pinned));

        MOCK_METHOD0(PrintLockStatistics, std::string());
        MOCK_METHOD0(PrintStatistics, std::string());
        MOCK_METHOD0(PrintProfile, std::string());
};

#endif /* CHUNK_INDEX_MOCK_H_ */
