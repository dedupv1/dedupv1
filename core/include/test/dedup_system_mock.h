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

#ifndef DEDUP_SYSTEM_MOCK_H_
#define DEDUP_SYSTEM_MOCK_H_

#include <gmock/gmock.h>
#include <core/dedup_system.h>

class MockDedupSystem : public dedupv1::DedupSystem {
    public:
        MOCK_METHOD0(Init, bool());
        MOCK_METHOD1(LoadOptions, bool(const std::string& filename_));
        MOCK_METHOD2(SetOption, bool(const std::string& option_name, const std::string& option));

        MOCK_METHOD2(Start, bool(const dedupv1::StartContext& start_context, dedupv1::InfoStore* info_store));
        MOCK_METHOD0(Stop, bool());
        MOCK_METHOD0(Close, bool());

        MOCK_METHOD7(MakeRequest, dedupv1::scsi::ScsiResult(
                dedupv1::Session* session,
                dedupv1::request_type rw,
                uint64_t request_index,
                uint64_t request_offset,
                uint64_t size,
                byte* buffer,
                dedupv1::base::ErrorContext* ec));

        MOCK_METHOD6(FastCopy, dedupv1::scsi::ScsiResult(
                uint64_t src_block_id,
                uint64_t src_offset,
                uint64_t target_block_id,
                uint64_t target_offset,
                uint64_t size,
                dedupv1::base::ErrorContext* ec));

        MOCK_CONST_METHOD0(block_size, uint32_t());

        MOCK_METHOD0(log, dedupv1::log::Log*());
        MOCK_METHOD0(chunk_index, dedupv1::chunkindex::ChunkIndex*());
        MOCK_METHOD0(block_index, dedupv1::blockindex::BlockIndex*());
        MOCK_METHOD0(storage, dedupv1::chunkstore::Storage*());
        MOCK_METHOD0(block_locks, dedupv1::BlockLocks*());
        MOCK_METHOD0(garbage_collector, dedupv1::gc::GarbageCollector*());
        MOCK_METHOD0(filter_chain, dedupv1::filter::FilterChain*());
        MOCK_METHOD0(idle_detector, dedupv1::IdleDetector*());
        MOCK_METHOD0(info_store, dedupv1::InfoStore*());
        MOCK_METHOD0(volume_info, dedupv1::DedupVolumeInfo*());
        MOCK_METHOD0(content_storage, dedupv1::ContentStorage*());
};

#endif /* DEDUP_SYSTEM_MOCK_H_ */
