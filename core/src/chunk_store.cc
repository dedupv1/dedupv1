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

#include <core/dedup.h>
#include <base/locks.h>

#include <core/chunk_store.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <base/index.h>
#include <core/storage.h>
#include <core/chunker.h>
#include <core/filter.h>
#include <base/strutil.h>
#include <base/timer.h>
#include <base/logging.h>
#include <core/chunk.h>
#include <sstream>

#include <dedupv1_stats.pb.h>

using std::string;
using std::stringstream;
using std::vector;
using dedupv1::base::ProfileTimer;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::log::Log;
using dedupv1::IdleDetector;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::ErrorContext;

LOGGER("ChunkStore");

namespace dedupv1 {
namespace chunkstore {

ChunkStore::ChunkStore() {
    chunk_storage_ = NULL;
}

ChunkStore::~ChunkStore() {
}

ChunkStore::Statistics::Statistics() {
    storage_reads_ = 0;
    storage_real_writes_ = 0;
    storage_total_writes_ = 0;

    storage_reads_bytes_ = 0;
    storage_real_writes_bytes_ = 0;
    storage_total_writes_bytes_ = 0;
}

bool ChunkStore::Init(const string& storage_type) {
    CHECK(storage_type.size() > 0, "Storage type not set");

    this->chunk_storage_ = Storage::Factory().Create(storage_type);
    CHECK(this->chunk_storage_, "Failed to create storage instance \"" << storage_type << "\"");
    return true;
}

bool ChunkStore::SetOption(const string& option_name, const string& option) {
    return this->chunk_storage_->SetOption(option_name, option);
}

StorageSession* ChunkStore::CreateSession() {
    CHECK_RETURN(this->chunk_storage_, NULL, "Chunk storage not set");
    StorageSession* storage_session = this->chunk_storage_->CreateSession();
    CHECK_RETURN(storage_session, NULL, "Cannot create storage session");
    return storage_session;
}

bool ChunkStore::Start(const StartContext& start_context, DedupSystem* system) {
    CHECK(this->chunk_storage_, "Storage not configured");
    return this->chunk_storage_->Start(start_context, system);
}

bool ChunkStore::Run() {
    CHECK(this->chunk_storage_, "Storage not configured");
    return this->chunk_storage_->Run();
}

bool ChunkStore::Stop(const dedupv1::StopContext& stop_context) {
    CHECK(this->chunk_storage_, "Storage not configured");

    return this->chunk_storage_->Stop(stop_context);
}

bool ChunkStore::Flush(dedupv1::base::ErrorContext* ec) {
    CHECK(this->chunk_storage_, "Storage not configured");
    return this->chunk_storage_->Flush(ec);
}

bool ChunkStore::Close() {
    if (this->chunk_storage_) {
        CHECK(this->chunk_storage_->Close(), "Closing chunk storage failed");
        this->chunk_storage_ = NULL;
    }
    delete this;

    return true;
}

bool ChunkStore::WriteBlock(StorageSession* storage_session, ChunkMapping* chunk_mapping,
                            ErrorContext* ec) {
    ProfileTimer timer(this->stats_.time_);

    CHECK(storage_session, "Session not set");
    CHECK(chunk_mapping, "Chunk mapping not set");
    CHECK(chunk_mapping->chunk() != NULL,
        "Chunk not set: " << chunk_mapping->DebugString());

    if (!chunk_mapping->is_known_chunk() &&
        chunk_mapping->data_address() == Storage::ILLEGAL_STORAGE_ADDRESS) {
        // write to storage if necessary
        uint64_t new_address = Storage::ILLEGAL_STORAGE_ADDRESS;
        CHECK(storage_session->WriteNew(chunk_mapping->fingerprint(),
                chunk_mapping->fingerprint_size(), chunk_mapping->chunk()->data(),
                chunk_mapping->chunk()->size(),
                chunk_mapping->is_indexed(),
                &new_address, ec),
            "Storing of new chunk failed: chunk " << chunk_mapping->DebugString());
        CHECK(new_address != Storage::ILLEGAL_STORAGE_ADDRESS, "Write failed: " <<
            "Illegal storage address: " << new_address << ", chunk " << chunk_mapping->DebugString());
        chunk_mapping->set_data_address(new_address);
        this->stats_.storage_real_writes_++;
        this->stats_.storage_real_writes_bytes_ += chunk_mapping->chunk()->size();
    }
    this->stats_.storage_total_writes_++;
    this->stats_.storage_total_writes_bytes_ += chunk_mapping->chunk()->size();

    return true;
}

bool ChunkStore::CheckIfFull() {
    if (!chunk_storage_) {
        return false;
    }
    return chunk_storage_->CheckIfFull();
}

bool ChunkStore::ReadBlock(StorageSession* storage_session,
                           BlockMappingItem* item, byte* buffer, size_t* buffer_size,
                           ErrorContext* ec) {
    ProfileTimer timer(this->stats_.time_);

    CHECK(storage_session, "Session not set");
    CHECK(item, "Item not set");
    CHECK(buffer, "Buffer not set");
    CHECK(buffer_size, "Buffer size not set");
    CHECK(*buffer_size > 0, "Buffer size value not set");

    uint64_t data_address = item->data_address();
    CHECK(storage_session->Read(data_address, item->fingerprint(),
            item->fingerprint_size(), buffer, buffer_size, ec), "Reading of chunk failed: " << item->DebugString());

    item->set_data_address(data_address);
    this->stats_.storage_reads_.fetch_and_increment();
    this->stats_.storage_reads_bytes_.fetch_and_add(*buffer_size);
    return true;
}

string ChunkStore::PrintLockStatistics() {
    return this->chunk_storage_->PrintLockStatistics();
}

bool ChunkStore::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ChunkStoreStatsData data;
    data.set_read_count(this->stats_.storage_reads_bytes_);
    data.set_write_count(this->stats_.storage_total_writes_bytes_);
    data.set_real_write_count(this->stats_.storage_real_writes_bytes_);
    CHECK(ps->Persist(prefix, data), "Failed to persist chunk store stats");
    if (this->chunk_storage_) {
        CHECK(this->chunk_storage_->PersistStatistics(prefix + ".storage", ps),
            "Failed to persist storage stats");
    }
    return true;
}

bool ChunkStore::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ChunkStoreStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore chunk store stats");
    this->stats_.storage_reads_bytes_ = data.read_count();
    this->stats_.storage_total_writes_bytes_ = data.write_count();
    this->stats_.storage_real_writes_bytes_ = data.real_write_count();
    if (this->chunk_storage_) {
        CHECK(this->chunk_storage_->RestoreStatistics(prefix + ".storage", ps),
            "Failed to restore storage stats");
    }
    return true;
}

string ChunkStore::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"storage reads\": " << this->stats_.storage_reads_bytes_  << "," << std::endl;
    sstr << "\"storage real writes\": " << this->stats_.storage_real_writes_bytes_  << "," << std::endl;
    sstr << "\"storage writes\": " << this->stats_.storage_total_writes_bytes_ << "," << std::endl;
    sstr << "\"storage\": " << (chunk_storage_ ? this->chunk_storage_->PrintStatistics() : "null") << std::endl;
    sstr << "}";
    return sstr.str();
}

string ChunkStore::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"chunk store\": " << this->stats_.time_.GetSum() << "," << std::endl;
    sstr << "\"storage\": " << (chunk_storage_ ? this->chunk_storage_->PrintProfile() : "null") << std::endl;
    sstr << "}";
    return sstr.str();
}

string ChunkStore::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"storage\": " << (chunk_storage_ ? this->chunk_storage_->PrintTrace() : "null") << std::endl;
    sstr << "}";
    return sstr.str();
}

}
}
