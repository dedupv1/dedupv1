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
#include <core/chunk_locks.h>

#include <dedupv1.pb.h>

#include <core/dedup.h>
#include <base/bitutil.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/locks.h>
#include <base/timer.h>
#include <base/hashing_util.h>
#include <core/fingerprinter.h>
#include <base/hash_index.h>

using std::string;
using std::stringstream;
using std::make_pair;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::ToString;
using dedupv1::base::ProfileTimer;
using dedupv1::base::bj_hash;
using dedupv1::Fingerprinter;

namespace dedupv1 {
namespace chunkindex {

LOGGER("ChunkLocks");

ChunkLocks::ChunkLocks() {
    this->chunk_lock_count_ = kDefaultChunkLocks;
    started_ = false;
}

ChunkLocks::Statistics::Statistics() {
    this->lock_free_ = 0;
    this->lock_busy_ = 0;
    this->held_count_ = 0;
}

bool ChunkLocks::SetOption(const string& option_name, const string& option) {
    if (option_name == "count") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->chunk_lock_count_ = ToStorageUnit(option).value();
        CHECK(this->chunk_lock_count_ > 0, "Chunk lock count must be larger than 0");
        return true;
    }
    ERROR("Illegal option: " << option_name);
    return false;
}

bool ChunkLocks::Start(const StartContext& start_context) {
    this->locks_.resize(this->chunk_lock_count_);
    started_ = true;
    return true;
}

int ChunkLocks::GetLockIndex(const void* fp, size_t fp_size) {
    DCHECK_RETURN(fp_size >= sizeof(uint64_t), -1, 
        "Illegal fp size: fp size " << fp_size);
    uint32_t hash_value = 0;
    dedupv1::base::murmur_hash3_x86_32(fp, fp_size, 0, &hash_value);
    return hash_value % this->chunk_lock_count_;
}

bool ChunkLocks::Lock(const void* fp, size_t fp_size) {
    DCHECK(started_, "Chunk locks not started");

    ProfileTimer timer(this->stats_.profiling_lock_);
    try {
        int i = GetLockIndex(fp, fp_size);
        DCHECK(i >= 0, "Failed to get lock index");
        TRACE("Try acquire chunk lock " << i << " of chunk " << Fingerprinter::DebugString(fp, fp_size) << " at " << i);
        DCHECK(i < locks_.size(), "Illegal lock");
        if (locks_[i].try_lock()) {
            this->stats_.lock_free_++;
        } else {
            locks_[i].lock();
            this->stats_.lock_busy_++;
        }
        TRACE("Acquired chunk lock " << i << " of chunk " << Fingerprinter::DebugString(fp, fp_size) << " at " << i);
        this->stats_.held_count_.fetch_and_increment();
    } catch (std::exception& e) {
        ERROR("Lock failed: " << e.what());
        return false;
    }
    return true;
}

bool ChunkLocks::TryLock(const void* fp, size_t fp_size, bool* locked) {
    DCHECK(started_, "Chunk locks not started");

    ProfileTimer timer(this->stats_.profiling_lock_);
    try {
        int i = GetLockIndex(fp, fp_size);
        DCHECK(i >= 0, "Illegal lock index");
        DCHECK(i < locks_.size(), "Illegal lock index");
        if (!locks_[i].try_lock()) {
            TRACE("Negative try lock result");
            if (locked) {
                *locked = false;
            }
            return true;
        }
        if (locked) {
            *locked = true;
        }
        TRACE("Acquired chunk lock " << i << " of chunk " << Fingerprinter::DebugString(fp, fp_size) << " at " << i);
        this->stats_.held_count_.fetch_and_increment();
    } catch (std::exception& e) {
        ERROR("Lock failed: " << e.what());
        return false;
    }
    return true;
}

bool ChunkLocks::Unlock(const void* fp, size_t fp_size) {
    DCHECK(started_, "Chunk locks not started");

    ProfileTimer timer(this->stats_.profiling_lock_);
    try {
        int i = GetLockIndex(fp, fp_size);
        DCHECK(i >= 0, "Illegal lock index");
        DCHECK(i < locks_.size(), "Illegal lock index");
        TRACE("Release block lock " << i << " of chunk " << Fingerprinter::DebugString(fp, fp_size) << " from " << i);
        locks_[i].unlock();
        this->stats_.held_count_.fetch_and_decrement();
    } catch (std::exception& e) {
        ERROR("Lock failed: " << e.what());
        return false;
    }
    return true;
}

string ChunkLocks::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"free\": " << this->stats_.lock_free_ << "," << std::endl;
    sstr << "\"busy\": " << this->stats_.lock_busy_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string ChunkLocks::PrintProfile() {
    stringstream sstr;
    sstr << this->stats_.profiling_lock_.GetSum() << std::endl;
    return sstr.str();
}

}
}
