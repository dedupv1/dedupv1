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

#include <core/block_locks.h>

#include <set>
#include <map>

#include <dedupv1.pb.h>

#include <core/dedup.h>
#include <base/bitutil.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/locks.h>
#include <base/timer.h>
#include <base/hashing_util.h>
#include <core/block_mapping.h>

using std::map;
using std::string;
using std::stringstream;
using std::list;
using std::set;
using std::make_pair;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::ToString;
using dedupv1::base::ProfileTimer;
using dedupv1::base::bj_hash;

namespace dedupv1 {

LOGGER("BlockLocks");

BlockLocks::BlockLocks() {
    this->block_lock_count_ = kDefaultBlockLocks;
}

BlockLocks::Statistics::Statistics() {
    this->block_lock_read_free_ = 0;
    this->block_lock_read_busy_ = 0;
    this->block_lock_write_busy_ = 0;
    this->block_lock_write_free_ = 0;
    this->write_held_count_ = 0;
    this->read_held_count_ = 0;
    read_waiting_count_ = 0;
    write_waiting_count_ = 0;
}

bool BlockLocks::SetOption(const string& option_name, const string& option) {
    CHECK(option.size() > 0, "Option not set");
    CHECK(option_name.size() > 0, "Option name not set");

    if (option_name == "count") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->block_lock_count_ = ToStorageUnit(option).value();
        CHECK(this->block_lock_count_ > 0, "Block lock count must be larger than 0");
        return true;
    }

    ERROR("Illegal option: " << option_name);
    return false;
}

bool BlockLocks::Start() {
    this->block_locks_.Init(this->block_lock_count_);
    this->lock_holder_.resize(this->block_lock_count_);
    for (size_t i = 0; i < this->lock_holder_.size(); i++) {
        this->lock_holder_[i] = kLockNotHeld;
    }
    return true;
}

unsigned int BlockLocks::GetLockIndex(uint64_t block_id) {
    return block_id % this->block_lock_count_;
}

bool BlockLocks::WriteLock(uint64_t block_id, const char* function, const char* file, int line) {
    DCHECK(block_id != dedupv1::blockindex::BlockMapping::ILLEGAL_BLOCK_ID, "Illegal block id: " << block_id);
    DCHECK(!block_locks_.empty(), "Block locks not started");

    ProfileTimer timer(this->stats_.profiling_lock_);

    unsigned int i = GetLockIndex(block_id);
    stats_.write_waiting_count_++;
    CHECK(this->block_locks_.Get(i)->AcquireWriteLockWithStatistics_(
            &this->stats_.block_lock_write_free_,
            &this->stats_.block_lock_write_busy_,
            function, file, line), "Failed to acquire block index write lock: block id " << block_id << ", block lock " << i);
    stats_.write_waiting_count_--;
    DEBUG("Acquired block lock " << i << " of block " << block_id << " at " << this->block_locks_.Get(i)->DebugString());
    this->lock_holder_[i] = block_id;
    this->stats_.write_held_count_.fetch_and_increment();
    return true;
}

bool BlockLocks::WriteUnlocks(const list<uint64_t>& blocks, const char* function, const char* file, int line) {
    DCHECK(!block_locks_.empty(), "Block locks not started");

    ProfileTimer timer(this->stats_.profiling_lock_);

    set<unsigned int> locks;
    for (list<uint64_t>::const_iterator i = blocks.begin(); i != blocks.end(); i++) {
        locks.insert(GetLockIndex(*i));
    }

    bool failed = false;
    set<unsigned int>::iterator j;
    for (j = locks.begin(); j != locks.end(); j++) {

        DCHECK(this->block_locks_.Get(*j)->IsHeldForWrites(),
                "Illegal unlock: Lock not held by current thread: " <<
                this->block_locks_.Get(*j)->DebugString());

        this->lock_holder_[*j] = kLockNotHeld;
        if (!this->block_locks_.Get(*j)->ReleaseLock_(
                function, file, line)) {
            ERROR("Failed to release block index write lock: block lock " << *j);
            failed = true;
        }

        DEBUG("Released block lock " << *j << ": " << this->block_locks_.Get(*j)->DebugString());

        this->stats_.write_held_count_.fetch_and_decrement();
    }

    return !failed;
}

bool BlockLocks::TryWriteLocks(const list<uint64_t>& blocks, list<uint64_t>* locked_blocks, list<uint64_t>* unlocked_blocks, const char* function, const char* file, int line) {
    DCHECK(!block_locks_.empty(), "Block locks not started");
    DCHECK(locked_blocks, "Locked blocks not set");
    DCHECK(unlocked_blocks, "Unlocked blocks not set");

    ProfileTimer timer(this->stats_.profiling_lock_);

    // a set is a) unique and b) ordered
    map<unsigned int, list<uint64_t> > locks;
    for (list<uint64_t>::const_iterator i = blocks.begin(); i != blocks.end(); i++) {
        locks[GetLockIndex(*i)].push_back(*i);
    }
    list<unsigned int> locked_locks;

    bool failed = false;
    map<unsigned int, list<uint64_t> >::iterator i;
    for (i = locks.begin(); i != locks.end(); i++) {
        unsigned int lock_index = i->first;
        bool locked = false;
        if (!this->block_locks_.Get(lock_index)->TryAcquireWriteLock_(&locked,
                function, file, line)) {
            ERROR("Failed to acquire block index write lock: block lock " << i->first);
            failed = true;
            break;
        }
        if (locked) {
            DEBUG("Acquired batch block lock " << lock_index << ": " << this->block_locks_.Get(lock_index)->DebugString());
            locked_locks.push_back(lock_index);
            locked_blocks->insert(locked_blocks->end(), i->second.begin(), i->second.end());
            this->lock_holder_[i->first] = kLockHeldByUnknown;
            this->stats_.write_held_count_.fetch_and_increment();
        } else {
            DEBUG("Tried to acquire batch block lock " << lock_index << ": " << this->block_locks_.Get(lock_index)->DebugString());
            unlocked_blocks->insert(unlocked_blocks->end(), i->second.begin(), i->second.end());
        }
    }

    if (failed) {
        // clean up
        list<unsigned int>::iterator j;
        for (j = locked_locks.begin(); j != locked_locks.end(); j++) {
            this->lock_holder_[*j] = kLockNotHeld;
            if (!this->block_locks_.Get(*j)->ReleaseLock_(
                    function, file, line)) {
                ERROR("Failed to release block index write lock: block lock " << *j);
            }

            this->stats_.write_held_count_.fetch_and_decrement();
        }
        locked_blocks->clear();
        unlocked_blocks->clear();
    }
    return !failed;
}

bool BlockLocks::TryWriteLock(uint64_t block_id, bool* locked, const char* function, const char* file, int line) {
    DCHECK(block_id != dedupv1::blockindex::BlockMapping::ILLEGAL_BLOCK_ID, "Illegal block id: " << block_id);
    DCHECK(!block_locks_.empty(), "Block locks not started");

    ProfileTimer timer(this->stats_.profiling_lock_);

    unsigned int i = GetLockIndex(block_id);
    CHECK(this->block_locks_.Get(i)->TryAcquireWriteLock_(locked,
            function, file, line), "Failed to acquire block index write lock: block id " << block_id << ", block lock " << i);
    if (*locked) {
        DEBUG("Acquired block lock " << i << " of block " << block_id << " at " <<
            this->block_locks_.Get(i)->DebugString());
    }
    if (*locked) {
        this->stats_.write_held_count_.fetch_and_increment();
        this->lock_holder_[i] = block_id;
    }
    return true;
}

bool BlockLocks::IsHeldForWrites(uint64_t block_id) {
    DCHECK(block_id != dedupv1::blockindex::BlockMapping::ILLEGAL_BLOCK_ID, "Illegal block id: " << block_id);
    DCHECK(!block_locks_.empty(), "Block locks not started");

    unsigned int i = GetLockIndex(block_id);
    return this->block_locks_.Get(i)->IsHeldForWrites();
}

bool BlockLocks::WriteUnlock(uint64_t block_id, LOCK_LOCATION_PARAM) {
    DCHECK(block_id != dedupv1::blockindex::BlockMapping::ILLEGAL_BLOCK_ID, "Illegal block id: " << block_id);
    DCHECK(!block_locks_.empty(), "Block locks not started");

    ProfileTimer timer(this->stats_.profiling_lock_);

    unsigned int i = GetLockIndex(block_id);
    DEBUG("Release block lock " << i << " of block " <<  block_id << " from " <<
        this->block_locks_.Get(i)->DebugString() <<
        ", function " << (function ? function : "") <<
        ", file " << (file ? file_basename(file) : "") <<
        ", line " << line << "]");
    if (this->lock_holder_[i] != block_id) {
        WARNING("Lock release by wrong block id: " <<
            "block id " << block_id <<
            ", holder block " << (this->lock_holder_[i] == kLockNotHeld ? "<not held>" : ToString(this->lock_holder_[i])) <<
            ", function " << (function ? function : "") <<
            ", file " << (file ? file_basename(file) : "") <<
            ", line " << line << "]");
#ifndef NDEBUG
        return false;
#endif
    }
    this->lock_holder_[i] = kLockNotHeld;
    CHECK(this->block_locks_.Get(i)->ReleaseLock(), "Lock unlock failed");
    this->stats_.write_held_count_.fetch_and_decrement();
    return true;
}

bool BlockLocks::ReadUnlock(uint64_t block_id, LOCK_LOCATION_PARAM) {
    DCHECK(block_id != dedupv1::blockindex::BlockMapping::ILLEGAL_BLOCK_ID, "Illegal block id: " << block_id);
    DCHECK(!block_locks_.empty(), "Block locks not started");

    ProfileTimer timer(this->stats_.profiling_lock_);

    unsigned int i = GetLockIndex(block_id);
    DEBUG("Release block lock " << i << " of block " <<  block_id << " from " <<
        this->block_locks_.Get(i)->DebugString() <<
        ", function " << (function ? function : "") <<
        ", file " << (file ? file_basename(file) : "") <<
        ", line " << line << "]");

    this->lock_holder_[i] = kLockNotHeld;
    CHECK(this->block_locks_.Get(i)->ReleaseLock(), "Lock unlock failed");
    this->stats_.read_held_count_.fetch_and_decrement();
    return true;
}

bool BlockLocks::ReadLock(uint64_t block_id, const char* function, const char* file, int line) {
    DCHECK(block_id != dedupv1::blockindex::BlockMapping::ILLEGAL_BLOCK_ID, "Illegal block id: " << block_id);
    DCHECK(!block_locks_.empty(), "Block locks not started");

    ProfileTimer timer(this->stats_.profiling_lock_);

    unsigned int i = GetLockIndex(block_id);
    stats_.read_waiting_count_++;
    CHECK(this->block_locks_.Get(i)->AcquireReadLockWithStatistics_(
            &this->stats_.block_lock_read_free_,
            &this->stats_.block_lock_read_busy_,
            function, file, line), "Failed to acquire block index read lock: block id " << block_id << ", block lock " << i);
    this->lock_holder_[i] = kLockHeldByUnknown;
    stats_.read_waiting_count_--;
    this->stats_.read_held_count_.fetch_and_increment();
    return true;
}

bool BlockLocks::TryReadLock(uint64_t block_id, bool* locked, const char* function, const char* file, int line) {
    DCHECK(block_id != dedupv1::blockindex::BlockMapping::ILLEGAL_BLOCK_ID, "Illegal block id: " << block_id);
    DCHECK(!block_locks_.empty(), "Block locks not started");

    ProfileTimer timer(this->stats_.profiling_lock_);

    unsigned int i = GetLockIndex(block_id);
    CHECK(this->block_locks_.Get(i)->TryAcquireReadLock_(locked, function, file, line),
        "Failed to acquire block index read lock: block id " << block_id << ", block lock " << i);
    if (*locked) {
        this->stats_.read_held_count_.fetch_and_increment();
        this->lock_holder_[i] = kLockHeldByUnknown;
    }
    return true;
}

string BlockLocks::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"write held count\": " << this->stats_.write_held_count_ << "," << std::endl;
    sstr << "\"read held count\": " << this->stats_.read_held_count_ << "," << std::endl;
    sstr << "\"write waiting count\": " << this->stats_.write_waiting_count_ << "," << std::endl;
    sstr << "\"read waiting count\": " << this->stats_.read_waiting_count_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string BlockLocks::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"read free\": " << this->stats_.block_lock_read_free_ << "," << std::endl;
    sstr << "\"read busy\": " << this->stats_.block_lock_read_busy_ << "," << std::endl;
    sstr << "\"write free\": " << this->stats_.block_lock_write_free_ << "," << std::endl;
    sstr << "\"write busy\": " << this->stats_.block_lock_write_busy_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string BlockLocks::PrintProfile() {
    stringstream sstr;
    sstr << this->stats_.profiling_lock_.GetSum() << std::endl;
    return sstr.str();
}

}
