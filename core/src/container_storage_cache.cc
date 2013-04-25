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

#include <core/container_storage_cache.h>

#include <sstream>

#include <core/container.h>
#include <core/container_storage.h>
#include <base/logging.h>
#include <base/locks.h>
#include <base/strutil.h>

#include "dedupv1_stats.pb.h"

using std::string;
using std::stringstream;
using std::vector;
using std::set;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::ProfileTimer;
using dedupv1::base::ReadWriteLock;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::lookup_result;
using dedupv1::base::strutil::ToString;
using tbb::tick_count;
using tbb::concurrent_hash_map;
using tbb::spin_rw_mutex;
using tbb::spin_mutex;

LOGGER("ContainerStorageCache");

namespace dedupv1 {
namespace chunkstore {

ContainerStorageReadCache::ContainerStorageReadCache(ContainerStorage* storage) {
    this->storage_ = storage;
    this->read_cache_size_ = kDefaultReadCacheSize;
}

ContainerStorageReadCache::Statistics::Statistics() {
    this->read_cache_lock_busy_ = 0;
    this->read_cache_lock_free_ = 0;
    this->cache_checks_ = 0;
    this->cache_updates_ = 0;
    this->cache_hits_ = 0;
    this->cache_miss_ = 0;
}

bool ContainerStorageReadCache::SetOption(const string& option_name, const string& option) {
    CHECK(read_cache_.size() == 0, "Read cache already started");

    if (option_name == "size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->read_cache_size_ = ToStorageUnit(option).value();
        CHECK(this->read_cache_size_ != 0, "Failed to set read cache size: size must be larger than 0");
        return true;
    }
    ERROR("Illegal option: " << option_name);
    return false;
}

bool ContainerStorageReadCache::Start() {
    CHECK(read_cache_.size() == 0, "Read cache already started");

    // read cache containers
    this->read_cache_.resize(this->read_cache_size_);
    for (int i = 0; i < this->read_cache_size_; i++) {
        this->read_cache_[i] = new Container(Storage::ILLEGAL_STORAGE_ADDRESS, this->storage_->GetContainerSize(), false);
    }
    // read cache locks
    this->read_cache_lock_.Init(this->read_cache_size_);
    this->read_cache_used_time_.resize(this->read_cache_size_);

    // read cache replacement time data
    for (int i = 0; i < this->read_cache_size_; i++) {
        this->read_cache_used_time_[i] = tick_count::now();
    }

    return true;
}

bool ContainerStorageReadCache::Close() {
    if (this->read_cache_.size() > 0) {
        vector<Container*>::iterator j;
        for (j = this->read_cache_.begin(); j != this->read_cache_.end(); j++) {
            Container* container = *j;
            if (container != NULL) {
                delete container;
            }
        }
        this->read_cache_.clear();
    }
    this->read_cache_used_time_.clear();
    return true;
}

enum lookup_result ContainerStorageReadCache::CheckCache(uint64_t container_id,
                                                         const Container** container,
                                                         bool no_update,
                                                         bool write_lock,
                                                         CacheEntry* entry) {
    DCHECK_RETURN(container, LOOKUP_ERROR, "Container not set");
    DCHECK_RETURN(entry, LOOKUP_ERROR, "Cache entry not set");

    ProfileTimer cache_timer(this->stats_.cache_check_time_);
    this->stats_.cache_checks_.fetch_and_increment();

    int read_cache_index = 0;
    if (no_update) {
        concurrent_hash_map<uint64_t, int>::const_accessor a;
        if (!this->reverse_cache_map_.find(a, container_id)) {
            // not found
            this->stats_.cache_miss_.fetch_and_increment();
            DEBUG("Didn't found container in read cache: container id " << container_id << ", no update");
            return LOOKUP_NOT_FOUND;
        }
        read_cache_index = a->second;
        a.release();
    } else {
        concurrent_hash_map<uint64_t, int>::accessor a;
        if (this->reverse_cache_map_.insert(a, container_id)) {
            // not found
            this->stats_.cache_miss_.fetch_and_increment();

            // we should free a place to use and than fill it later with data
            // we place is locked so that other threads accessing the same container is wait
            // Note: If something went wrong, the caller has to release the place under all circumstances

            if (!ReuseCacheLine(container_id, &a, entry)) {
                if (entry) {
                    entry->clear();
                }
                reverse_cache_map_.erase(a);
                WARNING("Failed to reuse a cache line");
            }
            // I do not need the reverse cache map anymore
            // It is not allowed to access the cache from here on

            // If something went wrong here or cache_line/cache lock is not set, we simply return LOOKUP_NOT_FOUND
            // The caller than has no responsibility to do anything.

            return LOOKUP_NOT_FOUND;
        }
        read_cache_index = a->second;
        a.release();
    }

    // I do not need the reverse cache map anymore
    // It is not allowed to access the cache from here on

    DCHECK_RETURN(read_cache_index < read_cache_.size(), LOOKUP_ERROR, "Illegal read cache index");
    ReadWriteLock* read_cache_lock = this->read_cache_lock_.Get(read_cache_index);
    DCHECK_RETURN(read_cache_lock, LOOKUP_ERROR, "Read cache lock not set");

    if (!write_lock) {
        CHECK_RETURN(read_cache_lock->AcquireReadLockWithStatistics(&this->stats_.read_cache_lock_free_, &this->stats_.read_cache_lock_busy_),
            LOOKUP_ERROR, "Failed to acquire read cache lock");
    } else {
        CHECK_RETURN(read_cache_lock->AcquireWriteLockWithStatistics(&this->stats_.read_cache_lock_free_, &this->stats_.read_cache_lock_busy_),
            LOOKUP_ERROR, "Failed to acquire read cache lock");
    }
    if (!this->read_cache_[read_cache_index]->HasId(container_id)) {
        // read cache entry has changed in the mean time
        this->reverse_cache_map_.erase(container_id); // the information is out dated
        if (!read_cache_lock->ReleaseLock()) {
            WARNING("Failed to release read cache lock");
        }

        this->stats_.cache_miss_.fetch_and_increment();
        entry->clear();

        // this usually should not happen too often. But it may happen due to expected races.
        DEBUG("Cache line already reassigned: cache container " << this->read_cache_[read_cache_index]->DebugString() <<
            ", target container id " << container_id <<
            ", cache line " << read_cache_index);
        return LOOKUP_NOT_FOUND;
    }

    DEBUG("Found container in read cache: container id " << container_id << ", update " << ToString(!no_update));

    // update used time
    spin_rw_mutex::scoped_lock scoped_lock(this->read_cache_used_time_lock_);
    this->read_cache_used_time_[read_cache_index] = tick_count::now();
    TRACE("Update used timestamp: " << read_cache_index <<
        ", container " <<  this->read_cache_[read_cache_index]->DebugString());
    scoped_lock.release();

    this->stats_.cache_hits_.fetch_and_increment();

    *container = this->read_cache_[read_cache_index];
    entry->set_line(read_cache_index).set_lock(read_cache_lock);
    return LOOKUP_FOUND;
}

enum lookup_result ContainerStorageReadCache::GetCache(uint64_t container_id, CacheEntry* cache_entry) {
    DCHECK_RETURN(cache_entry, LOOKUP_ERROR, "Cache entry not set");

    this->stats_.cache_checks_.fetch_and_increment();
    ProfileTimer cache_timer(this->stats_.cache_check_time_);

    TRACE("Get cache line: container id " << container_id);

    concurrent_hash_map<uint64_t, int>::accessor a;
    if (this->reverse_cache_map_.insert(a, container_id)) {
        // not found
        this->stats_.cache_miss_.fetch_and_increment();

        // we should free a place to use and than fill it later with data
        // we place is locked so that other threads accessing the same container is wait
        // Note: If something went wrong, the caller has to release the place under all circumstances

        if (!ReuseCacheLine(container_id, &a, cache_entry)) {
            cache_entry->clear();
            reverse_cache_map_.erase(a);
            WARNING("Failed to reuse a cache line");
        }
        // I do not need the reverse cache map anymore
        // It is not allowed to access the cache from here on

        // If something went wrong here or cache_line/cache lock is not set, we simply return LOOKUP_NOT_FOUND
        // The caller than has no responsibility to do anything.

        return LOOKUP_NOT_FOUND;
    }
    int read_cache_index = a->second;
    a.release();
    ReadWriteLock* read_cache_lock = this->read_cache_lock_.Get(read_cache_index);
    DCHECK_RETURN(read_cache_lock, LOOKUP_ERROR, "Read cache lock not set");
    CHECK_RETURN(read_cache_lock->AcquireReadLockWithStatistics(&this->stats_.read_cache_lock_free_, &this->stats_.read_cache_lock_busy_),
        LOOKUP_ERROR, "Failed to acquire read cache lock");
    if (!this->read_cache_[read_cache_index]->HasId(container_id)) {
        // read cache entry has changed in the mean time
        this->reverse_cache_map_.erase(container_id); // the information is out dated
        if (!read_cache_lock->ReleaseLock()) {
            WARNING("Failed to release read cache lock");
        }

        this->stats_.cache_miss_.fetch_and_increment();
        cache_entry->clear();

        // this usually should not happen too often. But it may happen due to expected races.
        DEBUG("Cache line already reassigned: cache container " << this->read_cache_[read_cache_index]->DebugString() <<
            ", target container id " << container_id <<
            ", cache line " << read_cache_index);
        return LOOKUP_NOT_FOUND;
    }
    if (!read_cache_lock->ReleaseLock()) {
        WARNING("Failed to release read cache lock");
    }
    return LOOKUP_FOUND;
}

bool ContainerStorageReadCache::AcquireCacheLineLock(uint64_t future_container_id, int cache_line, CacheEntry* cache_entry) {
    DCHECK(cache_entry, "Cache entry not set");
    Container* active_container = this->read_cache_[cache_line];
    DCHECK(active_container, "Active container not set");

    ScopedReadWriteLock used_cache_lock(this->read_cache_lock_.Get(cache_line));
    CHECK(used_cache_lock.AcquireWriteLock(),
        "Failed to acquire read cache lock: index " << cache_line);

    DEBUG("Reuse read cache container: index " << cache_line <<
        ", replaced " << (Storage::IsValidAddress(active_container->primary_id(), false) ? active_container->DebugString() : "null") <<
        ", new " << future_container_id);

    // remove old entries from reverse lookup map
    if (active_container->primary_id() != Storage::ILLEGAL_STORAGE_ADDRESS && active_container->primary_id() != future_container_id) {
        this->reverse_cache_map_.erase(active_container->primary_id());
    }
    for (set<uint64_t>::const_iterator k = active_container->secondary_ids().begin(); k != active_container->secondary_ids().end(); k++) {
        if (*k != future_container_id) {
            this->reverse_cache_map_.erase(*k);
        }
    }
    active_container->Reuse(Storage::ILLEGAL_STORAGE_ADDRESS);
    cache_entry->set_line(cache_line);
    cache_entry->set_lock(used_cache_lock.Get());
    used_cache_lock.Unset();
    return true;
}

bool ContainerStorageReadCache::ReuseCacheLine(uint64_t future_container_id,
                                               concurrent_hash_map<uint64_t, int>::accessor* accessor,
                                               CacheEntry* cache_entry) {
    DCHECK(cache_entry, "Cache entry not set");
    DCHECK(accessor, "Accessor not set");

    spin_rw_mutex::scoped_lock scoped_cache_used_time_lock;
    scoped_cache_used_time_lock.acquire(this->read_cache_used_time_lock_, false);

    int least_recently_used_index = -1;
    tick_count t = tick_count::now();
    for (int read_cache_index = 0; read_cache_index < this->read_cache_size_; read_cache_index++) {
        TRACE("cache index used time: " << read_cache_index <<
            ", container " << this->read_cache_[read_cache_index]->DebugString());
        tick_count::interval_t diff = t - this->read_cache_used_time_[read_cache_index];
        if ((least_recently_used_index == -1) || diff.seconds() > 0) {
            least_recently_used_index = read_cache_index;
            t = this->read_cache_used_time_[read_cache_index];

        }
    }
    if (least_recently_used_index >= 0) {
        if (!scoped_cache_used_time_lock.upgrade_to_writer()) {
            // rerun everything as we may be released the lock in the meantime
            for (int read_cache_index = 0; read_cache_index < this->read_cache_size_; read_cache_index++) {
                TRACE("cache index used time: " << read_cache_index <<
                    ", container " << this->read_cache_[read_cache_index]->DebugString());
                tick_count::interval_t diff = t - this->read_cache_used_time_[read_cache_index];
                if ((least_recently_used_index == -1) || diff.seconds() > 0) {
                    least_recently_used_index = read_cache_index;
                    t = this->read_cache_used_time_[read_cache_index];
                }
            }
        }
        this->read_cache_used_time_[least_recently_used_index] = tick_count::now();
        TRACE("Update used timestamp: " << least_recently_used_index);
    } else {
        ERROR("Failed to select cache entry to reuse");
        return false;
    }

    scoped_cache_used_time_lock.release();
    CHECK_RETURN(least_recently_used_index >= 0, LOOKUP_ERROR, "Failed to select cache entry to reuse");

    // the id is set, we will in the data later
    // add entries into reverse lookup map
    concurrent_hash_map<uint64_t, int>::accessor& a(*accessor);
    a->second = least_recently_used_index;
    a.release(); // release the lock

    // Here we have a race condition between the release of the reverse map and acquire of the cache line lock
    // We do this to avoid a cyclic wait situation between the reverse map lock and the cache line lock
    // if someone else infers with us and acquires the lock before us, we may override its cache line data
    // However, this only leads to cache misses. Before a cache is used, we check for the validity of the cache line

    if (!AcquireCacheLineLock(future_container_id, least_recently_used_index, cache_entry)) {
        WARNING("Failed to acquire cache line");
        cache_entry->clear();
    }
    return true;
}

bool ContainerStorageReadCache::CopyToReadCache(const Container& container,
                                                CacheEntry* cache_entry) {
    DCHECK(cache_entry && cache_entry->is_set(), "Cache entry not set");
    DCHECK(cache_entry->lock()->IsHeldForWrites(), "Cache line lock not held by current thread");
    DCHECK(this->read_cache_lock_.Get(cache_entry->line()) == cache_entry->lock(), "Illegal cache line lock");

    ProfileTimer cache_timer(this->stats_.cache_update_time_);

    this->stats_.cache_updates_.fetch_and_increment();

    TRACE("Insert container into cache: " << container.DebugString() << ", cache line " << cache_entry->DebugString());

    spin_rw_mutex::scoped_lock scoped_lock(this->read_cache_used_time_lock_);
    this->read_cache_used_time_[cache_entry->line()] = tick_count::now();
    TRACE("Update used timestamp: " << cache_entry->DebugString() <<
        ", container " <<  this->read_cache_[cache_entry->line()]->DebugString());
    scoped_lock.release();

    ScopedReadWriteLock used_cache_lock(NULL);
    used_cache_lock.SetLocked(cache_entry->lock());
    int cache_line = cache_entry->line();
    cache_entry->clear();

    DEBUG("Provide read cache container data: index " << cache_line <<
        ", new " << container.DebugString());

    Container* active_container = this->read_cache_[cache_line];
    active_container->Reuse(container.primary_id());
    if (!active_container->CopyFrom(container, true)) {
        ERROR("Cannot copy container: dest " << active_container->DebugString() << ", src " << container.DebugString());
        return false;
    }
    // add entries into reverse lookup map
    if (active_container->primary_id() != Storage::ILLEGAL_STORAGE_ADDRESS) {
        this->reverse_cache_map_.insert(std::make_pair(active_container->primary_id(), cache_line));
    }
    for (set<uint64_t>::const_iterator k = active_container->secondary_ids().begin(); k != active_container->secondary_ids().end(); k++) {
        this->reverse_cache_map_.insert(std::make_pair(*k, cache_line));
    }
    if (!used_cache_lock.ReleaseLock()) {
        WARNING("Failed to release cache lock");
    }
    // now everyone is free to use the cache line
    return true;
}

bool ContainerStorageReadCache::ClearCache() {
    ScopedReadWriteLock read_cache_lock(NULL);

    DEBUG("Clear read cache");

    for (size_t i = 0; i < this->read_cache_size_; i++) {
        CHECK(read_cache_lock.Set(this->read_cache_lock_.Get(i)), "Failed to set read cache: index " << i);
        CHECK(read_cache_lock.AcquireWriteLock(), "Failed to acquire read cache lock: index " << i);

        this->read_cache_[i]->Reuse(Storage::ILLEGAL_STORAGE_ADDRESS);

        CHECK(read_cache_lock.ReleaseLock(), "Failed to release read cache lock");
        CHECK(read_cache_lock.Set(NULL), "Failed to unset read cache lock");
    }
    this->reverse_cache_map_.clear();
    return true;
}

bool ContainerStorageReadCache::ReleaseCacheline(uint64_t container_id, CacheEntry* cache_entry) {
    DCHECK(cache_entry && cache_entry->is_set(), "Cache entry not set");
    DCHECK(cache_entry->lock()->IsHeldForWrites(), "Cache line lock not held by current thread");
    DCHECK(this->read_cache_lock_.Get(cache_entry->line()) == cache_entry->lock(), "Illegal cache line lock");

    TRACE("Release cache line: " << cache_entry->DebugString() <<
        "current container " << this->read_cache_[cache_entry->line()]->DebugString());

    // Release cache line lock after reverse cache map to avoid a cyclic lock with ReuseCacheLine
    this->reverse_cache_map_.erase(container_id);
    this->read_cache_[cache_entry->line()]->Reuse(Storage::ILLEGAL_STORAGE_ADDRESS);

    ReadWriteLock* lock = cache_entry->lock();
    cache_entry->clear();

    CHECK(lock->ReleaseLock(), "Failed to release lock");
    return true;
}

bool ContainerStorageReadCache::RemoveFromReadCache(uint64_t container_id, CacheEntry* cache_entry) {
    DCHECK(cache_entry && cache_entry->is_set(), "Cache entry not set");
    DCHECK(cache_entry->lock()->IsHeldForWrites(), "Cache line lock not held by current thread");
    DCHECK(this->read_cache_lock_.Get(cache_entry->line()) == cache_entry->lock(), "Illegal cache line lock");
    DCHECK(this->read_cache_lock_.Get(cache_entry->line()) == cache_entry->lock(), "Illegal cache line lock");

    CHECK(this->read_cache_[cache_entry->line()]->HasId(container_id), "Illegal cache entry: " <<
        this->read_cache_[cache_entry->line()]->DebugString() << ", expected container id " << container_id);

    // we found it
    // we also have to update the reverse cache map
    this->reverse_cache_map_.erase(this->read_cache_[cache_entry->line()]->primary_id());
    for (set<uint64_t>::const_iterator k = this->read_cache_[cache_entry->line()]->secondary_ids().begin();
         k != this->read_cache_[cache_entry->line()]->secondary_ids().end(); k++) {
        this->reverse_cache_map_.erase(*k);
    }
    this->read_cache_[cache_entry->line()]->Reuse(Storage::ILLEGAL_STORAGE_ADDRESS);
    DEBUG("Remove container " << container_id << " from read cache: index " << cache_entry->line());

    ReadWriteLock* lock = cache_entry->lock();
    cache_entry->clear();

    CHECK(lock->ReleaseLock(), "Failed to release lock");
    return true;
}

string ContainerStorageReadCache::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"cache check time\": " << this->stats_.cache_check_time_.GetSum() << "," << std::endl;
    sstr << "\"cache update time\": " << this->stats_.cache_update_time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

string ContainerStorageReadCache::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"read cache lock free\": " << this->stats_.read_cache_lock_free_ << "," << std::endl;
    sstr << "\"read cache lock busy\": " << this->stats_.read_cache_lock_busy_ << std::endl;
    sstr << "}";
    return sstr.str();
}

bool ContainerStorageReadCache::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ContainerStorageReadCacheStatsData data;
    data.set_hit_count(this->stats_.cache_hits_);
    data.set_miss_count(this->stats_.cache_miss_);
    data.set_check_count(this->stats_.cache_checks_);
    data.set_update_count(this->stats_.cache_updates_);
    CHECK(ps->Persist(prefix, data), "Failed to persist read cache stats");
    return true;
}

bool ContainerStorageReadCache::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ContainerStorageReadCacheStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore read cache stats");
    this->stats_.cache_hits_ = data.hit_count();
    this->stats_.cache_miss_ = data.miss_count();
    this->stats_.cache_checks_ = data.check_count();
    this->stats_.cache_updates_ = data.update_count();
    return true;
}

string ContainerStorageReadCache::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"cache checks\": " << this->stats_.cache_checks_ << "," << std::endl;
    sstr << "\"cache updates\": " << this->stats_.cache_updates_ << "," << std::endl;
    sstr << "\"cache hits\": " << this->stats_.cache_hits_ << "," << std::endl;
    sstr << "\"cache miss\": " << this->stats_.cache_miss_ << std::endl;
    sstr << "}";
    return sstr.str();
}

}
}
