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
#include <core/container_storage_write_cache.h>

#include <sstream>

#include <core/container.h>
#include <core/container_storage.h>
#include <base/logging.h>
#include <base/locks.h>
#include <base/strutil.h>

#include "dedupv1_stats.pb.h"

using std::string;
using std::stringstream;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::ProfileTimer;
using dedupv1::base::ReadWriteLock;
using tbb::tick_count;
using tbb::spin_rw_mutex;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;

LOGGER("ContainerStorageWriteCache");

namespace dedupv1 {
namespace chunkstore {

const std::string ContainerStorageWriteCache::kDefaultCacheStrategyType = "earliest-free";

ContainerStorageWriteCache::ContainerStorageWriteCache(ContainerStorage* storage) {
    this->storage_ = storage;
    this->write_cache_strategy_ = NULL;
    this->write_container_count_ = kDefaultWriteCacheSize;
}

ContainerStorageWriteCache::Statistics::Statistics() {
    this->write_container_lock_busy_ = 0;
    this->write_container_lock_free_ = 0;
    this->cache_hits_ = 0;
    this->cache_miss_ = 0;
    this->cache_checks_ = 0;
}

bool ContainerStorageWriteCache::Start() {
    CHECK(this->storage_, "Storage not set");
    CHECK(this->storage_->GetContainerSize() > 0, "Container size not set");

    this->write_container.resize(this->write_container_count_);
    for (int i = 0; i < this->write_container_count_; i++) {
        this->write_container[i] = new Container(Storage::ILLEGAL_STORAGE_ADDRESS, this->storage_->GetContainerSize(), false);
    }
    this->write_container_lock_.Init(this->write_container_count_);

    this->write_cache_changed_time_.resize(this->write_container_count_);

    // write cache timeout time data
    for (int i = 0; i < this->write_container_count_; i++) {
        this->write_cache_changed_time_[i] = tick_count::now();
    }

    if (this->write_cache_strategy_ == NULL) {
        // use default
        this->write_cache_strategy_ = ContainerStorageWriteCacheStrategyFactory::Create(kDefaultCacheStrategyType);
        CHECK(this->write_cache_strategy_, "Failed to create default write cache strategy");
    }
    CHECK(this->write_cache_strategy_->Start(this), "Failed to start write cache strategy");
    return true;
}

bool ContainerStorageWriteCache::SetOption(const string& option_name, const string& option) {
    if (option_name == "size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->write_container_count_ = ToStorageUnit(option).value();
        CHECK(this->write_container_count_ != 0, "Failed to set write cache size: size must be larger than 0");
        return true;
    }
    if (option_name == "strategy") {
        CHECK(this->write_cache_strategy_ == NULL, "Write cache strategy already set");
        this->write_cache_strategy_ = ContainerStorageWriteCacheStrategyFactory::Create(option);
        CHECK(this->write_cache_strategy_, "Failed to create write cache strategy");
        return true;
    }
    if (StartsWith(option_name, "strategy.")) {
        CHECK(this->write_cache_strategy_, "Write cache strategy not set");
        CHECK(this->write_cache_strategy_->SetOption(option_name.substr(strlen("strategy.")), option), "Config failed");
        return true;
    }
    ERROR("Illegal option: " << option_name);
    return false;
}

#ifdef DEDUPV1_CORE_TEST
void ContainerStorageWriteCache::ClearData() {
    if (this->write_container.size() > 0) {
        for (int i = 0; i < this->write_container_count_; i++) {
            if (this->write_container[i]) {
                this->write_container[i]->Reuse(Storage::ILLEGAL_STORAGE_ADDRESS);
            }
            delete this->write_container[i];
        }
        this->write_container.clear();
    }
}
#endif

lookup_result ContainerStorageWriteCache::GetWriteCacheContainer(uint64_t address,
                                                                 Container** write_container,
                                                                 ReadWriteLock** write_cache_lock,
                                                                 bool write_lock) {
    DCHECK_RETURN(this->write_container.size() > 0, LOOKUP_ERROR, "Write cache not started");

    ProfileTimer(this->stats_.cache_check_time_);

    // Test each write container

    this->stats_.cache_checks_.fetch_and_increment();
    for (int write_container_index = 0; write_container_index
         < this->write_container_count_; write_container_index++) {
        Container* wc = this->write_container[write_container_index];
        DCHECK_RETURN(wc, LOOKUP_ERROR, "Write container not set");

        ReadWriteLock* wl = this->write_container_lock_.Get(write_container_index);
        DCHECK_RETURN(wl, LOOKUP_ERROR, "Write lock not set");

        if (write_lock) {
            CHECK_RETURN(wl->AcquireWriteLockWithStatistics(
                    &this->stats_.write_container_lock_free_,
                    &this->stats_.write_container_lock_busy_), LOOKUP_ERROR, "Failed to acquire write container lock");
        } else {
            CHECK_RETURN(wl->AcquireReadLockWithStatistics(
                    &this->stats_.write_container_lock_free_,
                    &this->stats_.write_container_lock_busy_), LOOKUP_ERROR, "Failed to acquire write container lock");
        }

        if (wc->HasId(address)) {
            *write_container = wc;
            *write_cache_lock = wl;
            this->stats_.cache_hits_.fetch_and_increment();
            return LOOKUP_FOUND;
        } else {
            wl->ReleaseLock();
        }
    }
    this->stats_.cache_miss_.fetch_and_increment();
    *write_container = NULL;
    *write_cache_lock = NULL;
    return LOOKUP_NOT_FOUND;
}

bool ContainerStorageWriteCache::Close() {
    storage_ = NULL;
    if (this->write_container.size() > 0) {
        for (int i = 0; i < this->write_container_count_; i++) {
            if (this->write_container[i] && this->write_container[i]->item_count() > 0) {
                WARNING("Uncommitted container in write cache: " << this->write_container[i]->DebugString());
            }
            delete this->write_container[i];
        }
        this->write_container.clear();
    }
    if (this->write_cache_strategy_) {
        if (!this->write_cache_strategy_->Close()) {
            WARNING("Failed to close write cache strategy");
        }
        delete this->write_cache_strategy_;
        this->write_cache_strategy_ = NULL;
    }
    return true;
}

bool ContainerStorageWriteCache::GetWriteCacheContainerByIndex(int index, Container** write_container, ReadWriteLock** write_cache_lock) {
    DCHECK(this->write_container.size() >= index, "Illegal index " << index << ", size " << this->write_container.size());
    DCHECK(this->write_container_lock_.size() >= index, "Illegal index " << index << ", size " << this->write_container_lock_.size());

    *write_container = this->write_container[index];
    *write_cache_lock = this->write_container_lock_.Get(index);
    return true;
}

bool ContainerStorageWriteCache::ResetTimeout(int index) {
    DCHECK(this->write_cache_changed_time_.size() >= index, "Illegal index " << index << ", size " << this->write_cache_changed_time_.size());

    spin_rw_mutex::scoped_lock scoped_lock(this->write_cache_changed_time_lock_, true);
    this->write_cache_changed_time_[index] = tick_count::now();
    scoped_lock.release();
    return true;
}

bool ContainerStorageWriteCache::IsTimedOut(int index, double timeout_seconds) {
    DCHECK(this->write_cache_changed_time_.size() >= index, "Illegal index " << index << ", size " << this->write_cache_changed_time_.size());

    tick_count n = tick_count::now();

    spin_rw_mutex::scoped_lock scoped_lock(this->write_cache_changed_time_lock_, false);
    tick_count::interval_t i = n - this->write_cache_changed_time_[index];
    scoped_lock.release();

    return i.seconds() > timeout_seconds;
}

bool ContainerStorageWriteCache::GetNextWriteCacheContainer(Container** write_container, ReadWriteLock** write_cache_lock) {
    CHECK(write_cache_strategy_, "Write cache strategy is not set");

    return write_cache_strategy_->GetNextWriteCacheContainer(write_container, write_cache_lock);
}

string ContainerStorageWriteCache::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"write container lock free\": " << this->stats_.write_container_lock_free_ << "," << std::endl;
    sstr << "\"write container lock busy\": " << this->stats_.write_container_lock_busy_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string ContainerStorageWriteCache::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"write lock time\": " << this->stats_.cache_check_time_.GetSum() << "," << std::endl;
    sstr << "\"write lock time\": " << this->stats_.write_lock_wait_time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

bool ContainerStorageWriteCache::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ContainerStorageWriteCacheStatsData data;
    data.set_hit_count(this->stats_.cache_hits_);
    data.set_miss_count(this->stats_.cache_miss_);
    data.set_check_count(this->stats_.cache_checks_);
    CHECK(ps->Persist(prefix, data), "Failed to persist write cache stats");
    return true;
}

bool ContainerStorageWriteCache::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ContainerStorageWriteCacheStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore write cache stats");
    this->stats_.cache_hits_ = data.hit_count();
    this->stats_.cache_miss_ = data.miss_count();
    this->stats_.cache_checks_ = data.check_count();
    return true;
}

string ContainerStorageWriteCache::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"write cache hits\": " << this->stats_.cache_hits_ << "," << std::endl;
    sstr << "\"write cache miss\": " << this->stats_.cache_miss_ << "," << std::endl;
    sstr << "\"write cache checks\": " << this->stats_.cache_checks_ << std::endl;
    sstr << "}";
    return sstr.str();
}

ContainerStorageWriteCacheStrategy::ContainerStorageWriteCacheStrategy()  {
}

ContainerStorageWriteCacheStrategy::~ContainerStorageWriteCacheStrategy() {
}

bool ContainerStorageWriteCacheStrategy::Init() {
    return true;
}

bool ContainerStorageWriteCacheStrategy::SetOption(const string& option_name, const string& option) {
    ERROR("Illegal option: " << option_name);
    return false;
}

bool ContainerStorageWriteCacheStrategy::Start(ContainerStorageWriteCache* write_cache) {
    return true;
}

bool ContainerStorageWriteCacheStrategy::Close() {
    return true;
}

ContainerStorageWriteCacheStrategy* RoundRobinContainerStorageWriteCacheStrategy::CreateWriteCacheStrategy() {
    return new RoundRobinContainerStorageWriteCacheStrategy();
}

void RoundRobinContainerStorageWriteCacheStrategy::RegisterWriteCacheStrategy() {
    ContainerStorageWriteCacheStrategyFactory::GetFactory()->Register("round-robin",
        &RoundRobinContainerStorageWriteCacheStrategy::CreateWriteCacheStrategy);
}

RoundRobinContainerStorageWriteCacheStrategy::RoundRobinContainerStorageWriteCacheStrategy() {
    this->write_cache = NULL;
    this->next_write_container = 0;
}

RoundRobinContainerStorageWriteCacheStrategy::~RoundRobinContainerStorageWriteCacheStrategy() {
}

bool RoundRobinContainerStorageWriteCacheStrategy::Start(ContainerStorageWriteCache* write_cache) {
    CHECK(write_cache, "Write cache not set");
    this->write_cache = write_cache;
    DEBUG("Starting round-robin container write cache strategy");
    return true;
}

bool RoundRobinContainerStorageWriteCacheStrategy::GetNextWriteCacheContainer(Container** write_container,
                                                                              ReadWriteLock** write_cache_lock) {
    CHECK(this->write_cache, "Write cache not set");

    unsigned int write_container_index = this->next_write_container.fetch_and_increment() % this->write_cache->GetSize();
    *write_container = this->write_cache->GetCache()[write_container_index];
    *write_cache_lock = this->write_cache->GetCacheLock().Get(write_container_index);
    CHECK(write_container, "Write container not set");
    CHECK(write_cache_lock, "Write cache lock not set");

    this->write_cache->ResetTimeout(write_container_index);

    TRACE("Choose write container " << write_container_index <<
        ", container " << (*write_container)->primary_id());

    ProfileTimer lock_timer(this->write_cache->GetStatistics()->write_lock_wait_time_);
    CHECK((*write_cache_lock)->AcquireWriteLockWithStatistics(
            &this->write_cache->GetStatistics()->write_container_lock_free_,
            &this->write_cache->GetStatistics()->write_container_lock_busy_),
        "Write cache lock failed: write container index " << write_container_index);
    return true;
}

ContainerStorageWriteCacheStrategy* EarliestFreeContainerStorageWriteCacheStrategy::CreateWriteCacheStrategy() {
    return new EarliestFreeContainerStorageWriteCacheStrategy();
}

void EarliestFreeContainerStorageWriteCacheStrategy::RegisterWriteCacheStrategy() {
    ContainerStorageWriteCacheStrategyFactory::GetFactory()->Register("earliest-free",
        &EarliestFreeContainerStorageWriteCacheStrategy::CreateWriteCacheStrategy);
}

EarliestFreeContainerStorageWriteCacheStrategy::EarliestFreeContainerStorageWriteCacheStrategy() {
    this->write_cache = NULL;
    this->fallback_strategy = NULL;
}

EarliestFreeContainerStorageWriteCacheStrategy::~EarliestFreeContainerStorageWriteCacheStrategy() {
}

bool EarliestFreeContainerStorageWriteCacheStrategy::Start(ContainerStorageWriteCache* write_cache) {
    CHECK(write_cache, "Write cache not set");
    this->write_cache = write_cache;
    this->fallback_strategy = ContainerStorageWriteCacheStrategyFactory::Create("round-robin");
    CHECK(this->fallback_strategy, "Failed to create fallback strategy");
    CHECK(this->fallback_strategy->Start(write_cache), "Failed to start fallback strategy");

    DEBUG("Starting earliest-first container write cache strategy");
    return true;
}

bool EarliestFreeContainerStorageWriteCacheStrategy::Close() {
    if (this->fallback_strategy) {
        if (!this->fallback_strategy->Close()) {
            WARNING("Failed to close fallback strategy");
        }
        delete this->fallback_strategy;
        this->fallback_strategy = NULL;
    }
    return ContainerStorageWriteCacheStrategy::Close();
}

bool EarliestFreeContainerStorageWriteCacheStrategy::GetNextWriteCacheContainer(Container** write_container,
                                                                                ReadWriteLock** write_cache_lock) {
    DCHECK(write_container, "Write container not set");
    DCHECK(write_cache_lock, "Write cache lock not set");
    DCHECK(this->write_cache, "Write cache not set");
    DCHECK(this->fallback_strategy, "Fallback strategy not set");

    for (int i = 0; i < this->write_cache->GetSize(); i++) {
        ReadWriteLock* lock = this->write_cache->GetCacheLock().Get(i);
        DCHECK(lock, "Write cache lock not set");

        bool locked = false;
        CHECK(lock->TryAcquireWriteLock(&locked), "Lock failed");
        if (locked) {
            *write_container = this->write_cache->GetCache()[i];
            *write_cache_lock = this->write_cache->GetCacheLock().Get(i);
            CHECK(*write_container, "Write container not assigned");
            CHECK(*write_cache_lock, "Write cache lock not assigned");

            TRACE("Choose write container " << i << ", container " << (*write_container)->primary_id());

            this->write_cache->ResetTimeout(i);
            return true;
        }
    }
    return this->fallback_strategy->GetNextWriteCacheContainer(write_container, write_cache_lock);
}

ContainerStorageWriteCacheStrategyFactory ContainerStorageWriteCacheStrategyFactory::factory;

bool ContainerStorageWriteCacheStrategyFactory::Register(const string& name, ContainerStorageWriteCacheStrategy*(*factory)(void)) {
    factory_map[name] = factory;
    return true;
}

ContainerStorageWriteCacheStrategyFactory::ContainerStorageWriteCacheStrategyFactory() {
}

ContainerStorageWriteCacheStrategy* ContainerStorageWriteCacheStrategyFactory::Create(const string& name) {
    ContainerStorageWriteCacheStrategy* (*f)(void) = factory.factory_map[name];
    if (f) {
        ContainerStorageWriteCacheStrategy* a = f();
        CHECK_RETURN(a, NULL, "Cannot create new write cache strategy " << name);
        if (!a->Init()) {
            ERROR("Cannot init new write cache strategy: " << name);
            a->Close();
            return NULL;
        }
        return a;
    }
    ERROR("Cannot find write cache strategy: " << name);
    return NULL;
}

}
}
