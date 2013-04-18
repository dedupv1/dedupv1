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

#include <core/container_storage.h>

#include <ctime>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>

#include <sstream>
#include <list>

#include "dedupv1.pb.h"
#include "dedupv1_stats.pb.h"

#include <core/dedup.h>
#include <core/dedup_system.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <core/storage.h>
#include <base/bitutil.h>
#include <base/index.h>
#include <base/hashing_util.h>
#include <base/strutil.h>
#include <base/crc32.h>
#include <base/logging.h>
#include <core/container.h>
#include <base/fileutil.h>
#include <base/timer.h>
#include <base/compress.h>
#include <base/memory.h>
#include <core/container_storage_gc.h>
#include <core/container_storage_bg.h>
#include <core/container_storage_alloc.h>
#include <core/container_storage_cache.h>
#include <core/container_storage_write_cache.h>
#include <core/log.h>
#include <base/fault_injection.h>

LOGGER("ContainerStorage");

using std::string;
using std::vector;
using std::set;
using std::stringstream;
using std::pair;
using std::make_pair;
using std::list;
using dedupv1::base::strutil::FormatLargeNumber;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::FormatStorageUnit;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::strutil::To;
using dedupv1::base::Index;
using dedupv1::base::ProfileTimer;
using dedupv1::base::File;
using dedupv1::base::Compression;
using dedupv1::base::bits;
using dedupv1::base::Walltimer;
using dedupv1::base::MutexLock;
using dedupv1::base::ScopedLock;
using dedupv1::base::ReadWriteLock;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::base::TIMED_FALSE;
using dedupv1::base::TIMED_TRUE;
using dedupv1::base::TIMED_TIMEOUT;
using dedupv1::Fingerprinter;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::Thread;
using dedupv1::base::ThreadUtil;
using dedupv1::base::NewRunnable;
using dedupv1::log::EVENT_TYPE_CONTAINER_OPEN;
using dedupv1::log::EVENT_TYPE_CONTAINER_COMMITED;
using dedupv1::log::EVENT_TYPE_CONTAINER_COMMIT_FAILED;
using dedupv1::log::Log;
using dedupv1::IdleDetector;
using dedupv1::log::EVENT_TYPE_CONTAINER_MERGED;
using dedupv1::log::EVENT_TYPE_CONTAINER_DELETED;
using dedupv1::log::EVENT_TYPE_CONTAINER_MOVED;
using dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START;
using dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG;
using google::protobuf::Message;
using dedupv1::log::event_type;
using dedupv1::log::LogReplayContext;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::IndexCursor;
using dedupv1::base::ScopedPtr;
using dedupv1::base::ErrorContext;
using dedupv1::base::UUID;

namespace dedupv1 {
namespace chunkstore {

namespace {
bool IsValidAddressData(const ContainerStorageAddressData& address) {
    return address.has_primary_id() || (address.has_file_index() && address.has_file_offset());
}

/**
 * Helper class to deal with the in_move_set of the container storage
 */
class ScopedInMoveSetMembership {
private:
    set<uint64_t>* set_;
    tbb::spin_mutex* lock_;
    list<uint64_t> locked_container_id_list_;
public:

    ScopedInMoveSetMembership(set<uint64_t>* set,
                              tbb::spin_mutex* lock) : set_(set), lock_(lock) {
    }

    bool Insert(uint64_t container_id) {
        tbb::spin_mutex::scoped_lock scoped_lock(*lock_);

        if (set_->find(container_id) == set_->end()) {
            set_->insert(container_id);
            locked_container_id_list_.push_back(container_id);
            return true;
        }
        return false;
    }

    bool Insert(list<uint64_t>& container_id_list) {
        tbb::spin_mutex::scoped_lock scoped_lock(*lock_);

        bool ok = true;
        list<uint64_t>::iterator i;
        for (i = container_id_list.begin(); i != container_id_list.end(); i++) {
            uint64_t container_id = *i;
            if (set_->find(container_id) != set_->end()) {
                ok = false;
            }
        }

        if (!ok) {
            return false;
        }

        for (i = container_id_list.begin(); i != container_id_list.end(); i++) {
            uint64_t container_id = *i;
            set_->insert(container_id);
            locked_container_id_list_.push_back(container_id);
        }

        return true;
    }

    void RemoveAllFromSet() {
        tbb::spin_mutex::scoped_lock scoped_lock(*lock_);

        list<uint64_t>::iterator i;
        for (i = locked_container_id_list_.begin(); i != locked_container_id_list_.end(); i++) {
            DEBUG("Remove " << *i << " from in-move set");
            set_->erase(*i);
        }
        locked_container_id_list_.clear();
    }

    ~ScopedInMoveSetMembership() {
        RemoveAllFromSet();
    }
};

}

lookup_result ContainerStorage::ReadContainerWithCache(
    Container* container) {
    DCHECK_RETURN(container, LOOKUP_ERROR, "Container not set");
    DCHECK_RETURN(state_ == RUNNING || state_ == STARTED, LOOKUP_ERROR,
        "Illegal state to read container: " <<
        "state " << this->state_ << ", container " << container->primary_id());

    bool use_cache = !container->is_metadata_only();
    CacheEntry cache_entry;

    const Container* cache_container = NULL;
    // do not provide cache stuff the we only want meta data
    lookup_result r = this->cache_.CheckCache(container->primary_id(), &cache_container, !use_cache, false, &cache_entry);
    CHECK_RETURN(r != LOOKUP_ERROR, LOOKUP_ERROR, "Failed to check cache: container " << container->DebugString());

    if (r == LOOKUP_FOUND) {
        DCHECK_RETURN(cache_entry.is_set(), LOOKUP_ERROR, "Cache entry not set");
        DCHECK_RETURN(cache_container, LOOKUP_ERROR, "Cache container not set");
        TRACE("Read container: container " << container->DebugString() << " (read cache) ");

        bool copy_result = container->CopyFrom(*cache_container, true);
        if (!cache_entry.lock()->ReleaseLock()) {
            WARNING("Failed to release cache line lock");
        }
        cache_container = NULL;

        return copy_result ? LOOKUP_FOUND : LOOKUP_ERROR;
    }
    // not found in read cache, cache lock held
    cache_container = NULL;

    DCHECK_RETURN(use_cache || !cache_entry.is_set(), LOOKUP_ERROR,
        "We do not use cache, but we acquired a cache lock anyway");

    enum lookup_result b = ReadContainer(container); // now container lock held
    if (use_cache && cache_entry.is_set()) {
        if (b == LOOKUP_ERROR || b == LOOKUP_NOT_FOUND) {
            // We have to free the cache entry
            CHECK_RETURN(cache_.ReleaseCacheline(container->primary_id(), &cache_entry), LOOKUP_ERROR,
                "Failed to release cache line");
        } else {
            if (!this->cache_.CopyToReadCache(*container, &cache_entry)) {
                ERROR("Failed to add container to read cache: " << container->DebugString());
                return LOOKUP_ERROR;
            }
        }
    }
    return b;
}

pair<lookup_result, ContainerStorageAddressData> ContainerStorage::LookupContainerAddress(
    uint64_t container_id,
    ReadWriteLock** primary_container_lock,
    bool acquire_write_lock) {
    DCHECK_RETURN(state_ == RUNNING || state_ == STARTED, make_pair(LOOKUP_ERROR, ContainerStorageAddressData()),
        "Illegal state to lookup container: container " << container_id << ", state " << this->state_);
    DCHECK_RETURN(this->meta_data_index_, make_pair(LOOKUP_ERROR, ContainerStorageAddressData()), "Meta data index not set");

    TRACE("Lookup container address (no wait): container id " << container_id);

    ScopedReadWriteLock scoped_lock(&this->meta_data_lock_);
    CHECK_RETURN(scoped_lock.AcquireReadLock(), make_pair(LOOKUP_ERROR, ContainerStorageAddressData()),
        "Failed to acquire meta data lock");

    ContainerStorageAddressData container_address;
    ContainerStorageAddressData first_lookedup_container_address;
    lookup_result lr = this->meta_data_index_->Lookup(&container_id, sizeof(uint64_t),
        &container_address);
    if (lr == LOOKUP_ERROR || lr == LOOKUP_NOT_FOUND) {
        return make_pair(lr, container_address);
    }
    // lr == FOUND
    DCHECK_RETURN(IsValidAddressData(container_address),  make_pair(LOOKUP_ERROR, container_address), "Invalid address data: " << container_address.ShortDebugString());
    uint64_t primary_id = container_id;
    if (container_address.has_primary_id()) {
        // container id is secondary id => get the file address of the primary id
        first_lookedup_container_address = container_address;
        primary_id = container_address.primary_id();

        TRACE("Lookup primary container address: container id " << container_id << ", primary container id " << primary_id);

        lr = this->meta_data_index_->Lookup(&primary_id, sizeof(uint64_t),
            &container_address);
        if (lr == LOOKUP_ERROR) {
            ERROR("Failed to lookup primary id: secondary id " << container_id <<
                ", address " << container_address.ShortDebugString() <<
                ", first address looked up " << first_lookedup_container_address.ShortDebugString());
        }
        if (lr == LOOKUP_ERROR || lr == LOOKUP_NOT_FOUND) {
            return make_pair(lr, container_address);
        }
        DCHECK_RETURN(IsValidAddressData(container_address), make_pair(LOOKUP_ERROR, container_address), "Invalid address data: " << container_address.ShortDebugString());

        if (!container_address.has_primary_id()) {
            container_address.set_primary_id(primary_id); // reset the value to show that the lookup was redirected
        } else {
            // if the address has a primary id, we are in trouble. This case is handled as error in a few lines.
        }
        // lr == FOUND
    }
    CHECK_RETURN(container_address.has_file_index() && container_address.has_file_offset(),
        make_pair(LOOKUP_ERROR, container_address),
        "Illegal container address: " <<
        "address " << container_address.ShortDebugString() <<
        ", lookup container id " << container_id <<
        ", first looked up container address " << first_lookedup_container_address.ShortDebugString() <<
        ", reason should be primary");

    if (primary_container_lock) {
        ReadWriteLock* rw_lock = this->GetContainerLock(primary_id);
        DCHECK_RETURN(rw_lock,
            make_pair(LOOKUP_ERROR, ContainerStorageAddressData()), "Container lock not set");

        TRACE("Acquire container lock: primary container id " << primary_id);
        if (acquire_write_lock) {
            CHECK_RETURN(rw_lock->AcquireWriteLock(),
                make_pair(LOOKUP_ERROR, container_address), "Failed to acquire lock");
        } else {
            CHECK_RETURN(rw_lock->AcquireReadLock(),
                make_pair(LOOKUP_ERROR, container_address), "Failed to acquire lock");
        }
        TRACE("Acquired container lock: primary container id " << primary_id);

        *primary_container_lock = rw_lock;
    }
    return make_pair(lr, container_address);
}

pair<lookup_result, ContainerStorageAddressData> ContainerStorage::LookupContainerAddressWait(
    uint64_t container_id,
    ReadWriteLock** primary_container_lock,
    bool acquire_write_lock) {
    DCHECK_RETURN(state_ == RUNNING || state_ == STARTED, make_pair(LOOKUP_ERROR, ContainerStorageAddressData()),
        "Illegal state to lookup container: container " << container_id << ", state " << this->state_);
    DCHECK_RETURN(this->meta_data_index_, make_pair(LOOKUP_ERROR, ContainerStorageAddressData()), "Meta data index not set");

    TRACE("Lookup container address: container id " << container_id);
    pair<lookup_result, ContainerStorageAddressData> container_address =
        LookupContainerAddress(container_id, primary_container_lock, acquire_write_lock);
    CHECK_RETURN(container_address.first != LOOKUP_ERROR, container_address,
        "Failed to lookup container address: container id " << container_id);
    if (container_address.first == LOOKUP_NOT_FOUND) {
        // we do not hold the lock here
        TRACE("Container not found in meta data index: container id " << container_id);

        // container is in write cache
        tbb::concurrent_hash_map<uint64_t, ContainerStorageAddressData>::const_accessor a;
        while (address_map.find(a, container_id)) {
            a.release();
            dedupv1::base::ThreadUtil::Sleep(1);
        }
        a.release();

        // container not in the write cache, but in the background committer thing
        Option<bool> is_processed = this->background_committer_.IsCurrentlyProcessedContainerId(container_id);
        while (is_processed.valid() && is_processed.value()) {
            TRACE("Wait until currently processed container is committed: container " << container_id);
            dedupv1::base::timed_bool b = this->background_committer_.CommitFinishedConditionWaitTimeout(1);
            CHECK_RETURN(b != TIMED_FALSE, make_pair(LOOKUP_ERROR, ContainerStorageAddressData()), "Failed to wait for commit");
            if (b == TIMED_TIMEOUT) {
                TRACE("Wait for container commit timeout: container " << container_id);
            }

            is_processed = this->background_committer_.IsCurrentlyProcessedContainerId(container_id);
        }

        CHECK_RETURN(is_processed.valid(), make_pair(LOOKUP_ERROR, ContainerStorageAddressData()),
            "Failed to check processing state");
        TRACE("Finished waiting for currently processed container: container " << container_id);

        // re-lookup
        container_address = LookupContainerAddress(container_id, primary_container_lock, acquire_write_lock);
        CHECK_RETURN(container_address.first != LOOKUP_ERROR, container_address, "Failed to lookup container address: " << container_id);
        if (container_address.first == LOOKUP_NOT_FOUND) {
            return container_address; // return NOT FOUND
        }
    }
    // LOOKUP_FOUND
    return container_address;
}

std::string ContainerStorage::DebugString(const ContainerStorageAddressData& address_data) {
    stringstream sstr;

    if (address_data.has_primary_id()) {
        sstr << "primary id " <<  address_data.primary_id() << ", ";
    }
    sstr << address_data.file_index() << ":" << address_data.file_offset();
    if (address_data.has_log_id()) {
        sstr << ", log id " << address_data.log_id();
    }

    return sstr.str();
}

enum lookup_result ContainerStorage::ReadContainerLocked(Container* container,
                                                         const ContainerStorageAddressData& container_address) {
    DCHECK_RETURN(container, LOOKUP_ERROR, "Container not set");
    DCHECK_RETURN(state_ == RUNNING || state_ == STARTED, LOOKUP_ERROR,
        "Illegal state to read container: " << this->state_);
    ProfileTimer timer(this->stats_.total_read_container_time_);

    CHECK_RETURN(container->primary_id() != Storage::ILLEGAL_STORAGE_ADDRESS,
        LOOKUP_ERROR, "Container id not set: " << container->DebugString());
    uint64_t id = container->primary_id();

    // LOOKUP_FOUND
    unsigned int file_index = container_address.file_index();
    uint64_t file_offset = container_address.file_offset();
    if (has_superblock_) {
        file_offset += kSuperBlockSize;
    }
    CHECK_RETURN(file_index < this->file_.size(),
        LOOKUP_ERROR, "Illegal file index: " << file_index << ", file count " << this->file_.size());
    File* file = this->file_[file_index].file();
    CHECK_RETURN(file, LOOKUP_ERROR, "File not open: file index: " << file_index << ", file count " << this->file_.size());

    // Access file
    ScopedLock file_lock(this->file_[file_index].lock());
    {
        ProfileTimer file_lock_timer(this->stats_.total_file_lock_time_);
        CHECK_RETURN(file_lock.AcquireLockWithStatistics(
                &this->stats_.file_lock_free_,
                &this->stats_.file_lock_busy_), LOOKUP_ERROR, "Failed to acquire file lock: " <<
            "file index " << file_index <<
            ", container " << container->DebugString());
    }
    bool load_file = false;
    {
        ProfileTimer load_file_timer(this->stats_.total_file_load_time_);
        tbb::tick_count load_start = tbb::tick_count::now();
        load_file = container->LoadFromFile(file, file_offset, calculate_container_checksum_);
        this->stats_.average_container_load_latency_.Add((tbb::tick_count::now() - load_start).seconds() * 1000);
    }
    if (!load_file) {
        // error reporting code
        ERROR("Cannot load container: " <<
            "container id " << id <<
            ", (partially loaded) container " << container->DebugString() <<
            ", loaded address " << DebugString(container_address));
        return LOOKUP_ERROR;
    }
    CHECK_RETURN(file_lock.ReleaseLock(), LOOKUP_ERROR, "Unlock failed");
    this->stats_.readed_container_.fetch_and_increment();

    TRACE("Read container from disk: " <<
        container->DebugString() <<
        ", address " << DebugString(container_address));
    return LOOKUP_FOUND;
}

enum lookup_result ContainerStorage::ReadContainer(Container* container) {
    DCHECK_RETURN(container, LOOKUP_ERROR, "Container not set");
    DCHECK_RETURN(state_ == RUNNING || state_ == STARTED, LOOKUP_ERROR, "Illegal state to read container: " << this->state_);
    ProfileTimer timer(this->stats_.total_read_container_time_);

    CHECK_RETURN(container->primary_id() != Storage::ILLEGAL_STORAGE_ADDRESS,
        LOOKUP_ERROR, "Container id not set: " << container->DebugString());
    uint64_t id = container->primary_id();

    ReadWriteLock* lock = NULL;
    pair<lookup_result, ContainerStorageAddressData> container_address =
        LookupContainerAddressWait(id, &lock, false);
    CHECK_RETURN(container_address.first != LOOKUP_ERROR, LOOKUP_ERROR,
        "Failed to lookup container address: " << container->DebugString());
    if (container_address.first == LOOKUP_NOT_FOUND) {
        return LOOKUP_NOT_FOUND;
    }
    ScopedReadWriteLock scoped_lock(NULL);
    scoped_lock.SetLocked(lock);
    // LOOKUP_FOUND

    unsigned int file_index = container_address.second.file_index();
    uint64_t file_offset = container_address.second.file_offset();

    if (has_superblock_) {
        file_offset += kSuperBlockSize;
    }

    CHECK_RETURN(file_index < this->file_.size(),
        LOOKUP_ERROR, "Illegal file index: " << file_index << ", file count " << this->file_.size());
    File* file = this->file_[file_index].file();
    CHECK_RETURN(file, LOOKUP_ERROR, "File not open: file index: " << file_index << ", file count " << this->file_.size());

    // Access file

    ScopedLock file_lock(this->file_[file_index].lock());
    {
        ProfileTimer file_lock_timer(this->stats_.total_file_lock_time_);
        CHECK_RETURN(file_lock.AcquireLockWithStatistics(
                &this->stats_.file_lock_free_,
                &this->stats_.file_lock_busy_), LOOKUP_ERROR, "Failed to acquire file lock: " <<
            "file index " << file_index <<
            ", container " << container->DebugString());
    }
    bool load_file_result = false;
    {
        ProfileTimer load_file_timer(this->stats_.total_file_load_time_);
        tbb::tick_count load_start = tbb::tick_count::now();
        load_file_result = container->LoadFromFile(file, file_offset, calculate_container_checksum_);

        this->stats_.average_container_load_latency_.Add((tbb::tick_count::now() - load_start).seconds() * 1000);
    }
    if (!load_file_result) {
        // error reporting code
        pair<lookup_result, ContainerStorageAddressData> result = LookupContainerAddress(id, NULL, false);
        string first_address = "container " + ToString(id) + " address ";
        if (result.first == LOOKUP_ERROR) {
            first_address += "<Lookup error>";
        } else if (result.first == LOOKUP_NOT_FOUND) {
            first_address += "<Not found>";
        } else {
            first_address += DebugString(result.second);
        }
        result = LookupContainerAddress(container->primary_id(), NULL, false);
        string second_address = "container " + ToString(container->primary_id()) + " address ";
        if (result.first == LOOKUP_ERROR) {
            second_address += "<Lookup error>";
        } else if (result.first == LOOKUP_NOT_FOUND) {
            second_address += "<Not found>";
        } else {
            second_address += DebugString(result.second);
        }

        ERROR("Cannot load container: " <<
            "container id " << id <<
            ", (partially loaded) container " << container->DebugString() <<
            ", loaded address " << DebugString(container_address.second) <<
            ", " << first_address <<
            ", " << second_address);
        return LOOKUP_ERROR;
    }
    CHECK_RETURN(file_lock.ReleaseLock(), LOOKUP_ERROR, "Unlock failed");
    this->stats_.readed_container_.fetch_and_increment();

    TRACE("Read container from disk: " <<
        container->DebugString() <<
        ", address " << DebugString(container_address.second));
    return LOOKUP_FOUND;
}

bool ContainerStorage::WriteContainer(Container* container, const ContainerStorageAddressData& container_address) {
    DCHECK(container, "Container not set");
    DCHECK(container->primary_id() != 0, "Container id not set: " << container->DebugString());
    DCHECK(container->primary_id() != Storage::ILLEGAL_STORAGE_ADDRESS, "Illegal container id " + container->primary_id());
    DCHECK(state_ == RUNNING || state_ == STARTED, "Illegal state to write container: container id " << container->primary_id() <<
        ", state " << this->state_);
    FAULT_POINT("container-storage.write.pre");

    ProfileTimer write_timer(this->stats_.container_write_time_);

    CHECK(!start_context_.readonly(), "Container storage is in readonly mode");
    CHECK(IsValidAddressData(container_address), "Invalid address data: " << container_address.ShortDebugString());
    uint64_t container_id = container->primary_id();

    unsigned int file_index = container_address.file_index();
    uint64_t file_offset = container_address.file_offset();

    if (has_superblock_) {
        file_offset += kSuperBlockSize;
    }

    File* file = this->file_[file_index].file();
    CHECK(file, "File not open: file index " << file_index);

    // TODO(fermat): Is this lock needed any more? It should be no problem to access the same file at different positions concurrently.
    ScopedLock file_lock(this->file_[file_index].lock());
    CHECK(file_lock.AcquireLockWithStatistics(
            &this->stats_.file_lock_free_,
            &this->stats_.file_lock_busy_), "Failed to acquire file lock: file index " << file_index);

    CHECK(container->StoreToFile(file, file_offset, calculate_container_checksum_),
        "Cannot write container " << container_id << ": " << container->DebugString());
    CHECK(file_lock.ReleaseLock(), "Container unlock failed");
    FAULT_POINT("container-storage.write.after-write");
    TRACE("Write container: " << container->DebugString() << ", address " << DebugString(container_address));

    // the meta data index is not updated here, it is updated after some kind of log event commit
    FAULT_POINT("container-storage.write.post");
    return true;
}

bool ContainerStorage::MarkContainerCommitAsFailed(Container* container) {
    CHECK(container, "Container not set");
    CHECK(log_, "Log not set");

    WARNING("Failed to commit container: " << container->DebugString());
    ContainerCommitFailedEventData failed_data;
    failed_data.set_container_id(container->primary_id());

    std::vector<ContainerItem*>::iterator i;
    for (i = container->items().begin(); i != container->items().end(); i++) {
        ContainerItem* item = *i;
        CHECK(item, "Item not set");
        failed_data.add_item_key(item->key(), item->key_size());
    }

    CHECK(log_->CommitEvent(EVENT_TYPE_CONTAINER_COMMIT_FAILED, &failed_data, NULL, this, NO_EC),
        "Failed to commit container: " << failed_data.ShortDebugString());
    this->stats_.failed_container_++;
    return true;
}

bool ContainerStorage::CommitContainer(Container* container, const ContainerStorageAddressData& address) {
#ifdef DEDUPV1_CORE_TEST
    if (clear_data_called_) {
        return true;
    }
#endif
    DCHECK(container, "Container not set");
    DCHECK(container->primary_id() != 0, "Container id not set");
    DCHECK(container->primary_id() != Storage::ILLEGAL_STORAGE_ADDRESS, "Illegal container id: " + container->primary_id());
    DCHECK(state_ == RUNNING || state_ == STARTED, "Illegal state to commit container: " <<
        "container " << container->primary_id() << ", state " << this->state_);
    FAULT_POINT("container-storage.commit.pre");

    CHECK(!start_context_.readonly(), "Container storage is in readonly mode");
    uint64_t container_id = container->primary_id();

    TRACE("Committing container " << container_id << ": " << container->DebugString());

    // Lookup for security
    // we also could use IsCommitted here, but we get more information about all the stuff if we do it directly
    pair<lookup_result, ContainerStorageAddressData> result = this->LookupContainerAddress(container_id, NULL, false);
    CHECK(result.first != LOOKUP_ERROR, "Meta data lookup failed: container " << container->DebugString());
    if (result.first == LOOKUP_FOUND) {
        // there is already a container with that id stored
        ERROR("Container id " << container_id << " already stored: " <<
            "container " << container->DebugString() <<
            ", last given container id " << this->last_given_container_id_ <<
            ", address " << DebugString(result.second));

        CHECK(MarkContainerCommitAsFailed(container),
            "Failed to mark container commit as failed: " << container->DebugString());
        return false;
    }

    INFO("Commit container: " << container->DebugString() << ", " << DebugString(address));

    if (!this->WriteContainer(container, address)) {
        ERROR("Failed to write container: " << container->DebugString());
        this->meta_data_cache_.Unstick(container_id);

        CHECK(MarkContainerCommitAsFailed(container),
            "Failed to mark container commit as failed: " << container->DebugString());
        return false;
    }

    this->stats_.committed_container_.fetch_and_increment();

    ContainerCommittedEventData event_data;
    event_data.set_container_id(container_id);
    event_data.mutable_address()->CopyFrom(address);
    event_data.set_item_count(container->item_count());
    event_data.set_active_data_size(container->active_data_size());

    FAULT_POINT("container-storage.commit.before-gc");

    FAULT_POINT("container-storage.commit.before-log-commit");
    // Commit data only when on critical lock are hold, due to DIRECT processing
    int64_t event_log_id = 0;
    if (!this->log_->CommitEvent(EVENT_TYPE_CONTAINER_COMMITED, &event_data, &event_log_id, this, NO_EC)) {
        ERROR("Commit log entry about container commit failed: container " << container->DebugString());
        if (event_log_id != 0) {
            // if the log id is set in case or an error, the event has been written to disk.
            // it is therefore committed
            CHECK(this->meta_data_cache_.Update(container_id, STORAGE_ADDRESS_COMMITED),
                "Failed to update the meta data cache: container " << container->DebugString());
        } else {
            CHECK(this->meta_data_cache_.Unstick(container_id),
                "Failed to unstick the meta data cache: container " << container->DebugString());
            ContainerCommitFailedEventData failed_data;
            failed_data.set_container_id(container_id);
            CHECK(log_->CommitEvent(EVENT_TYPE_CONTAINER_COMMIT_FAILED, &failed_data, NULL, this, NO_EC),
                "Failed to commit container: " << failed_data.ShortDebugString());
        }
        return false;
    }

    // In some sense, it would be better to unpin it in the LogAck method, but there
    // we don't have to container available. But you should be aware that the
    // event might be replayed in parallel in the direct or even in the log bg thread.
    vector<ContainerItem*>::iterator i;
    for (i = container->items().begin(); i != container->items().end(); i++) {
        ContainerItem* item = *i;
        if (item) {
            DEBUG("Unpin chunk: container id " << container_id <<
                ", item " << item->DebugString());
            this->chunk_index_->ChangePinningState(item->key(), item->key_size(), false);
        }
    }

    // the cache is updated using the log acknowledgment
    // if we are here, the meta data cache item has been unsticked

    DEBUG("Committed container: " << container->DebugString()
                                  << ", address " << DebugString(address));
    container->Reuse(Storage::ILLEGAL_STORAGE_ADDRESS);
    FAULT_POINT("container-storage.commit.post");
    return true;
}

bool ContainerStorage::LogAck(event_type event_type, const Message* log_message, const LogReplayContext& context) {
    TRACE("Log ack: " << dedupv1::log::Log::GetEventTypeName(event_type) <<
        ", message " << (log_message ? log_message->ShortDebugString() : "") <<
        ", log id " << context.log_id());
    FAULT_POINT("container-storage.ack.pre");

    // Container Commit
    if (event_type == EVENT_TYPE_CONTAINER_COMMITED) {
        CHECK(log_message, "Log message not set");
        const ContainerCommittedEventData* event_message = dynamic_cast<const ContainerCommittedEventData*>(log_message);
        CHECK(event_message, "Log message not set")
        ContainerCommittedEventData event_data = *event_message;

        uint64_t container_id = event_data.container_id();

        // here we need the complex update mechanism to avoid a lock
        bool updated = false;
        while (!updated) {
            uint64_t hccid = highest_committed_container_id_;
            if (container_id > hccid) {
                // if it should updated.
                if (highest_committed_container_id_.compare_and_swap(container_id, hccid) == hccid) {
                    // success (no one changed the variable in between)
                    updated = true;
                }
            } else {
                // if the value is not higher, there is nothing to update
                break;
            }
        }

        ScopedReadWriteLock scoped_lock(&meta_data_lock_);
        CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire meta data lock");

        // Update log id
        event_data.mutable_address()->set_log_id(context.log_id());

        // Update index
        DCHECK(IsValidAddressData(event_data.address()), "Invalid address data: " << event_data.ShortDebugString());
        CHECK(this->meta_data_index_->Put(&container_id, sizeof(uint64_t), event_data.address()),
            "Meta data update failed: " << container_id <<
            ", address " << DebugString(event_data.address()));
        CHECK(this->meta_data_cache_.Update(container_id, STORAGE_ADDRESS_COMMITED), "Failed to update cache");

        CHECK(scoped_lock.ReleaseLock(), "Failed to release meta data lock");

        if (!gc_->OnCommit(event_data)) {
            WARNING("Error while updating storage gc: " << event_data.ShortDebugString());
        }

        // Container Moved
    } else if (event_type == EVENT_TYPE_CONTAINER_MOVED) {
        CHECK(log_message, "Log message not set");
        const ContainerMoveEventData* event_message = dynamic_cast<const ContainerMoveEventData*>(log_message);
        CHECK(event_message, "Log message not set")
        ContainerMoveEventData event_data = *event_message;

        uint64_t container_id = event_data.container_id();

        // Update log id
        event_data.mutable_new_address()->set_log_id(context.log_id());

        // Update index
        ScopedReadWriteLock scoped_lock(&meta_data_lock_);
        CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire meta data lock");

        DCHECK(IsValidAddressData(event_data.new_address()), "Invalid address data: " << event_data.ShortDebugString());
        DCHECK(IsValidAddressData(event_data.old_address()), "Invalid address data: " << event_data.ShortDebugString());

        CHECK(this->meta_data_index_->Put(&container_id, sizeof(uint64_t), event_data.new_address()),
            "Meta data update failed: " << container_id <<
            ", address " << DebugString(event_data.new_address()));

        ScopedReadWriteLock scoped_container_lock(this->GetContainerLock(event_data.container_id()));
        CHECK(scoped_container_lock.AcquireWriteLock(), "Cannot acquire container write lock");

        CHECK(scoped_lock.ReleaseLock(), "Failed to release meta data lock");

        if (this->allocator_) {
            if (!this->allocator_->OnMove(event_data)) {
                WARNING("Failed to update storage allocator after container move: " <<
                    event_data.ShortDebugString());
            }
        }
        if (this->gc_) {
            if (!this->gc_->OnMove(event_data)) {
                WARNING("Error while updating storage gc: " << event_data.ShortDebugString());
            }
        }
        CHECK(scoped_container_lock.ReleaseLock(), "Failed to release container lock");

        // Container Merged
    } else if (event_type == EVENT_TYPE_CONTAINER_MERGED) {
        CHECK(log_message, "Log message not set");
        const ContainerMergedEventData* event_message = dynamic_cast<const ContainerMergedEventData*>(log_message);
        CHECK(event_message, "Log message not set")
        ContainerMergedEventData event_data = *event_message;

        uint64_t container_id = event_data.new_primary_id();

        // Update log id
        event_data.mutable_new_address()->set_log_id(context.log_id());

        // Update index
        TRACE("Redirect primary id " << container_id << ": " << event_data.new_address().ShortDebugString());

        ScopedReadWriteLock scoped_lock(&meta_data_lock_);
        CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire meta data lock");
        DCHECK(IsValidAddressData(event_data.new_address()), "Invalid address data: " << event_data.ShortDebugString());
        CHECK(this->meta_data_index_->Put(&container_id, sizeof(uint64_t), event_data.new_address()),
            "Meta data update failed: " << container_id <<
            ", address " << DebugString(event_data.new_address()));
        FAULT_POINT("container-storage.ack.container-merge-after-put");

        // set the redirection pointer
        ContainerStorageAddressData secondary_data_address;
        secondary_data_address.set_primary_id(container_id);
        secondary_data_address.set_log_id(context.log_id());

        for (int i = 0; i < event_data.new_secondary_id_size(); i++) {
            uint64_t id = event_data.new_secondary_id(i);
            TRACE("Redirect merged container " << id << ": primary id " << secondary_data_address.primary_id());
            DCHECK(IsValidAddressData(secondary_data_address), "Invalid address data: " << secondary_data_address.ShortDebugString());
            CHECK(this->meta_data_index_->Put(&id, sizeof(id), secondary_data_address),
                "Cannot write new container address: container id " << id);
            FAULT_POINT("container-storage.ack.container-merge-after-secondary-put");
        }
        for (int i = 0; i < event_data.unused_ids_size(); i++) {
            uint64_t id = event_data.unused_ids(i);
            TRACE("Delete unused container id: container " << id);
            CHECK(this->meta_data_index_->Delete(&id, sizeof(id)), "Cannot delete unused container address: container id " << id);
            CHECK(this->meta_data_cache_.Delete(id), "Failed to delete unused container address from meta data cache: container id " << id);
            FAULT_POINT("container-storage.ack.container-merge-middle");
        }

        // acquire the container lock while the meta data lock is held. This is
        // the usual lock ordering
        ScopedReadWriteLock container_lock1(this->GetContainerLock(event_data.first_id()));
        CHECK(container_lock1.AcquireWriteLock(), "Cannot acquire container write lock: container id " << event_data.first_id());
        ScopedReadWriteLock container_lock2(this->GetContainerLock(event_data.second_id()));
        if (container_lock2.Get() != container_lock1.Get()) {
            CHECK(container_lock2.AcquireWriteLock(), "Cannot acquire container write lock: container id " << event_data.second_id());
        }
        ScopedReadWriteLock container_lock3(this->GetContainerLock(event_data.new_primary_id()));
        if (container_lock3.Get() != container_lock1.Get() && container_lock3.Get() != container_lock2.Get()) {
            CHECK(container_lock3.AcquireWriteLock(), "Cannot acquire container write lock: container id " << event_data.new_primary_id());
        }
        CHECK(scoped_lock.ReleaseLock(), "Failed to release meta data lock");

        // these two calls must be called a) after the commit and b) with a container lock held
        // No client is allowed to read the container on the old location after the allocator freed the space
        // We ensure this by acquiring the container lock while the meta data lock is held.
        // If any client has started a container read (with acquing the meta data lock) it is finished and
        // has released the meta data lock and the container lock when the thread is here.

        if (this->allocator_) {
            CHECK(this->allocator_->OnMerge(event_data),
                "Cannot get merge container address: " << event_data.ShortDebugString());
        }
        if (this->gc_) {
            CHECK(this->gc_->OnMerge(event_data),
                "Failed to report merge to gc: " << event_data.ShortDebugString());
        }

        CHECK(container_lock1.ReleaseLock(), "Cannot release container write lock");
        if (container_lock2.Get() != container_lock1.Get()) {
            CHECK(container_lock2.ReleaseLock(), "Cannot release container write lock");
        }
        if (container_lock3.Get() != container_lock1.Get() && container_lock3.Get() != container_lock2.Get()) {
            CHECK(container_lock3.ReleaseLock(), "Cannot release container write lock");
        }
    } else if (event_type == EVENT_TYPE_CONTAINER_DELETED) {
        CHECK(log_message, "Log message not set");
        const ContainerDeletedEventData* event_message = dynamic_cast<const ContainerDeletedEventData*>(log_message);
        CHECK(event_message, "Log message not set")
        ContainerDeletedEventData event_data = *event_message;

        uint64_t container_id = event_data.container_id();

        // remove old pointers
        set<uint64_t> all_old_ids;
        all_old_ids.insert(container_id);
        for (int i = 0; i < event_data.secondary_container_id_size(); i++) {
            all_old_ids.insert(event_data.secondary_container_id(i));
        }

        ScopedReadWriteLock scoped_lock(&meta_data_lock_);
        CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire meta data lock");

        // now all_old_ids only contains the id to which no chunk is referencing anymore.
        // no one is or ever will be interested anymore into such an container id
        for (set<uint64_t>::const_iterator i = all_old_ids.begin(); i != all_old_ids.end(); i++) {
            uint64_t id = *i;
            TRACE("Delete unused container id: container " << id);
            CHECK(this->meta_data_index_->Delete(&id, sizeof(id)), "Cannot delete unused container address: container id " << id);
            CHECK(this->meta_data_cache_.Delete(id), "Failed to delete unused container address from meta data cache: container id " << id);
            FAULT_POINT("container-storage.ack.container-delete-middle");
        }

        ScopedReadWriteLock container_lock(this->GetContainerLock(event_data.container_id()));
        CHECK(container_lock.AcquireWriteLock(), "Cannot acquire container write lock");

        CHECK(scoped_lock.ReleaseLock(), "Failed to release meta data lock");

        // Ok, there the probability of a race condition is non-existing because there should be 0 (in words zero)
        // clients accessing this container. It is gone. Deal with it.

        if (this->allocator_) {
            CHECK(this->allocator_->OnDeleteContainer(event_data),
                "Cannot get delete container address: " << event_data.ShortDebugString());
        }

        CHECK(container_lock.ReleaseLock(), "Failed to release container lock");

    }
    FAULT_POINT("container-storage.ack.post");
    return true;
}

bool ContainerStorage::CheckOpenContainerForTimeouts() {
    for (size_t i = 0; i < this->write_cache_.GetSize(); i++) {
        if (!this->write_cache_.IsTimedOut(i, this->timeout_seconds_)) {
            continue; // no timeout
        }
        this->write_cache_.ResetTimeout(i);
        ReadWriteLock* write_cache_lock = NULL;
        Container* check_write_container = NULL;
        CHECK(this->write_cache_.GetWriteCacheContainerByIndex(i, &check_write_container, &write_cache_lock),
            "Failed to get write cache container");
        CHECK(write_cache_lock, "Write cache lock not set");
        CHECK(check_write_container, "Write container not set");

        ScopedReadWriteLock scoped_write_container_lock(write_cache_lock);
        CHECK(scoped_write_container_lock.AcquireWriteLock(),
            "Failed to acquire write cache lock");

        if (Storage::IsValidAddress(check_write_container->primary_id())) {
            // Commit any timed out container to disk.
            TRACE("Found timed out container: " << check_write_container->DebugString());

            this->stats_.container_timeouts_.fetch_and_increment();
            if (!this->PrepareCommit(check_write_container)) {
                WARNING("Failed to prepare commit: " << check_write_container->DebugString());
            }
            // check write container has no a ILLEGAL_STORAGE_ADDRESS
            // The container gets a new id when a chunk is added
        }
    }
    return true;
}

bool ContainerStorage::TimeoutCommitRunner() {
    // Wait until the container storage is started
    while (this->state_ == STARTED) {
        ThreadUtil::Sleep(2, ThreadUtil::SECONDS);
    }
    DEBUG("Starting timeout committer");
    // Check for idle containers until the storage is stopped.
    while (!timeout_committer_should_stop_) {
        // Read all container write times.
        if (!CheckOpenContainerForTimeouts()) {
            WARNING("Container timeout check failed");
        }

        // Wait a few seconds as not to do busy waiting.
        ThreadUtil::Sleep(timeout_seconds_ / 2, ThreadUtil::SECONDS);
    }
    DEBUG("Stopping timeout committer");
    return true;
}

void ContainerStorage::RegisterStorage() {
    Storage::Factory().Register("container-storage",
        &ContainerStorage::CreateStorage);
}

Storage* ContainerStorage::CreateStorage() {
    Storage * storage = new ContainerStorage();
    return storage;
}

ContainerStorage::ContainerStorage() : meta_data_cache_(this),
    timeout_seconds_(kTimeoutSecondsDefault),
    cache_(this),
    write_cache_(this) {
    this->container_size_ = Container::kDefaultContainerSize;
    this->meta_data_index_ = NULL;
    this->last_given_container_id_ = 0;
    this->timeout_committer_ = NULL;
    this->compression_ = NULL;
    this->state_ = CREATED;
    this->idle_detector_ = NULL;
    this->gc_ = NULL;
    this->allocator_ = NULL;
    this->log_ = NULL;
    this->preallocate_ = false;
    this->size_ = 0;
    info_store_ = NULL;
    calculate_container_checksum_ = true;
    timeout_committer_should_stop_ = false;
    has_superblock_ = true;
    had_been_started_ = false;
    chunk_index_ = NULL;
    #ifdef DEDUPV1_CORE_TEST
    clear_data_called_ = false;
    #endif
}

ContainerStorage::Statistics::Statistics() : average_container_load_latency_(16) {
    this->reads_ = 0;
    this->file_lock_busy_ = 0;
    this->file_lock_free_ = 0;
    this->global_lock_busy_ = 0;
    this->global_lock_free_ = 0;
    this->handover_lock_busy_ = 0;
    this->handover_lock_free_ = 0;
    this->read_cache_hit_ = 0;
    this->write_cache_hit_ = 0;
    this->container_lock_free_ = 0;
    this->container_lock_busy_ = 0;

    this->committed_container_ = 0;
    this->container_timeouts_ = 0;
    this->readed_container_ = 0;
    this->committed_container_ = 0;
    this->moved_container_ = 0;
    this->merged_container_ = 0;
    this->failed_container_ = 0;
    this->deleted_container_ = 0;
}

ContainerStorage::~ContainerStorage() {
}

bool ContainerStorage::Init() {
    this->timeout_committer_ = new Thread<bool>(
        NewRunnable(this, &ContainerStorage::TimeoutCommitRunner),
        "timeout commit");
    CHECK(this->timeout_committer_, "Failed to alloc timeout committer");
    return true;
}

bool ContainerStorage::SetOption(const string& option_name, const string& option) {
    if (option_name == "container-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->container_size_ = ToStorageUnit(option).value();
        CHECK(this->container_size_ % PAGE_SIZE == 0, "Container not aligned");
        return true;
    }
    if (option_name == "size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->size_ = ToStorageUnit(option).value();
        CHECK(this->size_ % PAGE_SIZE == 0, "Storage size not aligned");
        return true;
    }
    if (option_name == "checksum") {
        CHECK(To<bool>(option).valid(), "Illegal option");
        this->calculate_container_checksum_ = To<bool>(option).value();
        return true;
    }
    if (option_name == "preallocate") {
        CHECK(To<bool>(option).valid(), "Illegal option");
        this->preallocate_ = To<bool>(option).value();
        return true;
    }
    if (option_name == "read-cache-size") {
        return this->cache_.SetOption("size", option);
    }
    if (option_name == "write-container-count") {
        return this->write_cache_.SetOption("size", option);
    }
    if (StartsWith(option_name, "background-commit.")) {
        CHECK(this->background_committer_.SetOption(option_name.substr(strlen("background-commit.")), option), "Config failed");
        return true;
    }
    if (option_name == "timeout-commit-timeout") {
        CHECK(this->timeout_committer_ != NULL, "Timeout committer not enabled");
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->timeout_seconds_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "compression") {
        CHECK(this->compression_ == NULL, "Compression already set");
        if (option == "deflate") {
            this->compression_ = Compression::NewCompression(Compression::COMPRESSION_ZLIB_1);
            return true;
        } else if (option == "bz2") {
            this->compression_ = Compression::NewCompression(Compression::COMPRESSION_BZ2);
            return true;
        } else if (option == "snappy") {
            this->compression_ = Compression::NewCompression(Compression::COMPRESSION_SNAPPY);
            return true;
        } else if (option == "lz4") {
            this->compression_ = Compression::NewCompression(Compression::COMPRESSION_LZ4);
            return true;
        } else if (option == "none") {
            this->compression_ = NULL;
            return true;
        } else {
            ERROR("Illegal compression option selected: " << option);
            return false;
        }
    }
    if (option_name == "filename") {
        CHECK(option.size() <= 255, "Filename too long");
        CHECK(option.size() > 0, "Filename too short");

        for (int i = 0; i < this->file_.size(); i++) {
            CHECK(this->file_[i].filename() != option, "Double container file: " << option);
        }

        // we do it this more complex way to avoid copy/assign operations
        int next_index = file_.size();
        file_.resize(file_.size() + 1); // new entry
        file_[next_index].Init(option);
        return true;
    }
    if (option_name == "filename.clear") {
        CHECK(To<bool>(option).valid(), "Illegal option");
        CHECK(To<bool>(option).value(), "Illegal option");
        file_.clear();
        return true;
    }
    if (option_name == "filesize") {
        CHECK(file_.size() > 0, "No file configured");
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        CHECK(ToStorageUnit(option).value() >= 0, "Illegal file size " << option);

        file_.back().set_file_size(ToStorageUnit(option).value());
        return true;
    }
    if (option_name == "meta-data") {
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Index creation failed");
        CHECK(index->IsPersistent(), "Index must be persistent");
        this->meta_data_index_ = index->AsPersistentIndex();

        // Set default options
        this->meta_data_index_->SetOption("max-key-size", "8");
        this->meta_data_index_->SetOption("max-value-size", "16");

        return true;
    }
    if (StartsWith(option_name, "meta-data.")) {
        CHECK(this->meta_data_index_, "Meta data index not set");
        CHECK(this->meta_data_index_->SetOption(option_name.substr(strlen("meta-data.")), option), "Config failed");
        return true;
    }
    if (StartsWith(option_name, "write-cache.")) {
        CHECK(this->write_cache_.SetOption(option_name.substr(strlen("write-cache.")), option), "Config failed");
        return true;
    }
    if (StartsWith(option_name, "read-cache.")) {
        CHECK(this->cache_.SetOption(option_name.substr(strlen("read-cache.")), option), "Config failed");
        return true;
    }
    if (option_name == "gc") {
        this->gc_ = ContainerGCStrategyFactory::Create(option);
        CHECK(this->gc_, "Cannot create gc strategy: " << option);
        return true;
    }
    if (StartsWith(option_name, "gc.")) {
        CHECK(this->gc_, "gc not set");
        CHECK(this->gc_->SetOption(option_name.substr(strlen("gc.")), option), "Configuration failed: " << option_name << " - " << option);
        return true;
    }
    if (option_name == "alloc") {
        this->allocator_ = ContainerStorageAllocatorFactory::Create(option);
        CHECK(this->allocator_, "Cannot create allocator: " << option);
        return true;
    }
    if (StartsWith(option_name, "alloc.")) {
        CHECK(this->allocator_, "allocator not set");
        CHECK(this->allocator_->SetOption(option_name.substr(strlen("alloc.")), option),
            "Configuration failed: " << option_name << " - " << option);
        return true;
    }
    ERROR("Illegal option: " << option_name);
    return false;
}

bool ContainerStorage::Format(const ContainerFile& file, File* format_file) {
    // The file_ member of format file it not yet set, therefore the format file
    DCHECK(format_file, "Format file not set");
    DCHECK(!file.uuid().IsNull(), "File uuid not set");

    if (has_superblock_) {
        ContainerSuperblockData superblock;
        superblock.set_uuid(file.uuid().ToString());

        CHECK(format_file->WriteSizedMessage(0, superblock, kSuperBlockSize, true) > 0,
            "Failed to write superblock: " << superblock.DebugString());
    }

    if (preallocate_) {
        uint64_t container_per_file = file.file_size() / container_size_;

        if (has_superblock_) {
            // Fallocate does never overwrite existing Data, so we can start at 0
            CHECK(format_file->Fallocate(0, container_per_file * container_size_ + kSuperBlockSize), "Could not preallocate file " << file.filename());
        } else {
            CHECK(format_file->Fallocate(0, container_per_file * container_size_), "Could not preallocate file " << file.filename());
        }
    }
    return true;
}

bool ContainerStorage::Start(const StartContext& start_context, DedupSystem* system) {
    unsigned int i = 0;
    Walltimer startup_timer;

    INFO("Starting container storage");

    start_context_ = start_context;
    DCHECK(system, "System not set");
    DCHECK(system->log(), "Log not set");
    DCHECK(system->log()->IsStarted(), "Log is not started");
    DCHECK(system->idle_detector(), "Idle detector not set");
    DCHECK(system->info_store(), "Info store not set");
    info_store_ = system->info_store();
    log_ = system->log();
    idle_detector_ = system->idle_detector();
    chunk_index_ = system->chunk_index();
    DCHECK(chunk_index_, "Chunk index not set");

    CHECK(this->file_.size() > 0, "Container files not configured");
    CHECK(this->meta_data_index_, "Metadata index not configured");
    CHECK(this->container_size_ > 0, "Container size invalid: container size " << this->container_size_);
    CHECK(this->allocator_, "Allocator not configured");
    CHECK(this->gc_, "Garbage collector not configured");
    CHECK(this->size_ > 0, "Container storage size not configured");

    this->container_lock_.Init(1024);
    this->state_ = STARTING;

    ContainerLogfileData log_data;
    lookup_result info_lookup = ReadMetaInfo(&log_data);
    CHECK(info_lookup != LOOKUP_ERROR, "Failed to read meta info");
    CHECK(!(info_lookup == LOOKUP_NOT_FOUND && !start_context.create()), "Failed to lookup meta info in non-create startup mode");

    if (info_lookup == LOOKUP_FOUND) {
        CHECK(this->container_size_ == log_data.container_size(),
            "Container size mismatch (logged size " << log_data.container_size()
                                                    << ", configured size " << this->container_size_ << ")");
        this->last_given_container_id_ = log_data.last_given_container_id();

        if (!log_data.has_contains_superblock() || !log_data.contains_superblock()) {
            has_superblock_ = false; // legacy mode
        }
    }

    // The complete file opening logic, we have to handle three (good/valid) cases
    // a) file existed and was already stored in the log data: Easy
    // b) file existed and was not stored in the log data. This happened on old systems that to not have
    //    the file information in the log data.
    // c) file doesn't exists
    //
    // another aspect that must be considered is the explicit setting of file sizes

    int64_t size_to_assign = size_;
    bool use_comp_mode = false;
    bool use_extend_feature = false; // extending storage later and explicit file sizes are
    // new features that are not compatible with the comp mode.
    // Workaround: start the system once with a unchanged config to upgrade the state to the
    // new mode. After that, the extend feature can be used.

    // open all existing files
    for (i = 0; i < this->file_.size(); i++) {
        ScopedPtr<File> tmp_file(File::Open(this->file_[i].filename(), O_RDWR | O_LARGEFILE | O_SYNC, 0));
        if (tmp_file.Get()) {
            // The file seems to be valid
            // special checks for old files
            if (file_[i].file_size() == 0) {
                // no explicit file size set and not a newly created file

                // now we have to search the correct file size
                // there are two possibilities: a) log info entry exists => done b) backup path => assume default file size
                if (log_data.file_size() <= i || !log_data.file(i).has_file_size()) {
                    // use default file size
                    uint64_t default_size_per_file = size_ /  file_.size();
                    file_[i].set_file_size(default_size_per_file);
                    use_comp_mode = true;
                } else {
                    file_[i].set_file_size(log_data.file(i).file_size());
                }
            } else {
                // files are not allowed to change their file size
                if (log_data.file_size() > i && log_data.file(i).has_file_size()) {
                    CHECK(file_[i].file_size() == log_data.file(i).file_size(), "Illegal file size: " << file_[i].filename() <<
                        ", reason file size doesn't match with old file size" <<
                        ", configured file size " <<  file_[i].file_size() <<
                        ", expected file size " << log_data.file(i).file_size());
                }
                use_extend_feature = true;
            }
            size_to_assign -= file_[i].file_size();
            TRACE("Assign " << file_[i].file_size() << " to new file " << file_[i].filename() << ", size to assign " << size_to_assign);

            // Check super block contents
            if (has_superblock_ && log_data.file_size() > i) {
                ContainerSuperblockData superblock;

                CHECK(tmp_file->ReadSizedMessage(0, &superblock, kSuperBlockSize, true),
                    "Failed to read super block");

                if (log_data.file(i).has_uuid() && superblock.has_uuid()) {
                    CHECK(log_data.file(i).uuid() == superblock.uuid(), "Illegal container file uuid: " <<
                        "stored uuid " << superblock.uuid() <<
                        ", expected uuid " << log_data.file(i).uuid());
                }
                Option<UUID> uuid = UUID::FromString(superblock.uuid());
                CHECK(uuid.valid(), "Invalid uuid in super block: " << superblock.ShortDebugString());
                file_[i].set_uuid(uuid.value());
            }

            if (preallocate_) {
                Option<int64_t> fs = tmp_file->GetSize();
                CHECK(fs.valid(), "Failed to get file size: " << file_[i].filename());
                uint64_t total_file_size = fs.value();
                if (has_superblock_) {
                    total_file_size -= kSuperBlockSize;
                }
                CHECK(total_file_size == file_[i].file_size(), "Illegal file size: " << file_[i].filename() <<
                    ", reason file size not matching with pre-allocated on-disk size" <<
                    ", expected file size " <<  file_[i].file_size() <<
                    ", actual file size " << total_file_size);
            }

            file_[i].Start(tmp_file.Get(), false);
            tmp_file.Release();
        }
    }
    CHECK(!(use_comp_mode && use_extend_feature), "Explicit file size assigning is not possible in compatibility mode");
    DEBUG("Size to assign: " << size_to_assign);
    // not found: what now
    uint64_t file_count_no_explicit_fs = 0;

    // remove all files with explicit set file size
    for (i = 0; i < this->file_.size(); i++) {
        if (!file_[i].file()) {
            if (file_[i].file_size() > 0) {
                DEBUG("File " << file_[i].filename() << " has explicit size " << file_[i].file_size());
                size_to_assign -= file_[i].file_size();
            } else {
                file_count_no_explicit_fs++;
            }
        }
    }
    DEBUG("Size to assign: " << size_to_assign << ", files to assign to " << file_count_no_explicit_fs);

    for (i = 0; i < file_.size(); i++) {
        if (!file_[i].file()) {
            // if the file existed before, we have a problem
            CHECK(log_data.file_size() <= i, "Error opening storage file: filename " << this->file_[i].filename() <<
                ", reason: File existed once, but cannot be opened");

            if (!start_context.create() && start_context.force()) {
                use_extend_feature = true;
                CHECK(!use_comp_mode, "Extending is not possible in compatibility mode");
            }
            CHECK(start_context.create() || start_context.force(),
                "Error opening storage file: filename " << this->file_[i].filename());
            INFO("Creating new container data file " << this->file_[i].filename());

            uint64_t file_size = file_[i].file_size(); // explicit file size
            if (file_size == 0) {
                int64_t default_size_per_file = size_to_assign / file_count_no_explicit_fs;
                CHECK(default_size_per_file > 0, "Illegal container config: size to assign " << FormatStorageUnit(size_to_assign) <<
                    ", files to assign to " << file_count_no_explicit_fs);
                file_[i].set_file_size(default_size_per_file);
                size_to_assign -= file_[i].file_size();
                file_count_no_explicit_fs--;
                file_size = file_[i].file_size();
                DEBUG("Assign " << file_size << " to new file " << file_[i].filename());
            }
            // validate file size
            CHECK(file_size % container_size_ == 0, "Size per file is not aligned with container size: " <<
                "filename " << file_[i].filename() <<
                ", size " << file_size <<
                ", container size " << container_size_);
            uint64_t container_per_file = file_size / container_size_;
            CHECK(container_per_file % 8 == 0, "Containers per file must be devidable by 8: " <<
                "containers in file " << container_per_file <<
                ", file " << file_[i].filename() <<
                ", container file size " << file_size <<
                ", total size " << size_);

            CHECK(File::MakeParentDirectory(this->file_[i].filename(), start_context.dir_mode().mode()),
                "Failed to check parent directories");

            file_[i].set_uuid(UUID::Generate());

            // open file without O_SYNC to speed up formatting
            ScopedPtr<File> format_file(File::Open(this->file_[i].filename(), O_RDWR | O_LARGEFILE | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP));
            CHECK(format_file.Get(), "Failed to open file for formatting: " << this->file_[i].filename());
            CHECK(Format(this->file_[i], format_file.Get()), "Failed to format " << this->file_[i].filename());
            CHECK(format_file.Release()->Close(), "Failed to close file " << this->file_[i].filename());

            ScopedPtr<File> tmp_file(File::Open(this->file_[i].filename(), O_RDWR | O_LARGEFILE | O_SYNC, 0));
            CHECK(tmp_file.Get(), "Failed to open container file " << file_[i].filename());
            CHECK(chmod(this->file_[i].filename().c_str(), start_context.file_mode().mode()) == 0,
                "Failed to change file permissions: " << this->file_[i].filename());
            if (start_context.file_mode().gid() != -1) {
                CHECK(chown(this->file_[i].filename().c_str(), -1, start_context.file_mode().gid()) == 0,
                    "Failed to change file group: " << this->file_[i].filename());
            }

            file_[i].Start(tmp_file.Release(), true);
        }
    }

    CHECK(size_to_assign == 0, "Illegal container configuration: total size " << FormatStorageUnit(size_) <<
        ", not assigned size " << FormatStorageUnit(size_to_assign));

    // we wait to dump the meta data until the formatting is done
    if (info_lookup == LOOKUP_NOT_FOUND && start_context.create()) {
        CHECK(DumpMetaInfo(), "Failed to dump info");
        this->last_given_container_id_ = 0;
    }
    // when the state, e.g. the last given container id is incorrect here because of a dirty shutdown
    // it will be restored during the log replay

    DEBUG("Init last given container id to " << this->last_given_container_id_);
    this->initial_given_container_id_ = this->last_given_container_id_;
    this->highest_committed_container_id_ = this->last_given_container_id_;

    CHECK(this->meta_data_index_->Start(start_context), "Container index start failed");
    CHECK(this->write_cache_.Start(), "Failed to start write cache");
    CHECK(this->cache_.Start(), "Failed to start cache");

    if (this->idle_detector_) {
        CHECK(this->idle_detector_->RegisterIdleConsumer("container-storage", this),
            "Cannot register container storage as idle tick consumer");
    }

    CHECK(this->log_->RegisterConsumer("container-storage", this), "Cannot register container storage as log consumer");

    CHECK(this->background_committer_.Start(this), "Cannot start committer");
    CHECK(this->gc_->Start(start_context, this), "Cannot start gc");
    CHECK(this->allocator_->Start(start_context, this), "Cannot start allocator");

    this->state_ = STARTED;

    if (!start_context.dirty() && !start_context_.readonly()) {
        // check container id
        uint64_t id = (this->last_given_container_id_ + 1);
        pair<lookup_result, ContainerStorageAddressData> result = this->LookupContainerAddress(id, NULL, false);
        CHECK(result.first != LOOKUP_ERROR, "Meta data lookup failed: container " << id);
        if (result.first == LOOKUP_FOUND) {
            ERROR("Last given container id " << id << " already stored: Illegal last given container id");
            return false;
        }
    } else {
        // it is not valid to check the validity of the container id here as it might be corrected by the log replay
        // or the system is readonly and we don't care because there is no way that the container is overwritten.
    }
    INFO("Started container storage (startup time: " << startup_timer.GetTime() << "ms" <<
        ", last given container id: " << this->last_given_container_id_ << ")");

    had_been_started_ = true;
    return true;
}

bool ContainerStorage::Run() {
    CHECK(this->state_ == STARTED, "Illegal state: " << state_);

    if (!start_context_.readonly()) {
        // check container id
        // the next container id should not be used
        uint64_t id = (this->last_given_container_id_ + 1);
        pair<lookup_result, ContainerStorageAddressData> result = this->LookupContainerAddress(id, NULL, false);
        CHECK(result.first != LOOKUP_ERROR, "Meta data lookup failed: container " << id);
        if (result.first == LOOKUP_FOUND) {
            ERROR("Last given container id " << id << " already stored: Illegal last given container id");
            return false;
        }

        // it might be possible that items have been added to a container, but never "reached" a block mapping
        // written commit. These items are ophrans. When the system crashed, we mark the items of a series
        // of containers as possible ophrans chunks to be on the safe side.
        if (start_context_.has_crashed()) {
            // over all containers that can be open someout. Usually the write cache + what is in the bg committer
            // should be enough, but we add 8 to be secure.
            int ophran_range = 8 + write_cache_.GetSize() + background_committer_.thread_count();
            id = Container::kLeastValidContainerId;
            if (last_given_container_id_ > ophran_range) {
                id = last_given_container_id_ - ophran_range;
            }
            DEBUG("Check for ophran chunks: low container id " << id << ", high container id " << last_given_container_id_);
            for (; id <= last_given_container_id_; id++) {
                TRACE("Check for ophrans in container: container id " << id);
                Container container;
                container.Init(id, container_size_);
                lookup_result lr = ReadContainerWithCache(&container);
                CHECK(lr != LOOKUP_ERROR, "Failed to read container: " << container.DebugString());
                if (lr == LOOKUP_FOUND) {
                    TRACE("Found container: " << container.DebugString());
                    OphranChunksEventData event_data;

                    vector<ContainerItem*>::const_iterator i;
                    for (i = container.items().begin(); i != container.items().end(); i++) {
                        ContainerItem* item = *i;
                        TRACE("Found container item: " << item->DebugString());
                        DCHECK(item, "Item not set");
                        if (item->is_deleted()) {
                            TRACE("Container item is deleted: " << item->DebugString());
                        } else if (item->original_id() != id) {
                            TRACE("Skip item: " << item->DebugString() <<
                                ", import container id " << id);
                        } else {
                            event_data.add_chunk_fp(item->key(), item->key_size());
                        }
                    }
                    if (event_data.chunk_fp_size() > 0) {
                        DEBUG("Mark chunks as possible ophrans: " <<
                            "container id " << id <<
                            ", chunk count " << event_data.chunk_fp_size());

                        CHECK(log_->CommitEvent(dedupv1::log::EVENT_TYPE_OPHRAN_CHUNKS, &event_data, NULL, NULL, NO_EC),
                            "Cannot commit log entry: " << event_data.ShortDebugString());
                    }
                } else {
                    // some containers might be missing, but we do not care. This happens
                }
            }
        }
    }
    DEBUG("Running container storage");

    CHECK(this->background_committer_.Run(), "Failed to run bg");
    if (this->timeout_committer_ != NULL) {
        CHECK(this->timeout_committer_->Start(), "Cannot start timeout committer");
    }
    if (allocator_) {
        CHECK(allocator_->Run(), "Failed to run allocator");
    }
    state_ = RUNNING;
    return true;
}

bool ContainerStorage::Close() {
    unsigned int i = 0;

    enum container_storage_state old_state = state_;
    if (state_ == RUNNING || state_ == STARTED) {
        if (!this->Stop(dedupv1::StopContext::FastStopContext())) {
            WARNING("Cannot stop container storage");
        }
    }
    // there the state might have changed.

    DEBUG("Closing container storage");
    if (this->gc_) {
        CHECK(this->gc_->Close(), "Cannot close gc");
        this->gc_ = NULL;
    }

    if (info_store_ && had_been_started_) {
        // if the storage is not started, there is nothing to dump
        if (!DumpMetaInfo()) {
            WARNING("Failed to dump meta info");
        }
    }

    // the committer can be closed before the last commit because
    // it bypasses the background thread.

    for (vector<Container*>::iterator i = this->write_cache_.GetCache().begin(); i != this->write_cache_.GetCache().end(); i++) {
        Container* write_container = *i;
        if (write_container &&
            write_container->primary_id() != 0 &&
            write_container->primary_id() != Storage::ILLEGAL_STORAGE_ADDRESS &&
            write_container->item_count() > 0) {
            WARNING("Container " << write_container->primary_id() << " not committed during shutdown");
        }
    }
    if (!this->background_committer_.Close()) {
        WARNING("Failed to close background committer");
    }
    if (!this->write_cache_.Close()) {
        WARNING("Failed to close write cache");
    }
    if (this->compression_) {
        // the compression must be closed after the last commit is done.s
        delete this->compression_;
        this->compression_ = NULL;
    }
    if (this->allocator_) {
        CHECK(this->allocator_->Close(), "Failed to close allocator");
        delete this->allocator_;
        this->allocator_ = NULL;
    }
    for (i = 0; i < this->file_.size(); i++) {
        if (this->file_[i].file()) {
            if (!this->file_[i].file()->Close()) {
                WARNING("Error closing container file");
            }
        }
    }
    file_.clear();
    if (!this->cache_.Close()) {
        WARNING("Failed to close container read cache");
    }
    if (this->meta_data_index_) {
        if (!this->meta_data_index_->Close()) {
            WARNING("Failed to close meta data index");
        }
        this->meta_data_index_ = NULL;
    }
    if (old_state == STARTED || old_state == RUNNING || old_state == STOPPED) {
        if (log_ && log_->IsRegistered("container-storage").value()) {
            if (!this->log_->UnregisterConsumer("container-storage")) {
                WARNING("Failed to unregister container storage");
            }
        }
    }
    if (this->idle_detector_ && this->idle_detector_->IsRegistered("container-storage").value()) {
        if (!this->idle_detector_->UnregisterIdleConsumer("container-storage")) {
            WARNING("Failed to unregister container storage (idle tick consumer");
        }
    }
    if (this->state_ != STOPPED && this->timeout_committer_->IsStarted()) {
        CHECK(this->timeout_committer_->Join(NULL), "Timeout committer returned false");
    }
    if (timeout_committer_) {
        delete this->timeout_committer_;
    }
    chunk_index_ = NULL;
    return Storage::Close();
}

bool ContainerStorage::Stop(const dedupv1::StopContext& stop_context) {
    bool failed = false;

    if (state_ == STARTING || state_ == STARTED || state_ == RUNNING) {
        INFO("Stopping container storage");
    }

    // We stop the timeout committed to avoid a race condition when
    // the stop method and the timeout thread try to commit the same container at the
    // same time
    DEBUG("Stop timeout committer");
    timeout_committer_should_stop_ = true;
    if (this->timeout_committer_->IsJoinable()) {
        bool result = false;
        CHECK(this->timeout_committer_->Join(&result), "Timeout committer returned false");
        if (!result) {
            WARNING("Timeout committer stopped with an error");
            failed = true;
        }
    }

    DEBUG("Shutdown commit");

    for (vector<Container*>::iterator i = this->write_cache_.GetCache().begin(); i != this->write_cache_.GetCache().end(); i++) {
        Container* write_container = *i;
        if (write_container &&
            write_container->primary_id() != 0 &&
            write_container->primary_id() != Storage::ILLEGAL_STORAGE_ADDRESS &&
            write_container->item_count() > 0) {
            TRACE("Shutdown commit for " << write_container->primary_id());
            tbb::concurrent_hash_map<uint64_t, ContainerStorageAddressData>::accessor a;
            if (!address_map.find(a, write_container->primary_id())) {
                ERROR("Failed to get pre-assigned address for container: " << write_container->DebugString());
                failed = true;
            } else {

                if (!this->CommitContainer(write_container, a->second)) {
                    ERROR("Failed to commit container: " << write_container->DebugString());
                    failed = true;
                }
                address_map.erase(a);
            }
        }
    }
    if (this->gc_) {
        CHECK(this->gc_->Stop(stop_context), "Cannot stop gc");
    }
    if (this->allocator_) {
        CHECK(this->allocator_->Stop(stop_context), "Cannot stop allocator");
    }
    CHECK(this->background_committer_.Stop(stop_context),
        "Cannot stop background committer");

    if (state_ != CREATED) {
        this->state_ = STOPPED;
    }

    DEBUG("Stopped container storage");
    return !failed;
}

bool ContainerStorage::DumpMetaInfo() {
    DCHECK(this->info_store_, "Info store not set");
    DEBUG("Saving container storage state");

    ContainerLogfileData data;
    data.set_last_given_container_id(this->last_given_container_id_);
    data.set_container_size(this->container_size_);
    data.set_size(this->size_);
    for (int i = 0; i < file_.size(); i++) {
        ContainerFileData* file_data = data.add_file();
        file_data->set_filename(file_[i].filename());
        file_data->set_file_size(file_[i].file_size());
        file_data->set_uuid(file_[i].uuid().ToString());
    }
    data.set_contains_superblock(has_superblock_);

    CHECK(info_store_->PersistInfo("container-storage", data), "Failed to persist container storage info: " << data.ShortDebugString());
    DEBUG("Saved container storage state: " << data.ShortDebugString());
    return true;

}

bool ContainerStorage::CheckIfFull() {
    if (!this->allocator_) {
        return false;
    }
    return this->allocator_->CheckIfFull();
}

uint32_t ContainerStorage::GetMaxItemsPerContainer() const {
    return Container::kMetaDataSize / Container::kMaxSerializedItemMetadataSize;
}

lookup_result ContainerStorage::ReadMetaInfo(ContainerLogfileData* log_data) {
    DCHECK_RETURN(this->info_store_, LOOKUP_ERROR, "Info store not set");
    DCHECK_RETURN(log_data, LOOKUP_ERROR, "Log data not set");

    DEBUG("Reading from info store");

    lookup_result lr = info_store_->RestoreInfo("container-storage", log_data);
    CHECK_RETURN(lr != LOOKUP_ERROR, LOOKUP_ERROR, "Failed to read container storage info");
    if (lr == LOOKUP_NOT_FOUND) {
        return lr;
    }

    return LOOKUP_FOUND;
}

StorageSession* ContainerStorage::CreateSession() {
    return new ContainerStorageSession(this);
}

bool ContainerStorage::Flush(dedupv1::base::ErrorContext* ec) {
    if (!(state_ == RUNNING || state_ == STARTED)) {
        return true;
    }

    DEBUG("Flush container storage");

    bool failed = false;
    for (uint32_t i = 0; i < this->write_cache_.GetSize(); i++) {
        ReadWriteLock* write_cache_lock = NULL;
        Container* check_write_container = NULL;
        CHECK(this->write_cache_.GetWriteCacheContainerByIndex(i, &check_write_container, &write_cache_lock),
            "Failed to get write cache container: state " << state_);
        CHECK(write_cache_lock, "Write cache lock not set");
        CHECK(check_write_container, "Write container not set");

        ScopedReadWriteLock scoped_write_container_lock(write_cache_lock);
        CHECK(scoped_write_container_lock.AcquireWriteLock(), "Failed to acquire write cache lock");

        if (check_write_container->primary_id() && check_write_container->primary_id() != Storage::ILLEGAL_STORAGE_ADDRESS) {
            ProfileTimer commit_timer(this->stats_.pre_commit_time_);

            tbb::concurrent_hash_map<uint64_t, ContainerStorageAddressData>::accessor a;
            if (!address_map.find(a, check_write_container->primary_id())) {
                ERROR("Failed to get pre-assigned address for container: " << check_write_container->DebugString());
                failed = true;
            } else {

                if (!this->CommitContainer(check_write_container, a->second)) {
                    ERROR("Failed to commit container: " << check_write_container->DebugString());
                    failed = true;
                }

                address_map.erase(a);
            }
            // check write container has no a ILLEGAL_STORAGE_ADDRESS
            // The container gets a new id when a chunk is added
        }
        CHECK(scoped_write_container_lock.ReleaseLock(), "Cannot unlock write container");
    }

    if (!this->background_committer_.WaitUntilProcessedContainerFinished()) {
        ERROR("Failed to wait for background committer");
        failed = true;
    }

    TRACE("Wait until replay queue is empty");
    if (this->log_) {
        CHECK(this->log_->WaitUntilDirectReplayQueueEmpty(15),
            "Failed to wait until direct replay queue was processed");
    }

    return !failed;
}

alloc_result ContainerStorage::GetNewContainerId(Container* container) {
    CHECK_RETURN(container, ALLOC_ERROR, "Container not set");

    ContainerStorageAddressData address;
    alloc_result ar = this->allocator_->OnNewContainer(*container, true, &address);
    CHECK_RETURN(ar != ALLOC_ERROR, ALLOC_ERROR, "Failed to get address");
    if (ar == ALLOC_FULL) {
        return ALLOC_FULL;
    }

    uint64_t last_given_container_id = this->last_given_container_id_.fetch_and_increment() + 1;
    container->Reuse(last_given_container_id);

    tbb::concurrent_hash_map<uint64_t, ContainerStorageAddressData>::accessor a;
    address_map.insert(a, container->primary_id());
    a->second = address;
    a.release();

    ContainerOpenedEventData event_data;
    event_data.set_container_id(last_given_container_id);
    event_data.mutable_address()->CopyFrom(address);

    CHECK_RETURN(this->log_->CommitEvent(dedupv1::log::EVENT_TYPE_CONTAINER_OPEN, &event_data, NULL, NULL, NO_EC),
        ALLOC_ERROR, "Failed to commit container event: " << event_data.ShortDebugString());

    DEBUG("Open container: container id " << last_given_container_id << ", address " << DebugString(event_data.address()));
    return ALLOC_OK;
}

bool ContainerStorage::PrepareCommit(Container* container) {
    DCHECK(container, "Container not set");
    DCHECK(Storage::IsValidAddress(container->primary_id(), false),
        "Container has no valid address: " << container->DebugString());

    TRACE("Prepare commit: " << container->DebugString());

    Walltimer commit_timer;

    tbb::concurrent_hash_map<uint64_t, ContainerStorageAddressData>::accessor a;
    if (!address_map.find(a, container->primary_id())) {
        ERROR("Failed to get pre-assigned address for container: " << container->DebugString());
        return false;
    } else {

        TRACE("Hand over container: container " << container->DebugString() <<
            ", address " << DebugString(a->second));

        ContainerStorageAddressData address_data = a->second;

        // This call might block, if old container is not yet written to disk
        dedupv1::base::timed_bool r = background_committer_.Handover(container, address_data);
        CHECK(r != TIMED_FALSE, "Failed to handover container: " << container->DebugString());
        CHECK(r != TIMED_TIMEOUT, "Timeout while handing over container: " << container->DebugString());

        address_map.erase(a);
        a.release();

        // We never have a container lock on a container that is not committed
        CacheEntry cache_entry;
        lookup_result lr = cache_.GetCache(container->primary_id(), &cache_entry);
        CHECK(lr != LOOKUP_ERROR, "Failed to check for cache line: container " << container->DebugString());
        if (lr == LOOKUP_NOT_FOUND && cache_entry.is_set()) {
            CHECK(this->cache_.CopyToReadCache(*container, &cache_entry),
                "Failed to copy container to read cache: " << container->DebugString());
        } else {
            // LOOKUP_FOUND or cache entry not set
            // This is a strange situation and I don't know how this can happen
            // but is is not dangerous. The new entry is only not in the read cache this time
        }
    }

    this->stats_.pre_commit_time_.Add(&commit_timer);
    // check write container has no a ILLEGAL_STORAGE_ADDRESS
    // The container gets a new id when a chunk is added
    container->Reuse(Storage::ILLEGAL_STORAGE_ADDRESS);
    return true;
}

bool ContainerStorage::FailWriteCacheContainer(uint64_t address) {
    Container* write_cache_container = NULL;
    ReadWriteLock* write_cache_lock = NULL;

    TRACE("Fail write cache container: container id " << address);

    tbb::concurrent_hash_map<uint64_t, ContainerStorageAddressData>::accessor a;
    if (!address_map.find(a, address)) {
        WARNING("Failed to get pre-assigned address for container id: container id" << address);
    } else {
        address_map.erase(a);
        a.release();
    }

    CHECK(write_cache_.GetWriteCacheContainer(address, &write_cache_container, &write_cache_lock, true) == LOOKUP_FOUND,
        "Failed to get write cache container: container id " << address);
    ScopedReadWriteLock scoped_write_container_lock(NULL);
    scoped_write_container_lock.SetLocked(write_cache_lock);
    CHECK(write_cache_container, "Write cache container not set");
    CHECK(MarkContainerCommitAsFailed(write_cache_container), "Failed to mark container as failed: " << write_cache_container);
    write_cache_container->Reuse(Storage::ILLEGAL_STORAGE_ADDRESS);

    CHECK(scoped_write_container_lock.ReleaseLock(),
        "Failed to release write container lock: " << scoped_write_container_lock.DebugString());
    return true;
}

ContainerStorageSession::ContainerStorageSession(ContainerStorage* storage) {
    this->storage_ = storage;
}

bool ContainerStorageSession::WriteNew(const void* key, size_t key_size,
                                       const void* data, size_t data_size,
                                       bool is_indexed,
                                       uint64_t* address,
                                       ErrorContext* ec) {
    ContainerStorage* c = this->storage_;
    ProfileTimer timer(storage_->stats_.total_write_time_);

    CHECK(c->state_ == ContainerStorage::RUNNING, "Illegal state to write new data: " << c->state_);
    CHECK(!this->storage_->start_context_.readonly(), "Container storage is in readonly mode");

    // Select write container
    Container* write_container = NULL;
    ReadWriteLock* write_container_lock = NULL;
    // TODO(fermat): We could send the size of the chunk here to get an open container, which can store the chunk, if there is one left.
    // This way we get a defragmentation.
    CHECK(this->storage_->write_cache_.GetNextWriteCacheContainer(&write_container, &write_container_lock),
        "Failed to get write container: key " << Fingerprinter::DebugString((const byte *) key, key_size));
    CHECK(write_container, "Write container not set");
    CHECK(write_container_lock, "Write container lock not set");
    ScopedReadWriteLock scoped_write_container_lock(NULL);
    scoped_write_container_lock.SetLocked(write_container_lock);

    if (!Storage::IsValidAddress(write_container->primary_id())) { // Write into new container
        alloc_result ar = c->GetNewContainerId(write_container);
        CHECK(ar != ALLOC_ERROR, "Init container id failed");
        if (ar == ALLOC_FULL) {
            WARNING("Container storage full");
            if (ec) {
                ec->set_full();
            }
            return false;
        }
    }

    CHECK(scoped_write_container_lock.IsHeldForWrites(),
        "Thread doesn't holds write container: lock: " <<
        ", lock " << scoped_write_container_lock.DebugString() <<
        ", key " << Fingerprinter::DebugString((const byte *) key, key_size));

    if (write_container->IsFull(key_size, data_size)) {
        TRACE("Container full: " << write_container->DebugString());

        // we do not need to reset the container cache timeout, because it has been reseted a few moments ago (GetNextWriteCacheContainer)
        CHECK(c->PrepareCommit(write_container),
            "Failed to prepare commit: " << write_container->DebugString());

        // PrepareCommit has build a new container in cache, which is now an empty container
        if (!Storage::IsValidAddress(write_container->primary_id())) { // Write into new container
            alloc_result ar = c->GetNewContainerId(write_container);
            CHECK(ar != ALLOC_ERROR, "Init container id failed");
            if (ar == ALLOC_FULL) {
                WARNING("Container storage full");
                if (ec) {
                    ec->set_full();
                }
                return false;
            }
        }
    }
    CHECK(scoped_write_container_lock.IsHeldForWrites(), "Thread doesn't holds write container lock");
    uint64_t container_id = write_container->primary_id();
    CHECK(container_id != 0 && container_id != Storage::EMPTY_DATA_STORAGE_ADDRESS && container_id != Storage::ILLEGAL_STORAGE_ADDRESS,
        "Illegal container id: " << write_container->primary_id());
    *address = container_id;

    TRACE("Write new key " << Fingerprinter::DebugString((byte *) key, key_size) << " to container " << container_id <<
        ", data size " << data_size <<
        ", active container data size " << write_container->active_data_size());

    CHECK(!write_container->IsFull(key_size, data_size), "Free Space assertion failed: fp " << Fingerprinter::DebugString((const byte *) key, key_size) << ", data size " << data_size << ", write container " << write_container->DebugString());

    // Scope for add timer
    {
        ProfileTimer add_timer(this->storage_->stats_.add_time_);
        CHECK(write_container->AddItem((byte *) key, key_size, (byte *) data, data_size,
                is_indexed,
                c->compression_),
            "Cannot add item: fp " << Fingerprinter::DebugString((const byte *) key, key_size) << ", data size " << data_size << ", write container " << write_container->DebugString());
    }
    DCHECK(write_container->primary_id() == container_id, "Container id changed illegally");
    CHECK(scoped_write_container_lock.ReleaseLock(), "Failed to release write container lock: " << scoped_write_container_lock.DebugString());
    return true;
}

bool ContainerStorage::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ContainerStorageStatsData data;
    data.set_read_count(this->stats_.reads_);
    data.set_write_cache_hit_count(this->stats_.write_cache_hit_);
    data.set_readed_container_count(this->stats_.readed_container_);
    data.set_container_timeout_count(this->stats_.container_timeouts_);
    data.set_committed_container_count(this->stats_.committed_container_);
    data.set_moved_container_count(this->stats_.moved_container_);
    data.set_merged_container_count(this->stats_.merged_container_);
    data.set_failed_container_count(this->stats_.failed_container_);
    data.set_deleted_container_count(this->stats_.deleted_container_);
    CHECK(ps->Persist(prefix, data), "Failed to persist container storage stats");

    if (gc_) {
        CHECK(gc_->PersistStatistics(prefix + ".gc", ps), "Failed to persist gc");
    }
    CHECK(cache_.PersistStatistics(prefix + ".cache", ps), "Failed to persist cache");
    CHECK(write_cache_.PersistStatistics(prefix + ".write_cache", ps), "Failed to persist write cache");
    if (allocator_) {
        CHECK(allocator_->PersistStatistics(prefix + ".alloc", ps), "Failed to persist allocator");
    }
    return true;
}

bool ContainerStorage::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ContainerStorageStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore container storage cache stats");
    this->stats_.reads_ = data.read_count();
    this->stats_.write_cache_hit_ = data.write_cache_hit_count();
    this->stats_.container_timeouts_ = data.container_timeout_count();
    this->stats_.readed_container_ = data.readed_container_count();
    this->stats_.committed_container_ = data.committed_container_count();
    this->stats_.moved_container_ = data.moved_container_count();
    this->stats_.merged_container_ = data.merged_container_count();
    this->stats_.failed_container_ = data.failed_container_count();
    this->stats_.deleted_container_ = data.deleted_container_count();

    if (gc_) {
        CHECK(gc_->RestoreStatistics(prefix + ".gc", ps), "Failed to restore gc");
    }
    CHECK(cache_.RestoreStatistics(prefix + ".cache", ps), "Failed to restore cache");
    CHECK(write_cache_.RestoreStatistics(prefix + ".write_cache", ps), "Failed to restore write cache");
    if (allocator_) {
        CHECK(allocator_->RestoreStatistics(prefix + ".alloc", ps), "Failed to restore allocator");
    }
    return true;
}

uint64_t ContainerStorage::GetActiveStorageDataSize() {
    if (allocator_) {
        return allocator_->GetActiveStorageDataSize();
    }
    return 0;
}

string ContainerStorage::PrintStatistics() {
    uint64_t allocated_storage_size = 0;
    for (unsigned int i = 0; i < this->file_.size(); i++) {
        if (this->file_[i].file()) {
            Option<off_t> s = this->file_[i].file()->GetSize();
            if (s.valid()) {
                allocated_storage_size += s.value();
            }
        }
    }

    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{";
    sstr << "\"gc\": " << (this->gc_ ? this->gc_->PrintStatistics() : "null") << "," << std::endl;
    sstr << "\"allocator\": " << (this->allocator_ ? this->allocator_->PrintStatistics() : "null") << "," << std::endl;
    sstr << "\"write cache\": " << this->write_cache_.PrintStatistics() << "," << std::endl;
    sstr << "\"read cache\": " << this->cache_.PrintStatistics() << "," << std::endl;
    sstr << "\"data size\": " << allocated_storage_size << "," << std::endl;
    sstr << "\"allocated storage size\": " << allocated_storage_size << "," << std::endl;
    sstr << "\"active storage size\": " << GetActiveStorageDataSize() << "," << std::endl;

    sstr << "\"reads\": " << this->stats_.reads_ << "," << std::endl;
    sstr << "\"write cache hits\": " << this->stats_.write_cache_hit_ << "," << std::endl;
    sstr << "\"committed container\": " << this->stats_.committed_container_ << "," << std::endl;
    sstr << "\"container timeouts\": " << this->stats_.container_timeouts_ << "," << std::endl;
    sstr << "\"readed container\": " << this->stats_.readed_container_ << "," << std::endl;

    sstr << "\"moved container\": " << this->stats_.moved_container_ << "," << std::endl;
    sstr << "\"merged container\": " << this->stats_.merged_container_ << "," << std::endl;
    sstr << "\"failed container\": " << this->stats_.failed_container_ << "," << std::endl;
    sstr << "\"deleted container\": " << this->stats_.deleted_container_ << "" << std::endl;
    sstr << "}";
    return sstr.str();
}

string ContainerStorage::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << this->background_committer_.PrintEmbeddedTrace() << "," << std::endl;
    sstr << "\"gc\": " << (this->gc_ ? this->gc_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"allocator\": " << (this->allocator_ ? this->allocator_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"read cache\": " << this->cache_.PrintTrace() << "," << std::endl;
    sstr << "\"write cache\": " << this->write_cache_.PrintTrace() << "," << std::endl;
    sstr << "\"meta data items\": " << (this->meta_data_index_ ? ToString(this->meta_data_index_->GetItemCount()) : "null") << "," << std::endl;
    sstr << "\"meta data size\": " << (this->meta_data_index_ ? ToString(this->meta_data_index_->GetPersistentSize()) : "null") << "," << std::endl;
    sstr << "\"last given container id\": " << this->GetLastGivenContainerId() << "" << std::endl;
    sstr << "}";
    return sstr.str();
}

string ContainerStorage::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"gc\": " << (this->gc_ ? this->gc_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"allocator\": " << (this->allocator_ ? this->allocator_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"read cache\": " << this->cache_.PrintLockStatistics() << "," << std::endl;
    sstr << "\"write cache\": " << this->write_cache_.PrintLockStatistics() << "," << std::endl;
    sstr << "\"meta data\": " << (this->meta_data_index_ ? this->meta_data_index_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"global lock free\": " << this->stats_.global_lock_free_ << "," << std::endl;
    sstr << "\"global lock busy\": " << this->stats_.global_lock_busy_ << "," << std::endl;
    sstr << "\"file lock free\": " << this->stats_.file_lock_free_ << "," << std::endl;
    sstr << "\"file lock busy\": " << this->stats_.file_lock_busy_ << "," << std::endl;
    sstr << "\"handover lock free\": " << this->stats_.handover_lock_free_ << "," << std::endl;
    sstr << "\"handover lock busy\": " << this->stats_.handover_lock_busy_ << "," << std::endl;
    sstr << "\"container lock free\": " << this->stats_.container_lock_free_ << "," << std::endl;
    sstr << "\"container lock busy\": " << this->stats_.container_lock_busy_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string ContainerStorage::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << this->background_committer_.PrintEmbeddedProfile() << "," << std::endl;
    sstr << "\"gc\": " << (this->gc_ ? this->gc_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"allocator\": " << (this->allocator_ ? this->allocator_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"meta data\": " << (this->meta_data_index_ ? this->meta_data_index_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"read container time\": " << this->stats_.total_read_container_time_.GetSum() << "," << std::endl;
    sstr << "\"read cache\": " << this->cache_.PrintProfile() << "," << std::endl;
    sstr << "\"write cache\": " << this->write_cache_.PrintProfile() << "," << std::endl;
    sstr << "\"read time\": " << this->stats_.total_read_time_.GetSum() << "," << std::endl;
    sstr << "\"write time\": " << this->stats_.total_write_time_.GetSum() << "," << std::endl;
    sstr << "\"add time\": " << this->stats_.add_time_.GetSum() << "," << std::endl;
    sstr << "\"pre commit time\": " << this->stats_.pre_commit_time_.GetSum() << "," << std::endl;
    sstr << "\"container write time\": " << this->stats_.container_write_time_.GetSum() << "," << std::endl;
    sstr << "\"commit state check time\": " << this->stats_.is_committed_time_.GetSum() << "," << std::endl;
    sstr << "\"file lock time\": " << this->stats_.total_file_lock_time_.GetSum() << "," << std::endl;
    sstr << "\"file load time\": " << this->stats_.total_file_load_time_.GetSum() << "," << std::endl;
    sstr << "\"replay time\": " << this->stats_.replay_time_.GetSum() << "," << std::endl;
    sstr << "\"average container load latency\": " << this->stats_.total_file_load_time_.GetAverage() << "," << std::endl;
    sstr << "\"delete time\": " << this->stats_.total_delete_time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

bool ContainerStorageSession::DoDelete(
    uint64_t container_id,
    uint64_t primary_id,
    const ContainerStorageAddressData& address,
    const list<bytestring>& key_list,
    CacheEntry* cache_entry,
    ErrorContext* ec) {
    ContainerStorage* c = this->storage_;
    DCHECK(c, "Container storage not set");
    DCHECK(cache_entry, "Cache entry not set");

    // we can now re-acquire the container lock.
    // we know for sure that the primary id is the same as no one else was allowed to do changes on the container

    ReadWriteLock* container_lock = NULL;
    std::pair<dedupv1::base::lookup_result, ContainerStorageAddressData> address2;
    address2 = this->storage_->LookupContainerAddressWait(container_id,
        &container_lock,
        true);
    if (address2.first == LOOKUP_ERROR) {
        ERROR("Failed to get address of container " << container_id);
        if (cache_entry->is_set()) {
            CHECK(cache_entry->lock()->ReleaseLock(), "Failed to release cache lock");
        }
        return false;
    }
    if (address2.first == LOOKUP_NOT_FOUND) {
        ERROR("Failed to get address of container " << container_id <<
            ", reason: container not found");
        if (cache_entry->is_set()) {
            CHECK(cache_entry->lock()->ReleaseLock(), "Failed to release cache lock");
        }
        return false;
    }

    ScopedReadWriteLock scoped_container_lock(NULL);
    scoped_container_lock.SetLocked(container_lock);

    if (address2.second.has_primary_id() && address2.second.primary_id() != primary_id) {
        ERROR("Illegal primary id: container id " << container_id <<
            ", should be primary id " << primary_id <<
            ", address " << address2.second.DebugString());
        if (cache_entry->is_set()) {
            CHECK(cache_entry->lock()->ReleaseLock(), "Failed to release cache lock");
        }
        return false;
    }

    Container container;
    if (!container.Init(container_id, this->storage_->GetContainerSize())) {
        ERROR("Failed to init container: container id " << container_id);

        if (cache_entry->is_set()) {
            CHECK(cache_entry->lock()->ReleaseLock(), "Failed to release cache lock");
        }

        return false;
    }

    // we have to force a read from disk to ensure that we use the latest version
    // if would not remove the container from the cache and use a cache-aware container read method (ReadContainerWithCache)
    // the container would be stored on disk without the deleted item, but the container with the item would remain in the
    // read cache. This would lead to race conditions

    if (cache_entry->is_set()) {
        CHECK(c->cache_.RemoveFromReadCache(primary_id, cache_entry),
            "Failed to remove container from read cache: container " << primary_id);
    }
    // if we held the cache entry lock before, we released it now

    // we have the container lock already
    lookup_result read_result = this->storage_->ReadContainerLocked(&container, address);
    CHECK(read_result != LOOKUP_ERROR, "Read of container failed: id " << container_id << ", primary id " << primary_id << ", reason: index lookup error");
    CHECK(read_result != LOOKUP_NOT_FOUND, "Read of container failed: id " << container_id << ", primary id " << primary_id << ", reason: container not found");

    // found
    CHECK(container.HasId(container_id), "Wrong active container: " << container.DebugString() << ", address " << container_id);

    uint32_t old_active_data_size = container.active_data_size();
    uint32_t old_item_count = container.item_count();

    for (list<bytestring>::const_iterator i = key_list.begin(); i != key_list.end(); i++) {
        const byte* key = reinterpret_cast<const byte*>(i->data());
        size_t key_size = i->size();
        TRACE("Delete item " << Fingerprinter::DebugString(key, key_size) << " from container " << container.DebugString() << " (read cache/disk)");
        CHECK(container.DeleteItem(key, key_size),
            "Failed to delete container item: " << Fingerprinter::DebugString(key, key_size) << " from container " << container_id);
    }
    ContainerStorageAddressData new_container_address;
    ContainerStorageAddressData old_container_address = address;

    // TODO(fermat): Why not isCommittedWait?
    if (this->storage_->IsCommitted(container.primary_id()) != STORAGE_ADDRESS_COMMITED) {
        TRACE("Container " << container.primary_id() << " is not-open, but uncommitted. Check for bg committer");
        // not open and not committed, means either an serious error or a bg committer
        Option<bool> b = this->storage_->background_committer_.IsCurrentlyProcessedContainerId(container.primary_id());
        DCHECK(b.valid(), "Failed to check is container id is currently processed");

        if (b.value()) {
            // we have luck
            enum storage_commit_state scs = this->storage_->IsCommitted(container.primary_id());
            size_t i = 0;
            while (scs == STORAGE_ADDRESS_NOT_COMMITED && i < (ContainerStorageBackgroundCommitter::kMaxThreads + 1)) {
                CHECK(this->storage_->background_committer_.CommitFinishedConditionWaitTimeout(10) == TIMED_TRUE,
                    "Failed to wait for container commit");
                scs = this->storage_->IsCommitted(container.primary_id());
                i++;
            }
            CHECK(scs == STORAGE_ADDRESS_COMMITED, "Container " << container.primary_id() << " still uncommitted");

        } else {
            ERROR("Cannot find non-open, but uncommitted container " << container.primary_id());
            return false;
        }
    }

    CHECK(this->storage_->allocator_->OnNewContainer(container, false, &new_container_address), "Failed to get new container address");
    CHECK(this->storage_->WriteContainer(&container, new_container_address),
        "Failed to write container " << container.DebugString());

    ContainerMoveEventData event_data;
    event_data.set_container_id(container.primary_id());
    event_data.mutable_new_address()->CopyFrom(new_container_address);
    event_data.mutable_old_address()->CopyFrom(old_container_address);
    event_data.set_active_data_size(container.active_data_size());
    event_data.set_old_active_data_size(old_active_data_size);
    event_data.set_item_count(container.item_count());
    event_data.set_old_item_count(old_item_count);

    INFO("Moved container " << container.DebugString() <<
        ", old address " << ContainerStorage::DebugString(old_container_address) <<
        ", new address " << ContainerStorage::DebugString(new_container_address));

    CHECK(scoped_container_lock.ReleaseLock(), "Failed to release container lock");

    // Commit data only when on critical lock are hold, due to DIRECT processing
    int64_t event_log_id = 0;
    CHECK(c->log_->CommitEvent(EVENT_TYPE_CONTAINER_MOVED, &event_data, &event_log_id, c, NO_EC),
        "Failed to commit container move");

    c->stats_.moved_container_++;
    return true;
}

bool ContainerStorageSession::Delete(uint64_t container_id,
                                     const list<bytestring>& key_list,
                                     ErrorContext* ec) {
    ProfileTimer timer(this->storage_->stats_.total_delete_time_);
    ContainerStorage* c = this->storage_;
    CHECK(c, "Container storage not set");

    Container* write_container = NULL;
    ReadWriteLock* write_container_lock = NULL;

    CHECK(c->state_ == ContainerStorage::RUNNING || c->state_ == ContainerStorage::STARTED,
        "Illegal state to delete data: " << c->state_);

    TRACE("Delete storage entry: address " << container_id);

    enum lookup_result r = c->write_cache_.GetWriteCacheContainer(container_id, &write_container, &write_container_lock, true);
    CHECK(r != LOOKUP_ERROR, "Failed to access write cache");
    if (r == LOOKUP_FOUND) {
        ScopedReadWriteLock scoped_write_cache_lock(NULL);
        scoped_write_cache_lock.SetLocked(write_container_lock);
        CHECK(write_container, "Write container not set");

        for (list<bytestring>::const_iterator i = key_list.begin(); i != key_list.end(); i++) {
            const byte* key = reinterpret_cast<const byte*>(i->data());
            size_t key_size = i->size();
            TRACE("Delete item " << Fingerprinter::DebugString(key, key_size) << " from container " << container_id << " (write cache)");
            CHECK(write_container->DeleteItem(key, i->size()),
                "Failed to delete container item: " << Fingerprinter::DebugString(key, key_size) << " from container " << container_id);
        }
        CHECK(scoped_write_cache_lock.ReleaseLock(), "Failed to release write container lock");
        return true;
    }

    // Not found in write cache => Try read cache

    // Here we get the container lock, but only later we get the cache lock
    // get primary id (we need the primary id for the locking)
    ReadWriteLock* container_lock = NULL;
    uint64_t primary_id = container_id;
    std::pair<dedupv1::base::lookup_result, ContainerStorageAddressData> address;
    bool has_access = false;
    while (!has_access) {

        address = this->storage_->LookupContainerAddressWait(container_id,
            &container_lock,
            true);
        CHECK(address.first != LOOKUP_ERROR, "Failed to get address of container " << container_id);
        CHECK(address.first != LOOKUP_NOT_FOUND, "Failed to get address of container " << container_id <<
            ", reason: container not found");

        if (address.second.has_primary_id()) {
            primary_id = address.second.primary_id();
        }

        tbb::spin_mutex::scoped_lock scoped_in_move_set_lock(this->storage_->in_move_set_lock_);
        if (storage_->in_move_set_.find(primary_id) == storage_->in_move_set_.end()) {
            // not in set
            TRACE("Adds container to in move set: container id " << primary_id);
            container_lock->ReleaseLock();
            container_lock = NULL;

            storage_->in_move_set_.insert(primary_id);
            scoped_in_move_set_lock.release();
            has_access = true;
        } else {
            // has no access
            container_lock->ReleaseLock();
            container_lock = NULL;
            scoped_in_move_set_lock.release();
            ThreadUtil::Sleep(50, ThreadUtil::MILLISECONDS);

            // repeat
        }
    }
    // Here we have access aka the container is in the in_move_set_
    // No one else does modifications on it. However, we hold no container lock.

    const Container* cached_container = NULL;
    CacheEntry cache_entry;
    lookup_result cache_lookup_result = storage_->GetReadCache()->CheckCache(primary_id, &cached_container, true,
        true, &cache_entry);

    bool delete_result = true;
    if (cache_lookup_result == LOOKUP_ERROR) {
        ERROR("Failed to cache read cache for container: " << primary_id);
        delete_result = false;
    } else {
        // now we hold a cache lock
        delete_result = DoDelete(container_id, primary_id, address.second, key_list, &cache_entry, ec);
        // container lock is released
    }

    // remove container id from in move set
    tbb::spin_mutex::scoped_lock scoped_in_move_set_lock(this->storage_->in_move_set_lock_);
    storage_->in_move_set_.erase(primary_id);
    scoped_in_move_set_lock.release();

    return delete_result;
}

enum lookup_result ContainerStorageSession::ReadInContainer(const Container& container, const void* key,
                                                            size_t key_size, void* data, size_t* data_size) {
    this->storage_->stats_.reads_.fetch_and_increment();

    const ContainerItem* item = container.FindItem(key, key_size);
    if (item == NULL) {
        string items_debug_string = "";
        vector<ContainerItem*>::const_iterator i;
        for (i = container.items().begin(); i != container.items().end(); i++) {
            if (i != container.items().begin()) {
                items_debug_string += ", ";
            }
            const ContainerItem* item = *i;
            if (item) {
                items_debug_string += item->DebugString() + "\n";
            } else {
                items_debug_string += "<NULL>\n";
            }
        }
        WARNING("Key not found in container: " <<
            "container " << container.DebugString() <<
            ", container items " << items_debug_string <<
            ", key " << Fingerprinter::DebugString(static_cast<const byte*>(key), key_size));
        return LOOKUP_NOT_FOUND;
    }
    CHECK_RETURN(!item->is_deleted(), LOOKUP_ERROR, "Found a deleted item: " << item->DebugString());
    CHECK_RETURN(item->raw_size() <= *data_size, LOOKUP_ERROR, "Data length error: " <<
        "container " << container.DebugString() <<
        ", item " << item->DebugString() <<
        ", data size " << (*data_size));
    if (data) {
        CHECK_RETURN(container.CopyRawData(item, data, *data_size), LOOKUP_ERROR,
            "Cannot copy data: " <<
            "container " << container.DebugString() <<
            ", item " << item->DebugString() <<
            ", fp " << Fingerprinter::DebugString((const byte *) key, key_size));
    }
    *data_size = item->raw_size();

    if (this->storage_->gc_) {
        if (!this->storage_->gc_->OnRead(container, key, key_size)) {
            WARNING("Error while updating storage gc");
        }
    }
    if (this->storage_->allocator_) {
        if (!this->storage_->allocator_->OnRead(container, key, key_size)) {
            WARNING("Error while updating storage allocator");
        }
    }
    return LOOKUP_FOUND;
}

bool ContainerStorageSession::Read(uint64_t address, const void* key,
                                   size_t key_size, void* data, size_t* data_size,
                                   ErrorContext* ec) {
    ProfileTimer timer(this->storage_->stats_.total_read_time_);

    CHECK(this->storage_->state_ == ContainerStorage::RUNNING ||
        this->storage_->state_ == ContainerStorage::STARTED, "Illegal state to read data: " << this->storage_->state_);

    // Handle default first
    if (StorageSession::Read(address, key, key_size, data, data_size, ec)) {
        return true;
    }

    CHECK(key, "Key NULL");
    CHECK(data_size, "Data Size Pointer NULL");
    CHECK(this->storage_, "Container storage not set");

    Container* write_container = NULL;
    ReadWriteLock* write_cache_lock = NULL;
    lookup_result r = this->storage_->write_cache_.GetWriteCacheContainer(address, &write_container, &write_cache_lock, false);
    CHECK(r != LOOKUP_ERROR, "Failed to access write cache");
    if (r == LOOKUP_FOUND) {
        CHECK(write_container, "Write container not set");
        CHECK(write_cache_lock, "Write cache log not set");
        ScopedReadWriteLock scoped_write_cache_lock(NULL);
        scoped_write_cache_lock.SetLocked(write_cache_lock);

        this->storage_->stats_.write_cache_hit_++;
        TRACE("Read item " << Fingerprinter::DebugString((byte *) key, key_size) << " from container " << address << " (write cache)");

        enum lookup_result r = this->ReadInContainer(*write_container, key, key_size, data, data_size);
        CHECK(r != LOOKUP_ERROR, "Failed to read in container: " << write_container->DebugString());
        scoped_write_cache_lock.ReleaseLock();
        return r == LOOKUP_FOUND;
    }

    // Not found in write cache => Try read cache
    enum lookup_result read_result;

    const Container* cache_container = NULL;
    CacheEntry cache_entry;
    read_result = this->storage_->cache_.CheckCache(address, &cache_container, false /* we want update the cache*/,
        true, &cache_entry);
    CHECK(read_result != LOOKUP_ERROR, "Read of container " << address << " failed: Cache check failed");
    if (read_result == LOOKUP_FOUND) {
        CHECK(cache_container, "Container not set");
        CHECK(cache_entry.lock(), "Cache lock not set");
        ScopedReadWriteLock scoped_cache_lock(NULL);
        scoped_cache_lock.SetLocked(cache_entry.lock());

        CHECK(cache_container->HasId(address), "Wrong active container: " << cache_container->DebugString() << ", address " << address);

        TRACE("Read item " << Fingerprinter::DebugString((byte *) key, key_size) << " from container " << cache_container->DebugString() << " (read cache)");

        // We hold the cache entry lock, we can now also allocate the container lock
        enum lookup_result r = this->ReadInContainer(*cache_container, key, key_size, data, data_size);
        CHECK(r != LOOKUP_ERROR, "Failed to read in container: "
            << "container " << cache_container->DebugString()
            << ", key " << Fingerprinter::DebugString(key, key_size));

        CHECK(scoped_cache_lock.ReleaseLock(), "Failed to release cache lock");
        return r == LOOKUP_FOUND;
    }
    // read_resulkt == LOOKUP_NOT_FOUND => not found in read cache
    // cache lock might/should be set
    TRACE("Read item " << Fingerprinter::DebugString((byte *) key, key_size) << " from container " << address << " (disk)");

    Container read_container;
    if (!read_container.Init(address, this->storage_->container_size_)) {
        ERROR("Failed to init container: container id " << address);
        if (!storage_->cache_.ReleaseCacheline(address, &cache_entry)) {
            WARNING("Failed to release cache line: container id " << address << ", cache line " << cache_entry.DebugString());
        }
        return false;
    }

    // I have no container lock here, we will acquire and release it during ReadContainer
    read_result = this->storage_->ReadContainer(&read_container); // without cache
    if (read_result == LOOKUP_ERROR) {
        ERROR("Read of container " << address << " failed: Lookup error");
        if (cache_entry.is_set()) {
            if (!storage_->cache_.ReleaseCacheline(address, &cache_entry)) {
                WARNING("Failed to release cache line: container id " << address << ", cache line " << cache_entry.DebugString());
            }
        }
        return false;
    }
    if (read_result == LOOKUP_NOT_FOUND) {
        ERROR("Read of container " << address << " failed: Container not found");
        if (cache_entry.is_set()) {
            if (!storage_->cache_.ReleaseCacheline(address, &cache_entry)) {
                WARNING("Failed to release cache line: container id " << address << ", cache line " << cache_entry.DebugString());
            }
        }
        return false;
    }

    if (cache_entry.is_set()) {
        if (!this->storage_->cache_.CopyToReadCache(read_container, &cache_entry)) {
            ERROR("Failed to add container to read cache: " << read_container.DebugString());

            // lock is freed
            return false;
        }
    }

    // found
    CHECK(read_container.HasId(address), "Wrong active container: " << read_container.DebugString() << ", address " << address);
    enum lookup_result r2 = this->ReadInContainer(read_container, key, key_size, data, data_size);
    CHECK(r2 != LOOKUP_ERROR, "Failed to read in container: "
        << "container " << read_container.DebugString()
        << ", key " << Fingerprinter::DebugString(key, key_size));
    return r2 == LOOKUP_FOUND;
}

storage_commit_state ContainerStorage::IsCommittedWait(uint64_t address) {
    ProfileTimer timer(stats_.is_committed_time_);

    TRACE("Check commit state (wait): " << address);
    if (address == Storage::EMPTY_DATA_STORAGE_ADDRESS) {
        // This should not happen, but would also not be a problem.
        return STORAGE_ADDRESS_COMMITED;
    }

    // the cache might be stale when the container is currently processed
    // therefore we ignore the cache state when the address was open before
    // TODO (dmeister): A better solution is the ensure a cache consistency or
    // to check if the meta data cache can be removed.
    bool ignore_cache = false;

    // wait if a) currently open
    {
        // We wait here, if the container is opened but not committed. While commiting it, it will be removed from address_map.
        tbb::concurrent_hash_map<uint64_t, ContainerStorageAddressData>::const_accessor a;
        while (address_map.find(a, address)) {
            ignore_cache = true;
            TRACE("Address still in address map: container id " << address <<
                ", address " << a->second.ShortDebugString());
            a.release();

            dedupv1::base::ThreadUtil::Sleep(1);
        }
    }

    // container not in the write cache, but in the background committer
    Option<bool> is_processed = this->background_committer_.IsCurrentlyProcessedContainerId(address);
    while (is_processed.valid() && is_processed.value()) {
        ignore_cache = true;
        TRACE("Wait until currently processed container is committed: container id " << address);
        dedupv1::base::timed_bool b = this->background_committer_.CommitFinishedConditionWaitTimeout(1);
        CHECK_RETURN(b != TIMED_FALSE, STORAGE_ADDRESS_ERROR, "Failed to wait for commit");
        if (b == TIMED_TIMEOUT) {
            TRACE("Wait for container commit timeout: container id " << address);
        }

        is_processed = this->background_committer_.IsCurrentlyProcessedContainerId(address);
    }

    enum storage_commit_state commit_state;
    bool found = false;
    if (!ignore_cache) {
        CHECK_RETURN(this->meta_data_cache_.Lookup(address, &found, &commit_state),
            STORAGE_ADDRESS_ERROR, "Failed to lookup the meta data cache");
        if (found) {
            TRACE("Checked commit state: container id " << address << ", result " << commit_state << ", source cache");
            return commit_state;
        }
    }
    // not found in cache

    lookup_result r = this->meta_data_index_->Lookup(&address, sizeof(uint64_t), NULL);
    CHECK_RETURN(r != LOOKUP_ERROR, STORAGE_ADDRESS_ERROR, "Meta data index lookup error for container id " << address);
    if (r == LOOKUP_NOT_FOUND) {
        if (address <= this->initial_given_container_id_) {
            commit_state = STORAGE_ADDRESS_WILL_NEVER_COMMITTED;
        } else {
            commit_state = STORAGE_ADDRESS_NOT_COMMITED;
        }
    } else {
        commit_state = STORAGE_ADDRESS_COMMITED;
    }

    CHECK_RETURN(this->meta_data_cache_.Update(address, commit_state),
        STORAGE_ADDRESS_ERROR, "Failed to update the meta data cache");
    if (commit_state == STORAGE_ADDRESS_COMMITED) {
        TRACE("Checked commit state: container id " << address << ", result " << commit_state << ", source index");
    }
    return commit_state;
}

storage_commit_state ContainerStorage::IsCommitted(uint64_t address) {
    ProfileTimer timer(stats_.is_committed_time_);

    if (address == Storage::EMPTY_DATA_STORAGE_ADDRESS) {
        return STORAGE_ADDRESS_COMMITED;
    }

    enum storage_commit_state commit_state;
    bool found = false;
    CHECK_RETURN(this->meta_data_cache_.Lookup(address, &found, &commit_state),
        STORAGE_ADDRESS_ERROR, "Failed to lookup the meta data cache");
    if (found) {
        TRACE("Checked commit state: container id " << address << ", result " << commit_state << ", source cache");
        if (commit_state != STORAGE_ADDRESS_COMMITED) {
            if (address <= this->initial_given_container_id_) {
                commit_state = STORAGE_ADDRESS_WILL_NEVER_COMMITTED;
            } else {
                commit_state = STORAGE_ADDRESS_NOT_COMMITED;
            }
        }
        return commit_state;
    } else {
        // not found in cache

        lookup_result r = this->meta_data_index_->Lookup(&address, sizeof(uint64_t), NULL);
        CHECK_RETURN(r != LOOKUP_ERROR, STORAGE_ADDRESS_ERROR, "Meta data index lookup error for container id " << address);
        if (r == LOOKUP_NOT_FOUND) {
            if (address <= this->initial_given_container_id_) {
                commit_state = STORAGE_ADDRESS_WILL_NEVER_COMMITTED;
            } else {
                commit_state = STORAGE_ADDRESS_NOT_COMMITED;
            }
        } else {
            commit_state = STORAGE_ADDRESS_COMMITED;
        }
    }
    CHECK_RETURN(this->meta_data_cache_.Update(address, commit_state),
        STORAGE_ADDRESS_ERROR, "Failed to update the meta data cache");
    if (commit_state == STORAGE_ADDRESS_COMMITED) {
        TRACE("Checked commit state: container id " << address << ", result " << commit_state << ", source index");
    }
    return commit_state;
}

bool ContainerStorage::LogReplay(dedupv1::log::event_type event_type,
                                 const LogEventData& event_value,
                                 const dedupv1::log::LogReplayContext& context) {
    ProfileTimer timer(this->stats_.replay_time_);
    uint64_t container_id = 0;
    if (event_type == dedupv1::log::EVENT_TYPE_REPLAY_STOPPED) {
        CHECK(DumpMetaInfo(), "Log Write Failed");
    } else if (event_type == log::EVENT_TYPE_CONTAINER_OPEN && context.replay_mode() == EVENT_REPLAY_MODE_REPLAY_BG) {
        ContainerOpenedEventData event_data = event_value.container_opened_event();
        container_id = event_data.container_id();

        DEBUG("Log Background Replay: " <<
            "Container open: container id " << container_id <<
            ", highest open container id " << highest_committed_container_id_);

        if (container_id >= highest_committed_container_id_) {
            CHECK(DumpMetaInfo(), "Log Write Failed");
        }
    } else if (event_type == EVENT_TYPE_CONTAINER_OPEN && context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
        ContainerOpenedEventData event_data = event_value.container_opened_event();
        container_id = event_data.container_id();

        // TOOD (dmeister) There must be a better way to do this
        if (container_id > last_given_container_id_) {
            if (container_id > last_given_container_id_) {
                DEBUG("Recover last given container id " << container_id << " from log");
                last_given_container_id_ = container_id;
                initial_given_container_id_ = container_id;
                highest_committed_container_id_ = container_id;
            }
        }
        opened_container_id_set_.insert(container_id);
    } else if (event_type == EVENT_TYPE_CONTAINER_COMMITED && context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
        ContainerCommittedEventData event_data = event_value.container_committed_event();

        DEBUG("Log replay: " << Log::GetEventTypeName(event_type) <<
            ", event " << event_data.ShortDebugString() <<
            ", log id " << context.log_id());

        ScopedReadWriteLock scoped_lock(&meta_data_lock_);
        CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire meta data lock");

        uint64_t container_id = event_data.container_id();

        // remove from open container id set
        opened_container_id_set_.erase(container_id);

        ContainerStorageAddressData address_data;
        lookup_result lr = meta_data_index_->Lookup(&container_id, sizeof(container_id), &address_data);
        CHECK(lr != LOOKUP_ERROR, "Failed to lookup meta data for container: container id " << container_id);

        bool update = (lr == LOOKUP_NOT_FOUND || address_data.log_id() < context.log_id());
        if (update) {
            // Update meta data
            ContainerStorageAddressData updated_address = event_data.address();
            updated_address.set_log_id(context.log_id());

            DEBUG("Update meta data for container: container id " << container_id <<
                ", address " << updated_address.ShortDebugString());

            DCHECK(IsValidAddressData(event_data.address()), "Invalid address data: " << event_data.ShortDebugString());
            CHECK(meta_data_index_->Put(&container_id, sizeof(container_id), updated_address),
                "Failed to update meta data index data: container id " << container_id <<
                ", address " << updated_address.ShortDebugString());
            CHECK(this->meta_data_cache_.Update(container_id, STORAGE_ADDRESS_COMMITED), "Failed to update cache");

            CHECK(scoped_lock.ReleaseLock(), "Failed to release meta data lock");
        } else {
            TRACE("Meta data already up-to-date: container id " << container_id <<
                ", meta data " << address_data.ShortDebugString() <<
                ", event data " << event_data.ShortDebugString() <<
                ", log id " << context.log_id());
        }
    } else if (event_type == EVENT_TYPE_CONTAINER_MOVED && context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
        ContainerMoveEventData event_data = event_value.container_moved_event();
        uint64_t container_id = event_data.container_id();

        DEBUG("Log replay: " << Log::GetEventTypeName(event_type) <<
            ", event " << event_data.ShortDebugString() <<
            ", log id " << context.log_id());

        // Update index
        ScopedReadWriteLock scoped_lock(&meta_data_lock_);
        CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire meta data lock");

        ContainerStorageAddressData address_data;
        lookup_result lr = meta_data_index_->Lookup(&container_id, sizeof(container_id), &address_data);
        CHECK(lr != LOOKUP_ERROR, "Failed to lookup meta data for container: container id " << container_id);

        bool update = (lr == LOOKUP_NOT_FOUND || address_data.log_id() < context.log_id());
        if (update) {
            // Update meta data
            ContainerStorageAddressData updated_address = event_data.new_address();
            updated_address.set_log_id(context.log_id());

            DCHECK(IsValidAddressData(updated_address), "Invalid address data: " << updated_address.ShortDebugString());
            CHECK(this->meta_data_index_->Put(&container_id, sizeof(container_id), updated_address),
                "Meta data update failed: " << container_id <<
                ", address " << DebugString(updated_address));

            CHECK(scoped_lock.ReleaseLock(), "Failed to release meta data lock");
        } else {
            TRACE("Meta data already up-to-date: container id " << container_id <<
                ", meta data " << address_data.ShortDebugString() <<
                ", event data " << event_data.ShortDebugString() <<
                ", log id " << context.log_id());
        }
    } else if (event_type == EVENT_TYPE_CONTAINER_MERGED && context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
        ContainerMergedEventData event_data = event_value.container_merged_event();
        uint64_t container_id = event_data.new_primary_id();

        DEBUG("Log replay: " << Log::GetEventTypeName(event_type) <<
            ", event " << event_data.ShortDebugString() <<
            ", log id " << context.log_id());

        ScopedReadWriteLock scoped_lock(&meta_data_lock_);
        CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire meta data lock");

        // primary id
        ContainerStorageAddressData address_data;
        lookup_result lr = meta_data_index_->Lookup(&container_id, sizeof(container_id), &address_data);
        CHECK(lr != LOOKUP_ERROR, "Failed to lookup meta data for container: container id " << container_id);
        bool update = (lr == LOOKUP_NOT_FOUND || address_data.log_id() < context.log_id());
        if (update) {
            // Update meta data
            ContainerStorageAddressData updated_address = event_data.new_address();
            updated_address.set_log_id(context.log_id());

            TRACE("Redirect primary id " << container_id << ": " << updated_address.ShortDebugString());

            DCHECK(IsValidAddressData(updated_address), "Invalid address data: " << updated_address.ShortDebugString());
            CHECK(this->meta_data_index_->Put(&container_id, sizeof(container_id), updated_address),
                "Meta data update failed: " << container_id <<
                ", address " << DebugString(updated_address));
        } else {
            TRACE("Meta data already up-to-date: container id " << container_id <<
                ", meta data " << address_data.ShortDebugString() <<
                ", event data " << event_data.ShortDebugString() <<
                ", log id " << context.log_id());
        }

        // set the redirection pointer
        ContainerStorageAddressData secondary_data_address;
        secondary_data_address.set_primary_id(container_id);
        secondary_data_address.set_log_id(context.log_id());

        for (int i = 0; i < event_data.new_secondary_id_size(); i++) {
            uint64_t id = event_data.new_secondary_id(i);

            lookup_result lr = meta_data_index_->Lookup(&id, sizeof(id), &address_data);
            CHECK(lr != LOOKUP_ERROR, "Failed to lookup meta data for container: container id " << id);
            bool update = (lr == LOOKUP_NOT_FOUND || address_data.log_id() < context.log_id());

            if (update) {
                TRACE("Redirect merged container " << id << ": primary id " << secondary_data_address.primary_id());
                DCHECK(IsValidAddressData(secondary_data_address), "Invalid address data: " << secondary_data_address.ShortDebugString());
                CHECK(this->meta_data_index_->Put(&id, sizeof(id), secondary_data_address),
                    "Cannot write new container address: container id " << id);
            } else {
                TRACE("Meta data already up-to-date: container id " << id <<
                    ", meta data " << address_data.ShortDebugString() <<
                    ", event data " << event_data.ShortDebugString());
            }
        }
        for (int i = 0; i < event_data.unused_ids_size(); i++) {
            uint64_t id = event_data.unused_ids(i);

            lookup_result lr = meta_data_index_->Lookup(&id, sizeof(id), &address_data);
            CHECK(lr != LOOKUP_ERROR, "Failed to lookup meta data for container: container id " << id);

            if (lr == LOOKUP_FOUND) {
                TRACE("Delete unused container id: container " << id);
                CHECK(this->meta_data_index_->Delete(&id, sizeof(id)), "Cannot delete unused container address: container id " << id);
            }
        }
    } else if (event_type == EVENT_TYPE_CONTAINER_DELETED && context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
        ContainerDeletedEventData event_data = event_value.container_deleted_event();
        uint64_t container_id = event_data.container_id();

        DEBUG("Log replay: " << Log::GetEventTypeName(event_type) << ", event " << event_data.ShortDebugString());

        // remove old pointers
        set<uint64_t> all_old_ids;
        all_old_ids.insert(container_id);
        for (int i = 0; i < event_data.secondary_container_id_size(); i++) {
            all_old_ids.insert(event_data.secondary_container_id(i));
        }

        ScopedReadWriteLock scoped_lock(&meta_data_lock_);
        CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire meta data lock");

        // now all_old_ids only contains the id to which no chunk is referencing anymore.
        // no one is or ever will be interested anymore into such an container id
        for (set<uint64_t>::const_iterator i = all_old_ids.begin(); i != all_old_ids.end(); i++) {
            uint64_t id = *i;
            TRACE("Delete unused container id: container " << id);

            ContainerStorageAddressData address_data;
            lookup_result lr = meta_data_index_->Lookup(&id, sizeof(id), &address_data);
            CHECK(lr != LOOKUP_ERROR, "Failed to lookup meta data for container: container id " << id);

            if (lr == LOOKUP_FOUND) {
                CHECK(this->meta_data_index_->Delete(&id, sizeof(id)), "Cannot delete unused container address: container id " << id);
            }
        }

        // now all_old_ids only contains the id to which no chunk is referencing anymore.
        // no one is or ever will be interested anymore into such an container id
        for (set<uint64_t>::const_iterator i = all_old_ids.begin(); i != all_old_ids.end(); i++) {
            uint64_t id = *i;
            TRACE("Delete unused container id: container " << id);
            CHECK(this->meta_data_index_->Delete(&id, sizeof(id)), "Cannot delete unused container address: container id " << id);
            CHECK(this->meta_data_cache_.Delete(id), "Failed to delete unused container address from meta data cache: container id " << id);
            FAULT_POINT("container-storage.ack.container-delete-middle");
        }

        CHECK(scoped_lock.ReleaseLock(), "Failed to release meta data lock");
    } else if (event_type == dedupv1::log::EVENT_TYPE_REPLAY_STOPPED && context.replay_mode()
               == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        ReplayStopEventData event_data = event_value.replay_stop_event();

        if (event_data.replay_type() == dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START) {
            // dirty replay finished

            if (event_data.success()) {
                CHECK(FinishDirtyLogReplay(), "Failed to finish dirty log replay");
            } else if (!event_data.success()) {
                WARNING("Log replay failed");
            }
        }
    }
    if (this->allocator_) {
        CHECK(this->allocator_->LogReplay(event_type, event_value, context),
            "Allocator failed to replay event");
    }

    return true;
}

bool ContainerStorage::FinishDirtyLogReplay() {
    set<uint64_t>::iterator i;
    for (i = opened_container_id_set_.begin(); i != opened_container_id_set_.end(); i++) {
        uint64_t container_id = *i;
        WARNING("Failed container: " << container_id);

        ContainerStorageAddressData address_data;
        lookup_result lr = meta_data_index_->Lookup(&container_id, sizeof(container_id), &address_data);
        CHECK(lr != LOOKUP_ERROR, "Failed to lookup meta data for container: container id " << container_id);

        if (lr == LOOKUP_FOUND) {
            // The container is not marked as committed, yet in the meta data index.
            // This might happen if parts of the log are truncated due to holes at the end of it
            // Everything that relies on this container has to be later in the log and is therefore also
            // truncated
            WARNING("Remove container artifact from container meta data index: " << container_id <<
                ", address data " << address_data.ShortDebugString());
            CHECK(this->meta_data_index_->Delete(&container_id, sizeof(container_id)),
                "Cannot delete unused container address: container id " << container_id);
        }
    }
    opened_container_id_set_.clear();
    return true;
}

uint64_t ContainerStorage::GetLastGivenContainerId() {
    return this->last_given_container_id_;
}

void ContainerStorage::SetLastGivenContainerId(uint64_t a) {
    this->last_given_container_id_ = a;
}

bool ContainerStorage::FillMergedContainerEventData(
    ContainerMergedEventData* event_data,
    const Container& leader_container, const ContainerStorageAddressData& leader_address,
    const Container& slave_container, const ContainerStorageAddressData& slave_address,
    const ContainerStorageAddressData& new_container_address) {
    DCHECK(event_data, "Event data not set");
    DCHECK(IsValidAddressData(leader_address), "Invalid leader address data: " << leader_address.ShortDebugString());
    DCHECK(IsValidAddressData(slave_address), "Invalid slave address data: " << slave_address.ShortDebugString());
    DCHECK(IsValidAddressData(new_container_address), "Invalid new container address data: " << new_container_address.ShortDebugString());

    event_data->set_first_id(leader_container.primary_id());
    event_data->set_second_id(slave_container.primary_id());

    event_data->mutable_first_address()->CopyFrom(leader_address);
    event_data->mutable_second_address()->CopyFrom(slave_address);

    set<uint64_t>::const_iterator j;
    for (j = leader_container.secondary_ids().begin(); j != leader_container.secondary_ids().end(); j++) {
        event_data->add_first_secondary_id(*j);
    }
    for (j = slave_container.secondary_ids().begin(); j != slave_container.secondary_ids().end(); j++) {
        event_data->add_second_secondary_id(*j);
    }

    event_data->mutable_new_address()->CopyFrom(new_container_address);
    return true;
}

void ContainerStorage::IdleTick() {
    if (this->gc_) {
        if (!this->gc_->OnIdle()) {
            WARNING("gc idle thread processing failed");
        }
    }
}

bool ContainerStorage::TryMergeContainer(uint64_t container_id_1, uint64_t container_id_2, bool* aborted) {
    CHECK(state_ == RUNNING || state_ == STARTED, "Illegal state to merge container: " << state_);
    CHECK(aborted, "Aborted not set");
    CHECK(!start_context_.readonly(), "Container storage is in readonly mode");

    FAULT_POINT("container-storage.merge.pre");
    // TODO (dmeister): We want an good state even if the merge fails

    CHECK(container_id_1 != container_id_2,
        "Illegal to merge a container with itself: container " << container_id_1);

    Container container1;
    CHECK(container1.Init(container_id_1, this->container_size_), "Failed to init container");

    Container container2;
    CHECK(container2.Init(container_id_2, this->container_size_), "Failed to init container");

    // get both addresses
    // we cannot use the normal lookup address method here as that would lead to problems when
    // both containers use the same container lock
    uint64_t id = container1.primary_id();
    ContainerStorageAddressData container_address1;
    lookup_result r = this->meta_data_index_->Lookup(&id, sizeof(uint64_t), &container_address1);
    CHECK(r == LOOKUP_FOUND, "Cannot get container address for container " << id);
    CHECK(container_address1.has_primary_id() == false, "Illegal merge candidate: " <<
        "container " << container1.DebugString() <<
        ", address " << container_address1.ShortDebugString());

    id = container2.primary_id();
    ContainerStorageAddressData container_address2;
    r = this->meta_data_index_->Lookup(&id, sizeof(uint64_t), &container_address2);
    CHECK(r == LOOKUP_FOUND, "Cannot get container address for container " << id);
    CHECK(container_address2.has_primary_id() == false, "Illegal merge candidate: " <<
        "container " << container2.DebugString() <<
        ", address " << container_address2.ShortDebugString());

    const Container* cached_container1 = NULL;
    CacheEntry cache_entry1;
    lookup_result cache_lookup_result1;

    const Container* cached_container2 = NULL;
    CacheEntry cache_entry2;
    lookup_result cache_lookup_result2;

    // Here we get cache enty locks.
    cache_lookup_result1 = cache_.CheckCache(container1.primary_id(), &cached_container1, true, true, &cache_entry1);
    cache_lookup_result2 = cache_.CheckCache(container2.primary_id(), &cached_container2, true, true, &cache_entry2);

    if (cache_lookup_result1 == LOOKUP_ERROR || cache_lookup_result2 == LOOKUP_ERROR) {
        ERROR("Failed to cache read cache for container " << container1.primary_id() << ", " << container2.primary_id());

        if (cache_entry1.is_set()) {
            CHECK(cache_entry1.lock()->ReleaseLock(), "Failed to release cache lock");
        }
        if (cache_entry2.is_set()) {
            CHECK(cache_entry2.lock()->ReleaseLock(), "Failed to release cache lock");
        }

        return false;
    }

    // here we get container locks.
    bool locked = false;
    ScopedReadWriteLock container_lock1(this->GetContainerLock(container_id_1));
    CHECK(container_lock1.TryAcquireWriteLock(&locked), "Cannot acquire container write lock");
    if (!locked) {
        DEBUG("Container currently locked: container id " << container_id_1);
        if (cache_entry1.is_set()) {
            CHECK(cache_entry1.lock()->ReleaseLock(), "Failed to release cache lock");
        }
        if (cache_entry2.is_set()) {
            CHECK(cache_entry2.lock()->ReleaseLock(), "Failed to release cache lock");
        }
        *aborted = true;
        return true;
    }

    ScopedReadWriteLock container_lock2(this->GetContainerLock(container_id_2));
    if (container_lock2.Get() != container_lock1.Get()) {
        CHECK(container_lock2.TryAcquireWriteLock(&locked),
            "Cannot acquire container write lock");
        if (!locked) {
            DEBUG("Container currently locked: container id " << container_id_1);
            if (cache_entry1.is_set()) {
                CHECK(cache_entry1.lock()->ReleaseLock(), "Failed to release cache lock");
            }
            if (cache_entry2.is_set()) {
                CHECK(cache_entry2.lock()->ReleaseLock(), "Failed to release cache lock");
            }
            *aborted = true;
            return true;
        }
    }

    bool cache_remove_result1 = true;
    bool cache_remove_result2 = true;
    if (cache_lookup_result1 == LOOKUP_FOUND) {
        cache_remove_result1 = this->cache_.RemoveFromReadCache(container1.primary_id(), &cache_entry1);
    }
    if (cache_lookup_result2 == LOOKUP_FOUND) {
        cache_remove_result2 = this->cache_.RemoveFromReadCache(container2.primary_id(), &cache_entry2);
    }

    if (!cache_remove_result1) {
        ERROR("Failed to remove container data from cache: container id " << container1.primary_id());
        return false;
    }
    if (!cache_remove_result2) {
        ERROR("Failed to remove container data from cache: container id " << container2.primary_id());
        return false;
    }
    // regardless of the cache results, the caches locks are released at this time

    ScopedInMoveSetMembership scoped_set_membership(&in_move_set_, &in_move_set_lock_);
    list<uint64_t> container_id_list;
    container_id_list.push_back(container_id_1);
    container_id_list.push_back(container_id_2);

    if (!scoped_set_membership.Insert(container_id_list)) {
        TRACE("Abort container merge: container in move set: container id " << container_id_1 << ", container id " << container_id_2);
        *aborted = true;
        return true;
    }
    TRACE("Adds containers to in move set: container id " << container_id_1 << ", container id " << container_id_2);

    // read container and check if the ids are really the primary ids
    // if not the locks would protect the wrong id.
    CHECK(this->ReadContainerLocked(&container1, container_address1),
        "Failed to read container id " << container_id_1 <<
        ", container " << container1.primary_id());

    // TODO (dmeister) You can also argue that this is more a abort situation than an error situation, but
    // anyways as the gc either does a delete or a merge from only a single thread, there should never be
    // a situation in which this check fails.
    CHECK(container1.primary_id() == container_id_1, "Container id mismatch: "
        "container id " << container_id_1 <<
        ", container " << container1.DebugString() <<
        ", reason container id should be primary");
    CHECK(this->ReadContainerLocked(&container2, container_address2),
        "Failed to read container id " << container_id_2 <<
        ", container " << container2.primary_id());
    CHECK(container2.primary_id() == container_id_2, "Container id mismatch: "
        "container id " << container_id_2 <<
        ", container " << container2.DebugString() <<
        ", reason container id should be primary");

    // merge the two containers into the new container
    Container new_container;
    CHECK(new_container.Init(0, this->container_size_), "Failed to init container");
    CHECK(new_container.MergeContainer(container1, container2), "Failed to merge containers: " <<
        container1.DebugString() << ", " << container2.DebugString());

    ContainerStorageAddressData new_container_address;
    enum alloc_result alloc_result = this->allocator_->OnNewContainer(new_container, false, &new_container_address);
    CHECK(alloc_result != ALLOC_ERROR, "Failed to get new container address");
    if (alloc_result == ALLOC_FULL) {
        // this is one of the worst situations to be in. There is no single place left to merge data. So it is not possible to free
        // one container area. But we need COW.
        *aborted = true;
        return true;
    }

    INFO("Merging container: " << container1.DebugString() << "(" << DebugString(container_address1) << ")" <<
        ", " << container2.DebugString() << "(" << DebugString(container_address2) << ")" <<
        ", new container " << new_container.DebugString() << "(" << DebugString(new_container_address) << ")");

    CHECK(IsValidAddressData(new_container_address), "Invalid address data: " << new_container_address.ShortDebugString());
    CHECK(this->WriteContainer(&new_container, new_container_address),
        "Failed to write container: " << new_container.DebugString());

    // the meta data index redirection is done after the log event commit
    // fill the data for the container merged log event
    ContainerMergedEventData event_data;
    event_data.set_new_item_count(new_container.item_count());
    event_data.set_new_active_data_size(new_container.active_data_size());
    if (container1.HasId(new_container.primary_id())) {
        CHECK(FillMergedContainerEventData(&event_data, container1, container_address1,
                container2, container_address2, new_container_address), "Cannot fill merged container address");
    } else if (container2.HasId(new_container.primary_id())) {
        CHECK(FillMergedContainerEventData(&event_data, container2, container_address2,
                container1, container_address1, new_container_address), "Cannot fill merged container address");
    } else {
        ERROR("Illegal merged container id: " <<
            "merged container " << new_container.DebugString() << "\n" <<
            ", " << container1.DebugString() << "\n" <<
            ", " << container2.DebugString());
        return false;
    }

    // set the redirection pointer
    event_data.set_new_primary_id(new_container.primary_id());
    for (set<uint64_t>::const_iterator i = new_container.secondary_ids().begin(); i != new_container.secondary_ids().end(); i++) {
        event_data.add_new_secondary_id(*i);
    }

    // remove old pointers
    set<uint64_t> all_old_ids;
    all_old_ids.insert(container1.primary_id());
    for (set<uint64_t>::const_iterator i = container1.secondary_ids().begin(); i != container1.secondary_ids().end(); i++) {
        all_old_ids.insert(*i);
    }
    all_old_ids.insert(container2.primary_id());
    for (set<uint64_t>::const_iterator i = container2.secondary_ids().begin(); i != container2.secondary_ids().end(); i++) {
        all_old_ids.insert(*i);
    }
    all_old_ids.erase(new_container.primary_id());
    for (set<uint64_t>::const_iterator i = new_container.secondary_ids().begin(); i != new_container.secondary_ids().end(); i++) {
        all_old_ids.erase(*i);
    }
    // now all_old_ids only contains the id to which no chunk is referencing anymore.
    // no one is or ever will be interested anymore into such an container id
    for (set<uint64_t>::const_iterator i = all_old_ids.begin(); i != all_old_ids.end(); i++) {
        event_data.add_unused_ids(*i);
        // the actual deletion of the ids happens after the merge log event is committed
    }

    // I release the lock here
    CHECK(container_lock1.ReleaseLock(), "Cannot release container write lock");
    if (container_lock2.Get() != container_lock1.Get()) {
        CHECK(container_lock2.ReleaseLock(), "Cannot release container write lock");
    }

    // a client now can read the container at the new or the old position. Both reads must be valid (and the
    // are at this point. During the commit call a lock on the meta data index ensures that a client
    // has either the now or the old location, but not an illegal one. The old location is not
    // illegal as we have not freed the old place.
    int64_t commit_log_id = 0;
    CHECK(this->log_->CommitEvent(EVENT_TYPE_CONTAINER_MERGED, &event_data, &commit_log_id, this, NO_EC),
        "Cannot commit merge event data: " << event_data.ShortDebugString());

    FAULT_POINT("container-storage.merge.before-gc");

    DEBUG("Merged container \n" << container1.DebugString() << " (" << DebugString(container_address1) << ")" <<
        " and \n" << container2.DebugString() << " (" << DebugString(container_address2) << ")" <<
        ", into \n" << new_container.DebugString() <<
        ", new address " << DebugString(new_container_address) <<
        ", log id " << commit_log_id <<
        ", event data " << event_data.ShortDebugString());
    this->stats_.merged_container_++;

    scoped_set_membership.RemoveAllFromSet();

    FAULT_POINT("container-storage.merge.post");
    *aborted = false;
    return true;
}

bool ContainerStorage::TryDeleteContainer(uint64_t container_id, bool* aborted) {
    CHECK(state_ == STARTED || state_ == RUNNING, "Illegal state to delete container: " << state_);
    CHECK(aborted, "Aborted not set");
    CHECK(!start_context_.readonly(), "Container storage is in readonly mode");

    FAULT_POINT("container-storage.delete.pre");
    // TODO (dmeister): We want an good state even if the delete fails

    Container container;
    CHECK(container.Init(container_id, this->container_size_), "Failed to init container");

    // we cannot use the normal lookup address method here as that would lead to problems with the locking
    uint64_t id = container.primary_id();
    ContainerStorageAddressData container_address;
    lookup_result r = this->meta_data_index_->Lookup(&id, sizeof(uint64_t), &container_address);
    CHECK(r == LOOKUP_FOUND, "Cannot get container address for container " << id);
    CHECK(container_address.has_primary_id() == false, "Illegal delete candidate: " <<
        "container " << container.DebugString() <<
        ", address " << container_address.ShortDebugString());

    const Container* cached_container = NULL;
    CacheEntry cache_entry;
    lookup_result cache_lookup_result;
    cache_lookup_result = cache_.CheckCache(container.primary_id(), &cached_container, true, true, &cache_entry);
    if (cache_lookup_result == LOOKUP_ERROR) {
        ERROR("Failed to check cache for container " << container.primary_id());
        return false;
    }

    bool locked = false;
    ScopedReadWriteLock container_lock(this->GetContainerLock(container_id));
    CHECK(container_lock.TryAcquireWriteLock(&locked), "Cannot acquire container write lock");
    if (!locked) {
        if (cache_entry.is_set()) {
            CHECK(cache_entry.lock()->ReleaseLock(), "Failed to release cache lock");
        }
        DEBUG("Container currently locked: container id " << container_id);
        *aborted = true;
        return true;
    }

    if (cache_lookup_result == LOOKUP_FOUND) {
        CHECK(this->cache_.RemoveFromReadCache(container.primary_id(), &cache_entry),
            "Failed to remove container data from cache: " << container.primary_id());
    }

    ScopedInMoveSetMembership scoped_set_membership(&in_move_set_, &in_move_set_lock_);
    if (!scoped_set_membership.Insert(container_id)) {
        TRACE("Abort container delete: container in move set: container id " << container_id);
        *aborted = true;
        return true;
    }
    TRACE("Adds container to in move set: container id " << container_id);

    // read container and check if the ids are really the primary ids
    // if not the locks would protect the wrong id.
    CHECK(this->ReadContainerLocked(&container, container_address),
        "Failed to read container id " << container_id <<
        ", container " << container.primary_id());
    CHECK(container.primary_id() == container_id, "Container id mismatch: "
        "container id " << container_id <<
        ", container " << container.DebugString());
    CHECK(container.item_count() == 0, "Container is contains items: << " << container.DebugString());

    INFO("Delete container " << container.DebugString() << ", old address " << DebugString(container_address));

    // fill the data for the container merged log event
    ContainerDeletedEventData event_data;
    event_data.set_container_id(container.primary_id());
    for (set<uint64_t>::const_iterator i = container.secondary_ids().begin(); i != container.secondary_ids().end(); i++) {
        event_data.add_secondary_container_id(*i);
    }
    event_data.mutable_address()->CopyFrom(container_address);

    FAULT_POINT("container-storage.delete.before-gc");
    if (this->gc_) {
        CHECK(this->gc_->OnDeleteContainer(container),
            "Failed to report delete to gc: " << container.DebugString());
    }

    CHECK(container_lock.ReleaseLock(), "Cannot release container write lock");

    CHECK(this->log_->CommitEvent(EVENT_TYPE_CONTAINER_DELETED, &event_data, NULL, this, NO_EC),
        "Cannot commit delete event data");
    this->stats_.deleted_container_++;

    scoped_set_membership.RemoveAllFromSet();

    *aborted = false;
    FAULT_POINT("container-storage.delete.post");
    return true;
}

enum lookup_result ContainerStorage::GetPrimaryId(uint64_t container_id,
                                                  uint64_t* primary_id,
                                                  dedupv1::base::ReadWriteLock** primary_container_lock,
                                                  bool acquire_write_lock) {
    pair<lookup_result, ContainerStorageAddressData> r =
        this->LookupContainerAddressWait(container_id, primary_container_lock, acquire_write_lock);
    CHECK_RETURN(r.first != LOOKUP_ERROR, LOOKUP_ERROR, "Failed to lookup primary id");
    if (r.first == LOOKUP_NOT_FOUND) {
        return LOOKUP_NOT_FOUND;
    }
    if (r.second.has_primary_id()) {
        *primary_id = r.second.primary_id();
    } else {
        *primary_id = container_id;
    }
    return LOOKUP_FOUND;
}

dedupv1::log::Log* ContainerStorage::log() {
    return log_;
}

#ifdef DEDUPV1_CORE_TEST
void ContainerStorage::ClearData() {
    TRACE("Clear data for crash simulation");

    clear_data_called_ = true;
    this->state_ = STOPPED;
    timeout_committer_should_stop_ = true;
    if (this->timeout_committer_->IsStarted()) {
        this->timeout_committer_->Join(NULL);
    }

    this->background_committer_.Stop(dedupv1::StopContext::WritebackStopContext());
    if (this->meta_data_index_) {
        this->meta_data_index_->Close();
        this->meta_data_index_ = NULL;
    }
    chunk_index_ = NULL;
    write_cache_.ClearData();
    if (gc_) {
        gc_->ClearData();
    }
    if (log_) {
        log_->UnregisterConsumer("container-storage");
        log_ = NULL;
    }
    if (allocator_) {
        allocator_->ClearData();
    }
    if (idle_detector_) {
        idle_detector_->UnregisterIdleConsumer("container-storage");
        idle_detector_ = NULL;
    }
    this->info_store_ = NULL;
}
#endif

ContainerStorageMetadataCache::ContainerStorageMetadataCache(ContainerStorage* storage) {
    this->storage_  = storage;
    this->cache_size_ = kDefaultCacheSize;
}

bool ContainerStorageMetadataCache::Lookup(uint64_t address, bool* found, storage_commit_state* state) {
    CHECK(storage_, "Storage not set");
    CHECK(found, "Found not set");
    CHECK(state, "State not set");

    tbb::spin_mutex::scoped_lock scoped_lock(mutex_);

    std::map<uint64_t, storage_commit_state>::iterator i = commit_state_map_.find(address);
    if (i == commit_state_map_.end()) {
        *found = false;
    } else {
        *found = true;
        *state = i->second;
        CHECK(this->cache_strategy_.Touch(address), "Failed to touch cache");
    }
    return true;
}

bool ContainerStorageMetadataCache::Delete(uint64_t address) {
    CHECK(storage_, "Storage not set");
    tbb::spin_mutex::scoped_lock scoped_lock(mutex_);

    std::map<uint64_t, storage_commit_state>::iterator i = commit_state_map_.find(address);
    if (i == commit_state_map_.end()) {
        return true;
    }
    TRACE("Erase cache entry: container id " << address);
    commit_state_map_.erase(i);
    CHECK(this->cache_strategy_.Delete(address), "Failed to delete from cache");

    return true;
}

bool ContainerStorageMetadataCache::Unstick(uint64_t address) {
    CHECK(storage_, "Storage not set");
    tbb::spin_mutex::scoped_lock scoped_lock(mutex_);

    std::map<uint64_t, storage_commit_state>::iterator i = commit_state_map_.find(address);
    if (i != commit_state_map_.end()) {
        CHECK(this->cache_strategy_.Touch(address), "Failed to touch cache");
    }
    return true;
}

bool ContainerStorageMetadataCache::Update(uint64_t address, storage_commit_state state, bool sticky) {
    CHECK(storage_, "Storage not set");
    tbb::spin_mutex::scoped_lock scoped_lock(mutex_);

    std::map<uint64_t, storage_commit_state>::iterator i = commit_state_map_.find(address);
    bool is_new = false;
    if (i == commit_state_map_.end()) {
        // new
        is_new = true;
    }
    commit_state_map_[address] = state;

    if (!sticky) {
        // do we have to throw out an entry?
        if (is_new && commit_state_map_.size() > cache_size_) {
            uint64_t replacement_address;
            CHECK(this->cache_strategy_.Replace(&replacement_address), "Failed to replace cache item: cache size " << this->cache_strategy_.size());

            TRACE("Erase cache entry: container id " << replacement_address);
            commit_state_map_.erase(replacement_address);
        }
        CHECK(this->cache_strategy_.Touch(address), "Failed to touch cache");
    } else if (sticky && !is_new) {
        // we remove sticky and old entries to avoid that they are removed from the cache
        CHECK(this->cache_strategy_.Delete(address), "Failed to delete entry from cache");
    }
    // we do tell the cache strategy about sticky items. Therefore they cannot be removed from the cache
    // However, we should be sure that every sticky item is either unsticked very fast or overwritten.
    return true;
}

ContainerStorage::ContainerFile::ContainerFile() {
    file_ = NULL;
    file_size_ = 0;
    new_ = false;
    lock_ = NULL;
}

ContainerStorage::ContainerFile::~ContainerFile() {
    if (lock_) {
        delete lock_;
        lock_ = NULL;
    }
}

bool ContainerStorage::ContainerFile::Start(File* file, bool is_new) {
    file_ = file;
    new_ = is_new;
    lock_ = new dedupv1::base::MutexLock();
    return true;
}

void ContainerStorage::ContainerFile::Init(const std::string& f) {
    filename_ = f;

}

}
}
