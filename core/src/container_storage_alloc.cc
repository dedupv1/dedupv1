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

#include "dedupv1.pb.h"

#include <unistd.h>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>

#include <sstream>

#include <core/dedup.h>
#include <base/logging.h>
#include <base/index.h>
#include <base/bitutil.h>
#include <base/memory.h>
#include <base/fault_injection.h>
#include <base/strutil.h>
#include <base/crc32.h>
#include <core/container.h>
#include <core/container_storage_alloc.h>
#include <core/container_storage.h>

#include "dedupv1.pb.h"

using std::stringstream;
using std::string;
using std::list;
using std::make_pair;
using dedupv1::base::bit_set;
using dedupv1::base::bit_test;
using dedupv1::base::bit_clear;
using dedupv1::base::ScopedLock;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::base::ScopedPtr;
using dedupv1::base::IndexCursor;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::lookup_result;
using dedupv1::Fingerprinter;
using dedupv1::base::DELETE_ERROR;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::Index;
using dedupv1::base::File;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::To;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::CRC;
using dedupv1::base::ProfileTimer;
using dedupv1::base::MutexLock;
using dedupv1::base::Bitmap;

LOGGER("ContainerStorageAllocator");

namespace dedupv1 {
namespace chunkstore {

ContainerStorageAllocator::ContainerStorageAllocator() {
}

bool ContainerStorageAllocator::Start(const StartContext& start_context, ContainerStorage* storage) {
    return true;
}

bool ContainerStorageAllocator::Run() {
    return true;
}

bool ContainerStorageAllocator::SetOption(const string& option_name, const string& option) {
    return false;
}

bool ContainerStorageAllocator::CheckIfFull() {
    return false;
}

bool ContainerStorageAllocator::OnCommitContainer(const Container& container, const ContainerStorageAddressData& address) {
    return true;
}

bool ContainerStorageAllocator::OnMerge(const ContainerMergedEventData& data) {
    return true;
}

bool ContainerStorageAllocator::OnMove(const ContainerMoveEventData& data) {
    return true;
}

bool ContainerStorageAllocator::OnDeleteContainer(const ContainerDeletedEventData& data) {
    return true;
}

bool ContainerStorageAllocator::OnRead(const Container& container, const void* key, size_t key_size) {
    return true;
}

bool ContainerStorageAllocator::LogReplay(dedupv1::log::event_type event_type,
                                          const LogEventData& event_value,
                                          const dedupv1::log::LogReplayContext& context) {
    return true;
}

bool ContainerStorageAllocator::Stop(const dedupv1::StopContext& stop_context) {
    return true;
}

ContainerStorageAllocator::~ContainerStorageAllocator() {
}

#ifdef DEDUPV1_CORE_TEST
void ContainerStorageAllocator::ClearData() {
}
#endif

// Factory

ContainerStorageAllocatorFactory ContainerStorageAllocatorFactory::factory;

bool ContainerStorageAllocatorFactory::Register(const string& name, ContainerStorageAllocator*(*factory)(void)) {
    factory_map[name] = factory;
    return true;
}

ContainerStorageAllocatorFactory::ContainerStorageAllocatorFactory() {

}

ContainerStorageAllocator* ContainerStorageAllocatorFactory::Create(const string& name) {
    ContainerStorageAllocator* (*f)(void) = factory.factory_map[name];
    if (f) {
        ContainerStorageAllocator* a = f();
        CHECK_RETURN(a, NULL, "Cannot create new allocator" << name);
        return a;
    }
    ERROR("Cannot find allocator: " << name);
    return NULL;
}

ContainerStorageAllocator* MemoryBitmapContainerStorageAllocator::CreateAllocator() {
    return new MemoryBitmapContainerStorageAllocator();
}

void MemoryBitmapContainerStorageAllocator::RegisterAllocator() {
    ContainerStorageAllocatorFactory::GetFactory()->Register("memory-bitmap", &MemoryBitmapContainerStorageAllocator::CreateAllocator);
}

MemoryBitmapContainerStorageAllocator::MemoryBitmapContainerStorageAllocator() {
    storage_ = NULL;
    persistent_bitmap_ = NULL;
    next_file_ = 0;
    free_count_ = 0;
    total_count_ = 0;
    log_ = NULL;
    state_ = CREATED;
}

MemoryBitmapContainerStorageAllocator::Statistics::Statistics() {
    alloc_count_ = 0;
    free_count_ = 0;
    persist_count_ = 0;
}

bool MemoryBitmapContainerStorageAllocator::SetOption(const string& option_name, const string& option) {
    CHECK(state_ == CREATED, "Illegal state");
    if (option_name == "type") {
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Index creation failed: " << option);
        this->persistent_bitmap_ = index->AsPersistentIndex();
        CHECK(this->persistent_bitmap_, "Bitmap index should be persistent");

        CHECK(this->persistent_bitmap_->SetOption("max-key-size", "8"), "Failed to set auto option");
        return true;
    }
    CHECK(this->persistent_bitmap_, "Bitmap not set");
    CHECK(this->persistent_bitmap_->SetOption(option_name, option), "Configuration failed: " << option_name << " - " << option);
    return true;
}

bool MemoryBitmapContainerStorageAllocator::Store(int file_index) {
    DCHECK(file_index < file_.size(), "Illegal file index: " << file_index);

    TRACE("Store bitmap: file " << file_index << ", free places " << this->file_[file_index].bitmap_->clean_bits());

    CHECK(this->file_[file_index].bitmap_->Store(false), "Unable to store bitmap file " << file_index);

    return true;
}

bool MemoryBitmapContainerStorageAllocator::CheckIfFull() {
    if (state_ == STARTED && free_count_ == 0) {
        return true;
    }
    return false;
}

bool MemoryBitmapContainerStorageAllocator::Start(const StartContext& start_context, ContainerStorage* storage) {
    CHECK(storage, "Storage not set");
    CHECK(state_ == CREATED, "Illegal state");
    CHECK(storage->GetFileCount() < UINT32_MAX, "Too much files.");

    ScopedReadWriteLock scoped_lock(&lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire allocator lock");

    this->storage_ = storage;
    CHECK(this->storage_->GetFileCount() > 0, "Illegal file count");
    CHECK(this->persistent_bitmap_, "Index not configured");

    DEBUG("Start storage allocator");

    log_ = storage->log();
    CHECK(log_, "Log not set");

    page_size_ = sysconf(_SC_PAGESIZE);

    CHECK(persistent_bitmap_->Start(start_context), "Failed to start bitmap index");

    free_count_ = 0;
    file_.resize(storage_->GetFileCount());
    file_locks_.Init(storage_->GetFileCount());
    for (uint32_t i = 0; i < storage_->GetFileCount(); i++) {
        size_t file_size = storage_->file(i).file_size();
        size_t container_in_file_ = file_size / storage_->GetContainerSize();

        Bitmap* bitmap = new Bitmap(container_in_file_);
        CHECK(bitmap->setPersistence(persistent_bitmap_, &i, sizeof(uint32_t), page_size_), "Could not set persistency for Bitmap " << i);
        if (storage_->file(i).new_file()) {
            ProfileTimer alloc_timer(this->stats_.disk_time_);
            bitmap->Store(true);
        } else {
            if (start_context.has_crashed()) {
                ProfileTimer alloc_timer(this->stats_.disk_time_);
                bitmap->Load(true);
                bitmap->Store(false); // This will only store metadata, as the pages are clean
            } else {
                ProfileTimer alloc_timer(this->stats_.disk_time_);
                bitmap->Load(false);
            }
        }
        free_count_ += bitmap->clean_bits();
        total_count_ += bitmap->size();
        file_[i].bitmap_ = bitmap;
    }

    state_ = STARTED;
    return true;
}

bool MemoryBitmapContainerStorageAllocator::Run() {
    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireReadLock();
    // During Crash Restart we loose the real number of clean bits, Therefore we repair this here
    free_count_ = 0;
    for (size_t i = 0; i < file_.size(); i++) {
        free_count_ += file_[i].bitmap_->clean_bits();
    }
    return true;
}

bool MemoryBitmapContainerStorageAllocator::Stop(const dedupv1::StopContext& stop_context) {
    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireWriteLock();

    if (state_ == STARTED && persistent_bitmap_) {
        ProfileTimer alloc_timer(this->stats_.disk_time_);
        for (size_t i = 0; i < file_.size(); i++) {
            CHECK(Store(i), "Failed to store file bitmap: file index " << i);
        }
    }
    state_ = STOPPED;
    return true;
}

MemoryBitmapContainerStorageAllocator::~MemoryBitmapContainerStorageAllocator() {
    if (!Stop(StopContext::FastStopContext())) {
        ERROR("Failed to stop bitmap storage allocator");
    }

    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireWriteLock(); // do not hold it during the stop

    for (size_t i = 0; i < file_.size(); i++) {
        if (file_[i].bitmap_) {
            delete file_[i].bitmap_;
            file_[i].bitmap_ = NULL;
        }
    }
    file_.clear();
    if (persistent_bitmap_) {
        delete persistent_bitmap_;
    }
    this->storage_ = NULL;
}

bool MemoryBitmapContainerStorageAllocator::MarkAddressUsed(const ContainerStorageAddressData& address,
                                                            bool is_crash_replay) {
    int file_index = address.file_index();
    CHECK(file_index >= 0 && file_index < file_.size(), "Illegal file index: " << address.ShortDebugString());

    CHECK(address.file_offset() % storage_->GetContainerSize() == 0, "Illegal container offset: " << address.ShortDebugString());

    DCHECK(file_index >= 0 && file_index < file_.size(), "Illegal file index");
    uint64_t bit_offset = address.file_offset() / storage_->GetContainerSize();

    ScopedLock file_lock(file_locks_.Get(file_index));
    CHECK(file_lock.AcquireLock(), "Failed to acquire file lock: file index " << file_index << ", file size " << file_.size());
    DCHECK(bit_offset < file_[file_index].bitmap_->size(), "Illegal bit offset");

    if (!is_crash_replay) {
        Option<bool> value = file_[file_index].bitmap_->is_set(bit_offset);
        CHECK(value.valid(), "Could not check if bit " << bit_offset << " of bitmap " << file_index << " is set");
        if (value.value()) {
            WARNING("Address already marked as used: " << address.ShortDebugString());
        }
    }

    DEBUG("Mark area as used: file " << file_index <<
        ", bit_offset " << bit_offset <<
        ", file free count " << (file_[file_index].bitmap_->clean_bits()));

    CHECK(file_[file_index].bitmap_->set(bit_offset), "Could not set bit " << bit_offset << " of bitmap " << file_index);

    CHECK(file_lock.ReleaseLock(), "Failed to release file lock");
    return true;
}

bool MemoryBitmapContainerStorageAllocator::FreeAddress(const ContainerStorageAddressData& address,
                                                        bool is_crash_replay) {
    DCHECK(log_, "Log not set");

    int file_index = address.file_index();
    CHECK(file_index >= 0 && file_index < file_.size(), "Illegal file index: " << address.ShortDebugString());

    CHECK(address.file_offset() % storage_->GetContainerSize() == 0, "Illegal container offset: " << address.ShortDebugString());

    uint64_t bit_offset = address.file_offset() / storage_->GetContainerSize();

    ScopedLock file_lock(file_locks_.Get(file_index));
    CHECK(file_lock.AcquireLock(), "Failed to acquire file lock: file index " << file_index << ", file size " << file_.size());

    DCHECK(bit_offset < file_[file_index].bitmap_->size(), "Invalid bit offset " << bit_offset << " for bitmap " << file_index);

    if (!is_crash_replay) {
        // during a crash replay, we might come into an invalid state. We only say that we are valid at the end of the replay

        Option<bool> value = file_[file_index].bitmap_->is_set(bit_offset);
        CHECK(value.valid(), "Unable to check bit " << bit_offset << " if bitmapt " << file_index);
        if (!value.value()) {
            WARNING("Address not marked as used: " << address.ShortDebugString());
        }
    }

    CHECK(file_[file_index].bitmap_->clear(bit_offset), "Unable to clear bit " << bit_offset << " of bitmap " << file_index);

    free_count_++;
    stats_.free_count_++;

    CHECK(file_lock.ReleaseLock(), "Failed to release file lock");
    return true;
}

dedupv1::base::Option<bool> MemoryBitmapContainerStorageAllocator::IsAddressFree(
    const ContainerStorageAddressData& address) {
    int file_index = address.file_index();
    CHECK(file_index >= 0 && file_index < file_.size(), "Illegal file index: " << address.ShortDebugString());

    CHECK(address.file_offset() % storage_->GetContainerSize() == 0, "Illegal container offset: " << address.ShortDebugString());
    uint64_t bit_offset = address.file_offset() / storage_->GetContainerSize();

    DEBUG("Checking if position " << bit_offset << " of file " << file_index << " is set");

    ScopedLock file_lock(file_locks_.Get(file_index));
    CHECK(file_lock.AcquireLock(), "Failed to acquire file lock: file index " << file_index << ", file size " << file_.size());

    DCHECK(bit_offset < file_[file_index].bitmap_->size(), "Illegal bit address");
    return file_[file_index].bitmap_->is_clean(bit_offset);
}

uint64_t MemoryBitmapContainerStorageAllocator::GetActiveStorageDataSize() {
    if (storage_ == NULL) {
        return 0;
    }
    uint64_t used_container_places = (total_count_ - free_count_);
    return used_container_places * this->storage_->GetContainerSize();
}

int MemoryBitmapContainerStorageAllocator::GetNextFile() {
    tbb::spin_mutex::scoped_lock scoped_lock(this->next_file_lock_);
    int f = next_file_;
    next_file_ = (next_file_ + 1) % this->file_.size();
    return f;
}

bool MemoryBitmapContainerStorageAllocator::PersistPage(int file_index, int item_index) {
    DCHECK(file_index >= 0 && file_index < file_.size(), "Illegal file index");
    DCHECK(file_locks_.Get(file_index)->IsHeld(), "Lock is not held");

    uint32_t page = file_[file_index].bitmap_->page(item_index);
    ProfileTimer alloc_timer(this->stats_.disk_time_);

    Option<bool> r = file_[file_index].bitmap_->StorePage(page);
    CHECK(r.valid(), "Could not store page " << page <<
        ", index " << item_index <<
        ", file " << file_index <<
        ", page size " << file_[file_index].bitmap_->page_size());
    if (r.value()) {
        stats_.persist_count_++;
    }

    return true;
}

bool MemoryBitmapContainerStorageAllocator::SearchFreeAddress(int file_index, ContainerStorageAddressData* new_address) {
    CHECK(new_address, "new_adresss not set");
    DCHECK(file_index >= 0 && file_index < file_.size(), "Illegal file index");

    Option<size_t> pos = file_[file_index].bitmap_->find_next_unset(file_[file_index].last_free_pos_,
        file_[file_index].last_free_pos_);
    CHECK(pos.valid(), "Could not find free position in bitmap " << file_index << " (Is it full?)");
    CHECK(file_[file_index].bitmap_->set(pos.value()), "Could not set clear found pos " << pos.value() << " if bitmap " << file_index);
    free_count_--;
    file_[file_index].last_free_pos_ = pos.value();

    DEBUG("Reserved position " << pos.value() << " on file " << file_index);
    new_address->set_file_index(file_index);
    new_address->set_file_offset(storage_->GetContainerSize() * pos.value());

    return true;
}

string MemoryBitmapContainerStorageAllocator::PrintProfile() {
    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireReadLock();

    stringstream sstr;
    sstr << "{";
    sstr << "\"alloc time\": " << this->stats_.alloc_time_.GetSum() << "," << std::endl;
    sstr << "\"replay time\": " << this->stats_.replay_time_.GetSum() << "," << std::endl;
    sstr << "\"disk time\": " << this->stats_.disk_time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

string MemoryBitmapContainerStorageAllocator::PrintStatistics() {
    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireReadLock();

    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{";
    if (this->total_count_ > 0) {
        double fill_ratio = 1.0 - (1.0 * free_count_ / total_count_);
        sstr << "\"fill ratio\": " << fill_ratio << "," << std::endl;
    } else {
        sstr << "\"fill ratio\": null," << std::endl;
    }
    sstr << "\"total count\": " << total_count_ << "," << std::endl;
    sstr << "\"free count\": " << free_count_ << "," << std::endl;

    sstr << "\"file\": [";

    for (int i = 0; i < file_.size(); i++) {

        ScopedLock scoped_lock(file_locks_.Get(i));
        scoped_lock.AcquireLock();

        if (i > 0) {
            sstr << ",";
        }
        sstr << "{";
        sstr << "\"total count\": " << file_[i].bitmap_->size() << "," << std::endl;
        sstr << "\"free count\": " << file_[i].bitmap_->clean_bits() << "," << std::endl;
        if (file_[i].bitmap_->size() > 0) {
            double file_fill_ratio = 1.0 - (1.0 * file_[i].bitmap_->clean_bits() / file_[i].bitmap_->size());
            sstr << "\"fill ratio\": " << file_fill_ratio << std::endl;
        } else {
            sstr << "\"fill ratio\": null" << std::endl;
        }
        sstr << "}";

        scoped_lock.ReleaseLock();
    }
    sstr << "]";
    sstr << "}";

    return sstr.str();
}

string MemoryBitmapContainerStorageAllocator::PrintTrace() {
    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireReadLock();

    stringstream sstr;
    sstr << "{";
    sstr << "\"persist count\": " << this->stats_.persist_count_ << "," << std::endl;
    sstr << "\"alloc operation count\": " << this->stats_.alloc_count_ << "," << std::endl;
    sstr << "\"free operation count\": " << this->stats_.free_count_ << "," << std::endl;
    sstr << "\"free place count\": " << this->free_count_ << std::endl;
    sstr << "}";
    return sstr.str();
}

bool MemoryBitmapContainerStorageAllocator::OnAbortContainer(const Container& container,
                                                             const ContainerStorageAddressData& address) {
    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireReadLock();

    ProfileTimer alloc_timer(this->stats_.alloc_time_);

    DEBUG("Abort container " << container.DebugString() <<
        ", address " << address.file_index() << ":" << address.file_offset());
    CHECK(FreeAddress(address, false), "Failed to free address " << address.ShortDebugString());
    return true;
}

bool MemoryBitmapContainerStorageAllocator::EnsurePagePersisted(const ContainerStorageAddressData& address) {
    uint32_t file_index = address.file_index();
    uint64_t file_offset = address.file_offset();
    DCHECK(file_index < file_.size(), "Illegal file index: " << file_index);
    DCHECK((file_offset % storage_->GetContainerSize()) == 0, "Invalid file offset " << file_offset << " for Container Size " << storage_->GetContainerSize());

    uint64_t item_index = file_offset / storage_->GetContainerSize();

    ScopedLock file_lock(file_locks_.Get(file_index));
    CHECK(file_lock.AcquireLock(), "Failed to acquire file lock: file index " << file_index << ", file size " << file_.size());

    DEBUG("Ensure that persisted: file index " << file_index << ", item index " << item_index);

    CHECK(PersistPage(file_index, item_index), "Failed to persist change");

    return true;
}

bool MemoryBitmapContainerStorageAllocator::LogReplay(dedupv1::log::event_type event_type,
                                                      const LogEventData& event_value, const dedupv1::log::LogReplayContext& context) {
    ProfileTimer timer(this->stats_.replay_time_);
    if (context.replay_mode() == dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG) {
        // no crash
        // we simple have to commit it
        if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_OPEN) {
            ContainerOpenedEventData event_data = event_value.container_opened_event();

            CHECK(EnsurePagePersisted(event_data.address()),
                "Failed to ensure persistence of page");

        } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_DELETED) {
            ContainerDeletedEventData event_data = event_value.container_deleted_event();

            CHECK(EnsurePagePersisted(event_data.address()),
                "Failed to ensure persistence of page");
        } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_MERGED) {
            ContainerMergedEventData event_data = event_value.container_merged_event();

            CHECK(EnsurePagePersisted(event_data.first_address()),
                "Failed to ensure persistence of page");
            CHECK(EnsurePagePersisted(event_data.second_address()),
                "Failed to ensure persistence of page");
            CHECK(EnsurePagePersisted(event_data.new_address()),
                "Failed to ensure persistence of page");
        } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_MOVED) {
            ContainerMoveEventData event_data = event_value.container_moved_event();

            CHECK(EnsurePagePersisted(event_data.old_address()),
                "Failed to ensure persistence of page");
            CHECK(EnsurePagePersisted(event_data.new_address()),
                "Failed to ensure persistence of page");
        }
    } else if (context.replay_mode() == dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START) {
        // crash!!!
        // We have to restore a correct state, but we do not persist it

        if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_COMMITED) {
            ContainerCommittedEventData event_data = event_value.container_committed_event();

            CHECK(MarkAddressUsed(event_data.address(), true),
                "Failed to mark address as used: " << event_data.ShortDebugString());
        } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_DELETED) {
            ContainerDeletedEventData event_data = event_value.container_deleted_event();

            CHECK(FreeAddress(event_data.address(), true), "Failed to free address: " <<
                event_data.ShortDebugString());
        } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_MERGED) {
            ContainerMergedEventData event_data = event_value.container_merged_event();

            CHECK(FreeAddress(event_data.first_address(), true), "Failed to free address: " <<
                event_data.ShortDebugString());
            CHECK(FreeAddress(event_data.second_address(), true), "Failed to free address: " <<
                event_data.ShortDebugString());
            CHECK(MarkAddressUsed(event_data.new_address(), true),
                "Failed to mark address as used: " << event_data.ShortDebugString());
        } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_MOVED) {
            ContainerMoveEventData event_data = event_value.container_moved_event();

            CHECK(FreeAddress(event_data.old_address(), true), "Failed to free address: " <<
                event_data.ShortDebugString());
            CHECK(MarkAddressUsed(event_data.new_address(), true),
                "Failed to mark address as used: " << event_data.ShortDebugString());
        }
    }
    return true;
}

enum alloc_result MemoryBitmapContainerStorageAllocator::OnNewContainer(const Container& container,
                                                                        bool is_new_container, ContainerStorageAddressData* new_address) {
    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireReadLock();
    DCHECK_RETURN(new_address, ALLOC_ERROR, "new_adresss not set");

    this->stats_.alloc_count_++;
    ProfileTimer alloc_timer(this->stats_.alloc_time_);

    DEBUG("Allocate a new address for container: " << container.DebugString());

    bool found_file = false;
    for (int i = 0; i < file_.size(); i++) {
        int file_index = GetNextFile();

        ScopedLock file_lock(file_locks_.Get(file_index));
        CHECK_RETURN(file_lock.AcquireLock(), ALLOC_ERROR, "Failed to acquire file lock: file index " << file_index <<
            ", file count " << file_.size());

        TRACE("Test file: " << file_index << ", free count " << file_[file_index].bitmap_->clean_bits());

        // The full check always leaved one container place free per file unless the operation is for a merge or delete item operation
        // This avoid being locked in a situation where there is no space left, but tons of gc-able containers, but we fail to free the space
        // because of the last free slot is taken
        bool file_is_full = ((file_[file_index].bitmap_->clean_bits() <= 1) && (is_new_container
                                                                                || (file_[file_index].bitmap_->clean_bits() == 0)));

        if (file_is_full) {
            CHECK_RETURN(file_lock.ReleaseLock(), ALLOC_ERROR, "Failed to release file lock");
            continue;
        }
        found_file = true;

        CHECK_RETURN(SearchFreeAddress(file_index, new_address), ALLOC_ERROR, "Failed to search free address");
        CHECK_RETURN(file_lock.ReleaseLock(), ALLOC_ERROR, "Failed to release file lock");
        break;
    }

    if (!found_file) {
        DEBUG("No container place available. Container storage is full");
        return ALLOC_FULL;
    }
    DEBUG("Found free address: container " << container.DebugString() <<
        ", address " << new_address->ShortDebugString());

    return ALLOC_OK;
}

bool MemoryBitmapContainerStorageAllocator::OnMerge(const ContainerMergedEventData& data) {
    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireReadLock();
    ProfileTimer alloc_timer(this->stats_.alloc_time_);

    FAULT_POINT("container-storage.alloc.merge.pre");
    DEBUG("Merge container " << data.ShortDebugString());

    CHECK(!(data.first_address().file_index() == data.new_address().file_index() &&
            data.first_address().file_offset() == data.new_address().file_offset()) ||
        !(data.second_address().file_index() && data.new_address().file_index() &&
          data.second_address().file_offset() && data.new_address().file_offset()),
        "Illegal merge: " << data.ShortDebugString());

    CHECK(FreeAddress(data.first_address(), false), "Failed to free address " << data.first_address().ShortDebugString()); FAULT_POINT("container-storage.alloc.merge.after-first-free");

    CHECK(FreeAddress(data.second_address(), false), "Failed to free address " << data.second_address().ShortDebugString()); FAULT_POINT("container-storage.alloc.merge.after-second-free");
    return true;
}

bool MemoryBitmapContainerStorageAllocator::OnDeleteContainer(const ContainerDeletedEventData& data) {
    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireReadLock();
    ProfileTimer alloc_timer(this->stats_.alloc_time_);

    DEBUG("Delete container " << data.ShortDebugString());
    CHECK(FreeAddress(data.address(), false), "Failed to free address " << data.address().ShortDebugString());
    return true;
}

bool MemoryBitmapContainerStorageAllocator::OnMove(const ContainerMoveEventData& data) {
    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireReadLock();
    ProfileTimer alloc_timer(this->stats_.alloc_time_);

    FAULT_POINT("container-storage.alloc2.move.pre");
    DEBUG("Move container " << data.ShortDebugString());
    CHECK(!(data.old_address().file_index() == data.new_address().file_index() &&
            data.old_address().file_offset() == data.new_address().file_offset()),
        "Illegal move: " << data.ShortDebugString());
    CHECK(FreeAddress(data.old_address(), false), "Failed to free address " << data.old_address().ShortDebugString());
    return true;
}

#ifdef DEDUPV1_CORE_TEST
void MemoryBitmapContainerStorageAllocator::ClearData() {
    if (this->persistent_bitmap_) {
        delete persistent_bitmap_;
        this->persistent_bitmap_ = NULL;
    }
    for (int i = 0; i < file_.size(); i++) {
        delete file_[i].bitmap_;
    }
    file_.clear();
}
#endif

}
}
