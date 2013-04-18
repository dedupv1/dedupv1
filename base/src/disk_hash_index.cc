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

#include <base/disk_hash_index.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sstream>

#include "dedupv1_base.pb.h"

#include <base/index.h>
#include <base/hashing_util.h>
#include <base/bitutil.h>
#include <base/fileutil.h>
#include <base/crc32.h>
#include <base/locks.h>
#include <base/strutil.h>
#include <base/resource_management.h>
#include <base/logging.h>
#include <base/timer.h>
#include <base/protobuf_util.h>
#include <base/disk_hash_index_transaction.h>
#include <base/disk_hash_cache_page.h>
#include <base/tc_hash_mem_index.h>
#include <base/memory.h>

using std::string;
using std::stringstream;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::CRC;
using dedupv1::base::crc;
using dedupv1::base::ProfileTimer;
using dedupv1::base::bits;
using dedupv1::base::File;
using dedupv1::base::ScopedLock;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::base::internal::DiskHashEntry;
using dedupv1::base::internal::DiskHashPage;
using dedupv1::base::internal::DiskHashCacheEntry;
using dedupv1::base::internal::DiskHashCachePage;
using dedupv1::base::internal::DiskHashIndexTransaction;
using dedupv1::base::internal::DiskHashIndexIterator;
using google::protobuf::Message;
using dedupv1::base::Option;
using tbb::spin_mutex;
using std::map;
using std::make_pair;
using std::pair;
using dedupv1::base::make_bytestring;
using dedupv1::base::ScopedArray;
using dedupv1::base::TCMemHashIndex;
using std::tr1::unordered_map;
LOGGER("DiskHashIndex");

namespace dedupv1 {
namespace base {

void DiskHashIndex::RegisterIndex() {
    Index::Factory().Register("static-disk-hash", &DiskHashIndex::CreateIndex);
}

Index* DiskHashIndex::CreateIndex() {
    Index* i = new DiskHashIndex();
    return i;
}

DiskHashIndex::DiskHashIndex() :
    PersistentIndex(PERSISTENT_ITEM_COUNT | HAS_ITERATOR | RETURNS_DELETE_NOT_FOUND | WRITE_BACK_CACHE | PUT_IF_ABSENT) {
    this->bucket_count_ = 0;
    this->page_size_ = 4 * 1024;
    this->info_file_ = NULL;
    this->page_locks_count_ = 64;

    this->sync_ = false;
    this->lazy_sync_ = true;
    this->max_key_size_ = 0;
    this->max_value_size_ = 0;
    this->crc_ = true;
    this->version_counter_ = 0;
    this->state_ = INITED;

    this->overflow_area_ = NULL;

    this->trans_system_ = NULL;
    this->item_count_ = 0;
    this->estimated_max_fill_ratio_ = kDefaultEstimatedMaxFillRatio;
    this->write_back_cache_ = NULL;
    max_cache_page_count_ = 0;
    max_cache_item_count_ = 0;
    dirty_item_count_ = 0;
    total_item_count_ = 0;
}

DiskHashIndex::Statistics::Statistics() {
    this->lock_busy_ = 0;
    this->lock_free_ = 0;
    sync_count_ = 0;
    sync_wait_count_ = 0;

    write_cache_hit_count_ = 0;
    write_cache_miss_count_ = 0;
    write_cache_evict_count_ = 0;
    write_cache_dirty_evict_count_ = 0;

    write_cache_free_page_count_ = 0;
    write_cache_used_page_count_ = 0;
    write_cache_dirty_page_count_ = 0;
    write_cache_persisted_page_count_ = 0;
}

DiskHashIndex::~DiskHashIndex() {
}

bool DiskHashIndex::SetOption(const string& option_name, const string& option) {
    CHECK(this->state_ == INITED, "Index already started");

    if (option_name == "page-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->page_size_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->size_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "sync") {
        if (option == "unsafe") {
            sync_ = false;
            lazy_sync_ = false;
            return true;
        }
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->sync_ = To<bool>(option).value();
        lazy_sync_ = !sync_;
        return true;
    }
    if (option_name == "max-fill-ratio") {
        CHECK(To<double>(option).valid(), "Illegal option " << option);
        this->estimated_max_fill_ratio_ = To<double>(option).value();
        CHECK(this->estimated_max_fill_ratio_ > 0, "Illegal option " << option);
        CHECK(this->estimated_max_fill_ratio_ <= 1, "Illegal option " << option);
        return true;
    }
    if (option_name == "filename") {
        if (this->filename_.size() + 1 > kMaxFiles) {
            ERROR("Too much files");
            return false;
        }
        if (option.size() > 255) {
            ERROR("Filename too long");
            return false;
        }
        if (option.size() == 0) {
            ERROR("Filename too short");
            return false;
        }
        this->filename_.push_back(option);

        // TODO (dmeister) Check if the fails are not duplicates
        return true;
    }
    if (option_name == "page-lock-count") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->page_locks_count_ = ToStorageUnit(option).value();
        CHECK(page_locks_count_ > 0, "Illegal page lock count");
        return true;
    }
    if (option_name == "max-key-size") {
        CHECK(To<size_t>(option).valid(), "Illegal option " << option);
        this->max_key_size_ = To<size_t>(option).value();
        return true;
    }
    if (option_name == "max-value-size") {
        CHECK(To<size_t>(option).valid(), "Illegal option " << option);
        this->max_value_size_ = To<size_t>(option).value();
        return true;
    }
    if (option_name == "checksum") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->crc_ = To<bool>(option).value();
        return true;
    }
    if (option_name == "estimated-max-fill-ratio") {
        CHECK(To<double>(option).valid(), "Illegal option " << option);
        this->estimated_max_fill_ratio_ = To<double>(option).value();
        CHECK(this->estimated_max_fill_ratio_ <= 0, "Illegal estimated max fill ratio: " << this->estimated_max_fill_ratio_);
        CHECK(this->estimated_max_fill_ratio_ >= 1, "Illegal estimated max fill ratio: " << this->estimated_max_fill_ratio_);
        return true;
    }
    // overflow
    if (option_name == "overflow-area") {
        CHECK(this->overflow_area_ == NULL, "Overflow area already created");
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Failed to create overflow area: type " << option);
        CHECK(index->IsPersistent(), "Overflow area is not persistent");
        CHECK(index->HasCapability(dedupv1::base::PERSISTENT_ITEM_COUNT), "Overflow index has no persistent item count");
        CHECK(index->HasCapability(dedupv1::base::RAW_ACCESS), "Overflow index doesn't allow raw access");
        this->overflow_area_ = index->AsPersistentIndex();
        return true;
    }
    if (StartsWith(option_name, "overflow-area.")) {
        CHECK(this->overflow_area_ != NULL, "Overflow area not created");
        CHECK(this->overflow_area_->SetOption(option_name.substr(strlen("overflow-file.")), option), "Overflow area configuration failed");
        return true;
    }
    if (option_name == "write-cache") {
        CHECK(this->write_back_cache_ == NULL, "Write back cache already created");
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        if (To<bool>(option).value()) {
            Index* index = Index::Factory().Create("tc-mem-hash");
            CHECK(index, "Failed to create write back cache index type tc-mem-hash");
            this->write_back_cache_ = static_cast<TCMemHashIndex*>(index);
        }
        return true;
    }
    if (option_name == "write-cache.max-item-count") {
        CHECK(this->write_back_cache_ != NULL, "Write back cache not created");
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->max_cache_item_count_ = ToStorageUnit(option).value();
        CHECK(max_cache_item_count_ > 0, "Illegal cache page count");
        return true;
    }
    if (option_name == "write-cache.max-page-count") {
        CHECK(this->write_back_cache_ != NULL, "Write back cache not created");
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->max_cache_page_count_ = ToStorageUnit(option).value();
        CHECK(max_cache_page_count_ > 0, "Illegal cache page count");
        return true;
    }
    if (StartsWith(option_name, "write-cache.")) {
        CHECK(this->write_back_cache_ != NULL, "Write back cache not created");
        CHECK(this->write_back_cache_->SetOption(option_name.substr(strlen("write-cache.")), option),
            "Write back cache configuration failed");
        return true;
    }
    // transactions
    if (StartsWith(option_name, "transactions.")) {
        if (this->trans_system_ == NULL) {
            this->trans_system_ = new internal::DiskHashIndexTransactionSystem(this);
            CHECK(this->trans_system_, "Failed to alloc transaction system");
        }
        CHECK(this->trans_system_->SetOption(option_name.substr(strlen("transactions.")), option),
            "Transaction system configuration failed");
        return true;
    }
    return PersistentIndex::SetOption(option_name, option);
}

bool DiskHashIndex::DumpData() {
    CHECK(this->info_file_, "Info file not set");
    DiskHashIndexLogfileData logfile_data;
    logfile_data.set_page_size(this->page_size_);
    logfile_data.set_size(this->size_);
    DEBUG("Dump data: " << logfile_data.ShortDebugString());

    if (this->overflow_area_) {
        logfile_data.set_overflow_area(true);
    }

    for (size_t i = 0; i < this->filename_.size(); i++) {
        logfile_data.add_filename(this->filename_[i]);
    }

    CHECK(this->info_file_->WriteSizedMessage(0, logfile_data, 4096, crc_) >= logfile_data.ByteSize(),
        "Cannot write disk hash index log file: " << logfile_data.ShortDebugString());
    CHECK(this->info_file_->Sync(), "Failed to sync info data: " << logfile_data.ShortDebugString());
    return true;
}

bool DiskHashIndex::ReadDumpData() {
    CHECK(this->info_file_, "Log file not set");

    Option<off_t> filesize = this->info_file_->GetSize();
    CHECK(filesize.valid(), "Failed to get the file size");
    DiskHashIndexLogfileData logfile_data;
    CHECK(this->info_file_->ReadSizedMessage(0, &logfile_data, filesize.value(), crc_),
        "Cannot read disk hash index index log data");

    DEBUG("Reading dump data: " << logfile_data.ShortDebugString());

    CHECK(this->page_size_ == logfile_data.page_size(),
        "Page size mismatch: " <<
        "stored " << logfile_data.page_size() <<
        ", configured " << this->page_size_);
    CHECK(this->size_ == logfile_data.size(), "Size mismatch: " <<
        "stored " << logfile_data.size() <<
        ", configured " << this->size_);
    CHECK(this->filename_.size() == logfile_data.filename_size(), "File count mismatch: " <<
        "stored " << logfile_data.filename_size() <<
        ", configured " << this->filename_.size());

    // TODO (dmeister) The renaming of files is forbidden. The uuid approach as used
    // by the storage files are better
    for (size_t i = 0; i < this->filename_.size(); i++) {
        CHECK(this->filename_[i] == logfile_data.filename(i), "File name mismatch: " <<
            "stored " << logfile_data.filename(i) <<
            ", configured " << this->filename_[i]);
    }

    if (logfile_data.has_overflow_area() && logfile_data.overflow_area()) {
        CHECK(this->overflow_area_ != NULL, "Overflow mismatch: stored true, configured false");
    }
    if (this->overflow_area_) {
        CHECK(logfile_data.has_overflow_area() && logfile_data.overflow_area(), "Overflow mismatch: stored false, configured true");
    }
    return true;
}

uint64_t DiskHashIndex::GetEstimatedMaxItemCount() {
    if ((this->max_key_size_ + max_value_size_) == 0) {
        return 0;
    }
    double available_space = this->size_ * this->estimated_max_fill_ratio_;
    return available_space / (this->max_key_size_ + max_value_size_);
}

bool DiskHashIndex::Start(const StartContext& start_context) {
    CHECK(this->state_ == INITED, "Index already started");
    CHECK(this->page_size_ > 0, "page-size not set");
    CHECK(this->size_ > 0, "Size not set");
    CHECK(this->filename_.size() > 0, "No filename set");
    CHECK(this->max_key_size_ > 0, "Key length not set");
    CHECK(this->max_value_size_ > 0, "Value length not set");

    if (this->info_filename_.size() == 0) {
        this->info_filename_ = this->filename_[0] + "-meta";
    }

    DEBUG("Starting disk-hash index: page size " << page_size_ <<
        ", size " << size_ <<
        ", file count " << filename_.size());

    if (!sync_ && !lazy_sync_) {
        WARNING("Unsafe sync configuration: Data loss after crash possible");
    } else {
        if (this->trans_system_ == NULL) {
            INFO("Auto configure transaction system");
            this->trans_system_ = new internal::DiskHashIndexTransactionSystem(this);
            CHECK(this->trans_system_, "Failed to alloc transaction system");

            for (size_t i = 0; i < this->filename_.size(); i++) {
                CHECK(this->trans_system_->SetOption("filename", filename_[i] + "_trans"), "Failed to auto-configure transaction system");
            }
        }
    }

    this->bucket_count_ = this->size_ / (this->page_size_);
    // Allocate locks
    CHECK(this->page_locks_.Init(this->page_locks_count_), "Failed to init page locks");

    // Allocate Sync Flags
    this->to_sync_flag_.resize(this->filename_.size());
    for (size_t i = 0; i < this->to_sync_flag_.size(); i++) {
        this->to_sync_flag_[i] = CLEAN;
    }
    to_sync_lock_.resize(this->filename_.size());

    int io_flags = O_RDWR | O_LARGEFILE;
    if (this->sync_) {
        io_flags |= O_SYNC;
    }
    this->file_.resize(this->filename_.size());
    uint64_t file_size = this->size_ / this->file_.size();

    bool files_created = false;
    for (size_t i = 0; i < this->file_.size(); i++) {
        File* tmp_file = File::Open(this->filename_[i], io_flags, 0);
        if (!tmp_file) {
            CHECK(start_context.create(), "Error opening storage file: " << this->filename_[i] << ", message no create mode");

            INFO("Creating index file " << this->filename_[i]);

            CHECK(File::MakeParentDirectory(this->filename_[i], start_context.dir_mode().mode()),
                "Failed to check parent directories");
            files_created = true;
            tmp_file = File::Open(this->filename_[i], O_RDWR | O_CREAT | O_EXCL | O_LARGEFILE,
                start_context.file_mode().mode());
            CHECK(tmp_file, "Error opening storage file: " << this->filename_[i] << ", message " << strerror(errno));

            if (start_context.file_mode().gid() != -1) {
                CHECK(chown(this->filename_[i].c_str(), -1, start_context.file_mode().gid()) == 0,
                    "Failed to change file group: " << this->filename_[i]);
            }

            CHECK(tmp_file->Fallocate(0, file_size), "Error allocating index file " << this->filename_[i]);
            tmp_file->Close();
            tmp_file = NULL;

            // Now reopen (used to avoid O_SYNC while format)
            tmp_file = File::Open(this->filename_[i], io_flags, 0);
            CHECK(tmp_file, "Error opening storage file: " << this->filename_[i] << ", message " << strerror(errno));

            // reset disk time because we don't want the formating to count
            statistics_.write_disk_time_.Reset();
            statistics_.read_disk_time_.Reset();

        }
        this->file_[i] = tmp_file;
    }

    CHECK(File::MakeParentDirectory(this->info_filename_, start_context.dir_mode().mode()),
        "Failed to check parent directories");

    this->info_file_ = File::Open(this->info_filename_, O_RDWR, 0);
    if (!this->info_file_) {
        DEBUG("Create new disk hash index log data file " << this->info_filename_);
        CHECK(start_context.create(), "Cannot open disk hash index log data file");
        // file not existing
        this->info_file_ = File::Open(this->info_filename_, O_RDWR | O_CREAT, start_context.file_mode().mode());
        CHECK(this->info_file_, "Cannot create disk hash index log data file");
        CHECK(this->DumpData(), "Cannot write disk hash index log data file");
    } else { // file already existing
        DEBUG("Open existing disk hash index log file" << this->info_filename_);
        CHECK(this->ReadDumpData(), "Cannot read disk hash index log data");
    }

    if (this->overflow_area_) {
        CHECK(this->overflow_area_->Start(start_context), "Failed to start overflow area");
    }
    if (this->write_back_cache_) {
        CHECK(max_cache_page_count_ > 0, "Maximal cache page count not set");
        CHECK(max_cache_page_count_ >= page_locks_count_, "Illegal cache page count");

        if (max_cache_item_count_ == 0) {
            // not configured
            uint32_t factor = (page_size_ / 4 * (max_key_size_ + max_value_size_));
            max_cache_item_count_ = max_cache_page_count_ * factor;
            INFO("Auto configure cache maximal item count: " << max_cache_item_count_);
        }
        CHECK(max_cache_item_count_ >= max_cache_page_count_, "Illegal cache item count");
        CHECK(this->write_back_cache_->Start(start_context), "Failed to start write back cache");

        cache_lines_.resize(page_locks_count_);
        for (int i = 0; i < page_locks_count_; i++) {
            CacheLine* cache_line = new CacheLine(i, max_cache_page_count_ / page_locks_count_, max_cache_item_count_
                / page_locks_count_);
            cache_lines_[i] = cache_line;
        }
        statistics_.write_cache_free_page_count_ = max_cache_page_count_;
    }

    if (trans_system_) {
        CHECK(this->trans_system_->SetOption("page-size", ToString(this->page_size() * 2)),
            "Failed to set page size");

        // if files are created, there should be no restoring of old transactions
        // this is a hack because we cannot rely on the fact that the create mode is only use once or during restoring
        // operations
        CHECK(this->trans_system_->Start(start_context, !files_created), "Failed to start transaction system");
    }
    this->state_ = STARTED;
    return true;
}

uint64_t DiskHashIndex::GetBucket(const void* key, size_t key_size) {
    DCHECK_RETURN(key, 0, "Key not set");
    DCHECK_RETURN(bucket_count_, 0, "Bucket count not set");

    uint32_t hash_value = 0;
    murmur_hash3_x86_32(key, key_size, 0, &hash_value);
    return hash_value % this->bucket_count_;
}

File* DiskHashIndex::GetFile(uint32_t file_index) {
    DCHECK_RETURN(file_index < this->file_.size(), NULL,
        "Illegal file index: " << file_index << ", file count " << this->file_.size());
    return this->file_[file_index];
}

void DiskHashIndex::GetFileIndex(uint64_t bucket_id, uint32_t* file_index, uint32_t* cache_index) {
#ifndef NDEBUG
    if (file_.size() == 0) {
        ERROR("Illegal file size");
        return;
    }
    if (page_locks_count_ == 0) {
        ERROR("Illegal page lock count");
        return;
    }
#endif
    if (likely(file_index != NULL)) {
        *file_index = bucket_id % this->file_.size();
    }
    if (likely(cache_index != NULL)) {
        *cache_index = bucket_id % this->page_locks_count_;
    }
}

lookup_result DiskHashIndex::InternalLookup(const void* key, size_t key_size, Message* message,
                                            enum cache_lookup_method cache_lookup_type, enum cache_dirty_mode dirty_mode) {
    DCHECK_RETURN(key, LOOKUP_ERROR, "Key not set");
    DCHECK_RETURN(key_size <= this->max_key_size_, LOOKUP_ERROR, "Illegal key size: key size " << key_size);
    DCHECK_RETURN(cache_lookup_type != CACHE_LOOKUP_ONLY, LOOKUP_ERROR, "Cache only lookup handled in LookupDirtyOnly");
    CHECK_RETURN(this->state_ == STARTED, LOOKUP_ERROR, "Index not started");

    ProfileTimer timer(this->statistics_.lookup_time_);

    enum lookup_result result = LOOKUP_NOT_FOUND;
    unsigned int bucket_id = this->GetBucket(key, key_size);
    unsigned int file_index = 0;
    unsigned int cache_index = 0;
    this->GetFileIndex(bucket_id, &file_index, &cache_index);
    File* file = this->file_[file_index];
    CHECK_RETURN(file, LOOKUP_ERROR, "File is not open");

    DEBUG("Lookup entry " << ToHexString(key, key_size) <<
        ", dirty mode " << ToString(dirty_mode) <<
        ", bucket id " << bucket_id <<
        ", cache line " << cache_index);

    // We have to acquire a write lock here because we want to update the cache later
    ScopedReadWriteLock scoped_lock(this->page_locks_.Get(cache_index));
    CHECK_RETURN(scoped_lock.AcquireWriteLockWithStatistics(&this->statistics_.lock_free_,
            &this->statistics_.lock_busy_), LOOKUP_ERROR,
        "Lock failed: lock index " << cache_index << ", lock " << scoped_lock.DebugString());

    DiskHashCachePage cache_page(bucket_id, this->page_size_, max_key_size_, max_value_size_);
    lookup_result write_back_result = LOOKUP_NOT_FOUND;

    bool found_data_in_cache = false; // true iff we find data in cache (dirty or not)
    if (write_back_cache_) {
        // write cache is configured
        CacheLine* cache_line = cache_lines_[cache_index];
        write_back_result = ReadFromWriteBackCache(cache_line, &cache_page);
        CHECK_RETURN(write_back_result != LOOKUP_ERROR, LOOKUP_ERROR, "Failed to check write back cache: "
            << "key " << ToHexString(key, key_size));
        if (write_back_result == LOOKUP_FOUND) {
            // there is data from this page available in the cache
            bool is_dirty = false;
            result = cache_page.Search(key, key_size, message, &is_dirty, NULL);
            CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR,
                "Hash index page search failed : " <<
                "page " << cache_page.DebugString() <<
                ", key " << ToHexString(key, key_size) <<
                ", file index " << file_index);
            if (result == LOOKUP_FOUND && is_dirty && dirty_mode == CACHE_ONLY_CLEAN) {
                // we found the item, but it is dirty and we don't want this here
                result = LOOKUP_NOT_FOUND;
            }
            if (cache_lookup_type == CACHE_LOOKUP_BYPASS) {
                result = LOOKUP_NOT_FOUND;
            }

            // update the statistics
            if (result == LOOKUP_FOUND) {
                found_data_in_cache = true;
                statistics_.write_cache_hit_count_++;
            } else {
                statistics_.write_cache_miss_count_++;
            }
        }
    }
    // no cache available or we didn't find the key or it is dirty
    if (result == LOOKUP_NOT_FOUND) {

        // we read the real page from this.
        byte buffer[this->page_size_];
        memset(buffer, 0, this->page_size_);
        DiskHashPage page(this, bucket_id, buffer, this->page_size_);
        CHECK_RETURN(page.Read(file), LOOKUP_ERROR, "Hash index page read failed");
        result = page.Search(key, key_size, message);

        CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR,
            "Hash index page search failed : " <<
            "page " << page.DebugString() <<
            ", key " << ToHexString(key, key_size) <<
            ", file index " << file_index);

        // we cache the data as non-dirty if possible
        // Note: We don't overwrite dirty data here
        if (write_back_cache_ && !found_data_in_cache && result == LOOKUP_FOUND && message != NULL) {
            // We have this fresh from disk
            CacheLine* cache_line = cache_lines_[cache_index];
            cache_line->current_cache_item_count_++;
            cache_page.Update(key, key_size, *message, false, false, false);
            CHECK_RETURN(CopyToWriteBackCache(cache_line, &cache_page), LOOKUP_ERROR,
                "Failed to put data to write back cache");
        }
    }

    CHECK_RETURN(scoped_lock.ReleaseLock(), LOOKUP_ERROR, "Unlock failed");
    return result;
}

lookup_result DiskHashIndex::Lookup(const void* key, size_t key_size, Message* message) {
    return InternalLookup(key, key_size, message, CACHE_LOOKUP_DEFAULT, CACHE_ONLY_CLEAN);
}

lookup_result DiskHashIndex::LookupDirty(const void* key, size_t key_size, enum cache_lookup_method cache_lookup_type,
                                         enum cache_dirty_mode dirty_mode, Message* message) {
    if (cache_lookup_type == CACHE_LOOKUP_ONLY) {
        return LookupCacheOnly(key, key_size, dirty_mode, message);
    }
    return InternalLookup(key, key_size, message, cache_lookup_type, dirty_mode);
}

lookup_result DiskHashIndex::LookupCacheOnly(const void* key, size_t key_size, enum cache_dirty_mode dirty_mode,
                                             google::protobuf::Message* message) {
    DCHECK_RETURN(key, LOOKUP_ERROR, "Key not set");
    DCHECK_RETURN(key_size <= this->max_key_size_, LOOKUP_ERROR, "Illegal key size: key size " << key_size);
    CHECK_RETURN(this->state_ == STARTED, LOOKUP_ERROR, "Index not started");

    if (unlikely(!write_back_cache_)) {
        return LOOKUP_NOT_FOUND;
    }
    ProfileTimer timer(this->statistics_.lookup_time_);

    DEBUG("Lookup entry " << ToHexString(key, key_size) << ", cache only");
    unsigned int bucket_id = this->GetBucket(key, key_size);
    unsigned int cache_index = 0;
    this->GetFileIndex(bucket_id, NULL, &cache_index);
    CacheLine* cache_line = cache_lines_[cache_index];
    DiskHashCachePage cache_page(bucket_id, this->page_size_, max_key_size_, max_value_size_);

    ScopedReadWriteLock scoped_lock(this->page_locks_.Get(cache_index));
    CHECK_RETURN(scoped_lock.AcquireReadLockWithStatistics(&this->statistics_.lock_free_,
            &this->statistics_.lock_busy_), LOOKUP_ERROR,
        "Lock failed: lock index " << cache_index << ", lock " << scoped_lock.DebugString());

    lookup_result write_back_result = ReadFromWriteBackCache(cache_line, &cache_page);
    CHECK_RETURN(write_back_result != LOOKUP_ERROR, LOOKUP_ERROR, "Failed to check write back cache: "
        << "key " << ToHexString(key, key_size));

    lookup_result result = LOOKUP_NOT_FOUND;
    if (write_back_result == LOOKUP_FOUND) {
        bool is_dirty = false;
        result = cache_page.Search(key, key_size, message, &is_dirty, NULL);
        CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR,
            "Hash index page search failed : " <<
            "page " << cache_page.DebugString() <<
            ", key " << ToHexString(key, key_size));
        if (dirty_mode == CACHE_ONLY_CLEAN && is_dirty) {
            result = LOOKUP_NOT_FOUND;
        }

    }

    if (result == LOOKUP_FOUND) {
        statistics_.write_cache_hit_count_++;
    } else {
        statistics_.write_cache_miss_count_++;
    }
    return result;
}

bool DiskHashIndex::DropAllPinned() {
    DCHECK(this->state_ == STARTED, "Index not started");

    for (int cache_line_id = 0; cache_line_id < this->page_locks_count_; cache_line_id++) {
        ScopedReadWriteLock scoped_lock(this->page_locks_.Get(cache_line_id));
        CHECK(scoped_lock.AcquireWriteLockWithStatistics(&this->statistics_.lock_free_,
                &this->statistics_.lock_busy_), "Lock failed");

        CacheLine* cache_line = this->cache_lines_[cache_line_id];

        unordered_map<uint64_t, uint32_t>::iterator i;
        for (i = cache_line->cache_page_map_.begin(); i != cache_line->cache_page_map_.end(); i++) {
            uint64_t bucket_id = i->first;
            uint32_t cache_page_id = i->second;

            if (cache_line->bucket_dirty_state_[cache_page_id]) {
                DiskHashCachePage cache_page(bucket_id, page_size_, max_key_size_, max_value_size_);
                lookup_result cache_lr = ReadFromWriteBackCache(cache_line, &cache_page);
                CHECK(cache_lr != LOOKUP_ERROR,
                    "Failed to read page from cache: " << cache_page.DebugString());
                CHECK(cache_lr != LOOKUP_NOT_FOUND,
                    "The page should really be here: " << cache_page.DebugString());

                uint64_t dropped_item_count = 0;
                CHECK(cache_page.DropAllPinned(&dropped_item_count),
                    "Failed to drop all pinned items in page: " << cache_page.DebugString());

                this->dirty_item_count_ -= dropped_item_count;
                this->total_item_count_ -= dropped_item_count;
                if (dropped_item_count > 0) {
                  DEBUG("Dropped items from page: " <<
                      cache_page.DebugString() <<
                      ", dropped item count " << dropped_item_count <<
                      ", updated dirty item count " << dirty_item_count_ <<
                      ", update total item count " << total_item_count_);
                }
                CHECK_RETURN(CopyToWriteBackCache(cache_line, &cache_page), LOOKUP_ERROR,
                    "Failed to put data to write back cache");
            } else {
                // a clean page cannot have a pinned item
            }
        }
    }
    return true;
}

bool DiskHashIndex::PersistAllDirty() {
    DCHECK(this->state_ == STARTED, "Index not started");

    for (int cache_line_id = 0; cache_line_id < this->page_locks_count_; cache_line_id++) {
        ScopedReadWriteLock scoped_lock(this->page_locks_.Get(cache_line_id));
        CHECK(scoped_lock.AcquireWriteLockWithStatistics(&this->statistics_.lock_free_,
                &this->statistics_.lock_busy_), "Lock failed");

        CacheLine* cache_line = this->cache_lines_[cache_line_id];

        unordered_map<uint64_t, uint32_t>::iterator i;
        for (i = cache_line->cache_page_map_.begin(); i != cache_line->cache_page_map_.end(); i++) {
            uint64_t bucket_id = i->first;
            uint32_t cache_page_id = i->second;

            if (cache_line->bucket_dirty_state_[cache_page_id]) {
                byte buffer[this->page_size_];
                memset(buffer, 0, this->page_size_);

                unsigned int file_index = 0;
                this->GetFileIndex(bucket_id, &file_index, NULL);
                File* file = this->file_[file_index];

                DiskHashPage page(this, bucket_id, buffer, this->page_size_);
                DiskHashCachePage cache_page(bucket_id, page_size_, max_key_size_, max_value_size_);

                ProfileTimer page_timer(this->statistics_.update_time_page_read_);
                CHECK(page.Read(file), "Hash index page read failed: " << page.DebugString());
                page_timer.stop();

                DiskHashIndexTransaction transaction(this->trans_system_, page);

                lookup_result write_back_result = ReadFromWriteBackCache(cache_line, &cache_page);
                CHECK(write_back_result == LOOKUP_FOUND, "Failed to check write back cache: "
                    << cache_page.DebugString());

                uint32_t pinned_item_count = 0;
                uint32_t merged_item_count = 0;
                uint32_t merged_new_item_count = 0;

                CHECK(page.MergeWithCache(&cache_page,
                        &pinned_item_count,
                        &merged_item_count,
                        &merged_new_item_count), "Failed to merge with cache: " << page.DebugString());

                CHECK(transaction.Start(file_index, page), "Failed to start transaction: " << page.DebugString());

                CHECK(page.Write(file), "Hash index page write failed: " << page.DebugString());
                CHECK(transaction.Commit(), "Commit failed");

                statistics_.write_cache_persisted_page_count_++;
                CHECK(CopyToWriteBackCache(cache_line, &cache_page),
                    "Failed to put data to write back cache: " << page.DebugString());
            } else {
                // a clean page cannot have a pinned item
            }
        }
    }

    return true;
}

bool DiskHashIndex::DoSyncFile(int file_index) {
    DCHECK(this->state_ == STARTED, "Index not started");

    if (this->to_sync_flag_[file_index] == IN_SYNC) {
        ProfileTimer timer(this->statistics_.sync_wait_time_);
        this->statistics_.sync_wait_count_++;

        TRACE("Wait until sync is done: file " << file_index);
        ScopedReadWriteLock scoped_lock(&this->to_sync_lock_[file_index]);
        CHECK(scoped_lock.AcquireReadLock(), "Failed to acquire read lock: file index " << file_index);
        // we wait until the sync is done
        TRACE("Waiting finished file: file " << file_index);
    } else if (this->to_sync_flag_[file_index].compare_and_swap(IN_SYNC, DIRTY) == DIRTY) {
        ProfileTimer timer(this->statistics_.sync_time_);
        this->statistics_.sync_count_++;

        ScopedReadWriteLock scoped_lock(&this->to_sync_lock_[file_index]);
        CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire write lock: file index " << file_index);
        TRACE("Sync file: file " << file_index);
        CHECK(file_[file_index]->Sync(), "Failed to sync disk hash index: file " << file_[file_index]->path());

        // if no one changed the state in between
        // we are not clean
        this->to_sync_flag_[file_index].compare_and_swap(CLEAN, IN_SYNC);
        TRACE("Synced file: file " << file_index);

    }
    return true;
}

put_result DiskHashIndex::InternalPut(const void* key, size_t key_size, const Message& message, bool keep) {
    DCHECK_RETURN(key_size <= this->max_key_size_, PUT_ERROR, "Key size > Max key size");
    CHECK_RETURN(this->state_ == STARTED, PUT_ERROR, "Index not started");

    ProfileTimer timer(this->statistics_.update_time_);

    unsigned int bucket_id = this->GetBucket(key, key_size);
    unsigned int file_index = 0;
    unsigned cache_index = 0;
    this->GetFileIndex(bucket_id, &file_index, &cache_index);
    File* file = this->file_[file_index];
    DCHECK_RETURN(file, PUT_ERROR, "File not set");

    DEBUG("Put item: key " << ToHexString(key, key_size) <<
        ", keep " << ToString(keep) <<
        ", bucket id " << bucket_id <<
        ", cache line " << cache_index);

    byte buffer[this->page_size_];
    memset(buffer, 0, this->page_size_);

    DiskHashPage page(this, bucket_id, buffer, this->page_size_);
    DiskHashCachePage cache_page(bucket_id, this->page_size_, max_key_size_, max_value_size_);

    ProfileTimer lock_timer(this->statistics_.update_time_lock_wait_);
    ScopedReadWriteLock scoped_lock(this->page_locks_.Get(cache_index));
    CHECK_RETURN(scoped_lock.AcquireWriteLockWithStatistics(&this->statistics_.lock_free_,
            &this->statistics_.lock_busy_), PUT_ERROR, "Lock failed");
    lock_timer.stop();

    ProfileTimer page_timer(this->statistics_.update_time_page_read_);
    CHECK_RETURN(page.Read(file), PUT_ERROR,
        "Hash index page read failed: " << page.DebugString() <<
        "key " << ToHexString(key, key_size));
    page_timer.stop();

    DiskHashIndexTransaction transaction(this->trans_system_, page);
    lookup_result write_back_result = LOOKUP_NOT_FOUND;
    enum put_result result = PUT_ERROR;
    bool item_added_to_cache = false;
    if (write_back_cache_) {
        CacheLine* cache_line = cache_lines_[cache_index];

        ProfileTimer cache_read_timer(this->statistics_.update_time_cache_read_);
        write_back_result = ReadFromWriteBackCache(cache_line, &cache_page);
        CHECK_RETURN(write_back_result != LOOKUP_ERROR, PUT_ERROR, "Failed to check write back cache: "
            << "key " << ToHexString(key, key_size));
        cache_read_timer.stop();

        int item_count_before = cache_page.item_count();
        result = cache_page.Update(key, key_size, message, keep, true, false);

        if (cache_page.item_count() > item_count_before) {
            total_item_count_++;
        }

        CHECK_RETURN(result, PUT_ERROR, "Cache page update failed");
        if (result == PUT_OK) {
            // if result is PUT_KEEP, we skip merging it right here
            uint32_t merged_new_item_count = 0;

            CHECK_RETURN(page.MergeWithCache(&cache_page,
                    NULL,
                    NULL,
                    &merged_new_item_count), PUT_ERROR,
                "Failed to merge with cache: " << page.DebugString());

            // item counts are updated in page.Update
            if (merged_new_item_count > 0) {
                item_added_to_cache = true;
            }
        }
    } else {
        result = page.Update(key, key_size, message, keep);
        CHECK_RETURN(result, PUT_ERROR, "Hash index page update failed");
    }

    if (result != PUT_KEEP) {
        // Only write it to disk if we changed something
        ProfileTimer transaction_start_timer(this->statistics_.update_time_transaction_start_);
        CHECK_RETURN(transaction.Start(file_index, page), PUT_ERROR,
            "Failed to start transaction: " <<
            "key " << ToHexString(key, key_size) << ", message " << message.ShortDebugString());
        transaction_start_timer.stop();

        ProfileTimer page_write_timer(this->statistics_.update_time_page_write_);
        CHECK_RETURN(page.Write(file), PUT_ERROR, "Hash index page write failed: " <<
            "key " << ToHexString(key, key_size) << ", message " << message.ShortDebugString());

        ProfileTimer commit_timer(this->statistics_.update_time_commit_);
        CHECK_RETURN(transaction.Commit(), PUT_ERROR, "Commit failed: " <<
            "key " << ToHexString(key, key_size) << ", message " << message.ShortDebugString());
        commit_timer.stop();

        if (write_back_cache_) {
            CacheLine* cache_line = cache_lines_[cache_index];
            if (item_added_to_cache) {
                cache_line->current_cache_item_count_++;
            }
            statistics_.write_cache_persisted_page_count_++;
            CHECK_RETURN(CopyToWriteBackCache(cache_line, &cache_page), PUT_ERROR,
                "Failed to put data to write back cache");
        }
    }

    CHECK_RETURN(scoped_lock.ReleaseLock(), PUT_ERROR, "Unlock failed: " <<
        "key " << ToHexString(key, key_size) << ", message " << message.ShortDebugString());
    return result;
}

put_result DiskHashIndex::Put(const void* key, size_t key_size, const Message& message) {
    return InternalPut(key, key_size, message, false);
}

put_result DiskHashIndex::PutIfAbsent(const void* key, size_t key_size, const Message& message) {
    return InternalPut(key, key_size, message, true);
}

enum lookup_result DiskHashIndex::ChangePinningState(const void* key, size_t key_size, bool new_pin_state) {
    CHECK_RETURN(this->state_ == STARTED, LOOKUP_ERROR, "Index not started");
    CHECK_RETURN(write_back_cache_ != NULL, LOOKUP_ERROR, "Pinning not supported without write cache");

    CHECK_RETURN(key_size <= this->max_key_size_, LOOKUP_ERROR, "Key size > Max key size");

    unsigned int bucket_id = this->GetBucket(key, key_size);
    unsigned cache_index = 0;
    this->GetFileIndex(bucket_id, NULL, &cache_index);
    CacheLine* cache_line = cache_lines_[cache_index];

    DEBUG("Pin: key " << ToHexString(key, key_size) <<
        ", bucket id " << bucket_id <<
        ", cache line id " << cache_index <<
        ", new pin state " << ToString(new_pin_state));

    DiskHashCachePage cache_page(bucket_id, page_size_, max_key_size_, max_value_size_);
    ScopedReadWriteLock scoped_lock(this->page_locks_.Get(cache_index));
    CHECK_RETURN(scoped_lock.AcquireWriteLockWithStatistics(&this->statistics_.lock_free_,
            &this->statistics_.lock_busy_), LOOKUP_ERROR, "Lock failed: page lock " << cache_index);

    lookup_result write_back_check_result = IsWriteBackPageDirty(bucket_id);
    CHECK_RETURN(write_back_check_result != LOOKUP_ERROR, LOOKUP_ERROR,
        "Failed to check write back cache: "
        << "key " << ToHexString(key, key_size));
    if (write_back_check_result == LOOKUP_NOT_FOUND) {
        // not dirty
        TRACE("Change Pinning State not possible: " <<
            "key " << ToHexString(key, key_size) << " is not dirty.");
        return LOOKUP_NOT_FOUND;
    }

    lookup_result write_back_result = ReadFromWriteBackCache(cache_line, &cache_page);
    CHECK_RETURN(write_back_result == LOOKUP_FOUND, LOOKUP_ERROR,
        "Failed to check write back cache: "
        << "key " << ToHexString(key, key_size));

    lookup_result lr = cache_page.ChangePinningState(key, key_size, new_pin_state);
    CHECK_RETURN(lr != LOOKUP_ERROR, LOOKUP_ERROR, "Failed to merge with cache");

    if (lr == LOOKUP_FOUND) {
        CHECK_RETURN(CopyToWriteBackCache(cache_line, &cache_page), LOOKUP_ERROR,
            "Failed to put data to write back cache");
    } else if (lr == LOOKUP_NOT_FOUND) {
        TRACE("Pinning skipped, key not found: " <<
          "key " << ToHexString(key, key_size) <<
          ", bucket id " << bucket_id <<
          ", cache line id " << cache_index <<
          ", new pin state " << ToString(new_pin_state));
    }

    CHECK_RETURN(scoped_lock.ReleaseLock(), LOOKUP_ERROR, "Unlock failed");
    return lr;
}

put_result DiskHashIndex::EnsurePersistent(const void* key,
    size_t key_size,
    bool* pinned) {
    CHECK_RETURN(this->state_ == STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key_size <= this->max_key_size_, PUT_ERROR, "Key size > Max key size");

    if (write_back_cache_ == NULL) {
        // if the write back cache is not configured, every write is persistent
        return PUT_KEEP;
    }

    unsigned int bucket_id = this->GetBucket(key, key_size);
    unsigned int file_index = 0;
    unsigned cache_index = 0;
    this->GetFileIndex(bucket_id, &file_index, &cache_index);
    File* file = this->file_[file_index];
    CacheLine* cache_line = cache_lines_[cache_index];

    DEBUG("Ensure persistence: key " << ToHexString(key, key_size) <<
        ", bucket id " << bucket_id <<
        ", cache line id " << cache_index);

    byte buffer[this->page_size_];
    memset(buffer, 0, this->page_size_);

    DiskHashPage page(this, bucket_id, buffer, this->page_size_);
    DiskHashCachePage cache_page(bucket_id, page_size_, max_key_size_, max_value_size_);
    ScopedReadWriteLock scoped_lock(this->page_locks_.Get(cache_index));
    CHECK_RETURN(scoped_lock.AcquireWriteLockWithStatistics(&this->statistics_.lock_free_,
            &this->statistics_.lock_busy_), PUT_ERROR,
        "Lock failed: page lock " << cache_index);

    lookup_result write_back_check_result = IsWriteBackPageDirty(bucket_id);
    CHECK_RETURN(write_back_check_result != LOOKUP_ERROR, PUT_ERROR,
        "Failed to check write back cache: "
        << "key " << ToHexString(key, key_size));
    if (write_back_check_result == LOOKUP_NOT_FOUND) {
        // not dirty
        TRACE("Cache page not dirty: bucket id " << bucket_id);
        return PUT_KEEP;
    }

    lookup_result write_back_result = ReadFromWriteBackCache(cache_line, &cache_page);
    CHECK_RETURN(write_back_result == LOOKUP_FOUND, PUT_ERROR,
        "Failed to check write back cache: "
        << "key " << ToHexString(key, key_size));

    bool is_dirty = false;
    bool is_pinned = false;
    lookup_result search_result = cache_page.Search(
        key, key_size, NULL, &is_dirty, &is_pinned);
    CHECK_RETURN(search_result != LOOKUP_ERROR, PUT_ERROR,
        "Failed to search cache page: " << cache_page.DebugString());
    if (search_result == LOOKUP_NOT_FOUND) {
        // we don't have a dirty version of the item
        TRACE("Cache item is not in dirty cache page: " <<
            cache_page.DebugString() <<
            ", key " << ToHexString(key, key_size));
        return PUT_KEEP;
    }
    if (!is_dirty) {
        // we don't have a dirty version of the item
        TRACE("Cache item is not dirty: " << cache_page.DebugString() <<
            ", key " << ToHexString(key, key_size));
        return PUT_KEEP;
    }
    if (is_pinned) {
        TRACE("Cache item still pinned: " << cache_page.DebugString() <<
            ", key " << ToHexString(key, key_size));
        if (pinned) {
            *pinned = true;
        }
        return PUT_KEEP;
    }

    ProfileTimer page_timer(this->statistics_.update_time_page_read_);
    CHECK_RETURN(page.Read(file), PUT_ERROR,
        "Hash index page read failed: " << page.DebugString());
    page_timer.stop();

    DiskHashIndexTransaction transaction(this->trans_system_, page);

    uint32_t pinned_item_count = 0;
    uint32_t merged_item_count = 0;
    uint32_t merged_new_item_count = 0;

    CHECK_RETURN(page.MergeWithCache(&cache_page,
            &pinned_item_count,
            &merged_item_count,
            &merged_new_item_count), PUT_ERROR,
        "Failed to merge with cache: " << page.DebugString());

    dirty_item_count_ -= merged_new_item_count;

    CHECK_RETURN(transaction.Start(file_index, page), PUT_ERROR,
        "Failed to start transaction: " <<
        "key " << ToHexString(key, key_size) <<
        ", page item count " << page.item_count());

    CHECK_RETURN(page.Write(file), PUT_ERROR,
        "Hash index page write failed: " <<
        "key " << ToHexString(key, key_size));
    CHECK_RETURN(transaction.Commit(), PUT_ERROR, "Commit failed");

    statistics_.write_cache_persisted_page_count_++;
    CHECK_RETURN(CopyToWriteBackCache(cache_line, &cache_page), PUT_ERROR,
        "Failed to put data to write back cache");

    CHECK_RETURN(scoped_lock.ReleaseLock(), PUT_ERROR, "Unlock failed");
    if (pinned) {
        *pinned = false;
    }
    return PUT_OK;
}

put_result DiskHashIndex::PutDirty(const void* key, size_t key_size, const Message& message, bool pin) {
    DCHECK_RETURN(key_size <= this->max_key_size_, PUT_ERROR, "Key size > Max key size");
    CHECK_RETURN(this->state_ == STARTED, PUT_ERROR, "Index not started");

    ProfileTimer timer(this->statistics_.update_time_);
    if (unlikely(write_back_cache_ == NULL)) {
        // if the write back cache is not configured, do it the old way.
        return Put(key, key_size, message);
    }
    unsigned int bucket_id = this->GetBucket(key, key_size);
    unsigned cache_index = 0;
    this->GetFileIndex(bucket_id, NULL, &cache_index);
    CacheLine* cache_line = cache_lines_[cache_index];

    DEBUG("Put item (dirty): key " << ToHexString(key, key_size) <<
        ", pin " << ToString(pin) <<
        ", bucket id " << bucket_id <<
        ", cache line " << cache_index);

    DiskHashCachePage cache_page(bucket_id, this->page_size_, max_key_size_, max_value_size_);

    ProfileTimer lock_timer(this->statistics_.update_time_lock_wait_);
    ScopedReadWriteLock scoped_lock(this->page_locks_.Get(cache_index));
    CHECK_RETURN(scoped_lock.AcquireWriteLockWithStatistics(&this->statistics_.lock_free_,
            &this->statistics_.lock_busy_), PUT_ERROR, "Lock failed: page lock " << cache_index);
    lock_timer.stop();

    ProfileTimer cache_read_timer(this->statistics_.update_time_cache_read_);
    lookup_result write_back_result = ReadFromWriteBackCache(cache_line, &cache_page);
    CHECK_RETURN(write_back_result != LOOKUP_ERROR, PUT_ERROR,
        "Failed to check write back cache: " <<
        "key " << ToHexString(key, key_size) <<
        ", pin " << ToString(pin) <<
        ", bucket id " << bucket_id <<
        ", cache line " << cache_index);
    cache_read_timer.stop();

    bool was_dirty = cache_page.is_dirty();
    uint32_t item_count_before = cache_page.item_count();
    put_result result = cache_page.Update(key, key_size, message, false, true, pin);
    CHECK_RETURN(result, PUT_ERROR, "Hash index page update failed: " <<
        "key " << ToHexString(key, key_size) <<
        ", pin " << ToString(pin) <<
        ", bucket id " << bucket_id <<
        ", cache line " << cache_index);

    if (item_count_before != cache_page.item_count()) {
        cache_line->current_cache_item_count_++;
        dirty_item_count_++;
        total_item_count_++;
    } else if (!was_dirty) {
        // an old non-dirty entry was overridden
        dirty_item_count_++;
    }

    CHECK_RETURN(CopyToWriteBackCache(cache_line, &cache_page), PUT_ERROR,
        "Failed to put data to write back cache: key " << ToHexString(key, key_size) <<
        ", pin " << ToString(pin) <<
        ", bucket id " << bucket_id <<
        ", cache line " << cache_index);
    CHECK_RETURN(scoped_lock.ReleaseLock(), PUT_ERROR, "Unlock failed");
    return result;
}

delete_result DiskHashIndex::Delete(const void* key, size_t key_size) {
    ProfileTimer timer(this->statistics_.delete_time_);
    unsigned int file_index = 0, cache_index = 0;

    CHECK_RETURN(this->state_ == STARTED, DELETE_ERROR, "Index not started");
    CHECK_RETURN(key_size <= this->max_key_size_, DELETE_ERROR, "Illegal key size");

    unsigned int bucket_id = this->GetBucket(key, key_size);
    this->GetFileIndex(bucket_id, &file_index, &cache_index);
    File* file = this->file_[file_index];

    byte buffer[this->page_size_];
    memset(buffer, 0, this->page_size_);

    DiskHashPage page(this, bucket_id, buffer, this->page_size_);
    DiskHashCachePage cache_page(bucket_id, page_size_, max_key_size_, max_value_size_);

    ScopedReadWriteLock scoped_lock(this->page_locks_.Get(cache_index));
    CHECK_RETURN(scoped_lock.AcquireWriteLockWithStatistics(&this->statistics_.lock_free_,
            &this->statistics_.lock_busy_), DELETE_ERROR, "Lock failed");

    // we need to read back to old page from disk for the transaction system
    CHECK_RETURN(page.Read(file), DELETE_ERROR, "Hash index page read failed");

    DiskHashIndexTransaction transaction(this->trans_system_, page);

    lookup_result write_back_result = LOOKUP_NOT_FOUND;
    delete_result result;
    if (write_back_cache_) {
        CacheLine* cache_line = cache_lines_[cache_index];

        write_back_result = ReadFromWriteBackCache(cache_line, &cache_page);
        CHECK_RETURN(write_back_result != LOOKUP_ERROR, DELETE_ERROR, "Failed to check write back cache: "
            << "key " << ToHexString(key, key_size));

        delete_result cache_delete_result = DELETE_NOT_FOUND;
        if (write_back_result == LOOKUP_FOUND) {
            cache_delete_result = cache_page.Delete(key, key_size);
            CHECK_RETURN(cache_delete_result != DELETE_ERROR, DELETE_ERROR,
                "Failed to delete key from cache page");

            if (cache_page.item_count() > 0) {
                uint32_t pinned_item_count = 0;
                uint32_t merged_item_count = 0;
                uint32_t merged_new_item_count = 0;

                CHECK_RETURN(page.MergeWithCache(&cache_page,
                        &pinned_item_count,
                        &merged_item_count,
                        &merged_new_item_count), DELETE_ERROR,
                    "Failed to merge with cache: " << page.DebugString());

                dirty_item_count_ -= merged_new_item_count;
            }
        }

        result = page.Delete(key, key_size);
        CHECK_RETURN(result != DELETE_ERROR, DELETE_ERROR,
            "Hash index page delete failed");
        CHECK_RETURN(transaction.Start(file_index, page), DELETE_ERROR,
            "Failed to start transaction");

        CHECK_RETURN(page.Write(file), DELETE_ERROR, "Hash index page write failed");
        CHECK_RETURN(transaction.Commit(), DELETE_ERROR, "Commit failed");

        if (cache_delete_result == DELETE_OK) {
            cache_line->current_cache_item_count_--;
        }
        CHECK_RETURN(CopyToWriteBackCache(cache_line, &cache_page), DELETE_ERROR,
            "Failed to put data to write back cache");
    } else {
        result = page.Delete(key, key_size);
        CHECK_RETURN(result != DELETE_ERROR, DELETE_ERROR,
            "Hash index page delete failed");
        CHECK_RETURN(transaction.Start(file_index, page), DELETE_ERROR,
            "Failed to start transaction");

        CHECK_RETURN(page.Write(file), DELETE_ERROR, "Hash index page write failed");
        CHECK_RETURN(transaction.Commit(), DELETE_ERROR, "Commit failed");
    }
    CHECK_RETURN(scoped_lock.ReleaseLock(), DELETE_ERROR, "Unlock failed");
    return result;
}

lookup_result DiskHashIndex::IsWriteBackPageDirty(uint64_t bucket_id) {
    DCHECK_RETURN(write_back_cache_, LOOKUP_ERROR, "Write back cache not set");

    uint32_t cache_line_id = 0;
    GetFileIndex(bucket_id, NULL, &cache_line_id);
    // We can access to lock because we aligned cache lines with the page locks
    CacheLine* cache_line = cache_lines_[cache_line_id];
    DCHECK_RETURN(cache_line, LOOKUP_ERROR, "Cache line not set");

    unordered_map<uint64_t, uint32_t>::iterator i = cache_line->cache_page_map_.find(bucket_id);
    if (i == cache_line->cache_page_map_.end()) {
        TRACE("Check dirty state: " <<
            "cache line id " << cache_line_id <<
            ", bucket id " << bucket_id <<
            ", bucket id not found");
        return LOOKUP_NOT_FOUND;
    }
    uint32_t cache_id = i->second;
    bool d = cache_line->bucket_dirty_state_[cache_id];
    if (d) {
        TRACE("Check dirty state: bucket id " << bucket_id <<
            ", cache line id " << cache_line_id <<
            ", cache id " << cache_id <<
            ", dirty true");
        return LOOKUP_FOUND;
    }
    TRACE("Check dirty state: bucket id " << bucket_id <<
        ", cache line id " << cache_line_id <<
        ", cache id " << cache_id <<
        ", dirty false");
    return LOOKUP_NOT_FOUND;
}

lookup_result DiskHashIndex::ReadFromWriteBackCache(CacheLine* cache_line, DiskHashCachePage* page) {
    ProfileTimer timer(this->statistics_.write_cache_read_time_);
    DCHECK_RETURN(cache_line, LOOKUP_ERROR, "Cache line not set");
    DCHECK_RETURN(page, LOOKUP_ERROR, "Page not set");
    DCHECK_RETURN(write_back_cache_, LOOKUP_ERROR, "Write back cache not set");

    unordered_map<uint64_t, uint32_t>::iterator i = cache_line->cache_page_map_.find(page->bucket_id());
    if (i == cache_line->cache_page_map_.end()) {
        TRACE("Page not found in cache: " <<
            "page " << page->DebugString() <<
            ", cache line id " << cache_line->cache_line_id_);

        return LOOKUP_NOT_FOUND;
        // lock auto released
    }
    uint32_t cache_id = i->second;
    uint64_t cache_map_id = cache_line->GetCacheMapId(cache_id);

    size_t buf_size = page->raw_buffer_size();
    lookup_result lr = write_back_cache_->RawLookup(&cache_map_id, sizeof(cache_map_id), page->mutable_raw_buffer(),
        &buf_size);
    if (lr == LOOKUP_ERROR && buf_size > page->raw_buffer_size()) {
        // the buffer was to small
        page->RaiseBuffer(buf_size);
        lr = write_back_cache_->RawLookup(&cache_map_id, sizeof(cache_map_id), page->mutable_raw_buffer(), &buf_size);
    }
    CHECK_RETURN(lr != LOOKUP_ERROR, LOOKUP_ERROR, "Failed to lookup page in write-back cache: " << page->DebugString());
    DCHECK_RETURN(lr == LOOKUP_FOUND, LOOKUP_ERROR,
        "Failed to find page: " <<
        "cache map id " << cache_map_id <<
        ", cache line id " << cache_line->cache_line_id_ <<
        ", cache id " << cache_id <<
        ", bucket id " << page->bucket_id());
    page->ParseData();

    page->set_dirty(cache_line->bucket_dirty_state_[cache_id]);
    page->set_pinned(cache_line->bucket_pinned_state_[cache_id]);
    cache_line->bucket_cache_state_[cache_id] = true; // it is used

    TRACE("Read page from cache: " <<
        "page " << page->DebugString() <<
        ", cache map id " << cache_map_id <<
        ", cache line id " << cache_line->cache_line_id_ <<
        ", cache id " << cache_id);

    return LOOKUP_FOUND;
}

bool DiskHashIndex::WriteBackCachePage(CacheLine* cache_line, DiskHashCachePage* cache_page) {
    DCHECK(cache_line, "Cache line not set");
    DCHECK(cache_page, "Cache page not set");

    uint32_t file_index = 0;
    this->GetFileIndex(cache_page->bucket_id(), &file_index, NULL);
    File* file = this->file_[file_index];
    DCHECK(file, "File not set");

    /**
     * one buffer for the on disk page, one buffer for the case
     */
    byte* page_buffer = new byte[this->page_size_];
    memset(page_buffer, 0, this->page_size_);
    ScopedArray<byte> scoped_page_buffer(page_buffer);

    DiskHashPage page(this, cache_page->bucket_id(), page_buffer, this->page_size_);
    ProfileTimer page_timer(this->statistics_.update_time_page_read_);
    CHECK(page.Read(file), "Hash index page read failed: " << page.DebugString());
    page_timer.stop();

    DiskHashIndexTransaction transaction(this->trans_system_, page);

    uint32_t pinned_item_count = 0;
    uint32_t merged_item_count = 0;
    uint32_t merged_new_item_count = 0;

    CHECK(page.MergeWithCache(cache_page,
            &pinned_item_count,
            &merged_item_count,
            &merged_new_item_count), "Failed to merge with cache: " << page.DebugString());

    dirty_item_count_ -= merged_new_item_count;

    ProfileTimer transaction_start_timer(this->statistics_.update_time_transaction_start_);
    CHECK(transaction.Start(file_index, page),
        "Failed to start transaction");
    transaction_start_timer.stop();

    ProfileTimer page_write_timer(this->statistics_.update_time_page_write_);
    CHECK(page.Write(file), "Hash index page write failed");

    ProfileTimer commit_timer(this->statistics_.update_time_commit_);
    CHECK(transaction.Commit(), "Commit failed");

    statistics_.write_cache_persisted_page_count_++;
    if (!cache_page->is_dirty() && merged_item_count > 0) {
        // remove this page from the dirty page tree
        ClearBucketDirtyState(cache_page->bucket_id());
    }

    return true;
}

void DiskHashIndex::MarkBucketAsDirty(uint64_t bucket_id,
                                      uint32_t cache_line_id,
                                      uint32_t cache_id) {
    TRACE("Mark bucket as dirty: bucket " << bucket_id);
    spin_mutex::scoped_lock l(dirty_page_map_lock_);

    dirty_page_map_[bucket_id] = make_pair(cache_line_id, cache_id);
}

void DiskHashIndex::ClearBucketDirtyState(uint64_t bucket_id) {
    TRACE("Clear bucket dirty state: bucket " << bucket_id);
    spin_mutex::scoped_lock l(dirty_page_map_lock_);

    dirty_page_map_.erase(bucket_id);
}

bool DiskHashIndex::IsBucketDirty(uint64_t bucket_id, uint32_t* cache_line_id, uint32_t* cache_id) {
    spin_mutex::scoped_lock l(dirty_page_map_lock_);

    std::map<uint64_t, pair<uint32_t, uint32_t> >::iterator i;
    i = dirty_page_map_.find(bucket_id);
    if (i != dirty_page_map_.end()) {
        if (cache_line_id) {
            *cache_line_id = i->second.first;
        }
        if (cache_id) {
            *cache_id = i->second.second;
        }
        return true;
    } else {
        return false;
    }
}

bool DiskHashIndex::GetNextDirtyBucket(uint64_t current_bucket_id,
                                       uint64_t* next_bucket_id,
                                       uint32_t* cache_line_id,
                                       uint32_t* cache_id)
{
    spin_mutex::scoped_lock l(dirty_page_map_lock_);
    if (dirty_page_map_.empty()) {
        return false;
    }
    std::map<uint64_t, pair<uint32_t, uint32_t> >::iterator i;

    i = dirty_page_map_.upper_bound(current_bucket_id);
    if (i == dirty_page_map_.end()) {
        // no higher bucket => wrap
        i = dirty_page_map_.begin();
    }
    if (next_bucket_id) {
        *next_bucket_id = i->first;
    }
    if (cache_line_id) {
        *cache_line_id = i->second.first;
    }
    if (cache_id) {
        *cache_id = i->second.second;
    }
    return true;
}

bool DiskHashIndex::TryPersistDirtyItem(uint32_t max_batch_size,
                                        uint64_t* resume_handle,
                                        bool* persisted) {
    DCHECK(persisted, "Persisted not set");

    if (write_back_cache_ == NULL) {
        // if the write back cache is not configured, every write is persistent
        return PUT_KEEP;
    }

    uint64_t dirty_bucket_id = 0;
    if (resume_handle) {
        dirty_bucket_id = *resume_handle;
    }
    for (int i = 0; i < max_batch_size; i++) {
        uint32_t cache_line_id;
        uint32_t cache_id;
        if (!GetNextDirtyBucket(dirty_bucket_id, &dirty_bucket_id, &cache_line_id,
                &cache_id)) {
            // no dirty bucket
            DEBUG("Found no dirty bucket");
            *persisted  = false;
            return true;
        }
        DEBUG("Found dirty bucket: bucket id " << dirty_bucket_id <<
            ", cache line id " << cache_line_id <<
            ", cache id " << cache_id);
        CacheLine* cache_line = cache_lines_[cache_line_id];

        ScopedReadWriteLock scoped_lock(this->page_locks_.Get(cache_line_id));
        CHECK(scoped_lock.AcquireWriteLockWithStatistics(&this->statistics_.lock_free_,
                &this->statistics_.lock_busy_), "Lock failed: page lock " << cache_line_id);

        DiskHashCachePage cache_page(0, page_size_, max_key_size_, max_value_size_);

        uint64_t cache_map_id = cache_line->GetCacheMapId(cache_id);
        size_t buf_size = cache_page.raw_buffer_size();
        lookup_result lr = write_back_cache_->RawLookup(&cache_map_id, sizeof(cache_map_id),
            cache_page.mutable_raw_buffer(), &buf_size);
        CHECK(lr != LOOKUP_ERROR, "Failed to lookup page in write-back cache: " << cache_map_id);
        DCHECK(lr == LOOKUP_FOUND, "Failed to find page: " <<
            "cache map id " << cache_map_id <<
            ", cache line id " << cache_line->cache_line_id_ <<
            ", cache id " << cache_id);
        cache_page.ParseData();
        cache_page.set_dirty(cache_line->bucket_dirty_state_[cache_id]);
        cache_page.set_pinned(cache_line->bucket_pinned_state_[cache_id]);
        TRACE("Found cache map entry: " << cache_page.DebugString() << ", size " << buf_size);

        if (cache_page.is_dirty()) {
            TRACE("Persist cache item: " <<
                "cache line id " << cache_line->cache_line_id_ <<
                ", cache id " << cache_id <<
                ", dirty item count " << dirty_item_count_);

            CHECK(WriteBackCachePage(cache_line, &cache_page), "Failed to write back cache page: " <<
                "cache line id " << cache_line->cache_line_id_ <<
                ", cache id " << cache_id <<
                ", cache page " << cache_page.DebugString());

            CHECK_RETURN(CopyToWriteBackCache(cache_line, &cache_page), PUT_ERROR,
                "Failed to put data to write back cache");
        }

        *persisted = true;
        CHECK(scoped_lock.ReleaseLock(), "Unlock failed");
    }
    if (resume_handle) {
        *resume_handle = dirty_bucket_id;
    }
    return true;
}

bool DiskHashIndex::EvictCacheItem(CacheLine* cache_line, uint32_t cache_id, bool dirty) {
    DCHECK(cache_line, "Cache line not set");

    DiskHashCachePage cache_page(0, page_size_, max_key_size_, max_value_size_);

    uint64_t cache_map_id = cache_line->GetCacheMapId(cache_id);
    size_t buf_size = cache_page.raw_buffer_size();
    lookup_result lr = write_back_cache_->RawLookup(&cache_map_id, sizeof(cache_map_id),
        cache_page.mutable_raw_buffer(), &buf_size);
    CHECK(lr != LOOKUP_ERROR, "Failed to lookup page in write-back cache: " << cache_map_id);
    DCHECK(lr == LOOKUP_FOUND, "Failed to find page: " <<
        "cache map id " << cache_map_id <<
        ", cache line id " << cache_line->cache_line_id_ <<
        ", cache id " << cache_id);
    cache_page.ParseData();
    cache_page.set_dirty(cache_line->bucket_dirty_state_[cache_id]);
    cache_page.set_pinned(cache_line->bucket_pinned_state_[cache_id]);
    TRACE("Found cache map entry: " << cache_page.DebugString() << ", size " << buf_size);

    uint64_t bucket_id = cache_page.bucket_id();

    TRACE("Evict cache item: " <<
        "cache line id " << cache_line->cache_line_id_ <<
        ", cache id " << cache_id <<
        ", bucket id " << bucket_id <<
        ", dirty " << ToString(dirty));
    if (dirty) {
        CHECK(WriteBackCachePage(cache_line, &cache_page), "Failed to write back cache page: " <<
            "cache line id " << cache_line->cache_line_id_ <<
            ", cache id " << cache_id <<
            ", cache page " << cache_page.DebugString());

        statistics_.write_cache_dirty_evict_count_++;
        statistics_.write_cache_dirty_page_count_--;
    }
    statistics_.write_cache_evict_count_++;

    CHECK(write_back_cache_->Delete(&cache_map_id, sizeof(cache_map_id)) != DELETE_ERROR,
        "Failed to evict page from write cache");

    cache_line->bucket_free_state_[cache_id] = true;
    cache_line->bucket_dirty_state_[cache_id] = false;
    cache_line->bucket_cache_state_[cache_id] = false;
    cache_line->bucket_cache_state2_[cache_id] = false;
    cache_line->bucket_pinned_state_[cache_id] = false;
    cache_line->current_cache_item_count_ -= cache_page.item_count();
    cache_line->cache_page_map_.erase(bucket_id);

    return true;
}

bool DiskHashIndex::IsWriteBackCacheEnabled() {
    if (write_back_cache_) {
        return true;
    }
    return false;
}

dedupv1::base::Option<bool> DiskHashIndex::CacheLine::SearchDirtyPage(uint32_t* cache_id) {
    DCHECK(cache_id, "Cache id not set");

    TRACE("Search dirty cache page: " <<
        "cache line id " << cache_line_id_ <<
        ", start victim id " << next_dirty_search_cache_victim_);
    uint32_t try_counter = max_cache_page_count_;
    uint32_t pinned_page_count = 0;
    for (;; ) {
        next_dirty_search_cache_victim_++;
        if (next_dirty_search_cache_victim_ == max_cache_page_count_) {
            next_dirty_search_cache_victim_ = 0;
        }
        if (bucket_pinned_state_[next_dirty_search_cache_victim_]) {
            pinned_page_count++;

            // we cannot evict pinned pages
            // There is simply no way. However, this results in serious problems.
            try_counter--;

            // We scanned the cache states four times and havn't found a page to use
            // There must be something wrong
            if (try_counter == 0) {
                TRACE("Checked all pages. Failed to find a page to evict: " <<
                    "cache line id " << DebugString() <<
                    ", end cache victim " << next_dirty_search_cache_victim_ <<
                    ", pinned page count " << pinned_page_count <<
                    ", max page count " << max_cache_page_count_);
                return make_option(false);
            }
        } else if (!bucket_dirty_state_[next_dirty_search_cache_victim_]) {
            // a clean page
            try_counter--;

            // We scanned the cache states four times and havn't found a page to use
            // There must be something wrong
            if (try_counter == 0) {
                TRACE("Checked all pages. Failed to find a page to evict: " <<
                    "cache line id " << DebugString() <<
                    ", end cache victim " << next_dirty_search_cache_victim_ <<
                    ", pinned page count " << pinned_page_count <<
                    ", max page count " << max_cache_page_count_);
                return make_option(false);
            }
        } else {
            TRACE("Found dirty cache id " << next_dirty_search_cache_victim_ <<
                ", bucket dirty state " << bucket_dirty_state_[next_dirty_search_cache_victim_] <<
                ", bucket pinned state " << bucket_pinned_state_[next_dirty_search_cache_victim_]);
            *cache_id = next_dirty_search_cache_victim_;
            return make_option(true);
        }
    }
    ERROR("Here are dragons");
    return false;
}

bool DiskHashIndex::CacheLine::SearchEvictPage(uint32_t* cache_id) {
    DCHECK(cache_id, "Cache id not set");

    TRACE("Search cache page to evict: " <<
        "cache line id " << cache_line_id_ <<
        ", start victim id " << next_cache_victim_);
    uint32_t try_counter = max_cache_page_count_;
    uint32_t pinned_page_count = 0;
    uint32_t ref_page_count = 0;
    uint32_t dirty_page_count = 0;
    for (;; ) {
        next_cache_victim_++;
        if (next_cache_victim_ == max_cache_page_count_) {
            next_cache_victim_ = 0;
        }
        TRACE("Search cache page for eviction: "
            "cache line id " << cache_line_id_ <<
            ", cache id " << next_cache_victim_ <<
            ", pin state " << ToString(bucket_pinned_state_[next_cache_victim_]) <<
            ", dirty state " << ToString(bucket_dirty_state_[next_cache_victim_]) <<
            ", cache state " << ToString(bucket_cache_state_[next_cache_victim_]) << "/" << ToString(bucket_cache_state2_[next_cache_victim_]));
        if (bucket_pinned_state_[next_cache_victim_]) {
            pinned_page_count++;

            // we cannot evict pinned pages
            // There is simply no way. However, this results in serious problems.
            try_counter--;

            // We scanned the cache states four times and havn't found a page to use
            // There must be something wrong
            CHECK(try_counter > 0,
                "Checked all pages. Failed to find a page to evict: " <<
                "cache line id " << DebugString() <<
                ", pinned page count " << pinned_page_count <<
                ", ref page count " << ref_page_count <<
                ", dirty page count " << dirty_page_count <<
                ", max page count " << max_cache_page_count_);
        } else {
            if (bucket_cache_state_[next_cache_victim_]) {
                // reset state
                ref_page_count++;
                if (bucket_cache_state2_[next_cache_victim_]) {
                    dirty_page_count++;
                    try_counter = max_cache_page_count_;
                    bucket_cache_state2_[next_cache_victim_] = false;
                    // this is dirty page. Defer marking the page as possible
                    // victim
                } else {
                    try_counter = max_cache_page_count_;
                    bucket_cache_state_[next_cache_victim_] = false;
                }

                // We scanned the cache states four times and havn't found a page to use
                // There must be something wrong
                CHECK(try_counter > 0,
                    "Checked all pages. Failed to find a page to evict: " <<
                    "cache line id " << DebugString() <<
                    ", pinned page count " << pinned_page_count <<
                    ", ref page count " << ref_page_count <<
                    ", dirty page count " << dirty_page_count <<
                    ", max page count " << max_cache_page_count_);
                try_counter--;
            } else {
                *cache_id = next_cache_victim_;
                return true;
            }
        }
    }
    ERROR("Here are dragons");
    return false;
}

bool DiskHashIndex::CacheLine::SearchFreePage(uint32_t* cache_id) {
    DCHECK(cache_id, "Cache id not set");

    TRACE("Search free page: " <<
        "cache line id " << cache_line_id_ <<
        ", start victim id " << next_cache_victim_ <<
        ", cache page count " << max_cache_page_count_);
    int32_t try_counter = max_cache_page_count_;
    for (;; ) {
        next_cache_victim_++;
        if (unlikely(next_cache_victim_ == max_cache_page_count_)) {
            next_cache_victim_ = 0;
        }
        TRACE("Search free page: " <<
            "victim " << next_cache_victim_ <<
            ", free state " << bucket_free_state_[next_cache_victim_]);

        if (bucket_free_state_[next_cache_victim_]) {
            // free => We found our place
            TRACE("Found free page: " <<
                "victim " << next_cache_victim_);
            *cache_id = next_cache_victim_;
            return true;
        } else {
            try_counter--;
            CHECK(try_counter >= 0,
                "Checked all pages. Failed to find free page: " <<
                "cache line id " << cache_line_id_);
        }
    }
    ERROR("Here are dragons");
    return false;
}

bool DiskHashIndex::CacheLine::IsCacheFull() {
    return (current_cache_page_count_ >= max_cache_page_count_) || (current_cache_item_count_ >= max_cache_item_count_);
}

bool DiskHashIndex::CopyToWriteBackCache(CacheLine* cache_line, internal::DiskHashCachePage* page) {
    ProfileTimer timer(this->statistics_.write_cache_update_time_);

    DCHECK(cache_line, "Cache line not set");
    DCHECK(page, "Page not set");
    DCHECK(write_back_cache_, "Write back cache not set");

    TRACE("Copy page to write-back cache: " <<
        "cache line id " << cache_line->DebugString() <<
        ", page " << page->DebugString());

    uint32_t cache_id = 0;
    std::tr1::unordered_map<uint64_t, uint32_t>::iterator i = cache_line->cache_page_map_.find(page->bucket_id());
    bool was_dirty = false;
    if (i == cache_line->cache_page_map_.end()) {
        // ok, it is not there yet
        TRACE("Page not found in cache: " <<
            "page " << page->DebugString() <<
            ", active cache items " << cache_line->current_cache_item_count_ <<
            ", active cache pages " << cache_line->current_cache_page_count_);

        // check if cache is full
        if (!cache_line->IsCacheFull()) {

            ProfileTimer timer(this->statistics_.cache_search_free_page_time_);
            // there are still free elements
            CHECK(cache_line->SearchFreePage(&cache_id),
                "Failed to find free page: " <<
                "cache line id " << cache_line->cache_line_id_ <<
                ", cache page count " << cache_line->current_cache_page_count_ <<
                ", max cache page count " << cache_line->max_cache_page_count_);
            cache_line->current_cache_page_count_++;

            statistics_.write_cache_free_page_count_--;
            statistics_.write_cache_used_page_count_++;
        } else {
            ProfileTimer evict_search_timer(this->statistics_.cache_search_evict_page_time_);
            CHECK(cache_line->SearchEvictPage(&cache_id),
                "Failed to find a page to evict: " <<
                "page " << page->DebugString() <<
                ", cache line id " << cache_line->cache_line_id_);
            evict_search_timer.stop();

            bool dirty = cache_line->bucket_dirty_state_[cache_id];
            CHECK(EvictCacheItem(cache_line, cache_id, dirty),
                "Failed to persist dirty cache page: " <<
                "page " << page->DebugString() <<
                ", cache line id " << cache_line->cache_line_id_ <<
                ", cache id " << cache_id);
            if (dirty) {
                was_dirty = true;
            }
        }
        cache_line->cache_page_map_[page->bucket_id()] = cache_id;
    } else {
        // page already in cache
        was_dirty = page->is_dirty();
        cache_id = i->second;
    }
    if (!was_dirty && page->is_dirty()) {
        statistics_.write_cache_dirty_page_count_++;
    } else if (was_dirty && !page->is_dirty()) {
        statistics_.write_cache_dirty_page_count_--;
    }
    cache_line->bucket_dirty_state_[cache_id] = page->is_dirty();
    cache_line->bucket_pinned_state_[cache_id] = page->is_pinned();
    cache_line->bucket_cache_state_[cache_id] = true; // it is used
    cache_line->bucket_cache_state2_[cache_id] = page->is_dirty(); // it is used
    cache_line->bucket_free_state_[cache_id] = false;

    uint64_t cache_map_id = cache_line->GetCacheMapId(cache_id);
    TRACE("Update cache: " <<
        page->DebugString() <<
        ", cache map id " << cache_map_id <<
        ", cache line id " << cache_line->cache_line_id_ <<
        ", cache id " << cache_id <<
        ", cache page size " << page->used_size() <<
        ", cache page was dirty " << ToHexString(was_dirty));

    page->Store();
    DCHECK(page->used_size() <= page_size_, "Illegal page used size: " <<
        page->DebugString() <<
        ", used size " << page->used_size() <<
        ", page size " << page_size_);
    put_result pr = write_back_cache_->RawPut(&cache_map_id, sizeof(cache_map_id), page->raw_buffer(),
        page->used_size());
    CHECK(pr != PUT_ERROR, "Failed to put write back page: " << page->DebugString());

    if (page->is_dirty() && page->is_pinned()) {
        MarkBucketAsDirty(page->bucket_id(), cache_line->cache_line_id_, cache_id);
    }
    return true;
}

uint64_t DiskHashIndex::CacheLine::GetCacheMapId(uint32_t cache_id) {
    uint64_t d = cache_line_id_;
    d = d << 32;
    d |= cache_id;
    return d;
}

DiskHashIndex::CacheLine::CacheLine(uint32_t cache_line_id, uint32_t cache_page_count, uint32_t cache_item_count) {
    cache_line_id_ = cache_line_id;
    max_cache_page_count_ = cache_page_count;
    max_cache_item_count_ = cache_item_count;
    current_cache_page_count_ = 0;
    current_cache_item_count_ = 0;
    bucket_cache_state_.resize(max_cache_page_count_);
    bucket_cache_state2_.resize(max_cache_page_count_);
    bucket_dirty_state_.resize(max_cache_page_count_);
    bucket_free_state_.resize(max_cache_page_count_);
    bucket_pinned_state_.resize(max_cache_page_count_);
    for (int i = 0; i < max_cache_page_count_; i++) {
        bucket_free_state_[i] = true;
    }

    next_cache_victim_ = 0;
    next_dirty_search_cache_victim_ = 0;
}

string DiskHashIndex::CacheLine::DebugString() const {
    return ToString(cache_line_id_);
}

string DiskHashIndex::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"item count\": " << this->GetItemCount() << "," << std::endl;
    sstr << "\"dirty item count\": " << this->GetDirtyItemCount() << "," << std::endl;
    sstr << "\"total item count\": " << this->GetTotalItemCount() << "," << std::endl;

    if (lazy_sync_) {
        sstr << "\"sync count\": " << this->statistics_.sync_count_ << "," << std::endl;
        sstr << "\"sync wait count\": " << this->statistics_.sync_wait_count_ << "," << std::endl;
    }
    if (overflow_area_) {
        sstr << "\"overflow item count\": " << overflow_area_->GetItemCount() << "," << std::endl;
    }
    if (overflow_area_) {
        sstr << "\"overflow area\": " << this->overflow_area_->PrintTrace() << "," << std::endl;
    }
    if (write_back_cache_) {
        sstr << "\"write back\": {";
        sstr << "\"miss count\": " << statistics_.write_cache_miss_count_ << "," << std::endl;
        sstr << "\"hit count\": " << statistics_.write_cache_hit_count_ << "," << std::endl;
        sstr << "\"estimated max item count\": " << this->GetEstimatedMaxCacheItemCount() << "," << std::endl;

        if (statistics_.write_cache_hit_count_ + statistics_.write_cache_miss_count_ > 0) {
            double hit_rate = (1.0 * statistics_.write_cache_hit_count_) / (1.0 * (statistics_.write_cache_hit_count_
                                                                                   + statistics_.write_cache_miss_count_));
            sstr << "\"hit ratio\": " << hit_rate << ",";
        } else {
            sstr << "\"hit ratio\": null,";
        }

        sstr << "\"free page count\": " << statistics_.write_cache_free_page_count_ << "," << std::endl;
        sstr << "\"used page count\": " << statistics_.write_cache_used_page_count_ << "," << std::endl;
        sstr << "\"dirty page count\": " << statistics_.write_cache_dirty_page_count_ << "," << std::endl;
        sstr << "\"persisted page count\": " << statistics_.write_cache_persisted_page_count_ << "," << std::endl;
        sstr << "\"evict count\": " << statistics_.write_cache_evict_count_ << "," << std::endl;
        sstr << "\"dirty evict count\": " << statistics_.write_cache_dirty_evict_count_ << "," << std::endl;
        sstr << "\"data\": " << write_back_cache_->PrintTrace();
        sstr << "}," << std::endl;
    }
    if (trans_system_) {
        sstr << "\"transaction\": " << trans_system_->PrintTrace() << "," << std::endl;
    }
    sstr << "\"estimated max item count\": " << this->GetEstimatedMaxItemCount() << std::endl;
    sstr << "}";
    return sstr.str();
}

string DiskHashIndex::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    if (trans_system_) {
        sstr << "\"transaction\": " << trans_system_->PrintLockStatistics() << "," << std::endl;
    }
    if (overflow_area_) {
        sstr << "\"overflow area\": " << this->overflow_area_->PrintLockStatistics() << "," << std::endl;
    }
    if (write_back_cache_) {
        sstr << "\"write back cache\": " << this->write_back_cache_->PrintLockStatistics() << "," << std::endl;
    }
    sstr << "\"lock free\": " << this->statistics_.lock_free_ << "," << std::endl;
    sstr << "\"lock busy\": " << this->statistics_.lock_busy_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string DiskHashIndex::PrintProfile() {
    if (this->state_ != STARTED) {
        return "{}";
    }
    stringstream sstr;
    sstr << "{";
    if (trans_system_) {
        sstr << "\"transaction\": " << trans_system_->PrintProfile() << "," << std::endl;
    }
    sstr << "\"lookup time\": " << this->statistics_.lookup_time_.GetSum() << "," << std::endl;
    sstr << "\"update time\": " << this->statistics_.update_time_.GetSum() << "," << std::endl;
    sstr << "\"update time lock wait\": " << this->statistics_.update_time_lock_wait_.GetSum() << "," << std::endl;
    sstr << "\"update time page read\": " << this->statistics_.update_time_page_read_.GetSum() << "," << std::endl;
    sstr << "\"update time page update\": " << this->statistics_.update_time_page_update_.GetSum() << "," << std::endl;
    sstr << "\"update time transaction start\": " << this->statistics_.update_time_transaction_start_.GetSum() << ","
         << std::endl;
    sstr << "\"update time page write\": " << this->statistics_.update_time_page_write_.GetSum() << "," << std::endl;
    sstr << "\"update time commit\": " << this->statistics_.update_time_commit_.GetSum() << "," << std::endl;
    sstr << "\"delete time\": " << this->statistics_.delete_time_.GetSum() << "," << std::endl;
    if (lazy_sync_) {
        sstr << "\"sync disk time\": " << this->statistics_.sync_time_.GetSum() << "," << std::endl;
        sstr << "\"sync wait time\": " << this->statistics_.sync_wait_time_.GetSum() << "," << std::endl;
    }
    if (overflow_area_) {
        sstr << "\"overflow area\": " << this->overflow_area_->PrintProfile() << "," << std::endl;
    }
    if (write_back_cache_) {
        sstr << "\"write cache read time\": " << this->statistics_.write_cache_read_time_.GetSum() << "," << std::endl;
        sstr << "\"write cache update time\": " << this->statistics_.write_cache_update_time_.GetSum() << ","
             << std::endl;
        sstr << "\"write cache data\": " << this->write_back_cache_->PrintProfile() << "," << std::endl;

        sstr << "\"update time cache read\": " << this->statistics_.update_time_cache_read_.GetSum() << ","
             << std::endl;
        sstr << "\"write cache evict search time\": " << this->statistics_.cache_search_evict_page_time_.GetSum()
             << "," << std::endl;
        sstr << "\"write cache free search time\": " << this->statistics_.cache_search_free_page_time_.GetSum() << ","
             << std::endl;
    }
    sstr << "\"write disk time\": " << this->statistics_.write_disk_time_.GetSum() << "," << std::endl;
    sstr << "\"read disk time\": " << this->statistics_.read_disk_time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

uint64_t DiskHashIndex::GetPersistentSize() {
    size_t file_size = 0;
    for (size_t i = 0; i < this->file_.size(); i++) {
        if (this->file_[i]) {
            Option<off_t> fs = this->file_[i]->GetSize();
            if (fs.valid()) {
                file_size += fs.value();
            }
        }
    }
    return file_size;
}

uint64_t DiskHashIndex::GetDirtyItemCount() {
    return dirty_item_count_;
}

uint64_t DiskHashIndex::GetItemCount() {
    return this->item_count_ + (this->overflow_area_ ? this->overflow_area_->GetItemCount() : 0);
}

uint64_t DiskHashIndex::GetTotalItemCount() {
    return this->total_item_count_ + (this->overflow_area_ ? this->overflow_area_->GetItemCount() : 0);
}

bool DiskHashIndex::Close() {
    DEBUG("Closing disk-based hash index");

    // we do not dump the data during closing as all data that is allowed to change
    // can be recovered from the transactions.

    for (size_t i = 0; i < this->file_.size(); i++) {
        if (this->file_[i]) {
            if (!this->file_[i]->Sync()) {
                WARNING("Failed to sync transaction file: " << this->file_[i]->path());
            }
            if (!this->file_[i]->Close()) {
                WARNING("Failed to close file " << this->filename_[i]);
            }
            this->file_[i] = NULL;
        }
    }
    this->file_.clear();

    if (this->info_file_) {
        if (!this->info_file_->Close()) {
            WARNING("Cannot close chunk index log file");
        }
        this->info_file_ = NULL;
    }
    if (this->overflow_area_) {
        if (!this->overflow_area_->Close()) {
            WARNING("Failed to close overflow area");
        }
        this->overflow_area_ = NULL;
    }
    if (write_back_cache_) {
        if (!write_back_cache_->Close()) {
            WARNING("Failed to close write back cache");
        }
        write_back_cache_ = NULL;
    }
    for (int i = 0; i < cache_lines_.size(); i++) {
        CacheLine* cache_line = cache_lines_[i];
        cache_lines_[i] = NULL;
        if (cache_line) {
            delete cache_line;
        }
    }
    cache_lines_.clear();
    if (this->trans_system_) {
        if (!this->trans_system_->Close()) {
            WARNING("Failed to close transaction system");

        }
        delete this->trans_system_;
        this->trans_system_ = NULL;
    }
    delete this;
    return true;
}

IndexIterator* DiskHashIndex::CreateIterator() {
    CHECK_RETURN(this->state_ == STARTED, NULL, "Index not started");
    return new DiskHashIndexIterator(this);
}

uint64_t DiskHashIndex::GetEstimatedMaxCacheItemCount() {
    return this->max_cache_item_count_;
}

lookup_result DiskHashPage::Search(const void* key, size_t key_size, Message* message) {
    DCHECK_RETURN(key, LOOKUP_ERROR, "Key not set");
    DCHECK_RETURN(index_, LOOKUP_ERROR, "Index not set");
    DCHECK_RETURN(data_buffer_, LOOKUP_ERROR, "Data buffer not set");

    TRACE("Search bucket: bucket " << bucket_id_ <<
        ", key " << ToHexString(key, key_size) <<
        ", items " << this->item_count_ << (this->overflow_ ? ", overflow mode " : ""));

    DiskHashEntry entry(this->index_->max_key_size(), this->index_->max_value_size());
    for (uint32_t i = 0; i < this->item_count(); i++) {
        uint32_t offset = i * entry.entry_data_size();
        DCHECK_RETURN(entry.entry_data_size() <= this->data_buffer_size_ - offset,
            LOOKUP_ERROR,
            "entry data size " << entry.entry_data_size() <<
            "available size " << this->data_buffer_size_ - offset);
        CHECK_RETURN(entry.ParseFrom(this->data_buffer_ + offset, entry.entry_data_size()),
            LOOKUP_ERROR,
            "Failed to parse entry data: " <<
            "offset " << offset <<
            ", bucket page " << this->DebugString() <<
            ", bucket data " << ToHexString(data_buffer_, data_buffer_size_));
        TRACE("Found: i " << i << ": " << entry.DebugString() << ", offset " << offset);
        if (raw_compare(entry.key(), entry.key_size(), key, key_size) == 0) {
            /* Found it */
            if (likely(message != NULL)) {
                CHECK_RETURN(message->ParseFromArray(entry.value(), entry.value_size()),
                    LOOKUP_ERROR, "Failed to parse message: " <<
                    "key " << ToHexString(key, key_size) <<
                    ", value " << ToHexString(entry.value(), entry.value_size()) <<
                    ", size " << entry.value_size() <<
                    ", entry " << entry.DebugString() <<
                    ", bucket page " << this->DebugString() <<
                    ", bucket data " << ToHexString(buffer_, buffer_size_) <<
                    ", raw page data " << page_data_.ShortDebugString() <<
                    ", message " << message->InitializationErrorString());
            }
            return LOOKUP_FOUND;
        }
    }
    // if haven't found a message, we also have to search the overflow
    // index if the page is marked as overflowed page.
    if (unlikely(this->overflow_)) {
        CHECK_RETURN(this->index_->overflow_area_, LOOKUP_ERROR, "Overflow area not set");
        return index_->overflow_area_->Lookup(key, key_size, message);
    }
    return LOOKUP_NOT_FOUND;
}

delete_result DiskHashPage::Delete(const void* key, unsigned int key_size) {
    changed_since_last_serialize_ = true;

    TRACE("Delete from bucket: bucket " << bucket_id_ << ", key " << ToHexString(key, key_size));

    DiskHashEntry entry(this->index_->max_key_size(), this->index_->max_value_size());
    for (uint32_t i = 0; i < this->item_count(); i++) {
        uint32_t offset = i * entry.entry_data_size();
        DCHECK_RETURN(entry.entry_data_size() <= this->data_buffer_size_ - offset,
            DELETE_ERROR,
            "entry data size " << entry.entry_data_size() <<
            "available size " << this->data_buffer_size_ - offset);
        CHECK_RETURN(entry.ParseFrom(this->data_buffer_ + offset, entry.entry_data_size()),
            DELETE_ERROR,
            "Failed to parse entry data: " <<
            "offset " << offset <<
            ", bucket page " << this->DebugString() <<
            ", bucket data " << ToHexString(data_buffer_, data_buffer_size_));
        TRACE("Found: i " << i << ": " << entry.DebugString() << ", offset " << offset);
        if (raw_compare(entry.key(), entry.key_size(), key, key_size) == 0) {
            // Found key

            // Copy last to the position of the deleted item, not necessary if i is the last item
            if (i != this->item_count_ - 1) {
                uint32_t last_offset = (this->item_count_ - 1) * entry.entry_data_size();
                DCHECK_RETURN(offset + entry.entry_data_size() <= last_offset, DELETE_ERROR,
                    "Copy overlapping: index " << i << ", item count " << this->item_count_);
                DCHECK_RETURN(last_offset + entry.entry_data_size() <= data_buffer_size_, DELETE_ERROR, "Illegal offset");
                memcpy(this->data_buffer_ + offset, this->data_buffer_ + last_offset, entry.entry_data_size());
            }
            // this->index_->item_count_--;

            this->item_count_--;
            this->index_->total_item_count_--;
            this->index_->item_count_--;
            return DELETE_OK;
        }
    }

    // If we haven't found the key, we have to lookup into the overflow
    // area if the page is marked as overflow page.
    if (unlikely(this->overflow_)) {
        // TODO (dmeister): Why do we have to lookup first?
        CHECK_RETURN(this->index_->overflow_area_, DELETE_ERROR, "Overflow area not set");
        enum lookup_result r = this->index_->overflow_area_->Lookup(key, key_size, NULL);
        CHECK_RETURN(r != LOOKUP_ERROR, DELETE_ERROR, "Error lookup up overflow area");
        if (r == LOOKUP_FOUND) {
            return this->index_->overflow_area_->Delete(key, key_size);
        }
    }
    return DELETE_NOT_FOUND;
}

put_result DiskHashPage::Update(const void* key, size_t key_size, const Message& message, bool keep) {
    changed_since_last_serialize_ = true;
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(this->index_, PUT_ERROR, "Container storage not set");

    TRACE("Update bucket: bucket " << bucket_id_ <<
        ", key " << ToHexString(key, key_size) <<
        ", key size " << key_size <<
        ", page " << this->DebugString());

    DiskHashEntry entry(this->index_->max_key_size(), this->index_->max_value_size());

    for (uint32_t i = 0; i < this->item_count(); i++) {
        uint32_t offset = i * entry.entry_data_size();
        CHECK_RETURN(entry.entry_data_size() <= this->data_buffer_size_ - offset,
            PUT_ERROR,
            "entry data size " << entry.entry_data_size() <<
            "available size " << this->data_buffer_size_ - offset);
        CHECK_RETURN(entry.ParseFrom(this->data_buffer_ + offset, entry.entry_data_size()),
            PUT_ERROR,
            "Failed to parse entry data: " <<
            "offset " << offset <<
            ", item index " << i <<
            ", item count " << item_count_ <<
            ", bucket page " << this->DebugString());
        TRACE("Found: i " << i << ": " << entry.DebugString() << ", offset " << offset);
        if (raw_compare(entry.key(), entry.key_size(), key, key_size) == 0) {
            if (keep) {
                return PUT_KEEP;
            }
            // Found it
            CHECK_RETURN(entry.AssignValue(message), PUT_ERROR, "Failed to assign value data");
            TRACE("Update bucket entry: bucket " << bucket_id_ << ", key " << ToHexString(key, key_size) <<
                ", offset " << offset << ", entry " << entry.DebugString());

            return PUT_OK;
        }
    }

    // If we haven't found it, we lookup the overflow area if the key
    // can be found there (if the page is marked as overflowed page)
    if (unlikely(this->overflow_)) {
        CHECK_RETURN(this->index_->overflow_area_, PUT_ERROR, "Overflow area not set");
        enum lookup_result r = this->index_->overflow_area_->Lookup(key, key_size, NULL);
        CHECK_RETURN(r != LOOKUP_ERROR, PUT_ERROR, "Error lookup up overflow area");
        if (r == LOOKUP_FOUND) {
            if (keep) {
                return PUT_KEEP;
            }
            return this->index_->overflow_area_->Put(key, key_size, message);
        } else {
            // NOT FOUND
        }
    }

    // Not found
    uint32_t next_offset = this->item_count() * entry.entry_data_size();
    bool isOverflow = (this->item_count() + 1) * entry.entry_data_size() >= this->data_buffer_size();
    if (likely(!isOverflow)) {
        TRACE("Add entry to page: bucket " << bucket_id_ <<
            ", offset " << next_offset <<
            ", key " << ToHexString(key, key_size) <<
            ", key size " << key_size);

        byte* entry_buffer = this->data_buffer_ + next_offset;
        size_t entry_size = this->data_buffer_size_ - next_offset;
        CHECK_RETURN(entry.AssignBuffer(entry_buffer, entry_size), PUT_ERROR,
            "Failed to parse entry data: offset " << next_offset << ", entry size " << entry_size << ", buffer size " << this->data_buffer_size_);

        CHECK_RETURN(entry.AssignKey(key, key_size), PUT_ERROR,
            "Failed to assign value data: key size " << key_size << ", max key size " << entry.max_key_size());
        CHECK_RETURN(entry.AssignValue(message), PUT_ERROR,
            "Failed to assign value data: " << message.ShortDebugString());

        this->index_->item_count_++;
        this->item_count_++;
    } else {
        // overflow: The page cannot hold the new item
        // if there is no configured overflow area, we
        // return an error indicating that the bucket is full

        // We are in an overflow situation. For debugging we would like to see if there is one page getting
        // to much data, or if the whole system is going mad. Therefore we calculate at this point the
        // average load over all pages and compare it with this page.
        uint64_t max_count = this->index_->GetEstimatedMaxItemCount() / this->index_->estimated_max_fill_ratio_;
        double average_fill_ratio = this->index_->GetItemCount() / max_count;

        CHECK_RETURN(this->index_->overflow_area_, PUT_ERROR, "Bucket full: " <<
            "bucket id " << this->bucket_id_ <<
            ", page size " << this->buffer_size_ <<
            ", data buffer size " << this->data_buffer_size_ <<
            ", item count " << this->item_count() <<
            ", entry data size " << entry.entry_data_size() <<
            ", max key size " << this->index_->max_key_size() <<
            ", max value size " << this->index_->max_value_size() <<
            ", index item count " << index_->GetItemCount() <<
            ", estimated max index item count " << index_->GetEstimatedMaxItemCount() <<
            ", index bucket count " << index_->bucket_count() <<
            ", average page fill ratio " << average_fill_ratio <<
            ", overflow area not set");
        if (!this->overflow_) {
            this->overflow_ = true;
            TRACE("Bucket " << this->bucket_id_ << " switches to overflow mode:" <<
                ", page size " << this->buffer_size_ <<
                ", data buffer size " << this->data_buffer_size_ <<
                ", item count " << this->item_count() <<
                ", entry data size " << entry.entry_data_size() <<
                ", max key size " << this->index_->max_key_size() <<
                ", max value size " << this->index_->max_value_size() <<
                ", index item count " << index_->GetItemCount() <<
                ", estimated max index item count " << index_->GetEstimatedMaxItemCount() <<
                ", index bucket count " << index_->bucket_count() <<
                ", average page fill ratio " << average_fill_ratio);
        }
        CHECK_RETURN(this->index_->overflow_area_->Put(key, key_size, message) != PUT_ERROR, PUT_ERROR,
            "Failed to put data to overflow area");
    }
    DCHECK_RETURN(used_size() <= raw_buffer_size(), PUT_ERROR, "Illegal used size: " << DebugString() <<
        ", used size " << used_size() <<
        ", buffer size " << raw_buffer_size());
    TRACE("Added bucket entry: bucket " << bucket_id_ << ", key " << ToHexString(key, key_size) <<
        ", offset " << next_offset << ", entry " << entry.DebugString());
    return PUT_OK;
}

DiskHashPage::DiskHashPage(DiskHashIndex* c, uint64_t bucket_id, byte* buffer, size_t buffer_size) {
    this->index_ = c;
    this->bucket_id_ = bucket_id;
    this->buffer_ = buffer;
    this->buffer_size_ = buffer_size;
    this->data_buffer_ = this->buffer_ + kPageDataSize;
    this->data_buffer_size_ = this->buffer_size_ - kPageDataSize;
    this->item_count_ = 0;
    this->overflow_ = false;
    changed_since_last_serialize_ = true;
}

bool DiskHashPage::SerializeToBuffer() {
    if (!changed_since_last_serialize_) {
        return true;
    }
    page_data_.Clear();
    page_data_.set_entry_count(this->item_count_);
    if (this->overflow_) {
        page_data_.set_overflow(true);
    }

    if (index_->crc_) {
        CRC crc;
        crc.Update(this->data_buffer_, used_data_size());
        page_data_.set_crc(crc.GetRawValue());
    }

    Option<size_t> s = SerializeSizedMessage(page_data_, this->buffer_, kPageDataSize, this->index_->crc_);
    CHECK(s.valid(), "Failed to serialize page data" << page_data_.ShortDebugString());
    DCHECK(s.value() <= kPageDataSize, "Illegal serialized header");

    TRACE("Serialize page header " << page_data_.ShortDebugString() <<
        ", page header size " << page_data_.GetCachedSize());
    changed_since_last_serialize_ = false;
    return s.valid();
}

bool DiskHashPage::Write(File* file) {
    DCHECK(file, "File not set");
    DCHECK(index_, "Index not set");

    uint64_t offset = (bucket_id_ / index_->file_.size()) * index_->page_size_;
    CHECK(SerializeToBuffer(), "Failed to serialize page to buffer: " << DebugString());

    TRACE("Write page: " << this->DebugString() << ", file " << file->path() <<
        ", offset " << offset <<
        ", page size " << buffer_size_ <<
        ", checksum " << page_data_.crc());

    // We always write the complete page. Writing less only leads to read/modify write cycles
    ProfileTimer timer(index_->statistics_.write_disk_time_);
    int result = file->Write(offset, this->buffer_, this->buffer_size_);
    CHECK(result == this->buffer_size_ && result != File::kIOError,
        "Hash write failed: " << DebugString());
    timer.stop();
    TRACE("Finished write page: " << this->DebugString());
    return true;
}

bool DiskHashPage::MergeWithCache(DiskHashCachePage* cache_page, uint32_t* pinned_item_count,
                                  uint32_t* merged_item_count, uint32_t* merged_new_item_count) {
    DCHECK(cache_page, "Cache page not set");
    changed_since_last_serialize_ = true;
    std::map<bytestring, uint16_t> cache_entries;

    TRACE("Merge with cache page: " <<
        "page " << this->DebugString() <<
        ", cache page " << cache_page->DebugString());

    DiskHashCacheEntry cache_entry(cache_page->mutable_raw_buffer(), cache_page->raw_buffer_size(),
                                   this->index_->max_key_size(), this->index_->max_value_size());
    uint32_t pinned_count = 0;
    lookup_result cache_lr = cache_page->IterateInit(&cache_entry);
    while (cache_lr == LOOKUP_FOUND) {
        if (cache_entry.is_dirty() && !cache_entry.is_pinned()) {
            TRACE("Found mergeable cache entry: " << cache_entry.DebugString());
            // only merge if entry is dirty and not pinned

            cache_entry.set_dirty(false);
            cache_entry.Store();

            cache_entries[make_bytestring(static_cast<const byte*>(cache_entry.key()), cache_entry.key_size())]
                = cache_entry.current_offset();
        } else {
            if (cache_entry.is_pinned()) {
                pinned_count++;
            }
            TRACE("Found un-mergeable cache entry: " << cache_entry.DebugString());
        }
        cache_lr = cache_page->Iterate(&cache_entry);
    }

    DiskHashEntry entry(this->index_->max_key_size(), this->index_->max_value_size());
    for (uint32_t i = 0; i < this->item_count(); i++) {
        uint32_t offset = i * entry.entry_data_size();
        CHECK(entry.entry_data_size() <= this->data_buffer_size_ - offset,
            "entry data size " << entry.entry_data_size() <<
            "available size " << this->data_buffer_size_ - offset);
        CHECK(entry.ParseFrom(this->data_buffer_ + offset, entry.entry_data_size()),
            "Failed to parse entry data: " <<
            "offset " << offset <<
            ", bucket page " << this->DebugString() <<
            ", bucket data " << ToHexString(data_buffer_, data_buffer_size_));
        TRACE("Look for cache items to merge: " << entry.DebugString() << ", offset " << offset);

        std::map<bytestring, uint16_t>::iterator j = cache_entries.find(make_bytestring(
                static_cast<const byte*>(entry.key()), entry.key_size()));
        if (j != cache_entries.end()) {
            uint16_t offset_in_cache = j->second;
            TRACE("Found: i " << i << ": " << entry.DebugString() << ", offset " << offset <<
                " in cache");
            CHECK(cache_entry.ParseFrom(offset_in_cache),
                "Failed to parse cache entry data: " <<
                "offset " << offset_in_cache <<
                ", bucket page " << this->DebugString() <<
                ", bucket data " << ToHexString(data_buffer_, data_buffer_size_));

            entry.AssignKey(cache_entry.key(), cache_entry.key_size());
            entry.AssignRawValue(static_cast<const byte*>(cache_entry.value()), cache_entry.value_size());
            cache_entries.erase(j);

            if (merged_item_count) {
                (*merged_item_count)++;
            }
        }
    }

    std::map<bytestring, uint16_t>::iterator j;
    for (j = cache_entries.begin(); j != cache_entries.end(); j++) {
        uint16_t offset_in_cache = j->second;
        CHECK(cache_entry.ParseFrom(offset_in_cache),
            "Failed to parse cache entry data: " <<
            "offset " << offset_in_cache <<
            ", bucket page " << this->DebugString() <<
            ", bucket data " << ToHexString(data_buffer_, data_buffer_size_));

        // If we haven't found it, we lookup the overflow area if the key
        // can be found there (if the page is marked as overflowed page)
        if (unlikely(this->overflow_)) {
            CHECK_RETURN(this->index_->overflow_area_, PUT_ERROR, "Overflow area not set");
            enum lookup_result r =
                this->index_->overflow_area_->Lookup(cache_entry.key(), cache_entry.key_size(), NULL);
            CHECK_RETURN(r != LOOKUP_ERROR, PUT_ERROR, "Error lookup up overflow area");
            if (r == LOOKUP_FOUND) {
                this->index_->overflow_area_->RawPut(cache_entry.key(), cache_entry.key_size(), cache_entry.value(),
                    cache_entry.value_size());
                continue;
            } else {
                // NOT FOUND
            }
        }

        TRACE("Found new cache entry to merge: " << cache_entry.DebugString() << ", offset " << offset_in_cache);
        uint32_t next_offset = this->item_count() * entry.entry_data_size();
        bool isOverflow = (this->item_count() + 1) * entry.entry_data_size() >= this->data_buffer_size();

        if (unlikely(isOverflow)) {
            // overflow: The page cannot hold the new item
            // if there is no configured overflow area, we
            // return an error indicating that the bucket is full

            // We are in an overflow situation. For debugging we would like to see if there is one page getting
            // to much data, or if the whole system is going mad. Therefore we calculate at this point the
            // average load over all pages and compare it with this page.
            uint64_t max_count = this->index_->GetEstimatedMaxItemCount() / this->index_->estimated_max_fill_ratio_;
            double average_fill_ratio = this->index_->GetItemCount() / max_count;

            CHECK_RETURN(this->index_->overflow_area_, PUT_ERROR, "Bucket full: " <<
                "bucket id " << this->bucket_id_ <<
                ", page size " << this->buffer_size_ <<
                ", data buffer size " << this->data_buffer_size_ <<
                ", item count " << this->item_count() <<
                ", entry data size " << entry.entry_data_size() <<
                ", max key size " << this->index_->max_key_size() <<
                ", max value size " << this->index_->max_value_size() <<
                ", index item count " << index_->GetItemCount() <<
                ", estimated max index item count " << index_->GetEstimatedMaxItemCount() <<
                ", index bucket count " << index_->bucket_count() <<
                ", average page fill ratio " << average_fill_ratio <<
                ", overflow area not set");
            if (!this->overflow_) {
                this->overflow_ = true;
                TRACE("Bucket " << this->bucket_id_ << " switches to overflow mode:" <<
                    ", page size " << this->buffer_size_ <<
                    ", data buffer size " << this->data_buffer_size_ <<
                    ", item count " << this->item_count() <<
                    ", entry data size " << entry.entry_data_size() <<
                    ", max key size " << this->index_->max_key_size() <<
                    ", max value size " << this->index_->max_value_size() <<
                    ", index item count " << index_->GetItemCount() <<
                    ", estimated max index item count " << index_->GetEstimatedMaxItemCount() <<
                    ", index bucket count " << index_->bucket_count() <<
                    ", average page fill ratio " << average_fill_ratio);
            }
            this->index_->overflow_area_->RawPut(cache_entry.key(), cache_entry.key_size(), cache_entry.value(),
                cache_entry.value_size());
        } else {
            byte* entry_buffer = this->data_buffer_ + next_offset;
            size_t entry_size = this->data_buffer_size_ - next_offset;
            CHECK(entry.AssignBuffer(entry_buffer, entry_size),
                "Failed to parse entry data: offset " << next_offset << ", entry size " << entry_size << ", buffer size " << this->data_buffer_size_);

            entry.AssignKey(cache_entry.key(), cache_entry.key_size());
            entry.AssignRawValue(static_cast<const byte*>(cache_entry.value()), cache_entry.value_size());
            this->item_count_++;
            this->index_->item_count_++;
        }

        if (merged_item_count) {
            (*merged_item_count)++;
        }
        if (merged_new_item_count) {
            (*merged_new_item_count)++;
        }
    }

    cache_page->set_pinned(pinned_count > 0);
    cache_page->set_dirty(pinned_count > 0);

    if (pinned_item_count) {
        *pinned_item_count = pinned_count;
    }

    // Check post conditions
    DCHECK(used_size() <= raw_buffer_size(), "Illegal used size: " << DebugString() <<
        ", used size " << used_size() <<
        ", buffer size " << raw_buffer_size());
    return true;
}

bool DiskHashEntry::ParseFrom(byte* buffer, size_t buffer_size) {
    this->buffer_ = buffer;
    this->buffer_size_ = buffer_size;
    DCHECK(buffer_size >= sizeof(key_size_) + sizeof(value_size_), "Illegal buffer size: buffer size " << buffer_size);

    memcpy(&key_size_, buffer, sizeof(key_size_));
    memcpy(&value_size_, buffer + sizeof(key_size_), sizeof(value_size_));
    CHECK(buffer_size >= sizeof(key_size_) + sizeof(value_size_) + key_size_ + value_size_,
        "Illegal buffer size: buffer size " << buffer_size <<
        ", key size " << key_size_ <<
        ", value size " << value_size_);

    CHECK(key_size_ <= this->max_key_size_, "Illegal key size: key size " << key_size_ << ", max key size " << max_key_size_);
    CHECK(value_size_ <= this->max_value_size_, "Illegal value size: value size " << value_size_ << ", max value size " << max_value_size_);
    return true;
}

bool DiskHashPage::ParseBuffer() {
    page_data_.Clear();
    CHECK(ParseSizedMessage(&page_data_, this->buffer_, this->index_->page_size_, this->index_->crc_).valid(),
        "Failed to parse page data " << ToHexString(buffer_, this->index_->page_size_));
    this->item_count_ = page_data_.entry_count();
    if (unlikely(page_data_.has_overflow() && page_data_.overflow())) {
        this->overflow_ = true;
    }
    if (likely(page_data_.has_crc() && index_->crc_)) {
        CRC crc_gen;
        crc_gen.Update(this->data_buffer_, used_data_size());
        uint32_t stored = page_data_.crc();
        uint32_t generated = crc_gen.GetRawValue();
        CHECK(stored == generated, "CRC check failed: " <<
            "stored " << stored <<
            ", generated " << generated <<
            ", page data " << page_data_.ShortDebugString() <<
            ", page " << DebugString());
    }
    changed_since_last_serialize_ = true;
    return true;
}

bool DiskHashPage::Read(File* file) {
    DCHECK(index_, "Container not set");
    DCHECK(file, "File not set");

    uint64_t offset = (bucket_id_ / index_->file_.size()) * index_->page_size_;

    // Scope for profile timing
    {
        ProfileTimer timer(this->index_->statistics_.read_disk_time_);
        ssize_t r = file->Read(offset, this->buffer_, this->index_->page_size_);
        CHECK(r == this->index_->page_size_,
            "Cannot read page data: " <<
            "bucket " << bucket_id_ <<
            ", file " << file->path() <<
            ", offset " << offset <<
            ", page size " << index_->page_size_ <<
            ", result " << r <<
            ", file size " << file->GetSize().value());
    }
    CHECK(ParseBuffer(),
        "Failed to parse data from read buffer: " <<
        "file " << file->path() <<
        ", offset " << offset <<
        ", bucket id " << bucket_id_);
    TRACE("Read bucket id " << bucket_id_ <<
        ": item count " << this->item_count_ <<
        ", file " << file->path() <<
        ", offset " << offset);
    changed_since_last_serialize_ = true;
    return true;
}

string DiskHashPage::DebugString() const {
    stringstream sstr;
    sstr << "[page " << this->bucket_id_ << ", item count " << this->item_count() << ", page size "
         << index_->page_size_;
    if (overflow_) {
        sstr << ", overflow";
    }
    sstr << "]";
    return sstr.str();
}

DiskHashPage::~DiskHashPage() {
}

bool DiskHashEntry::AssignBuffer(byte* buffer, size_t buffer_size) {
    DCHECK(buffer, "Buffer not set");
    this->key_size_ = 0;
    this->buffer_ = buffer;
    this->buffer_size_ = buffer_size;
    this->value_size_ = 0;
    return true;
}

bool DiskHashEntry::AssignValue(const Message& message) {
    DCHECK(this->buffer_ != NULL, "Buffer not set");
    CHECK(this->max_value_size_ >= message.ByteSize(),
        "Illegal size: " << message.DebugString());

    memset(this->mutable_value(), 0, this->max_value_size_);
    uint32_t value_size = message.ByteSize();
    byte* value = reinterpret_cast<byte*>(this->mutable_value());
    CHECK(message.SerializeWithCachedSizesToArray(value),
        "Failed to serialize array: message " << message.DebugString());

    memcpy(buffer_ + sizeof(key_size_), &value_size, sizeof(uint32_t));
    this->value_size_ = value_size;
    return true;
}

bool DiskHashEntry::AssignRawValue(const byte* new_value, size_t value_size) {
    DCHECK(this->buffer_ != NULL, "Buffer not set");
    DCHECK(this->max_value_size_ >= value_size,
        "Illegal size: " << value_size);

    memset(this->mutable_value(), 0, this->max_value_size_);
    byte* value = reinterpret_cast<byte*>(this->mutable_value());
    memcpy(value, new_value, value_size);
    memcpy(buffer_ + sizeof(key_size_), &value_size, sizeof(uint32_t));
    this->value_size_ = value_size;
    return true;
}

DiskHashEntry::DiskHashEntry(uint32_t max_key_size, uint32_t max_value_size) {
    this->max_key_size_ = max_key_size;
    this->key_size_ = 0;
    this->buffer_ = NULL;
    this->buffer_size_ = 0;
    this->max_value_size_ = max_value_size;
    this->value_size_ = 0;
}

string DiskHashEntry::DebugString() {
    return "key " + ToHexString(this->key(), this->key_size()) + ", key size " + ToString(this->key_size())
           + ", value " + ToHexString(this->value(), this->value_size()) + ", value size " + ToString(
        this->value_size());
}

DiskHashIndexIterator::DiskHashIndexIterator(DiskHashIndex* index) {
    this->index_ = index;
    this->bucket_id = -1;
    this->page = NULL;
    this->current_entry_index = 0;
    this->version_counter_ = index->version_counter_;
    this->overflow_iterator_ = NULL;

    this->buffer_ = new byte[index->page_size()];
    this->buffer_size = index->page_size();
}

DiskHashIndexIterator::~DiskHashIndexIterator() {
    if (this->page) {
        delete page;
        this->page = NULL;
    }
    if (this->buffer_) {
        delete[] this->buffer_;
        this->buffer_ = NULL;
    }
    if (this->overflow_iterator_) {
        delete this->overflow_iterator_;
        this->overflow_iterator_ = NULL;
    }
}

DiskHashPage* DiskHashIndexIterator::LoadBucket() {
    memset(this->buffer_, 0, this->buffer_size);
    DiskHashPage* new_page = new DiskHashPage(index_, this->bucket_id, this->buffer_, this->buffer_size);
    unsigned int file_index = 0;
    unsigned int lock_index = 0;
    this->index_->GetFileIndex(this->bucket_id, &file_index, &lock_index);
    File* file = this->index_->file_[file_index];
    DCHECK_RETURN(file, NULL, "File is not open");

    byte buffer[this->index_->page_size_];
    memset(buffer, 0, this->index_->page_size_);
    ScopedReadWriteLock scoped_lock(this->index_->page_locks_.Get(lock_index));
    CHECK_RETURN(scoped_lock.AcquireReadLockWithStatistics(&this->index_->statistics_.lock_free_, &this->index_->statistics_.lock_busy_), NULL, "Lock failed");

    CHECK_RETURN(new_page->Read(file), NULL,
        "Hash index page read failed: " <<
        "page " << new_page->DebugString() <<
        ", file " << file->path());
    CHECK_RETURN(scoped_lock.ReleaseLock(), NULL, "Unlock failed");
    return new_page;
}

lookup_result DiskHashIndexIterator::Next(void* key, size_t* key_size, Message* message) {
    CHECK_RETURN(this->version_counter_ == this->index_->version_counter_, LOOKUP_ERROR, "Concurrent modification error");

    TRACE("Get next entry: current bucket " << this->bucket_id <<
        ", current index " << this->current_entry_index <<
        (this->overflow_iterator_ ? ", overflow mode" : ""));

    if (this->overflow_iterator_ == NULL) {

        // check if the page is empty or if we have not loaded a page before
        while (this->page == NULL || this->page->item_count() == 0 || (this->page->item_count() > 0
                                                                       && this->current_entry_index >= this->page->item_count())) {

            // check if there buckets left
            if (this->bucket_id + 1U < this->index_->bucket_count()) {
                this->bucket_id++;
                delete this->page;
                this->page = this->LoadBucket();
                this->current_entry_index = 0;
                CHECK_RETURN(this->page, LOOKUP_ERROR, "Failed to load bucket " << this->bucket_id);
            } else {
                break;
            }
        }
        // no more to do in regular area
        if (this->current_entry_index >= this->page->item_count()) {
            if (this->index_->overflow_area_ == NULL) {
                // we are finished if there is no overflow area
                return LOOKUP_NOT_FOUND;
            } else {
                TRACE("Switch overflow iterator mode");
                this->overflow_iterator_ = this->index_->overflow_area_->CreateIterator();
                CHECK_RETURN(this->overflow_iterator_, LOOKUP_ERROR, "Failed to create overflow iterator");
            }
        } else {
            DiskHashEntry entry(this->index_->max_key_size(), this->index_->max_value_size());
            uint32_t offset = this->current_entry_index * entry.entry_data_size();
            CHECK_RETURN(entry.ParseFrom(this->page->mutable_data_buffer() + offset,
                    this->page->data_buffer_size() - offset), LOOKUP_ERROR,
                "Failed to parse entry data");

            CHECK_RETURN(entry.key_size() <= *key_size, LOOKUP_ERROR,
                "Illegal key size: " << (*key_size));
            memcpy(key, entry.key(), entry.key_size());
            *key_size = entry.key_size();

            if (message) {
                CHECK_RETURN(message->ParseFromArray(entry.value(), entry.value_size()), LOOKUP_ERROR,
                    "Failed to parse entry value");

                TRACE("Found key " << ToHexString(entry.key(), entry.key_size()) <<
                    ", key size " << entry.key_size() <<
                    ", " << message->ShortDebugString());
            }
            this->current_entry_index++;
            return LOOKUP_FOUND;
        }
    }
    // we do not use else here because if above block might set the overflow iterator so
    // that is block should be executed after the above if block was executed
    if (this->overflow_iterator_) {
        return this->overflow_iterator_->Next(key, key_size, message);
    }
    ERROR("Here are dragons");
    return LOOKUP_ERROR;
}

}
}
