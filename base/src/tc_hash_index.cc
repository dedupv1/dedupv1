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

#include <base/tc_hash_index.h>
#include <base/index.h>
#include <base/base.h>
#include <base/hashing_util.h>
#include <base/locks.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/timer.h>
#include <base/memory.h>
#include <base/protobuf_util.h>
#include <base/fileutil.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <tcutil.h>
#include <tchdb.h>
#include <stdint.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

#include <sstream>

using std::pair;
using std::make_pair;
using std::string;
using std::stringstream;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::ProfileTimer;
using google::protobuf::Message;
using dedupv1::base::ScopedArray;
using dedupv1::base::ParseSizedMessage;
using dedupv1::base::SerializeSizedMessageToString;
using dedupv1::base::File;

LOGGER("TCHashIndex");

namespace dedupv1 {
namespace base {

#define CHECK_TC_ECODE(hdb, x, msg) {if (!x) { ERROR(msg << tchdberrmsg(tchdbecode(hdb))); goto error; } \
}

#define LOG_TC_ERROR(bdb, msg) ERROR(msg << tchdberrmsg(tchdbecode(bdb)))
#define LOG_TC_WARNING(bdb, msg) WARNING(msg << tchdberrmsg(tchdbecode(bdb)))

void TCHashIndex::RegisterIndex() {
    Index::Factory().Register("tc-disk-hash", &TCHashIndex::CreateIndex);
}

Index* TCHashIndex::CreateIndex() {
    Index* i = new TCHashIndex();
    return i;
}

TCHashIndex::TCHashIndex() : PersistentIndex(PERSISTENT_ITEM_COUNT | RETURNS_DELETE_NOT_FOUND | PUT_IF_ABSENT) {
    this->buckets_ = 131071; // default value of TC
    this->record_alignment_ = -1; // default value selected by TC
    this->free_pool_size_ = -1; // default value selected by TC
    this->compression_ = TC_HASH_INDEX_COMPRESSION_NONE;
    this->cache_size_ = 0; // Cache disabled
    this->defrag_unit_ = 1;
    this->mem_mapped_size_ = -1; // default value selected by TC
    this->version_counter_ = 0;
    this->state_ = TC_HASH_INDEX_STATE_CREATED;
    this->estimated_max_items_per_bucket_ = kDefaultEstimatedMaxItemsPerBucket;
    this->checksum_ = true;
}

bool TCHashIndex::SetOption(const string& option_name, const string& option) {
    if (option_name == "filename") {
        CHECK(option.size() < 1024, "Illegal filename");

        this->filename_.push_back(option);
        return true;
    }
    if (option_name == "buckets") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->buckets_ = ToStorageUnit(option).value();
        CHECK(this->buckets_ > 0, "Illegal option " << option);
        return true;
    }
    if (option_name == "max-items-per-bucket") {
        CHECK(To<int32_t>(option).valid(), "Illegal option " << option);
        this->estimated_max_items_per_bucket_ = To<int32_t>(option).value();
        return true;
    }
    if (option_name == "record-alignment") {
        CHECK(To<int8_t>(option).valid(), "Illegal option " << option);
        this->record_alignment_ = To<int8_t>(option).value();
        return true;
    }
    if (option_name == "free-pool-size") {
        CHECK(To<int8_t>(option).valid(), "Illegal option " << option);
        this->free_pool_size_ = To<int8_t>(option).value();
        return true;
    }
    if (option_name == "compression") {
        if (option == "none" || option == "false") {
            this->compression_ = TC_HASH_INDEX_COMPRESSION_NONE;
        } else if (option == "deflate") {
            this->compression_ = TC_HASH_INDEX_COMPRESSION_DEFLATE;
        } else if (option == "bzip2") {
            this->compression_ = TC_HASH_INDEX_COMPRESSION_BZIP2;
        } else if (option == "tcbs") {
            this->compression_ = TC_HASH_INDEX_COMPRESSION_TCBS;
        } else {
            ERROR("Illegal compression: " << option);
            return false;
        }
        return true;
    }
    if (option_name == "cache-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->cache_size_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "mem-mapped-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->mem_mapped_size_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "defrag") {
        CHECK(To<int32_t>(option).valid(), "Illegal option " << option);
        this->defrag_unit_ = To<int32_t>(option).value();
        return true;
    }
    if (option_name == "checksum") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->checksum_ = To<bool>(option).value();
        return true;
    }
    return PersistentIndex::SetOption(option_name, option);
}

bool TCHashIndex::Start(const StartContext& start_context) {
    CHECK(this->state_ == TC_HASH_INDEX_STATE_CREATED, "Index in invalid state");
    CHECK(this->filename_.size() > 0, "No filename specified");

    uint8_t compression_flag = 0;

    int open_flags = HDBOWRITER | HDBOTSYNC;

    switch (this->compression_) {
    case TC_HASH_INDEX_COMPRESSION_NONE:
        compression_flag = 0;
        break;
    case TC_HASH_INDEX_COMPRESSION_DEFLATE:
        compression_flag = HDBTDEFLATE;
        break;
    case TC_HASH_INDEX_COMPRESSION_BZIP2:
        compression_flag = HDBTBZIP;
        break;
    case TC_HASH_INDEX_COMPRESSION_TCBS:
        compression_flag = HDBTTCBS;
        break;
    default:
        ERROR("Illegal compression setting");
        return false;
    }

    uint64_t buckets_per_bdb = this->buckets_ / this->filename_.size();
    int64_t mem_mapped_per_bdb = -1;
    if (mem_mapped_size_ > 0) {
        mem_mapped_per_bdb = mem_mapped_size_ /  this->filename_.size();
    }

    this->locks_.Init(this->filename_.size());
    this->hdb_.resize(this->filename_.size());
    for (size_t i = 0; i < this->hdb_.size(); i++) {
        this->hdb_[i] = NULL;
    }
    for (size_t i = 0; i < this->filename_.size(); i++) {
        this->hdb_[i] = tchdbnew();
        CHECK(this->hdb_[i], "Failed to create hdb");
        bool failed = false;

        if (!tchdbsetmutex(this->hdb_[i])) { // Allow concurrency
            LOG_TC_ERROR(this->hdb_[i], "Failed to set mutex");
            failed = true;
        }
        if (!failed) {
            if (!tchdbtune(this->hdb_[i], buckets_per_bdb, this->record_alignment_, this->free_pool_size_, HDBTLARGE | compression_flag)) {
                LOG_TC_ERROR(this->hdb_[i], "Failed to tune database");
                failed = true;
            }
        }

        if (!failed) {
            if (!tchdbsetxmsiz(this->hdb_[i], mem_mapped_per_bdb)) {
                LOG_TC_ERROR(this->hdb_[i], "Failed to set memory mapped size");
                failed = true;
            }
        }

        if (!failed) {
            if (!tchdbsetdfunit(this->hdb_[i], this->defrag_unit_)) {
                LOG_TC_ERROR(this->hdb_[i], "Failed to set defrag unit");
                failed = true;
            }
        }

        if (!failed) {
            Option<bool> exists = File::Exists(this->filename_[i].c_str());
            CHECK(exists.valid(), "Failed to check db file: " << this->filename_[i].c_str());

            int local_open_flags = open_flags;
            if (!exists.value() && start_context.create()) {
                CHECK(File::MakeParentDirectory(this->filename_[i], start_context.dir_mode().mode()),
                    "Failed to check parent directories");

                local_open_flags |= HDBOCREAT;
                INFO("Creating index file " << this->filename_[i]);
            }
            if (!tchdbopen(this->hdb_[i], this->filename_[i].c_str(), local_open_flags)) {
                LOG_TC_ERROR(this->hdb_[i],  "Failed to open file " << this->filename_[i] << ": ");
                failed = true;
            }

            if (!failed && start_context.create()) {
                struct stat stat;
                CHECK(File::Stat(filename_[i], &stat), "Failed stats of file: " << this->filename_[i]);

                if ((stat.st_mode & 0777) != start_context.file_mode().mode()) {
                    CHECK(chmod(this->filename_[i].c_str(), start_context.file_mode().mode()) == 0,
                        "Failed to change file permissions: " << this->filename_[i] << ", message " << strerror(errno) <<
                        ", stat mode " << std::hex << stat.st_mode << ", context mode " << start_context.file_mode().mode());
                }
                if (start_context.file_mode().gid() != -1) {
                    if (stat.st_gid != start_context.file_mode().gid()) {
                        CHECK(chown(this->filename_[i].c_str(), -1, start_context.file_mode().gid()) == 0,
                            "Failed to change file group: " << this->filename_[i] << ", message " << strerror(errno));
                    }
                }

                string wal_filename = this->filename_[i] + ".wal";
                exists = File::Exists(wal_filename);
                CHECK(exists.valid(), "Failed to check wal file: " << wal_filename);

                if (!exists.value()) {
                    // the only purpose of creating the wal file is that we want to control the permissions
                    File* wal_file = File::Open(wal_filename, O_RDWR | O_CREAT | O_LARGEFILE, start_context.file_mode().mode());
                    CHECK(wal_file, "Failed to create wal file " << wal_filename);
                    delete wal_file;
                }
                CHECK(File::Stat(wal_filename, &stat), "Failed stats of file: " << wal_filename);
                if ((stat.st_mode & 0777) != start_context.file_mode().mode()) {
                    CHECK(chmod(wal_filename.c_str(), start_context.file_mode().mode()) == 0,
                        "Failed to change file permissions: " << wal_filename << ", message " << strerror(errno));
                }
                if (start_context.file_mode().gid() != -1) {
                    if (stat.st_gid != start_context.file_mode().gid()) {
                        CHECK(chown(wal_filename.c_str(), -1, start_context.file_mode().gid()) == 0,
                            "Failed to change file group: " << wal_filename << ", message " << strerror(errno));
                    }
                }
            }
        }
        if (failed) {
            tchdbdel(this->hdb_[i]);
            this->hdb_[i] = NULL;
            return false;

        }
    }
    this->state_ = TC_HASH_INDEX_STATE_STARTED;
    return true;
}

enum lookup_result TCHashIndex::Lookup(const void* key, size_t key_size,
                                       Message* message) {
    ProfileTimer timer(this->profiling_);

    CHECK_RETURN(this->state_ == TC_HASH_INDEX_STATE_STARTED, LOOKUP_ERROR, "Index not started");
    CHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    pair<TCHDB*, ReadWriteLock*> current = GetHashDatabase(key, key_size);
    CHECK_RETURN(current.first && current.second, LOOKUP_ERROR, "Cannot get hdb");
    TCHDB* current_hdb = current.first;
    ScopedReadWriteLock scoped_lock(current.second);
    CHECK_RETURN(scoped_lock.AcquireReadLock(), LOOKUP_ERROR, "Failed to acquire hdb lock");

    int result_size = 0;
    void* result = tchdbget(current_hdb, key, key_size, &result_size);
    if (result == NULL) {
        // Either no entry or error
        int ecode = tchdbecode(current_hdb);
        CHECK_RETURN(ecode == TCENOREC, LOOKUP_ERROR, tchdberrmsg(ecode));
        return LOOKUP_NOT_FOUND;
    }
    bool b = true;
    if (message) {
        b = ParseSizedMessage(message, result, result_size, checksum_).valid();
        if (!b) {
            ERROR("Failed to parse message: " << ToHexString(result, result_size));
        }
    }
    free(result); // allocated by tc.
    return b ? LOOKUP_FOUND : LOOKUP_ERROR;
}

put_result TCHashIndex::Put(
    const void* key, size_t key_size,
    const Message& message) {
    ProfileTimer timer(this->profiling_);

    CHECK_RETURN(this->state_ == TC_HASH_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= INT_MAX, PUT_ERROR, "Key size too large");

    pair<TCHDB*, ReadWriteLock*> current = GetHashDatabase(key, key_size);
    CHECK_RETURN(current.first && current.second, PUT_ERROR, "Cannot get hdb");
    TCHDB* current_hdb = current.first;
    ScopedReadWriteLock scoped_lock(current.second);
    CHECK_RETURN(scoped_lock.AcquireWriteLock(), PUT_ERROR, "Failed to acquire hdb lock");

    string target;
    CHECK_RETURN(SerializeSizedMessageToString(message, &target, checksum_),
        PUT_ERROR,
        "Failed to serialize message: " << message.ShortDebugString());

    if (!tchdbtranbegin(current_hdb)) {
        LOG_TC_ERROR(current_hdb, "Failed to begin transaction");
        return PUT_ERROR;
    }

    if (!tchdbput(current_hdb, key, key_size, target.data(), target.size())) {
        if (!tchdbtranabort(current_hdb)) {
            LOG_TC_WARNING(current_hdb, "Failed to abort transaction");
        }

        LOG_TC_ERROR(current_hdb, "Failed to put data: ");
        return PUT_ERROR;
    }

    if (!tchdbtrancommit(current_hdb)) {
        LOG_TC_ERROR(current_hdb, "Failed to commit transaction");
        return PUT_ERROR;
    }
    this->version_counter_.fetch_and_increment();
    return PUT_OK;
}

enum put_result TCHashIndex::PutIfAbsent(
    const void* key, size_t key_size,
    const Message& message) {
    ProfileTimer timer(this->profiling_);

    CHECK_RETURN(this->state_ == TC_HASH_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= INT_MAX, PUT_ERROR, "Key size too large");

    pair<TCHDB*, ReadWriteLock*> current = GetHashDatabase(key, key_size);
    CHECK_RETURN(current.first && current.second, PUT_ERROR, "Cannot get hdb");
    TCHDB* current_hdb = current.first;
    ScopedReadWriteLock scoped_lock(current.second);
    CHECK_RETURN(scoped_lock.AcquireWriteLock(), PUT_ERROR, "Failed to acquire hdb lock");

    string target;
    CHECK_RETURN(SerializeSizedMessageToString(message, &target, checksum_),
        PUT_ERROR,
        "Failed to serialize message: " << message.ShortDebugString());

    if (!tchdbtranbegin(current_hdb)) {
        LOG_TC_ERROR(current_hdb, "Failed to begin transaction");
        return PUT_ERROR;
    }

    bool r = tchdbputkeep(current_hdb, key, key_size, target.data(), target.size());
    if (!r) {
        if (!tchdbtranabort(current_hdb)) {
            LOG_TC_WARNING(current_hdb, "Failed to abort transaction");
        }

        int e = tchdbecode(current_hdb);
        if (e == TCEKEEP) {
            return PUT_KEEP;
        }
        ERROR(tchdberrmsg(e));
        return PUT_ERROR;
    }

    if (!tchdbtrancommit(current_hdb)) {
        LOG_TC_ERROR(current_hdb, "Failed to commit transaction");
        return PUT_ERROR;
    }

    this->version_counter_.fetch_and_increment();
    return PUT_OK;
}

enum delete_result TCHashIndex::Delete(const void* key, size_t key_size) {
    ProfileTimer timer(this->profiling_);

    CHECK_RETURN(this->state_ == TC_HASH_INDEX_STATE_STARTED, DELETE_ERROR, "Index not started");
    CHECK_RETURN(key, DELETE_ERROR, "Key not set");
    CHECK_RETURN(key_size <= INT_MAX, DELETE_ERROR, "Key size too large");

    pair<TCHDB*, ReadWriteLock*> current = GetHashDatabase(key, key_size);
    CHECK_RETURN(current.first && current.second, DELETE_ERROR, "Cannot get hdb");
    TCHDB* current_hdb = current.first;
    ScopedReadWriteLock scoped_lock(current.second);
    CHECK_RETURN(scoped_lock.AcquireWriteLock(), DELETE_ERROR, "Failed to acquire hdb lock");

    if (!tchdbtranbegin(current_hdb)) {
        LOG_TC_ERROR(current_hdb, "Failed to begin transaction");
        return DELETE_ERROR;
    }

    bool r = tchdbout(current_hdb, key, key_size);
    if (!r) {
        if (!tchdbtranabort(current_hdb)) {
            LOG_TC_WARNING(current_hdb, "Failed to abort transaction");
        }

        int e = tchdbecode(current_hdb);
        if (e == TCENOREC) {
            return DELETE_NOT_FOUND;
        }
        ERROR(tchdberrmsg(e));
        return DELETE_ERROR;
    }

    if (!tchdbtrancommit(current_hdb)) {
        LOG_TC_ERROR(current_hdb, "Failed to commit transaction");
        return DELETE_ERROR;
    }
    this->version_counter_.fetch_and_increment();
    return DELETE_OK;
}

TCHashIndex::~TCHashIndex() {
    for (size_t i = 0; i < this->hdb_.size(); i++) {
        if (this->hdb_[i]) {
            if (!tchdbclose(this->hdb_[i])) {
                WARNING(tchdberrmsg(tchdbecode(this->hdb_[i])));
            }
            tchdbdel(this->hdb_[i]);
            this->hdb_[i] = NULL;
        }
    }
    this->hdb_.clear();
}

string TCHashIndex::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"index\": " << this->profiling_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

uint64_t TCHashIndex::GetPersistentSize() {
    uint64_t size_sum = 0;
    for (size_t i = 0; i < this->hdb_.size(); i++) {
        if (this->hdb_[i]) {
            size_sum += tchdbfsiz(this->hdb_[i]);
        }
    }
    return size_sum;
}

uint64_t TCHashIndex::GetEstimatedMaxItemCount() {
    return this->buckets_ * estimated_max_items_per_bucket_;
}

uint64_t TCHashIndex::GetItemCount() {
    uint64_t items_sum = 0;
    for (size_t i = 0; i < this->hdb_.size(); i++) {
        if (this->hdb_[i]) {
            items_sum += tchdbrnum(this->hdb_[i]);
        }
    }
    return items_sum;
}

pair<TCHDB*, ReadWriteLock*> TCHashIndex::GetHashDatabase(const void* key, size_t key_size) {
    if (this->hdb_.size() == 1) {
        make_pair(this->hdb_[0], this->locks_.Get(0));
    }
    int index_ = bj_hash(key, key_size) % this->hdb_.size();
    return make_pair(this->hdb_[index_], this->locks_.Get(index_));
}

IndexIterator* TCHashIndex::CreateIterator() {
    if (this->state_ != TC_HASH_INDEX_STATE_STARTED) {
        return NULL;
    }
    return new TCHashIndexIterator(this);
}

TCHashIndexIterator::TCHashIndexIterator(TCHashIndex* index_) {
    this->index_ = index_;
    this->hash_index_ = 0;
    this->version_counter_ = index_->version_counter_;
    tchdbiterinit(this->index_->hdb_[this->hash_index_]);
}

TCHashIndexIterator::~TCHashIndexIterator() {
}

enum lookup_result TCHashIndexIterator::Next(void* key, size_t* key_size,
                                             Message* message) {
    int result_size = 0;
    int iter_key_size = 0;
    void* result = NULL;
    void* iter_key = NULL;
    CHECK_RETURN(this->version_counter_ == this->index_->version_counter_, LOOKUP_ERROR, "Concurrent modification error");

    iter_key = tchdbiternext(this->index_->hdb_[this->hash_index_], &iter_key_size);
    if (iter_key == NULL) {
        this->hash_index_++;
        for (; static_cast<size_t>(this->hash_index_) < this->index_->hdb_.size() && iter_key == NULL; this->hash_index_++) {
            tchdbiterinit(this->index_->hdb_[this->hash_index_]);
            iter_key = tchdbiternext(this->index_->hdb_[this->hash_index_], &iter_key_size);
            if (iter_key) {
                break;
            }

        }
        if (iter_key == NULL) {
            return LOOKUP_NOT_FOUND;
        }
    }

    if (key) {
        CHECK_GOTO(*key_size >= (size_t) iter_key_size, "Illegal key size " << (*key_size));
        memcpy(key, iter_key, iter_key_size);
        *key_size = iter_key_size;
    }

    result_size = 0;
    result = tchdbget(this->index_->hdb_[this->hash_index_], iter_key, iter_key_size, &result_size);
    if (result == NULL) {
        // Either no entry or error
        int ecode = tchdbecode(this->index_->hdb_[this->hash_index_]);
        CHECK_GOTO(ecode == TCENOREC, tchdberrmsg(ecode));
        return LOOKUP_NOT_FOUND;
    }
    if (message) {
        CHECK_GOTO(ParseSizedMessage(message, result, result_size, this->index_->checksum_).valid(),
            "Failed to parse message");
    }
    free(iter_key); // allocated by tc
    free(result); // allocated by tc.
    return LOOKUP_FOUND;
error:
    if (iter_key) {
        free(iter_key); // allocated by tc
    }
    if (result) {
        free(result);
    }
    return LOOKUP_ERROR;
}

}
}
