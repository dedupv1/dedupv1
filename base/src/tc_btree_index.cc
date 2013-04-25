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

#include <base/base.h>
#include <base/tc_btree_index.h>
#include <base/index.h>
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
#include <tcbdb.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>

#include <sstream>

using std::string;
using std::stringstream;
using std::pair;
using std::make_pair;
using dedupv1::base::ReadWriteLock;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::ProfileTimer;
using google::protobuf::Message;
using dedupv1::base::ScopedArray;
using dedupv1::base::SerializeSizedMessageToString;
using dedupv1::base::ParseSizedMessage;
using dedupv1::base::File;

LOGGER("TCBTreeIndex");

namespace dedupv1 {
namespace base {

#define CHECK_TC_ECODE(bdb, x, msg) CHECK(x, msg << tcbdberrmsg(tcbdbecode(bdb)))

#define LOG_TC_ERROR(bdb, msg) ERROR(msg << tcbdberrmsg(tcbdbecode(bdb)))
#define LOG_TC_WARNING(bdb, msg) WARNING(msg << tcbdberrmsg(tcbdbecode(bdb)))

void TCBTreeIndex::RegisterIndex() {
    Index::Factory().Register("tc-disk-btree", &TCBTreeIndex::CreateIndex);
}

Index* TCBTreeIndex::CreateIndex() {
    Index* i = new TCBTreeIndex();
    return i;
}

TCBTreeIndex::TCBTreeIndex() : PersistentIndex(PERSISTENT_ITEM_COUNT | RETURNS_DELETE_NOT_FOUND | PUT_IF_ABSENT) {
    this->leaf_members_ = -1; // default value selected by TC
    this->non_leaf_members_ = -1; // default value selected by TC
    this->buckets_ = 32748;
    this->record_alignment_ = -1; // default value selected by TC
    this->free_pool_size_ = -1; // default value selected by TC
    this->compression_ = TC_BTREE_INDEX_COMPRESSION_NONE;
    this->leaf_cache_size_ = 0; // Cache disabled
    this->non_leaf_cache_size_ = 0;
    this->defrag_unit_ = 1;
    this->mem_mapped_size_ = -1;
    this->version_counter_ = 0;
    this->state_ = TC_BTREE_INDEX_STATE_CREATED;
    checksum_ = true;
    this->estimated_max_items_per_bucket_ = kDefaultEstimatedMaxItemsPerBucket;
}

TCBTreeIndex::Statistics::Statistics() {
    this->lock_busy_ = 0;
    this->lock_free_ = 0;
    this->lookup_count_ = 0;
    this->update_count_ = 0;
    this->delete_count_ = 0;
}

bool TCBTreeIndex::SetOption(const string& option_name,const string& option) {
    CHECK(this->state_ == TC_BTREE_INDEX_STATE_CREATED, "Illegal state " << this->state_);
    if (option_name == "filename") {
        CHECK(option.size() < 1024, "Illegal filename");
        this->filename_.push_back(option);
        return true;
    }
    if (option_name == "max-items-per-bucket") {
        CHECK(To<int32_t>(option).valid(), "Illegal option " << option);
        this->estimated_max_items_per_bucket_ = To<int32_t>(option).value();
        return true;
    }
    if (option_name == "leaf-members") {
        CHECK(To<int32_t>(option).valid(), "Illegal option " << option);
        this->leaf_members_ = To<int32_t>(option).value();
        return true;
    }
    if (option_name == "non-leaf-members") {
        CHECK(To<int32_t>(option).valid(), "Illegal option " << option);
        this->non_leaf_members_ = To<int32_t>(option).value();
        return true;
    }
    if (option_name == "buckets") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->buckets_ = ToStorageUnit(option).value();
        CHECK(this->buckets_ > 0, "Illegal option " << option);
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
            this->compression_ = TC_BTREE_INDEX_COMPRESSION_NONE;
        } else if (option == "deflate") {
            this->compression_ = TC_BTREE_INDEX_COMPRESSION_DEFLATE;
        } else if (option == "bzip2") {
            this->compression_ = TC_BTREE_INDEX_COMPRESSION_BZIP2;
        } else if (option == "tcbs") {
            this->compression_ = TC_BTREE_INDEX_COMPRESSION_TCBS;
        } else {
            ERROR("Illegal compression: " << option);
            return false;
        }
        return true;
    }
    if (option_name == "leaf-cache-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->leaf_cache_size_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "non-leaf-cache-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->non_leaf_cache_size_ = ToStorageUnit(option).value();
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

bool TCBTreeIndex::Start(const StartContext& start_context) {
    CHECK(this->state_ == TC_BTREE_INDEX_STATE_CREATED, "Index in invalid state");
    CHECK(this->filename_.size() > 0, "No filename specified");

    uint8_t compression_flag = 0;

    int open_flags = BDBOWRITER | BDBOTSYNC;

    switch (this->compression_) {
    case TC_BTREE_INDEX_COMPRESSION_NONE:
        compression_flag = 0;
        break;
    case TC_BTREE_INDEX_COMPRESSION_DEFLATE:
        compression_flag = BDBTDEFLATE;
        break;
    case TC_BTREE_INDEX_COMPRESSION_BZIP2:
        compression_flag = BDBTBZIP;
        break;
    case TC_BTREE_INDEX_COMPRESSION_TCBS:
        compression_flag = BDBTTCBS;
        break;
    default:
        ERROR("Illegal compression setting");
        return false;
    }

    CHECK(this->buckets_ % this->filename_.size() == 0,
        "Buckets are not aligned to the number of files: " <<
        "buckets " << buckets_ <<
        ", files " << this->filename_.size());

    uint64_t buckets_per_bdb = this->buckets_ / this->filename_.size();
    int64_t mem_mapped_per_bdb = -1;
    if (mem_mapped_size_ > 0) {
        mem_mapped_per_bdb = mem_mapped_size_ /  this->filename_.size();
    }

    this->bdb_.resize(this->filename_.size());
    this->bdb_locks_.Init(this->filename_.size());
    for (size_t i = 0; i < this->filename_.size(); i++) {
        TCBDB* new_db = tcbdbnew();
        CHECK(new_db, "Cannot create index");
        bool failed = false;

        // allow concurrency
        if (!tcbdbsetmutex(new_db)) {
            LOG_TC_ERROR(new_db, "Failed to set mutex");
            failed = true;
        }

        if (!failed) {
            if (!tcbdbsetcmpfunc(new_db, tccmpdecimal, NULL)) {
                LOG_TC_ERROR(new_db, "Failed to set comparison function: ");
                failed = true;
            }
        }

        if (!failed) {
            if (!tcbdbtune(new_db, this->leaf_members_, this->non_leaf_members_,
                    buckets_per_bdb, this->record_alignment_, this->free_pool_size_,
                    HDBTLARGE | compression_flag)) {
                LOG_TC_ERROR(new_db, "Failed to tune tc btree: ");
                failed = true;
            }
        }
        if (!failed) {
            if (!tcbdbsetcache(new_db, this->leaf_cache_size_,
                    this->non_leaf_cache_size_)) {
                LOG_TC_ERROR(new_db, "Failed to set cache size: ");
                failed = true;
            }
        }
        if (!failed) {
            if (!tcbdbsetdfunit(new_db, this->defrag_unit_)) {
                LOG_TC_ERROR(new_db, "Failed to set defrag setting: ");
                failed = true;
            }
        }
        if (!failed) {
            if (!tcbdbsetxmsiz(new_db, mem_mapped_per_bdb)) {
                LOG_TC_ERROR(new_db, "Failed to set memory mapping size: ");
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

                local_open_flags |= BDBOCREAT;
                INFO("Creating index file " << this->filename_[i]);
            }
            if (!tcbdbopen(new_db, this->filename_[i].c_str(), local_open_flags)) {
                LOG_TC_ERROR(new_db, "Failed to open file " << this->filename_[i] << ": ");
                failed = true;
            }
            if (!failed && start_context.create()) {
                struct stat stat;
                CHECK(File::Stat(filename_[i], &stat), "Failed stats of file: " << this->filename_[i]);

                if ((stat.st_mode & 0777) != start_context.file_mode().mode()) {
                    CHECK(chmod(this->filename_[i].c_str(), start_context.file_mode().mode()) == 0,
                        "Failed to change file permissions: " << this->filename_[i] << ", message " << strerror(errno));
                }
                if (start_context.file_mode().gid() != -1) {
                    if (stat.st_gid != start_context.file_mode().gid()) {
                        CHECK(chown(this->filename_[i].c_str(), -1, start_context.file_mode().gid()) == 0,
                            "Failed to change file group: " << this->filename_[i] << ", message " << strerror(errno));
                    }
                }

                string wal_filename = this->filename_[i] + ".wal";
                Option<bool> exists = File::Exists(wal_filename);
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
            tcbdbdel(new_db);
            this->bdb_[i] = NULL;
            return false;
        } else {
            this->bdb_[i] = new_db;
        }
    }
    this->state_ = TC_BTREE_INDEX_STATE_STARTED;
    return true;
}

lookup_result TCBTreeIndex::Lookup(const void* key, size_t key_size,
                                   Message* message) {
    ProfileTimer timer(this->stats_.total_time_);
    ProfileTimer lookup_timer(this->stats_.lookup_time_);

    CHECK_RETURN(this->state_ == TC_BTREE_INDEX_STATE_STARTED, LOOKUP_ERROR, "Index not started");
    CHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    pair<TCBDB*, ReadWriteLock*> current = GetBTree(key, key_size);
    CHECK_RETURN(current.first && current.second, LOOKUP_ERROR, "Cannot get bdb");
    TCBDB* current_db = current.first;
    ScopedReadWriteLock scoped_lock(current.second);
    ProfileTimer lock_timer(this->stats_.lock_time_);
    CHECK_RETURN(scoped_lock.AcquireReadLockWithStatistics(&stats_.lock_free_, &stats_.lock_busy_), LOOKUP_ERROR,
        "Failed to acquire bdb lock: " <<
        " key " << ToHexString(key, key_size) <<
        ", db filename " << tcbdbpath(current_db));
    lock_timer.stop();

    int result_size = 0;
    ProfileTimer tc_timer(this->stats_.tc_time_);
    void* result = tcbdbget(current_db, key, key_size, &result_size);
    tc_timer.stop();

    CHECK_RETURN(scoped_lock.ReleaseLock(), LOOKUP_ERROR, "Failed to release bdb lock");

    if (result == NULL) {
        // Either no entry or error
        int ecode = tcbdbecode(current_db);
        CHECK_RETURN(ecode == TCENOREC, LOOKUP_ERROR, "Failed to lookup: message " << tcbdberrmsg(ecode));
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
    this->stats_.lookup_count_++;
    return b ? LOOKUP_FOUND : LOOKUP_ERROR;
}

enum put_result TCBTreeIndex::Put(const void* key, size_t key_size,
                                  const Message& message) {
    ProfileTimer timer(this->stats_.total_time_);
    ProfileTimer update_timer(this->stats_.update_time_);

    CHECK_RETURN(this->state_ == TC_BTREE_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= INT_MAX, PUT_ERROR, "Key size too large");

    string target;
    CHECK_RETURN(SerializeSizedMessageToString(message, &target, checksum_),
        PUT_ERROR,
        "Failed to serialize message: " << message.ShortDebugString());

    pair<TCBDB*, ReadWriteLock*> current = GetBTree(key, key_size);
    CHECK_RETURN(current.first && current.second, PUT_ERROR, "Cannot get bdb");
    TCBDB* current_db = current.first;
    ScopedReadWriteLock scoped_lock(current.second);

    ProfileTimer lock_timer(this->stats_.lock_time_);
    CHECK_RETURN(scoped_lock.AcquireWriteLockWithStatistics(&stats_.lock_free_, &stats_.lock_busy_), PUT_ERROR,
        "Failed to acquire bdb lock: " <<
        " key " << ToHexString(key, key_size) <<
        ", value " << message.ShortDebugString() <<
        ", db filename " << tcbdbpath(current_db));
    lock_timer.stop();

    TRACE("Put: key " << ToHexString(key, key_size) << ", value " << message.ShortDebugString());

    ProfileTimer tc_timer(this->stats_.tc_time_);
    if (!tcbdbtranbegin(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to begin transaction: " <<
            "db filename " << tcbdbpath(current_db) <<
            ", key " <<  ToHexString(key, key_size) <<
            ", message ");
        return PUT_ERROR;
    }
    if (!tcbdbput(current_db, key, key_size, target.data(), target.size())) {
        if (!tcbdbtranabort(current_db)) {
            LOG_TC_WARNING(current_db, "Failed to abort transaction: ");
        }

        LOG_TC_ERROR(current_db, "Failed to put value: " <<
            " key " << ToHexString(key, key_size) <<
            ", value " << message.ShortDebugString() <<
            ", message ");
        return PUT_ERROR;
    }

    if (!tcbdbtrancommit(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to commit transaction: ");
        return PUT_ERROR;
    }
    tc_timer.stop();

    CHECK_RETURN(scoped_lock.ReleaseLock(), PUT_ERROR,
        "Failed to release bdb lock: " <<
        " key " << ToHexString(key, key_size) <<
        ", value " << message.ShortDebugString());
    this->stats_.update_count_++;
    this->version_counter_.fetch_and_increment();
    return PUT_OK;
}

enum put_result TCBTreeIndex::PutIfAbsent(const void* key, size_t key_size,
                                          const Message& message) {
    ProfileTimer timer(this->stats_.total_time_);
    ProfileTimer update_timer(this->stats_.update_time_);

    CHECK_RETURN(this->state_ == TC_BTREE_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= INT_MAX, PUT_ERROR, "Key size too large");

    string target;
    CHECK_RETURN(SerializeSizedMessageToString(message, &target, checksum_),
        PUT_ERROR,
        "Failed to serialize message: " << message.ShortDebugString());

    pair<TCBDB*, ReadWriteLock*> current = GetBTree(key, key_size);
    CHECK_RETURN(current.first && current.second, PUT_ERROR, "Cannot get bdb");
    TCBDB* current_db = current.first;
    ScopedReadWriteLock scoped_lock(current.second);
    ProfileTimer lock_timer(this->stats_.lock_time_);
    CHECK_RETURN(scoped_lock.AcquireWriteLockWithStatistics(&stats_.lock_free_, &stats_.lock_busy_), PUT_ERROR,
        "Failed to acquire bdb lock: " <<
        " key " << ToHexString(key, key_size) <<
        ", value " << message.ShortDebugString() <<
        ", db filename " << tcbdbpath(current_db));
    lock_timer.stop();

    TRACE("Put: key " << ToHexString(key, key_size) << ", value " << message.ShortDebugString());

    ProfileTimer tc_timer(this->stats_.tc_time_);
    if (!tcbdbtranbegin(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to begin transaction");
        return PUT_ERROR;
    }

    bool r = tcbdbputkeep(current_db, key, key_size, target.data(), target.size());
    if (!r) {
        int e = tcbdbecode(current_db);
        if (e == TCEKEEP) {
            if (!tcbdbtranabort(current_db)) {
                LOG_TC_WARNING(current_db, "Failed to abort transaction");
            }
            return PUT_KEEP;
        }
        if (!tcbdbtranabort(current_db)) {
            LOG_TC_WARNING(current_db, "Failed to abort transaction");
        }
        ERROR(tcbdberrmsg(e));
        return PUT_ERROR;
    }

    if (!tcbdbtrancommit(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to commit transaction");
        return PUT_ERROR;
    }
    tc_timer.stop();
    this->stats_.update_count_++;
    this->version_counter_.fetch_and_increment();
    return PUT_OK;
}

enum delete_result TCBTreeIndex::Delete(const void* key, size_t key_size) {
    ProfileTimer timer(this->stats_.total_time_);
    ProfileTimer delete_timer(this->stats_.delete_time_);

    CHECK_RETURN(this->state_ == TC_BTREE_INDEX_STATE_STARTED, DELETE_ERROR, "Index not started");
    CHECK_RETURN(key, DELETE_ERROR, "Key not set");
    CHECK_RETURN(key_size <= INT_MAX, DELETE_ERROR, "Key size too large");

    pair<TCBDB*, ReadWriteLock*> current = GetBTree(key, key_size);
    CHECK_RETURN(current.first && current.second, DELETE_ERROR, "Cannot get bdb");
    TCBDB* current_db = current.first;
    ScopedReadWriteLock scoped_lock(current.second);
    ProfileTimer lock_timer(this->stats_.lock_time_);
    CHECK_RETURN(scoped_lock.AcquireWriteLockWithStatistics(&stats_.lock_free_, &stats_.lock_busy_), DELETE_ERROR,
        "Failed to acquire bdb lock");
    lock_timer.stop();

    ProfileTimer tc_timer(this->stats_.tc_time_);
    if (!tcbdbtranbegin(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to begin transaction");
        return DELETE_ERROR;
    }

    bool r = tcbdbout(current_db, key, key_size);
    if (!r) {
        int e = tcbdbecode(current_db);
        if (e == TCENOREC) {
            if (!tcbdbtranabort(current_db)) {
                LOG_TC_WARNING(current_db, "Failed to abort transaction");
            }
            return DELETE_NOT_FOUND;
        }
        if (!tcbdbtranabort(current_db)) {
            LOG_TC_WARNING(current_db, "Failed to abort transaction");
        }
        ERROR(tcbdberrmsg(e));
        return DELETE_ERROR;
    }

    if (!tcbdbtrancommit(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to commit transaction");
        return DELETE_ERROR;
    }
    tc_timer.stop();
    this->stats_.delete_count_++;
    this->version_counter_.fetch_and_increment();
    return DELETE_OK;
}

TCBTreeIndex::~TCBTreeIndex() {
    for (size_t i = 0; i < this->bdb_.size(); i++) {
        if (this->bdb_[i]) {
            if (!tcbdbclose(this->bdb_[i])) {
                LOG_TC_WARNING(this->bdb_[i], "Failed to close database: ");
            }
            tcbdbdel(this->bdb_[i]);
            this->bdb_[i] = NULL;
        }
    }
    this->bdb_.clear();
}

bool TCBTreeIndex::SupportsCursor() {
    return this->filename_.size() == 1;
}

IndexCursor* TCBTreeIndex::CreateCursor() {
    if (this->bdb_.size() == 1) {
        BDBCUR* cur = tcbdbcurnew(this->bdb_[0]);
        CHECK_RETURN(cur, NULL, "Cannot create cursor");
        IndexCursor* c = new SingleFileTCBTreeCursor(this, cur);
        return c;
    }
    ERROR("Cursor not supported for multi-file index");
    return NULL;
}

string TCBTreeIndex::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"lock free\": " << this->stats_.lock_free_ << "," << std::endl;
    sstr << "\"lock busy\": " << this->stats_.lock_busy_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string TCBTreeIndex::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"lookup count\": " << this->stats_.lookup_count_ << "," << std::endl;
    sstr << "\"update count\": " << this->stats_.update_count_ << "," << std::endl;
    sstr << "\"delete count\": " << this->stats_.delete_count_ << "" << std::endl;
    sstr << "}";
    return sstr.str();
}

string TCBTreeIndex::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"total time\": " << this->stats_.total_time_.GetSum() << "," << std::endl;
    sstr << "\"lookup time\": " << this->stats_.lookup_time_.GetSum() << "," << std::endl;
    sstr << "\"update time\": " << this->stats_.update_time_.GetSum() << "," << std::endl;
    sstr << "\"delete time\": " << this->stats_.delete_time_.GetSum() << "," << std::endl;
    sstr << "\"lock time\": " << this->stats_.lock_time_.GetSum() << "," << std::endl;
    sstr << "\"tc time\": " << this->stats_.tc_time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

uint64_t TCBTreeIndex::GetPersistentSize() {
    uint64_t size_sum = 0;
    for (size_t i = 0; i < this->bdb_.size(); i++) {
        if (this->bdb_[i]) {
            size_sum += tcbdbfsiz(this->bdb_[i]);
        }
    }
    return size_sum;
}

uint64_t TCBTreeIndex::GetEstimatedMaxItemCount() {
    return this->buckets_ * estimated_max_items_per_bucket_;
}

uint64_t TCBTreeIndex::GetItemCount() {
    uint64_t items_sum = 0;
    for (size_t i = 0; i < this->bdb_.size(); i++) {
        if (this->bdb_[i]) {
            items_sum += tcbdbrnum(this->bdb_[i]);
        }
    }
    return items_sum;
}

pair<TCBDB*, ReadWriteLock*> TCBTreeIndex::GetBTree(const void* key, size_t key_size) {
    if (this->bdb_.size() == 1) {
        make_pair(this->bdb_[0], this->bdb_locks_.Get(0));
    }
    uint32_t hash_value;
    murmur_hash3_x86_32(key, key_size, 0, &hash_value);
    int index_ = hash_value % this->bdb_.size();
    return make_pair(this->bdb_[index_], this->bdb_locks_.Get(index_));
}

SingleFileTCBTreeCursor::SingleFileTCBTreeCursor(TCBTreeIndex* index_, BDBCUR* cur) {
    this->cur = cur;
    this->index_ = index_;
}

enum lookup_result SingleFileTCBTreeCursor::First() {
    if (!tcbdbcurfirst(this->cur)) {
        return LOOKUP_NOT_FOUND;
    }
    return LOOKUP_FOUND;
}

enum lookup_result SingleFileTCBTreeCursor::Next() {
    if (!tcbdbcurnext(this->cur)) {
        return LOOKUP_NOT_FOUND;
    }
    return LOOKUP_FOUND;
}

enum lookup_result SingleFileTCBTreeCursor::Last() {
    if (!tcbdbcurlast(this->cur)) {
        return LOOKUP_NOT_FOUND;
    }
    return LOOKUP_FOUND;
}

enum lookup_result SingleFileTCBTreeCursor::Jump(const void* key, size_t key_size) {
    if (!tcbdbcurjump(this->cur, key, key_size)) {
        return LOOKUP_NOT_FOUND;
    }
    return LOOKUP_FOUND;
}

bool SingleFileTCBTreeCursor::Remove() {

    // use index 0 as the cursor is only allowed with a single file
    TCBDB* current_db = this->index_->bdb_[0];

    ScopedReadWriteLock scoped_lock(this->index_->bdb_locks_.Get(0));
    CHECK_RETURN(scoped_lock.AcquireWriteLock(), DELETE_ERROR, "Failed to acquire bdb lock");

    if (!tcbdbtranbegin(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to begin transaction");
        return DELETE_ERROR;
    }

    if (!tcbdbcurout(this->cur)) {
        LOG_TC_ERROR(current_db, "Cannot remove item under cursor: ");
        if (!tcbdbtranabort(current_db)) {
            LOG_TC_WARNING(current_db, "Failed to abort transaction");
        }
        return DELETE_ERROR;
    }

    if (!tcbdbtrancommit(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to commit transaction: ");
        return DELETE_ERROR;
    }

    CHECK_RETURN(scoped_lock.ReleaseLock(), DELETE_ERROR, "Failed to release bdb lock");
    return true;
}

bool SingleFileTCBTreeCursor::Get(void* key, size_t* key_size,
                                  Message* message) {
    int s = 0;
    void* result = NULL;

    ScopedReadWriteLock scoped_lock(this->index_->bdb_locks_.Get(0));
    CHECK_RETURN(scoped_lock.AcquireReadLock(), DELETE_ERROR, "Failed to acquire bdb lock");

    if (key) {
        CHECK(key_size, "Key size not given");
        result = tcbdbcurkey(this->cur, &s);
        if (!result) {
            *key_size = 0;
            ERROR("Cannot read key under cursor");
            return false;
        }
        if (*key_size < (size_t) s) {
            free(result);
            ERROR("Too small key size");
            return false;
        }
        memcpy(key, result, s);
        free(result); // allocated by tc.
        *key_size = s;
    }
    if (message) {
        result = tcbdbcurval(this->cur, &s);
        if (!result) {
            ERROR("Cannot read value under cursor");
            return false;
        }
        bool b = ParseSizedMessage(message, result, s, this->index_->checksum_).valid();
        if (!b) {
            ERROR("Failed to parse message: " << ToHexString(result, s));
            free(result);
            return false;
        }
        free(result); // allocated by tc.
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool SingleFileTCBTreeCursor::Put(const Message& message) {
    // use index 0 as the cursor is only allowed with a single file
    TCBDB* current_db = this->index_->bdb_[0];
    ScopedReadWriteLock scoped_lock(this->index_->bdb_locks_.Get(0));
    CHECK_RETURN(scoped_lock.AcquireWriteLock(), DELETE_ERROR, "Failed to acquire bdb lock");

    string target;
    CHECK_RETURN(SerializeSizedMessageToString(message, &target, this->index_->checksum_),
        PUT_ERROR,
        "Failed to serialize message: " << message.ShortDebugString());

    if (!tcbdbtranbegin(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to begin transaction");
        return PUT_ERROR;
    }

    if (!tcbdbcurput(this->cur, target.data(), target.size(), BDBCPCURRENT)) {
        LOG_TC_ERROR(current_db, "Failed to put value under cursor: ");
        if (!tcbdbtranabort(current_db)) {
            LOG_TC_WARNING(current_db, "Failed to abort transaction");
        }
        return PUT_ERROR;
    }

    if (!tcbdbtrancommit(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to commit transaction: ");
        return PUT_ERROR;
    }

    CHECK(scoped_lock.ReleaseLock(), "Failed to releaser bdb lock");
    return PUT_OK;
}

bool SingleFileTCBTreeCursor::IsValidPosition() {
    int s = 0;
    void* value = tcbdbcurkey(this->cur, &s);
    free(value);
    return value != NULL;
}

SingleFileTCBTreeCursor::~SingleFileTCBTreeCursor() {
    tcbdbcurdel(this->cur);
}

IndexIterator* TCBTreeIndex::CreateIterator() {
    if (this->state_ != TC_BTREE_INDEX_STATE_STARTED) {
        return NULL;
    }
    return new TCBTreeIndexIterator(this);
}

TCBTreeIndexIterator::TCBTreeIndexIterator(TCBTreeIndex* index_) {
    this->index_ = index_;
    this->version_counter_ = index_->version_counter_;
    this->cur = NULL;

    this->tree_index = 0;
    this->cur_valid = false;
    for (; static_cast<size_t>(this->tree_index) < this->index_->bdb_.size(); this->tree_index++) {
        this->cur = tcbdbcurnew(index_->bdb_[this->tree_index]);
        if (this->cur == NULL) {
            WARNING(tcbdberrmsg(tcbdbecode(this->index_->bdb_[this->tree_index])));
            return;
        }
        this->cur_valid = tcbdbcurfirst(this->cur);
        TRACE("Init: index " << tree_index << ", valid " << ToString(cur_valid));
        if (!this->cur_valid) {
            tcbdbcurdel(this->cur);
            this->cur = NULL;
        } else {
            break;
        }
    }
}

TCBTreeIndexIterator::~TCBTreeIndexIterator() {
    if (this->cur) {
        tcbdbcurdel(this->cur);
        this->cur = NULL;
    }
}

enum lookup_result TCBTreeIndexIterator::Next(void* key, size_t* key_size,
                                              Message* message) {
    CHECK_RETURN(this->version_counter_ == this->index_->version_counter_, LOOKUP_ERROR,
        "Concurrent modification error: index version " << this->index_->version_counter_ <<
        ", iterator version " << this->version_counter_);

    TRACE("Next: valid " << ToString(cur_valid) << ", index " << tree_index);

    if (!this->cur_valid) {
        return LOOKUP_NOT_FOUND;
    }
    CHECK_RETURN(this->cur != NULL, LOOKUP_ERROR, "Cursor not set");
    int s = 0;
    void* result = NULL;

    if (key) {
        CHECK_RETURN(key_size, LOOKUP_ERROR, "Key size not given");
        result = tcbdbcurkey(this->cur, &s);
        if (!result) {
            *key_size = 0;
            ERROR("Cannot read key under cursor");
            return LOOKUP_ERROR;
        }
        if (*key_size < (size_t) s) {
            ERROR("Too small key size");
            free(result);
            return LOOKUP_ERROR;
        }
        memcpy(key, result, s);
        free(result); // allocated by tc.
        *key_size = s;
    }
    if (message) {
        result = tcbdbcurval(this->cur, &s);
        if (!result) {
            ERROR("Cannot read value under cursor");
            return LOOKUP_ERROR;
        }
        if (message) {
            bool b = ParseSizedMessage(message, result, s, this->index_->checksum_).valid();
            if (!b) {
                ERROR("Failed to parse message: " << ToHexString(result, s));
                free(result);
                return LOOKUP_ERROR;
            }
        }
        free(result); // allocated by tc.
    }
    TRACE("Found: key " << (key ? ToHexString(key, *key_size) : "null") << ", value " << (message ? message->ShortDebugString() : "null"));
    this->cur_valid = tcbdbcurnext(this->cur);
    if (!this->cur_valid) {
        tcbdbcurdel(this->cur);
        this->cur = NULL;

        this->tree_index++;
        while (static_cast<size_t>(this->tree_index) < this->index_->bdb_.size()) {
            this->cur = tcbdbcurnew(index_->bdb_[this->tree_index]);
            if (this->cur == NULL) {
                WARNING(tcbdberrmsg(tcbdbecode(this->index_->bdb_[this->tree_index])));
                return LOOKUP_ERROR;
            }
            this->cur_valid = tcbdbcurfirst(this->cur);
            TRACE("Move cursor: valid " << ToString(cur_valid) << ", index " << tree_index);
            if (!this->cur_valid) {
                tcbdbcurdel(this->cur);
                this->cur = NULL;
                this->tree_index++;
            } else {
                break;
            }
        }
    }
    return LOOKUP_FOUND;
}

}
}
