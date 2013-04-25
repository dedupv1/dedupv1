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

#include <base/tc_fixed_index.h>
#include <base/index.h>
#include <base/hashing_util.h>
#include <base/locks.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/timer.h>
#include <base/memory.h>
#include <base/protobuf_util.h>
#include <base/fileutil.h>

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <tcutil.h>
#include <tcfdb.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>

#include <sstream>

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

LOGGER("TCFixedIndex");

namespace dedupv1 {
namespace base {

#define CHECK_TC_ECODE(bdb, x, msg) {if (!x) { ERROR(msg << tcfdberrmsg(tcfdbecode(bdb))); goto error; } \
}

#define LOG_TC_ERROR(bdb, msg) ERROR(msg << tcfdberrmsg(tcfdbecode(bdb)))
#define LOG_TC_WARNING(bdb, msg) WARNING(msg << tcfdberrmsg(tcfdbecode(bdb)))

void TCFixedIndex::RegisterIndex() {
    Index::Factory().Register("tc-disk-fixed", &TCFixedIndex::CreateIndex);
}

Index* TCFixedIndex::CreateIndex() {
    Index* i = new TCFixedIndex();
    return i;
}

TCFixedIndex::TCFixedIndex() : IDBasedIndex(PERSISTENT_ITEM_COUNT | RETURNS_DELETE_NOT_FOUND | PUT_IF_ABSENT) {
    this->width_ = -1; // default value selected by TC
    this->size = 0;
    this->version_counter_ = 0;
    this->state_ = TC_FIXED_INDEX_STATE_CREATED;
    checksum_ = true;
}

bool TCFixedIndex::SetOption(const string& option_name, const string& option) {

    if (option_name == "filename") {
        CHECK(option.size() < 1024, "Illegal filename");
        this->filename_.push_back(option);
        return true;
    }
    if (option_name == "width") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->width_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->size = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "transactions") {
        WARNING("Option transactions is depreciated");
        return true;
    }
    if (option_name == "checksum") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->checksum_ = To<bool>(option).value();
        return true;
    }
    return PersistentIndex::SetOption(option_name, option);
}

bool TCFixedIndex::Start(const StartContext& start_context) {
    CHECK(this->state_ == TC_FIXED_INDEX_STATE_CREATED, "Index in invalid state");
    CHECK(this->filename_.size() > 0, "No filename specified");
    CHECK(this->size > 0, "Size not specified");
    CHECK((this->size % this->filename_.size()) == 0, "Size " << this->size << " illegal for " << this->filename_.size() << " files");

    int open_flags = FDBOWRITER;
    if (start_context.create()) {
        open_flags |= FDBOCREAT;
    }
    open_flags |= FDBOTSYNC;

    this->fdb_.resize(this->filename_.size());

    for (size_t i = 0; i < this->filename_.size(); i++) {
        this->fdb_[i] = tcfdbnew();
        CHECK(this->fdb_[i], "Cannot create index");

        bool failed = false;
        if (!tcfdbsetmutex(this->fdb_[i])) {
            LOG_TC_ERROR(this->fdb_[i], "Failed to set mutex: ");
            failed = true;
        }

        if (!failed) {
            if (!tcfdbtune(this->fdb_[i], this->width_, this->size / this->filename_.size())) {
                LOG_TC_ERROR(this->fdb_[i], "Failed to tune fixed database: ");
                failed = true;
            }
        }
        if (!failed) {
            if (start_context.create()) {
                INFO("Creating index file " << this->filename_[i]);
            }

            CHECK(File::MakeParentDirectory(this->filename_[i], start_context.dir_mode().mode()),
                "Failed to check parent directories");

            if (!tcfdbopen(this->fdb_[i], this->filename_[i].c_str(), open_flags)) {
                LOG_TC_ERROR(this->fdb_[i], "Failed to open file " << this->filename_[i] << ": ");
                failed = true;
            }
            if (!failed && start_context.create()) {
                CHECK(chmod(this->filename_[i].c_str(), start_context.file_mode().mode()) == 0,
                    "Failed to change file permissions: " << this->filename_[i]);
                if (start_context.file_mode().gid() != -1) {
                    CHECK(chown(this->filename_[i].c_str(), -1, start_context.file_mode().gid()) == 0,
                        "Failed to change file group: " << this->filename_[i]);
                }
            }
        }
        if (failed) {
            tcfdbdel(this->fdb_[i]);
            this->fdb_[i] = NULL;
            return false;
        }
    }

    this->state_ = TC_FIXED_INDEX_STATE_STARTED;
    return true;
}

bool TCFixedIndex::GetDB(int64_t id, TCFDB** db, int64_t* db_id) {
    CHECK(db_id, "db id not set");
    CHECK(db, "db not set");

    int db_index = id % this->fdb_.size();
    *db = this->fdb_[db_index];
    *db_id = (id / this->fdb_.size()) + 1;
    return true;
}

lookup_result TCFixedIndex::Lookup(const void* key, size_t key_size,
                                   Message* message) {
    ProfileTimer timer(this->profiling);
    int ecode = 0;
    int result_size = 0;
    void* result = NULL;
    int64_t id = 0;

    CHECK_RETURN(key_size <= sizeof(int64_t), LOOKUP_ERROR, "Illegal key size: " << key_size);
    CHECK_RETURN(this->state_ == TC_FIXED_INDEX_STATE_STARTED, LOOKUP_ERROR, "Index not started");
    CHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    id = 0;
    memcpy(&id, key, key_size);

    DEBUG("Lookup id " << id);

    TCFDB* current_db = NULL;
    int64_t current_id = 0;
    CHECK_RETURN(GetDB(id, &current_db, &current_id), LOOKUP_ERROR, "Failed to get id");

    result = tcfdbget(current_db, current_id, &result_size);
    if (result == NULL) {
        // Either no entry or error
        ecode = tcfdbecode(current_db);
        if ((id == FDBIDMIN || id == FDBIDMAX) && ecode == TCEINVALID && this->GetItemCount() == 0) {
            return LOOKUP_NOT_FOUND;
        }
        CHECK_RETURN(ecode == TCENOREC, LOOKUP_ERROR, tcfdberrmsg(ecode));
        return LOOKUP_NOT_FOUND;
    }

    if (message) {
        bool b = ParseSizedMessage(message, result, result_size, checksum_).valid();
        if (!b) {
            ERROR("Failed to parse message: " << ToHexString(result, result_size));
            free(result);
            return LOOKUP_ERROR;
        }
    }
    free(result); // allocated by tc.
    return LOOKUP_FOUND;
}

put_result TCFixedIndex::Put(const void* key, size_t key_size,
                             const Message& message) {
    ProfileTimer timer(this->profiling);

    CHECK_RETURN(this->state_ == TC_FIXED_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= sizeof(int64_t), PUT_ERROR, "Illegal key size: " << key_size);

    int64_t id = 0;
    memcpy(&id, key, key_size);

    DEBUG("Write id " << id);

    TCFDB* current_db = NULL;
    int64_t current_id = 0;
    CHECK_RETURN(GetDB(id, &current_db, &current_id), PUT_ERROR, "Failed to get id");

    string target;
    CHECK_RETURN(SerializeSizedMessageToString(message, &target, checksum_),
        PUT_ERROR,
        "Failed to serialize message: " << message.ShortDebugString());

    if (!tcfdbtranbegin(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to begin transaction: ");
        return PUT_ERROR;
    }

    bool r = tcfdbput(current_db, current_id, target.data(), target.size());
    if (!r) {
        if (!tcfdbtranabort(current_db)) {
            LOG_TC_WARNING(current_db, "Failed to abort transaction");
        }
        LOG_TC_ERROR(current_db, "Failed to put value: " << ": id " << id <<
            ", current id " << current_id <<
            ", data " << message.DebugString() <<
            ", limit id " << tcfdblimid(current_db) <<
            ", maximal id " << tcfdbmax(current_db) <<
            ", message ");
        return PUT_ERROR;
    }

    if (!tcfdbtrancommit(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to commit transaction: ");
        return PUT_ERROR;
    }

    this->version_counter_.fetch_and_increment();
    return PUT_OK;
}

enum put_result TCFixedIndex::PutIfAbsent(const void* key, size_t key_size,
                                          const Message& message) {
    ProfileTimer timer(this->profiling);
    CHECK_RETURN(this->state_ == TC_FIXED_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= sizeof(int64_t), PUT_ERROR, "Illegal key size: " << key_size);

    int64_t id = 0;
    memcpy(&id, key, key_size);

    DEBUG("Write id " << id);

    TCFDB* current_db = NULL;
    int64_t current_id = 0;
    CHECK_RETURN(GetDB(id, &current_db, &current_id), PUT_ERROR, "Failed to get id");

    string target;
    CHECK_RETURN(SerializeSizedMessageToString(message, &target, checksum_),
        PUT_ERROR,
        "Failed to serialize message: " << message.ShortDebugString());

    if (!tcfdbtranbegin(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to begin transaction");
        return PUT_ERROR;
    }

    bool r = tcfdbputkeep(current_db, current_id, target.data(), target.size());
    if (!r) {
        if (!tcfdbtranabort(current_db)) {
            LOG_TC_WARNING(current_db, "Failed to abort transaction");
        }
        int e = tcfdbecode(current_db);
        if (e == TCEKEEP) {
            return PUT_KEEP;
        }
        LOG_TC_ERROR(current_db, "Failed to put value: " << ": id " << id <<
            ", current id " << current_id <<
            ", data " << message.DebugString() <<
            ", limit id " << tcfdblimid(current_db) <<
            ", maximal id " << tcfdbmax(current_db) <<
            ", message ");
        return PUT_ERROR;
    }

    if (!tcfdbtrancommit(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to commit transaction");
        return PUT_ERROR;
    }
    this->version_counter_.fetch_and_increment();
    return PUT_OK;
}

enum delete_result TCFixedIndex::Delete(const void* key, size_t key_size) {
    ProfileTimer timer(this->profiling);

    CHECK_RETURN(this->state_ == TC_FIXED_INDEX_STATE_STARTED, DELETE_ERROR, "Index not started");
    CHECK_RETURN(key, DELETE_ERROR, "Key not set");
    CHECK_RETURN(key_size <= sizeof(int64_t), DELETE_ERROR, "Illegal key size: " << key_size);

    int64_t id = 0;
    memcpy(&id, key, key_size);

    DEBUG("Delete id " << id);

    TCFDB* current_db = NULL;
    int64_t current_id = 0;
    CHECK_RETURN(GetDB(id, &current_db, &current_id), DELETE_ERROR, "Failed to get id");
    CHECK_RETURN(current_db, DELETE_ERROR, "Failed to get current db");

    if (!tcfdbtranbegin(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to begin transaction");
        return DELETE_ERROR;
    }

    bool r = tcfdbout(current_db, current_id);
    if (!r) {
        if (!tcfdbtranabort(current_db)) {
            LOG_TC_WARNING(current_db, "Failed to abort transaction");
        }
        int e = tcfdbecode(current_db);
        if (e == TCENOREC) {
            return DELETE_NOT_FOUND;
        }
        ERROR(tcfdberrmsg(e));
        return DELETE_ERROR;
    }

    if (!tcfdbtrancommit(current_db)) {
        LOG_TC_ERROR(current_db, "Failed to commit transaction");
        return DELETE_ERROR;
    }

    this->version_counter_.fetch_and_increment();
    return DELETE_OK;
}

TCFixedIndex::~TCFixedIndex() {
    for (size_t i = 0; i < this->fdb_.size(); i++) {
        if (this->fdb_[i]) {
            if (!tcfdbclose(this->fdb_[i])) {
                WARNING(tcfdberrmsg(tcfdbecode(this->fdb_[i])));
            }
            tcfdbdel(this->fdb_[i]);
            this->fdb_[i] = NULL;
        }
    }
    this->fdb_.clear();
}

string TCFixedIndex::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"index\": " << this->profiling.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

uint64_t TCFixedIndex::GetPersistentSize() {
    uint64_t size_sum = 0;
    for (size_t i = 0; i < this->fdb_.size(); i++) {
        size_sum += tcfdbfsiz(this->fdb_[i]);
    }
    return size_sum;
}

string TCFixedIndex::PrintTrace() {
    uint64_t size_sum = 0;
    uint64_t limit_size = 0;

    for (size_t i = 0; i < this->fdb_.size(); i++) {
        size_sum += tcfdbfsiz(this->fdb_[i]);
        limit_size += tcfdblimsiz(this->fdb_[i]);
    }

    stringstream sstr;
    sstr << "{";
    sstr << "\"limit id\": " << GetLimitId() << "," << std::endl;
    sstr << "\"limit size\": " << limit_size << "," << std::endl;
    sstr << "\"items\": " << GetItemCount() << "," << std::endl;
    sstr << "\"size\": " << size_sum << std::endl;
    sstr << "}";
    return sstr.str();
}

uint64_t TCFixedIndex::GetItemCount() {
    uint64_t items_sum = 0;
    for (size_t i = 0; i < this->fdb_.size(); i++) {
        items_sum += tcfdbrnum(this->fdb_[i]);
    }
    return items_sum;
}

int64_t TCFixedIndex::GetLimitId() {
    CHECK_RETURN(this->fdb_.size() > 0, 0, "Database not set");

    TCFDB* last_db = this->fdb_[this->fdb_.size() - 1];
    CHECK_RETURN(last_db, 0, "Database not set");
    uint64_t limit_id = tcfdblimid(last_db);
    limit_id *= this->fdb_.size();
    limit_id -= 1; // due to the remapping from 0 to 1
    return limit_id;
}

int64_t TCFixedIndex::GetMaxId() {
    uint64_t max_id = 0;
    for (size_t i = 0; i < this->fdb_.size(); i++) {
        uint64_t local_max_id = tcfdbmax(this->fdb_[i]);
        if (local_max_id > 0) {
            local_max_id =  ((local_max_id - 1) * this->fdb_.size()) + (i + 1);
            if (local_max_id > max_id) {
                max_id = local_max_id;
            }
        }
    }
    if (max_id == 0) {
        return -1; // no items
    }
    max_id -= 1; // due to the remapping from 0 to 1
    return max_id;
}

int64_t TCFixedIndex::GetMinId() {
    uint64_t min_id = LONG_MAX;
    for (size_t i = 0; i < this->fdb_.size(); i++) {
        uint64_t local_min_id = tcfdbmin(this->fdb_[i]);
        if (local_min_id > 0) {
            local_min_id =  ((local_min_id - 1) * this->fdb_.size()) + (i + 1);
            if (local_min_id < min_id) {
                min_id = local_min_id;
            }
        }
    }
    if (min_id == LONG_MAX) {
        return -1;
    }
    min_id -= 1; // due to the remapping from 0 to 1
    return min_id;
}

IndexIterator* TCFixedIndex::CreateIterator() {
    if (this->state_ != TC_FIXED_INDEX_STATE_STARTED) {
        return NULL;
    }
    return new TCFixedIndexIterator(this);
}

TCFixedIndexIterator::TCFixedIndexIterator(TCFixedIndex* index_) {
    this->index_ = index_;
    this->fixed_index_ = 0;
    this->version_counter_ = index_->version_counter_;
    tcfdbiterinit(this->index_->fdb_[this->fixed_index_]);
}

TCFixedIndexIterator::~TCFixedIndexIterator() {
}

lookup_result TCFixedIndexIterator::Next(void* key, size_t* key_size,
                                         Message* message) {
    int result_size = 0;
    void* result = NULL;
    uint64_t iter_key;
    CHECK_RETURN(this->version_counter_ == this->index_->version_counter_, LOOKUP_ERROR, "Concurrent modification error");
    iter_key = tcfdbiternext(this->index_->fdb_[this->fixed_index_]);
    if (iter_key == 0) {
        this->fixed_index_++;
        for (; static_cast<size_t>(this->fixed_index_) < this->index_->fdb_.size() && iter_key == 0; this->fixed_index_++) {
            tcfdbiterinit(this->index_->fdb_[this->fixed_index_]);
            iter_key = tcfdbiternext(this->index_->fdb_[this->fixed_index_]);

            if (iter_key != 0) {
                break;
            }
        }
        if (iter_key == 0) {
            return LOOKUP_NOT_FOUND;
        }
    }
    if (key) {
        CHECK_GOTO(*key_size >= sizeof(uint64_t), "Illegal key size " << (*key_size));
        uint64_t user_key = (iter_key - 1) * this->index_->fdb_.size() + (this->fixed_index_ + 1);
        user_key--;
        memcpy(key, &user_key, sizeof(uint64_t));
        *key_size = sizeof(uint64_t);
    }

    result_size = 0;
    result = tcfdbget(this->index_->fdb_[this->fixed_index_], iter_key, &result_size);
    if (result == NULL) {
        // Either no entry or error
        int ecode = tcfdbecode(this->index_->fdb_[this->fixed_index_]);
        CHECK_GOTO(ecode == TCENOREC, tcfdberrmsg(ecode));
        return LOOKUP_NOT_FOUND;
    }
    if (message) {
        CHECK_GOTO(ParseSizedMessage(message, result, result_size, this->index_->checksum_).valid(),
            "Failed to parse message");
    }
    free(result); // allocated by tc.
    return LOOKUP_FOUND;
error:
    if (result) {
        free(result);
    }
    return LOOKUP_ERROR;
}

}
}
