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

#include <base/tc_hash_mem_index.h>

#include <sstream>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>

#include <tcutil.h>
#include <tchdb.h>
#include <stdint.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

#include <base/index.h>
#include <base/base.h>
#include <base/hashing_util.h>
#include <base/locks.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/timer.h>
#include <base/memory.h>
#include <base/protobuf_util.h>

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

LOGGER("TCMemHashIndex");

namespace dedupv1 {
namespace base {

void TCMemHashIndex::RegisterIndex() {
    Index::Factory().Register("tc-mem-hash", &TCMemHashIndex::CreateIndex);
}

Index* TCMemHashIndex::CreateIndex() {
    Index* i = new TCMemHashIndex();
    return i;
}

TCMemHashIndex::TCMemHashIndex() : MemoryIndex(HAS_ITERATOR | RETURNS_DELETE_NOT_FOUND | RAW_ACCESS | COMPARE_AND_SWAP | PUT_IF_ABSENT) {
    this->mdb_ = NULL;
    this->buckets_ = 0;
    this->state_ = TC_HASH_MEM_INDEX_STATE_CREATED;
    version_counter_ = 0;
    iterator_counter_ = 0;
    checksum_ = true;
}

bool TCMemHashIndex::SetOption(const string& option_name, const string& option) {
    if (option_name == "bucket-count") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->buckets_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "checksum") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->checksum_ = To<bool>(option).value();
        return true;
    }
    return Index::SetOption(option_name, option);
}

bool TCMemHashIndex::Start(const StartContext& start_context) {
    CHECK(this->state_ == TC_HASH_MEM_INDEX_STATE_CREATED, "Index in invalid state");

    this->mdb_ = tcmdbnew2(this->buckets_);
    CHECK(this->mdb_, "Cannot create in-memory hash table");

    this->state_ = TC_HASH_MEM_INDEX_STATE_STARTED;
    return true;
}

bool TCMemHashIndex::Clear() {
    if (this->state_ != TC_HASH_MEM_INDEX_STATE_STARTED) {
        return true;
    }
    CHECK(this->mdb_, "Memory database not set");

    tcmdbvanish(this->mdb_);
    return true;
}

enum lookup_result TCMemHashIndex::RawLookup(const void* key, size_t key_size,
                                             void* value, size_t* value_size) {
    DCHECK_RETURN(this->mdb_, LOOKUP_ERROR, "Memory database not set");
    DCHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    ProfileTimer timer(this->lookup_time_);

    CHECK_RETURN(this->state_ == TC_HASH_MEM_INDEX_STATE_STARTED, LOOKUP_ERROR, "Index not started");

    int result_size = 0;
    void* region = tcmdbget(this->mdb_, key, key_size, &result_size);
    if (!region) {
        return LOOKUP_NOT_FOUND;
    }
    bool b = true;
    if (value) {
        DCHECK_RETURN(value_size, LOOKUP_ERROR, "Value size not set");
        if (result_size > (*value_size)) {
            *value_size = result_size;
            return LOOKUP_ERROR;
        }

        memcpy(value, region, result_size);
        *value_size = result_size;
    } else if (value_size) {
        *value_size = result_size;
    }

    free(region); // allocated in TC
    return b ? LOOKUP_FOUND : LOOKUP_ERROR;
}

enum lookup_result TCMemHashIndex::Lookup(
    const void* key, size_t key_size,
    Message* message) {
    DCHECK_RETURN(this->mdb_, LOOKUP_ERROR, "Memory database not set");
    DCHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    ProfileTimer timer(this->lookup_time_);

    CHECK_RETURN(this->state_ == TC_HASH_MEM_INDEX_STATE_STARTED, LOOKUP_ERROR, "Index not started");

    int result_size = 0;
    void* region = tcmdbget(this->mdb_, key, key_size, &result_size);
    if (!region) {
        return LOOKUP_NOT_FOUND;
    }
    bool b = true;
    if (message) {
        b = ParseSizedMessage(message, region, result_size, checksum_).valid();
        if (!b) {
            ERROR("Failed to parse message: " << ToHexString(region, result_size));
        }
    }
    free(region); // allocated in TC
    return b ? LOOKUP_FOUND : LOOKUP_ERROR;
}

enum put_result TCMemHashIndex::RawPut(
    const void* key, size_t key_size,
    const void* value, size_t value_size) {
    DCHECK_RETURN(this->mdb_, PUT_ERROR, "Memory database not set");
    DCHECK_RETURN(key, PUT_ERROR, "Key not set");
    DCHECK_RETURN(key_size <= INT_MAX, PUT_ERROR, "Key size too large");
    CHECK_RETURN(this->state_ == TC_HASH_MEM_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");

    ProfileTimer timer(this->update_time_);
    tcmdbput(this->mdb_, key, key_size, value, value_size); // tcmdbput returns void
    this->version_counter_.fetch_and_increment();
    return PUT_OK;
}

enum put_result TCMemHashIndex::Put(
    const void* key, size_t key_size,
    const Message& message) {
    ProfileTimer timer(this->update_time_);
    CHECK_RETURN(this->state_ == TC_HASH_MEM_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(this->mdb_, PUT_ERROR, "Memory database not set");

    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= INT_MAX, PUT_ERROR, "Key size too large");

    string target;
    CHECK_RETURN(SerializeSizedMessageToString(message, &target, checksum_),
        PUT_ERROR,
        "Failed to serialize message: " << message.ShortDebugString());

    tcmdbput(this->mdb_, key, key_size, target.data(), target.size()); // tcmdbput returns void
    this->version_counter_.fetch_and_increment();
    return PUT_OK;
}

void* TCMemHashIndex::TCDuplicationHandler(const void *vbuf, int vsiz, int *sp, void *op) {
    bool* b = reinterpret_cast<bool*>(op);
    *b = true; // it is a duplicate
    return NULL; // no not keep
}

enum put_result TCMemHashIndex::RawPutIfAbsent(
    const void* key, size_t key_size,
    const void* value, size_t value_size) {
    ProfileTimer timer(this->update_time_);
    CHECK_RETURN(this->state_ == TC_HASH_MEM_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");
    DCHECK_RETURN(this->mdb_, PUT_ERROR, "Memory database not set");
    DCHECK_RETURN(key, PUT_ERROR, "Key not set");
    DCHECK_RETURN(key_size <= INT_MAX, PUT_ERROR, "Key size too large");

    bool duplicate = false;
    bool r = tcmdbputproc(this->mdb_, key, key_size, value, value_size, &TCDuplicationHandler, &duplicate);
    if (likely(r)) {
        this->version_counter_.fetch_and_increment();
        return PUT_OK;
    } else if (duplicate) {
        return PUT_KEEP;
    } else {
        ERROR("Failed to put data into memory hash index: " << ToHexString(key, key_size));
        return PUT_ERROR;
    }
}

enum put_result TCMemHashIndex::CompareAndSwap(const void* key, size_t key_size,
                                               const Message& message,
                                               const Message& compare_message,
                                               Message* result_message) {
    ProfileTimer timer(this->update_time_);
    CHECK_RETURN(this->state_ == TC_HASH_MEM_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");
    DCHECK_RETURN(this->mdb_, PUT_ERROR, "Memory database not set");
    DCHECK_RETURN(key, PUT_ERROR, "Key not set");
    DCHECK_RETURN(key_size <= INT_MAX, PUT_ERROR, "Key size too large");

    string target;
    CHECK_RETURN(SerializeSizedMessageToString(message, &target, checksum_),
        PUT_ERROR,
        "Failed to serialize message: " << message.ShortDebugString());

    string compare_target;
    CHECK_RETURN(SerializeSizedMessageToString(compare_message, &compare_target, checksum_),
        PUT_ERROR,
        "Failed to serialize compare message: " << compare_message.ShortDebugString());

    int sp;
    void* r = tcmdbcas(this->mdb_, key, key_size, target.data(), target.size(),
        compare_target.data(), compare_target.size(), &sp);
    CHECK_RETURN(r, PUT_ERROR, "Failed to find value in hash mem index");
    if (raw_compare(target.data(), target.size(), r, sp) == 0) {
        free(r);
        r = NULL;
        result_message->CopyFrom(message);
        return PUT_OK;
    } else {
        Option<size_t> b = ParseSizedMessage(result_message, r, sp, checksum_);
        if (!b.valid()) {
            ERROR("Failed to parse message: " << ToHexString(r, sp));
            free(r);
            return PUT_ERROR;
        }
        free(r);
        r = NULL;
        return PUT_KEEP;
    }
}

enum put_result TCMemHashIndex::PutIfAbsent(
    const void* key, size_t key_size,
    const Message& message) {
    ProfileTimer timer(this->update_time_);
    CHECK_RETURN(this->state_ == TC_HASH_MEM_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");
    DCHECK_RETURN(this->mdb_, PUT_ERROR, "Memory database not set");
    DCHECK_RETURN(key, PUT_ERROR, "Key not set");
    DCHECK_RETURN(key_size <= INT_MAX, PUT_ERROR, "Key size too large");

    string target;
    CHECK_RETURN(SerializeSizedMessageToString(message, &target, checksum_),
        PUT_ERROR,
        "Failed to serialize message: " << message.ShortDebugString());

    bool duplicate = false;
    bool r = tcmdbputproc(this->mdb_, key, key_size, target.data(), target.size(), &TCDuplicationHandler, &duplicate);
    if (likely(r)) {
        this->version_counter_.fetch_and_increment();
        return PUT_OK;
    } else if (duplicate) {
        return PUT_KEEP;
    } else {
        ERROR("Failed to put data into memory hash index: " << ToHexString(key, key_size) << ", message" << message.ShortDebugString());
        return PUT_ERROR;
    }
}

enum delete_result TCMemHashIndex::Delete(const void* key, size_t key_size) {
    ProfileTimer timer(this->update_time_);
    CHECK_RETURN(this->state_ == TC_HASH_MEM_INDEX_STATE_STARTED, DELETE_ERROR, "Index not started");
    CHECK_RETURN(key, DELETE_ERROR, "Key not set");
    CHECK_RETURN(key_size <= INT_MAX, DELETE_ERROR, "Key size too large");
    CHECK_RETURN(this->mdb_, DELETE_ERROR, "Memory database not set");

    bool r = tcmdbout(this->mdb_, key, key_size);
    if (r == false) {
        return DELETE_NOT_FOUND;
    }

    this->version_counter_.fetch_and_increment();
    return DELETE_OK;
}

TCMemHashIndex::~TCMemHashIndex() {
    if (this->mdb_) {
        tcmdbdel(this->mdb_);
        this->mdb_ = NULL;
    }
}

string TCMemHashIndex::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"update time\": " << this->update_time_.GetSum() << "," << std::endl;
    sstr << "\"lookup time\": " << this->lookup_time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

uint64_t TCMemHashIndex::GetMemorySize() {
    if (this->mdb_ == NULL) {
        return 0;
    }
    return tcmdbmsiz(this->mdb_);
}

uint64_t TCMemHashIndex::GetItemCount() {
    if (this->mdb_ == NULL) {
        return 0;
    }
    return tcmdbrnum(this->mdb_);
}

IndexIterator* TCMemHashIndex::CreateIterator() {
    CHECK_RETURN(state_ == TC_HASH_MEM_INDEX_STATE_STARTED, NULL, "Illegal state: " << state_);
    CHECK_RETURN(iterator_counter_ == 0, NULL, "Already open iterators");

    int c = iterator_counter_.compare_and_swap(1, 0);
    CHECK_RETURN(c == 0, NULL, "Already open iterators");
    return new TCMemHashIndexIterator(this);
}

TCMemHashIndexIterator::~TCMemHashIndexIterator() {
    index_->iterator_counter_--;
}

TCMemHashIndexIterator::TCMemHashIndexIterator(TCMemHashIndex* index_) {
    this->index_ = index_;
    this->hash_index_ = 0;
    this->version_counter_ = index_->version_counter_;
    tcmdbiterinit(index_->mdb_);
}

enum lookup_result TCMemHashIndexIterator::Next(void* key, size_t* key_size, Message* message) {
    CHECK_RETURN(this->version_counter_ == this->index_->version_counter_, LOOKUP_ERROR, "Concurrent modification error");

    void *iter_key = NULL;
    int iter_key_size = 0;
    void *result = NULL;
    int result_size = 0;

    // get the key
    iter_key = tcmdbiternext(index_->mdb_, &iter_key_size);

    if (key) {
        CHECK_GOTO(*key_size >= (size_t) iter_key_size, "Illegal key size " << (*key_size));
        memcpy(key, iter_key, iter_key_size);
        *key_size = iter_key_size;
    }

    // get the entry
    result = tcmdbget(index_->mdb_, iter_key, iter_key_size, &result_size);
    if (result == NULL) {
        return LOOKUP_NOT_FOUND;
    }

    if (message) {
        CHECK_GOTO(ParseSizedMessage(message, result, result_size, this->index_->checksum_).valid(),
            "Failed to parse message");
    }
    free(iter_key); // allocated by tc.
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
