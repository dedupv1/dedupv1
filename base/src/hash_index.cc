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
#include <base/hash_index.h>
#include <base/logging.h>
#include <base/index.h>
#include <base/hashing_util.h>
#include <base/locks.h>
#include <base/strutil.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>

#include <sstream>

using std::string;
using std::stringstream;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToStorageUnit;
using google::protobuf::Message;

LOGGER("HashIndex");

namespace dedupv1 {
namespace base {

void HashIndex::RegisterIndex() {
    Index::Factory().Register("mem-chained-hash", &HashIndex::CreateIndex);
}

Index* HashIndex::CreateIndex() {
    Index* i = new HashIndex();
    return i;
}

bool HashIndex::SetOption(const string& option_name, const string& option) {
    if (option_name == "buckets") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->bucket_count = ToStorageUnit(option).value();
        CHECK(this->bucket_count > 0, "Illegal option " << option);
        return true;
    }
    if (option_name == "sub-buckets") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->sub_bucket_count = ToStorageUnit(option).value();
        CHECK(this->sub_bucket_count > 0, "Illegal option " << option);
        return true;
    }
    if (option_name == "lock-count") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->lock_count = ToStorageUnit(option).value();
        CHECK(this->lock_count > 0, "Illegal option " << option);
        return true;
    }
    return Index::SetOption(option_name, option);
}

bool HashIndex::Start(const StartContext& start_context) {
    unsigned int i = 0;

    CHECK(this->bucket_count > 0, "Buckets not set");

    /* Init buckets */
    this->buckets = new HashEntry * *[this->bucket_count];
    CHECK(this->buckets, "Alloc hash buckets failed");
    memset(this->buckets, 0, this->bucket_count * sizeof(HashEntry * *));

    for (i = 0; i < this->bucket_count; i++) {
        this->buckets[i] = new HashEntry *[this->sub_bucket_count];
        CHECK(this->buckets[i], "Alloc hash sub buckes failed");
        memset(this->buckets[i], 0, this->sub_bucket_count  * sizeof(HashEntry*));
    }
    /* Init locks */
    this->lock.Init(this->lock_count);
    return true;
}

HashIndex::HashIndex() : MemoryIndex(RETURNS_DELETE_NOT_FOUND | COMPARE_AND_SWAP | PUT_IF_ABSENT) {
    this->bucket_count = 0;
    this->sub_bucket_count = 1;
    this->lock_count = 16;
    this->buckets = NULL;
    this->item_count = 0;
    memset(&this->statistics, 0, sizeof(struct hash_index_statistics));
}

bool HashIndex::Clear() {
    if (!this->buckets) {
        return true;
    }

    for (int i = 0; i < this->bucket_count; i++) {
        unsigned int lock_index = i % this->lock_count;
        ScopedReadWriteLock scoped_lock(this->lock.Get(lock_index));
        CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire bucket lock");

        for (int j = 0; j < this->sub_bucket_count; j++) {
            while (this->buckets[i][j]) {
                HashEntry* entry = this->buckets[i][j];
                this->buckets[i][j] = entry->next;
                delete entry;

                this->item_count.fetch_and_decrement();
            }
        }

        CHECK(scoped_lock.ReleaseLock(), "Failed to release bucket lock");
    }
    return true;
}

lookup_result HashIndex::Lookup(const void* key, size_t key_size,
                                Message* message) {
    enum lookup_result result = LOOKUP_NOT_FOUND;
    unsigned int bucket_id = 0;
    unsigned int sub_bucket_id = 0;
    unsigned lock_index = 0;
    ReadWriteLock* lock = NULL;
    unsigned int count = 0;
    HashEntry* entry = NULL;
    HashEntry** sub_bucket = NULL;
    uint32_t hash_value = 0;

    CHECK_RETURN(this->buckets, LOOKUP_ERROR, "Index not started");
    CHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    murmur_hash3_x86_32(key, key_size, 0, &hash_value);
    bucket_id = hash_value % this->bucket_count;
    lock_index = bucket_id % this->lock_count;
    lock = this->lock.Get(lock_index);

    CHECK_RETURN(lock->AcquireReadLockWithStatistics(&this->statistics.rdlock_free,
            &this->statistics.rdlock_busy), LOOKUP_ERROR, "Lock failed");

    sub_bucket = this->buckets[bucket_id];
    sub_bucket_id = (hash_value >> 16) % this->sub_bucket_count;
    entry = sub_bucket[sub_bucket_id];
    count = 0;
    while (entry) {
        count++;
        if (raw_compare(entry->key, entry->key_size, key, key_size) == 0) {
            if (message) {
                CHECK_RETURN(message->ParseFromArray(entry->data, entry->data_size), LOOKUP_ERROR,
                    "Failed to parse message from entry data");
            }
            result = LOOKUP_FOUND;
            break;
        }
        entry = entry->next;
    }
    this->statistics.read_operations++;
    this->statistics.linked_list_length += count;

    /* Release lock */
    CHECK_RETURN(lock->ReleaseLock(), LOOKUP_ERROR, "Unlock failed");
    return result;
}

HashEntry::HashEntry(HashEntry* next) {
    this->key = NULL;
    this->key_size = 0;
    this->data = NULL;
    this->data_size = 0;
    this->next = next;
}

bool HashEntry::Assign(const void* key, size_t key_size, const Message& message) {
    if (!AssignValue(message)) {
        return false;
    }

    this->key = new byte[key_size];
    CHECK(this->key, "Failed to alloc key");
    memcpy(this->key, key, key_size);
    this->key_size = key_size;
    return true;
}

bool HashEntry::AssignValue(const Message& message) {
    byte* buffer = new byte[message.ByteSize()];
    if (!buffer) {
        ERROR("Failed to alloc data");
        return false;
    }
    if (!message.SerializeWithCachedSizesToArray(buffer)) {
        ERROR("Failed to serialize message");
        delete[] buffer;
        return false;
    }
    if (this->data) {
        delete[] reinterpret_cast<byte*>(this->data);
    }
    this->data = buffer;
    this->data_size = message.GetCachedSize();
    return true;
}

enum put_result HashEntry::CompareAndSwap(const Message& message,
                                          const Message& compare_message,
                                          Message* result_message) {
    byte* compare_buffer = new byte[compare_message.ByteSize()];
    if (!compare_buffer) {
        ERROR("Failed to alloc data");
        return PUT_ERROR;
    }
    if (!compare_message.SerializeWithCachedSizesToArray(compare_buffer)) {
        ERROR("Failed to serialize message");
        delete[] compare_buffer;
        return PUT_ERROR;
    }

    if (raw_compare(compare_buffer, compare_message.ByteSize(), this->data, this->data_size) == 0) {
        delete[] compare_buffer;
        compare_buffer = NULL;

        if (!this->AssignValue(message)) {
            ERROR("Failed to assign message. " << message.DebugString());
            delete[] compare_buffer;
            return PUT_ERROR;
        }
        result_message->CopyFrom(message);
        // no change
        return PUT_OK;
    } else {
        delete[] compare_buffer;
        compare_buffer = NULL;

        CHECK_RETURN(result_message->ParseFromArray(data, data_size), PUT_ERROR,
            "Failed to parse message from entry data");

        return PUT_KEEP;
    }
}

HashEntry::~HashEntry() {
    if (data) {
        delete[] reinterpret_cast<byte*>(data);
    }
    if (key) {
        delete[] reinterpret_cast<byte*>(key);
    }
}

put_result HashIndex::Put(const void* key, size_t key_size,
                          const Message& message) {
    int found = false;
    unsigned int bucket_id = 0, sub_bucket_id = 0;
    unsigned int lock_index = 0;
    ReadWriteLock* lock = NULL;
    HashEntry* entry = NULL;
    HashEntry* new_entry = NULL;
    HashEntry** sub_bucket = NULL;
    uint32_t hash_value = 0;
    enum put_result result = PUT_OK;

    CHECK_RETURN(this->buckets, PUT_ERROR, "Index not started");

    murmur_hash3_x86_32(key, key_size, 0, &hash_value);
    bucket_id = hash_value % this->bucket_count;
    lock_index = bucket_id % this->lock_count;
    lock = this->lock.Get(lock_index);

    CHECK_RETURN(lock->AcquireWriteLockWithStatistics(&this->statistics.wrlock_free,
            &this->statistics.wrlock_busy), PUT_ERROR, "Lock failed");

    sub_bucket = this->buckets[bucket_id];
    sub_bucket_id = (hash_value >> 16) % this->sub_bucket_count;
    entry = sub_bucket[sub_bucket_id];
    while (entry) {
        if (raw_compare(entry->key, entry->key_size, key, key_size) == 0) {
            /* Found entry with that key => Operation overwrites old data */
            CHECK_GOTO(entry->AssignValue(message), "Failed to assign data");
            found = true;
        }
        entry = entry->next;
    }

    if (!found) {
        /* No match found */
        new_entry = new HashEntry(this->buckets[bucket_id][sub_bucket_id]);
        CHECK_GOTO(new_entry, "Alloc hash entry failed");
        CHECK_GOTO(new_entry->Assign(key, key_size, message), "Failed to assign data");
        this->buckets[bucket_id][sub_bucket_id] = new_entry;
        this->item_count.fetch_and_increment();
    }
    CHECK_RETURN(lock->ReleaseLock(), PUT_ERROR, "Unlock failed");

    return result;
error:
    if (new_entry) {
        delete new_entry;
        new_entry = NULL;
    }
    CHECK_RETURN(lock->ReleaseLock(), PUT_ERROR, "Unlock failed");
    return PUT_ERROR;
}

put_result HashIndex::PutIfAbsent(const void* key, size_t key_size,
                                  const Message& message) {
    int found = false;
    unsigned int bucket_id = 0, sub_bucket_id = 0;
    unsigned int lock_index = 0;
    ReadWriteLock* lock = NULL;
    HashEntry* entry = NULL;
    HashEntry* new_entry = NULL;
    HashEntry** sub_bucket = NULL;
    uint32_t hash_value = 0;
    enum put_result result = PUT_ERROR;

    CHECK_RETURN(this->buckets, PUT_ERROR, "Index not started");

    murmur_hash3_x86_32(key, key_size, 0, &hash_value);
    bucket_id = hash_value % this->bucket_count;
    lock_index = bucket_id % this->lock_count;
    lock = this->lock.Get(lock_index);
    CHECK_RETURN(lock->AcquireWriteLockWithStatistics(&this->statistics.wrlock_free,
            &this->statistics.wrlock_busy), PUT_ERROR, "Lock failed");

    sub_bucket = this->buckets[bucket_id];
    sub_bucket_id = (hash_value >> 16) % this->sub_bucket_count;
    entry = sub_bucket[sub_bucket_id];
    while (entry) {
        if (raw_compare(entry->key, entry->key_size, key, key_size) == 0) {
            result = PUT_KEEP;
            found = true;
        }
        entry = entry->next;
    }

    if (!found) {
        /* No match found */
        new_entry = new HashEntry(this->buckets[bucket_id][sub_bucket_id]);
        CHECK_GOTO(new_entry, "Alloc hash entry failed");
        CHECK_GOTO(new_entry->Assign(key, key_size, message), "Failed to assign data");
        this->buckets[bucket_id][sub_bucket_id] = new_entry;
        this->item_count.fetch_and_increment();
        result = PUT_OK;
    }
    CHECK_RETURN(lock->ReleaseLock(), PUT_ERROR, "Unlock failed");

    return result;
error:
    if (new_entry) {
        delete new_entry;
        new_entry = NULL;
    }
    CHECK_RETURN(lock->ReleaseLock(), PUT_ERROR, "Unlock failed");
    return PUT_ERROR;
}

enum put_result HashIndex::CompareAndSwap(const void* key, size_t key_size,
                                          const Message& message,
                                          const Message& compare_message,
                                          Message* result_message) {
    int found = false;
    unsigned int bucket_id = 0, sub_bucket_id = 0;
    unsigned int lock_index = 0;
    ReadWriteLock* lock = NULL;
    HashEntry* entry = NULL;
    HashEntry** sub_bucket = NULL;
    uint32_t hash_value = 0;
    enum put_result result = PUT_ERROR;

    CHECK_RETURN(this->buckets, PUT_ERROR, "Index not started");

    murmur_hash3_x86_32(key, key_size, 0, &hash_value);
    bucket_id = hash_value % this->bucket_count;
    lock_index = bucket_id % this->lock_count;
    lock = this->lock.Get(lock_index);
    CHECK_RETURN(lock->AcquireWriteLockWithStatistics(&this->statistics.wrlock_free,
            &this->statistics.wrlock_busy), PUT_ERROR, "Lock failed");

    sub_bucket = this->buckets[bucket_id];
    sub_bucket_id = (hash_value >> 16) % this->sub_bucket_count;
    entry = sub_bucket[sub_bucket_id];
    while (entry) {
        if (raw_compare(entry->key, entry->key_size, key, key_size) == 0) {
            result = entry->CompareAndSwap(message, compare_message, result_message);
            CHECK_GOTO(result != PUT_ERROR, "Failed to compare-and-swap data");
            found = true;
        }
        entry = entry->next;
    }

    if (!found) {
        /* No match found */
        ERROR("Failed to find entry in hash index");
        result = PUT_ERROR;
    }
    CHECK_RETURN(lock->ReleaseLock(), PUT_ERROR, "Unlock failed");

    return result;
error:
    CHECK_RETURN(lock->ReleaseLock(), PUT_ERROR, "Unlock failed");
    return PUT_ERROR;
}

delete_result HashIndex::Delete(const void* key, size_t key_size) {
    HashEntry* entry = NULL;
    HashEntry* last = NULL;
    HashEntry** sub_bucket = NULL;
    unsigned int bucket_id = 0, sub_bucket_id = 0;
    unsigned int lock_index = 0;
    uint32_t hash_value = 0;
    ReadWriteLock* lock = NULL;

    CHECK_RETURN(this->buckets, DELETE_ERROR, "Index not started");

    murmur_hash3_x86_32(key, key_size, 0, &hash_value);
    bucket_id = hash_value % this->bucket_count;
    lock_index = bucket_id % this->lock_count;
    lock = this->lock.Get(lock_index);
    CHECK_RETURN(lock->AcquireWriteLockWithStatistics(&this->statistics.wrlock_free,
            &this->statistics.wrlock_busy), DELETE_ERROR, "Lock failed");

    sub_bucket = this->buckets[bucket_id];
    sub_bucket_id = (hash_value >> 16) % this->sub_bucket_count;
    entry = sub_bucket[sub_bucket_id];
    last = NULL;
    enum delete_result result = DELETE_NOT_FOUND;
    while (entry) {
        if (raw_compare(entry->key, entry->key_size, key, key_size) == 0) {
            if (last) {
                last->next = entry->next;
            } else {
                this->buckets[bucket_id][sub_bucket_id] = entry->next;
            }
            this->item_count.fetch_and_decrement();
            result = DELETE_OK;
            delete entry;
            break;
        }
        last = entry;
        entry = entry->next;
    }
    /* Release lock */
    CHECK_RETURN(lock->ReleaseLock(), DELETE_ERROR, "Unlock failed");
    return result;
}

HashIndex::~HashIndex() {
    unsigned int i, j;

    if (this->buckets) {
        for (i = 0; i < this->bucket_count; i++) {
            HashEntry** sub_bucket = this->buckets[i];
            for (j = 0; j < this->sub_bucket_count; j++) {
                HashEntry* entry = sub_bucket[j];
                while (entry) {
                    HashEntry* next = entry->next;
                    delete entry;
                    entry = next;
                }
            }
            delete [] this->buckets[i];
            this->buckets[i] = NULL;
        }
        delete[] this->buckets;
        this->buckets = NULL;
    }
}

uint64_t HashIndex::GetMemorySize() {
    return 0;
}

uint64_t HashIndex::GetItemCount() {
    return this->item_count;
}

string HashIndex::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"read lock free\":" << this->statistics.rdlock_free << "," << std::endl;
    sstr << "\"read lock busy\": " << this->statistics.rdlock_busy << "," << std::endl;
    sstr << "\"write lock free\": " << this->statistics.wrlock_free << "," << std::endl;
    sstr << "\"write lock busy\": " << this->statistics.wrlock_busy << "" << std::endl;
    sstr << "}";
    return sstr.str();
}

}
}

