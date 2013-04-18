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

#include <base/disk_hash_cache_page.h>

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
using google::protobuf::Message;
using dedupv1::base::Option;
using tbb::spin_mutex;
using std::map;
using dedupv1::base::make_bytestring;
using dedupv1::base::ScopedArray;

LOGGER("DiskHashIndex");

namespace {
template<class T> T GetFromBuffer(const byte* b, off_t offset) {
    const byte* p = b + offset;
    const T* t = (const T *) p;
    return *t;
}

template<class T> void SetToBuffer(byte* b, off_t offset, T value) {
    byte* p = b + offset;
    T* t = (T *) p;
    *t = value;
}
}

namespace dedupv1 {
namespace base {
namespace internal {

lookup_result DiskHashCachePage::Search(const void* key, size_t key_size, Message* message, bool* is_dirty,
                                        bool* is_pinned) {
    DCHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    TRACE("Search cache page: bucket " << bucket_id_ << ", key " << ToHexString(key, key_size));

    DiskHashCacheEntry entry(buffer_, buffer_size_, max_key_size_, max_value_size_);
    lookup_result cache_lr = IterateInit(&entry);
    while (cache_lr == LOOKUP_FOUND) {
        if (raw_compare(entry.key(), entry.key_size(), key, key_size) == 0) {
            /* Found it */
            if (message) {
                CHECK_RETURN(message->ParseFromArray(entry.value(), entry.value_size()),
                    LOOKUP_ERROR, "Failed to parse message: " <<
                    "key " << ToHexString(key, key_size) <<
                    ", value " << ToHexString(entry.value(), entry.value_size()) <<
                    ", size " << entry.value_size() <<
                    ", entry " << entry.DebugString() <<
                    ", cache page " << this->DebugString() <<
                    ", message " << message->InitializationErrorString());
            }
            if (is_dirty) {
                *is_dirty = entry.is_dirty();
            }
            if (is_pinned) {
                *is_pinned = entry.is_pinned();
            }
            return LOOKUP_FOUND;
        }
        cache_lr = Iterate(&entry);
    }
    return LOOKUP_NOT_FOUND;
}

bool DiskHashCachePage::DropAllPinned(uint64_t* dropped_item_count) {
    TRACE("Drop all pinned from cache page: bucket " << bucket_id_);

    if (dropped_item_count) {
      *dropped_item_count = 0;
    }

    DiskHashCacheEntry entry(buffer_, buffer_size_, max_key_size_, max_value_size_);
    lookup_result cache_lr = IterateInit(&entry);
    while (cache_lr == LOOKUP_FOUND) {
        if (entry.is_pinned()) {
            TRACE("Drop pinned entry: " << entry.DebugString());
            byte* next_buffer = buffer_ + entry.current_offset() + entry.entry_data_size();
            byte* current_buffer = buffer_ + entry.current_offset();
            size_t s = buffer_size_ - (next_buffer - buffer_);

            DCHECK(next_buffer + s <= buffer_ + buffer_size_, "Illegal next buffer: " <<
                "buffer size " << buffer_size_ <<
                "copy size " << s <<
                "next buffer offset " << static_cast<int>(next_buffer - buffer_));

            // The next line will bring sometimes a valgind error:
            //     Source and destination overlap in memcpy
            // This is completely o.k. here.
            memcpy(current_buffer, next_buffer, s);
            memset(buffer_ + buffer_size_ - entry.entry_data_size(), 0, entry.entry_data_size());
            item_count_--;

            if (dropped_item_count) {
              (*dropped_item_count)++;
            }

            CHECK(entry.ParseFrom(entry.current_offset()), "Error parsing entry at offset " << entry.current_offset());
        } else {
            cache_lr = Iterate(&entry);
        }
    }
    return true;
}

delete_result DiskHashCachePage::Delete(const void* key, unsigned int key_size) {
    DCHECK_RETURN(key, DELETE_ERROR, "Key not set");

    TRACE("Delete from cache page: bucket " << bucket_id_ << ", key " << ToHexString(key, key_size));

    DiskHashCacheEntry entry(buffer_, buffer_size_, max_key_size_, max_value_size_);
    lookup_result cache_lr = IterateInit(&entry);
    while (cache_lr == LOOKUP_FOUND) {
        if (raw_compare(entry.key(), entry.key_size(), key, key_size) == 0) {
            /* Found it */

            byte* next_buffer = buffer_ + entry.current_offset() + entry.entry_data_size();
            byte* current_buffer = buffer_ + entry.current_offset();
            size_t s = buffer_size_ - (next_buffer - buffer_);

            TRACE("Found delete entry: key " << ToHexString(key, key_size) << ", offset " << entry.current_offset() <<
                ", next buffer offset " << static_cast<int>(next_buffer - buffer_) <<
                ", copy size " << s);

            DCHECK_RETURN(next_buffer + s <= buffer_ + buffer_size_, DELETE_ERROR,
                "Illegal next buffer: " <<
                "buffer size " << buffer_size_ <<
                "copy size " << s <<
                "next buffer offset " << static_cast<int>(next_buffer - buffer_));

            // The next line will bring sometimes a valgrind error:
            //     Source and destination overlap in memcpy
            // This is completely o.k. here.
            memcpy(current_buffer, next_buffer, s);
            memset(buffer_ + buffer_size_ - entry.entry_data_size(), 0, entry.entry_data_size());
            item_count_--;
            return DELETE_OK;
        }
        cache_lr = Iterate(&entry);
    }
    return DELETE_NOT_FOUND;
}

enum lookup_result DiskHashCachePage::IterateInit(DiskHashCacheEntry* cache_entry) const {
    DCHECK_RETURN(cache_entry, LOOKUP_ERROR, "Cache entry not set");

    if (cache_entry->ParseFrom(kHeaderOffset) && (cache_entry->key_size() != 0)) {
        return LOOKUP_FOUND;
    }
    return LOOKUP_NOT_FOUND;
}

enum lookup_result DiskHashCachePage::Iterate(DiskHashCacheEntry* cache_entry) const {
    DCHECK_RETURN(cache_entry, LOOKUP_ERROR, "Cache entry not set");

    if (cache_entry->ParseFrom(cache_entry->current_offset() + cache_entry->entry_data_size())
        && (cache_entry->key_size() != 0)) {
        return LOOKUP_FOUND;
    }
    return LOOKUP_NOT_FOUND;
}

enum lookup_result DiskHashCachePage::ChangePinningState(const void* key, size_t key_size, bool new_pinning_state) {
    DCHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    TRACE("Pin: bucket " << bucket_id_ <<
        ", key " << ToHexString(key, key_size) <<
        ", key size " << key_size <<
        ", page bin state " << pinned_ <<
        ", page dirty state " << dirty_ <<
        ", new key pin state " << ToString(new_pinning_state));

    uint32_t pinned_count = 0;
    bool found = false;
    DiskHashCacheEntry entry(buffer_, buffer_size_, max_key_size_, max_value_size_);
    lookup_result cache_lr = IterateInit(&entry);
    while (cache_lr == LOOKUP_FOUND) {
        if (raw_compare(entry.key(), entry.key_size(), key, key_size) == 0) {
            // Found it
            entry.set_pinned(new_pinning_state);
            entry.Store();
            found = true;
        }
        if (entry.is_pinned()) {
            pinned_count++;
        }
        cache_lr = Iterate(&entry);
    }
    this->pinned_ = (pinned_count > 0);
    if (found) {
        return LOOKUP_FOUND;
    }
    return LOOKUP_NOT_FOUND;
}

void DiskHashCachePage::RaiseBuffer(size_t minimal_new_buffer_size) {
    if (minimal_new_buffer_size < buffer_size_) {
        return;
    }
    size_t new_buffer_size = RoundUpFullBlocks(minimal_new_buffer_size, page_size_);
    byte* new_buffer = new byte[new_buffer_size];
    memcpy(new_buffer, buffer_, buffer_size_);
    memset(new_buffer + buffer_size_, 0, new_buffer_size - buffer_size_);

    TRACE("Raise buffer: old size " << buffer_size_ << ", new size " << new_buffer_size);

    buffer_size_ = new_buffer_size;
    delete[] buffer_;
    buffer_ = new_buffer;
}

put_result DiskHashCachePage::Update(const void* key, size_t key_size, const Message& message, bool keep,
                                     bool dirty_change, bool pin) {
    DCHECK_RETURN(key, PUT_ERROR, "Key not set");

    TRACE("Update cache page: bucket " << bucket_id_ <<
        ", key " << ToHexString(key, key_size) <<
        ", key size " << key_size);

    DiskHashCacheEntry entry(buffer_, buffer_size_, max_key_size_, max_value_size_);
    lookup_result cache_lr = IterateInit(&entry);
    while (cache_lr == LOOKUP_FOUND) {
        if (raw_compare(entry.key(), entry.key_size(), key, key_size) == 0) {
            if (keep) {
                return PUT_KEEP;
            }
            // Found it
            CHECK_RETURN(entry.AssignValue(message), PUT_ERROR, "Failed to assign value data");
            if (dirty_change) {
                this->dirty_ = true;
                entry.set_dirty(true);
            }
            if (pin) {
                this->pinned_ = true;
                entry.set_pinned(pin);
            }
            entry.Store();
            TRACE("Update cache entry: bucket " << bucket_id_ << ", key " << ToHexString(key, key_size) <<
                ", offset " << entry.current_offset() << ", entry " << entry.DebugString());
            return PUT_OK;
        }
        cache_lr = Iterate(&entry);
    }

    // Not found, entry is positioned correctly
    if (unlikely(!IsAcceptingNewEntries())) {
        // enlarge buffer if necessary
        RaiseBuffer(buffer_size_ * 2);
        entry.SetBuffer(buffer_, buffer_size_);
    }
    TRACE("New cache entry: bucket " << bucket_id_ <<
        ", offset " << entry.current_offset() <<
        ", key " << ToHexString(key, key_size) <<
        ", key size " << key_size);
    CHECK_RETURN(entry.AssignKey(key, key_size), PUT_ERROR,
        "Failed to assign value data: key size " << key_size << ", max key size " << entry.max_key_size());
    CHECK_RETURN(entry.AssignValue(message), PUT_ERROR,
        "Failed to assign value data: " << message.ShortDebugString());
    if (dirty_change) {
        this->dirty_ = true;
        entry.set_dirty(true);
    }
    if (pin) {
        this->pinned_ = true;
        entry.set_pinned(true);
    }
    entry.Store();
    item_count_++;
    return PUT_OK;
}

bool DiskHashCachePage::IsAcceptingNewEntries() {
    uint32_t space_needed = kHeaderOffset + ((item_count_ + 1) * (4 + this->max_key_size_ + this->max_value_size_));
    return space_needed < this->buffer_size_;
}

DiskHashCachePage::DiskHashCachePage(uint64_t bucket_id, size_t page_size, uint32_t max_key_size,
                                     uint32_t max_value_size) {
    this->bucket_id_ = bucket_id;
    this->page_size_ = page_size;
    dirty_ = false;
    pinned_ = false;
    max_key_size_ = max_key_size;
    max_value_size_ = max_value_size;
    item_count_ = 0;
    buffer_ = new byte[page_size_];
    memset(buffer_, 0, this->page_size_);
    buffer_size_ = page_size_;
}

bool DiskHashCachePage::ParseData() {
    bucket_id_ = GetFromBuffer<uint64_t>(buffer_, 0);
    item_count_ = GetFromBuffer<uint16_t>(buffer_, 8);

    DCHECK(item_count_ >= 0 && item_count_ <= 1024,
        "Illegal item count");

    dirty_ = false;
    pinned_ = false;
    DiskHashCacheEntry entry(buffer_, buffer_size_, max_key_size_, max_value_size_);
    lookup_result cache_lr = IterateInit(&entry);
    while (cache_lr == LOOKUP_FOUND) {
        if (entry.is_dirty()) {
            dirty_ = true;
        }
        if (entry.is_pinned()) {
            pinned_ = true;
        }
        cache_lr = Iterate(&entry);
    }
    return true;
}

bool DiskHashCachePage::Store() {
    DCHECK(item_count_ >= 0 && item_count_ <= 1024,
        "Illegal item count");

    SetToBuffer<uint64_t>(buffer_, 0, bucket_id_);
    SetToBuffer<uint16_t>(buffer_, 8, (uint16_t) item_count_);
    return true;
}

string DiskHashCachePage::DebugString() const {
    stringstream sstr;
    sstr << "[cache page " << this->bucket_id_ << ", cache buffer size " << buffer_size_ << ", item count "
         << item_count_ << ", dirty " << ToString(dirty_) << ", pinned " << ToString(pinned_) << "]";
    return sstr.str();
}

DiskHashCachePage::~DiskHashCachePage() {
    if (buffer_) {
        delete[] buffer_;
        buffer_ = NULL;
    }
}

void DiskHashCacheEntry::SetBuffer(byte* buf, size_t buf_size) {
    this->buffer_ = buf;
    this->buffer_size_ = buf_size;
}

bool DiskHashCacheEntry::ParseFrom(uint32_t offset) {
    offset_ = offset;

    const byte* buf = buffer_ + offset;

    if (offset + entry_data_size() > buffer_size_) {
        key_size_ = 0;
        value_size_ = 0;
        dirty_ = false;
        pinned_ = false;
        return false;
    }

    key_size_ = GetFromBuffer<uint8_t>(buf, 0);
    value_size_ = GetFromBuffer<uint16_t>(buf, 1);
    uint8_t state = GetFromBuffer<uint8_t>(buf, 3);
    dirty_ = state & 1;
    pinned_ = state & 2;

    DCHECK(key_size_ <= max_key_size_, "Illegal key size: " << key_size_);
    DCHECK(value_size_ <= max_value_size_, "Illegal value size: " << value_size_);

    TRACE("Parse entry: "
        "key " << ToHexString(key(), key_size()) <<
        ", key size " << key_size_ <<
        ", value size " << value_size_ <<
        ", state " << static_cast<uint32_t>(state) <<
        ", offset " << offset_);
    return true;
}

void DiskHashCacheEntry::Store() {
    byte* buf = buffer_ + offset_;

    SetToBuffer<uint8_t>(buf, 0, (uint8_t) key_size_);
    SetToBuffer<uint16_t>(buf, 1, (uint16_t) value_size_);

    uint8_t state = (dirty_) | (pinned_ << 1);
    SetToBuffer<uint8_t>(buf, 3, (uint8_t) state);
}

bool DiskHashCacheEntry::AssignKey(const void* key, size_t key_size) {
    key_size_ = key_size;
    void* key_buf = const_cast<void*>(this->key());
    memcpy(key_buf, key, key_size);
    this->key_size_ = key_size;
    return true;
}

bool DiskHashCacheEntry::AssignValue(const Message& message) {
    DCHECK(this->buffer_ != NULL, "Buffer not set");
    CHECK(this->max_value_size_ >= message.ByteSize(),
        "Illegal size: " << message.DebugString());

    void* value_buf = const_cast<void*>(this->value());
    memset(value_buf, 0, this->max_value_size_);
    uint32_t value_size = message.ByteSize();
    CHECK(message.SerializeWithCachedSizesToArray(reinterpret_cast<byte*>(value_buf)),
        "Failed to serialize array: message " << message.DebugString());
    this->value_size_ = value_size;
    return true;
}

DiskHashCacheEntry::DiskHashCacheEntry(byte* buffer, size_t buffer_size, uint32_t max_key_size, uint32_t max_value_size) {
    buffer_ = buffer;
    buffer_size_ = buffer_size;
    this->key_size_ = 0;
    this->value_size_ = 0;
    dirty_ = false;
    pinned_ = false;
    offset_ = 0;
    max_key_size_ = max_key_size;
    max_value_size_ = max_value_size;
}

string DiskHashCacheEntry::DebugString() {
    return "cache entry: key " + ToHexString(this->key(), this->key_size()) + ", key size "
           + ToString(this->key_size()) + ", value " + ToHexString(this->value(), this->value_size())
           + ", value size " + ToString(this->value_size()) + ", dirty " + ToString(dirty_) + ", pinned " + ToString(
        pinned_);
}

}
}
}
