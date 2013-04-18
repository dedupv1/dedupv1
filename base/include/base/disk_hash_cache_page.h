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

#ifndef DISK_HASH_INDEX_CACHE_H__
#define DISK_HASH_INDEX_CACHE_H__

#include <vector>
#include <list>
#include <string>

#include <tbb/atomic.h>
#include <tbb/concurrent_vector.h>
#include <tbb/spin_mutex.h>
#include <tbb/concurrent_hash_map.h>

#include <base/base.h>
#include <base/index.h>
#include <base/profile.h>
#include <base/resource_management.h>
#include <base/fileutil.h>

#include <gtest/gtest_prod.h>

#include "dedupv1_base.pb.h"

namespace dedupv1 {
namespace base {

namespace internal {

/**
 * Class storing a cached entry
 */
class DiskHashCacheEntry {
    private:
        DISALLOW_COPY_AND_ASSIGN(DiskHashCacheEntry);
        /**
         * Buffer for the hash entry
         */
        byte* buffer_;

        /**
         * size of the buffer
         */
        size_t buffer_size_;

        /**
         * size of the key
         */
        uint32_t key_size_;

        /**
         * maximal allowed size for the key
         */
        uint32_t max_key_size_;

        /**
         * size of the value
         */
        uint32_t value_size_;

        /**
         * maximal allowed size for the value
         */
        uint32_t max_value_size_;

        /**
         * true if the page is dirty and should be written back to disk eventually
         */
        bool dirty_;

        /**
         * true if the page is pinned and therefore should not be written back to disk right now.
         */
        bool pinned_;

        /**
         * offset of the entry within the page
         */
        uint32_t offset_;
    public:
        /**
         * Constructor.
         * Buffer is not assigned. key and value are set to zero length.
         *
         * @param max_key_size maximal size allowed for a key.
         * @param max_value_size maximal size allowed for a entry value.
         */
        DiskHashCacheEntry(byte* buffer, size_t buffer_size, uint32_t max_key_size, uint32_t max_value_size);

        /**
         * Parses a entry from the given buffer.
         *
         * @param buffer
         * @param buffer_size
         * @return true iff ok, otherwise an error has occurred
         */
        bool ParseFrom(uint32_t offset);

        /**
         * Assigns a new key.
         * The key data is placed in the correct positions in the buffer.
         * A buffer must be assigned before this method is called.
         *
         * @param key
         * @param key_size
         * @return true iff ok, otherwise an error has occurred
         */
        bool AssignKey(const void* key, size_t key_size);

        /**
         * Assigns a new value.
         *
         * The key data is placed in the correct positions in the buffer.
         * A buffer must be assigned before this method is called.
         *
         * @param message
         * @return true iff ok, otherwise an error has occurred
         */
        bool AssignValue(const google::protobuf::Message& message);

        /**
         * Stores the data back to the buffer.
         * Especially the meta data like key size, value size, state is not stored there
         * before
         */
        void Store();

        /**
         * returns the current key.
         */
        inline const void* key() const;

        /**
         * returns the current key size
         * @return
         */
        inline uint32_t key_size() const;

        /**
         * returns the current value
         */
        inline const void* value() const;

        /**
         * returns the current value size
         * @return
         */
        inline uint32_t value_size() const;

        /**
         * returns the maximal allowed key size
         * @return
         */
        inline uint32_t max_key_size() const;

        /**
         * returns the maximal allowed value size
         * @return
         */
        inline uint32_t max_value_size() const;

        /**
         * Returns a developer-readable representation of the string
         * @return
         */
        std::string DebugString();

        /**
         * returns the total data size of a entries
         * @return
         */
        inline size_t entry_data_size() const;

        /**
         * returns true if the entry is dirty
         */
        inline bool is_dirty() const;

        /**
         * sets the dirty state
         */
        inline void set_dirty(bool d);

        /**
         * returns true if the entry is pinned
         */
        inline bool is_pinned() const;

        /**
         * sets the pinned state
         */
        inline void set_pinned(bool p);

        /**
         * Current offset in bytes from the beginning of the cache page where
         * this entry is stored
         */
        inline uint32_t current_offset() const;

        /**
         * Resets the buffer to a different offset
         */
        void SetBuffer(byte* buf, size_t buf_size);
};

/**
 * Class representing a cache page
 */
class DiskHashCachePage {
        DISALLOW_COPY_AND_ASSIGN(DiskHashCachePage);
    private:
        /**
         * id of the bucket
         */
        uint64_t bucket_id_;

        /**
         * The size of the buffer is the page_size
         */
        byte* buffer_;

        /**
         * size of the buffer.
         */
        size_t buffer_size_;

        /**
         * original page size.
         * Normal page_size_ == buffer_size_, but when the cache page overflows the
         * buffer size can be a multiple of the page size
         */
        size_t page_size_;

        /**
         * true iff any entry on the page is dirty
         */
        bool dirty_;

        /**
         * true iff any entry on the page is pinned
         */
        bool pinned_;

        /**
         * maximal allowed size for the key
         */
        uint32_t max_key_size_;

        /**
         * maximal allowed size for the value
         */
        uint32_t max_value_size_;

        /**
         * current number of entries in the page
         */
        uint32_t item_count_;
    public:

        byte* ReplaceBufferPointer(byte* replacement_buffer) {
            byte* b = buffer_;
            buffer_ = replacement_buffer;
            return b;
        }

        /**
         * true if new entries have enough space with the current size of the buffer.
         */
        bool IsAcceptingNewEntries();

        /**
         * 8 byte bucket id
         * 2 byte item count
         */
        static const size_t kHeaderOffset = 10;

        /**
         * Constructor
         * @param bucket_id id of the bucket
         * @param buffer buffer to use. All entry data is encoded and should be encoded in memory provided
         * by the buffer.
         * @param buffer_size size of a the buffer.
         * @return
         */
        DiskHashCachePage(uint64_t bucket_id,
                size_t page_size,
                uint32_t max_key_size, uint32_t max_value_size);

        /**
         * Destructor
         * @return
         */
        ~DiskHashCachePage();

        /**
         * Increases the buffer to at least the given size, but still a multiple of the
         * page size
         */
        void RaiseBuffer(size_t minimal_new_buffer_size);

        /**
         * searches for a given key in the page and fills the message if the key is found.
         *
         * @param key key to search for
         * @param key_size size of the key
         * @param message message to fill when a entry with this key is found.
         * @return
         */
        enum lookup_result Search(const void* key, size_t key_size,
                google::protobuf::Message* message,
                bool* is_dirty,
                bool* is_pinned);

        /**
         * Deletes the given key from the page
         *
         * @param key
         * @param key_size
         * @return
         */
        enum delete_result Delete(const void* key,
                unsigned int key_size);

        /**
         * Updates the given key in the page.

         * @param key
         * @param key_size
         * @param message
         * @return
         */
        enum put_result Update(const void* key, size_t key_size,
                const google::protobuf::Message& message,
                bool keep,
                bool dirty_change,
                bool pin);

        bool ParseData();

        bool Store();

        /**
         * Starts an iterator on the page.
         */
        enum lookup_result IterateInit(DiskHashCacheEntry* cache_entry) const;

        /**
         * Forwards the entry to the next page.
         */
        enum lookup_result Iterate(DiskHashCacheEntry* cache_entry) const;

        /**
         * returns the raw buffer.
         *
         * The data buffer differs from the raw buffer in that
         * the data buffer is the place for the page payload (the entries).
         * The raw buffer consists of a page header of kPageDataSize byte
         * followed by the data buffer.
         */
        inline const byte* raw_buffer() const;

        inline byte* mutable_raw_buffer();

        /**
         * returns the size of the raw buffer, which is the size of the
         * page
         */
        inline size_t raw_buffer_size() const;

        /**
         * returns the bucket id of the page
         */
        inline uint64_t bucket_id() const;

        /**
         * returns a developer-readable representation of the page
         */
        std::string DebugString() const;

        /**
         * true if any entry of the page is dirty
         */
        inline bool is_dirty() const;

        /**
         * sets the dirty state of the page
         */
        inline void set_dirty(bool d);

        /**
         * true if any entry of the page is dirty
         */
        inline bool is_pinned() const;

        /**
         * sets the pinning state of the page
         */
        inline void set_pinned(bool b);

        /**
         * Changes the pinned state of an entry with the given key
         */
        enum lookup_result ChangePinningState(const void* key, size_t key_size, bool new_pinning_state);

        /**
         * Drop all pinned entries on the page
         */
        bool DropAllPinned(uint64_t* dropped_item_count);

        /**
         * size used by the page in RAM
         */
        inline size_t used_size() const;

        /**
         * number of entries in the cache page
         */
        inline uint32_t item_count() const;
};

const void* DiskHashCacheEntry::key() const {
    if (buffer_ == NULL) {
        return NULL;
    }
    return buffer_ + offset_ + 4;
}

uint32_t DiskHashCacheEntry::key_size() const {
    return this->key_size_;
}

const void* DiskHashCacheEntry::value() const {
    if (buffer_ == NULL) {
        return NULL;
    }
    return buffer_ + offset_ + 4 + max_key_size_;
}

uint32_t DiskHashCacheEntry::value_size() const {
    return this->value_size_;
}

const byte* DiskHashCachePage::raw_buffer() const {
    return this->buffer_;
}

byte* DiskHashCachePage::mutable_raw_buffer() {
    return this->buffer_;
}

size_t DiskHashCachePage::raw_buffer_size() const {
    return this->buffer_size_;
}

size_t DiskHashCacheEntry::entry_data_size() const {
    return 4 + this->max_key_size_ + this->max_value_size_;
}

size_t DiskHashCachePage::used_size() const {
    return kHeaderOffset + (item_count_ * (4 + this->max_key_size_ + this->max_value_size_));
}

uint32_t DiskHashCachePage::item_count() const {
    return item_count_;
}

uint32_t DiskHashCacheEntry::current_offset() const {
    return offset_;
}

uint64_t DiskHashCachePage::bucket_id() const {
    return this->bucket_id_;
}
uint32_t DiskHashCacheEntry::max_key_size() const {
    return this->max_key_size_;
}

uint32_t DiskHashCacheEntry::max_value_size() const {
    return this->max_value_size_;
}

bool DiskHashCacheEntry::is_dirty() const {
    return this->dirty_;
}

bool DiskHashCachePage::is_pinned() const {
    return this->pinned_;
}

void DiskHashCachePage::set_pinned(bool p) {
    this->pinned_ = p;
}

bool DiskHashCachePage::is_dirty() const {
    return this->dirty_;
}

void DiskHashCachePage::set_dirty(bool d) {
    dirty_ = d;
}

void DiskHashCacheEntry::set_dirty(bool d) {
    dirty_ = d;
}

bool DiskHashCacheEntry::is_pinned() const {
    return this->pinned_;
}

void DiskHashCacheEntry::set_pinned(bool p) {
    pinned_ = p;
}

}
}
}

#endif  // DISK_HASH_INDEX_H__
