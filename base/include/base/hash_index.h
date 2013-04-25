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

#ifndef HASH_INDEX_H__
#define HASH_INDEX_H__

#include <tbb/atomic.h>

#include <base/base.h>
#include <base/index.h>
#include <base/locks.h>

namespace dedupv1 {
namespace base {

/**
* Data structure for hash entry
* The hash entries form a linked list
*/
class HashEntry {
    private:
        void* key;
        size_t key_size;

        void* data;
        size_t data_size;

        HashEntry* next;

        HashEntry(HashEntry* next);
        ~HashEntry();

        bool AssignValue(const google::protobuf::Message& message);
        bool Assign(const void* key, size_t key_size, const google::protobuf::Message& message);

        enum put_result CompareAndSwap(const google::protobuf::Message& message,
                const google::protobuf::Message& compare_message,
                google::protobuf::Message* result_message);

        friend class HashIndex;
};


/**
* In-Memory chained hashtable.
*
* While the index is in-test, it has not been used for a long time.
*
* TODO (dmeister): Update to the current style.
*/
class HashIndex : public MemoryIndex {
    private:

        /**
        * Type for statistics about the hash index
        */
        struct hash_index_statistics {
            tbb::atomic<uint32_t> rdlock_free;
            tbb::atomic<uint32_t> rdlock_busy;
            tbb::atomic<uint32_t> wrlock_free;
            tbb::atomic<uint32_t> wrlock_busy;

            tbb::atomic<uint64_t> read_operations;
            tbb::atomic<uint64_t> linked_list_length;
        };

        /**
        *  Number of buckets
        */
        uint32_t bucket_count;

        uint32_t sub_bucket_count;

        /**
        * Array of buckets of size bucket_count
        */
        HashEntry*** buckets;
        dedupv1::base::ReadWriteLockVector lock;
        uint16_t lock_count;
        struct hash_index_statistics statistics;

        tbb::atomic<uint64_t> item_count;
    public:
        /**
        * Inits the hash index
        */
        HashIndex();
        virtual ~HashIndex();

        static Index* CreateIndex();
        static void RegisterIndex();

        /**
        * Sets option of the hash index
        * buckets: Number of hash buckets
        * lock-count: Number of locks to protect the buckets. In any real system it is impossible for
        * every bucket to have an own lock. However, a single lock reduces the concurrency of the dedup system.
        * The lock count parameter is a way to tradeoff between concurrency level and space consumption. Values
        * between 2 * (number of concurrent threads) should be good values
        *
        * Available options:
        * - buckets: StorageUnit
        * - sub-buckets: StorageUnit
        * - lock-count: StorageUnit
        */
        bool SetOption(const std::string& option_name, const std::string& option);


        /**
        * Starts the hash index
         * @return true iff ok, otherwise an error has occurred
        */
        virtual bool Start(const dedupv1::StartContext& start_context);

        /**
        * Looks up the key in the hash index by hashing the key into a bucket and then searching
        * for the key in the bucket linked list.
        */
        enum lookup_result Lookup(const void* key, size_t key_size,
                google::protobuf::Message* message);

        virtual enum put_result Put(const void* key, size_t key_size,
                const google::protobuf::Message& message);

        virtual enum put_result PutIfAbsent(
                const void* key, size_t key_size,
                const google::protobuf::Message& message);

        virtual enum put_result CompareAndSwap(const void* key, size_t key_size,
                const google::protobuf::Message& message,
                const google::protobuf::Message& compare_message,
                google::protobuf::Message* result_message);

        virtual enum delete_result Delete(const void* key, size_t key_size);

        virtual std::string PrintLockStatistics();

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool Clear();

        virtual uint64_t GetItemCount();

        virtual uint64_t GetMemorySize();

        friend class DiskHashEntryPage;
};

}
}

#endif  // HASH_INDEX_H__
