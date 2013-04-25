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

#ifndef TC_HASH_MEM_INDEX_H__
#define TC_HASH_MEM_INDEX_H__

#include <string>

#include <tbb/atomic.h>

#include <tcutil.h>
#include <tchdb.h>

#include <base/base.h>
#include <base/index.h>
#include <base/locks.h>
#include <base/profile.h>

#include <tbb/atomic.h>

namespace dedupv1 {
namespace base {

class TCMemHashIndex;

/**
 * Iterator implementation class to iterate over a hash index iterator.
 *
 */
class TCMemHashIndexIterator : public IndexIterator {
        DISALLOW_COPY_AND_ASSIGN(TCMemHashIndexIterator);
        /**
         * Hash index
         */
        TCMemHashIndex* index_;

        /**
         * Index which hash database to use
         */
        int hash_index_;

        /**
         * version counter at the time of creation.
         * Used to detect concurrent changed.
         */
        uint64_t version_counter_;

    public:

        explicit TCMemHashIndexIterator(TCMemHashIndex* index_);
        virtual ~TCMemHashIndexIterator();

        /**
         * @param key Pointer to a memory region that will be filled with the key of the next entry
         * @param key_size Size of the returned key
         * @param message Protobufmessage that will have parsed the key (???)
         */
        virtual enum lookup_result Next(void* key, size_t* key_size, google::protobuf::Message* message);
};

/**
 * Tokyo cabinet based in-memory hash index.
 */
class TCMemHashIndex : public MemoryIndex {
        DISALLOW_COPY_AND_ASSIGN(TCMemHashIndex);
        friend class TCMemHashIndexIterator;

        enum tc_hash_mem_index_state {
            TC_HASH_MEM_INDEX_STATE_CREATED,
            TC_HASH_MEM_INDEX_STATE_STARTED
        };
        /**
         * Pointer to the in-memory db of tc
         */
        TCMDB *mdb_;

        /**
         * Configured number of buckets to use
         */
        int64_t buckets_;

        /**
         * State of the index
         */
        enum tc_hash_mem_index_state state_;

        /**
         * Profiling information
         */
        dedupv1::base::Profile update_time_;

        dedupv1::base::Profile lookup_time_;

        tbb::atomic<uint64_t> version_counter_;

        tbb::atomic<int> iterator_counter_;

        /**
         * iff true, a checksum is stored for all messages in this index.
         * No checksum is stored for raw access
         */
        bool checksum_;

        static void* TCDuplicationHandler(const void *vbuf, int vsiz, int *sp, void *op);
    public:
        TCMemHashIndex();

        virtual ~TCMemHashIndex();

        static Index* CreateIndex();

        static void RegisterIndex();


        /**
         *
         * Available options:
         * - bucket-count: StorageUnit
         * - checksum: Boolean
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context);

        IndexIterator* CreateIterator();

        /**
         *
         * @param key
         * @param key_size
         * @param message
         * @return
         */
        enum lookup_result Lookup(const void* key, size_t key_size,
                google::protobuf::Message* message);

        /**
         *
         * @param key
         * @param key_size
         * @param message
         * @return
         */
        virtual enum put_result Put(const void* key, size_t key_size,
                const google::protobuf::Message& message);

        /**
         *
         * @param key
         * @param key_size
         * @param message
         * @return
         */
        virtual enum put_result PutIfAbsent(
                const void* key, size_t key_size,
                const google::protobuf::Message& message);

        /**
         *
         * @param key
         * @param key_size
         * @return
         */
        virtual enum delete_result Delete(const void* key, size_t key_size);

        virtual uint64_t GetItemCount();

        virtual uint64_t GetMemorySize();

        virtual bool Clear();

        virtual std::string PrintProfile();

        virtual enum put_result RawPutIfAbsent(
                const void* key, size_t key_size,
                const void* value, size_t value_size);

        virtual enum put_result RawPut(
                const void* key, size_t key_size,
                const void* value, size_t value_size);

        virtual enum lookup_result RawLookup(const void* key, size_t key_size,
                void* value, size_t* value_size);

        virtual enum put_result CompareAndSwap(const void* key, size_t key_size,
                const google::protobuf::Message& message,
                const google::protobuf::Message& compare_message,
                google::protobuf::Message* result_message);
};

}
}

#endif  // TC_HASH_MEM_INDEX_H__
