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
#ifndef TC_HASH_INDEX_H__
#define TC_HASH_INDEX_H__

#include <string>
#include <vector>

#include <tbb/atomic.h>

#include <tcutil.h>
#include <tchdb.h>

#include <base/base.h>
#include <base/index.h>
#include <base/profile.h>
#include <base/locks.h>

namespace dedupv1 {
namespace base {

class TCHashIndex;

/**
 * Iterator implementation class to iterate over a hash index iterator.
 *
 */
class TCHashIndexIterator : public IndexIterator {
    DISALLOW_COPY_AND_ASSIGN(TCHashIndexIterator);
    /**
        * Hash index
        */
    TCHashIndex* index_;

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

    explicit TCHashIndexIterator(TCHashIndex* index);
    virtual ~TCHashIndexIterator();

    /**
     *
     * @param key
     * @param key_size
     * @param message
     * @return
     */
    virtual enum lookup_result Next(void* key, size_t* key_size, google::protobuf::Message* message);
};

/**
 * Disk-based Hash Index in the Tokyo Cabinet implementation.
 * The type name is "tc-disk-hash".
 */
class TCHashIndex : public PersistentIndex {
        DISALLOW_COPY_AND_ASSIGN(TCHashIndex);
        friend class TCHashIndexIterator;

        /**
         * Enumeration of possible compression parameters
         */
        enum tc_hash_index_compression {
                TC_HASH_INDEX_COMPRESSION_NONE,   //!< TC_HASH_INDEX_COMPRESSION_NONE
                TC_HASH_INDEX_COMPRESSION_DEFLATE,//!< TC_HASH_INDEX_COMPRESSION_DEFLATE
                TC_HASH_INDEX_COMPRESSION_BZIP2,  //!< TC_HASH_INDEX_COMPRESSION_BZIP2
                TC_HASH_INDEX_COMPRESSION_TCBS    //!< TC_HASH_INDEX_COMPRESSION_TCBS
        };

        /**
         * Enumeration of index states
         */
        enum tc_hash_index_state {
                TC_HASH_INDEX_STATE_CREATED,//!< TC_HASH_INDEX_STATE_CREATED
                TC_HASH_INDEX_STATE_STARTED //!< TC_HASH_INDEX_STATE_STARTED
        };

        static const double kDefaultEstimatedMaxItemsPerBucket = 16;

        std::vector<TCHDB*> hdb_;
        std::vector<std::string> filename_;

        /**
         * R/W per db. It is a good question why we need an external R/W lock when the tc
         * implementation used one R/W lock per db internally. The problem is that the transaction mechanism
         * is externally and not very good. We have seen problems with it and we can prevent it with an extra R/W lock
         */
        dedupv1::base::ReadWriteLockVector locks_;

        // Tune Options

        /**
         * Number of elements of the bucket array.
         * The default value is 131071.
         * Suggested size of the bucket array is about from 0.5 to 4 times of the number of all records to be stored.
         */
        int64_t buckets_;

        /**
         * Size of record alignment by power of 2.
         * If it is negative, the default value is specified.
         * The default value is 4 standing for 2^4=16.
         */
        int8_t record_alignment_;

        /**
         * Maximum number of elements of the free block pool by power of 2.
         * If it is negative, the default value is specified.
         *  The default value is 10 standing for 2^10=1024.
         */
        int8_t free_pool_size_;

        /**
         * Compression
         */
        enum tc_hash_index_compression compression_;

        // Cache Options

        /**
         * Maximum number of records to be cached.
         * If it is not more than 0, the record cache is disabled.
         * It is disabled by default.
         */
        int32_t cache_size_;

        /**
         * Size of the extra mapped memory of a B+ tree database object.
         * If it is not more than 0, the extra mapped memory is disabled. It is disabled by default.
         */
        int64_t mem_mapped_size_;

        /**
         * Unit step number of auto defragmentation of a hash database object.
         * If it is not more than 0, the auto defragmentation is disabled. It is enabled by default.
         */
        int32_t defrag_unit_;

        double estimated_max_items_per_bucket_;

        enum tc_hash_index_state state_;

        dedupv1::base::Profile profiling_;

        tbb::atomic<uint64_t> version_counter_;

        bool checksum_;

        std::pair<TCHDB*, dedupv1::base::ReadWriteLock*> GetHashDatabase(const void* key, size_t key_size);
    public:
        TCHashIndex();

        virtual ~TCHashIndex();

        static Index* CreateIndex();

        static void RegisterIndex();

        /**
         *
         * Available options:
         * - filename: String with file where the transaction data is stored (multi)
         * - sync (is depreciated)
         * - lazy-sync (is depreciated)
         * - buckets: StorageUnit
         * - max-items-per-bucket: int32_t
         * - record-alignment: int8_t
         * - free-pool-size: int8_t
         * - compression: String (none, deflate, bzip2, tcbs)
         * - cache-size: StorageUnit
         * - mem-mapped-size: StorageUnit
         * - defrag: int32_t
         * - transactions (is depreciated)
         * - checksum: Boolean
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context);

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
        virtual enum put_result PutIfAbsent(
                const void* key, size_t key_size,
                const google::protobuf::Message& message);

        /**
         *
         * @param key
         * @param key_size
         * @param message
         * @return
         */
        virtual enum put_result Put(const void* key, size_t key_size,
                const google::protobuf::Message& message);

        virtual enum delete_result Delete(const void* key, size_t key_size);

        virtual uint64_t GetEstimatedMaxItemCount();

        virtual uint64_t GetItemCount();

        virtual std::string PrintProfile();

        virtual uint64_t GetPersistentSize();

        virtual IndexIterator* CreateIterator();
};

}
}

#endif  // TC_HASH_INDEX_H__
