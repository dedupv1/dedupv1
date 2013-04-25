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
#ifndef TC_BTREE_INDEX_H__
#define TC_BTREE_INDEX_H__

#include <stdbool.h>
#include <stdint.h>

#include <tbb/atomic.h>

#include <tcutil.h>
#include <tcbdb.h>

#include <gtest/gtest_prod.h>

#include <string>
#include <vector>

#include <base/base.h>
#include <base/index.h>
#include <base/profile.h>
#include <base/locks.h>

namespace dedupv1 {
namespace base {

class TCBTreeIndex;

/**
 * Iterator for the b-tree implementation
 */
class TCBTreeIndexIterator : public IndexIterator {
        DISALLOW_COPY_AND_ASSIGN(TCBTreeIndexIterator);

        /**
         * Reference to the parent index
         */
        TCBTreeIndex* index_;

        /**
         * index of the tree to use
         */
        int tree_index;

        /**
         * Current b-tree cursor
         */
        BDBCUR* cur;

        /**
         * flag if the cursor is valid
         */
        bool cur_valid;

        /**
         * version counter at the time the iterator is created.
         * Used to check if the index has not been modified in between.
         */
        uint64_t version_counter_;
    public:

        /**
         * Constructor
         */
        explicit TCBTreeIndexIterator(TCBTreeIndex* index_);

        /**
         * Destructor
         */
        ~TCBTreeIndexIterator();

        /**
         * Get the next item from the b-tree.
         */
        virtual enum lookup_result Next(void* key, size_t* key_size,
                google::protobuf::Message* message);
};

/**
 * Cursor class for a tc-disk-btree.
 * This variant is used if the btree has only a single file
 *
 * Instances are created via the CreateCursor method.
 */
class SingleFileTCBTreeCursor : public IndexCursor {
        DISALLOW_COPY_AND_ASSIGN(SingleFileTCBTreeCursor);
        BDBCUR* cur;
        TCBTreeIndex* index_;

        SingleFileTCBTreeCursor(TCBTreeIndex* index_, BDBCUR* cur);
    public:
        virtual enum lookup_result First();
        virtual enum lookup_result Next();
        virtual enum lookup_result Last();
        virtual enum lookup_result Jump(const void* key, size_t key_size);
        virtual bool Remove();
        virtual bool Get(void* key, size_t* key_size,
                google::protobuf::Message* message);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Put(const google::protobuf::Message& message);

        virtual bool IsValidPosition();

        ~SingleFileTCBTreeCursor();

        friend class TCBTreeIndex;
};

/**
 * Disk-based B+-Tree in the Tokyo Cabinet implementation.
 * You can get further information on it at following page:  http://fallabs.com/tokyocabinet/spex-en.html#tcbdbapi
 *
 * The type name is "tc-disk-btree".
 */
class TCBTreeIndex : public PersistentIndex {
        DISALLOW_COPY_AND_ASSIGN(TCBTreeIndex);
        friend class SingleFileTCBTreeCursor;
        friend class TCBTreeIndexIterator;
        friend class TCBTreeIndexTest;
        FRIEND_TEST(TCBTreeIndexTest, GetBTree);

        /**
         * Flags to specify the compression mode of the btree.
         */
        enum tc_btree_index_compression {
                TC_BTREE_INDEX_COMPRESSION_NONE,
                TC_BTREE_INDEX_COMPRESSION_DEFLATE,
                TC_BTREE_INDEX_COMPRESSION_BZIP2,
                TC_BTREE_INDEX_COMPRESSION_TCBS
        };

        /**
         * States of the index
         */
        enum tc_btree_index_state {
                TC_BTREE_INDEX_STATE_CREATED,
                TC_BTREE_INDEX_STATE_STARTED
        };

        static const double kDefaultEstimatedMaxItemsPerBucket = 16;

        /**
         * Vector of Tokyo Cabinet b-tree data bases
         */
        std::vector<TCBDB*> bdb_;

        /**
         * Vector of the filenames
         */
        std::vector<std::string> filename_;

        /**
         * R/W per db. It is a good question why we need an external R/W lock when the tc
         * implementation used one R/W lock per db internally. The problem is that the transaction mechanism
         * is externally and not very good. We have seen problems with it and we can prevent it with an extra R/W lock
         */
        dedupv1::base::ReadWriteLockVector bdb_locks_;

        // Tune Options
        /**
         * Number of members in each leaf page.
         * Default: 128
         */
        int32_t leaf_members_;

        /**
         * Number of members in each non-leaf page.
         * Default: 256
         */
        int32_t non_leaf_members_;

        /**
         * Number of elements of the bucket array.
         * Default: 32749
         *
         * Suggested size of the bucket array is about from 1 to 4 times of the number of all
         * pages to be stored.
         */
        int64_t buckets_;

        double estimated_max_items_per_bucket_;

        /**
         * Record alignment by power of 2.
         * The default value is 8 standing for 2^8=256
         */
        int8_t record_alignment_;

        /**
         * Maximum number of elements of the free block pool by power of 2.
         * The default value is 10 standing for 2^10=1024.
         */
        int8_t free_pool_size_;

        /**
         * Compression.
         * Default: TC_BTREE_INDEX_COMPRESSION_NONE
         */
        enum tc_btree_index_compression compression_;

        // Cache Options
        /**
         * Maximum number of leaf nodes to be cached.
         * The default value is 1024
         */
        int32_t leaf_cache_size_;

        /**
         * Maximum number of non-leaf nodes to be cached.
         * The default value is 512.
         */
        int32_t non_leaf_cache_size_;


        /**
         * Size of the extra mapped memory of a B+ tree database object.
         * If it is not more than 0, the extra mapped memory is disabled. It is disabled by default.
         */
        int64_t mem_mapped_size_;

        /**
         * Unit step number of auto defragmentation of a B+ tree database object.
         * If it is not more than 0, the auto defragmentation is disabled. It is enabled by default.
         */
        int32_t defrag_unit_;

        enum tc_btree_index_state state_;

        /**
         * Statistics about the tc-btree
         */
        class Statistics {
            public:
                Statistics();

                /**
                 * Number of times the btree lock was found free
                 */
                tbb::atomic<uint32_t> lock_free_;

                /**
                 * Number of times the btree lock was found busy. A high ratio between free and busy indicates
                 * a lock contention problem
                 */
                tbb::atomic<uint32_t> lock_busy_;

                /**
                 * ms spend in lookup calls since the last start
                 */
                dedupv1::base::Profile lookup_time_;

                /**
                 * ms spend in update calls since the last start
                 */
                dedupv1::base::Profile update_time_;

                /**
                 * ms spend in delete calls since the last start
                 */
                dedupv1::base::Profile delete_time_;

                /**
                 * ms spend waiting for the btree lock since the last start
                 */
                dedupv1::base::Profile lock_time_;

                /**
                 * ms spend in calls of the tc library functions
                 */
                dedupv1::base::Profile tc_time_;

                /**
                 * ms spend in the main functions (lookup/update/delete)
                 */
                dedupv1::base::Profile total_time_;

                tbb::atomic<uint64_t> lookup_count_;

                tbb::atomic<uint64_t> update_count_;

                tbb::atomic<uint64_t> delete_count_;
        };

        /**
         * Statistics about the btree
         */
        Statistics stats_;

        /**
         * Current version counter
         */
        tbb::atomic<uint64_t> version_counter_;

        bool checksum_;

        std::pair<TCBDB*, dedupv1::base::ReadWriteLock*> GetBTree(const void* key, size_t key_size);
    public:
        /**
         * Constructor
         */
        TCBTreeIndex();

        virtual ~TCBTreeIndex();

        static Index* CreateIndex();

        static void RegisterIndex();

        /**
         *
         * Available options:
         * - filename: String with file where the transaction data is stored (multi)
         * - max-items-per-bucket: int32_t
         * - leaf-members: int32_t
         * - non-leaf-members: int32_t
         * - buckets: StorageUnit
         * - record-alignment: int8_t
         * - free-pool-size: int8_t
         * - compression: String (none, deflate, bzip2, tcbs)
         * - leaf-cache-size: StorageUnit
         * - non-leaf-cache-size: StorageUnit
         * - mem-mapped-size: StorageUnit
         * - defrag: int32_t
         * - checksum: Boolean
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context);

        virtual enum lookup_result Lookup(const void* key, size_t key_size,
                google::protobuf::Message* message);

        virtual enum put_result Put(const void* key, size_t key_size,
                const google::protobuf::Message& message);

        virtual enum put_result PutIfAbsent(
                const void* key, size_t key_size,
                const google::protobuf::Message& message);

        virtual enum delete_result Delete(const void* key, size_t key_size);

        virtual std::string PrintProfile();

        virtual std::string PrintLockStatistics();

        /**
         * prints trace information about the btree
         */
        virtual std::string PrintTrace();

        virtual uint64_t GetItemCount();

        virtual uint64_t GetEstimatedMaxItemCount();

        virtual uint64_t GetPersistentSize();

        /**
         * returns true iff the index supports cursors. The tc-btree only
         * supports cursor for b-tress with a single file
         */
        virtual bool SupportsCursor();

        /**
         * Creates a new cursor to walk thought the b-tree.
         *
         * The cursor access is only supported for b-trees with a single file.
         */
        virtual IndexCursor* CreateCursor();

        virtual IndexIterator* CreateIterator();
};

}
}

#endif  // TC_BTREE_INDEX_H__
