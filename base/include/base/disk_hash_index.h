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
#ifndef DISK_HASH_INDEX_H__
#define DISK_HASH_INDEX_H__

#include <vector>
#include <list>
#include <string>
#include <tr1/unordered_map>
#include <map>

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

class DiskHashIndex;
class TCMemHashIndex;

namespace internal {

// declared in disk_hash_index_transaction.h
class DiskHashIndexTransactionSystem;
class DiskHashIndexTransaction;
class DiskHashCachePage;
class DiskHashCacheEntry;

/**
 * Data structure for hash entry
 * The hash entries form a linked list when loaded in main memory.
 */
class DiskHashEntry {
    private:
        DISALLOW_COPY_AND_ASSIGN(DiskHashEntry);
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
         * returns the current key
         */
        inline void* mutable_key();

        /**
         * returns a mutable pointer to the current value.
         * When a client copied data into the value, the maximal
         * allowed length is still max_value_size()
         */
        inline void* mutable_value();
    public:
        /**
         * Constructor.
         * Buffer is not assigned. key and value are set to zero length.
         *
         * @param max_key_size maximal size allowed for a key.
         * @param max_value_size maximal size allowed for a entry value.
         */
        DiskHashEntry(uint32_t max_key_size, uint32_t max_value_size);

        /**
         * Parses a entry from the given buffer.
         *
         * @param buffer
         * @param buffer_size
         * @return true iff ok, otherwise an error has occurred
         */
        bool ParseFrom(byte* buffer, size_t buffer_size);

        /**
         * Assigns a new buffer to the entry.
         * Only after a buffer is assigned by this method or ParseFrom, the
         * key and the value can be set. The difference to ParseFrom is that
         * parse from tries to extract current key/value pairs from the data,
         * while this method only assigns a buffer to which later the key/value
         * pair is assigned
         *
         * @param buffer
         * @param buffer_size
         * @return true iff ok, otherwise an error has occurred
         */
        bool AssignBuffer(byte* buffer, size_t buffer_size);

        /**
         * Assigns a new key.
         * The key data is placed in the correct positions in the buffer.
         * A buffer must be assigned before this method is called.
         *
         * @param key
         * @param key_size
         * @return true iff ok, otherwise an error has occurred
         */
        inline bool AssignKey(const void* key, size_t key_size);

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
         * Assigns a new value by pointing to a byte array.
         * The byte array has to contain a message that has been serialized before.
         */
        bool AssignRawValue(const byte* new_value, size_t value_size);

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

        friend class DiskHashIndex;
        friend class DiskHashPage;
};

/**
 * Data structure representing the data page.
 * Sometimes this element is also called bucket.
 *
 * The items are still organized as a simple list.
 *
 * TODO (dmeister): The searching of a page might be optimized by a sorting so that
 * a binary search is possible.
 * TODO (dmeister) The space usage can be optimized
 */
class DiskHashPage {
        DISALLOW_COPY_AND_ASSIGN(DiskHashPage);
    public:
        /**
         * Maximal size of the header including everything.
         * The current message size is around 10 bytes, even with the
         * protobuf size and the message crc 32 byte are fine.
         */
        static const size_t kPageDataSize = 32;

        static const uint32_t kEntryOffsetOverflow = -1;
    private:
        /**
         * Item count in main data area
         */
        uint32_t item_count_;

        /**
         * Reference to the hash index
         */
        DiskHashIndex* index_;

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
         * The data buffer is equal to the buffer, but with the first
         * kPageDataSize bytes reserved for the page header.
         *
         */
        byte* data_buffer_;

        /**
         * Size of the data buffer
         */
        size_t data_buffer_size_;

        /**
         * Signals that the bucket is in overflow mode.
         *
         * TODO (dmeister): We should improve the implementation so that
         * a page also might "lose" its overflowed state.
         */
        bool overflow_;

        /**
         * message data about the page
         */
        DiskHashPageData page_data_;

        bool changed_since_last_serialize_;
    public:

        /**
         * Constructor
         * @param c index to which this page belongs to
         * @param bucket_id id of the bucket
         * @param buffer buffer to use. All entry data is encoded and should be encoded in memory provided
         * by the buffer.
         * @param buffer_size size of a the buffer.
         * @return
         */
        DiskHashPage(DiskHashIndex* c, uint64_t bucket_id, byte* buffer, size_t buffer_size);

        /**
         * Destructor
         * @return
         */
        ~DiskHashPage();

        /**
         * searches for a given key in the page and fills the message if the key is found.
         *
         * @param key key to search for
         * @param key_size size of the key
         * @param message message to fill when a entry with this key is found.
         * @return
         */
        enum lookup_result Search(const void* key, size_t key_size,
                google::protobuf::Message* message);

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
         * @param keep if true, an existing key is not updated
         * @return
         */
        enum put_result Update(const void* key, size_t key_size,
                const google::protobuf::Message& message, bool keep = false);

        /**
         * Merges a cache with a cache page with the intent for writting it back
         */
        bool MergeWithCache(DiskHashCachePage* cache_page,
                uint32_t* pinned_item_count,
                uint32_t* merged_item_count,
                uint32_t* merged_new_item_count);

        /**
         * Writes the page to a file.
         * The position within the file is calculated by the bucket id.
         *
         * @param file
         * @return
         */
        bool Write(dedupv1::base::File* file);

        /**
         * Reads the page form a file
         * The position within the file is calculated by the bucket id.
         *
         * @param file
         * @return
         */
        bool Read(dedupv1::base::File* file);

        bool ParseBuffer();

        bool SerializeToBuffer();

        /**
         * returns the number of in the apge
         * @return
         */
        inline uint32_t item_count() const;

        /**
         * returns a const pointer to the data buffer.
         *
         * The data buffer differs from the raw buffer in that
         * the data buffer is the place for the page payload (the entries).
         * The raw buffer consists of a page header of kPageDataSize byte
         * followed by the data buffer.
         */
        inline const byte* data_buffer() const;

        /**
         * returns a mutable pointer to the data buffer.
         * The data buffer differs from the raw buffer in that
         * the data buffer is the place for the page payload (the entries).
         * The raw buffer consists of a page header of kPageDataSize byte
         * followed by the data buffer.
         *
         */
        inline byte* mutable_data_buffer();

        inline size_t data_buffer_size() const;

        /**
         * returns the raw buffer.
         *
         * The data buffer differs from the raw buffer in that
         * the data buffer is the place for the page payload (the entries).
         * The raw buffer consists of a page header of kPageDataSize byte
         * followed by the data buffer.
         */
        inline const byte* raw_buffer() const;

        /**
         * Mutable pointer to the raw buffer of the page.
         * Use with extreme care. However, we need this so that the write cache
         * can update the data of a page.
         */
        inline byte* mutable_raw_buffer();

        /**
         * returns the size of the raw buffer, which is the size of the
         * page
         */
        inline size_t raw_buffer_size() const;

        inline size_t used_size() const;

        inline size_t used_data_size() const;

        /**
         * returns the bucket id of the page
         */
        inline uint64_t bucket_id() const;

        /**
         * returns a developer-readable representation of the page
         */
        std::string DebugString() const;
};

/**
 * The iterator is used to iterate through all entries of the disk-base index
 */
class DiskHashIndexIterator : public IndexIterator {
        DISALLOW_COPY_AND_ASSIGN(DiskHashIndexIterator);

        /**
         * Reference to the base index
         */
        DiskHashIndex* index_;

        /**
         * Id of the currently processed bucket
         */
        uint64_t bucket_id;

        /**
         * Pointer to the currently processed page
         */
        DiskHashPage* page;

        /**
         * index of the current entry within the page
         */
        uint64_t current_entry_index;

        /**
         * Version of the index at the time the iterator has been created.
         * The index is not allowed to be modified during the iteration.
         */
        uint64_t version_counter_;

        /**
         * buffer
         */
        byte* buffer_;

        /**
         * size of the buffer
         */
        size_t buffer_size;

        /**
         * Iterator within the overflow area
         */
        IndexIterator* overflow_iterator_;

        /**
         * Loads the current bucket
         * @return
         */
        DiskHashPage* LoadBucket();
    public:
        /**
         * Constructor.
         * @param index
         * @return
         */
        explicit DiskHashIndexIterator(DiskHashIndex* index);

        /**
         * Destructor
         * @return
         */
        virtual ~DiskHashIndexIterator();

        /**
         * Moves to the next entry.
         *
         * @param key
         * @param key_size
         * @param message
         * @return
         */
        virtual enum lookup_result Next(void* key, size_t* key_size,
                google::protobuf::Message* message);
};

}

/**
 * A paged-disk based hash table.
 * The on-disk structure is:
 *
 * hash(k) % bucket_count = 3
 * \verbatim
 * ----------------------------------------------------
 * -                     -                     -      -
 * ----------------------------------------------------
 * - bucket 1 - bucket 2 - bucket 3 - bucket 4 - .... -
 * ----------------------------------------------------
 * - k/v, k/v - k/v      -          - k/v             -
 *----------------------------------------------------*
 * \endverbatim
 *
 * The disk hash index can be configured to use a write-back cache.
 * The details are explained in the documentation of the methods and the
 * write_back_cache_ member. However, it is save to use the normal
 * access methods even if the write back cache is used. In this case, it works
 * like a write-through case.
 *
 */
class DiskHashIndex : public PersistentIndex {
    public:
    DISALLOW_COPY_AND_ASSIGN(DiskHashIndex);
    friend class internal::DiskHashIndexIterator;
    friend class internal::DiskHashPage;
    friend class internal::DiskHashIndexTransactionSystem;
    friend class internal::DiskHashIndexTransaction;
    friend class DiskHashIndexTest;
    FRIEND_TEST(DiskHashIndexTest, GetFileSequential);
    FRIEND_TEST(DiskHashIndexTest, RecoverItemCount);
    friend class DiskHashIndexTransactionTest;
    FRIEND_TEST(DiskHashIndexTransactionTest, NormalCommit);
    FRIEND_TEST(DiskHashIndexTransactionTest, NormalCommitWithRecovery);

    enum lazy_sync_state {
        CLEAN,
        DIRTY,
        IN_SYNC
    };

    /**
     * Statistics about the disk-based hash index
     */
    class Statistics {
        public:
            /**
             * Constructor
             */
            Statistics();

            /**
             * number of times a page lock was acquired and found free.
             */
            tbb::atomic<uint32_t> lock_free_;

            /**
             * number of times a page lock was acquire and found busy.
             */
            tbb::atomic<uint32_t> lock_busy_;

            dedupv1::base::Profile lookup_time_;
            dedupv1::base::Profile update_time_;
            dedupv1::base::Profile update_time_lock_wait_;
            dedupv1::base::Profile update_time_page_read_;
            dedupv1::base::Profile update_time_page_update_;
            dedupv1::base::Profile update_time_transaction_start_;
            dedupv1::base::Profile update_time_page_write_;
            dedupv1::base::Profile update_time_commit_;
            dedupv1::base::Profile delete_time_;
            dedupv1::base::Profile read_disk_time_;
            dedupv1::base::Profile write_disk_time_;

            dedupv1::base::Profile sync_time_;
            dedupv1::base::Profile sync_wait_time_;

            tbb::atomic<uint64_t> sync_count_;
            tbb::atomic<uint64_t> sync_wait_count_;

            tbb::atomic<uint64_t> write_cache_hit_count_;
            tbb::atomic<uint64_t> write_cache_miss_count_;

            /**
             * Number of evicted cache pages
             */
            tbb::atomic<uint64_t> write_cache_evict_count_;

            /**
             * Number of dirty evicted cache pages
             */
            tbb::atomic<uint64_t> write_cache_dirty_evict_count_;

            /**
             * Number of unused free pages
             */
            tbb::atomic<uint64_t> write_cache_free_page_count_;

            /**
             * Number of used (including dirty pages)
             */
            tbb::atomic<uint64_t> write_cache_used_page_count_;

            /**
             * Number of dirty pages
             */
            tbb::atomic<uint64_t> write_cache_dirty_page_count_;

            tbb::atomic<uint64_t> write_cache_persisted_page_count_;

            /**
             * Time spend in the write cache
             */
            dedupv1::base::Profile write_cache_read_time_;

            dedupv1::base::Profile write_cache_update_time_;

            dedupv1::base::Profile update_time_cache_read_;

            dedupv1::base::Profile cache_search_evict_page_time_;
            dedupv1::base::Profile cache_search_free_page_time_;

    };

    /**
     * Constant for the maximal allowed number of files for the disk-based hash index
     */
    static const size_t kMaxFiles = 32;

    static const double kDefaultEstimatedMaxFillRatio = 0.7;

    /**
     * Enumeration for the states of the disk-based hash index
     */
    enum disk_hash_index_state {
        INITED,//!< INITED
        STARTED//!< STARTED
    };
    private:

    /**
     * Number of items in the index.
     *
     * The item count is restored after a restart by the transaction system
     */
    tbb::atomic<uint64_t> item_count_;

    /**
     * Number of items in the cache that are currently dirty
     */
    tbb::atomic<uint64_t> dirty_item_count_;

    /**
     * number of items with unique keys on disk and in the write-back cache
     */
    tbb::atomic<uint64_t> total_item_count_;

    /**
     *  Number of buckets
     */
    uint64_t bucket_count_;

    /**
     * Overall file of the hash table in bytes.
     * If overflow area that is used if buckets are full is not limited.
     */
    uint64_t size_;

    /**
     * Name of the log file that stores index meta data
     */
    std::string info_filename_;

    /**
     * File object that stored index meta data.
     * Usually we try to avoid storing info data in plain files and use the info store instead, but
     * a) the info store is a dedupv1 core concept and is therefore not available here
     * b) The info store is only written during the initial create phase and it is therefore save to use a plain file
          (without any transaction system).

     * Set after a successful start, before the start the value is NULL
     */
    dedupv1::base::File* info_file_;

    /**
     * Vector of all index data filename
     */
    std::vector<std::string> filename_;

    /**
     * Vector of all index data files
     */
    std::vector<dedupv1::base::File*> file_;

    /**
     * Flag indicating if the data should be written using O_SYNC
     */
    bool sync_;

    /**
     * Flag indicating if the data should be flushed to disk if Sync() is called
     */
    bool lazy_sync_;

    /**
     * number of page locks that should be used
     */
    uint32_t page_locks_count_;

    /**
     * Locks two ensure that no two writer are active in the same bucket at one time.
     *
     */
    dedupv1::base::ReadWriteLockVector page_locks_;

    tbb::concurrent_vector<tbb::atomic<enum lazy_sync_state> > to_sync_flag_;

    tbb::concurrent_vector<dedupv1::base::ReadWriteLock> to_sync_lock_;

    size_t page_size_;

    /**
     * maximal allowed size an entry key can have
     */
    size_t max_key_size_;

    /**
     * maximal allowed size a entry value can have
     */
    size_t max_value_size_;

    /**
     * Statistics about the disk hash index
     */
    Statistics statistics_;

    /**
     * A version counter used to prevent updates when an
     * iterator is used. It is also used to check which transactions areas have to update the item count
     *
     * The version counter is restored after a restart by the transaction system.
     * The version numer is updated in DiskHashIndexTransaction::Start.
     */
    tbb::atomic<uint64_t> version_counter_;

    /**
     * state of the disk-based hash index
     */
    enum disk_hash_index_state state_;

    /**
     * Optional index that stores all entries that could not be stored in the normal
     * data area because the buckets of the entries have been full.
     *
     * The current overflow handling is extremely dump. If a bucket overflows the bucket switches
     * to an overflow mode in which entries that do not fit into the bucket are stored in an other index (the
     * overflow area). In searches, puts and deletes for this bucket this index is additionally checked.
     * The major problem is that the bucket holds no backpointer to the overflow items. Therefore
     * if a bucket is in overflow mode it stays in overflow mode for ever.
     */
    PersistentIndex* overflow_area_;

    /**
     * Flag if crc (for end-to-end data integrity) should be used
     */
    bool crc_;

    /**
     * Subsystem to allow transactions.
     * If the system crashes in the middle of a write, the system might get in an incorrect state.
     *
     * The usage of a transaction system is mandatory. If the transaction system is not
     * configured, it is configured with default values. This means e.g. that for each file of the
     * index a transaction file is created with the same filename, but an suffix "_trans".
     */
    internal::DiskHashIndexTransactionSystem* trans_system_;

    double estimated_max_fill_ratio_;

    /**
     * Index to hold pages to might be read to write back.
     *
     * The semantics of the Put/PutIfAbsend/Delete methods w.r.t. to the data safety are unchanged, except that
     * caching is used. This is similar to a write-through cache.
     *
     * The method PutDirty and possible similar methods with the Dirty keyword put data into the write cache without
     * persisting it. The data is persisted a) when an write-through-type call persists the dirty page, b)
     * EnsurePersisted writes the dirty page to disk or c) the system stops.
     *
     * The client of the write back cache is responsible for the cache eviction policy.
     */
    dedupv1::base::TCMemHashIndex* write_back_cache_;

    /**
     * maximal number of pages cached by the write back cache
     */
    uint64_t max_cache_page_count_;

    /**
     * maximal number of items cached in the write back cache.
     * Each page contains at least one item.
     */
    uint64_t max_cache_item_count_;


    /**
     * Class to hold information about the cache lines
     */
    class CacheLine {
        public:
            /**
             * Constructor
             */
            CacheLine(uint32_t cache_line_id, uint32_t cache_page_count, uint32_t cache_item_count);

            /**
             * cache line id
             */
            uint32_t cache_line_id_;

            /**
             * map from a cache entry to the bucket id currently stored in the entry.
             * If the element is not existing, no bucket is currently stored at that entry.
             */
            std::tr1::unordered_map<uint64_t, uint32_t> cache_page_map_;

            /**
             * maximal number of cache pages
             */
            uint32_t max_cache_page_count_;

            /**
             * maximal number of cached items
             */
            uint32_t max_cache_item_count_;

            /**
             * current number of cache pages
             */
            uint32_t current_cache_page_count_;

            /**
             * current number of cached items
             */
            uint32_t current_cache_item_count_;

            /**
             * Reference bit per cache page in the cache line
             */
            std::vector<bool> bucket_cache_state_;

            /**
             * 2. reference bit.
             * The second reference bit is set for dirty pages. It bit is removed
             * during the victim search.
             *
             * This favors clean pages for eviction instead of dirty pages.
             * However, even dirty pages are evicted in the second round.
             */
            std::vector<bool> bucket_cache_state2_;

            /**
             * a bit per page if the page is dirty
             */
            std::vector<bool> bucket_dirty_state_;

            /**
             * a bit per page if the page is free
             */
            std::vector<bool> bucket_free_state_;

            /**
             * a bit per page if the page is pinned
             */
            std::vector<bool> bucket_pinned_state_;

            /**
             * next victim pointer.
             * Used to find the next free id and to find the
             * next unused id
             */
            int next_cache_victim_;

            int next_dirty_search_cache_victim_;

            /**
             * Search the next evict page
             */
            bool SearchEvictPage(uint32_t* cache_id);

            dedupv1::base::Option<bool> SearchDirtyPage(uint32_t* cache_id);

            /**
             * Search a free cache page to use
             */
            bool SearchFreePage(uint32_t* cache_id);

            /**
             * 32-bit should also be enough. I don't expect more than
             * 4B cache pages anytime soon.
             */
            uint64_t GetCacheMapId(uint32_t cache_id);

            /**
             * returns true iff the cache is full.
             */
            bool IsCacheFull();

            /**
             * returns a developer-readable representation of the page
             */
            std::string DebugString() const;
    };

    /**
     * Vector of cache lines.
     * A cache line should only be accessed if the page lock with the same
     * id is held. Using different cache line here allows a higher parallelism.
     * All cache lines are processed in parallel.
     */
    std::vector<CacheLine*> cache_lines_;


    /**
     * Tree that stores information about all dirty disk pages
     */
    std::map<uint64_t, std::pair<uint32_t, uint32_t> > dirty_page_map_;

    /**
     * Spin lock to protect the dirty page tree
     */
    tbb::spin_mutex dirty_page_map_lock_;

    void MarkBucketAsDirty(uint64_t bucket_id, uint32_t cache_line_id, uint32_t cache_id);

    /**
     * Checks if the bucket is marked as dirty.
     * The information may be outdated on usage
     */
    bool IsBucketDirty(uint64_t bucket_id, uint32_t* cache_line_id, uint32_t* cache_id);
    void ClearBucketDirtyState(uint64_t bucket_id);

    bool GetNextDirtyBucket(uint64_t current_bucket_id,
        uint64_t* next_bucket_id,
        uint32_t* cache_line_id,
        uint32_t* cache_id);

    /**
     * Evict the page with the given id out of cache.
     *
     * Page lock is held when calling this methods
     *
     * @param cache_line the cache line holding the page
     * @param cache_id The id to be evicted
     * @param dirty if true, the page will be written back before deletion
     */
    bool EvictCacheItem(CacheLine* cache_line, uint32_t cache_id, bool dirty);

    /**
     * Writes information about the state and the configuration of the index to disk.
     * The system might be in an incorrect, not-recoverable state the system fails during the dumping.
     * Therefore if transactions are used, the state is only dumped at creation time. The dumped data
     * also contains the item count. If transactions are used, the item count is recovered from the transactions
     *
     * @return
     */
    bool DumpData();

    /**
     * Reads informations about the state and configuration of the index from disk.
     *
     * @return
     */
    bool ReadDumpData();

    /**
     * Cache index
     */
    void GetFileIndex(uint64_t bucket_id,
            uint32_t* file_index,
            uint32_t* cache_index);

    /**
     * Returns the file with the given file index
     * @param file_index
     * @return
     */
    File* GetFile(unsigned int file_index);

    bool DoSyncFile(int file_index);

    /**
     * marks a file as dirty.
     */
    inline void MarkAsDirty(int file_index);
    inline bool SyncFile(int file_index);

    /**
     * Checks if the given bucket is dirty.
     *
     * Hold bucket id lock while calling this method
     *
     * @return LOOKUP_FOUND if the page is dirty, LOOKUP_NOT_FOUND if the page is
     * in the cache and not dirty or if the page is not in the cache. LOOKUP_ERROR if
     * something went wrong.
     */
    lookup_result IsWriteBackPageDirty(uint64_t bucket_id);

    /**
     * Updates the page data/buffer with the data from the write cache if there is updated
     * data in the cache.
     *
     * Hold bucket id lock while calling this method
     *
     * @param allow_dirty if false, LOOKUP_NOT_FOUND is returned if the page is dirty.
     * This should NOT be used if the intention is to update the page later.
     *
     * @return LOOKUP_FOUND if the page was found in the cache and the page data is updated,
     * LOOKUP_NOT_FOUND if the page is not in the cache, the page data is unchanged in this case
     * LOOKUP_ERROR if something went wrong.
     */
    lookup_result ReadFromWriteBackCache(CacheLine* cache_line, internal::DiskHashCachePage* page);

    /**
     * Copies the page to the write cache.
     * Updates the write cache if the page was stored there before.
     * If dirty is set to true, the page must be persisted later.
     *
     * Hold bucket id lock while calling this method.
     *
     * The page can essentially be seen as const, but we have to update its internal buffer structure
     */
    bool CopyToWriteBackCache(CacheLine* cache_line, internal::DiskHashCachePage* page);

    /**
     * Internal method used by Put and PutOverwrite to avoid copying nearly the whole method
     * implementation.
     */
    put_result InternalPut(const void* key, size_t key_size, const google::protobuf::Message& message, bool keep = false);

    /**
     * Internal method used by Lookup and LookupDirty to avoid copying nearly the whole method implementation.
     */
    lookup_result InternalLookup(const void* key, size_t key_size, google::protobuf::Message* message,
            enum cache_lookup_method cache_lookup_type,
            enum cache_dirty_mode dirty_mode);

    lookup_result LookupCacheOnly(const void* key, size_t key_size,
            enum cache_dirty_mode dirty_mode,
            google::protobuf::Message* message);

    bool WriteBackCachePage(CacheLine* cache_line, internal::DiskHashCachePage* cache_page);

    public:
    /**
     * Constructor
     * @return
     */
    DiskHashIndex();

    /**
     * Destructor
     * @return
     */
    virtual ~DiskHashIndex();

    static Index* CreateIndex();

    /**
     * Registers the disk-based hash index as "static-disk-hash" in the index factory
     */
    static void RegisterIndex();

    /**
     * Configures the disk-hash index.
     *
     * Available options:
     * - page-size: StorageUnit
     * - size: StorageUnit
     * - sync: Boolean
     * - use-key-as-hash: Boolean
     * - max-fill-ratio: Double, >0 & <=1
     * - filename: String with file where the transaction data is stored (multi)
     * - page-lock-count: StorageUnit
     * - max-key-size: size_t
     * - max-value-size: size_t
     * - checksum: Boolean
     * - estimated-max-fill-ratio: Double, >0 & <1 (has to be checked)
     * - overflow-area: String
     * - overflow-area.: String
     * - write-cache: String
     * - write-cache.: String
     * - transactions.: String
     *
     * @param option_name
     * @param option
     * @return
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Starts (maybe created) the hash index.
     */
    bool Start(const dedupv1::StartContext& start_context);

    /**
     * searches the hash index for a given key.
     * As the index, is a page-based hash index, the key is hashed to
     * a page, then the page is loaded and parsed. The entries of the page
     * form a linked list in which a key for this key is searched.
     *
     * Also checks the write back cache. The method doesn't go to disk if there
     * is a clean version of the page
     */
    enum lookup_result Lookup(const void* key, size_t key_size,
            google::protobuf::Message* message);

    /**
     * searches the hash index for a given key.
     * As the index, is a page-based hash index, the key is hashed to
     * a page, then the page is loaded and parsed. The entries of the page
     * form a linked list in which a key for this key is searched.
     *
     * Also checks the write back cache. The method doesn't go to disk if there
     * is a clean or a dirty version of the page.
     *
     * The client has to deal with the fact that the data read might not be persisted
     * on disk in case of crashes. The client has to ensure that its behavior is correct in this
     * case, too.
     */
    enum lookup_result LookupDirty(const void* key, size_t key_size,
            enum cache_lookup_method cache_lookup_type,
            enum cache_dirty_mode dirty_mode,
            google::protobuf::Message* message);

    /**
     * Inserts or possibly overwrites a key/value pair.
     *
     * The method also checks the write back cache and used the newer version stored
     * there. It persists a possible dirty version and marks the page clean.
     */
    virtual enum put_result Put(const void* key, size_t key_size,
            const google::protobuf::Message& message);

    /**
     * Insert but not overwrites a key/value pair.
     */
    virtual enum put_result PutIfAbsent(
            const void* key, size_t key_size,
            const google::protobuf::Message& message);

    /**
     * Inserts or updates the key/value pair and marks it as dirty. The next time the page
     * is written, the value is put to disk.
     *
     * The method also checks the write back cache and used the newer version stored
     * there. It persists a possible dirty version and marks the page clean.
     */
    virtual enum put_result PutDirty(const void* key, size_t key_size,
                const google::protobuf::Message& message, bool pin);

    /**
     * If there is a dirty version of the page in memory, force it to disk
     */
    virtual enum put_result EnsurePersistent(const void* key, size_t key_size, bool* pinned);

    /**
     * Method is used to try to persist any dirty page. Is scans a series of cache lines and searches for the next
     * dirty cache page and writes the page back. It at most persistes max_batch_size many pages. There is no control
     * which pages are selected.
     *
     * It is similar to PersistAllDirty, but can be used in parallel and in a way that can be interrupted.
     *
     * It searched for dirty page in at most max_batch_size many cache lines. If it found any dirty pages, the
     * persisted out variable is set to true.
     */
    bool TryPersistDirtyItem(
            uint32_t max_batch_size,
            uint64_t* resume_handle,
            bool* persisted);

    virtual enum lookup_result ChangePinningState(const void* key, size_t key_size, bool new_pin_state);

    /**
     * returns true iff the configuration allows the usage of the write cache.
     */
    virtual bool IsWriteBackCacheEnabled();

    /**
     * Goes to everything in the cache
     */
    virtual bool DropAllPinned();

    /**
     * Goes to everything in the cache
     */
    virtual bool PersistAllDirty();

    /**
     * Deletes a key/value pair by key.
     *
     * The method also checks the write back cache and used the newer version stored
     * there. It persists a possible dirty version and marks the page clean.
     */
    virtual enum delete_result Delete(const void* key, size_t key_size);

    virtual std::string PrintTrace();

    virtual std::string PrintLockStatistics();

    /**
     * Prints profile information about the disk-based hash index
     * @return
     */
    virtual std::string PrintProfile();

    /**
     * returns the size in bytes, the index uses on persistent storage.
     */
    virtual uint64_t GetPersistentSize();

    /**
     * Returns the number of current items.
     * Implementation note: This value will be incorrect after crashes.
     * @return
     */
    virtual uint64_t GetItemCount();

    /**
     * Returns the number of items that are currently dirty in the cache
     */
    virtual uint64_t GetDirtyItemCount();

    virtual uint64_t GetTotalItemCount();

    /**
     * Returns the configured page size.
     * @return
     */
    inline size_t page_size() const;

    /**
     * returns the configured maximal allowed key size
     * @return
     */
    inline size_t max_key_size() const;

    /**
     * returns the configured maximal allowed value size
     * @return
     */
    inline size_t max_value_size() const;

    /**
     * returns the configured minimal value size.
     * @return
     */
    inline size_t min_value_size() const;

    /**
     * returns the number of bucket.
     * @return
     */
    inline unsigned int bucket_count() const;

    /**
     * returns a pointer to the transaction system or NULL
     * if no transaction system is used
     * @return
     */
    inline internal::DiskHashIndexTransactionSystem* transaction_system();

    /**
     * Creates a new iterator
     */
    virtual IndexIterator* CreateIterator();

    /**
     * Returns the index of the page bucket into which the key data would be stored.
     * This is not a kind of test that the key is stored there, but if the key
     * exists it will be stored in that bucket.
     *
     * @param key
     * @param key_size
     * @return
     */
    uint64_t GetBucket(const void* key, size_t key_size);

    uint64_t GetEstimatedMaxItemCount();

    uint64_t GetEstimatedMaxCacheItemCount();
};

void DiskHashIndex::MarkAsDirty(int file_index) {
    to_sync_flag_[file_index] = DIRTY;
}

bool DiskHashIndex::SyncFile(int file_index) {
    if (!lazy_sync_) {
        return true;
    }
    return DoSyncFile(file_index);
}

const void* internal::DiskHashEntry::key() const {
    if (buffer_ == NULL) {
        return NULL;
    }
    return buffer_ + sizeof(key_size_) + sizeof(value_size_);
}

void* internal::DiskHashEntry::mutable_key() {
    if (buffer_ == NULL) {
        return NULL;
    }
    return buffer_ + sizeof(key_size_) + sizeof(value_size_);
}

uint32_t internal::DiskHashEntry::key_size() const {
    return this->key_size_;
}

const void* internal::DiskHashEntry::value() const {
    if (buffer_ == NULL) {
        return NULL;
    }
    return buffer_ + sizeof(key_size_) + sizeof(value_size_) + this->max_key_size_;
}

void* internal::DiskHashEntry::mutable_value() {
    if (buffer_ == NULL) {
        return NULL;
    }
    return buffer_ + sizeof(key_size_) + sizeof(value_size_) + this->max_key_size_;
}

uint32_t internal::DiskHashEntry::value_size() const {
    return this->value_size_;
}

uint32_t internal::DiskHashPage::item_count() const {
    return this->item_count_;
}

const byte* internal::DiskHashPage::raw_buffer() const {
    return this->buffer_;
}

byte* internal::DiskHashPage::mutable_raw_buffer() {
    return this->buffer_;
}

size_t internal::DiskHashPage::raw_buffer_size() const {
    return this->buffer_size_;
}

const byte* internal::DiskHashPage::data_buffer() const {
    return this->data_buffer_;
}

byte* internal::DiskHashPage::mutable_data_buffer() {
    return this->data_buffer_;
}

size_t internal::DiskHashPage::data_buffer_size() const {
    return this->data_buffer_size_;
}

size_t internal::DiskHashPage::used_size() const {
    return kPageDataSize + used_data_size();
}

size_t internal::DiskHashPage::used_data_size() const {
    size_t normal_size_per_entry = sizeof(uint32_t) + sizeof(uint32_t) + index_->max_key_size() + index_->max_value_size();
    return (item_count() *  normal_size_per_entry);
}

bool internal::DiskHashEntry::AssignKey(const void* key, size_t key_size) {
    if (this->buffer_ == NULL) return false;
    if (this->max_key_size_ < key_size) return false;

    memcpy(this->buffer_, &key_size, sizeof(uint32_t));
    memcpy(this->mutable_key(), key, key_size);
    this->key_size_ = key_size;
    return true;
}

size_t internal::DiskHashEntry::entry_data_size() const {
    return sizeof(uint32_t) + sizeof(uint32_t) + this->max_key_size_ + this->max_value_size_;
}

uint64_t internal::DiskHashPage::bucket_id() const {
    return this->bucket_id_;
}
uint32_t internal::DiskHashEntry::max_key_size() const {
    return this->max_key_size_;
}

uint32_t internal::DiskHashEntry::max_value_size() const {
    return this->max_value_size_;
}

internal::DiskHashIndexTransactionSystem* DiskHashIndex::transaction_system() {
    return this->trans_system_;
}

size_t DiskHashIndex::page_size() const {
    return page_size_;
}

size_t DiskHashIndex::max_key_size() const {
    return max_key_size_;
}

size_t DiskHashIndex::max_value_size() const {
    return max_value_size_;
}

unsigned int DiskHashIndex::bucket_count() const {
    return this->bucket_count_;
}

}
}

#endif  // DISK_HASH_INDEX_H__
