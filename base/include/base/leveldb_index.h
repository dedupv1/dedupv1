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
#ifndef LEVELDB_INDEX_H__
#define LEVELDB_INDEX_H__

#include <stdbool.h>
#include <stdint.h>

#include <tbb/atomic.h>

#include <vector>
#include <tr1/tuple>

#include <leveldb/db.h>

#include <base/base.h>
#include <base/index.h>
#include <base/profile.h>
#include <base/locks.h>

  namespace dedupv1 {
  namespace base {

  /**
   * Disk-based LSM-tree implementation based on Leveldb.
   * Further information can be found at: https://code.google.com/p/leveldb/
   */
  class LeveldbIndex : public PersistentIndex {
    private:
      friend class LeveldbIndexIterator;
      DISALLOW_COPY_AND_ASSIGN(LeveldbIndex);

      leveldb::DB* db_;

      /**
       * Statistics about the leveldb
       */
      class Statistics {
        public:
          Statistics();

          dedupv1::base::Profile total_time_;

          dedupv1::base::Profile lookup_time_;

          dedupv1::base::Profile update_time_;

          dedupv1::base::Profile delete_time_;

          tbb::atomic<uint64_t> lookup_count_;

          tbb::atomic<uint64_t> update_count_;

          tbb::atomic<uint64_t> delete_count_;
      };

      Statistics stats_;

      /**
       * Option to use a Bloom filter.
       *
       * Default: 2 bits per key
       */
      int bloom_filter_bits_per_key_;

      /**
       * Option to use compression.
       */
      bool use_compression_;

      /**
       * Configurable block size the leveldb instance should use.
       * Can be set using SetOptions().
       */
      size_t block_size_;

      /**
       * Directory to store the index data in
       */
      std::string index_dir_;

      /**
       * Option to sync at each write.
       * Default: true
       */
      bool sync_;

      /**
       * Option to verify the checksum at each read options.
       */
      bool checksum_;

      /**
       * Configurable cache size in number of cached blocks.
       * 0 means no cache.
       */
      uint64_t cache_size_;

      /**
       * Option to set the maximal number of item of the index.
       * This value is only used for for displaying the fill ratio.
       * It is not used as a hard limit.
       */
      uint64_t estimated_max_item_count_;

      tbb::atomic<uint64_t> version_counter_;

      tbb::atomic<uint64_t> item_count_;

      /**
       * Interval between two times the lazy item count
       * is persisted.
       * Default: 1024
       */
      uint64_t lazy_item_count_persistent_interval_;

      /**
       * restores the item count from the index.
       * Should only be called in Start after the database has been
       * opened
       */
      bool RestoreItemCount();

      /**
       * Stores the item count in the index itself depending on the
       * version count. The item count is always stored if
       * force is true.
       */
      bool LazyStoreItemCount(uint64_t version_count, bool force);

      static const std::string kItemCountKeyString;
      static const int kDefaultBloomFilterBitsPerKey;
    public:
      /**
       * Constructor
       */
      LeveldbIndex();

      /**
       * Destructor
       */
      virtual ~LeveldbIndex();

      static Index* CreateIndex();

      static void RegisterIndex();

      /**
       * Configure the leveldb index instance
       */
      bool SetOption(const std::string& option_name, const std::string& option);

      /**
       * Starts the index
       * @return true iff ok, otherwise an error has occured
       */
      bool Start(const dedupv1::StartContext& start_context);

      virtual enum lookup_result Lookup(const void* key, size_t key_size,
          google::protobuf::Message* message);

      virtual enum put_result Put(const void* key, size_t key_size,
          const google::protobuf::Message& message);

      /**
       * Write batch support for leveldb
       */
      virtual put_result PutBatch(const std::vector<std::tr1::tuple<bytestring,
          const google::protobuf::Message*> >& data);

    virtual enum delete_result Delete(const void* key, size_t key_size);

    /**
     * Print profile information about the leveldb
     */
    virtual std::string PrintProfile();

    /**
     * Print trace information about the leveldb
     */
    virtual std::string PrintTrace();

    virtual uint64_t GetItemCount();

    virtual uint64_t GetEstimatedMaxItemCount();

    virtual uint64_t GetPersistentSize();

    /**
     * Creats an iterator instance of the database
     */
    virtual IndexIterator* CreateIterator();
};

}
}

#endif
