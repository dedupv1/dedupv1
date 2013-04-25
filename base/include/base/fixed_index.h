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

#ifndef FIXED_INDEX_H__
#define FIXED_INDEX_H__

#include <tbb/atomic.h>

#include <stdbool.h>
#include <stdint.h>

#include <tcutil.h>
#include <tcfdb.h>
#include <gtest/gtest_prod.h>
#include <string>
#include <vector>

#include <base/base.h>
#include <base/index.h>
#include <base/profile.h>
#include <base/fileutil.h>

namespace dedupv1 {
namespace base {

class FixedIndex;

/**
 * Iterator for the fixed index.
 */
class FixedIndexIterator : public IndexIterator {
    private:
        DISALLOW_COPY_AND_ASSIGN(FixedIndexIterator);

        /**
         * Index of the iterator
         */
        FixedIndex* index_;
        int64_t id;

        /**
         * version counter of the iterator at the time it is created.
         * An iterator should fail if the index is changed after the iterator is created
         */
        int version_counter;
    public:
        /**
         * Constructor
         */
        explicit FixedIndexIterator(FixedIndex* index);

        /**
         * Destructor
         */
        virtual ~FixedIndexIterator();

        /**
         * Lookups the next entry in the fixed index
         */
        virtual enum lookup_result Next(void* key, size_t* key_size,
                google::protobuf::Message* message);

};

/**
 * Disk-based Fixed Size array in an own implementation.
 * The type name is "disk-fixed".
 */
class FixedIndex : public IDBasedIndex {
    private:
        friend class FixedIndexTest;
        friend class FixedIndexIterator;
        FRIEND_TEST(FixedIndexTest, GetFile);

        /**
         * State enumeration
         */
        enum fixed_index_state {
            FIXED_INDEX_STATE_CREATED,
            FIXED_INDEX_STATE_STARTED
        };

        /**
         * Files used by the fixed index
         */
        std::vector<dedupv1::base::File*> files;

        /**
         * File names used by the fixed index
         */
        std::vector<std::string> filename;

        // Tune Options

        /**
         * Fixed width on an individual entry
         */
        uint32_t width;

        /**
         * width + meta data. Aligned on full powers of a sector size, e.g.
         * 512, 1024, 2048, ...
         */
        uint32_t bucket_size;

        /**
         * Maximal size of the complete fixed index in bytes.
         * This includes the per entry meta data as well as the per-index and per-file overhead.
         * So the sum of all file sizes will never be larger than this value.
         * If the files are preallocated, the files will have size this after the creating start.
         */
        uint64_t size;

        /**
         * current state
         */
        enum fixed_index_state state;

        dedupv1::base::Profile profiling;
        dedupv1::base::Profile disk_time;

        /**
         * version counter. The version counter is changed each time
         * the index is changed
         */
        tbb::atomic<uint64_t> version_counter;

        bool GetFile(int64_t id, dedupv1::base::File** file, int64_t* file_id);

        size_t GetOffset(dedupv1::base::File* file, int64_t file_id);

        /**
         * Write a bucket
         */
        enum put_result WriteBucket(dedupv1::base::File* file,
                int64_t file_id,
                int64_t global_id,
                const google::protobuf::Message& message);

        /**
         * Reads a bucket
         */
        enum lookup_result ReadBucket(dedupv1::base::File* file,
                int64_t file_id,
                int64_t global_id,
                google::protobuf::Message* message);

        /**
         * Deletes a bucket.
         * The bucket is marked with a "delete" flag
         */
        enum delete_result DeleteBucket(dedupv1::base::File* file, int64_t file_id, int64_t global_id);

        bool Format(File* file);

        /**
         * Checks if the fixed index super block is valid
         */
        bool CheckFileSuperBlock(File* file);
    public:
        static const uint64_t kDefaultSize = 0;
        static const uint32_t kDefaultWidth = 512;

        /**
         * Constructor
         */
        FixedIndex();

        /**
         * Destructor
         */
        virtual ~FixedIndex();

        /**
         * Create a new index of the given type
         */
        static Index* CreateIndex();

        static void RegisterIndex();

        /**
         * configured the index
         *
         * Available options:
         * - filename: String with file where the transaction data is stored (multi)
         * - width: StorageUnit
         * - size: StorageUnit
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Starts the given fixed index.
         */
        bool Start(const dedupv1::StartContext& start_context);

        /**
         * Lookup a given bucket.
         * The key is interpreted as uint64 index.
         *
         * If the given index has not been written before or if the last
         * operation was a delete option, LOOKUP_NOT_FOUND is returned.
         *
         * If the given key is beyond the index capacity, LOOKUP_NOT_FOUND is returned.
         */
        virtual enum lookup_result Lookup(const void* key, size_t key_size,
                google::protobuf::Message* message);

        /**
         * Puts a given bucket.
         * The key is interpreted as uint64 index.
         *
         * If the given key is beyond the index capacity, PUT_ERROR is returned.
         */
        virtual enum put_result Put(const void* key, size_t key_size,
                const google::protobuf::Message& message);

        /**
         * The fixed index does not support PutIfAbsent to avoid reading the data before writing for performance reasons.
         * A call of this method always returns PUT_ERROR.
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
         * Deletes a given bucket.
         * The key is interpreted as uint64 index.
         *
         * If the given key is beyond the index capacity, DELETE_ERROR is returned.
         */
        virtual enum delete_result Delete(const void* key, size_t key_size);

        virtual std::string PrintProfile();
        virtual std::string PrintTrace();

        virtual uint64_t GetPersistentSize();

        virtual uint64_t GetItemCount();

        int64_t GetLimitId();

        virtual bool SupportsIterator();
        virtual IndexIterator* CreateIterator();
};

}
}

#endif  // FIXED_INDEX_H__
