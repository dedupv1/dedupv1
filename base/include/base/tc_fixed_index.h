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
#ifndef TC_FIXED_INDEX_H__
#define TC_FIXED_INDEX_H__

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

namespace dedupv1 {
namespace base {

class TCFixedIndex;

class TCFixedIndexIterator : public IndexIterator {
        DISALLOW_COPY_AND_ASSIGN(TCFixedIndexIterator);
        TCFixedIndex* index_;
        int fixed_index_;
        uint64_t version_counter_;
    public:
        explicit TCFixedIndexIterator(TCFixedIndex* index_);
        virtual ~TCFixedIndexIterator();

        virtual enum lookup_result Next(void* key, size_t* key_size,
                google::protobuf::Message* message);

};

/**
 * Disk-based Fixed Size array in the Tokyo Cabinet implementation.
 * The type name is "tc-disk-fixed".
 */
class TCFixedIndex : public IDBasedIndex {
        DISALLOW_COPY_AND_ASSIGN(TCFixedIndex);
        friend class SingleFileTCFixedIndexCursor;
        friend class TCFixedIndexTest;
        friend class TCFixedIndexIterator;
        FRIEND_TEST(TCFixedIndexTest, GetDB);

        enum tc_fixed_index_state {
                TC_FIXED_INDEX_STATE_CREATED,
                TC_FIXED_INDEX_STATE_STARTED
        };

        std::vector<TCFDB*> fdb_;
        std::vector<std::string> filename_;

        // Tune Options

        /**
         * Fixed width on an individual entry
         */
        uint32_t width_;

        /**
         * Maximal size of the complete fixed index in bytes
         */
        uint64_t size;

        enum tc_fixed_index_state state_;

        dedupv1::base::Profile profiling;

        tbb::atomic<uint64_t> version_counter_;

        bool checksum_;

        bool GetDB(int64_t id, TCFDB** db, int64_t* db_id);
    public:
        TCFixedIndex();

        virtual ~TCFixedIndex();

        static Index* CreateIndex();

        static void RegisterIndex();

        /**
         *
         * Available options:
         * - filename: String with file where the transaction data is stored (multi)
         * - width: StorageUnit
         * - size: StorageUnit
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

        virtual enum lookup_result Lookup(const void* key, size_t key_size,
                google::protobuf::Message* message);

        virtual enum put_result Put(const void* key, size_t key_size,
                const google::protobuf::Message& message);

        virtual enum put_result PutIfAbsent(
                const void* key, size_t key_size,
                const google::protobuf::Message& message);

        virtual enum delete_result Delete(const void* key, size_t key_size);

        virtual std::string PrintProfile();
        virtual std::string PrintTrace();

        virtual uint64_t GetItemCount();

        virtual uint64_t GetPersistentSize();

        int64_t GetLimitId();

        int64_t GetMaxId();
        int64_t GetMinId();

        virtual IndexIterator* CreateIterator();

};

}
}

#endif  // TC_FIXED_INDEX_H__
