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

#include <string>
#include <map>

#include <gtest/gtest.h>

#include <base/index.h>
#include <base/disk_hash_index.h>
#include <base/option.h>
#include <base/fileutil.h>
#include <base/strutil.h>
#include <base/protobuf_util.h>
#include "index_test.h"
#include <test_util/log_assert.h>
#include <base/logging.h>
#include "dedupv1_base.pb.h"

using std::string;
using std::map;
using dedupv1::base::strutil::FromHexString;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::SerializeSizedMessage;

LOGGER("DiskHashIndexTest");

namespace dedupv1 {
namespace base {

class DiskHashIndexCacheTest : public testing::TestWithParam<std::string> {
protected:
    USE_LOGGING_EXPECTATION();

    PersistentIndex* index;
    std::string config;

    virtual void SetUp() {
        config = GetParam();
        index = NULL;

        Index* i = IndexTest::CreateIndex(config);
        ASSERT_TRUE(i);
        index = i->AsPersistentIndex();
        ASSERT_TRUE(index);
        ASSERT_TRUE(index->Start(dedupv1::StartContext()));
    }

    virtual void TearDown() {
        if (index) {
            delete index;
        }
    }
};

INSTANTIATE_TEST_CASE_P(DiskHashIndex,
    DiskHashIndexCacheTest,
    ::testing::Values(
        // Write-back cache
        string("static-disk-hash;max-key-size=8;max-value-size=8;page-lock-count=1;page-size=4K;size=4M;filename=work/data/hash_test_data;write-cache=true;write-cache.bucket-count=1K;write-cache.max-page-count=4")
        ));

TEST_P(DiskHashIndexCacheTest, EnsurePersistent) {
    uint64_t key = 10;
    IntData value;
    value.set_i(5);
    DEBUG("Put Dirty key " << ToHexString(&key, sizeof(key)) << " with key size " << sizeof(key));
    ASSERT_EQ(index->PutDirty(&key, sizeof(key), value, true), PUT_OK);

    TRACE("LookupDirty cache and dirty");
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_DEFAULT, CACHE_ALLOW_DIRTY, &value), LOOKUP_FOUND);

    TRACE("LookupDirty disk and clean");
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_BYPASS, CACHE_ONLY_CLEAN, &value), LOOKUP_NOT_FOUND);
    TRACE("LookupDirty cache and clean");
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_ONLY, CACHE_ONLY_CLEAN, &value), LOOKUP_NOT_FOUND);

    TRACE("EnsurePersistent");
    bool is_pinned = false;
    ASSERT_EQ(index->EnsurePersistent(&key, sizeof(key), &is_pinned), PUT_KEEP);
    ASSERT_TRUE(is_pinned);

    TRACE("ChangePinningState");
    ASSERT_EQ(index->ChangePinningState(&key, sizeof(key), false), LOOKUP_FOUND);

    TRACE("EnsurePersistent");
    is_pinned = false;
    ASSERT_EQ(index->EnsurePersistent(&key, sizeof(key), &is_pinned), PUT_OK);

    TRACE("LookupDirty disk and clean");
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_BYPASS, CACHE_ONLY_CLEAN, &value), LOOKUP_FOUND);
    TRACE("LookupDirty cache and clean");
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_ONLY, CACHE_ONLY_CLEAN, &value), LOOKUP_FOUND);
    TRACE("Test done");
}

TEST_P(DiskHashIndexCacheTest, EnsurePersistentAfterUpdate) {
    uint64_t key = 10;
    IntData value;
    value.set_i(5);
    ASSERT_EQ(index->PutDirty(&key, sizeof(key), value, true), PUT_OK);

    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_DEFAULT, CACHE_ALLOW_DIRTY, &value), LOOKUP_FOUND);
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_BYPASS, CACHE_ONLY_CLEAN, &value), LOOKUP_NOT_FOUND);
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_ONLY, CACHE_ONLY_CLEAN, &value), LOOKUP_NOT_FOUND);

    value.set_i(7);
    ASSERT_EQ(index->PutDirty(&key, sizeof(key), value, true), PUT_OK); // also pinned

    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_DEFAULT, CACHE_ALLOW_DIRTY, &value), LOOKUP_FOUND);
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_BYPASS, CACHE_ONLY_CLEAN, &value), LOOKUP_NOT_FOUND);
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_ONLY, CACHE_ONLY_CLEAN, &value), LOOKUP_NOT_FOUND);

    value.set_i(8);
    ASSERT_EQ(index->PutDirty(&key, sizeof(key), value, false), PUT_OK); // not pinned

    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_DEFAULT, CACHE_ALLOW_DIRTY, &value), LOOKUP_FOUND);
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_BYPASS, CACHE_ONLY_CLEAN, &value), LOOKUP_NOT_FOUND);
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_ONLY, CACHE_ONLY_CLEAN, &value), LOOKUP_NOT_FOUND);

    bool is_pinned = false;
    ASSERT_EQ(index->EnsurePersistent(&key, sizeof(key), &is_pinned), PUT_KEEP);
    ASSERT_TRUE(is_pinned);

    ASSERT_EQ(index->ChangePinningState(&key, sizeof(key), false), LOOKUP_FOUND);

    value.set_i(8);
    ASSERT_EQ(index->PutDirty(&key, sizeof(key), value, false), PUT_OK); // not pinned

    is_pinned = false;
    ASSERT_EQ(index->EnsurePersistent(&key, sizeof(key), &is_pinned), PUT_OK);

    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_BYPASS, CACHE_ONLY_CLEAN, &value), LOOKUP_FOUND);
    ASSERT_EQ(index->LookupDirty(&key, sizeof(key), CACHE_LOOKUP_ONLY, CACHE_ONLY_CLEAN, &value), LOOKUP_FOUND);
}

}
}
