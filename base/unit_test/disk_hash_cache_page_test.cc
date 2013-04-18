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

#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <map>

#include <gtest/gtest.h>

#include <base/index.h>
#include <base/disk_hash_cache_page.h>
#include <base/logging.h>
#include <base/protobuf_util.h>
#include "index_test.h"
#include <test_util/log_assert.h>

using std::map;
using dedupv1::base::internal::DiskHashCachePage;
using dedupv1::base::internal::DiskHashCacheEntry;
using dedupv1::base::LOOKUP_FOUND;

LOGGER("DiskHashCachePageTest");

namespace dedupv1 {
namespace base {

class DiskHashCachePageTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    size_t buffer_size;
    byte* shared_buffer;

    virtual void SetUp() {
        buffer_size = 4096;
        shared_buffer = new byte[buffer_size];
    }

    virtual void TearDown() {
        delete[] shared_buffer;
    }
};

TEST_F(DiskHashCachePageTest, Update) {

    DiskHashCachePage page(0, buffer_size, 8, 32);

    uint64_t key = 2;
    IntData value;
    value.set_i(17);
    ASSERT_EQ(page.Update(&key, sizeof(key), value, false, true, false), PUT_OK);
    page.Store();
    memcpy(shared_buffer, page.raw_buffer(), page.raw_buffer_size());

    DiskHashCachePage page2(0, buffer_size, 8, 32);
    memcpy(page2.mutable_raw_buffer(), shared_buffer, page2.raw_buffer_size());

    IntData value2;
    bool is_dirty = false;
    bool is_pinned = false;
    ASSERT_EQ(page2.Search(&key, sizeof(key), &value2, &is_dirty, &is_pinned), LOOKUP_FOUND);
    ASSERT_EQ(value2.i(), 17);
    ASSERT_TRUE(is_dirty);
    ASSERT_FALSE(is_pinned);
}

TEST_F(DiskHashCachePageTest, Pin) {

    DiskHashCachePage page(0, buffer_size, 8, 32);

    uint64_t key = 2;
    IntData value;
    value.set_i(17);
    ASSERT_EQ(page.Update(&key, sizeof(key), value, false, true, true), PUT_OK);
    page.Store();
    memcpy(shared_buffer, page.raw_buffer(), page.raw_buffer_size());

    DiskHashCachePage page2(0, buffer_size, 8, 32);
    memcpy(page2.mutable_raw_buffer(), shared_buffer, page2.raw_buffer_size());

    IntData value2;
    bool is_dirty = false;
    bool is_pinned = false;
    ASSERT_EQ(page2.Search(&key, sizeof(key), &value2, &is_dirty, &is_pinned), LOOKUP_FOUND);
    ASSERT_EQ(value2.i(), 17);
    ASSERT_TRUE(is_dirty);
    ASSERT_TRUE(is_pinned);

    ASSERT_EQ(page2.ChangePinningState(&key, sizeof(key), false), LOOKUP_FOUND);
    page2.Store();
    memcpy(shared_buffer, page2.raw_buffer(), page2.raw_buffer_size());

    DiskHashCachePage page3(0, buffer_size, 8, 32);
    memcpy(page3.mutable_raw_buffer(), shared_buffer, page3.raw_buffer_size());

    value2.Clear();
    is_dirty = false;
    is_pinned = false;
    ASSERT_EQ(page3.Search(&key, sizeof(key), &value2, &is_dirty, &is_pinned), LOOKUP_FOUND);
    ASSERT_EQ(value2.i(), 17);
    ASSERT_TRUE(is_dirty);
    ASSERT_FALSE(is_pinned);
}

TEST_F(DiskHashCachePageTest, DropAllPinned) {

    DiskHashCachePage page(0, buffer_size, 8, 32);

    uint64_t key = 2;
    IntData value;
    value.set_i(17);
    ASSERT_EQ(page.Update(&key, sizeof(key), value, false, true, true), PUT_OK);
    key = 3;
    value.set_i(17);
    ASSERT_EQ(page.Update(&key, sizeof(key), value, false, true, false), PUT_OK);
    key = 4;
    value.set_i(17);
    ASSERT_EQ(page.Update(&key, sizeof(key), value, false, true, true), PUT_OK);

    uint64_t dropped_item_count = 0;
    ASSERT_TRUE(page.DropAllPinned(&dropped_item_count));
    ASSERT_EQ(page.item_count(), 1);
    ASSERT_EQ(2, dropped_item_count);
    page.Store();
    memcpy(shared_buffer, page.raw_buffer(), page.raw_buffer_size());

    DiskHashCachePage page2(0, buffer_size, 8, 32);
    memcpy(page2.mutable_raw_buffer(), shared_buffer, page2.raw_buffer_size());

    IntData value2;
    key = 3;
    bool is_dirty = false;
    bool is_pinned = false;
    ASSERT_EQ(page2.Search(&key, sizeof(key), &value2, &is_dirty, &is_pinned), LOOKUP_FOUND);
    ASSERT_EQ(value2.i(), 17);
    ASSERT_TRUE(is_dirty);
    ASSERT_FALSE(is_pinned);

    key = 2;
    ASSERT_EQ(page2.Search(&key, sizeof(key), &value2, &is_dirty, &is_pinned), LOOKUP_NOT_FOUND);
    key = 4;
    ASSERT_EQ(page2.Search(&key, sizeof(key), &value2, &is_dirty, &is_pinned), LOOKUP_NOT_FOUND);
}

TEST_F(DiskHashCachePageTest, OverwriteUpdate) {

    DiskHashCachePage page(0, buffer_size, 8, 32);

    uint64_t key = 2;
    IntData value;
    value.set_i(17);
    ASSERT_EQ(page.Update(&key, sizeof(key), value, false, true, false), PUT_OK);
    value.set_i(42);
    ASSERT_EQ(page.Update(&key, sizeof(key), value, false, false, false), PUT_OK);
    page.Store();
    memcpy(shared_buffer, page.raw_buffer(), page.raw_buffer_size());

    DiskHashCachePage page2(0, buffer_size, 8, 32);
    memcpy(page2.mutable_raw_buffer(), shared_buffer, page2.raw_buffer_size());

    IntData value2;
    bool is_dirty = false;
    bool is_pinned = false;
    ASSERT_EQ(page2.Search(&key, sizeof(key), &value2, &is_dirty, &is_pinned), LOOKUP_FOUND);
    ASSERT_EQ(value2.i(), 42);
    ASSERT_TRUE(is_dirty);
    ASSERT_FALSE(is_pinned);
}

TEST_F(DiskHashCachePageTest, DoubleUpdate) {

    DiskHashCachePage page(0, buffer_size, 8, 32);

    uint64_t key = 2;
    IntData value;
    value.set_i(17);
    ASSERT_EQ(page.Update(&key, sizeof(key), value, false, true, false), PUT_OK);
    key = 3;
    value.set_i(42);
    ASSERT_EQ(page.Update(&key, sizeof(key), value, false, false, false), PUT_OK);
    page.Store();
    memcpy(shared_buffer, page.raw_buffer(), page.raw_buffer_size());

    DiskHashCachePage page2(0, buffer_size, 8, 32);
    memcpy(page2.mutable_raw_buffer(), shared_buffer, page2.raw_buffer_size());

    IntData value2;
    bool is_dirty = false;
    bool is_pinned = false;
    ASSERT_EQ(page2.Search(&key, sizeof(key), &value2, &is_dirty, &is_pinned), LOOKUP_FOUND);
    ASSERT_EQ(value2.i(), 42);
    ASSERT_FALSE(is_dirty);
    ASSERT_FALSE(is_pinned);
}

TEST_F(DiskHashCachePageTest, RaiseBuffer) {
    DiskHashCachePage page(0, buffer_size, 8, 32);

    for (int i = 0; i < 256; i++) {
        uint64_t key = i;
        IntData value;
        value.set_i(i);
        ASSERT_EQ(page.Update(&key, sizeof(key), value, false, true, false), PUT_OK);
        page.Store();
    }
}

TEST_F(DiskHashCachePageTest, FullPage) {
    int custom_buffer_size = 507; // magic number that leads to an illegal read. Might change when the cache page format changes
    DiskHashCachePage page(0, custom_buffer_size, 8, 4);

    int i = 0;
    for (i = 0;; i++) {
        if (!page.IsAcceptingNewEntries()) {
            break;
        }
        uint64_t key = i;
        IntData value;
        value.set_i(i);
        ASSERT_EQ(page.Update(&key, sizeof(key), value, false, true, false), PUT_OK);
        page.Store();
    }

    uint64_t key = 123123;
    ASSERT_EQ(page.Search(&key, sizeof(key), NULL, NULL, NULL), LOOKUP_NOT_FOUND);
}

TEST_F(DiskHashCachePageTest, FullPageRunBad) {
    int custom_buffer_size = 507; // magic number that leads to an illegal read. Might change when the cache page format changes
    DiskHashCachePage page(0, custom_buffer_size, 8, 4);

    byte* custom_buffer = new byte[1024];
    memset(custom_buffer, 0xFF, 1024);
    memset(custom_buffer, 0, custom_buffer_size); // we manipulate the area behind the normal buffer to trigger bad stuff

    byte* old_buffer = page.ReplaceBufferPointer(custom_buffer);

    int i = 0;
    for (i = 0;; i++) {
        if (!page.IsAcceptingNewEntries()) {
            break;
        }
        uint64_t key = i;
        IntData value;
        value.set_i(i);
        ASSERT_EQ(page.Update(&key, sizeof(key), value, false, true, false), PUT_OK);
        ASSERT_TRUE(page.Store());
    }

    uint64_t key = 123123;
    ASSERT_EQ(page.Search(&key, sizeof(key), NULL, NULL, NULL), LOOKUP_NOT_FOUND);

    page.ReplaceBufferPointer(old_buffer);
    delete[] custom_buffer;
}

TEST_F(DiskHashCachePageTest, FullPageDropPinned) {
    int custom_buffer_size = 507; // magic number that leads to an illegal read. Might change when the cache page format changes
    DiskHashCachePage page(0, custom_buffer_size, 8, 4);

    int i = 0;
    for (i = 0;; i++) {
        if (!page.IsAcceptingNewEntries()) {
            break;
        }
        uint64_t key = i;
        IntData value;
        value.set_i(i);
        ASSERT_EQ(page.Update(&key, sizeof(key), value, false, true, true), PUT_OK); // pinned
        page.Store();
    }

    uint64_t dropped_item_count = 0;
    ASSERT_TRUE(page.DropAllPinned(&dropped_item_count));
    ASSERT_EQ(0, page.item_count());
}

}
}
