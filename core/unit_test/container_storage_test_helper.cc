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
#include <list>

#include <gtest/gtest.h>

#include <core/dedup.h>
#include <base/locks.h>

#include <core/log_consumer.h>
#include <base/index.h>
#include <core/container.h>
#include <core/log.h>
#include <core/storage.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/crc32.h>
#include <core/container_storage.h>
#include <core/fingerprinter.h>

#include "storage_test.h"
#include "container_storage_test_helper.h"
#include <test_util/log_assert.h>

using std::string;
using std::pair;
using dedupv1::base::crc;
using dedupv1::base::strutil::ToString;
using dedupv1::Fingerprinter;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::lookup_result;
using dedupv1::log::Log;
using dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START;
using std::tr1::make_tuple;

namespace dedupv1 {
namespace chunkstore {

void SetDefaultStorageOptions(Storage* storage) {
    SetDefaultStorageOptions(storage, make_tuple(4, 0, false, 16));
}

void SetDefaultStorageOptions(Storage* storage, ::std::tr1::tuple<int, int, bool, int> param) {
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-1"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-2"));
    ASSERT_TRUE(storage->SetOption("meta-data", "sqlite-disk-btree"));
    ASSERT_TRUE(storage->SetOption("meta-data.filename", "work/container-metadata"));
    ASSERT_TRUE(storage->SetOption("meta-data.cache-size", "2K"));
    ASSERT_TRUE(storage->SetOption("meta-data.max-item-count", "8M"));
    ASSERT_TRUE(storage->SetOption("container-size", "512K"));
    ASSERT_TRUE(storage->SetOption("size", "1G"));

    int background_thread_count = ::std::tr1::get<0>(param);
    int use_compression = ::std::tr1::get<1>(param);
    bool gc_type = ::std::tr1::get<2>(param);
    int write_container_count = ::std::tr1::get<3>(param);

    ASSERT_TRUE(storage->SetOption("background-commit.thread-count", ToString(background_thread_count)));

    if (use_compression == 1) {
        ASSERT_TRUE(storage->SetOption("compression", "deflate"));
    } else if (use_compression == 2) {
        ASSERT_TRUE(storage->SetOption("compression", "bz2"));
    }
    if (gc_type) {
        ASSERT_TRUE(storage->SetOption("gc", "greedy"));
        ASSERT_TRUE(storage->SetOption("gc.type","sqlite-disk-btree"));
        ASSERT_TRUE(storage->SetOption("gc.filename", "work/merge-candidates"));
        ASSERT_TRUE(storage->SetOption("gc.max-item-count", "64"));
        ASSERT_TRUE(storage->SetOption("alloc", "bitmap"));
        ASSERT_TRUE(storage->SetOption("alloc.type","sqlite-disk-btree"));
        ASSERT_TRUE(storage->SetOption("alloc.filename", "work/container-bitmap"));
        ASSERT_TRUE(storage->SetOption("alloc.max-item-count", "2K"));
    } else {
        ASSERT_TRUE(storage->SetOption("size", "1G"));
        ASSERT_TRUE(storage->SetOption("gc", "greedy"));
        ASSERT_TRUE(storage->SetOption("gc.type","sqlite-disk-btree"));
        ASSERT_TRUE(storage->SetOption("gc.filename", "work/merge-candidates"));
        ASSERT_TRUE(storage->SetOption("gc.max-item-count", "64"));
        ASSERT_TRUE(storage->SetOption("alloc", "memory-bitmap"));
        ASSERT_TRUE(storage->SetOption("alloc.type","sqlite-disk-btree"));
        ASSERT_TRUE(storage->SetOption("alloc.filename", "work/container-bitmap"));
        ASSERT_TRUE(storage->SetOption("alloc.max-item-count", "2K"));
    }
    if (write_container_count > 0) {
        ASSERT_TRUE(storage->SetOption("write-container-count", ToString(write_container_count)));
    }
}

void SetDefaultStorageOptions(ChunkStore* chunk_store, ::std::tr1::tuple<int, int, bool, int> param) {
    ASSERT_TRUE(chunk_store->SetOption("filename", "work/container-data-1"));
    ASSERT_TRUE(chunk_store->SetOption("filename", "work/container-data-2"));
    ASSERT_TRUE(chunk_store->SetOption("meta-data", "sqlite-disk-btree"));
    ASSERT_TRUE(chunk_store->SetOption("meta-data.filename", "work/container-metadata"));
    ASSERT_TRUE(chunk_store->SetOption("meta-data.max-item-count", "8M"));
    ASSERT_TRUE(chunk_store->SetOption("meta-data.cache-size", "2K"));
    ASSERT_TRUE(chunk_store->SetOption("container-size", "512K"));
    ASSERT_TRUE(chunk_store->SetOption("size", "1G"));

    int background_thread_count = ::std::tr1::get<0>(param);
    int use_compression = ::std::tr1::get<1>(param);
    bool gc_type = ::std::tr1::get<2>(param);
    int write_container_count = ::std::tr1::get<3>(param);

    ASSERT_TRUE(chunk_store->SetOption("background-commit.thread-count", ToString(background_thread_count)));

    if (use_compression == 1) {
        ASSERT_TRUE(chunk_store->SetOption("compression", "deflate"));
    } else if (use_compression == 2) {
        ASSERT_TRUE(chunk_store->SetOption("compression", "bz2"));
    }
    if (gc_type) {
        ASSERT_TRUE(chunk_store->SetOption("gc", "greedy"));
        ASSERT_TRUE(chunk_store->SetOption("gc.type","sqlite-disk-btree"));
        ASSERT_TRUE(chunk_store->SetOption("gc.filename", "work/merge-candidates"));
        ASSERT_TRUE(chunk_store->SetOption("gc.max-item-count", "64"));
        ASSERT_TRUE(chunk_store->SetOption("alloc", "bitmap"));
        ASSERT_TRUE(chunk_store->SetOption("alloc.type","sqlite-disk-btree"));
        ASSERT_TRUE(chunk_store->SetOption("alloc.filename", "work/container-bitmap"));
        ASSERT_TRUE(chunk_store->SetOption("alloc.max-item-count", "2K"));
    } else {
        ASSERT_TRUE(chunk_store->SetOption("size", "1G"));
        ASSERT_TRUE(chunk_store->SetOption("gc", "greedy"));
        ASSERT_TRUE(chunk_store->SetOption("gc.type","sqlite-disk-btree"));
        ASSERT_TRUE(chunk_store->SetOption("gc.filename", "work/merge-candidates"));
        ASSERT_TRUE(chunk_store->SetOption("gc.max-item-count", "64"));
        ASSERT_TRUE(chunk_store->SetOption("alloc", "memory-bitmap"));
        ASSERT_TRUE(chunk_store->SetOption("alloc.type","sqlite-disk-btree"));
        ASSERT_TRUE(chunk_store->SetOption("alloc.filename", "work/container-bitmap"));
        ASSERT_TRUE(chunk_store->SetOption("alloc.max-item-count", "2K"));
    }
    if (write_container_count > 0) {
        ASSERT_TRUE(chunk_store->SetOption("write-container-count", ToString(write_container_count)));
    }
}

}
}

