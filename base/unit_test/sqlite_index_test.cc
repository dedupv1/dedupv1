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

#include <gtest/gtest.h>

#include <base/index.h>
#include <base/sqlite_index.h>
#include "index_test.h"
#include <test_util/log_assert.h>
#include <base/logging.h>

namespace dedupv1 {
namespace base {

class SqliteIndexTest : public testing::TestWithParam<std::string> {
protected:
    USE_LOGGING_EXPECTATION();

    Index* index;
    std::string config;

    byte buffer[8192];
    int buffer_size;

    virtual void SetUp() {
        config = GetParam();

        index = IndexTest::CreateIndex(config);
        ASSERT_TRUE(index) << "Failed to create index";

        buffer_size = 8192;
        memset(buffer, 0, buffer_size);
    }

    virtual void TearDown() {
        if (index) {
            delete index;
        }
    }
};

INSTANTIATE_TEST_CASE_P(SqliteIndex,
    IndexTest,
    ::testing::Values("sqlite-disk-btree;filename=work/tc_test_data1;filename=work/tc_test_data2;max-item-count=16K",
        "sqlite-disk-btree;filename=work/tc_test_data;cache-size=16000;max-item-count=16K",
        "sqlite-disk-btree;filename=work/tc_test_data;max-key-size=8;max-item-count=16K",
        "sqlite-disk-btree;filename=work/tc_test_data;max-key-size=8;max-item-count=16K;preallocated-size=4M"
        ));

INSTANTIATE_TEST_CASE_P(SqliteIndex,
    SqliteIndexTest,
    ::testing::Values("sqlite-disk-btree;filename=work/tc_test_data1;filename=work/tc_test_data2;max-item-count=16K",
        "sqlite-disk-btree;filename=work/tc_test_data;cache-size=16000;max-item-count=16K",
        "sqlite-disk-btree;filename=work/tc_test_data;max-key-size=8;max-item-count=16K;preallocated-size=4M"
        ));

}
}
