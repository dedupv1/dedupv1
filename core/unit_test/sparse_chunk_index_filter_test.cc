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

#include "filter_test.h"
#include <test_util/log_assert.h>
#include <core/filter.h>
#include <gtest/gtest.h>
#include <core/dedup_system.h>
#include <core/sparse_chunk_index_filter.h>
#include <base/logging.h>
#include "dedup_system_test.h"
#include <string>
#include <cryptopp/cryptlib.h>
#include <cryptopp/rng.h>

using std::string;
using testing::TestWithParam;
using dedupv1::chunkindex::ChunkMapping;
using namespace CryptoPP;
using dedupv1::filter::SparseChunkIndexFilter;

LOGGER("SparseChunkIndexFilterTest");

namespace dedupv1 {
namespace filter {

class SparseChunkIndexFilterTest : public TestWithParam<const char*> {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;
    DedupSystem* system_;

    virtual void SetUp() {
        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());
        system_ = DedupSystemTest::CreateDefaultSystem(GetParam(),
            &info_store, &tp, true, false, false);
        ASSERT_TRUE(system_);
    }

    virtual void TearDown() {
        if (system_) {
            ASSERT_TRUE(system_->Stop(StopContext::FastStopContext()));
            ASSERT_TRUE(system_->Close());
            system_ = NULL;
        }
    }
};

TEST_P(SparseChunkIndexFilterTest, SimpleCheck) {
    SparseChunkIndexFilter* sparse_filter =
        dynamic_cast<SparseChunkIndexFilter*>(system_->filter_chain()->GetFilterByName(
                                                  "sparse-chunk-index-filter"));
    ASSERT_TRUE(sparse_filter);

    uint32_t non_existing_count = 0;
    uint32_t weak_count = 0;
    uint32_t count = 64 * 1024;
    for (int i = 0; i < count; i++) {
        // random fingerprint
        byte fp[20];
        LC_RNG rng(i);
        rng.GenerateBlock(fp, 20);
        ChunkMapping mapping(fp, 20);

        Filter::filter_result fr =  sparse_filter->Check(NULL,
            NULL,
            &mapping,
            NO_EC);
        ASSERT_FALSE(fr == Filter::FILTER_ERROR);
        if (fr == Filter::FILTER_WEAK_MAYBE) {
            weak_count++;
        } else if (fr == Filter::FILTER_NOT_EXISTING) {
            non_existing_count++;
        }
    }

    uint32_t min_expected_non_existing_count = count /
      (sparse_filter->sampling_factor() * 1.2);
    uint32_t max_expected_non_existing_count = count /
      (sparse_filter->sampling_factor() / 1.2);

    ASSERT_GE(non_existing_count, min_expected_non_existing_count);
    ASSERT_LE(non_existing_count, max_expected_non_existing_count);
}

INSTANTIATE_TEST_CASE_P(SparseChunkIndexFilter,
    SparseChunkIndexFilterTest,
    ::testing::Values("data/dedupv1_sparse_test.conf"));

INSTANTIATE_TEST_CASE_P(SparseChunkIndexFilter,
    FilterTest,
    ::testing::Values("sparse-chunk-index-filter;sampling-factor=32"));

INSTANTIATE_TEST_CASE_P(SparseChunkIndexFilter,
    DedupSystemTest,
    ::testing::Values("data/dedupv1_sparse_test.conf"));
}
}
