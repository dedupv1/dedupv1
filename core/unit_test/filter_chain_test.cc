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

#include <gtest/gtest.h>
#include <core/filter_chain.h>
#include <core/filter.h>
#include <test_util/log_assert.h>

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <json/json.h>

using std::string;

namespace dedupv1 {
namespace filter {

class FilterChainTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    FilterChain* filter_chain;

    virtual void SetUp() {
        filter_chain = new FilterChain();
        ASSERT_TRUE(filter_chain);

        ASSERT_TRUE(filter_chain->AddFilter("block-index-filter"));
        ASSERT_TRUE(filter_chain->AddFilter("chunk-index-filter"));
    }

    virtual void TearDown() {
        if (filter_chain) {
            delete filter_chain;
            filter_chain = NULL;
        }
    }
};

TEST_F(FilterChainTest, Init) {
    std::list<Filter*>::const_iterator i = filter_chain->GetChain().begin();

    // Block Index Filter
    Filter* fc = *i;
    ASSERT_TRUE(fc);
    ASSERT_EQ(fc->GetMaxFilterLevel(), Filter::FILTER_STRONG_MAYBE);
    i++;

    // Chunk Index Filter
    fc = *i;
    ASSERT_TRUE(fc);
    ASSERT_EQ(fc->GetMaxFilterLevel(), Filter::FILTER_STRONG_MAYBE);
}

/**
 * Tests that the filter chain does not the same filter twice
 */
TEST_F(FilterChainTest, DoubleFilter) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("Filter .* already configured").Times(2);

    // these filter have already been added in SetUp
    ASSERT_FALSE(filter_chain->AddFilter("block-index-filter"));
    ASSERT_FALSE(filter_chain->AddFilter("chunk-index-filter"));
}

TEST_F(FilterChainTest, PrintLockStatistics) {
    string s = filter_chain->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_F(FilterChainTest, PrintStatistics) {
    string s = filter_chain->PrintStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_F(FilterChainTest, PrintTrace) {
    string s = filter_chain->PrintTrace();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_F(FilterChainTest, PrintProfile) {
    string s = filter_chain->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

}
}
