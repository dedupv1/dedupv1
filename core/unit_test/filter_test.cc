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
#include <test/filter_test.h>
#include <base/index.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <test/json_test_util.h>

#include <json/json.h>

#include <string>
#include <vector>

using std::string;
using std::vector;
using dedupv1::base::strutil::Split;
using dedupv1::testing::IsJson;
using testing::Return;

LOGGER("FilterTest");

namespace dedupv1 {
namespace filter {

void FilterTest::SetUp() {
    config = GetParam();

    EXPECT_CALL(system_, chunk_index()).WillRepeatedly(Return(&chunk_index_));
    EXPECT_CALL(system_, block_index()).WillRepeatedly(Return(&block_index_));

    filter = CreateFilter(config);
    ASSERT_TRUE(filter) << "Failed to create filter";
}

void FilterTest::TearDown() {
    if (filter) {
        ASSERT_TRUE(filter->Close());
    }
}

Filter* FilterTest::CreateFilter(string config_option) {
    vector<string> options;
    CHECK_RETURN(Split(config_option, ";", &options), NULL, "Failed to split: " << config_option);

    Filter* filter = Filter::Factory().Create(options[0]);
    CHECK_RETURN(filter, NULL, "Failed to create filter type: " << options[0]);

    for (size_t i = 1; i < options.size(); i++) {
        string option_name;
        string option;
        CHECK_RETURN(Split(options[i], "=", &option_name, &option), NULL, "Failed to split " << options[i]);
        CHECK_RETURN(filter->SetOption(option_name, option), NULL, "Failed set option: " << options[i]);
    }
    return filter;
}

TEST_P(FilterTest, Create) {
}

TEST_P(FilterTest, CreateDisabled) {
    if (filter) {
        ASSERT_TRUE(filter->Close());
    }
    filter = CreateFilter(config + ";enabled=false");
    ASSERT_TRUE(filter) << "Failed to create filter";

    ASSERT_TRUE(filter->Start(&system_));
    ASSERT_FALSE(filter->is_enabled_by_default());
}

TEST_P(FilterTest, Start) {
    ASSERT_TRUE(filter->Start(&system_));
}

TEST_P(FilterTest, PrintLockStatistics) {
    ASSERT_TRUE(filter->Start(&system_));

    string s = filter->PrintLockStatistics();
    ASSERT_TRUE(IsJson(s));
}

TEST_P(FilterTest, PrintStatistics) {
    ASSERT_TRUE(filter->Start(&system_));

    string s = filter->PrintStatistics();
    ASSERT_TRUE(IsJson(s));
}

TEST_P(FilterTest, PrintTrace) {
    ASSERT_TRUE(filter->Start(&system_));

    string s = filter->PrintTrace();
    ASSERT_TRUE(IsJson(s));
}

TEST_P(FilterTest, PrintProfile) {
    ASSERT_TRUE(filter->Start(&system_));

    string s = filter->PrintLockStatistics();
    ASSERT_TRUE(IsJson(s));
}

}
}

