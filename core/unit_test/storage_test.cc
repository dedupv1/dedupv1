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
#include "storage_test.h"

#include <core/storage.h>
#include <base/strutil.h>
#include <base/logging.h>

#include <json/json.h>

#include <string>
#include <vector>

using std::string;
using std::vector;
using dedupv1::chunkstore::Storage;
using dedupv1::base::strutil::Split;

LOGGER("StorageTest");

void StorageTest::SetUp() {
    config = GetParam();

    storage = CreateStorage(config);
    ASSERT_TRUE(storage) << "Failed to create storage";
}

void StorageTest::TearDown() {
    if (storage) {
        delete storage;
    }
}

Storage* StorageTest::CreateStorage(string config_option) {
    vector<string> options;
    CHECK_RETURN(Split(config_option, ";", &options), NULL, "Failed to split: " << config_option);

    Storage* storage = Storage::Factory().Create(options[0]);
    CHECK_RETURN(storage, NULL, "Failed to create storage type: " << options[0]);

    for (size_t i = 1; i < options.size(); i++) {
        string option_name;
        string option;
        CHECK_RETURN(Split(options[i], "=", &option_name, &option), NULL, "Failed to split " << options[i]);
        CHECK_RETURN(storage->SetOption(option_name, option), NULL, "Failed set option: " << options[i]);
    }
    return storage;
}

TEST_P(StorageTest, Create) {
}

TEST_P(StorageTest, PrintLockStatistics) {
    string s = storage->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_P(StorageTest, PrintStatistics) {
    string s = storage->PrintStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}

TEST_P(StorageTest, PrintProfile) {
    string s = storage->PrintLockStatistics();
    ASSERT_TRUE(s.size() > 0);
    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( s, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages();
}
