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

#include <list>
#include <map>
#include <string>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <base/strutil.h>
#include <core/dedup_system.h>
#include <base/option.h>
#include <test_util/log_assert.h>

#include "dedupv1d.h"
#include "dedupv1d_group.h"
#include "dedupv1d_group_info.h"

#include <test/log_mock.h>
#include <test/dedup_system_mock.h>
#include <test/block_index_mock.h>
#include <test/storage_mock.h>

#include "dedupv1d.pb.h"

using std::vector;
using std::list;
using std::pair;
using std::string;
using testing::Return;
using testing::_;
using dedupv1::DedupVolumeInfo;
using dedupv1::log::EVENT_TYPE_VOLUME_ATTACH;
using dedupv1::log::EVENT_TYPE_VOLUME_DETACH;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::DELETE_NOT_FOUND;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::PUT_OK;
using dedupv1::base::Option;

namespace dedupv1d {

class Dedupv1dGroupInfoTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    static const std::string kDefaultGroupName;

    Dedupv1dGroupInfo* group_info;

    virtual void SetUp() {
        group_info = new Dedupv1dGroupInfo();
        ASSERT_TRUE(group_info);
    }

    virtual void TearDown() {
        if (group_info) {
            delete group_info;
            group_info = NULL;
        }
    }

    void SetDefaultOptions(Dedupv1dGroupInfo* ti) {
        ASSERT_TRUE(ti->SetOption("type", "sqlite-disk-btree"));
        ASSERT_TRUE(ti->SetOption("filename", "work/dedupv1_group_info"));
        ASSERT_TRUE(ti->SetOption("max-item-count", "64K"));
    }

    void Restart() {
        delete group_info;

        // restart
        group_info = new Dedupv1dGroupInfo();
        SetDefaultOptions(group_info);
        ASSERT_TRUE(group_info->Start(dedupv1::StartContext()));
    }
};

const std::string Dedupv1dGroupInfoTest::kDefaultGroupName = "backup";

TEST_F(Dedupv1dGroupInfoTest, Create) {
    // no nothing
}

TEST_F(Dedupv1dGroupInfoTest, StartWithoutOptions) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_FALSE(group_info->Start(dedupv1::StartContext()));
}

TEST_F(Dedupv1dGroupInfoTest, StartWithDefaultOptions) {
    SetDefaultOptions(group_info);
    ASSERT_TRUE(group_info->Start(dedupv1::StartContext()));
    ASSERT_EQ(group_info->GetGroupNames().value().size(), 0U);
}

TEST_F(Dedupv1dGroupInfoTest, StartWithOneGroup) {

    SetDefaultOptions(group_info);
    ASSERT_TRUE(group_info->SetOption("group", Dedupv1dGroupInfoTest::kDefaultGroupName));

    ASSERT_TRUE(group_info->Start(dedupv1::StartContext()));
    ASSERT_EQ(group_info->GetGroupNames().value().size(), 1U);
    Option<Dedupv1dGroup> group = group_info->FindGroup(Dedupv1dGroupInfoTest::kDefaultGroupName);
    ASSERT_TRUE(group.valid());
    ASSERT_EQ(group.value().name(), Dedupv1dGroupInfoTest::kDefaultGroupName);
}

TEST_F(Dedupv1dGroupInfoTest, AddGroup) {
    SetDefaultOptions(group_info);
    ASSERT_TRUE(group_info->Start(dedupv1::StartContext()));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("name", Dedupv1dGroupInfoTest::kDefaultGroupName));
    ASSERT_TRUE(group_info->AddGroup(options));

    Option<Dedupv1dGroup> group = group_info->FindGroup(Dedupv1dGroupInfoTest::kDefaultGroupName);
    ASSERT_TRUE(group.valid());
    ASSERT_EQ(group.value().name(), Dedupv1dGroupInfoTest::kDefaultGroupName);
}

TEST_F(Dedupv1dGroupInfoTest, AddGroupWithRestart) {
    SetDefaultOptions(group_info);
    ASSERT_TRUE(group_info->Start(dedupv1::StartContext()));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("name", Dedupv1dGroupInfoTest::kDefaultGroupName));
    ASSERT_TRUE(group_info->AddGroup(options));

    Restart();

    Option<Dedupv1dGroup> group = group_info->FindGroup(Dedupv1dGroupInfoTest::kDefaultGroupName);
    ASSERT_TRUE(group.valid());
    ASSERT_EQ(group.value().name(), Dedupv1dGroupInfoTest::kDefaultGroupName);
}

TEST_F(Dedupv1dGroupInfoTest, RemoveGroup) {
    SetDefaultOptions(group_info);
    ASSERT_TRUE(group_info->Start(dedupv1::StartContext()));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("name", Dedupv1dGroupInfoTest::kDefaultGroupName));
    ASSERT_TRUE(group_info->AddGroup(options));

    Restart();

    ASSERT_TRUE(group_info->RemoveGroup(Dedupv1dGroupInfoTest::kDefaultGroupName));

    ASSERT_FALSE(group_info->FindGroup(Dedupv1dGroupInfoTest::kDefaultGroupName).valid()) << "Shouldn't find group after detach";

    Restart();

    ASSERT_FALSE(group_info->FindGroup(Dedupv1dGroupInfoTest::kDefaultGroupName).valid()) << "Shouldn't find group after detach. Removing should be persistent";
}

TEST_F(Dedupv1dGroupInfoTest, RemoveGroupPreconfigured) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    SetDefaultOptions(group_info);
    ASSERT_TRUE(group_info->SetOption("group", Dedupv1dGroupInfoTest::kDefaultGroupName));
    ASSERT_TRUE(group_info->Start(dedupv1::StartContext()));

    ASSERT_FALSE(group_info->RemoveGroup(Dedupv1dGroupInfoTest::kDefaultGroupName)) << "A preconfigured group cannot be removed";
    ASSERT_TRUE(group_info->FindGroup(Dedupv1dGroupInfoTest::kDefaultGroupName).valid());
}

TEST_F(Dedupv1dGroupInfoTest, PreconfigureDoubleName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(group_info);
    ASSERT_TRUE(group_info->SetOption("group", Dedupv1dGroupInfoTest::kDefaultGroupName));
    ASSERT_TRUE(group_info->SetOption("group", Dedupv1dGroupInfoTest::kDefaultGroupName));
    ASSERT_FALSE(group_info->Start(dedupv1::StartContext())) << "Should not start with double group name";
}

TEST_F(Dedupv1dGroupInfoTest, AddGroupDoubleName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(group_info);
    ASSERT_TRUE(group_info->SetOption("group", Dedupv1dGroupInfoTest::kDefaultGroupName));
    ASSERT_TRUE(group_info->Start(dedupv1::StartContext()));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("name", Dedupv1dGroupInfoTest::kDefaultGroupName));
    ASSERT_FALSE(group_info->AddGroup(options)) << "Adding a group with an already used name should fail";
}

}
