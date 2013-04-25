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
#include "dedupv1d_target.h"
#include "dedupv1d_target_info.h"
#include "dedupv1d_volume_info.h"
#include "dedupv1d_user_info.h"

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

class Dedupv1dTargetInfoTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    static const std::string kDefaultTargetName;

    Dedupv1dTargetInfo* target_info;
    Dedupv1dVolumeInfo* volume_info;
    Dedupv1dUserInfo* user_info;

    virtual void SetUp() {
        target_info = new Dedupv1dTargetInfo();
        ASSERT_TRUE(target_info);

        volume_info = new Dedupv1dVolumeInfo();
        ASSERT_TRUE(volume_info);

        user_info = new Dedupv1dUserInfo();
        ASSERT_TRUE(user_info);

        SetDefaultOptions(user_info);
    }

    virtual void TearDown() {
        if (target_info) {
            delete target_info;
            target_info = NULL;
        }
        if (volume_info) {
            delete volume_info;
            volume_info = NULL;
        }
        if (user_info) {
            delete user_info;
            user_info = NULL;
        }
    }

    void SetDefaultOptions(Dedupv1dTargetInfo* ti) {
        ASSERT_TRUE(ti->SetOption("type", "sqlite-disk-btree"));
        ASSERT_TRUE(ti->SetOption("filename", "work/dedupv1_target_info"));
        ASSERT_TRUE(ti->SetOption("max-item-count", "64K"));
    }

    void SetDefaultOptions(Dedupv1dUserInfo* ui) {
        ASSERT_TRUE(ui->SetOption("type", "sqlite-disk-btree"));
        ASSERT_TRUE(ui->SetOption("filename", "work/dedupv1_user_info"));
        ASSERT_TRUE(ui->SetOption("max-item-count", "64K"));
    }

    void Restart() {
        delete target_info;
        delete user_info;
        delete volume_info;

        // restart
        volume_info = new Dedupv1dVolumeInfo();
        ASSERT_TRUE(volume_info);

        user_info = new Dedupv1dUserInfo();
        ASSERT_TRUE(user_info);

        target_info = new Dedupv1dTargetInfo();
        ASSERT_TRUE(target_info);

        SetDefaultOptions(user_info);
        SetDefaultOptions(target_info);
        ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));
        ASSERT_TRUE(user_info->Start(dedupv1::StartContext()));
    }
};

const std::string Dedupv1dTargetInfoTest::kDefaultTargetName = "iqn.2010.05:example";

TEST_F(Dedupv1dTargetInfoTest, Create) {
    // no nothing
}

TEST_F(Dedupv1dTargetInfoTest, StartWithoutOptions) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_FALSE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));
}

TEST_F(Dedupv1dTargetInfoTest, StartWithDefaultOptions) {
    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));
    ASSERT_EQ(target_info->GetTargets().value().size(), 0U);
}

TEST_F(Dedupv1dTargetInfoTest, StartWithOneTarget) {

    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->SetOption("target", "2"));
    ASSERT_TRUE(target_info->SetOption("target.name", Dedupv1dTargetInfoTest::kDefaultTargetName));

    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));
    ASSERT_EQ(target_info->GetTargets().value().size(), 1U);
    Option<Dedupv1dTarget> target = target_info->FindTargetByName(Dedupv1dTargetInfoTest::kDefaultTargetName);
    ASSERT_TRUE(target.valid());
    ASSERT_EQ(target.value().name(), Dedupv1dTargetInfoTest::kDefaultTargetName);
}

TEST_F(Dedupv1dTargetInfoTest, AddTarget) {
    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("tid", "2"));
    options.push_back( pair<string, string>("name", Dedupv1dTargetInfoTest::kDefaultTargetName));
    ASSERT_TRUE(target_info->AddTarget(options));

    Option<Dedupv1dTarget> target = target_info->FindTargetByName(Dedupv1dTargetInfoTest::kDefaultTargetName);
    ASSERT_TRUE(target.valid());
    ASSERT_EQ(target.value().name(), Dedupv1dTargetInfoTest::kDefaultTargetName);
}

TEST_F(Dedupv1dTargetInfoTest, ChangeName) {
    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));
    ASSERT_TRUE(user_info->Start(dedupv1::StartContext()));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("tid", "2"));
    options.push_back( pair<string, string>("name", Dedupv1dTargetInfoTest::kDefaultTargetName));
    ASSERT_TRUE(target_info->AddTarget(options));

    Option<Dedupv1dTarget> target = target_info->FindTargetByName(Dedupv1dTargetInfoTest::kDefaultTargetName);
    ASSERT_TRUE(target.valid());
    ASSERT_EQ(target.value().name(), Dedupv1dTargetInfoTest::kDefaultTargetName);

    options.clear();
    options.push_back( pair<string, string>("name", Dedupv1dTargetInfoTest::kDefaultTargetName + "2"));
    ASSERT_TRUE(target_info->ChangeTargetParams(2, options));

    target = target_info->FindTargetByName(Dedupv1dTargetInfoTest::kDefaultTargetName);
    ASSERT_FALSE(target.valid());

    target = target_info->FindTargetByName(Dedupv1dTargetInfoTest::kDefaultTargetName + "2");
    ASSERT_TRUE(target.valid());
}

TEST_F(Dedupv1dTargetInfoTest, ChangeNameDoubleName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));
    ASSERT_TRUE(user_info->Start(dedupv1::StartContext()));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("tid", "2"));
    options.push_back( pair<string, string>("name", Dedupv1dTargetInfoTest::kDefaultTargetName));
    ASSERT_TRUE(target_info->AddTarget(options));

    options.clear();
    options.push_back( pair<string, string>("tid", "3"));
    options.push_back( pair<string, string>("name", Dedupv1dTargetInfoTest::kDefaultTargetName + "2"));
    ASSERT_TRUE(target_info->AddTarget(options));

    Option<Dedupv1dTarget> target = target_info->FindTargetByName(Dedupv1dTargetInfoTest::kDefaultTargetName);
    ASSERT_TRUE(target.valid());
    ASSERT_EQ(target.value().name(), Dedupv1dTargetInfoTest::kDefaultTargetName);

    options.clear();
    options.push_back( pair<string, string>("name", Dedupv1dTargetInfoTest::kDefaultTargetName + "2"));
    ASSERT_FALSE(target_info->ChangeTargetParams(2, options));
}

TEST_F(Dedupv1dTargetInfoTest, AddTargetWithRestart) {
    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("tid", "2"));
    options.push_back( pair<string, string>("name", Dedupv1dTargetInfoTest::kDefaultTargetName));
    ASSERT_TRUE(target_info->AddTarget(options));

    Restart();

    Option<Dedupv1dTarget> target = target_info->FindTargetByName(Dedupv1dTargetInfoTest::kDefaultTargetName);
    ASSERT_TRUE(target.valid());
    ASSERT_EQ(target.value().name(), Dedupv1dTargetInfoTest::kDefaultTargetName);
}

TEST_F(Dedupv1dTargetInfoTest, IllegalTargetName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("tid", "2"));
    options.push_back( pair<string, string>("name", "iqn.2010-05:info.christmann:example,hello"));
    ASSERT_FALSE(target_info->AddTarget(options));

    options.clear();
    options.push_back( pair<string, string>("tid", "2"));
    options.push_back( pair<string, string>("name", "iqn.2010-05:info.christmann:example.Hello"));
    ASSERT_FALSE(target_info->AddTarget(options));
}

TEST_F(Dedupv1dTargetInfoTest, RemoveTarget) {
    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("tid", "2"));
    options.push_back( pair<string, string>("name", Dedupv1dTargetInfoTest::kDefaultTargetName));
    ASSERT_TRUE(target_info->AddTarget(options));

    Restart();

    ASSERT_TRUE(target_info->RemoveTarget(2));

    ASSERT_FALSE(target_info->FindTargetByName(Dedupv1dTargetInfoTest::kDefaultTargetName).valid()) << "Shouldn't find target after detach";

    Restart();

    ASSERT_FALSE(target_info->FindTargetByName(Dedupv1dTargetInfoTest::kDefaultTargetName).valid()) << "Shouldn't find target after detach. Removing should be persistent";
}

TEST_F(Dedupv1dTargetInfoTest, RemoveTargetPreconfigured) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->SetOption("target", "2"));
    ASSERT_TRUE(target_info->SetOption("target.name", Dedupv1dTargetInfoTest::kDefaultTargetName));
    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));

    ASSERT_FALSE(target_info->RemoveTarget(2)) << "A preconfigured target cannot be removed";
    ASSERT_TRUE(target_info->FindTargetByName(Dedupv1dTargetInfoTest::kDefaultTargetName).valid());
}

TEST_F(Dedupv1dTargetInfoTest, PreconfigureDoubleID) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->SetOption("target", "2"));
    ASSERT_TRUE(target_info->SetOption("target.name", Dedupv1dTargetInfoTest::kDefaultTargetName));
    ASSERT_TRUE(target_info->SetOption("target", "2"));
    ASSERT_TRUE(target_info->SetOption("target.name", Dedupv1dTargetInfoTest::kDefaultTargetName + "_2"));
    ASSERT_FALSE(target_info->Start(dedupv1::StartContext(), volume_info, user_info)) << "Should not start with double target id";
}

TEST_F(Dedupv1dTargetInfoTest, PreconfigureDoubleName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->SetOption("target", "2"));
    ASSERT_TRUE(target_info->SetOption("target.name", Dedupv1dTargetInfoTest::kDefaultTargetName));
    ASSERT_TRUE(target_info->SetOption("target", "3"));
    ASSERT_TRUE(target_info->SetOption("target.name", Dedupv1dTargetInfoTest::kDefaultTargetName));
    ASSERT_FALSE(target_info->Start(dedupv1::StartContext(), volume_info, user_info)) << "Should not start with double target name";
}

TEST_F(Dedupv1dTargetInfoTest, AddTargetDoubleName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(target_info);
    ASSERT_TRUE(target_info->SetOption("target", "2"));
    ASSERT_TRUE(target_info->SetOption("target.name", Dedupv1dTargetInfoTest::kDefaultTargetName));
    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("tid", "3"));
    options.push_back( pair<string, string>("name", Dedupv1dTargetInfoTest::kDefaultTargetName));
    ASSERT_FALSE(target_info->AddTarget(options)) << "Adding a volume with an already used name should fail";
}

}
