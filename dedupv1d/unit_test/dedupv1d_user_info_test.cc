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
#include "dedupv1d_user.h"
#include "dedupv1d_user_info.h"
#include "dedupv1d_target_info.h"
#include "dedupv1d_volume_info.h"

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

class Dedupv1dUserInfoTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    static const std::string kDefaultUserName;
    static const std::string kDefaultUserSecret;

    Dedupv1dUserInfo* user_info;
    Dedupv1dTargetInfo* target_info;
    Dedupv1dVolumeInfo* volume_info;

    virtual void SetUp() {
        user_info = new Dedupv1dUserInfo();
        ASSERT_TRUE(user_info);

        volume_info = new Dedupv1dVolumeInfo();
        ASSERT_TRUE(volume_info);
        target_info = new Dedupv1dTargetInfo();
        ASSERT_TRUE(target_info);
        SetTargetInfoOptions(target_info);
        ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));
    }

    void SetTargetInfoOptions(Dedupv1dTargetInfo* ti) {
        ASSERT_TRUE(ti->SetOption("type", "sqlite-disk-btree"));
        ASSERT_TRUE(ti->SetOption("filename", "work/dedupv1_target_info"));
        ASSERT_TRUE(ti->SetOption("max-item-count", "64K"));
        ASSERT_TRUE(ti->SetOption("target", "1"));
        ASSERT_TRUE(ti->SetOption("target.name", "iqn.2005-10.de.jgu:example"));
    }

    virtual void TearDown() {
        if (user_info) {
            delete user_info;
            user_info = NULL;
        }
        if (target_info) {
            delete target_info;
            target_info = NULL;
        }
        if (volume_info) {
            delete volume_info;
            volume_info = NULL;
        }

    }

    void SetDefaultOptions(Dedupv1dUserInfo* ti) {
        ASSERT_TRUE(ti->SetOption("type", "sqlite-disk-btree"));
        ASSERT_TRUE(ti->SetOption("filename", "work/dedupv1_user_info"));
        ASSERT_TRUE(ti->SetOption("max-item-count", "64K"));
    }

    void Restart() {
        delete volume_info;
        delete target_info;
        delete user_info;

        dedupv1::StartContext start_context(dedupv1::StartContext::NON_CREATE);

        volume_info = new Dedupv1dVolumeInfo();
        ASSERT_TRUE(volume_info);
        target_info = new Dedupv1dTargetInfo();
        ASSERT_TRUE(target_info);
        SetTargetInfoOptions(target_info);
        user_info = new Dedupv1dUserInfo();
        SetDefaultOptions(user_info);

        ASSERT_TRUE(target_info->Start(start_context, volume_info, user_info));
        ASSERT_TRUE(user_info->Start(start_context));
    }
};

const std::string Dedupv1dUserInfoTest::kDefaultUserName = "admin1";
const std::string Dedupv1dUserInfoTest::kDefaultUserSecret =
    Dedupv1dUser::EncodePassword("admin1?admin1");

TEST_F(Dedupv1dUserInfoTest, Create) {
    // no nothing
}

TEST_F(Dedupv1dUserInfoTest, StartWithoutOptions) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_FALSE(user_info->Start(dedupv1::StartContext()));
}

TEST_F(Dedupv1dUserInfoTest, StartWithDefaultOptions) {
    SetDefaultOptions(user_info);
    ASSERT_TRUE(user_info->Start(dedupv1::StartContext()));
    ASSERT_EQ(user_info->GetUserNames().value().size(), 0U);
}

TEST_F(Dedupv1dUserInfoTest, StartWithOneUser) {
    SetDefaultOptions(user_info);
    ASSERT_TRUE(user_info->SetOption("user", Dedupv1dUserInfoTest::kDefaultUserName));
    ASSERT_TRUE(user_info->SetOption("user.secret-hash", Dedupv1dUserInfoTest::kDefaultUserSecret));

    ASSERT_TRUE(user_info->Start(dedupv1::StartContext()));
    ASSERT_EQ(user_info->GetUserNames().value().size(), 1U);
    Option<Dedupv1dUser> user = user_info->FindUser(Dedupv1dUserInfoTest::kDefaultUserName);
    ASSERT_TRUE(user.valid());
    ASSERT_EQ(user.value().name(), Dedupv1dUserInfoTest::kDefaultUserName);
}

TEST_F(Dedupv1dUserInfoTest, AddUser) {
    SetDefaultOptions(user_info);
    ASSERT_TRUE(user_info->Start(dedupv1::StartContext()));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("name", Dedupv1dUserInfoTest::kDefaultUserName));
    options.push_back( pair<string, string>("secret-hash", Dedupv1dUserInfoTest::kDefaultUserSecret));
    ASSERT_TRUE(user_info->AddUser(options));

    Option<Dedupv1dUser> user = user_info->FindUser(Dedupv1dUserInfoTest::kDefaultUserName);
    ASSERT_TRUE(user.valid());
    ASSERT_EQ(user.value().name(), Dedupv1dUserInfoTest::kDefaultUserName);
}

TEST_F(Dedupv1dUserInfoTest, ChangeUser) {
    SetDefaultOptions(user_info);
    ASSERT_TRUE(user_info->Start(dedupv1::StartContext()));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("name", Dedupv1dUserInfoTest::kDefaultUserName));
    options.push_back( pair<string, string>("secret-hash", Dedupv1dUserInfoTest::kDefaultUserSecret));
    ASSERT_TRUE(user_info->AddUser(options));

    Option<Dedupv1dUser> user = user_info->FindUser(Dedupv1dUserInfoTest::kDefaultUserName);
    ASSERT_TRUE(user.valid());
    ASSERT_EQ(user.value().name(), Dedupv1dUserInfoTest::kDefaultUserName);
    ASSERT_TRUE(user.value().secret_hash().size() > 0);
    string first_secret = user.value().secret_hash();

    options.clear();
    options.push_back( pair<string, string>("name", Dedupv1dUserInfoTest::kDefaultUserName));
    options.push_back( pair<string, string>("secret-hash", Dedupv1dUserInfoTest::kDefaultUserSecret + "2"));
    ASSERT_TRUE(user_info->ChangeUser(options));

    user = user_info->FindUser(Dedupv1dUserInfoTest::kDefaultUserName);
    ASSERT_TRUE(user.valid());
    ASSERT_EQ(user.value().name(), Dedupv1dUserInfoTest::kDefaultUserName);
    string second_secret = user.value().secret_hash();
    ASSERT_TRUE(user.value().secret_hash().size() > 0);
    ASSERT_NE(first_secret, second_secret);
}

TEST_F(Dedupv1dUserInfoTest, AddUserWithRestart) {
    SetDefaultOptions(user_info);
    ASSERT_TRUE(user_info->Start(dedupv1::StartContext()));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("name", Dedupv1dUserInfoTest::kDefaultUserName));
    options.push_back( pair<string, string>("secret-hash", Dedupv1dUserInfoTest::kDefaultUserSecret));
    ASSERT_TRUE(user_info->AddUser(options));

    Restart();

    Option<Dedupv1dUser> user = user_info->FindUser(Dedupv1dUserInfoTest::kDefaultUserName);
    ASSERT_TRUE(user.valid());
    ASSERT_EQ(user.value().name(), Dedupv1dUserInfoTest::kDefaultUserName);
}

TEST_F(Dedupv1dUserInfoTest, RemoveUser) {
    SetDefaultOptions(user_info);
    ASSERT_TRUE(user_info->Start(dedupv1::StartContext()));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("name", Dedupv1dUserInfoTest::kDefaultUserName));
    options.push_back( pair<string, string>("secret-hash", Dedupv1dUserInfoTest::kDefaultUserSecret));
    ASSERT_TRUE(user_info->AddUser(options));

    Restart();

    ASSERT_TRUE(user_info->RemoveUser(Dedupv1dUserInfoTest::kDefaultUserName));

    ASSERT_FALSE(user_info->FindUser(Dedupv1dUserInfoTest::kDefaultUserName).valid()) << "Shouldn't find user after detach";

    Restart();

    ASSERT_FALSE(user_info->FindUser(Dedupv1dUserInfoTest::kDefaultUserName).valid()) << "Shouldn't find user after detach. Removing should be persistent";
}

TEST_F(Dedupv1dUserInfoTest, RemoveUserPreconfigured) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    SetDefaultOptions(user_info);
    ASSERT_TRUE(user_info->SetOption("user", Dedupv1dUserInfoTest::kDefaultUserName));
    ASSERT_TRUE(user_info->SetOption("user.secret-hash", Dedupv1dUserInfoTest::kDefaultUserSecret));
    ASSERT_TRUE(user_info->Start(dedupv1::StartContext()));

    ASSERT_FALSE(user_info->RemoveUser(Dedupv1dUserInfoTest::kDefaultUserName)) << "A preconfigured user cannot be removed";
    ASSERT_TRUE(user_info->FindUser(Dedupv1dUserInfoTest::kDefaultUserName).valid());
}

TEST_F(Dedupv1dUserInfoTest, PreconfigureDoubleName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(user_info);
    ASSERT_TRUE(user_info->SetOption("user", Dedupv1dUserInfoTest::kDefaultUserName));
    ASSERT_TRUE(user_info->SetOption("user.secret-hash", Dedupv1dUserInfoTest::kDefaultUserSecret));
    ASSERT_TRUE(user_info->SetOption("user", Dedupv1dUserInfoTest::kDefaultUserName));
    ASSERT_TRUE(user_info->SetOption("user.secret-hash", Dedupv1dUserInfoTest::kDefaultUserSecret));
    ASSERT_FALSE(user_info->Start(dedupv1::StartContext())) << "Should not start with double user name";
}

TEST_F(Dedupv1dUserInfoTest, AddUserDoubleName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(user_info);
    ASSERT_TRUE(user_info->SetOption("user", Dedupv1dUserInfoTest::kDefaultUserName));
    ASSERT_TRUE(user_info->SetOption("user.secret-hash", Dedupv1dUserInfoTest::kDefaultUserSecret));
    ASSERT_TRUE(user_info->Start(dedupv1::StartContext()));

    list < pair<string, string> > options;
    options.push_back( pair<string, string>("name", Dedupv1dUserInfoTest::kDefaultUserName));
    options.push_back( pair<string, string>("secret-hash", Dedupv1dUserInfoTest::kDefaultUserSecret));
    ASSERT_FALSE(user_info->AddUser(options)) << "Adding a user with an already used name should fail";
}

}
