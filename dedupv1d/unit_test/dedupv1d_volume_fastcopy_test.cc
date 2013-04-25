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

#include <json/json.h>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <core/dedup_volume.h>
#include <core/dedup_volume_info.h>
#include <base/strutil.h>
#include <core/dedup_system.h>
#include <base/logging.h>
#include <test_util/log_assert.h>
#include <core/idle_detector.h>

#include "dedupv1d.h"
#include "dedupv1d_volume.h"
#include "dedupv1d_volume_info.h"
#include "dedupv1d_user_info.h"
#include "dedupv1d_volume_detacher.h"
#include "scst_handle.h"
#include "command_handler.h"

#include <test/log_mock.h>
#include <test/dedup_system_mock.h>
#include <test/block_index_mock.h>
#include <test/storage_mock.h>
#include <test/log_mock.h>
#include <test/content_storage_mock.h>
#include <test/session_mock.h>

#include "dedupv1d.pb.h"

using std::vector;
using std::list;
using std::pair;
using std::string;
using std::make_pair;
using testing::Return;
using testing::_;
using dedupv1::DedupVolumeInfo;
using dedupv1::log::EVENT_TYPE_VOLUME_ATTACH;
using dedupv1::log::EVENT_TYPE_VOLUME_DETACH;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::ToString;
using dedupv1::base::DELETE_NOT_FOUND;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::PUT_OK;
using dedupv1::IdleDetector;

LOGGER("Dedupv1dVolumeFastCopyTest");

namespace dedupv1d {

class Dedupv1dVolumeFastCopyTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    MockLog log;
    MockDedupSystem dedup_system;
    MockBlockIndex block_index;
    MockStorage storage;
    MockContentStorage content_storage;
    MockSession session;

    IdleDetector idle_detector;
    dedupv1::MemoryInfoStore info_store;

    Dedupv1dVolumeInfo* volume_info;
    DedupVolumeInfo* base_volume_info;
    Dedupv1dGroupInfo* group_info;
    Dedupv1dTargetInfo* target_info;
    Dedupv1dUserInfo* user_info;

    Dedupv1dVolumeFastCopy* fast_copy;

    virtual void SetUp() {
        EXPECT_CALL(dedup_system, storage()).WillRepeatedly(Return(&storage));
        EXPECT_CALL(dedup_system, idle_detector()).WillRepeatedly(Return(&idle_detector));
        EXPECT_CALL(dedup_system, block_index()).WillRepeatedly(Return(&block_index));
        EXPECT_CALL(dedup_system, block_size()).WillRepeatedly(Return(64 * 1024));
        EXPECT_CALL(dedup_system, info_store()).WillRepeatedly(Return(&info_store));
        EXPECT_CALL(dedup_system, log()).WillRepeatedly(Return(&log));
        EXPECT_CALL(dedup_system, content_storage()).WillRepeatedly(Return(&content_storage));
        EXPECT_CALL(dedup_system, FastCopy(_,_,_,_,_,_)).WillRepeatedly(Return(dedupv1::scsi::ScsiResult::kOk));

        base_volume_info = new DedupVolumeInfo();
        ASSERT_TRUE(base_volume_info);
        ASSERT_TRUE(base_volume_info->Start(&dedup_system));
        EXPECT_CALL(dedup_system, volume_info()).WillRepeatedly(Return(base_volume_info));

        user_info = new Dedupv1dUserInfo();
        ASSERT_TRUE(user_info);

        group_info = new Dedupv1dGroupInfo();
        ASSERT_TRUE(group_info);

        volume_info = new Dedupv1dVolumeInfo();
        ASSERT_TRUE(volume_info);

        target_info = new Dedupv1dTargetInfo();
        ASSERT_TRUE(target_info);

        SetGroupInfoOptions(group_info);
        ASSERT_TRUE(group_info->Start(dedupv1::StartContext()));
        SetTargetInfoOptions(target_info);
        ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));
        SetDefaultOptions(volume_info);
        ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));
        ASSERT_TRUE(volume_info->Run());

        EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

        list <pair <string, string> > options;
        options.push_back( make_pair("id", "0"));
        options.push_back( make_pair("logical-size", "1G"));
        options.push_back( make_pair("maintenance", "true"));
        ASSERT_TRUE(volume_info->AttachVolume(options));

        options.clear();
        options.push_back( make_pair("id", "1"));
        options.push_back( make_pair("logical-size", "1G"));
        options.push_back( make_pair("maintenance", "true"));
        ASSERT_TRUE(volume_info->AttachVolume(options));

        options.clear();
        options.push_back( make_pair("id", "2"));
        options.push_back( make_pair("logical-size", "500M"));
        options.push_back( make_pair("maintenance", "true"));
        ASSERT_TRUE(volume_info->AttachVolume(options));

        fast_copy = volume_info->fast_copy();
        ASSERT_TRUE(fast_copy);
    }

    virtual void TearDown() {
        if (volume_info) {
            delete volume_info;
            volume_info = NULL;
            fast_copy = NULL;
        }
        if (target_info) {
            delete target_info;
            target_info = NULL;
        }
        if (group_info) {
            delete group_info;
            group_info = NULL;
        }
        if (base_volume_info) {
            delete base_volume_info;
            base_volume_info = NULL;
        }
        if (user_info) {
            delete user_info;
            user_info = NULL;
        }
    }

    void SetDefaultOptions(Dedupv1dVolumeInfo* vi) {
        ASSERT_TRUE(vi->SetOption("type", "sqlite-disk-btree"));
        ASSERT_TRUE(vi->SetOption("filename", "work/dedupv1_volume_info"));
        ASSERT_TRUE(vi->SetOption("max-item-count", "64K"));
    }

    void SetTargetInfoOptions(Dedupv1dTargetInfo* ti) {
        ASSERT_TRUE(ti->SetOption("type", "sqlite-disk-btree"));
        ASSERT_TRUE(ti->SetOption("filename", "work/dedupv1_target_info"));
        ASSERT_TRUE(ti->SetOption("max-item-count", "64K"));
        ASSERT_TRUE(ti->SetOption("target", "2"));
        ASSERT_TRUE(ti->SetOption("target.name", "iqn.2010.05.example"));
        ASSERT_TRUE(ti->SetOption("target", "3"));
        ASSERT_TRUE(ti->SetOption("target.name", "a"));
        ASSERT_TRUE(ti->SetOption("target", "4"));
        ASSERT_TRUE(ti->SetOption("target.name", "b"));
        ASSERT_TRUE(ti->SetOption("target", "5"));
        ASSERT_TRUE(ti->SetOption("target.name", "c"));
    }

    void SetGroupInfoOptions(Dedupv1dGroupInfo* gi) {
        ASSERT_TRUE(gi->SetOption("type", "sqlite-disk-btree"));
        ASSERT_TRUE(gi->SetOption("filename", "work/dedupv1_group_info"));
        ASSERT_TRUE(gi->SetOption("max-item-count", "64K"));
        ASSERT_TRUE(gi->SetOption("group", "Default"));
        ASSERT_TRUE(gi->SetOption("group", "a"));
        ASSERT_TRUE(gi->SetOption("group", "b"));
        ASSERT_TRUE(gi->SetOption("group", "c"));
    }

    void Restart() {
        delete volume_info;
        volume_info = NULL;
        fast_copy = NULL;

        delete base_volume_info;
        base_volume_info = new DedupVolumeInfo();
        ASSERT_TRUE(base_volume_info);
        ASSERT_TRUE(base_volume_info->Start(&dedup_system));
        EXPECT_CALL(dedup_system, volume_info()).WillOnce(Return(base_volume_info));

        delete group_info;
        delete target_info;
        delete user_info;
        // restart

        dedupv1::StartContext start_context(dedupv1::StartContext::NON_CREATE);
        group_info = new Dedupv1dGroupInfo();
        ASSERT_TRUE(group_info);
        SetGroupInfoOptions(group_info);
        ASSERT_TRUE(group_info->Start(start_context));

        volume_info = new Dedupv1dVolumeInfo();
        SetDefaultOptions(volume_info);

        user_info = new Dedupv1dUserInfo();
        ASSERT_TRUE(user_info);

        target_info = new Dedupv1dTargetInfo();
        ASSERT_TRUE(target_info);
        SetTargetInfoOptions(target_info);
        ASSERT_TRUE(target_info->Start(start_context, volume_info, user_info));

        ASSERT_TRUE(volume_info->Start(start_context, group_info, target_info, &dedup_system));
        ASSERT_TRUE(volume_info->Run());

        fast_copy = volume_info->fast_copy();
        ASSERT_TRUE(fast_copy);
    }
};

TEST_F(Dedupv1dVolumeFastCopyTest, Create) {
    // no nothing
}

TEST_F(Dedupv1dVolumeFastCopyTest, SimpleFastCopy) {
    ASSERT_TRUE(volume_info->FastCopy(1, 2, 0, 0, 1024 * 1024 * 16));

    sleep(5);
}

TEST_F(Dedupv1dVolumeFastCopyTest, Restart) {
    ASSERT_TRUE(volume_info->FastCopy(1, 2, 0, 0, 1024 * 1024 * 16));

    sleep(1);

    Restart();

    sleep(5);
}

}
