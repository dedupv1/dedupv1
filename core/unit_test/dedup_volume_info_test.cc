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
#include <gmock/gmock.h>

#include <core/dedup.h>
#include <base/locks.h>

#include <core/log_consumer.h>
#include <core/dedup_system.h>
#include <core/dedup_volume.h>
#include <core/dedup_volume_info.h>
#include <base/strutil.h>

#include <test_util/log_assert.h>
#include <test/dedup_system_mock.h>
#include <test/content_storage_mock.h>
#include <test/session_mock.h>
#include <test/log_mock.h>

using testing::_;
using testing::Return;
using dedupv1::base::strutil::ToString;
using dedupv1::log::EVENT_TYPE_VOLUME_ATTACH;
using dedupv1::log::EVENT_TYPE_VOLUME_DETACH;
using dedupv1::base::make_option;

namespace dedupv1 {

class DedupVolumeInfoTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    MockLog log;
    MockDedupSystem dedup_system;
    MockContentStorage content_storage;
    MockSession session;

    DedupVolumeInfo* volume_info;

    DedupVolume volumes[4];

    virtual void SetUp() {
        volume_info = new DedupVolumeInfo();

        EXPECT_CALL(dedup_system, content_storage()).WillRepeatedly(Return(&content_storage));
        EXPECT_CALL(dedup_system, log()).WillRepeatedly(Return(&log));
        std::list<dedupv1::filter::Filter*> filter_list;
        EXPECT_CALL(content_storage, GetFilterList(_)).WillRepeatedly(Return(make_option(filter_list)));

        for (int i = 0; i < 4; i++) {
            ASSERT_TRUE(volumes[i].SetOption("id", ToString(i + 4)));
            ASSERT_TRUE(volumes[i].SetOption("logical-size", "1G"));
            ASSERT_TRUE(volumes[i].Start(&dedup_system, false));
        }
    }

    virtual void TearDown() {
        if (volume_info) {
            ASSERT_TRUE(volume_info->Close());
            volume_info = NULL;
        }
        for (int i = 0; i < 4; i++) {
            ASSERT_TRUE(volumes[i].Close());
        }
    }
};

TEST_F(DedupVolumeInfoTest, Create) {
    // do nothing
}

TEST_F(DedupVolumeInfoTest, Start) {
    ASSERT_TRUE(volume_info->Start(&dedup_system));
}

TEST_F(DedupVolumeInfoTest, StartWithoutLog) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_FALSE(volume_info->Start(NULL));
}

TEST_F(DedupVolumeInfoTest, Register) {
    ASSERT_TRUE(volume_info->Start(&dedup_system));
    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(volume_info->RegisterVolume(&volumes[i]));
        ASSERT_EQ(volume_info->GetVolumeCount(), 1U + i);
        for (int j = 0; j <= i; j++) {
            ASSERT_EQ(volume_info->FindVolume(j + 4), &volumes[j]);
        }
    }

    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(volume_info->UnregisterVolume(&volumes[i]));
        ASSERT_EQ(volume_info->GetVolumeCount(), 3U - i);
        for (int j = 0; j <= i; j++) {
            ASSERT_TRUE(volume_info->FindVolume(j + 4) == NULL);
        }
        for (int j = i + 1; j < 4; j++) {
            ASSERT_EQ(volume_info->FindVolume(j + 4), &volumes[j]);
        }
    }
}

TEST_F(DedupVolumeInfoTest, Attach) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH, _, _, _, _)).Times(4).WillRepeatedly(Return(true));

    ASSERT_TRUE(volume_info->Start(&dedup_system));

    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(volume_info->AttachVolume(&volumes[i]));
        ASSERT_EQ(volume_info->GetVolumeCount(), 1U + i);
        for (int j = 0; j <= i; j++) {
            ASSERT_EQ(volume_info->FindVolume(j + 4), &volumes[j]);
        }
    }

    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(volume_info->UnregisterVolume(&volumes[i]));
    }
}

TEST_F(DedupVolumeInfoTest, Detach) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_DETACH, _, _, _, _)).Times(4).WillRepeatedly(Return(true));

    ASSERT_TRUE(volume_info->Start(&dedup_system));

    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(volume_info->RegisterVolume(&volumes[i]));
    }

    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(volume_info->DetachVolume(&volumes[i]));
        ASSERT_EQ(volume_info->GetVolumeCount(), 3U - i);
        for (int j = 0; j <= i; j++) {
            ASSERT_TRUE(volume_info->FindVolume(j + 4) == NULL);
        }
        for (int j = i + 1; j < 4; j++) {
            ASSERT_EQ(volume_info->FindVolume(j + 4), &volumes[j]);
        }
    }
}

}
