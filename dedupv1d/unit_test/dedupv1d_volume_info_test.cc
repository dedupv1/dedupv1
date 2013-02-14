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
#include <test/log_assert.h>
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
#include <test/filter_mock.h>
#include <test/filter_chain_mock.h>

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

LOGGER("Dedupv1dVolumeInfoTest");

namespace dedupv1d {

class Dedupv1dVolumeInfoTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    MockLog log;
    MockDedupSystem dedup_system;
    MockBlockIndex block_index;
    MockStorage storage;
    MockContentStorage content_storage;
    MockSession session;
    MockFilterChain filter_chain;
    MockFilter filter;

    IdleDetector idle_detector;
    dedupv1::MemoryInfoStore info_store;

    Dedupv1dVolumeInfo* volume_info;
    DedupVolumeInfo* base_volume_info;
    Dedupv1dGroupInfo* group_info;
    Dedupv1dTargetInfo* target_info;
    Dedupv1dUserInfo* user_info;

    Dedupv1dVolumeInfoTest() : filter("test", dedupv1::filter::Filter::FILTER_EXISTING) {
    }

    virtual void SetUp() {
        EXPECT_CALL(dedup_system, storage()).WillRepeatedly(Return(&storage));
        EXPECT_CALL(dedup_system, idle_detector()).WillRepeatedly(Return(&idle_detector));
        EXPECT_CALL(dedup_system, block_index()).WillRepeatedly(Return(&block_index));
        EXPECT_CALL(dedup_system, block_size()).WillRepeatedly(Return(64 * 1024));
        EXPECT_CALL(dedup_system, info_store()).WillRepeatedly(Return(&info_store));
        EXPECT_CALL(dedup_system, log()).WillRepeatedly(Return(&log));
        EXPECT_CALL(dedup_system, content_storage()).WillRepeatedly(Return(&content_storage));
        EXPECT_CALL(dedup_system, filter_chain()).WillRepeatedly(Return(&filter_chain));
        EXPECT_CALL(filter_chain, GetFilterByName(_)).WillRepeatedly(Return(&filter));
        EXPECT_CALL(content_storage, CreateSession(_, _)).WillRepeatedly(Return(&session));

        base_volume_info = new DedupVolumeInfo();
        ASSERT_TRUE(base_volume_info);
        ASSERT_TRUE(base_volume_info->Start(&dedup_system));
        EXPECT_CALL(dedup_system, volume_info()).WillRepeatedly(Return(base_volume_info));

        user_info = new Dedupv1dUserInfo();
        ASSERT_TRUE(user_info);

        group_info = new Dedupv1dGroupInfo();
        ASSERT_TRUE(group_info);
        SetGroupInfoOptions(group_info);
        ASSERT_TRUE(group_info->Start(dedupv1::StartContext()));

        volume_info = new Dedupv1dVolumeInfo();
        ASSERT_TRUE(volume_info);

        target_info = new Dedupv1dTargetInfo();
        ASSERT_TRUE(target_info);
        SetTargetInfoOptions(target_info);
        ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));
    }

    virtual void TearDown() {
        if (volume_info) {
            ASSERT_TRUE(volume_info->Close());
            volume_info = NULL;
        }
        if (target_info) {
            ASSERT_TRUE(target_info->Close());
            target_info = NULL;
        }
        if (group_info) {
            ASSERT_TRUE(group_info->Close());
            group_info = NULL;
        }
        if (base_volume_info) {
            ASSERT_TRUE(base_volume_info->Close());
            base_volume_info = NULL;
        }
        if (user_info) {
            ASSERT_TRUE(user_info->Close());
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
        ASSERT_TRUE(volume_info->Close());

        base_volume_info->Close();
        base_volume_info = new DedupVolumeInfo();
        ASSERT_TRUE(base_volume_info);
        ASSERT_TRUE(base_volume_info->Start(&dedup_system));
        EXPECT_CALL(dedup_system, volume_info()).WillOnce(Return(base_volume_info));

        ASSERT_TRUE(group_info->Close());
        ASSERT_TRUE(target_info->Close());
        ASSERT_TRUE(user_info->Close());
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
    }
};

TEST_F(Dedupv1dVolumeInfoTest, Create) {
    // no nothing
}

TEST_F(Dedupv1dVolumeInfoTest, StartWithoutOptions) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_FALSE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));
}

TEST_F(Dedupv1dVolumeInfoTest, StartWithDefaultOptions) {
    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_EQ(volume_info->GetVolumes(NULL).value().size(), 0U);
}

TEST_F(Dedupv1dVolumeInfoTest, StartWithOneVolume) {
    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id", "0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size", "1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_EQ(volume_info->GetVolumes(NULL).value().size(), 1U);
    Dedupv1dVolume* volume = volume_info->FindVolume(0, NULL);
    ASSERT_TRUE(volume);
    ASSERT_EQ(volume->id(), 0U);
    ASSERT_TRUE(volume->volume() != NULL);
    ASSERT_EQ(volume->block_size(), 512);
}

TEST_F(Dedupv1dVolumeInfoTest, PreconfiguredWithBlockSize) {
    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id", "0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size", "1G"));
    ASSERT_TRUE(volume_info->SetOption("volume.sector-size", "4096"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_EQ(volume_info->GetVolumes(NULL).value().size(), 1U);
    Dedupv1dVolume* volume = volume_info->FindVolume(0, NULL);
    ASSERT_TRUE(volume);
    ASSERT_EQ(volume->id(), 0U);
    ASSERT_TRUE(volume->volume() != NULL);
    ASSERT_EQ(volume->block_size(), 4096);
}

TEST_F(Dedupv1dVolumeInfoTest, Attach) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->FindVolume(0, NULL) != NULL);
    ASSERT_TRUE(base_volume_info->FindVolume(0) != NULL);
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->logical_size(), (uint64_t) ToStorageUnit("1G").value());
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->volume(), base_volume_info->FindVolume(0));
}

TEST_F(Dedupv1dVolumeInfoTest, SetDefaultVolumeCommandThreadCount) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillOnce(Return(true));
    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("default-thread-count", "13"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "0"));
    options.push_back( make_pair("logical-size", "1G"));
    Dedupv1dVolume* volume = volume_info->AttachVolume(options);
    ASSERT_TRUE(volume);
    ASSERT_EQ(volume->command_thread_count(), 13);
}

TEST_F(Dedupv1dVolumeInfoTest, AttachWithOwnFilterChain) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "0"));
    options.push_back( make_pair("logical-size", "1G"));
    options.push_back( make_pair("filter", "block-index-filter"));
    options.push_back( make_pair("filter", "chunk-index-filter"));
    options.push_back( make_pair("filter", "bytecompare-filter"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->FindVolume(0, NULL) != NULL);
    ASSERT_TRUE(base_volume_info->FindVolume(0) != NULL);
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->logical_size(), (uint64_t) ToStorageUnit("1G").value());
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->volume(), base_volume_info->FindVolume(0));
}

TEST_F(Dedupv1dVolumeInfoTest, AttachPersistantAfterClose) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    Restart();

    ASSERT_TRUE(volume_info->FindVolume(0, NULL) != NULL) << "Should find volume 0 after close";
    ASSERT_TRUE(base_volume_info->FindVolume(0) != NULL);
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->logical_size(), (uint64_t) ToStorageUnit("1G").value());
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->volume(), base_volume_info->FindVolume(0));
}

TEST_F(Dedupv1dVolumeInfoTest, AttachPersistantAfterCloseWithBlockSize) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "0"));
    options.push_back( make_pair("logical-size", "1G"));
    options.push_back( make_pair("sector-size", "4096"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_EQ(volume_info->FindVolume(0, NULL)->block_size(), 4096);

    Restart();

    ASSERT_TRUE(volume_info->FindVolume(0, NULL) != NULL) << "Should find volume 0 after close";
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->block_size(), 4096);
}

TEST_F(Dedupv1dVolumeInfoTest, AttachWithGroups) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "0"));
    options.push_back( make_pair("group", "a:1"));
    options.push_back( make_pair("group", "b:2"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    Restart();

    ASSERT_TRUE(volume_info->FindVolume(0, NULL) != NULL) << "Should find volume 0 after close";
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->groups().size(), 2U);
}

TEST_F(Dedupv1dVolumeInfoTest, AttachWithTargets) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "0"));
    options.push_back( make_pair("target", "a:1"));
    options.push_back( make_pair("target", "b:2"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    Restart();

    ASSERT_TRUE(volume_info->FindVolume(0, NULL) != NULL) << "Should find volume 0 after close";
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->targets().size(), 2U);
}

TEST_F(Dedupv1dVolumeInfoTest, Detach) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillOnce(Return(true));
    EXPECT_CALL(storage, Flush(_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    Restart();

    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_DETACH,_,_,_,_)).WillOnce(Return(true));
    ASSERT_TRUE(volume_info->DetachVolume(0));

    ASSERT_TRUE(volume_info->FindVolume(0, NULL) == NULL) << "Shouldn't find volume 0 after detach";
    ASSERT_TRUE(base_volume_info->FindVolume(0) == NULL);

    // volume is now in detaching state
    Dedupv1dVolumeDetacher* detacher = volume_info->detacher();
    ASSERT_TRUE(detacher->DeclareFullyDetached(1));

    Restart();

    ASSERT_TRUE(volume_info->FindVolume(0, NULL) == NULL) << "Detachment should be persistent";
    ASSERT_TRUE(base_volume_info->FindVolume(0) == NULL);

    // volume is now in detaching state
    detacher = volume_info->detacher();
    ASSERT_TRUE(detacher->DeclareFullyDetached(1));
}

TEST_F(Dedupv1dVolumeInfoTest, DetachPreconfigured) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_FALSE(volume_info->DetachVolume(0)) << "A preconfigured value should not be detached";

    ASSERT_TRUE(volume_info->FindVolume(0, NULL)) << "A preconfigured value should not be detached";
}

TEST_F(Dedupv1dVolumeInfoTest, PreconfigureDoubleID) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));

    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_FALSE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system)) << "Should not start with double id";
}

TEST_F(Dedupv1dVolumeInfoTest, PreconfigureTooLargeID) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(volume_info);
    uint32_t large_volume_id = dedupv1::DedupVolume::kMaxVolumeId + 1;
    ASSERT_TRUE(volume_info->SetOption("volume.id",ToString(large_volume_id)));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_FALSE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system)) << "Should not start with too large volume id";
}

TEST_F(Dedupv1dVolumeInfoTest, PreconfigureDoubleDefaultDeviceName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test-1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));

    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test-1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_FALSE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system)) << "Should not start with double name";
}

TEST_F(Dedupv1dVolumeInfoTest, PreconfigureDoubleGroup) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test-1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));

    ASSERT_TRUE(volume_info->SetOption("volume.id","1"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test-1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_FALSE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system)) << "Should not start with double name";
}

TEST_F(Dedupv1dVolumeInfoTest, PreconfigureGroup) {
    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id", "0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name", "test-1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size", "1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_TRUE(volume_info->FindVolume(0, NULL) != NULL) << "Should find volume 0 after close";
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->groups().size(), 1U);
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->groups()[0].first, "a");
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->groups()[0].second, 0);
}

TEST_F(Dedupv1dVolumeInfoTest, PreconfigureTarget) {
    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id", "0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name", "test-1"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "iqn.2010.05.example:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size", "1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_TRUE(volume_info->FindVolume(0, NULL) != NULL) << "Should find volume 0 after close";
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->targets().size(), 1U);
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->targets()[0].first, "iqn.2010.05.example");
    ASSERT_EQ(volume_info->FindVolume(0, NULL)->targets()[0].second, 0);
}

TEST_F(Dedupv1dVolumeInfoTest, AttachDoubleID) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_FALSE(volume_info->AttachVolume(options)) << "Attaching a volume with an already use id should fail";
}

TEST_F(Dedupv1dVolumeInfoTest, AttachGroup) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group","a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("group","a:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_FALSE(volume_info->AttachVolume(options)) << "Attachment with an already used group should fail";
}

TEST_F(Dedupv1dVolumeInfoTest, AttachTarget) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.target","iqn.2010.05.example:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("target","iqn.2010.05.example:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_FALSE(volume_info->AttachVolume(options)) << "Attachment with an already used group should fail";
}

TEST_F(Dedupv1dVolumeInfoTest, AttachedDoubleDeviceName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id", "0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name", "test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size", "1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test1"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_FALSE(volume_info->AttachVolume(options)) << "Attaching a volume with an already used device name should fail";
}

TEST_F(Dedupv1dVolumeInfoTest, ReattachWithVolumeInDetachingState) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_DETACH,_,_,_,_)).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(storage, Flush(_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->RemoveFromGroup(1, "a"));
    ASSERT_TRUE(volume_info->DetachVolume(1));

    // volume is no in detaching state
    Dedupv1dVolumeDetacher* detacher = volume_info->detacher();
    bool detaching_state = false;
    ASSERT_TRUE(detacher->IsDetaching(1, &detaching_state));
    ASSERT_TRUE(detaching_state);

    list <pair <string, string> > options2;
    options2.push_back( make_pair("id", "1"));
    options2.push_back( make_pair("device-name", "test3"));
    options2.push_back( make_pair("group", "b:0"));
    options2.push_back( make_pair("logical-size", "1G"));

    ASSERT_FALSE(volume_info->AttachVolume(options2));
}

TEST_F(Dedupv1dVolumeInfoTest, ReattachAfterVolumeLeftDetachingState) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).Times(2).WillRepeatedly(Return(true));
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_DETACH,_,_,_,_)).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(storage, Flush(_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id", "0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name", "test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size", "1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->RemoveFromGroup(1, "a"));
    ASSERT_TRUE(volume_info->DetachVolume(1));

    // volume is now in detaching state
    Dedupv1dVolumeDetacher* detacher = volume_info->detacher();
    ASSERT_TRUE(detacher->DeclareFullyDetached(1));

    // after the volume is fully detached, a reattachment is possible
    list <pair <string, string> > options2;
    options2.push_back( make_pair("id", "1"));
    options2.push_back( make_pair("device-name", "test3"));
    options2.push_back( make_pair("group", "b:0"));
    options2.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options2));
}

TEST_F(Dedupv1dVolumeInfoTest, AttachDoubleGroup) {
    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->SetOption("volume.group","Default:0"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("logical-size", "1G"));
    options.push_back( make_pair("volume.group","Default:0"));

    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    ASSERT_FALSE(volume_info->AttachVolume(options));
}

TEST_F(Dedupv1dVolumeInfoTest, AttachDoubleToSameGroup) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    EXPECT_CALL(storage, Flush(_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("logical-size", "1G"));
    options.push_back( make_pair("group", "a:0"));
    options.push_back( make_pair("group", "a:1"));
    ASSERT_FALSE(volume_info->AttachVolume(options));
}

TEST_F(Dedupv1dVolumeInfoTest, ReattachGroup) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_DETACH,_,_,_,_)).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).Times(2).WillRepeatedly(Return(true));
    EXPECT_CALL(storage, Flush(_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->RemoveFromGroup(1, "a"));
    ASSERT_TRUE(volume_info->DetachVolume(1));

    list <pair <string, string> > options2;
    options2.push_back( make_pair("id", "3"));
    options2.push_back( make_pair("device-name", "test3"));
    options2.push_back( make_pair("group", "b:0"));
    options2.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options2));
}

TEST_F(Dedupv1dVolumeInfoTest, RunBeforeStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("not started").Once();

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name", "test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size", "1G"));

    ASSERT_FALSE(volume_info->Run()) << "Run should fail if info has not been started before";
}

TEST_F(Dedupv1dVolumeInfoTest, Run) {
    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name", "test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size", "1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_TRUE(volume_info->Run());
    sleep(2);
    ASSERT_TRUE(volume_info->Stop(dedupv1::StopContext::FastStopContext()));
}

TEST_F(Dedupv1dVolumeInfoTest, RunWithAttachAndDetach) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_DETACH,_,_,_,_)).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(block_index, DeleteBlockInfo(_, _)).WillRepeatedly(Return(DELETE_NOT_FOUND));
    EXPECT_CALL(storage, Flush(_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));
    ASSERT_TRUE(volume_info->Run());
    sleep(1);

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));
    sleep(1);

    ASSERT_TRUE(volume_info->DetachVolume(0));
    // volume is no in detaching state

    Dedupv1dVolumeDetacher* detacher = volume_info->detacher();
    bool detaching_state = false;
    ASSERT_TRUE(detacher->IsDetaching(0, &detaching_state));
    ASSERT_TRUE(detaching_state) << "Volume 0 is not in detaching state directly after the detachment";
    sleep(5);

    ASSERT_TRUE(volume_info->Stop(dedupv1::StopContext::FastStopContext()));
}

TEST_F(Dedupv1dVolumeInfoTest, RunWithAttachAndDetachWithDetachingFinish) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_DETACH,_,_,_,_)).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(block_index, DeleteBlockInfo(_, _)).WillRepeatedly(Return(DELETE_NOT_FOUND));
    EXPECT_CALL(storage, Flush(_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    sleep(1);

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));
    sleep(1);

    ASSERT_TRUE(volume_info->DetachVolume(0));
    // volume is no in detaching state

    Dedupv1dVolumeDetacher* detacher = volume_info->detacher();
    VolumeInfoDetachingData detaching_data;
    uint32_t volume_id = 0;
    ASSERT_EQ(detacher->detaching_info()->Lookup(&volume_id, sizeof(volume_id), &detaching_data), LOOKUP_FOUND);
    ASSERT_FALSE(detaching_data.has_current_block_id()) << "A fresh detaching info should not have a current id";

    // manipulte the data so that the detaching finishes
    detaching_data.set_current_block_id(detaching_data.end_block_id() - 2);
    ASSERT_EQ(detacher->detaching_info()->Put(&volume_id, sizeof(volume_id), detaching_data), PUT_OK);

    ASSERT_TRUE(volume_info->Run());
    sleep(6);
    ASSERT_TRUE(volume_info->Stop(dedupv1::StopContext::FastStopContext()));

    bool detaching_state = false;
    ASSERT_TRUE(detacher->IsDetaching(0, &detaching_state));
    ASSERT_FALSE(detaching_state) << "Volume 0 should not be in detaching mode anymore";
}

TEST_F(Dedupv1dVolumeInfoTest, AddToGroup) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:1"));
    options.push_back( make_pair("group", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->AddToGroup(1, "b:1"));

    Dedupv1dVolume* volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);

    bool found = false;
    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = volume->groups().begin(); i != volume->groups().end(); i++) {
        if (i->first == "b" && i->second == 1) {
            found = true;
        }
    }
    ASSERT_TRUE(found) << "Should find group entry b:1";
    volume = NULL;

    volume = volume_info->FindVolumeByGroup("b", 1, NULL);
    ASSERT_TRUE(volume) << "Should find volume";
    ASSERT_EQ(volume->id(), 1);
    volume = NULL;
}

TEST_F(Dedupv1dVolumeInfoTest, AddToTarget) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "iqn.2010.05.example:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("target", "iqn.2010.05.example:1"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->AddToTarget(1, "iqn.2010.05.example_2:0"));

    Dedupv1dVolume* volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);

    bool found = false;
    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = volume->targets().begin(); i != volume->targets().end(); i++) {
        if (i->first == "iqn.2010.05.example_2" && i->second == 0) {
            found = true;
        }
    }
    ASSERT_TRUE(found) << "Should find target entry iqn.2010.05.example_2:0";
    volume = NULL;

    volume = volume_info->FindVolumeByTarget("iqn.2010.05.example_2", 0, NULL);
    ASSERT_TRUE(volume) << "Should find volume";
    ASSERT_EQ(volume->id(), 1);
    volume = NULL;
}

/**
 * Checks if the AddToGroup method has added the volume persistently (that means
 * after a restart).
 *
 * The test case is the same as the AddToGroup test only that the system is restarted in
 * the middle of the test.
 */
TEST_F(Dedupv1dVolumeInfoTest, AddGroupPersisting) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH, _, _,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:1"));
    options.push_back( make_pair("group", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->AddToGroup(1, "b:1"));

    Restart();

    Dedupv1dVolume* volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);

    bool found = false;
    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = volume->groups().begin(); i != volume->groups().end(); i++) {
        if (i->first == "b" && i->second == 1) {
            found = true;
        }
    }
    ASSERT_TRUE(found) << "Should find group entry b:1";
    volume = NULL;

    volume = volume_info->FindVolumeByGroup("b", 1, NULL);
    ASSERT_TRUE(volume) << "Should find volume";
    ASSERT_EQ(volume->id(), 1);
    volume = NULL;
}

/**
 * Checks if the AddToGroup method has added the volume persistently (that means
 * after a restart).
 *
 * The test case is the same as the AddToTarget test only that the system is restarted in
 * the middle of the test.
 */
TEST_F(Dedupv1dVolumeInfoTest, AddTargetPersisting) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH, _, _,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("target", "a:1"));
    options.push_back( make_pair("target", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->AddToTarget(1, "b:1"));

    Restart();

    Dedupv1dVolume* volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);

    bool found = false;
    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = volume->targets().begin(); i != volume->targets().end(); i++) {
        if (i->first == "b" && i->second == 1) {
            found = true;
        }
    }
    ASSERT_TRUE(found) << "Should find target entry b:1";
    volume = NULL;

    volume = volume_info->FindVolumeByTarget("b", 1, NULL);
    ASSERT_TRUE(volume) << "Should find volume";
    ASSERT_EQ(volume->id(), 1);
    volume = NULL;
}

TEST_F(Dedupv1dVolumeInfoTest, AddGroupDouble) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("already assigned").Once();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:1"));
    options.push_back( make_pair("group", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_FALSE(volume_info->AddToGroup(1, "b:0")) << "Adding a group twice should fail";
}

TEST_F(Dedupv1dVolumeInfoTest, AddTargetDouble) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("already assigned").Once();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("target", "a:1"));
    options.push_back( make_pair("target", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_FALSE(volume_info->AddToTarget(1, "b:0")) << "Adding a target twice should fail";
}

TEST_F(Dedupv1dVolumeInfoTest, AddGroupDoubleToSameGroup) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:1"));
    options.push_back( make_pair("group", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_FALSE(volume_info->AddToGroup(1, "c:1"));
}

TEST_F(Dedupv1dVolumeInfoTest, AddTargetDoubleToSameGroup) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("target", "a:1"));
    options.push_back( make_pair("target", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_FALSE(volume_info->AddToTarget(1, "c:1"));
}

TEST_F(Dedupv1dVolumeInfoTest, AddGroupToPreconfiguredVolume) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_FALSE(volume_info->AddToGroup(0, "c:0"));
}

TEST_F(Dedupv1dVolumeInfoTest, AddTargetToPreconfiguredVolume) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_FALSE(volume_info->AddToTarget(0, "c:0"));
}

TEST_F(Dedupv1dVolumeInfoTest, AddGroupToNonExistingVolume) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_FALSE(volume_info->AddToGroup(1, "c:0")) << "Adding a non existing volume to a group should fail";
}

TEST_F(Dedupv1dVolumeInfoTest, AddTargetToNonExistingVolume) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_FALSE(volume_info->AddToTarget(1, "c:0")) << "Adding a non existing volume to a target should fail";
}

TEST_F(Dedupv1dVolumeInfoTest, RemoveFromGroup) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:1"));
    options.push_back( make_pair("group", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->RemoveFromGroup(1, "c"));

    Dedupv1dVolume* volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);

    bool found = false;
    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = volume->groups().begin(); i != volume->groups().end(); i++) {
        if (i->first == "c") {
            found = true;
        }
    }
    ASSERT_FALSE(found) << "Should not find group entry b:1";
    volume = NULL;

    volume = volume_info->FindVolumeByGroup("c", 0, NULL);
    ASSERT_FALSE(volume) << "Should not find volume";
}

TEST_F(Dedupv1dVolumeInfoTest, RemoveFromTarget) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("target", "a:1"));
    options.push_back( make_pair("target", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->RemoveFromTarget(1, "c"));

    Dedupv1dVolume* volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);

    bool found = false;
    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = volume->targets().begin(); i != volume->targets().end(); i++) {
        if (i->first == "c") {
            found = true;
        }
    }
    ASSERT_FALSE(found) << "Should not find target entry b:1";
    volume = NULL;

    volume = volume_info->FindVolumeByTarget("c", 0, NULL);
    ASSERT_FALSE(volume) << "Should not find volume";
}

/**
 * Checks if the RemoveFromGroup method has removed the volume persistently (that means
 * after a restart).
 *
 * The test case is the same as the RemoveFromGroup test only that the system is restarted in
 * the middle of the test.
 */
TEST_F(Dedupv1dVolumeInfoTest, RemoveGroupPersisting) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id", "0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name", "test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:1"));
    options.push_back( make_pair("group", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->RemoveFromGroup(1, "c"));

    Restart();

    Dedupv1dVolume* volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);

    bool found = false;
    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = volume->groups().begin(); i != volume->groups().end(); i++) {
        if (i->first == "c") {
            found = true;
        }
    }
    ASSERT_FALSE(found) << "Should not find group entry b:1";
    volume = NULL;

    volume = volume_info->FindVolumeByGroup("c", 0, NULL);
    ASSERT_FALSE(volume) << "Should not find volume";
}

TEST_F(Dedupv1dVolumeInfoTest, RemoveGroupFromPreconfiguredVolume) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    ASSERT_FALSE(volume_info->RemoveFromGroup(0, "b"));
}

TEST_F(Dedupv1dVolumeInfoTest, RemoveNotExistingGroup) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:1"));
    options.push_back( make_pair("group", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_FALSE(volume_info->RemoveFromGroup(1, "x"));

    // Now we have to check that the call hasn't destroyed the state
    Dedupv1dVolume* volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);

    ASSERT_EQ(volume->groups().size(), 2);
    bool found_a = false;
    bool found_c = false;
    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = volume->groups().begin(); i != volume->groups().end(); i++) {
        if (i->first == "a") {
            found_a = true;
        }
        if (i->first == "c") {
            found_c = true;
        }
    }
    ASSERT_TRUE(found_a && found_c) << "Should find the old entries";
    volume = NULL;

    volume = volume_info->FindVolumeByGroup("a", 1, NULL);
    ASSERT_TRUE(volume) << "Should find volume";
    ASSERT_EQ(volume->id(), 1);
    volume = NULL;

    volume = volume_info->FindVolumeByGroup("c", 0, NULL);
    ASSERT_TRUE(volume) << "Should find volume";
    ASSERT_EQ(volume->id(), 1);
    volume = NULL;
}

TEST_F(Dedupv1dVolumeInfoTest, MaintenanceModeOutsideRunningState) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    // attach a dynamic volume so that we have something to play
    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:1"));
    options.push_back( make_pair("group", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->ChangeMaintainceMode(1, true));

    Dedupv1dVolume* volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);
    ASSERT_TRUE(volume->maintenance_mode()) << "Volume should be in maintenance mode";

    Restart();

    volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);
    ASSERT_TRUE(volume->maintenance_mode()) << "Volume should still be in maintenance mode";

    ASSERT_TRUE(volume_info->ChangeMaintainceMode(1, false));

    volume = volume_info->FindVolume(1, NULL);
    ASSERT_TRUE(volume);
    ASSERT_FALSE(volume->maintenance_mode()) << "Volume should be in running mode";
}

/**
 * This test cases ensures that a volume can switch into maintenance mode and back into the
 * running mode.
 */
TEST_F(Dedupv1dVolumeInfoTest, MaintenanceModeInRunningStateWithoutRestart) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));
    ASSERT_TRUE(volume_info->Run());

    // attach a dynamic volume so that we have something to play
    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:1"));
    options.push_back( make_pair("group", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    for (int i = 0; i < 5; i++) {
        ASSERT_TRUE(volume_info->ChangeMaintainceMode(1, true));

        Dedupv1dVolume* volume = volume_info->FindVolume(1, NULL);
        ASSERT_TRUE(volume);
        ASSERT_TRUE(volume->maintenance_mode()) << "Volume should be in maintenance mode";

        volume = volume_info->FindVolume(1, NULL);
        ASSERT_TRUE(volume);
        ASSERT_TRUE(volume->maintenance_mode()) << "Volume should still be in maintenance mode";

        ASSERT_TRUE(volume_info->ChangeMaintainceMode(1, false));

        volume = volume_info->FindVolume(1, NULL);
        ASSERT_TRUE(volume);
        ASSERT_FALSE(volume->maintenance_mode()) << "Volume should be in running mode";

    }
}

TEST_F(Dedupv1dVolumeInfoTest, PrintStatistics) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","test1"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "a:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.group", "b:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));
    ASSERT_TRUE(volume_info->Run());

    // attach a dynamic volume so that we have something to play
    list <pair <string, string> > options;
    options.push_back( make_pair("id", "1"));
    options.push_back( make_pair("device-name", "test2"));
    options.push_back( make_pair("group", "a:1"));
    options.push_back( make_pair("group", "c:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    string content = volume_info->PrintStatistics();

    Json::Reader reader;
    Json::Value root;
    bool parsingSuccessful = reader.parse( content, root );
    ASSERT_TRUE(parsingSuccessful) <<  "Failed to parse configuration: " << reader.getFormatedErrorMessages() << "\n" << content;
}

TEST_F(Dedupv1dVolumeInfoTest, TargetMismatch) {
    EXPECT_CALL(log, CommitEvent(EVENT_TYPE_VOLUME_ATTACH,_,_,_,_)).WillRepeatedly(Return(true));

    // close the default target info, we need here a special config
    ASSERT_TRUE(target_info->Close());
    target_info = NULL;
    unlink("work/dedupv1_target_info");
    unlink("work/dedupv1_target_info.wal");

    target_info = new Dedupv1dTargetInfo();
    ASSERT_TRUE(target_info);
    ASSERT_TRUE(target_info->SetOption("type", "sqlite-disk-btree"));
    ASSERT_TRUE(target_info->SetOption("filename", "work/dedupv1_target_info"));
    ASSERT_TRUE(target_info->SetOption("max-item-count", "64K"));
    ASSERT_TRUE(target_info->SetOption("target", "1"));
    ASSERT_TRUE(target_info->SetOption("target.name", "iqn.2010-06.de.pc2:dedupv1"));
    ASSERT_TRUE(target_info->SetOption("target", "2"));
    ASSERT_TRUE(target_info->SetOption("target.name", "iqn.2005-03.info.christmann:backup:special"));

    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","dedupv1"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "iqn.2010-06.de.pc2:dedupv1:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));

    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

    list <pair <string, string> > options;
    options.push_back( make_pair("id", "3"));
    options.push_back( make_pair("device-name", "Backup1"));
    options.push_back( make_pair("target", "iqn.2005-03.info.christmann:backup:special:1"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    options.clear();
    options.push_back( make_pair("id", "4"));
    options.push_back( make_pair("device-name", "Backup2"));
    options.push_back( make_pair("target", "iqn.2005-03.info.christmann:backup:special:0"));
    options.push_back( make_pair("logical-size", "1G"));
    ASSERT_TRUE(volume_info->AttachVolume(options));

    ASSERT_TRUE(volume_info->Close());
    volume_info = NULL;
    ASSERT_TRUE(target_info->Close());
    target_info = NULL;

    target_info = new Dedupv1dTargetInfo();
    ASSERT_TRUE(target_info);
    ASSERT_TRUE(target_info->SetOption("type", "sqlite-disk-btree"));
    ASSERT_TRUE(target_info->SetOption("filename", "work/dedupv1_target_info"));
    ASSERT_TRUE(target_info->SetOption("max-item-count", "64K"));
    ASSERT_TRUE(target_info->SetOption("target", "1"));
    ASSERT_TRUE(target_info->SetOption("target.name", "iqn.2010-06.de.pc2:dedupv1"));
    ASSERT_TRUE(target_info->SetOption("target", "2"));
    ASSERT_TRUE(target_info->SetOption("target.name", "iqn.2005-03.info.christmann:backup:special"));

    volume_info = new Dedupv1dVolumeInfo();
    ASSERT_TRUE(volume_info);
    SetDefaultOptions(volume_info);
    ASSERT_TRUE(volume_info->SetOption("volume.id","0"));
    ASSERT_TRUE(volume_info->SetOption("volume.device-name","dedupv1"));
    ASSERT_TRUE(volume_info->SetOption("volume.target", "iqn.2010-06.de.pc2:dedupv1:0"));
    ASSERT_TRUE(volume_info->SetOption("volume.logical-size","1G"));

    ASSERT_TRUE(target_info->Start(dedupv1::StartContext(), volume_info, user_info));
    ASSERT_TRUE(volume_info->Start(dedupv1::StartContext(), group_info, target_info, &dedup_system));

}

}
