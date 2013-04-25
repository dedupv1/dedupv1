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
#include <gmock/gmock.h>

#include <test/dedup_system_mock.h>
#include <test/content_storage_mock.h>
#include <test/session_mock.h>

#include <base/logging.h>
#include <core/dedup_system.h>
#include <core/dedup_volume.h>
#include <base/strutil.h>
#include <core/dedupv1_scsi.h>
#include <test_util/log_assert.h>
#include <test/filter_chain_mock.h>
#include <test/filter_mock.h>

using std::list;
using std::pair;
using std::string;
using std::make_pair;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::scsi::ScsiResult;
using testing::Return;
using testing::_;

LOGGER("DedupVolumeTest");

namespace dedupv1 {

class DedupVolumeTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    MockDedupSystem system;
    MockContentStorage content_storage;
    MockSession* session;
    MockFilterChain filter_chain;
    MockFilter filter;
    DedupVolume* volume;

    size_t buffer_size;
    byte buffer[8 * 1024];

    DedupVolumeTest() : filter("test", dedupv1::filter::Filter::FILTER_EXISTING) {
    }

    virtual void SetUp() {
        volume = new DedupVolume();
        buffer_size = 8 * 1024;

        // get new session by new. this is necessary to hook into the resource management
        session = new MockSession();

        EXPECT_CALL(system, block_size()).WillRepeatedly(Return(64 * 1024));
        EXPECT_CALL(system, content_storage()).WillRepeatedly(Return(&content_storage));
        EXPECT_CALL(system, filter_chain()).WillRepeatedly(Return(&filter_chain));
        EXPECT_CALL(filter_chain, GetFilterByName(_)).WillRepeatedly(Return(&filter));
        EXPECT_CALL(filter_chain, GetFilterByName("chunk-index-filter")).WillRepeatedly(Return(&filter));
        EXPECT_CALL(content_storage, CreateSession(_, _)).WillRepeatedly(Return(session));
    }

    virtual void TearDown() {
        if (volume) {
            ASSERT_TRUE(volume->Close());
            delete volume;
            volume = NULL;
        }
        if (session) {
            // session may be freed by resource management system, but here it is not
            delete session;
        }
        session = NULL;
    }
};

TEST_F(DedupVolumeTest, Create) {
    // do nothing
}

TEST_F(DedupVolumeTest, StartWithoutConfig) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_FALSE(volume->Start(&system, false));
}

TEST_F(DedupVolumeTest, StartWithoutSystem) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(volume->SetOption( "id", "0"));
    ASSERT_TRUE(volume->SetOption( "logical-size", "1G"));
    ASSERT_FALSE(volume->Start(NULL, false));
}

TEST_F(DedupVolumeTest, StartWithDefaultConfig) {
    ASSERT_TRUE(volume->SetOption( "id", "0"));
    ASSERT_TRUE(volume->SetOption( "logical-size", "1G"));

    ASSERT_TRUE(volume->Start(&system, false));

    ASSERT_EQ(volume->GetId(), 0U);
    ASSERT_EQ(volume->GetLogicalSize(), ToStorageUnit("1G").value());
}

TEST_F(DedupVolumeTest, StartWithinMainteinanceMode) {
    ASSERT_TRUE(volume->SetOption( "id", "0"));
    ASSERT_TRUE(volume->SetOption( "logical-size", "1G"));

    ASSERT_TRUE(volume->Start(&system, true));

    ASSERT_FALSE(volume->chunker());
    ASSERT_FALSE(volume->session_management());

    ASSERT_TRUE(volume->ChangeMaintenanceMode(false));

    ASSERT_TRUE(volume->session_management());

    ASSERT_TRUE(volume->ChangeMaintenanceMode(true));

    ASSERT_FALSE(volume->session_management());
}

TEST_F(DedupVolumeTest, StartWithFilterConfig) {
    ASSERT_TRUE(volume->SetOption( "id", "0"));
    ASSERT_TRUE(volume->SetOption( "logical-size", "1G"));

    ASSERT_TRUE(volume->SetOption("filter","chunk-index-filter"));
    ASSERT_TRUE(volume->Start(&system, false));

    ASSERT_EQ(volume->GetId(), 0U);
    ASSERT_EQ(volume->GetLogicalSize(), ToStorageUnit("1G").value());
}

TEST_F(DedupVolumeTest, StartWithWrongFilterConfig) {
    dedupv1::filter::Filter* f = NULL;
    EXPECT_CALL(filter_chain, GetFilterByName("bla-index-filter")).WillOnce(Return(f));
    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    ASSERT_TRUE(volume->SetOption( "id", "0"));
    ASSERT_TRUE(volume->SetOption( "logical-size", "1G"));

    ASSERT_TRUE(volume->SetOption("filter","bla-index-filter"));
    ASSERT_FALSE(volume->Start(&system, false)) << "Failed to start index";
}

TEST_F(DedupVolumeTest, StartWithChunkingConfig) {
    ASSERT_TRUE(volume->SetOption( "id", "0"));
    ASSERT_TRUE(volume->SetOption( "logical-size", "1G"));

    ASSERT_TRUE(volume->SetOption("chunking","rabin"));
    ASSERT_TRUE(volume->Start(&system, false));

    ASSERT_EQ(volume->GetId(), 0U);
    ASSERT_EQ(volume->GetLogicalSize(), ToStorageUnit("1G").value());
}

TEST_F(DedupVolumeTest, StartWithChunkingConfigChangeFilter) {
    ASSERT_TRUE(volume->SetOption( "id", "0"));
    ASSERT_TRUE(volume->SetOption( "logical-size", "1G"));

    ASSERT_TRUE(volume->SetOption("chunking","rabin"));
    ASSERT_TRUE(volume->Start(&system, false));

    ASSERT_EQ(volume->GetId(), 0U);
    ASSERT_EQ(volume->GetLogicalSize(), ToStorageUnit("1G").value());

    ASSERT_TRUE(volume->ChangeMaintenanceMode(true));

    list<pair<string, string> > options;
    options.push_back(make_pair("filter", "chunk-index-filter"));
    ASSERT_TRUE(volume->ChangeOptions(options));

    ASSERT_TRUE(volume->ChangeMaintenanceMode(false));

    ASSERT_TRUE(volume->chunker());
    ASSERT_GT(volume->chunking_config().size(), 0);

    DEBUG(volume->DebugString());
}

TEST_F(DedupVolumeTest, MakeRequest) {
    EXPECT_CALL(system, MakeRequest(_, REQUEST_READ, 0, 0, buffer_size, buffer, _))
    .Times(1)
    .WillOnce(::testing::Return(ScsiResult::kOk));

    ASSERT_TRUE(volume->SetOption( "id", "0"));
    ASSERT_TRUE(volume->SetOption( "logical-size", "1G"));
    ASSERT_TRUE(volume->Start(&system, false));

    ASSERT_TRUE(volume->MakeRequest(REQUEST_READ, 0, buffer_size, buffer, NO_EC));

    session = NULL; // session will be auto-freed by resource management. A kind of bad hack necessary
    // because of all the mocking
}

TEST_F(DedupVolumeTest, GetBlockInterval) {
    ASSERT_TRUE(volume->SetOption( "id", "0"));
    ASSERT_TRUE(volume->SetOption( "logical-size", "64M"));
    ASSERT_TRUE(volume->Start(&system, false));

    uint64_t start_block_id = 0;
    uint64_t end_block_id = 0;
    ASSERT_TRUE(volume->GetBlockInterval(&start_block_id, &end_block_id));
    ASSERT_EQ(start_block_id, 0);
    ASSERT_EQ(end_block_id, 1024);
}

TEST_F(DedupVolumeTest, GetBlockIntervalWithOtherVolumeId) {
    ASSERT_TRUE(volume->SetOption( "id", "1"));
    ASSERT_TRUE(volume->SetOption( "logical-size", "64M"));
    ASSERT_TRUE(volume->Start(&system, false));

    uint64_t start_block_id = 0;
    uint64_t end_block_id = 0;
    ASSERT_TRUE(volume->GetBlockInterval(&start_block_id, &end_block_id));
    ASSERT_GE(start_block_id, 0);
    ASSERT_EQ(end_block_id, start_block_id + 1024);
}

TEST_F(DedupVolumeTest, RequestOutOfRange) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("out of range").Once();

    ASSERT_TRUE(volume->SetOption("id", "0"));
    ASSERT_TRUE(volume->SetOption("logical-size", "1G"));
    ASSERT_TRUE(volume->Start(&system, false));

    ScsiResult result = volume->MakeRequest(REQUEST_READ, ToStorageUnit("1G").value(), buffer_size, buffer, NO_EC);
    ASSERT_EQ(result.sense_key(), dedupv1::scsi::SCSI_KEY_ILLEGAL_REQUEST);
}

}
