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
#include <core/dedup.h>
#include <base/locks.h>

#include <core/block_mapping.h>
#include <core/open_request.h>
#include <core/session.h>
#include <core/chunker.h>
#include <test_util/log_assert.h>
#include "filter_chain_test_util.h"
#include <core/dedup_system.h>

using dedupv1::chunkstore::ChunkStore;

namespace dedupv1 {
namespace contentstorage {

class SessionTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Session* session;

    DedupSystem* system;
    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;

    virtual void SetUp() {
        session = NULL;
        system = NULL;

        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());

        system = new DedupSystem();
        ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
        ASSERT_TRUE(system->Start(StartContext(), &info_store, &tp));
        ASSERT_TRUE(system->Run());

        DedupVolume* volume = system->GetVolume(0);
        ASSERT_TRUE(volume);

        session = new Session();
        ASSERT_TRUE(session);
        ASSERT_TRUE(session->Init(volume));
    }

    virtual void TearDown() {
        if (session) {
            ASSERT_TRUE(session->Close());
            session = NULL;
        }
        if (system) {
            ASSERT_TRUE(system->Close());
        }
    }
};

TEST_F(SessionTest, Start) {
    ASSERT_EQ(session->open_request_count(), 0U);
}

TEST_F(SessionTest, SingleOpenRequest) {
    dedupv1::blockindex::BlockMapping m1(0, 64 * 1024);
    dedupv1::blockindex::BlockMapping m2(64 * 1024);
    m2.CopyFrom(m1);

    ASSERT_TRUE(session->AppendBlock(m1, m2));
    ASSERT_EQ(session->open_request_count(), 1U);
    ASSERT_TRUE(session->GetRequest(0));
    ASSERT_EQ(session->GetRequest(0)->block_id(), 0U);

    ASSERT_TRUE(session->DeleteRequest(0));
    ASSERT_EQ(session->open_request_count(), 0U);

}

TEST_F(SessionTest, TwoOpenRequest2) {
    dedupv1::blockindex::BlockMapping m1(0, 64 * 1024);
    dedupv1::blockindex::BlockMapping m2(64 * 1024);
    m2.CopyFrom(m1);

    dedupv1::blockindex::BlockMapping m3(1, 64 * 1024);
    dedupv1::blockindex::BlockMapping m4(64 * 1024);
    m4.CopyFrom(m3);

    ASSERT_TRUE(session->AppendBlock(m1, m2));
    ASSERT_EQ(session->open_request_count(), 1U);
    ASSERT_TRUE(session->GetRequest(0));
    ASSERT_EQ(session->GetRequest(0)->block_id(), 0U);

    ASSERT_TRUE(session->AppendBlock(m3, m4));
    ASSERT_EQ(session->open_request_count(), 2U);
    ASSERT_TRUE(session->GetRequest(0));
    ASSERT_EQ(session->GetRequest(0)->block_id(), 0U);
    ASSERT_TRUE(session->GetRequest(1));
    ASSERT_EQ(session->GetRequest(1)->block_id(), 1U);

    ASSERT_TRUE(session->ClearRequests(1));
    ASSERT_EQ(session->open_request_count(), 1U);
    ASSERT_TRUE(session->GetRequest(0));
    ASSERT_EQ(session->GetRequest(0)->block_id(), 1U);
}

}
}

