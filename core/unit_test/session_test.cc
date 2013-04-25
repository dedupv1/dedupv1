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

using dedupv1::chunkstore::ChunkStore;

namespace dedupv1 {
namespace contentstorage {

class SessionTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Session* session;
    Chunker* chunker;
    Fingerprinter* fingerprinter;
    dedupv1::filter::FilterChain* filter_chain;

    virtual void SetUp() {
        session = NULL;
        filter_chain = NULL;

        chunker = Chunker::Factory().Create("static-chunker");
        ASSERT_TRUE(chunker->Start());
        fingerprinter = Fingerprinter::Factory().Create("sha1");

        ASSERT_TRUE(chunker);
        ASSERT_TRUE(fingerprinter);

        session = new Session();
        ASSERT_TRUE(session);
        std::set<const dedupv1::filter::Filter*> filters;
        ASSERT_TRUE(session->Init(64 * 1024, chunker, fingerprinter, filters));
    }

    virtual void TearDown() {
        if (session) {
            ASSERT_TRUE(session->Close());
            session = NULL;
        }

        // The ownership is taken over by the session
        fingerprinter = NULL;

        ASSERT_TRUE(chunker->Close());
        chunker = NULL;

        if (filter_chain) {
            ASSERT_TRUE(filter_chain->Close());
            filter_chain = NULL;
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

