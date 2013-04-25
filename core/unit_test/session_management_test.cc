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
#include <core/session_management.h>
#include <core/dedup_system.h>
#include <test_util/log_assert.h>

using dedupv1::base::ResourceManagement;

namespace dedupv1 {
namespace contentstorage {

class SessionManagementTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    ResourceManagement<Session>* smc;
    DedupSystem* system;
    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;

    virtual void SetUp() {
        smc = NULL;
        system = NULL;

        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());

        system = new DedupSystem();
        ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
        ASSERT_TRUE(system->Start(StartContext(), &info_store, &tp));
        ASSERT_TRUE(system->Run());

        DedupVolume* volume = system->GetVolume(0);
        ASSERT_TRUE(volume);

        smc = new ResourceManagement<Session>();
        ASSERT_TRUE(smc);
        ASSERT_TRUE(smc->Init("session", 2, new SessionResourceType(volume)));
    }

    virtual void TearDown() {
        if (smc) {
            ASSERT_TRUE(smc->Close());
            smc = NULL;
        }
        if (system) {
            ASSERT_TRUE(system->Close());
        }
    }
};

TEST_F(SessionManagementTest, Start) {

}

TEST_F(SessionManagementTest, Cycle) {
    Session* s = smc->Acquire();
    ASSERT_TRUE(s);

    ASSERT_TRUE(smc->Release(s));
}

TEST_F(SessionManagementTest, DoubleCycle) {
    Session* s1 = smc->Acquire();
    ASSERT_TRUE(s1);
    ASSERT_TRUE(smc->Release(s1));

    Session* s2 = smc->Acquire();
    ASSERT_TRUE(s2);
    ASSERT_TRUE(smc->Release(s2));
}

TEST_F(SessionManagementTest, DoubleInterleaved) {
    Session* s1 = smc->Acquire();
    ASSERT_TRUE(s1);
    Session* s2 = smc->Acquire();
    ASSERT_TRUE(s2);

    ASSERT_TRUE(smc->Release(s2));
    ASSERT_TRUE(smc->Release(s1));
}

TEST_F(SessionManagementTest, DoubleReveresed) {
    Session* s1 = smc->Acquire();
    ASSERT_TRUE(s1);
    Session* s2 = smc->Acquire();
    ASSERT_TRUE(s2);

    ASSERT_TRUE(smc->Release(s1));
    ASSERT_TRUE(smc->Release(s2));
}

TEST_F(SessionManagementTest, Full) {
    EXPECT_LOGGING(dedupv1::test::WARN).Once();

    Session* s1 = smc->Acquire();
    ASSERT_TRUE(s1);
    Session* s2 = smc->Acquire();
    ASSERT_TRUE(s2);

    ASSERT_FALSE(smc->Acquire()) << "Acquire should fail because all sessions are used";

    ASSERT_TRUE(smc->Release(s1));
    ASSERT_TRUE(smc->Release(s2));
}

}
}
