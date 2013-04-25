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

#include <core/idle_detector.h>
#include <base/logging.h>
#include <test_util/log_assert.h>

LOGGER("IdleDetectorTest");

namespace dedupv1 {

class IdleDetectorTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    IdleDetector* id;

    virtual void SetUp() {

        id = new IdleDetector();
        ASSERT_TRUE(id);
    }

    virtual void TearDown() {
        if (id) {
            delete id;
            id = NULL;
        }
    }

    void SetDefaultOptions(IdleDetector* id) {
        ASSERT_TRUE(id->SetOption("idle-tick-interval", "1"));
    }
};

TEST_F(IdleDetectorTest, Init) {
    // do nothing
}

TEST_F(IdleDetectorTest, StartWithoutConfig) {
    ASSERT_TRUE(id->Start());
}

TEST_F(IdleDetectorTest, Start) {
    SetDefaultOptions(id);

    ASSERT_TRUE(id->Start());
    ASSERT_EQ(id->GetIdleTickInterval(), 1U);
}

TEST_F(IdleDetectorTest, DoubleStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    SetDefaultOptions(id);
    ASSERT_TRUE(id->Start());
    ASSERT_FALSE(id->Start());
}

TEST_F(IdleDetectorTest, StopWithoutRun) {
    SetDefaultOptions(id);
    ASSERT_TRUE(id->Start());
    ASSERT_TRUE(id);
}

TEST_F(IdleDetectorTest, StopWithoutStart) {
    SetDefaultOptions(id);
    ASSERT_TRUE(id->Stop(StopContext::FastStopContext()));
}

TEST_F(IdleDetectorTest, RunAndStop) {
    SetDefaultOptions(id);
    ASSERT_TRUE(id->Start());
    ASSERT_TRUE(id->Run());
    ASSERT_TRUE(id->Stop(StopContext::FastStopContext()));
}

TEST_F(IdleDetectorTest, RunAndFastStop) {
    SetDefaultOptions(id);
    ASSERT_TRUE(id->Start());
    ASSERT_TRUE(id->Run());
    ASSERT_TRUE(id->Stop(dedupv1::StopContext::FastStopContext()));
}

TEST_F(IdleDetectorTest, RunWithoutStop) {
    SetDefaultOptions(id);
    ASSERT_TRUE(id->Start());
    ASSERT_TRUE(id->Run());
}

TEST_F(IdleDetectorTest, RunWithoutStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_FALSE(id->Run()) << "Run should fail";
}

class IdleDetectorTestConsumer : public IdleTickConsumer {
private:
    int tick_count;
public:

    ~IdleDetectorTestConsumer() {

    }
    IdleDetectorTestConsumer() {
        tick_count = 0;
    }

    int GetTickCount() {
        return tick_count;
    }

    virtual void IdleTick() {
        this->tick_count++;
    }
};

TEST_F(IdleDetectorTest, RegisterAndUnregister) {
    IdleDetectorTestConsumer c;

    ASSERT_TRUE(id->RegisterIdleConsumer("test", &c));
    ASSERT_TRUE(id->UnregisterIdleConsumer("test"));
}

TEST_F(IdleDetectorTest, RegisterWithoutName) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    IdleDetectorTestConsumer c;
    ASSERT_FALSE(id->RegisterIdleConsumer("", &c)) << "Register should fail";
}

TEST_F(IdleDetectorTest, RegisterWithoutConsumer) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_FALSE(id->RegisterIdleConsumer("test", NULL)) << "Register should fail";
}

TEST_F(IdleDetectorTest, DoubleRegister) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    IdleDetectorTestConsumer c;

    ASSERT_TRUE(id->RegisterIdleConsumer("test", &c));
    ASSERT_FALSE(id->RegisterIdleConsumer("test", &c)) << "Register should fail"; // second time

    ASSERT_TRUE(id->UnregisterIdleConsumer("test"));
}

TEST_F(IdleDetectorTest, OnRequest) {
    SetDefaultOptions(id);
    ASSERT_TRUE(id->Start());
    ASSERT_TRUE(id->Run());

    IdleDetectorTestConsumer c;

    ASSERT_TRUE(id->RegisterIdleConsumer("test", &c));

    uint32_t old_tick_count = c.GetTickCount();
    ASSERT_TRUE(id->OnRequestEnd(REQUEST_READ, 0, 1024, 20 * 1024 * 1024, 100.0)); // 20MB
    sleep(10);
    ASSERT_LE(c.GetTickCount(), old_tick_count + 2);

    ASSERT_TRUE(id->UnregisterIdleConsumer("test"));
}

TEST_F(IdleDetectorTest, OnRequestWait) {
    SetDefaultOptions(id);
    ASSERT_TRUE(id->Start());
    ASSERT_TRUE(id->Run());

    IdleDetectorTestConsumer c;

    ASSERT_TRUE(id->RegisterIdleConsumer("test", &c));
    ASSERT_TRUE(id->OnRequestEnd(REQUEST_READ, 0, 1024, 1024, 100.0));
    sleep(9);
    ASSERT_GT(c.GetTickCount(), 2);

    ASSERT_TRUE(id->UnregisterIdleConsumer("test"));
}

TEST_F(IdleDetectorTest, OnRequestStateChangeBack) {
    SetDefaultOptions(id);
    ASSERT_TRUE(id->Start());
    ASSERT_TRUE(id->Run());

    IdleDetectorTestConsumer c;

    ASSERT_TRUE(id->RegisterIdleConsumer("test", &c));

    uint32_t old_tick_count = c.GetTickCount();
    ASSERT_TRUE(id->OnRequestEnd(REQUEST_READ, 0, 1024, 20 * 1024 * 1024, 100.0)); // 20MB
    sleep(20);
    ASSERT_LE(c.GetTickCount(), old_tick_count + 2);
    sleep(20);
    ASSERT_TRUE(id->IsIdle());
    ASSERT_GE(c.GetTickCount(), old_tick_count);

    DEBUG("Unregister");
    ASSERT_TRUE(id->UnregisterIdleConsumer("test"));

    DEBUG("Shutdown");
}

}
