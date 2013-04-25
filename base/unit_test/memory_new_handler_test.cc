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

#include <list>

#include <gtest/gtest.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <base/memory_new_handler.h>
#include <base/logging.h>
#include <test_util/log_assert.h>

using std::list;
using dedupv1::base::memory::NewHandlerListener;

LOGGER("NewHandlerTest");

/**
 * mock memory problem listener used for the unit tests
 */
class MockNewHandlerListener : public NewHandlerListener {
private:
    /**
     * number of times out or memory events have been received
     */
    int outOfMemoryEventReceived_;
public:
    MockNewHandlerListener() : NewHandlerListener(), outOfMemoryEventReceived_(0) {
    }
    bool ReceiveOutOfMemoryEvent() {
        DEBUG("Received out of memory event");
        outOfMemoryEventReceived_++;
        return true;
    }
    int outOfMemoryEventReceived() {
        return this->outOfMemoryEventReceived_;
    }
};

/**
 * Unit tests for the memory parachute.
 */
class NewHandlerTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    MockNewHandlerListener mock_listener;

    virtual void SetUp() {
        ASSERT_TRUE(dedupv1::base::memory::AddMemoryParachuteListener(&mock_listener));
    }

    virtual void TearDown() {
        ASSERT_TRUE(dedupv1::base::memory::RemoveMemoryParachuteListener(&mock_listener));
        ASSERT_TRUE(dedupv1::base::memory::ClearMemoryParachute());
    }
};

TEST_F(NewHandlerTest, ClearWithoutRegister) {
    // parachute is cleared in TearDown
}

/**
 * Note: During the test case messages
 * a la "tcmalloc: large alloc 0 bytes == (nil) @" are logged. This
 * is normal
 */
TEST_F(NewHandlerTest, RegisterAndCallNewsHandler) {
    dedupv1::base::memory::RegisterMemoryParachute(128 * 1024 * 1024);

    // Force a failed allocation
    int* tooLarge;
    tooLarge = new (std::nothrow) int signed [-1];
    EXPECT_FALSE(tooLarge);
    EXPECT_EQ(1, mock_listener.outOfMemoryEventReceived());

    // Force a failed allocation again. Should not call the listener again.
    tooLarge = new (std::nothrow) int signed [-1];
    EXPECT_FALSE(tooLarge);
    EXPECT_EQ(1, mock_listener.outOfMemoryEventReceived());
}

TEST_F(NewHandlerTest, AllocateMuchMemory) {
    struct rlimit limit;
    memset(&limit, 0, sizeof(limit));
    struct rlimit old_limit;
    memset(&old_limit, 0, sizeof(old_limit));

    ASSERT_EQ(getrlimit(RLIMIT_AS, &old_limit), 0);
    memcpy(&limit, &old_limit, sizeof(old_limit));
    limit.rlim_cur = 300 * 1024 * 1024;
    ASSERT_EQ(setrlimit(RLIMIT_AS, &limit), 0);

    dedupv1::base::memory::RegisterMemoryParachute(128 * 1024 * 1024);

    list<byte*> regions;
    int memcount = 0;
    while (mock_listener.outOfMemoryEventReceived() == 0) {
        byte* new_region = new byte[1 * 1024 * 1024];
        EXPECT_TRUE(new_region);
        regions.push_back(new_region);
        memcount++;
        if (memcount % 1024 == 0) {
            DEBUG("" << (memcount) << " MB");
        }
    }
    EXPECT_EQ(1, mock_listener.outOfMemoryEventReceived());

    // cleanup
    list<byte*>::iterator i;
    for (i = regions.begin(); i != regions.end(); i++) {
        byte* region = *i;
        delete[] region;
    }
    regions.clear();

    ASSERT_EQ(setrlimit(RLIMIT_AS, &old_limit), 0);
}
