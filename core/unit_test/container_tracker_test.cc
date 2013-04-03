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

#include <base/logging.h>
#include <core/container_tracker.h>
#include <core/storage.h>
#include <core/container_storage.h>
#include <core/container.h>
#include <test_util/log_assert.h>

using dedupv1::chunkstore::Container;
using dedupv1::chunkstore::Storage;

LOGGER("ContainerTrackerTest");

namespace dedupv1 {

class ContainerTrackerTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(ContainerTrackerTest, Init) {
    ContainerTracker tracker;
    ASSERT_EQ(tracker.GetNextProcessingContainer(), Storage::ILLEGAL_STORAGE_ADDRESS);
    ASSERT_TRUE(tracker.ShouldProcessContainer(Container::kLeastValidContainerId));
    ASSERT_TRUE(tracker.ShouldProcessContainer(Container::kLeastValidContainerId + 1));
}

TEST_F(ContainerTrackerTest, ProcessedBeforeShouldProcess) {
    ContainerTracker tracker;

    ASSERT_TRUE(tracker.ProcessedContainer(1));
    ASSERT_FALSE(tracker.ShouldProcessContainer(1));
}

TEST_F(ContainerTrackerTest, Process) {
    ContainerTracker tracker;
    for (int i = 1; i < 10; i++) {
        ASSERT_TRUE(tracker.ShouldProcessContainer(i)) << "1. query for " << i << " failed";
        ASSERT_TRUE(tracker.ProcessedContainer(i)) << "processing of " << i << " failed";
        ASSERT_FALSE(tracker.ShouldProcessContainer(i)) << "2. query for " << i << " failed";
    }
    for (int i = 10; i < 20; i++) {
        ASSERT_TRUE(tracker.ShouldProcessContainer(i)) << "query for " << i << " failed";
    }
}

TEST_F(ContainerTrackerTest, Reset) {
    ContainerTracker tracker;
    for (int i = 1; i < 10; i++) {
        ASSERT_TRUE(tracker.ShouldProcessContainer(i));
        ASSERT_TRUE(tracker.ProcessedContainer(i));
        ASSERT_FALSE(tracker.ShouldProcessContainer(i));
    }
    tracker.Reset();
    for (int i = 10; i < 20; i++) {
        ASSERT_TRUE(tracker.ShouldProcessContainer(i));
    }
}

TEST_F(ContainerTrackerTest, ProcessReverse) {
    ContainerTracker tracker;
    for (int i = 9; i >= 1; i--) {
        ASSERT_TRUE(tracker.ShouldProcessContainer(i)) << "1. query for " << i << " failed";
        ASSERT_TRUE(tracker.ProcessedContainer(i)) << "processing of " << i << " failed";
        ASSERT_FALSE(tracker.ShouldProcessContainer(i)) << "2. query for " << i << " failed";
    }
}

TEST_F(ContainerTrackerTest, SerializeAndParse) {
    ContainerTracker tracker;
    for (int i = 1; i < 10; i++) {
        ASSERT_TRUE(tracker.ShouldProcessContainer(i)) << "1. query for " << i << " failed";
        ASSERT_TRUE(tracker.ProcessedContainer(i)) << "processing of " << i << " failed";
        ASSERT_FALSE(tracker.ShouldProcessContainer(i)) << "2. query for " << i << " failed";
    }
    ContainerTrackerData data;
    tracker.SerializeTo(&data);

    DEBUG("Restart");

    ContainerTracker tracker2;
    tracker2.ParseFrom(data);
    tracker2.Reset();
    DEBUG("Tracker after restart: " << tracker2.DebugString());
    for (int i = 1; i < 10; i++) {
        ASSERT_FALSE(tracker2.ShouldProcessContainer(i)) << "query for " << i << " failed";
    }
    for (int i = 10; i < 20; i++) {
        ASSERT_TRUE(tracker2.ShouldProcessContainer(i)) << "query for " << i << " failed";
    }
}

TEST_F(ContainerTrackerTest, SerializeAndParseWithHoles) {
    ContainerTracker tracker;
    for (int i = 1; i < 20; i++) {
        ASSERT_TRUE(tracker.ShouldProcessContainer(i)) << "1. query for " << i << " failed";
    }
    for (int i = 1; i < 17; i++) {
        ASSERT_TRUE(tracker.ProcessedContainer(i));
    }
    for (int i = 19; i < 20; i++) {
        ASSERT_TRUE(tracker.ProcessedContainer(i));
    }

    ContainerTrackerData data;
    tracker.SerializeTo(&data);

    DEBUG("Restart");

    ContainerTracker tracker2;
    tracker2.ParseFrom(data);
    DEBUG("Tracker after restart: " << tracker2.DebugString());
    ASSERT_EQ(17, tracker2.GetNextProcessingContainer());
}

}
