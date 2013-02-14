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

#include <core/chunk_locks.h>
#include <base/thread.h>
#include <base/runnable.h>
#include <test/log_assert.h>

namespace dedupv1 {
namespace chunkindex {

class ChunkLocksTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    ChunkLocks* locks_;

    virtual void SetUp() {
        locks_ = new ChunkLocks();
    }

    virtual void TearDown() {
        if (locks_) {
            delete locks_;
        }
    }
};

TEST_F(ChunkLocksTest, Init) {
}

TEST_F(ChunkLocksTest, Start) {
    ASSERT_TRUE(locks_->Start(dedupv1::StartContext()));
}

TEST_F(ChunkLocksTest, SetOption) {
    ASSERT_TRUE(locks_->SetOption("count", "1k"));
    ASSERT_TRUE(locks_->Start(dedupv1::StartContext()));
}

bool TryToLock(ChunkLocks* locks, uint64_t fp) {
    bool locked = false;
    locks->TryLock(&fp, sizeof(fp), &locked);
    return locked;
}

TEST_F(ChunkLocksTest, TryLock) {
    ASSERT_TRUE(locks_->Start(dedupv1::StartContext()));

    uint64_t fp = 12;

    bool locked = false;
    ASSERT_TRUE(locks_->TryLock(&fp, sizeof(fp), &locked));
    ASSERT_TRUE(locked);

    ASSERT_FALSE(dedupv1::base::Thread<bool>::RunThread(dedupv1::base::NewRunnable(&TryToLock, locks_, fp)));

    ASSERT_TRUE(locks_->Unlock(&fp, sizeof(fp)));
}

}
}
