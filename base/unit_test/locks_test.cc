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

#include <gtest/gtest.h>
#include <base/timer.h>
#include <base/locks.h>
#include <test_util/log_assert.h>
#include <base/thread.h>
#include <base/runnable.h>

using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using std::string;
using dedupv1::base::MutexLock;
using dedupv1::base::ReadWriteLock;
using dedupv1::base::Condition;
using dedupv1::base::Walltimer;
using dedupv1::base::ScopedReadWriteLock;

class MutexLockTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(MutexLockTest, Init) {
    MutexLock lock;
}

TEST_F(MutexLockTest, DebugStringWithoutHolder) {
    MutexLock lock;

    string s = lock.DebugString();
    ASSERT_GT(s.size(), 0);
}

bool HoldLock(MutexLock* b, int s) {
    b->AcquireLock();
    sleep(s);
    b->ReleaseLock();
    return true;
}

TEST_F(MutexLockTest, TryWait) {
    MutexLock lock;

    Thread<bool> t1(NewRunnable(&HoldLock, &lock, 10), "mutex-test");
    ASSERT_TRUE(t1.Start());
    sleep(1);

    Walltimer t;
    bool locked = false;
    ASSERT_TRUE(lock.TryAcquireLock(&locked));
    ASSERT_FALSE(locked);
    ASSERT_LE(t.GetTime(), 1000.0);

    ASSERT_TRUE(t1.Join(NULL));
}

TEST_F(MutexLockTest, DebugStringWithHolder) {
    MutexLock lock;
    ASSERT_TRUE(lock.AcquireLock());

    string s = lock.DebugString();
    ASSERT_GT(s.size(), 0);

    ASSERT_TRUE(lock.ReleaseLock());
}

class ReadWriteLockTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(ReadWriteLockTest, Init) {
    ReadWriteLock lock;
}

TEST_F(ReadWriteLockTest, DebugStringWithoutHolder) {
    ReadWriteLock lock;

    string s = lock.DebugString();
    ASSERT_GT(s.size(), 0);
}

TEST_F(ReadWriteLockTest, DebugStringWithHolder) {
    ReadWriteLock lock;
    ASSERT_TRUE(lock.AcquireWriteLock());

    string s = lock.DebugString();
    ASSERT_GT(s.size(), 0);

    ASSERT_TRUE(lock.ReleaseLock());
}

class ScopedReadWriteLockTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();    
};

TEST_F(ScopedReadWriteLockTest, BaseUsageWriteLock) {
    ReadWriteLock rw_lock;
    
    {
        ScopedReadWriteLock scoped_lock(&rw_lock);
        ASSERT_TRUE(scoped_lock.AcquireWriteLock());
    }
    // lock should be released here
    bool locked = false;
    ASSERT_TRUE(rw_lock.TryAcquireWriteLock(&locked));
    ASSERT_TRUE(locked);
    ASSERT_TRUE(rw_lock.ReleaseLock());
}

TEST_F(ScopedReadWriteLockTest, BaseUsageReadLock) {
    ReadWriteLock rw_lock;
    
    {
        ScopedReadWriteLock scoped_lock(&rw_lock);
        ASSERT_TRUE(scoped_lock.AcquireReadLock());
    }
    // lock should be released here
    bool locked = false;
    ASSERT_TRUE(rw_lock.TryAcquireWriteLock(&locked));
    ASSERT_TRUE(locked);
    ASSERT_TRUE(rw_lock.ReleaseLock());
}

TEST_F(ScopedReadWriteLockTest, Unset) {
    ReadWriteLock rw_lock;
    
    {
        ScopedReadWriteLock scoped_lock(&rw_lock);
        ASSERT_TRUE(scoped_lock.AcquireWriteLock());
        
        scoped_lock.Unset();
    }
    // lock should not be released here
    bool locked = false;
    ASSERT_TRUE(rw_lock.TryAcquireWriteLock(&locked));
    ASSERT_FALSE(locked);
    
    ASSERT_TRUE(rw_lock.ReleaseLock());
}

class ConditionTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(ConditionTest, Init) {
    MutexLock l;
    Condition c;

    Walltimer t;
    ASSERT_TRUE(l.AcquireLock());
    ASSERT_EQ(c.ConditionWaitTimeout(&l, 10), dedupv1::base::TIMED_TIMEOUT);

    ASSERT_GE(t.GetTime(), 4000.0);
    ASSERT_LE(t.GetTime(), 6000.0);

    ASSERT_TRUE(l.ReleaseLock());
}

bool FireCondition(Condition* c, int delay) {
    sleep(delay);
    return c->Broadcast();
}

TEST_F(ConditionTest, Fire) {
    MutexLock l;
    Condition c;

    Thread<bool> t1(NewRunnable(&FireCondition, &c, 4), "test");
    ASSERT_TRUE(t1.Start());
    sleep(1);

    Walltimer t;
    ASSERT_TRUE(l.AcquireLock());
    ASSERT_EQ(c.ConditionWaitTimeout(&l, 10), dedupv1::base::TIMED_TRUE);
    ASSERT_LE(t.GetTime(), 8000.0);

    ASSERT_TRUE(l.ReleaseLock());
}
