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
#include <sys/resource.h>
#include <base/threadpool.h>
#include <base/runnable.h>
#include <test_util/log_assert.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <base/multi_signal_condition.h>

#ifndef NVALGRIND
#include <valgrind.h>
#endif

LOGGER("ThreadpoolTest");

using dedupv1::base::strutil::ToString;

namespace dedupv1 {
namespace base {

class ThreadpoolTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Threadpool t;

    virtual void SetUp() {
    }

    virtual void TearDown() {
        ASSERT_TRUE(t.Stop());
    }
};

TEST_F(ThreadpoolTest, Create) {

}

TEST_F(ThreadpoolTest, StartWithoutSizeParameter) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_FALSE(t.Start()) << "Start should fail without configuration";
}

TEST_F(ThreadpoolTest, StartWithSizeOne) {
    ASSERT_TRUE(t.SetOption("size", "1"));
    ASSERT_TRUE(t.Start());
}

TEST_F(ThreadpoolTest, StartWithSizeTen) {
    ASSERT_TRUE(t.SetOption("size", "10"));
    ASSERT_TRUE(t.Start());
}

TEST_F(ThreadpoolTest, StopWithoutItems) {
    ASSERT_TRUE(t.SetOption("size", "10"));
    ASSERT_TRUE(t.Start());
    ASSERT_TRUE(t.Stop());
}

TEST_F(ThreadpoolTest, StopWithoutStart) {
    ASSERT_TRUE(t.Stop());
}

class ThreadpoolTestRunnable {
public:
    bool started;
    int count_;
    tbb::tick_count run_tick;
    int sleep_time_;
    MultiSignalCondition* multi_signal_condition_;
    Barrier* barrier_;

    ThreadpoolTestRunnable(int sleep_time = 5, MultiSignalCondition* multi_signal_condition = NULL, Barrier* barrier = NULL) {
        started = false;
        count_ = 0;
        sleep_time_ = sleep_time;
        barrier_ = barrier;
        multi_signal_condition_ = multi_signal_condition;
    }

    virtual bool Runner() {
        TRACE("Execute task");
        started = true;
        run_tick = tbb::tick_count::now();
        ThreadUtil::Sleep(sleep_time_);
        count_++;

        if (multi_signal_condition_) {
            if (!multi_signal_condition_->Signal()) {
                return false;
            }
        }
        // wait until the barrier is visited by the correct number of threads
        if (barrier_) {
            if (!barrier_->Wait()) {
                return false;
            }
        }

        return true;
    }

    inline int count() const {
        return count_;
    }
};

TEST_F(ThreadpoolTest, StartWithSize256) {
    ASSERT_TRUE(t.SetOption("size", "256"));
    ASSERT_TRUE(t.Start());

    ThreadpoolTestRunnable r;

    Future<bool>* f = t.Submit(NewRunnable<bool>(&r, &ThreadpoolTestRunnable::Runner));
    EXPECT_TRUE(f);
    if (f) {
        EXPECT_TRUE(f->Wait());
        delete f;
        f = NULL;
    }
    sleep(1);
    EXPECT_TRUE(t.Stop());
    EXPECT_TRUE(r.started);
}

TEST_F(ThreadpoolTest, SimpleSubmit) {
    ASSERT_TRUE(t.SetOption("size", "10"));
    ASSERT_TRUE(t.Start());

    ThreadpoolTestRunnable r;

    Future<bool>* f = t.Submit(NewRunnable<bool>(&r, &ThreadpoolTestRunnable::Runner));
    ASSERT_TRUE(f);
    delete f;
    f = NULL;

    sleep(1);
    ASSERT_TRUE(t.Stop());

    ASSERT_TRUE(r.started);
}

TEST_F(ThreadpoolTest, Priority) {
    ASSERT_TRUE(t.SetOption("size", "1"));
    ASSERT_TRUE(t.Start());

    ThreadpoolTestRunnable r1;
    ThreadpoolTestRunnable r2;

    Future<bool>* f = t.Submit(NewRunnable<bool>(&r1, &ThreadpoolTestRunnable::Runner), Threadpool::BACKGROUND_PRIORITY);
    ASSERT_TRUE(f);
    delete f;
    f = NULL;

    f = t.Submit(NewRunnable<bool>(&r2, &ThreadpoolTestRunnable::Runner), Threadpool::HIGH_PRIORITY);
    ASSERT_TRUE(f);
    delete f;
    f = NULL;

    sleep(15);
    ASSERT_TRUE(t.Stop());

    ASSERT_TRUE(r1.started);
    ASSERT_TRUE(r2.started);
}

/**
 * This is simply a really bad situation to be in
 */
TEST_F(ThreadpoolTest, TooMuchThreads) {
    bool should_run = true;

#ifndef NVALGRIND
    should_run = !(RUNNING_ON_VALGRIND);
#endif
#ifdef __APPLE__
    should_run = false;
#endif
    // memory limit where the creatio of 1000 threads should fail. Must be set by experiments
    int thread_count = 1024;
    int memory_limit = 512 * 1024 * 1024;

    if (!should_run) {
        INFO("Skip this");
        return;
    }

    EXPECT_LOGGING(dedupv1::test::ERROR).Repeatedly();

    struct rlimit limit;
    memset(&limit, 0, sizeof(limit));
    struct rlimit oldlimit;
    memset(&limit, 0, sizeof(oldlimit));

    ASSERT_EQ(getrlimit(RLIMIT_AS, &oldlimit), 0);
    memcpy(&limit, &oldlimit, sizeof(limit));
    limit.rlim_cur = memory_limit;
    ASSERT_EQ(setrlimit(RLIMIT_AS, &limit), 0);

    ASSERT_TRUE(t.SetOption("size", ToString(thread_count)));
    ASSERT_FALSE(t.Start());

    ASSERT_EQ(setrlimit(RLIMIT_AS, &oldlimit), 0);
}

TEST_F(ThreadpoolTest, Reject) {
    EXPECT_LOGGING(dedupv1::test::WARN).Once();

    ASSERT_TRUE(t.SetOption("size", "1"));
    ASSERT_TRUE(t.SetOption("queue-size", "1"));
    ASSERT_TRUE(t.Start());

    MultiSignalCondition c1(1);
    Barrier b1(2);
    Barrier b2(2);
    ThreadpoolTestRunnable blocking1(0, &c1, &b1);
    ThreadpoolTestRunnable blocking2(0, NULL, &b2);
    ThreadpoolTestRunnable blocked;

    // job 1
    Future<bool>* f1 = t.Submit(NewRunnable<bool>(&blocking1, &ThreadpoolTestRunnable::Runner));
    EXPECT_TRUE(f1);

    // wait so that f1 is running. job 1 will keep running until b.Wait() is called
    EXPECT_TRUE(c1.Wait());

    // job 2 will always be submitted into the queue
    Future<bool>* f2 = t.Submit(NewRunnable<bool>(&blocking2, &ThreadpoolTestRunnable::Runner));
    EXPECT_TRUE(f2);

    // we know now of sure that job 2 is in the queue, therefore job 3 should be rejected.
    // the overflow check currently only considers the queue.
    Runnable<bool>* secondRunnable = NewRunnable<bool>(&blocked, &ThreadpoolTestRunnable::Runner);
    EXPECT_FALSE(t.Submit(secondRunnable, Threadpool::BACKGROUND_PRIORITY, Threadpool::REJECT));
    delete secondRunnable;
    secondRunnable = NULL;

    // here we release job 1
    EXPECT_TRUE(b1.Wait());
    // here we release job 2
    EXPECT_TRUE(b2.Wait());

    if (f1) {
        // Wait here until job 1 is finished
        EXPECT_TRUE(f1->Wait());
        delete f1;
        f1 = NULL;
    }
    if (f2) {
        // Wait here until job 2 is finished
        EXPECT_TRUE(f2->Wait());
        delete f2;
        f2 = NULL;
    }
    ThreadUtil::Sleep(5);
    EXPECT_TRUE(t.Stop());
}

TEST_F(ThreadpoolTest, CallerRuns) {
    ASSERT_TRUE(t.SetOption("size", "1"));
    ASSERT_TRUE(t.SetOption("queue-size", "1"));
    ASSERT_TRUE(t.Start());

    MultiSignalCondition c1(1);
    Barrier b1(2);
    Barrier b2(2);
    ThreadpoolTestRunnable blocking1(0, &c1, &b1);
    ThreadpoolTestRunnable blocking2(0, NULL, &b2);

    ThreadpoolTestRunnable blocked(2);

    // job 1
    Future<bool>* f1 = t.Submit(NewRunnable<bool>(&blocking1, &ThreadpoolTestRunnable::Runner));
    EXPECT_TRUE(f1);

    // wait so that f1 is running. job 1 will keep running until b.Wait() is called
    EXPECT_TRUE(c1.Wait());

    // job 2 will always be submitted into the queue
    Future<bool>* f2 = t.Submit(NewRunnable<bool>(&blocking2, &ThreadpoolTestRunnable::Runner));
    EXPECT_TRUE(f2);

    // job 3 will should be started in caller runs mode
    Future<bool>* f3 = t.Submit(NewRunnable<bool>(&blocked, &ThreadpoolTestRunnable::Runner),
        Threadpool::BACKGROUND_PRIORITY, Threadpool::CALLER_RUNS);
    EXPECT_TRUE(f3);

    // here we release job 1
    EXPECT_TRUE(b1.Wait());
    // here we release job 2
    EXPECT_TRUE(b2.Wait());

    if (f1) {
        // Wait here until job 1 is finished
        EXPECT_TRUE(f3->Wait());
        delete f1;
        f1 = NULL;
    }
    if (f2) {
        // Wait here until job 2 is finished
        EXPECT_TRUE(f3->Wait());
        delete f2;
        f2 = NULL;
    }
    if (f3) {
        // Wait here until job 3 is finished
        EXPECT_TRUE(f3->Wait());
        delete f3;
        f3 = NULL;
    }
    ThreadUtil::Sleep(5);
    EXPECT_TRUE(t.Stop());

    EXPECT_TRUE(blocking1.started);
    EXPECT_TRUE(blocking2.started);
    EXPECT_TRUE(blocked.started);
}

class MinimalThreadpoolTestRunnable {
public:
    MultiSignalCondition* barrier_;
    tbb::tick_count run_tick;

    MinimalThreadpoolTestRunnable(MultiSignalCondition* barrier = NULL) {
        barrier_ = barrier;
    }
    virtual bool Runner() {
        TRACE("Execute task");
        sleep(0);
        run_tick = tbb::tick_count::now();
        barrier_->Signal();
        return true;
    }
};

TEST_F(ThreadpoolTest, SubmitWithoutFuture) {
    ASSERT_TRUE(t.SetOption("size", "1"));
    ASSERT_TRUE(t.Start());

    int count = 4 * 1024;
    MultiSignalCondition barrier(count);
    ThreadpoolTestRunnable r(0, &barrier);
    for (int i = 0; i < count; i++) {
        ASSERT_TRUE(t.SubmitNoFuture(NewRunnable<bool>(&r, &ThreadpoolTestRunnable::Runner)));
    }
    tbb::tick_count t1 = tbb::tick_count::now();

    DEBUG("Start waiting");
    barrier.Wait();
    DEBUG("Stopped waiting");

    tbb::tick_count t2 = tbb::tick_count::now();
    DEBUG("Waiting time: " << (t2 - t1).seconds() * 1000 << "ms");
    ASSERT_TRUE(t.Stop());
}

}
}
