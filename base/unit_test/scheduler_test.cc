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

#include <base/threadpool.h>
#include <base/scheduler.h>
#include <base/runnable.h>
#include <test_util/log_assert.h>
#include <base/logging.h>

LOGGER("SchedulerTest");

namespace dedupv1 {
namespace base {

class SchedulerTestCallback {
private:
    int runs_;
    int abort_runs_;
public:
    SchedulerTestCallback() {
        runs_ = 0;
        abort_runs_ = 0;
    }

    bool Run(const ScheduleContext& context) {
        if (!context.abort()) {
            runs_++;
            DEBUG("Schedules task runs: " << runs_);
        } else {
            abort_runs_++;
            DEBUG("Schedules task abort: " << abort_runs_);
        }
        return true;
    }

    int runs() const {
        return runs_;
    }

    int abort_runs() const {
        return abort_runs_;
    }
};

class SchedulerTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Threadpool t;
    Scheduler s;

    SchedulerTestCallback c;

    virtual void SetUp() {
    }

    virtual void TearDown() {
        ASSERT_TRUE(s.Stop());
        ASSERT_TRUE(t.Stop());
    }

    bool SubmitTestCallback() {
        Callback1<bool, const ScheduleContext&>* callback = dedupv1::base::NewCallback(&c, &SchedulerTestCallback::Run);
        ScheduleOptions options(1);
        bool r = s.Submit("test", options, callback);
        if (!r) {
            delete callback;
        }
        return r;
    }
};

TEST_F(SchedulerTest, Create) {

}

TEST_F(SchedulerTest, Start) {
    ASSERT_TRUE(t.SetOption("size", "10"));
    ASSERT_TRUE(t.Start());

    ASSERT_TRUE(s.Start(&t));
}

TEST_F(SchedulerTest, StartRun) {
    ASSERT_TRUE(t.SetOption("size", "10"));
    ASSERT_TRUE(t.Start());

    ASSERT_TRUE(s.Start(&t));
    ASSERT_TRUE(s.Run());
}

TEST_F(SchedulerTest, StartStop) {
    ASSERT_TRUE(t.SetOption("size", "10"));
    ASSERT_TRUE(t.Start());

    ASSERT_TRUE(s.Start(&t));
    sleep(4);
    ASSERT_TRUE(s.Stop());
}

TEST_F(SchedulerTest, StartRunStop) {
    ASSERT_TRUE(t.SetOption("size", "10"));
    ASSERT_TRUE(t.Start());

    ASSERT_TRUE(s.Start(&t));
    ASSERT_TRUE(s.Run());
    sleep(4);
    ASSERT_TRUE(s.Stop());
}

TEST_F(SchedulerTest, SubmitBeforeStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_FALSE(SubmitTestCallback());
}

TEST_F(SchedulerTest, IsScheduled) {
    ASSERT_TRUE(t.SetOption("size", "10"));
    ASSERT_TRUE(t.Start());

    ASSERT_TRUE(s.Start(&t));

    ASSERT_TRUE(s.IsScheduled("test").valid());
    ASSERT_FALSE(s.IsScheduled("test").value());

    ASSERT_TRUE(SubmitTestCallback());

    ASSERT_TRUE(s.IsScheduled("test").valid());
    ASSERT_TRUE(s.IsScheduled("test").value());
}

TEST_F(SchedulerTest, SubmitWithoutRemove) {
    ASSERT_TRUE(t.SetOption("size", "10"));
    ASSERT_TRUE(t.Start());

    ASSERT_TRUE(s.Start(&t));

    ASSERT_TRUE(SubmitTestCallback());

    ASSERT_TRUE(s.Run());
    sleep(4);
    ASSERT_TRUE(s.Stop());
    ASSERT_GT(c.runs(), 2);
    ASSERT_EQ(c.abort_runs(), 1);
}

TEST_F(SchedulerTest, SubmitWithRemove) {
    ASSERT_TRUE(t.SetOption("size", "10"));
    ASSERT_TRUE(t.Start());

    ASSERT_TRUE(s.Start(&t));

    ASSERT_TRUE(SubmitTestCallback());

    ASSERT_TRUE(s.Run());
    sleep(4);
    ASSERT_TRUE(s.Remove("test"));
    sleep(4);
    ASSERT_TRUE(s.Stop());
    ASSERT_GT(c.runs(), 2);
    ASSERT_LE(c.runs(), 4);
    ASSERT_EQ(c.abort_runs(), 0);
}

}
}
