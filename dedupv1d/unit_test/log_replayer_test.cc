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

#include <unistd.h>

#include <map>
#include <list>
#include <iostream>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <core/log.h>
#include <base/logging.h>
#include <base/runnable.h>
#include <base/thread.h>
#include <test_util/log_assert.h>
#include <test/dedup_system_mock.h>
#include <base/strutil.h>

#include "log_replayer.h"

using dedupv1::base::strutil::ToString;
using dedupv1::base::NewRunnable;
using dedupv1::base::Thread;
using dedupv1::base::ThreadUtil;
using dedupv1::log::Log;
using dedupv1::log::EVENT_TYPE_VOLUME_ATTACH;
using dedupv1::log::EVENT_TYPE_VOLUME_DETACH;
using dedupv1::IdleDetector;
using dedupv1::log::event_type;
using dedupv1::log::LogConsumer;
using testing::Return;

LOGGER("LogReplayerTest");

namespace dedupv1d {

class LogReplayerTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    LogReplayer* log_replayer;
    Log* log;
    IdleDetector* idle;
    dedupv1::MemoryInfoStore info_store;
    MockDedupSystem system;

    virtual void SetUp() {
        this->SetUp(false);
    }

    virtual void SetUp(bool restart) {
        TRACE("SetUp restart is " << restart);
        EXPECT_CALL(system, info_store()).WillRepeatedly(Return(&info_store));

        log = new Log();
        ASSERT_TRUE(log->SetOption("filename", "work/log"));
        ASSERT_TRUE(log->SetOption("max-log-size", "16M"));
        ASSERT_TRUE(log->SetOption("info.type", "sqlite-disk-btree"));
        ASSERT_TRUE(log->SetOption("info.filename", "work/log-info"));
        ASSERT_TRUE(log->SetOption("info.max-item-count", "16"));
        if (restart) {
            ASSERT_TRUE(log->Start(dedupv1::StartContext(dedupv1::StartContext::NON_CREATE), &system));
        } else {
            ASSERT_TRUE(log->Start(dedupv1::StartContext(), &system));
        }

        idle = new IdleDetector();
        ASSERT_TRUE(idle);
        ASSERT_TRUE(idle->Start());

        log_replayer = new LogReplayer();
        ASSERT_TRUE(log_replayer);
    }

    void Restart() {
        TearDown();
        SetUp(true);
    }

    virtual void TearDown() {
        if (log_replayer) {
            delete log_replayer;
            log_replayer = NULL;
        }
        if (log) {
            delete log;
            log = NULL;
        }
        if (idle) {
            delete idle;
            idle = NULL;
        }
    }

    static bool PostEventThreadLoop(::std::tr1::tuple<Log*, int, int> t) {
        Log* log = ::std::tr1::get<0>(t);
        int n = ::std::tr1::get<1>(t);
        int s = ::std::tr1::get<2>(t);

        int i = 0;
        for (i = 0; i < n; i++) {
            DEBUG("Round " << i);
            VolumeAttachedEventData attach_data;
            attach_data.set_volume_id(1);

            if (!log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &attach_data, NULL, NULL, NO_EC)) {
                return false;
            }

            VolumeDetachedEventData detached_data;
            detached_data.set_volume_id(1);
            if (!log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &detached_data, NULL, NULL, NO_EC)) {
                return false;
            }

            if (s > 0) {
                ThreadUtil::Sleep(s);
            } else {
                ThreadUtil::Yield();
            }
        }
        DEBUG("Finished submitting " << n << " events");
        return true;
    }

    static bool ToggleStatesThreadLoop(LogReplayer* log_replayer) {

        CHECK(log_replayer->Resume(), "Cannot change state");
        sleep(1);

        CHECK(log_replayer->Pause(), "Cannot change state");
        sleep(2);

        CHECK(log_replayer->Resume(), "Cannot change state");
        sleep(3);

        CHECK(log_replayer->Pause(), "Cannot change state");
        sleep(4);

        CHECK(log_replayer->Resume(), "Cannot change state");
        sleep(5);

        CHECK(log_replayer->Pause(), "Cannot change state");
        sleep(6);

        CHECK(log_replayer->Resume(), "Cannot change state");
        DEBUG("Finished toggling states");
        return true;
    }
};

TEST_F(LogReplayerTest, Create) {
    // do nothing
}

TEST_F(LogReplayerTest, StartWithoutStop) {
    ASSERT_TRUE(log_replayer->Start(log, idle));

    sleep(2);
}

TEST_F(LogReplayerTest, StartWithStop) {
    ASSERT_TRUE(log_replayer->Start(log, idle));

    sleep(2);

    ASSERT_TRUE(log_replayer->Stop(dedupv1::StopContext::FastStopContext()));
}

TEST_F(LogReplayerTest, ToogleStates) {
    ASSERT_TRUE(log_replayer->Start(log, idle));
    sleep(2);
    ASSERT_TRUE(log_replayer->Run());
    sleep(2);

    ASSERT_TRUE(log_replayer->Resume());
    sleep(1);

    ASSERT_TRUE(log_replayer->Pause());
    sleep(1);

    ASSERT_TRUE(log_replayer->Resume());
    sleep(1);

    ASSERT_TRUE(log_replayer->Pause());
    sleep(1);

    ASSERT_TRUE(log_replayer->Resume());
    sleep(1);

    ASSERT_TRUE(log_replayer->Pause());
    sleep(1);
}

TEST_F(LogReplayerTest, ToogleStatesFast) {
    ASSERT_TRUE(log_replayer->Start(log, idle));
    ASSERT_TRUE(log_replayer->Run());

    ASSERT_TRUE(log_replayer->Pause());
    ASSERT_TRUE(log_replayer->Resume());
    ASSERT_TRUE(log_replayer->Pause());
    ASSERT_TRUE(log_replayer->Resume());
    ASSERT_TRUE(log_replayer->Pause());
    ASSERT_TRUE(log_replayer->Resume());
}

class TestLogConsumer : public LogConsumer {
public:
    std::map<enum event_type, int> events;
    std::list<uint32_t> container_commited;

    virtual bool LogReplay(dedupv1::log::event_type event_type,
                           const LogEventData& event_data,
                           const dedupv1::log::LogReplayContext& context) {
        if (context.replay_mode() != dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG) {
            return true;
        }

        if (events.find(event_type) == events.end()) {
            events.insert(std::pair<enum event_type, int>(event_type, 0));
        }
        events[event_type]++;
        if (event_type == EVENT_TYPE_VOLUME_ATTACH) {
            uint64_t container_id = event_data.volume_attached_event().volume_id();
            container_commited.push_back(container_id);
        }
        return true;
    }
};

TEST_F(LogReplayerTest, Replay) {
    TestLogConsumer context;

    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    ASSERT_TRUE(log_replayer->Start(log, idle));
    ASSERT_TRUE(log_replayer->Run());
    ASSERT_TRUE(log_replayer->Resume());

    Thread<bool> t(NewRunnable(LogReplayerTest::PostEventThreadLoop, ::std::tr1::tuple<Log*, int, int>(log, 10, 2)), "post");
    ASSERT_TRUE(t.Start());
    ASSERT_TRUE(t.Join(NULL));

    sleep(10); // give the log replayer some time to replay the events

    ASSERT_TRUE(log_replayer->Stop(dedupv1::StopContext::FastStopContext()));

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_EQ(context.events[EVENT_TYPE_VOLUME_ATTACH], 10);
    ASSERT_EQ(context.events[EVENT_TYPE_VOLUME_DETACH], 10);
}

TEST_F(LogReplayerTest, RestartAfterReplay) {
    TestLogConsumer context;

    ASSERT_TRUE(log != NULL) << "Log is not available";
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    ASSERT_TRUE(log_replayer->Start(log, idle));
    ASSERT_TRUE(log_replayer->Run());
    ASSERT_TRUE(log_replayer->Resume());

    Thread<bool> t(NewRunnable(LogReplayerTest::PostEventThreadLoop, ::std::tr1::tuple<Log*, int, int>(log, 10, 2)), "post");
    ASSERT_TRUE(t.Start());
    ASSERT_TRUE(t.Join(NULL));

    sleep(10);

    ASSERT_TRUE(log_replayer->Stop(dedupv1::StopContext::FastStopContext()));

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_EQ(context.events[EVENT_TYPE_VOLUME_ATTACH], 10);
    ASSERT_EQ(context.events[EVENT_TYPE_VOLUME_DETACH], 10);

    Restart();
}

TEST_F(LogReplayerTest, ReplayConcurrentToggleStates) {
    TestLogConsumer context;

    ASSERT_TRUE(log->RegisterConsumer("context",&context));

    ASSERT_TRUE(log_replayer->Start(log, idle));
    ASSERT_TRUE(log_replayer->Run());

    Thread<bool> t(NewRunnable(&LogReplayerTest::PostEventThreadLoop, ::std::tr1::tuple<Log*, int, int>(log, 10, 2)), "post");
    Thread<bool> t2(NewRunnable(&LogReplayerTest::ToggleStatesThreadLoop, log_replayer), "toogle");

    ASSERT_TRUE(t.Start());
    ASSERT_TRUE(t2.Start());

    ASSERT_TRUE(t.Join(NULL));
    ASSERT_TRUE(t2.Join(NULL));

    sleep(30);

    DEBUG("Final Stop");
    ASSERT_TRUE(log_replayer->Stop(dedupv1::StopContext::FastStopContext()));

    ASSERT_TRUE(log->UnregisterConsumer("context"));

    ASSERT_EQ(context.events[EVENT_TYPE_VOLUME_ATTACH], 10);
    ASSERT_EQ(context.events[EVENT_TYPE_VOLUME_DETACH], 10);
}

TEST_F(LogReplayerTest, LotsOfEvents) {
    TestLogConsumer context;

    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    ASSERT_TRUE(log_replayer->Start(log, idle));
    ASSERT_TRUE(log_replayer->Run());
    ASSERT_TRUE(log_replayer->Resume());

    Thread<bool> t(NewRunnable(LogReplayerTest::PostEventThreadLoop, ::std::tr1::tuple<Log*, int, int>(log, 512, 0)),"post");
    ASSERT_TRUE(t.Start());
    ASSERT_TRUE(t.Join(NULL));

    sleep(5);
    ASSERT_TRUE(log_replayer->Stop(dedupv1::StopContext::FastStopContext()));

    ASSERT_TRUE(log->UnregisterConsumer("context"));

    INFO("Replayed event count: " << (context.events[EVENT_TYPE_VOLUME_ATTACH] + context.events[EVENT_TYPE_VOLUME_DETACH]));
    ASSERT_GT(context.events[EVENT_TYPE_VOLUME_ATTACH], 0);
    ASSERT_GT(context.events[EVENT_TYPE_VOLUME_DETACH], 0);
}

}
