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

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

#include <map>
#include <string>
#include <list>

#include <gtest/gtest.h>

#include <core/dedup_system.h>
#include <base/strutil.h>
#include <base/memory.h>
#include <core/storage.h>
#include <core/dedup.h>
#include <base/locks.h>
#include <base/option.h>
#include <base/crc32.h>
#include <base/thread.h>
#include <base/runnable.h>

#include <core/log_consumer.h>
#include <core/log.h>
#include <base/index.h>
#include <base/logging.h>
#include <test_util/log_assert.h>
#include <test/dedup_system_mock.h>
#include <cryptopp/cryptlib.h>
#include <cryptopp/rng.h>

using std::map;
using std::list;
using std::vector;
using std::string;
using dedupv1::base::crc;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::Split;
using dedupv1::base::PersistentIndex;
using dedupv1::base::Index;
using dedupv1::base::DELETE_OK;
using dedupv1::StartContext;
using testing::Return;
using dedupv1::base::Option;
using dedupv1::base::NewRunnable;
using dedupv1::base::Thread;

using namespace CryptoPP;
LOGGER("LogTest");

namespace dedupv1 {
namespace log {

class LogTestLogConsumer : public LogConsumer {
public:
    map<enum event_type, uint32_t> type_map;
    map<enum replay_mode, uint32_t> replay_mode_map;
    list<LogEventData> value_list;
    list<enum event_type> type_list_;

    uint32_t waiting_time_;

    LogTestLogConsumer() {
        waiting_time_ = 0;
    }

    virtual bool LogReplay(enum event_type event_type, const LogEventData& event_value,
                           const LogReplayContext& context) {
        replay_mode_map[context.replay_mode()]++;
        type_map[event_type]++;

        value_list.push_back(event_value);
        type_list_.push_back(event_type);

        if (waiting_time_ > 0) {
            sleep(waiting_time_);
        }

        DEBUG("Replay event " << Log::GetReplayModeName(context.replay_mode()) << " - " << Log::GetEventTypeName(
                event_type));
        return true;
    }

    void set_waiting_time(uint32_t s) {
        waiting_time_ = s;
    }

    void Clear() {
        type_map.clear();
        replay_mode_map.clear();
    }

    const list<enum event_type>& type_list() const {
        return type_list_;
    }
};

/**
 * Testing the operations log
 */
class LogTest : public testing::TestWithParam<std::tr1::tuple<std::string, int> > {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1::MemoryInfoStore info_store;
    MockDedupSystem system;

    Log* log;
    bool use_size_limit;

    static const enum event_type kEventTypeTestLarge;
    static const enum event_type kEventTypeTestLarge2;

    const string& config_file() {
        return std::tr1::get<0>(GetParam());
    }

    int message_size() {
        return std::tr1::get<1>(GetParam());
    }

    virtual void SetUp() {
        use_size_limit = false;
        log = NULL;

        dedupv1::log::EventTypeInfo::RegisterEventTypeInfo(kEventTypeTestLarge, dedupv1::log::EventTypeInfo(
                LogEventData::kMessageDataFieldNumber, true));
        dedupv1::log::EventTypeInfo::RegisterEventTypeInfo(kEventTypeTestLarge2, dedupv1::log::EventTypeInfo(
                LogEventData::kMessageDataFieldNumber, true));

        EXPECT_CALL(system, info_store()).WillRepeatedly(Return(&info_store));
    }

    void FillMessage(MessageData* message) {
        char* data = new char[message_size()];
        memset(data, 1, message_size());
        message->set_message(data, message_size());
        delete[] data;
    }

    virtual void TearDown() {
        if (log) {
            bool started = false;
            if (log->state() == Log::LOG_STATE_RUNNING || log->state() == Log::LOG_STATE_STARTED) {
                Option<bool> b = log->CheckLogId();
                ASSERT_TRUE(b.valid());
                ASSERT_TRUE(b.value());
                started = true;
            }
            if (log->wasStarted_) {
                started = true;
            }

            ASSERT_TRUE(log->Close());

            // Here we check if it is possible to reopen the log
            log = CreateLog(config_file());
            if (use_size_limit) {
                ASSERT_TRUE(log->SetOption( "max-log-size", "64K"));
            }
            ASSERT_TRUE(log);
            if (started) {
                EXPECT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system));
            } else {
                EXPECT_TRUE(log->Start(StartContext(), &system));
            }
            ASSERT_TRUE(log->Close());
        }
    }

    void StartSizeLimitedLog(Log* log, bool start = true, bool crashed = false, bool restart = false) {
        use_size_limit = true;
        ASSERT_TRUE(log->SetOption( "max-log-size", "64K"));
        if (start) {
            StartContext start_context;
            if (crashed) {
                start_context.set_crashed(true);
            }
            if (restart) {
                start_context.set_create(StartContext::NON_CREATE);
            }
            ASSERT_TRUE(log->Start(start_context, &system));
        }
    }

    Log* CreateLog(const string& config_option) {
        vector<string> options;
        CHECK_RETURN(Split(config_option, ";", &options), NULL, "Failed to split: " << config_option);

        Log* log = new Log();
        for (size_t i = 0; i < options.size(); i++) {
            string option_name;
            string option;
            CHECK_RETURN(Split(options[i], "=", &option_name, &option), NULL, "Failed to split " << options[i]);
            CHECK_RETURN(log->SetOption(option_name, option), NULL, "Failed set option: " << options[i]);
        }
        return log;
    }

    dedupv1::base::PersistentIndex* OpenLogIndex(const string& config_option) {
        vector<string> options;
        CHECK_RETURN(Split(config_option, ";", &options), NULL, "Failed to split: " << config_option);

        dedupv1::base::PersistentIndex* index = NULL;

        for (size_t i = 0; i < options.size(); i++) {
            string option_name;
            string option;
            CHECK_RETURN(Split(options[i], "=", &option_name, &option), NULL, "Failed to split " << options[i]);
            if (option_name == "type") {
                CHECK_RETURN(index == NULL, NULL, "Index already created");
                Index* i = Index::Factory().Create(option);
                CHECK_RETURN(i, NULL, "Failed to create index");
                index = i->AsPersistentIndex();
                CHECK_RETURN(index, NULL, "Index is not persistent");
                CHECK_RETURN(index->SetOption("width", ToString(Log::kDefaultLogEntryWidth)), NULL, "Failed to set width");
            }
            if (option_name == "max-log-size") {
                if (index == NULL) {
                    Index* i = Index::Factory().Create(Log::kDefaultLogIndexType);
                    CHECK_RETURN(i, NULL, "Failed to create index");
                    index = i->AsPersistentIndex();
                    CHECK_RETURN(index, NULL, "Index is not persistent");
                    CHECK_RETURN(index->SetOption("width", ToString(Log::kDefaultLogEntryWidth)), NULL, "Failed to set width");
                }
                CHECK_RETURN(index->SetOption("size", option), NULL, "Failed set option: " << options[i]);
            }
            if (option_name == "filename") {
                if (index == NULL) {
                    Index* i = Index::Factory().Create(Log::kDefaultLogIndexType);
                    CHECK_RETURN(i, NULL, "Failed to create index");
                    index = i->AsPersistentIndex();
                    CHECK_RETURN(index, NULL, "Index is not persistent");
                    CHECK_RETURN(index->SetOption("width", ToString(Log::kDefaultLogEntryWidth)), NULL, "Failed to set width");
                }
                CHECK_RETURN(index->SetOption(option_name, option), NULL, "Failed set option: " << options[i]);
            }
        }
        return index;
    }
};

const enum event_type LogTest::kEventTypeTestLarge = EVENT_TYPE_NEXT_ID;
const enum event_type LogTest::kEventTypeTestLarge2 = static_cast<enum event_type>(EVENT_TYPE_NEXT_ID + 1);

INSTANTIATE_TEST_CASE_P(Log,
    LogTest,
    ::testing::Combine(
        ::testing::Values("max-log-size=1M;filename=work/test-log;info.type=sqlite-disk-btree;info.filename=work/test-log-info;info.max-item-count=16",
            "max-log-size=1M;filename=work/test-log1;filename=work/test-log2;info.type=sqlite-disk-btree;info.filename=work/test-log-info;info.max-item-count=16")
        ,
        ::testing::Values(10, 2 * 1024)));

TEST_P(LogTest, Init) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
}

TEST_P(LogTest, Start) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    ASSERT_EQ(log->consumer_count(), 0U);
}

TEST_P(LogTest, DoubleStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    ASSERT_FALSE(log->Start(StartContext(), &system));
}

TEST_P(LogTest, Restart) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));
    SystemStartEventData event_data;
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_SYSTEM_START, &event_data, NULL, NULL, NO_EC));
    ASSERT_TRUE(log->Close());

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system));
}

TEST_P(LogTest, SimpleCommit) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    VolumeDetachedEventData event_data;
    event_data.set_volume_id(1);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &event_data, NULL, NULL, NO_EC));
}

TEST_P(LogTest, EmptyCommit) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_NONE, NULL, NULL, NULL, NO_EC));
}

TEST_P(LogTest, RegisterAndUnregister) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer c1;
    LogTestLogConsumer c2;

    ASSERT_TRUE(log->RegisterConsumer("c1", &c1));
    ASSERT_TRUE(log->RegisterConsumer("c2", &c2));

    ASSERT_EQ(log->consumer_count(), 2U);

    ASSERT_TRUE(log->UnregisterConsumer("c1"));
    ASSERT_TRUE(log->UnregisterConsumer("c2"));

    ASSERT_EQ(log->consumer_count(), 0U);
}

/**
 * Tests if the IsRegistered method works correctly
 */
TEST_P(LogTest, IsRegistered) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer c1;

    ASSERT_FALSE(log->IsRegistered("c1").value());
    ASSERT_TRUE(log->RegisterConsumer("c1", &c1));
    ASSERT_TRUE(log->IsRegistered("c1").value());

    ASSERT_TRUE(log->UnregisterConsumer("c1"));
    ASSERT_FALSE(log->IsRegistered("c1").value());
}

/**
 * Tests if it is able to register a consumer before the log is started
 */
TEST_P(LogTest, RegisterBeforeStart) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);

    LogTestLogConsumer c1;
    LogTestLogConsumer c2;

    ASSERT_TRUE(log->RegisterConsumer("c1", &c1));
    ASSERT_TRUE(log->RegisterConsumer("c2", &c2));

    ASSERT_EQ(log->consumer_count(), 2U);

    ASSERT_TRUE(log->UnregisterConsumer("c1"));
    ASSERT_TRUE(log->UnregisterConsumer("c2"));

    ASSERT_EQ(log->consumer_count(), 0U);
}

TEST_P(LogTest, ReplayWithoutConsumer) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    VolumeAttachedEventData message;
    message.set_volume_id(19);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NULL, NULL, NO_EC));
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());
}

TEST_P(LogTest, ReplayDirect) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    VolumeAttachedEventData message;
    message.set_volume_id(1);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NULL, NULL, NO_EC));
    VolumeDetachedEventData message2;
    message2.set_volume_id(1);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &message2, NULL, NULL, NO_EC));

    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRECT], 2U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 1U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_DETACH], 1U);
    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, ReplayDirectThread) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));
    ASSERT_TRUE(log->Run());

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    VolumeAttachedEventData message;
    message.set_volume_id(1);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NULL, NULL, NO_EC));
    VolumeDetachedEventData message2;
    message2.set_volume_id(1);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &message2, NULL, NULL, NO_EC));

    sleep(5);

    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRECT], 2U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 1U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_DETACH], 1U);
    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

/**
 * Tests a that the ordering of the direct replay is correct
 */
TEST_P(LogTest, ReplayDirectThreadRace) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));
    ASSERT_TRUE(log->Run());

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    VolumeAttachedEventData message;
    message.set_volume_id(1);

    LogEventData event_data;
    event_data.mutable_volume_attached_event()->CopyFrom(message);

    LogReplayEntry replay_entry1(2, EVENT_TYPE_VOLUME_ATTACH, event_data, false, 1);
    log->replay_event_queue_.push(replay_entry1);

    LogReplayEntry replay_entry2(3, EVENT_TYPE_VOLUME_ATTACH, event_data, false, 1);
    log->replay_event_queue_.push(replay_entry2);

    LogReplayEntry replay_entry3(1, EVENT_TYPE_VOLUME_ATTACH, event_data, false, 1);
    log->replay_event_queue_.push(replay_entry3);

    LogReplayEntry replay_entry4(4, EVENT_TYPE_VOLUME_ATTACH, event_data, false, 1);
    log->replay_event_queue_.push(replay_entry4);

    sleep(5);

    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRECT], 4U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 4U);
    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, MovingReplayID) {
    // EXPECT_LOGGING(dedupv1::test::WARN).Once();
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    VolumeAttachedEventData message;
    message.set_volume_id(1);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NULL, NULL, NO_EC));
    VolumeDetachedEventData message2;
    message2.set_volume_id(1);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &message2, NULL, NULL, NO_EC));

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;
    context.Clear();

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    uint64_t replay_log_id = 0;
    ASSERT_EQ(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, &replay_log_id, NULL), LOG_REPLAY_OK);

    ASSERT_GT(log->replay_id(), replay_log_id);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, ReplayCrash) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    VolumeAttachedEventData message;
    message.set_volume_id(1);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NULL, NULL, NO_EC));
    VolumeDetachedEventData message2;
    message2.set_volume_id(1);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &message2, NULL, NULL, NO_EC));

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;
    context.Clear();

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_GT(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 0U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 1U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_DETACH], 1U);

    ASSERT_EQ(context.type_map[EVENT_TYPE_REPLAY_STARTED], 2U); // 1 DIRECT, 1 CRASH
    ASSERT_EQ(context.type_map[EVENT_TYPE_REPLAY_STOPPED], 1U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_EMPTY], 1U);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

/**
 * Simple log replay function that is started in a different thread
 * Is used by ForbidParallelReplay.
 */
bool ParallelLogReplay(Log* log) {

    if (!log->ReplayStart(EVENT_REPLAY_MODE_REPLAY_BG, false)) {
        return false;
    }
    if (!log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)) {
        log->ReplayStop(EVENT_REPLAY_MODE_REPLAY_BG, false);
        return false;
    }
    if (!log->ReplayStop(EVENT_REPLAY_MODE_REPLAY_BG, true)) {
        return false;
    }
    return true;
}

TEST_P(LogTest, ForbidParallelReplay) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("Log is already replaying").Once();

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 4; i++) {
        VolumeAttachedEventData message;
        message.set_volume_id(1);
        ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NULL, NULL, NO_EC));
        VolumeDetachedEventData message2;
        message2.set_volume_id(1);
        ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &message2, NULL, NULL, NO_EC));
    }
    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;
    context.Clear();

    context.set_waiting_time(1);
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    Thread<bool> t1(NewRunnable(log, &Log::PerformFullReplayBackgroundMode, true), "log 1");
    ASSERT_TRUE(t1.Start());
    dedupv1::base::ThreadUtil::Sleep(100, dedupv1::base::ThreadUtil::MILLISECONDS);

    Thread<bool> t2(NewRunnable(&ParallelLogReplay, log), "log 2");
    ASSERT_TRUE(t2.Start());

    bool result;
    ASSERT_TRUE(t1.Join(&result));
    ASSERT_TRUE(result);

    ASSERT_TRUE(t2.Join(&result));
    ASSERT_FALSE(result);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, ReplayWithConsumer) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    ASSERT_TRUE(log->Start(StartContext(), &system));

    VolumeAttachedEventData message;
    message.set_volume_id(1);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NULL, NULL, NO_EC));
    VolumeDetachedEventData message2;
    message2.set_volume_id(1);
    ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &message2, NULL, NULL, NO_EC));

    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());
    ASSERT_GT(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 0U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 2U); // 1 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_DETACH], 2U); // 1 DIRECT, 1 BG

    ASSERT_EQ(context.type_map[EVENT_TYPE_REPLAY_STARTED], 2U); // 1 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[EVENT_TYPE_REPLAY_STOPPED], 1U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_EMPTY], 1U);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, ReplayDifferentNumberOfEvents) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    ASSERT_TRUE(log->Start(StartContext(), &system));

    EXPECT_LOGGING(dedupv1::test::WARN).Times(1);

    for (int i = 0; i < 16; i++) {
        VolumeAttachedEventData message;
        message.set_volume_id(i);
        ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NULL, NULL, NO_EC));
    }
    // ASSERT_EQ(log->log_size(), 17U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRECT], 17U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRTY_START], 0U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 0U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 16U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_NEW], 1U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_EMPTY], 0U);

    uint32_t number_replay = 100;

    ASSERT_EQ(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 0, NULL, &number_replay), LOG_REPLAY_OK);
    ASSERT_EQ(number_replay, 0U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRECT], 17U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRTY_START], 0U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 0U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 16U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_NEW], 1U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_EMPTY], 0U);

    ASSERT_EQ(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, &number_replay), LOG_REPLAY_OK);
    ASSERT_EQ(number_replay, 1U);
    // ASSERT_EQ(log->log_size(), 16U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRECT], 17U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRTY_START], 0U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 1U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 16U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_NEW], 2U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_EMPTY], 0U);

    number_replay = 0;
    ASSERT_EQ(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, &number_replay), LOG_REPLAY_OK);
    ASSERT_EQ(number_replay, 1U);
    // ASSERT_EQ(log->log_size(), 15U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRECT], 17U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRTY_START], 0U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 2U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 17U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_NEW], 2U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_EMPTY], 0U);

    number_replay = 0;
    ASSERT_EQ(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, &number_replay), LOG_REPLAY_OK);
    ASSERT_EQ(number_replay, 1U);
    // ASSERT_EQ(log->log_size(), 14U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRECT], 17U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRTY_START], 0U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 3U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 18U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_NEW], 2U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_EMPTY], 0U);

    number_replay = 0;
    ASSERT_EQ(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 2, NULL, &number_replay), LOG_REPLAY_OK);
    ASSERT_EQ(number_replay, 2U);
    // ASSERT_EQ(log->log_size(), 12U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRECT], 17U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRTY_START], 0U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 5U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 20U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_NEW], 2U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_EMPTY], 0U);

    number_replay = 0;
    ASSERT_EQ(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 8, NULL, &number_replay), LOG_REPLAY_OK);
    ASSERT_EQ(number_replay, 8U);
    // ASSERT_EQ(log->log_size(), 4U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRECT], 17U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRTY_START], 0U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 13U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 28U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_NEW], 2U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_EMPTY], 0U);

    number_replay = 0;
    ASSERT_EQ(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 8, NULL, &number_replay), LOG_REPLAY_NO_MORE_EVENTS);
    ASSERT_EQ(number_replay, 4U);
    // ASSERT_EQ(log->log_size(), 0U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRECT], 18U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_DIRTY_START], 0U);
    ASSERT_EQ(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 17U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 32U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_NEW], 2U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_EMPTY], 1U);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, FullReplayWithDifferentSizedEventsBackgroundReplayWithoutBoundaris) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log->Start(StartContext(), &system));

    const uint32_t max_events = 100; // Will place no more then this number of messages
    const uint32_t max_event_size = 10; // In Partitions
    const uint32_t min_event_size = 1; // In Partitions

    uint32_t commited_events = 0;
    LC_RNG rng(1024);
    char* data = new char[1024 * max_event_size];
    memset(data, 1, 1024 * max_event_size);

    while (commited_events < max_events) {
        uint32_t size = rng.GenerateWord32(min_event_size, max_event_size);
        MessageData message;
        message.set_message(data, (1024 * size) - 512);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        commited_events++;
        TRACE("Placed events with size " << size);
    }
    delete[] data;
    INFO("Placed " << commited_events << " events");

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();

    ASSERT_TRUE(log->PerformFullReplayBackgroundMode(false));

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();
}

TEST_P(LogTest, FullReplayWithDifferentSizedEventsBackgroundReplayWitBoundaris) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log->Start(StartContext(), &system));

    const uint32_t max_events = 100; // Will place no more then this number of messages
    const uint32_t max_event_size = 10; // In Partitions
    const uint32_t min_event_size = 1; // In Partitions

    uint32_t commited_events = 0;
    LC_RNG rng(1024);
    char* data = new char[1024 * max_event_size];
    memset(data, 1, 1024 * max_event_size);

    while (commited_events < max_events) {
        uint32_t size = rng.GenerateWord32(min_event_size, max_event_size);
        MessageData message;
        message.set_message(data, (1024 * size) - 512);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        commited_events++;
        TRACE("Placed events with size " << size);
    }
    delete[] data;
    INFO("Placed " << commited_events << " events");

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();

    ASSERT_TRUE(log->PerformFullReplayBackgroundMode(true));

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();
}

TEST_P(LogTest, FullReplayWithDifferentSizedEventsBackgroundReplayRandomNumber) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log->Start(StartContext(), &system));

    const uint32_t max_events = 100; // Will place no more then this number of messages
    const uint32_t max_event_size = 10; // In Partitions
    const uint32_t min_event_size = 1; // In Partitions
    const uint32_t max_replay_events = 50;
    const uint32_t min_replay_events = 0;

    uint32_t commited_events = 0;
    uint32_t zero_replayed = 0;
    LC_RNG rng(1024);
    char* data = new char[1024 * max_event_size];
    memset(data, 1, 1024 * max_event_size);

    while (commited_events < max_events) {
        uint32_t size = rng.GenerateWord32(min_event_size, max_event_size);
        MessageData message;
        message.set_message(data, (1024 * size) - 512);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        commited_events++;
        TRACE("Placed events with size " << size);
    }
    delete[] data;
    INFO("Placed " << commited_events << " events");

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();

    log_replay_result replay_result = LOG_REPLAY_OK;
    while (replay_result == LOG_REPLAY_OK) {
        uint32_t size = rng.GenerateWord32(min_replay_events, max_replay_events);
        uint32_t replayed = 0;
        uint64_t last_replayed_id = 0;
        TRACE("Will try to replay " << size << " Events.");
        replay_result = log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, size, &last_replayed_id, &replayed);
        if (size == 0) {
            zero_replayed++;
        }
        ASSERT_GE(size, replayed) << "Replayed " << replayed << " events, but should not be more then " << size << ". Last Replayed Log ID " << last_replayed_id;
        EXPECT_LOGGING(dedupv1::test::FATAL).Never();
        EXPECT_LOGGING(dedupv1::test::ERROR).Never();
        if (zero_replayed > 0) {
            EXPECT_LOGGING(dedupv1::test::WARN).Times(zero_replayed);
        } else {
            EXPECT_LOGGING(dedupv1::test::WARN).Never();
        }
    }
    ASSERT_EQ(LOG_REPLAY_NO_MORE_EVENTS, replay_result);
}

TEST_P(LogTest, FullReplayWithDifferentSizedEventsDirtyReplayRandomNumber) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log->Start(StartContext(), &system));

    const uint32_t max_events = 100; // Will place no more then this number of messages
    const uint32_t max_event_size = 10; // In Partitions
    const uint32_t min_event_size = 1; // In Partitions
    const uint32_t max_replay_events = 50;
    const uint32_t min_replay_events = 0;

    uint32_t commited_events = 0;
    uint32_t zero_replayed = 0;
    LC_RNG rng(1024);
    char* data = new char[1024 * max_event_size];
    memset(data, 1, 1024 * max_event_size);

    while (commited_events < max_events) {
        uint32_t size = rng.GenerateWord32(min_event_size, max_event_size);
        MessageData message;
        message.set_message(data, (1024 * size) - 512);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        commited_events++;
        TRACE("Placed events with size " << size);
    }
    delete[] data;
    INFO("Placed " << commited_events << " events");

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();

    log_replay_result replay_result = LOG_REPLAY_OK;
    while (replay_result == LOG_REPLAY_OK) {
        uint32_t size = rng.GenerateWord32(min_replay_events, max_replay_events);
        uint32_t replayed = 0;
        uint64_t last_replayed_id = 0;
        TRACE("Will try to replay " << size << " Events.");
        replay_result = log->Replay(EVENT_REPLAY_MODE_DIRTY_START, size, &last_replayed_id, &replayed);
        if (size == 0) {
            zero_replayed++;
        }
        ASSERT_GE(size, replayed) << "Replayed " << replayed << " events, but should not be more then " << size << ". Last Replayed Log ID " << last_replayed_id;
        EXPECT_LOGGING(dedupv1::test::FATAL).Never();
        EXPECT_LOGGING(dedupv1::test::ERROR).Never();
        if (zero_replayed > 0) {
            EXPECT_LOGGING(dedupv1::test::WARN).Times(zero_replayed);
        } else {
            EXPECT_LOGGING(dedupv1::test::WARN).Never();
        }
    }
    ASSERT_EQ(LOG_REPLAY_NO_MORE_EVENTS, replay_result);
}

/**
 * Tests the behavior of the log when more events are committed that it can store. The log
 * overflows.
 */
TEST_P(LogTest, Overflow) {
    if (message_size() > 1024) {
        return; // skip large message sizes
    }
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 8; i++) {
        VolumeAttachedEventData message;
        message.set_volume_id(1);
        ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NULL, NULL, NO_EC));
        VolumeDetachedEventData message2;
        message2.set_volume_id(1);
        ASSERT_TRUE(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &message2, NULL, NULL, NO_EC));
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());
    ASSERT_GT(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 0U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 16U); // 1 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_DETACH], 16U); // 1 DIRECT, 1 BG

    ASSERT_EQ(context.type_map[EVENT_TYPE_REPLAY_STARTED], 2U); // 1 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[EVENT_TYPE_REPLAY_STOPPED], 1U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_LOG_EMPTY], 1U);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, LargeValues) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }
    log = CreateLog(config_file());
    ASSERT_TRUE(log);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    ASSERT_TRUE(log->Start(StartContext(), &system));

    char buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);
    MessageData message;
    message.set_message(buffer, 16 * 1024);

    context.value_list.clear();
    ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, RestartWithLargeValues) {
    if (message_size() != 16 * 1024) {
        return; // skip small message sizes
    }

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    byte buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);

    for (int i = 0; i < 4; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));
    }

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 4U); // 4 DIRECT, 0 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 4U); // 4 DIRECT, 0 BG

    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed)); // new log event
    ASSERT_EQ(3, number_replayed);

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 5U); // 4 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 5U); // 4 DIRECT, 1 BG

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 4; i < 8; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 16U); // 1 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 16U); // 1 DIRECT, 1 BG

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, RestartWithLargeValuesTailDestroyedNearHead) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }
    EXPECT_LOGGING(dedupv1::test::WARN).Times(0, 1);

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    char buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);
    MessageData message;
    message.set_message(buffer, 16 * 1024);
    ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));

    uint64_t replayed_ids[2];
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log event
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 0, NULL));
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 1, NULL));

    INFO("Replayed " << replayed_ids[0] << ", " << replayed_ids[1]);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext()));
    int64_t key = (replayed_ids[1] * 2) - 1;
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    key = (replayed_ids[1] * 2);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    ASSERT_TRUE(log_index->Close());
    log_index = NULL;

    INFO("Destroy " << (replayed_ids[1] * 2) - 1 << ", " << ((replayed_ids[1] * 2)));

    INFO("Restart");
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 4; i < 8; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 12U);
    ASSERT_GE(context.type_map[kEventTypeTestLarge2], 9U);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

/**
 * Restart the log with large values (multiple buckets) when the tail id destroyed. This
 * means that the first elements have been removed, but the last elements of a multi-bucket entry
 * not
 */
TEST_P(LogTest, FailedRestartWithDestroyedReplayEvent) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }
    EXPECT_LOGGING(dedupv1::test::WARN).Once();
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(2);

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    byte buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);

    for (int i = 0; i < 4; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));
    }

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 4U); // 4 DIRECT, 0 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 4U); // 4 DIRECT, 0 BG

    uint64_t replayed_ids[2];
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log event
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 0, NULL));
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 1, NULL));

    INFO("Replayed " << replayed_ids[0] << ", " << replayed_ids[1]);

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 5U); // 4 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 5U); // 4 DIRECT, 1 BG

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext()));
    int64_t key = (replayed_ids[1] * 2) - 1;
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    key = (replayed_ids[1] * 2);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    ASSERT_TRUE(log_index->Close());
    log_index = NULL;

    INFO("Destroy " << (replayed_ids[1] * 2) - 1 << ", " << ((replayed_ids[1] * 2)));

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_FALSE(log->Start(start_context, &system));
    log->ClearData();
    log->Close();
    log = NULL;
}

TEST_P(LogTest, RestartWithHeadDestroyedNearTail) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }
    EXPECT_LOGGING(dedupv1::test::WARN).Once();

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    char buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);

    int64_t commit_log_id = 0;

    // two items
    MessageData message;
    message.set_message(buffer, 16 * 1024);
    ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, &commit_log_id, NULL, NO_EC));

    uint64_t replayed_ids[2];
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log event
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 0, NULL));
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 1, NULL));

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext()));

    int64_t event_size = replayed_ids[1] - replayed_ids[0];

    int64_t key = commit_log_id + event_size - 2;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    key = commit_log_id + event_size - 1;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    ASSERT_TRUE(log_index->Close());
    log_index = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 4; i < 8; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 12U);
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 9U); // one entry is detroyed

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, RestartWithLargeValuesNearHeadDestroyed) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }
    EXPECT_LOGGING(dedupv1::test::WARN).Once();
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(0, 4).Logger("Log");

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    byte buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);

    int64_t commit_log_id = 0;
    for (int i = 0; i < 4; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, &commit_log_id, NULL, NO_EC));
    }

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 4U); // 4 DIRECT, 0 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 4U); // 4 DIRECT, 0 BG

    uint64_t replayed_ids[2];
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log event
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 0, NULL));
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 1, NULL));

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 5U); // 4 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 5U); // 4 DIRECT, 1 BG

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext(StartContext::NON_CREATE)));

    int64_t event_size = replayed_ids[1] - replayed_ids[0];

    int64_t key = commit_log_id + event_size - 1;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);

    INFO("Manipulate last written id: " << commit_log_id - (event_size * 2));
    for (key = commit_log_id; key < commit_log_id + event_size - 2; key++) {
        DEBUG("Read log id: " << key);
        LogEntryData log_data;
        ASSERT_EQ(dedupv1::base::LOOKUP_FOUND, log_index->Lookup(&key, sizeof(key), &log_data));
        log_data.set_last_fully_written_log_id(commit_log_id - (event_size * 2));
        log_index->Put(&key, sizeof(key), log_data);
    }

    ASSERT_TRUE(log_index->Close());
    log_index = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));
    ASSERT_TRUE(log->PerformDirtyReplay());

    ASSERT_TRUE(log->UnregisterConsumer("context"));

    ASSERT_TRUE(log->Close());
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system));
    ASSERT_TRUE(log->PerformDirtyReplay());
    ASSERT_TRUE(log->Close());
    log = NULL;
}

TEST_P(LogTest, NoRestartWithDestroyedLog) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(0, 4).Logger("Log");
    EXPECT_LOGGING(dedupv1::test::WARN).Times(0, 4).Logger("Log");

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    byte buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);

    for (int i = 0; i < 10; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }
    sleep(1); // wait of the direct replay

    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed));
    ASSERT_EQ(1, number_replayed); // New Log, then skip

    number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 2, NULL, &number_replayed));
    ASSERT_EQ(2, number_replayed); // Two of the following ones

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext()));

    int64_t key = 19;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);

    ASSERT_TRUE(log_index->Close());
    log_index = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_FALSE(log->Start(start_context, &system));
    log->ClearData();
    log->Close();
    log = NULL;
}

/**
 * Restart the log with large values (multiple buckets) when the head id destroyed. This
 * means that the last elements of an log event have not been written
 */
TEST_P(LogTest, RestartWithLargeValuesHeadDestroyed) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }

    EXPECT_LOGGING(dedupv1::test::WARN).Once();
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    byte buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);

    int64_t commit_log_id = 0;
    for (int i = 0; i < 4; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, &commit_log_id, NULL, NO_EC));
    }

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 4U); // 4 DIRECT, 0 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 4U); // 4 DIRECT, 0 BG

    uint64_t replayed_ids[2];
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log event
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 0, NULL));
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 1, NULL));

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 5U); // 4 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 5U); // 4 DIRECT, 1 BG

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext()));

    int64_t event_size = replayed_ids[1] - replayed_ids[0];

    int64_t key = commit_log_id + event_size - 2;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    key = commit_log_id + event_size - 1;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    ASSERT_TRUE(log_index->Close());
    log_index = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 4; i < 8; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 16U);
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 15U); // one entry is detroyed

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, RestartWithLogEntries) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 4; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));
    }

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 4U); // 4 DIRECT, 0 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 4U); // 4 DIRECT, 0 BG

    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed)); // new log event
    ASSERT_EQ(1, number_replayed); // New Log

    number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed)); // new log event
    ASSERT_EQ(1, number_replayed); // kEventTypeTestLarge

    number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed)); // new log event
    ASSERT_EQ(1, number_replayed); // kEventTypeTestLarge2

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 5U); // 4 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 5U); // 4 DIRECT, 1 BG

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 4; i < 8; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 16U); // 1 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 16U); // 1 DIRECT, 1 BG

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, WaitUntilDirectlyReplayed) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    ASSERT_TRUE(log->Run());

    context.set_waiting_time(1);

    for (int i = 0; i < 4; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }

    ASSERT_TRUE(log->Stop(StopContext::FastStopContext()));

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, PickCorrectReplayIdAfterCrash) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 4; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));
    }

    ASSERT_TRUE(log->PerformDirtyReplay());
    ASSERT_TRUE(log->UnregisterConsumer("context"));

    int64_t replay_id = log->replay_id();
    log->ClearData();
    log->Close();
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));

    int64_t replay_id_2 = log->replay_id();
    INFO("replay id " << replay_id << ", replay id after restart " << replay_id_2);

    ASSERT_TRUE(abs(replay_id_2 - replay_id) < 2) << "Difference should be small";
}

TEST_P(LogTest, RestartAfterCrash) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 4; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));
    }

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 4U); // 4 DIRECT, 0 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 4U); // 4 DIRECT, 0 BG

    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed)); // new log event
    ASSERT_EQ(1, number_replayed);

    number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed)); // Large1
    ASSERT_EQ(1, number_replayed);

    number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed)); // Large2
    ASSERT_EQ(1, number_replayed);

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 5U); // 4 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 5U); // 4 DIRECT, 1 BG

    ASSERT_TRUE(log->UnregisterConsumer("context"));

    log->SetLogPosition(0); // introduce a corrupt state
    // log->SetReplayPosition(0); // introduce a corrupt state

    ASSERT_TRUE(log->Close());
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));

    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 4; i < 8; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NULL, NULL, NO_EC));
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 16U); // 1 DIRECT, 1 BG
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 16U); // 1 DIRECT, 1 BG

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, RestartLogWithOverflow) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);

    StartSizeLimitedLog(log);
    int limit_count = log->log_data_->GetLimitId();
    int overflow_count = limit_count + 10;

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < overflow_count - 20; i++) {
        MessageData message;
        message.set_message("Hello World");
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());
    for (int i = overflow_count - 20; i < overflow_count; i++) {
        MessageData message;
        message.set_message("Hello World");
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }

    EXPECT_GE(context.type_map[kEventTypeTestLarge], overflow_count + 10); // 4 DIRECT, 0 BG

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, false, true);
    ASSERT_EQ(limit_count, log->log_data_->GetLimitId());
    EXPECT_LE(overflow_count, log->log_id_);
    EXPECT_GT(log->replay_id_, 10);
}

TEST_P(LogTest, GenerateEmptyLogEvent) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    int64_t limit_id = log->log_data()->GetLimitId();
    DEBUG("Limit id " << limit_id);

    int64_t current_log_id = 0;
    int64_t current_replay_id = 0;

    int commit_count = 4;
    int replay_count = 4;

    DEBUG("Commit " << commit_count << ", replay " << replay_count << ", limit id " << limit_id);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < commit_count; i++) {
        MessageData message;
        FillMessage(&message);
        EXPECT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }

    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, (replay_count + 1), NULL, &number_replayed)); // new log event
    ASSERT_EQ(1, number_replayed); // New Log

    number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, replay_count, NULL, &number_replayed)); // new log event
    ASSERT_EQ(replay_count, number_replayed);

    current_log_id = log->log_id();
    current_replay_id = log->replay_id();

    EXPECT_GT(context.type_list().size(), 0);
    EXPECT_EQ(context.type_list().back(), EVENT_TYPE_LOG_EMPTY);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, false, true);
    ASSERT_EQ(limit_id, log->log_data()->GetLimitId());
    EXPECT_EQ(current_log_id, log->log_id());
    EXPECT_EQ(current_replay_id, log->replay_id());

}

TEST_P(LogTest, RestartRandom) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log event

    int64_t limit_id = log->log_data()->GetLimitId();
    DEBUG("Limit id " << limit_id);
    LC_RNG rng(1024);

    int64_t current_log_id = 0;
    int64_t current_replay_id = 0;
    for (int i = 0; i < 256; i++) {
        INFO("Round " << i);
        int commit_count = rng.GenerateWord32(1, limit_id / 2);
        int replay_count = rng.GenerateWord32(limit_id / 4, 0.75 * limit_id);
        if (message_size() > 1024) {
            commit_count = rng.GenerateWord32(1, limit_id / 4);
            replay_count = rng.GenerateWord32(limit_id / 5, 0.5 * limit_id);
        }

        DEBUG("Round " << i << ", commit " << commit_count << ", replay " << replay_count << ", limit id " << limit_id);

        for (int i = 0; i < commit_count; i++) {
            MessageData message;
            FillMessage(&message);
            ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        }

        uint32_t sum_number_replayed = 0;
        dedupv1::log::log_replay_result result = dedupv1::log::LOG_REPLAY_OK;
        while ((sum_number_replayed < replay_count) && (result == dedupv1::log::LOG_REPLAY_OK)) {
            uint32_t number_replayed = 0;
            result = log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, replay_count - sum_number_replayed, NULL, &number_replayed);
            ASSERT_GE(replay_count, number_replayed);
            sum_number_replayed += number_replayed;
        }
        ASSERT_FALSE(result == dedupv1::log::LOG_REPLAY_ERROR);
        ASSERT_GE(replay_count, sum_number_replayed);
        ASSERT_FALSE((result == dedupv1::log::LOG_REPLAY_OK) && (sum_number_replayed != replay_count));

        current_log_id = log->log_id();
        current_replay_id = log->replay_id();

        Option<bool> b = log->CheckLogId();
        ASSERT_TRUE(b.valid());
        ASSERT_TRUE(b.value());

        ASSERT_TRUE(log->Close());
        log = NULL;

        log = CreateLog(config_file());
        ASSERT_TRUE(log);
        StartSizeLimitedLog(log, true, false, true);
        ASSERT_EQ(limit_id, log->log_data()->GetLimitId());
        EXPECT_EQ(current_log_id, log->log_id());
        EXPECT_EQ(current_replay_id, log->replay_id());
    }
}

TEST_P(LogTest, RestartAll) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    int64_t limit_id = log->log_data()->GetLimitId();
    DEBUG("Limit id " << limit_id);

    int64_t current_log_id = 0;
    int64_t current_replay_id = 0;
    int rounds = 16;
    if (message_size() > 1024) {
        rounds = 6;
    }

    for (int i = 0; i < rounds; i++) {
        int commit_count = 6;
        int replay_count = 5;

        DEBUG("Round " << i << ", commit " << commit_count << ", replay " << replay_count << ", limit id " << limit_id);

        for (int j = 0; j < commit_count; j++) {
            MessageData message;
            FillMessage(&message);
            ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
        }

        if (i == 0) {
            ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // Log New
        }

        uint32_t number_replayed = 0;
        ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, replay_count, NULL, &number_replayed));
        ASSERT_EQ(replay_count, number_replayed);

        current_log_id = log->log_id();
        current_replay_id = log->replay_id();

        Option<bool> b = log->CheckLogId();
        ASSERT_TRUE(b.valid());
        ASSERT_TRUE(b.value());

        ASSERT_TRUE(log->Close());
        log = NULL;

        log = CreateLog(config_file());
        ASSERT_TRUE(log);
        StartSizeLimitedLog(log, true, false, true);
        ASSERT_EQ(limit_id, log->log_data()->GetLimitId());
        EXPECT_EQ(current_log_id, log->log_id());
        EXPECT_EQ(current_replay_id, log->replay_id());
    }
}

TEST_P(LogTest, RestartLogWithLogIdOnPositionZero) {
    if (message_size() > 1024) {
        return; // skip for large values
    }
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    int64_t limit_id = log->log_data()->GetLimitId();
    DEBUG("Limit id " << limit_id);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < limit_id - 10; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode(false));

    for (int i = 0; i < 9; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }
    // Replay all elements to delete them.
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode(false));
    MessageData message;
    message.set_message("Hello World");

    ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode(false));

    int64_t current_log_id = log->log_id();
    int64_t current_replay_id = log->replay_id();

    ASSERT_TRUE(log->UnregisterConsumer("context"));

    ASSERT_TRUE(log->Close());
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, false, true);
    ASSERT_EQ(limit_id, log->log_data()->GetLimitId());
    EXPECT_EQ(current_log_id, log->log_id());
    EXPECT_EQ(current_replay_id, log->replay_id());
}

TEST_P(LogTest, RestartLogWithOverflowAndDeletedLastHalfAfterCrash) {
    if (message_size() > 1024) {
        return; // skip for large values
    }

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    int64_t limit_id = log->log_data_->GetLimitId();
    DEBUG("Limit id " << limit_id);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < limit_id - 10; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }

    EXPECT_GE(context.type_map[kEventTypeTestLarge], limit_id - 10); // limit DIRECT

    // Replay all elements to delete them.
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    // Insert some more elements
    for (int i = 0; i < 20; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }

    int64_t current_log_id = log->log_id_;
    int64_t current_replay_id = log->replay_id_;

    ASSERT_TRUE(log->UnregisterConsumer("context"));

    log->SetLogPosition(log->replay_id_); // introduce a corrupt state
    // log->SetReplayPosition(0); // introduce a corrupt state

    ASSERT_TRUE(log->Close());
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, true, true); // start crashed
    ASSERT_EQ(limit_id, log->log_data_->GetLimitId());
    EXPECT_EQ(current_log_id, log->log_id_);
    EXPECT_EQ(current_replay_id, log->replay_id_);
}

TEST_P(LogTest, RestartLogWithOverflowAndDeletedLastHalf) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    int64_t limit_id = log->log_data_->GetLimitId();

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 10; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }

    EXPECT_GE(context.type_map[kEventTypeTestLarge], 10); // 10 DIRECT, ? BG

    // Replay all elements to delete them.
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log
    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 10, NULL, &number_replayed));
    ASSERT_EQ(10, number_replayed);

    // Insert some more elements
    for (int i = 0; i < 4; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }

    int64_t current_log_id = log->log_id_;
    int64_t current_replay_id = log->replay_id_;

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, false, true);
    ASSERT_EQ(limit_id, log->log_data_->GetLimitId());
    EXPECT_EQ(current_log_id, log->log_id_);
    EXPECT_EQ(current_replay_id, log->replay_id_);
}

TEST_P(LogTest, RestartLogWithOverflowAndDeletedMiddle) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);
    // Lower the limit so that the log doesn't replay everything.
    log->nearly_full_limit_ = 2;
    int64_t limit_id = log->log_data_->GetLimitId();

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 10; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }

    EXPECT_GE(context.type_map[kEventTypeTestLarge], 10); // 4 DIRECT, 0 BG

    // Replay 8 elements to delete them. There are still 2 valid elements at the start of the array
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log
    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 8, NULL, &number_replayed));
    ASSERT_EQ(8, number_replayed);

    // Insert some more elements. Situation is now (v: valid, d: deleted): vvvvddddvv
    for (int i = 0; i < 4; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }

    int64_t current_log_id = log->log_id_;
    int64_t current_replay_id = log->replay_id_;

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, false, true);
    ASSERT_EQ(limit_id, log->log_data_->GetLimitId());
    EXPECT_EQ(current_log_id, log->log_id_);
    EXPECT_EQ(current_replay_id, log->replay_id_);
}

TEST_P(LogTest, RestartLogWithOverflowAndDeletedStartAndEnd) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);
    // Lower the limit so that the log doesn't replay everything.
    log->nearly_full_limit_ = 2;

    int64_t limit_id = log->log_data_->GetLimitId();

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 9; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }

    EXPECT_GE(context.type_map[kEventTypeTestLarge], 9);

    // Delete all elements
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log
    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 9, NULL, &number_replayed));
    ASSERT_EQ(9, number_replayed);

    // Insert some more elements. Situation is now (v: valid, d: deleted): vvvvvvvvdd
    for (int i = 0; i < 8; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NULL, NULL, NO_EC));
    }

    // Delete two more elements from the start: ddvvvvvvdd
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // Log empty
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // Next one

    int64_t current_log_id = log->log_id_;
    int64_t current_replay_id = log->replay_id_;

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    ASSERT_TRUE(log->Close());
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, false, true);
    ASSERT_EQ(limit_id, log->log_data_->GetLimitId());
    EXPECT_EQ(current_log_id, log->log_id_);
    EXPECT_EQ(current_replay_id, log->replay_id_);
}

}
}
