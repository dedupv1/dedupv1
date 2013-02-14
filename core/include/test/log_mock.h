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

#ifndef LOG_MOCK_H_
#define LOG_MOCK_H_

#include <gmock/gmock.h>
#include <core/log.h>
#include <core/log_consumer.h>

class MockLog : public dedupv1::log::Log {
    public:
        MOCK_METHOD5(CommitEvent, bool(dedupv1::log::event_type,
                const google::protobuf::Message*,
                int64_t*,
                dedupv1::log::LogAckConsumer*,
                dedupv1::base::ErrorContext*));
        MOCK_METHOD1(ReplayAll, bool(dedupv1::log::replay_mode replay_mode));
        MOCK_METHOD1(ReplayStart, bool(dedupv1::log::replay_mode replay_mode));
        MOCK_METHOD1(Replay, dedupv1::log::log_replay_result(dedupv1::log::replay_mode replay_mode));
        MOCK_METHOD1(ReplayCommit, bool(dedupv1::log::replay_mode replay_mode));
        MOCK_METHOD1(ReplayStop, bool(dedupv1::log::replay_mode replay_mode));
        MOCK_METHOD0(IsReplaying, bool());

        MOCK_METHOD2(RegisterConsumer, bool(const std::string& consumer_name, dedupv1::log::LogConsumer* consumer_));
        MOCK_METHOD1(UnregisterConsumer, bool(const std::string& consumer_name));
};

#endif /* LOG_MOCK_H_ */
