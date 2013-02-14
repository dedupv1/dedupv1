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

#include <core/dedup.h>
#include <core/log_consumer.h>
#include <core/log.h>
#include <base/strutil.h>

using dedupv1::base::strutil::ToString;

namespace dedupv1 {
namespace log {

LogConsumer::LogConsumer() {
}

LogConsumer::~LogConsumer() {
}

LogAckConsumer::LogAckConsumer() {
}

LogAckConsumer::~LogAckConsumer() {
}

LogReplayContext::LogReplayContext(enum replay_mode mode, int64_t log_id) {
    this->mode_ = mode;
    this->log_id_ = log_id;
}

replay_mode LogReplayContext::replay_mode() const {
    return this->mode_;
}

int64_t LogReplayContext::log_id() const {
    return this->log_id_;
}

std::string LogReplayContext::DebugString() const {
    return "[mode " + Log::GetReplayModeName(this->mode_) + ", event log id " + ToString(log_id()) + "]";
}

std::map<event_type, EventTypeInfo> EventTypeInfo::info_map_ = EventTypeInfo::CreateInfoMap();

const std::map<event_type, EventTypeInfo> EventTypeInfo::CreateInfoMap() {
    std::map<event_type, EventTypeInfo> m;
    m[EVENT_TYPE_CONTAINER_OPEN] = EventTypeInfo(LogEventData::kContainerOpenedEventFieldNumber, true);
    m[EVENT_TYPE_CONTAINER_COMMIT_FAILED] = EventTypeInfo(LogEventData::kContainerCommitFailedEventFieldNumber, true);
    m[EVENT_TYPE_CONTAINER_COMMITED] = EventTypeInfo(LogEventData::kContainerCommittedEventFieldNumber, true);
    m[EVENT_TYPE_CONTAINER_MERGED] = EventTypeInfo(LogEventData::kContainerMergedEventFieldNumber, true);
    m[EVENT_TYPE_CONTAINER_MOVED] = EventTypeInfo(LogEventData::kContainerMovedEventFieldNumber, true);
    m[EVENT_TYPE_BLOCK_MAPPING_WRITTEN] = EventTypeInfo(LogEventData::kBlockMappingWrittenEventFieldNumber, true);
    m[EVENT_TYPE_BLOCK_MAPPING_DELETED] = EventTypeInfo(LogEventData::kBlockMappingDeletedEventFieldNumber, true);
    m[EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED] = EventTypeInfo(LogEventData::kBlockMappingWriteFailedEventFieldNumber, true);
    m[EVENT_TYPE_VOLUME_DETACH] = EventTypeInfo(LogEventData::kVolumeDetachedEventFieldNumber, true);
    m[EVENT_TYPE_VOLUME_ATTACH] = EventTypeInfo(LogEventData::kVolumeAttachedEventFieldNumber, true);
    m[EVENT_TYPE_CONTAINER_DELETED] = EventTypeInfo(LogEventData::kContainerDeletedEventFieldNumber, true);
    m[EVENT_TYPE_OPHRAN_CHUNKS] = EventTypeInfo(LogEventData::kOphranChunksEventFieldNumber, true);

    m[EVENT_TYPE_REPLAY_STARTED] = EventTypeInfo(LogEventData::kReplayStartEventFieldNumber, true);
    m[EVENT_TYPE_REPLAY_STOPPED] = EventTypeInfo(LogEventData::kReplayStopEventFieldNumber, true);
    m[EVENT_TYPE_SYSTEM_START] = EventTypeInfo(LogEventData::kSystemStartEventFieldNumber, true);
    return m;
}

}
}
