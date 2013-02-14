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

#ifndef LOG_CONSUMER_H__
#define LOG_CONSUMER_H__

#include <string>
#include <google/protobuf/message.h>
#include <dedupv1.pb.h>

namespace dedupv1 {
namespace log {

/**
 * Enumeration of the different modes that a
 * log entry might be replayed in.
 *
 */
enum replay_mode {
    EVENT_REPLAY_MODE_DIRECT = 1,              //!< EVENT_REPLAY_MODE_DIRECT
    EVENT_REPLAY_MODE_REPLAY_BG = 2,   //!< EVENT_REPLAY_MODE_REPLAY_BG
    EVENT_REPLAY_MODE_DIRTY_START = 4,//!< EVENT_REPLAY_MODE_DIRTY_START
};

/**
 * Enumeration for all event types that can be committed.
 * Note: If you add a new event type, you should also adapt
 * Log::GetEventTypeName().
 */
enum event_type {
    EVENT_TYPE_NONE = 0,

    EVENT_TYPE_CONTAINER_OPEN = 15,

    /**
     * Event type logged when the commit of a container failed.
     * The event value is an instance of the ContainerCommitFailedEventData message.
     */
    EVENT_TYPE_CONTAINER_COMMIT_FAILED = 16,

    /**
     * Event type logged when a new container is committed.
     * The event value is an instance of the ContainerComittedEventData message.
     */
    EVENT_TYPE_CONTAINER_COMMITED = 17,

    /**
     * Event type logged when a container is merged.
     * The event value is an instance of the ContainerMergedEventData message.
     */
    EVENT_TYPE_CONTAINER_MERGED = 18,

    /**
     * Event type logged when a container is moved to a new direction, e.g.
     * after elements have been deleted
     */
    EVENT_TYPE_CONTAINER_MOVED = 19,

    /**
     * Event type logged when a block mapping is written.
     * The event does guarantee that the block mapping is already fully committed.
     *
     * The event value is an instance of the BlockMappingWrittenEventData message.
     */
    EVENT_TYPE_BLOCK_MAPPING_WRITTEN = 20,

    /**
     * Event type logged when a block mapping is deleted.
     *
     * The event value is an instance of the BlockMappingDeletedEventData message.
     */
    EVENT_TYPE_BLOCK_MAPPING_DELETED = 21,

    /**
     * Event type logged when a block mapping write failed.
     *
     * The data of this event has not been persistent in any way so usually
     * there is not need for activity. However, the gc might check if some of the
     * item had been new and might be a gc candidate.
     */
    EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED = 22,

    /**
     * The event has no value data
     */
    EVENT_TYPE_REPLAY_STARTED = 32,

    /**
     * The event has no value data
     */
    EVENT_TYPE_REPLAY_STOPPED = 33,

    /**
     * The event has no value data
     */
    EVENT_TYPE_REPLAY_COMMIT = 34,

    /**
     * Event type logged when all events are replayed and currently no further events
     * have to be logged.
     * The event has no value data.
     *
     * A special note: In contrast to all other events, the log empty
     * event is replay without considering the direct replay queue.
     */
    EVENT_TYPE_LOG_EMPTY = 35,

    EVENT_TYPE_LOG_NEW = 36,

    /**
     * The event value is an instance of the VolumeAttachedEventData message
     */
    EVENT_TYPE_VOLUME_ATTACH = 41,

    /**
     * The event value is an instance of the VolumeDetachedEventData message
     */
    EVENT_TYPE_VOLUME_DETACH = 42,

    /**
     * Event that is triggered if the system is restarted.
     *
     * The event has no value data
     */
    EVENT_TYPE_SYSTEM_START = 51,

    /**
     * The event has no value data
     */
    EVENT_TYPE_SYSTEM_RUN = 52,

    /**
     * A log barrier is used to introduce a barrier into the log replay.
     * The barrier should never be published or stored. It is e.g. used to implement the
     * WaitUntilDirectlyReplayed method
     */
    EVENT_TYPE_LOG_BARRIER = 61,

    /**
     * Event type logged when a container is deletes
     */
    EVENT_TYPE_CONTAINER_DELETED = 70,

    /**
     * Event that notifies the system that certain chunks might be ophraned.
     * This is not necessarily the case, but the system should check for it.
     *
     * Usually this event is triggered, when the filter chain failed.
     */
    EVENT_TYPE_OPHRAN_CHUNKS = 71,

    /**
     * a event type id larger than any real event type id.
     * Usually used for testing
     */
    EVENT_TYPE_NEXT_ID = 72,

    EVENT_TYPE_MAX_ID = 96
};

/**
 * Information over the event types.
 * These information are used as a kind of reflection, e.g. to find the correct
 * protobuf field id for a event type.
 */
class EventTypeInfo {
    private:
        /**
         * map from event types to type information
         */
        static std::map<event_type, EventTypeInfo> info_map_;

        /**
         * Initializes the info map.
         */
        static const std::map<event_type, EventTypeInfo> CreateInfoMap();

        /**
         * field number for the given event type in the LogEventData message.
         * If set to 0, no field is assigned for that type
         */
        int event_data_message_field_;

        bool persistent_;

    public:
        /**
         * Constructor.
         * By default a event type is persistent
         */
        EventTypeInfo() {
            event_data_message_field_ = 0;
            persistent_ = true;
        }

        /**
         * Constructor with a explicit protobuf field id
         */
        EventTypeInfo(int event_data_message_field, bool persistent) {
            event_data_message_field_ = event_data_message_field;
            persistent_ = persistent;
        }

        /**
         * returns the field id
         */
        int event_data_message_field() const {
            return event_data_message_field_;
        }

        bool is_persistent() const {
            return persistent_;
        }

        /**
         * Returns the event type info for a event type
         */
        static const EventTypeInfo& GetInfo(enum event_type event_type) {
            return info_map_[event_type];
        }

        /**
         * Registers a additional event type.
         * Usually only called by GetInfoMap() and for testing.
         */
        static void RegisterEventTypeInfo(enum event_type event_type, const EventTypeInfo& event_type_info) {
            info_map_[event_type] = event_type_info;
        }
};

/**
 * Contains context information about the replay of the event.
 * We use this context object instead of direct parameter to allow the add information to the
 * context later without breaking the interface.
 */
class LogReplayContext {
    private:
        /**
         * replay mode
         */
        enum replay_mode mode_;

        /**
         * log id of the event currently replayed
         */
        int64_t log_id_;
    public:
        /**
         * Constructor.
         *
         * @param mode
         * @param log_id
         * @return
         */
        LogReplayContext(enum replay_mode mode, int64_t log_id);

        /**
         * returns the replay mode
         * @return
         */
        enum replay_mode replay_mode() const;

        /**
         * returns the log id of the event
         * currently replayed
         * @return
         */
        int64_t log_id() const;

        /**
         * returns a human-developer readable string of
         * the replay context.
         * @return
         */
        std::string DebugString() const;
};

/**
 * A log ack consumer is notified about all log events
 */
class LogAckConsumer {
    public:
        /**
         * Constructor
         * @return
         */
        LogAckConsumer();

        /**
         * Destructor
         * @return
         */
        virtual ~LogAckConsumer();

        /**
         * Called when a log entry is committed, but not yet replayed.
         *
         * @param event_type
         * @param log_message
         * @param context the current replay context
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool LogAck(enum event_type event_type, const google::protobuf::Message* log_message, const LogReplayContext& context) = 0;
};

/**
 * A log consumer is notified about all log events
 */
class LogConsumer {
    public:
        /**
         * Constructor
         * @return
         */
        LogConsumer();

        /**
         * Destructor
         * @return
         */
        virtual ~LogConsumer();

        /**
         * Called when a log entry is replayed.
         * Every log consumer should accept that new event types are added. Not known an event type should not be seen as an
         * failure. A log handling, especially in direct mode should be fast. It may be executed on the critical path.
         *
         * A log consumer has to handle that a single event is replayed multiple times in the background replay mode. As an event
         * is replayed multiple times when the system crashes in the middle of a replay (At-Least Once semantics).
         *
         * A log consumer should be very careful accessing locks. Also it is not allowed to write log entries during a log replay.
         *
         * @param event_type
         * @param event_value
         * @param context the current replay context
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool LogReplay(enum event_type event_type,
                const LogEventData& event_value,
                const LogReplayContext& context) = 0;
};

}
}

#endif  // LOG_CONSUMER_H__
