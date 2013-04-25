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

#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <errno.h>

#include <string>
#include <list>
#include <sstream>

#include <tbb/tick_count.h>

#include <dedupv1.pb.h>
#include <dedupv1_stats.pb.h>
#include <google/protobuf/descriptor.h>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <core/log.h>
#include <base/strutil.h>
#include <base/fileutil.h>
#include <base/protobuf_util.h>
#include <base/index.h>
#include <base/tc_fixed_index.h>
#include <base/logging.h>
#include <base/memory.h>
#include <base/timer.h>
#include <base/fault_injection.h>
#include <core/dedup_system.h>

using std::list;
using std::string;
using std::stringstream;
using std::string;
using std::vector;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::Join;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::strutil::FriendlySubstr;
using dedupv1::base::ProfileTimer;
using dedupv1::base::MutexLock;
using dedupv1::base::timed_bool;
using dedupv1::base::ScopedLock;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::base::TIMED_FALSE;
using dedupv1::base::ScopedArray;
using dedupv1::base::NewRunnable;
using dedupv1::base::IDBasedIndex;
using dedupv1::base::ThreadUtil;
using dedupv1::base::Option;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::DELETE_ERROR;
using dedupv1::base::Index;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::put_result;
using dedupv1::base::SerializeMessageToString;
using tbb::tick_count;
using tbb::spin_mutex;
using tbb::spin_rw_mutex;
using dedupv1::base::Thread;
using google::protobuf::Message;
using google::protobuf::FieldDescriptor;
using dedupv1::base::make_option;
using dedupv1::base::ThreadUtil;

LOGGER("Log");

namespace dedupv1 {
namespace log {

const std::string Log::kDefaultLogIndexType = "disk-fixed";
const int Log::kDefaultDirectReplayThreadPrio = 20;
const uint32_t Log::kDefaultLogEntryWidth = 1000;

/**
 * A log consumer that prints out progress reports during the replay
 */
class FullReplayLogConsumer : public LogConsumer {
private:
    int64_t replay_id_at_start;
    int64_t log_id_at_start;
    int last_full_percent_progress;
    tick_count start_time;
public:
    /**
     * Constructor
     */
    FullReplayLogConsumer(int64_t replay_id_at_start, int64_t log_id_at_start);

    bool LogReplay(dedupv1::log::event_type event_type, const LogEventData& data,
                   const dedupv1::log::LogReplayContext& context);
};

Log::Log() : replay_thread_(NewRunnable(this, &Log::ReplayLoop), "log direct"),
    replay_thread_start_barrier_(2) {
    this->state_ = LOG_STATE_CREATED;
    this->log_data_ = NULL;
    this->log_id_ = 1;
    this->replay_id_ = 1;
    this->stats_.event_count_ = 0;
    this->max_log_entry_width_ = 0; // Default will be assigned later
    this->max_log_value_size_per_bucket_ = 0;
    this->max_log_size_ = 32 * 1024 * 1024;
    this->nearly_full_limit_ = kDefaultNearlyFullLimit;
    this->direct_replay_thread_prio_ = kDefaultDirectReplayThreadPrio;
    this->last_empty_log_id_ = 0;
    this->last_fully_written_log_id_ = 0;
    this->readonly_ = false;
    this->is_replaying_ = false;
    this->max_area_size_dirty_replay_ = kDefaultMaxAreaSizeDirtyReplay_;
    this->max_area_size_full_replay_ = kDefaultMaxAreaSizeFullReplay_;
    this->log_id_update_intervall_ = kDefaultLogIDUpdateIntervall_;
    this->wasStarted_ = false; // TODO (dmeister) Coding convention
    this->is_last_read_event_data_valid_ = false;
#ifdef DEDUPV1_CORE_TEST
    this->data_cleared = false;
#endif
}

Log::Statistics::Statistics() :
    average_commit_latency_(256), average_read_event_latency_(256), average_replay_events_latency_(256),
    average_replayed_events_per_step_(256), average_ack_latency_(256) {
    this->event_count_ = 0;
    this->throttle_count_ = 0;
    this->multi_entry_event_count_ = 0;
    this->direct_replay_count_ = 0;
    this->replayed_events_ = 0;
    for (int i = 0; i < EVENT_TYPE_NEXT_ID; i++) {
        this->replayed_events_by_type_[i] = 0;
    }
}

Log::~Log() {
}

IDBasedIndex* Log::CreateDefaultLogData() {
    IDBasedIndex* index = dynamic_cast<IDBasedIndex*>(Index::Factory().Create(Log::kDefaultLogIndexType));
    CHECK_RETURN(index, NULL, "Cannot create log data store");
    return index;
}

bool Log::SetOption(const string& option_name, const string& option) {
    CHECK(this->state_ == LOG_STATE_CREATED, "Log already started");

    if (option_name == "filename") {
        CHECK(option.size() <= 255, "Filename too long");
        CHECK(option.size() > 0, "Filename too short");
        if (this->log_data_ == NULL) {
            this->log_data_ = CreateDefaultLogData();
        }
        CHECK(this->log_data_->SetOption("filename", option), "Illegal filename: " << option);
        return true;
    } else if (option_name == "delayed-replay-thread-prio") {
        CHECK(To<int>(option).valid(), "Illegal option " << option);
        size_t c = To<int>(option).value();
        CHECK(c >= 0 && c <= 30, "Illegal delayed-replay-thread-prio");
        this->direct_replay_thread_prio_ = c;
        return true;
    } else if (option_name == "max-log-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->max_log_size_ = ToStorageUnit(option).value();
        return true;
    } else if (option_name == "area-size-dirty-replay") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        CHECK(ToStorageUnit(option).value() > 0, "Maximum max area size dirty replay has to be greater 0.");
        CHECK(ToStorageUnit(option).value() < UINT32_MAX, "Maximum area size dirty replay " << UINT32_MAX << ", illegal option " << option);
        this->max_area_size_dirty_replay_ = ToStorageUnit(option).value();
        return true;
    } else if (option_name == "area-size-full-replay") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        CHECK(ToStorageUnit(option).value() > 0, "Maximum max area size full replay has to be greater 0.");
        CHECK(ToStorageUnit(option).value() < UINT32_MAX, "Maximum area size full replay " << UINT32_MAX << ", illegal option " << option);
        this->max_area_size_full_replay_ = ToStorageUnit(option).value();
        return true;
    } else if (option_name == "log-id-update-interval") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        CHECK(ToStorageUnit(option).value() > 0, "log-id-update-interval area size has to be greater 0. (1 means each)");
        CHECK(ToStorageUnit(option).value() < UINT32_MAX, "Maximum log-id-update-interval size " << UINT32_MAX << ", illegal option " << option);
        this->log_id_update_intervall_ = ToStorageUnit(option).value();
        return true;
    } else if (option_name == "max-entry-width") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->max_log_entry_width_ = ToStorageUnit(option).value();
        CHECK(max_log_entry_width_ >= 512, "Illegal option " << option);
        return true;
    } else if (option_name == "type") {
        CHECK(this->log_data_ == NULL, "Log data already set");
        Index* index = Index::Factory().Create(option);
        CHECK(index != NULL, "Failed to create index type: " << option);
        this->log_data_ = dynamic_cast<IDBasedIndex*>(index);
        if (this->log_data_ == NULL) {
            if (!index->Close()) {
                WARNING("Failed to close index");
            }
            ERROR("Index not id-based: type " << option);
            return false;
        }
        return true;
    } else if (StartsWith(option_name, "index.")) {
        if (this->log_data_ == NULL) {
            this->log_data_ = CreateDefaultLogData();
        }
        CHECK(this->log_data_->SetOption(option_name.substr(strlen("index.")), option), "Failed to configure log data index");
        return true;
    } else if (StartsWith(option_name, "throttle.")) {
        CHECK(this->throttling_.SetOption(option_name.substr(strlen("throttle.")), option),
            "Failed to configure log throttling");
        return true;
    } else if (StartsWith(option_name, "info.")) {
        CHECK(this->log_info_store_.SetOption(option_name.substr(strlen("info.")), option), "Failed to configure log info store");
        return true;
    }
    ERROR("Illegal option name: " << option_name);
    return false;
}

bool Log::Start(const StartContext& start_context, dedupv1::DedupSystem* system) {
    DCHECK(system, "System not set");

    CHECK(this->state_ == LOG_STATE_CREATED, "Log already started");
    CHECK(this->max_log_size_ > 0, "Max log size not set");

    INFO("Start log");

    CHECK(this->log_info_store_.Start(start_context), "Failed to start log info store");

    LogLogIDData logID_data;
    LogReplayIDData replayID_data;
    LogStateData state_data;
    lookup_result info_lookup = ReadMetaInfo(&logID_data, &replayID_data, &state_data);

    CHECK(info_lookup != LOOKUP_ERROR, "Failed to read meta info");
    CHECK(!(info_lookup == LOOKUP_NOT_FOUND && !start_context.create()),
        "Failed to lookup meta info in non-create startup mode");
    CHECK(!(info_lookup == LOOKUP_FOUND && start_context.create()),
        "Found meta info in create mode, please clean up first");

    readonly_ = start_context.readonly();
    if (this->log_data_ == NULL) {
        this->log_data_ = CreateDefaultLogData();
    }
    CHECK_RETURN(this->log_data_, LOOKUP_ERROR, "Log database could not be initialized.");

    // The log size might be set via log.size and via log.index.size.
    // We prefer the first method
    if (max_log_size_) {
        CHECK(this->log_data_->SetOption("size", ToString(max_log_size_)),
            "Illegal size: max log size " << max_log_size_);
    }

    // We use this method to assign the default so that we are able to
    // chance the default log entry width while maintaining disk-format compatibility
    // with old settings.
    if (max_log_entry_width_ == 0) {
        // entry width not set
        if (state_data.has_log_entry_width()) {
            max_log_entry_width_ = state_data.log_entry_width();
        } else {
            // use default
            // at the next start, the width is configured by the stored data
            max_log_entry_width_ = kDefaultLogEntryWidth;
        }
    }
    CHECK(this->log_data_->SetOption("width", ToString(this->max_log_entry_width_)),
        "Failed to set default width: entry width " << max_log_entry_width_);

    CHECK(this->log_data_->IsPersistent(), "Log should be persistent");
    CHECK(this->log_data_->Start(start_context), "Cannot start log data");
    this->max_log_value_size_per_bucket_ = this->max_log_entry_width_ - 48;

    if (!start_context.create()) {
        this->log_id_ = logID_data.log_id();
        this->replay_id_ = replayID_data.replay_id();
        if (state_data.has_limit_id()) {
            CHECK(this->log_data_->GetLimitId() == state_data.limit_id(),
                "Limit id mismatch: " <<
                "configured " << this->log_data_->GetLimitId() <<
                ", stored " << state_data.limit_id());
        }
        if (state_data.has_log_entry_width()) {
            CHECK(max_log_entry_width_ == state_data.log_entry_width(),
                "Log entry width mismatch: " <<
                "configured " << max_log_entry_width_ <<
                ", stored " << state_data.log_entry_width());
        }
        if (start_context.has_crashed()) {
            DEBUG("Searching actual log id after replay");

            Option<bool> b = CheckLogId();
            CHECK(b.valid(), "Error while checking Log");
            CHECK(b.value(), "Error while checking Log (result was false)");
        }
    } else {
        this->log_id_ = 0;
        this->replay_id_ = 0;
        CHECK(DumpMetaInfo(), "Failed to dump info");
    }

    // init replay thread
    if (this->direct_replay_thread_prio_ > 0) {
        CHECK(this->replay_thread_.SetPriority(direct_replay_thread_prio_),
            "Failed to set replay thread priority");
    }

    this->state_ = LOG_STATE_STARTED;

    if (start_context.create()) {
        // We want to make sure that there is at least one entry in the log
        // after the start of the log finished
        CHECK(this->CommitEvent(EVENT_TYPE_LOG_NEW, NULL, NULL, NULL, NO_EC),
            "Failed to commit new log event");
    }
    this->wasStarted_ = true;
    last_directly_replayed_log_id_ = log_id_ - 1;
    this->last_fully_written_log_id_ = this->log_id_ - 1;

    INFO("Started log: replay id " << this->replay_id_ << ", log id " << this->log_id_);
    return true;
}

bool Log::Run() {
    CHECK(this->state_ == LOG_STATE_STARTED, "Illegal state: state " << this->state_);

    INFO("Running log");

    this->replay_event_queue_.clear(); // Clear
    CHECK(this->replay_thread_.Start(),
        "Failed to start replay thread");
    this->state_ = LOG_STATE_RUNNING;
    CHECK(replay_thread_start_barrier_.Wait(), "Failed to wait for replay thread");

    ScopedLock scoped_lock(&direct_replay_queue_empty_lock_);
    CHECK(scoped_lock.AcquireLock(),
        "Failed to acquire direct replay empty queue condition lock");

    CHECK(direct_replay_queue_empty_condition_.ConditionWait(&direct_replay_queue_empty_lock_),
        "Failed to wait for direct replay empty queue condition");

    return true;
}

bool Log::Stop(const dedupv1::StopContext& stop_context) {
    bool failed = false;
    if (this->state_ == LOG_STATE_RUNNING) {
        INFO("Stopping log");
        this->state_ = LOG_STATE_STOPPED;

        if (this->replay_thread_.IsStarted() || this->replay_thread_.IsFinished()) {
            bool r = false;
            CHECK(this->replay_thread_.Join(&r), "Failed to join with replay thread");
            if (!r) {
                WARNING("Replay thread existed with error");
                failed = true;
            }
        }

        DEBUG("Stopped log");
    }
    return !failed;
}

int64_t Log::GetLogPositionFromId(int64_t id) {
    return id % this->log_data_->GetLimitId();
}

bool Log::IsNearlyFull(int reserve) {
    CHECK(this->log_data_, "Log data not set");
    int64_t log_id_with_space = this->log_id_ + reserve;

    int64_t diff = log_id_with_space - this->replay_id_;
    int64_t boundary = (this->log_data_->GetLimitId() - nearly_full_limit_);
    if (diff >= boundary) {
        return true;
    }
    return false;
}

Option<bool> Log::Throttle(int thread_id, int thread_count) {
    CHECK(this->log_data_, "Log data not set");

    double thread_ratio = 1.0 * thread_id / thread_count;
    Option<bool> r = throttling_.Throttle(GetFillRatio(), thread_ratio);
    if (r.valid() && r.value()) {
        this->stats_.throttle_count_++;
    }

    double replay_queue_fill_ratio = (this->replay_event_queue_.unsafe_size() / 100000);
    thread_ratio = 1.0 * thread_id / thread_count;
    r = throttling_.Throttle(replay_queue_fill_ratio, thread_ratio);
    if (r.valid() && r.value()) {
        this->stats_.throttle_count_++;
    }

    return r;
}

bool Log::GetRemainingFreeLogPlaces(int64_t* remaining_log_places) {
    DCHECK(this->log_data_, "Log data not set");
    DCHECK(remaining_log_places, "out parameter not set");

    spin_mutex::scoped_lock l(this->lock_);
    int64_t diff = this->log_id_ - this->replay_id_;
    *remaining_log_places = this->log_data_->GetLimitId() - diff;
    return true;
}

bool Log::IsFull(bool hard_limit) {
    DCHECK(this->log_data_, "Log data not set");

    double factor = 1 - throttling_.soft_limit_factor();
    if (hard_limit) {
        factor = 1 - throttling_.hard_limit_factor();
    }
    int64_t remaining = 0;
    CHECK(GetRemainingFreeLogPlaces(&remaining), "Failed to get remaining free log places");
    bool r = (remaining < (this->log_data_->GetLimitId() * factor));
    return r;
}

bool Log::ReplayDirectReplayEntry(const LogReplayEntry& replay_entry) {
    if (!replay_entry.failed() && replay_entry.event_type() != EVENT_TYPE_LOG_EMPTY) {
        direct_replay_state_.set_type(replay_entry.event_type());
        direct_replay_state_.set_log_id(replay_entry.log_id());
        if (replay_entry.event_type() != EVENT_TYPE_LOG_BARRIER) {
            LogReplayContext replay_context(EVENT_REPLAY_MODE_DIRECT, replay_entry.log_id());
            if (!this->PublishEvent(replay_context, replay_entry.event_type(), replay_entry.event_value())) {
                ERROR("Failed to publish event (direct): event type " << Log::GetEventTypeName(
                        replay_entry.event_type()));
                return false;
            }
        }
        this->stats_.direct_replay_count_++;
        direct_replay_state_.set_type(EVENT_TYPE_NONE);
    }
    return true;
}

bool Log::ReplayLoop() {
    DEBUG("Starting direct replay thread: log id " << log_id_);

    last_directly_replayed_log_id_ = log_id_ - 1;
    std::map<uint64_t, LogReplayEntry> delayed_entry_map;

    CHECK(replay_thread_start_barrier_.Wait(), "Failed to wait for barrier");

    bool should_exit = false;
    while (!should_exit) {
        bool consider_queue = true;
        if (!delayed_entry_map.empty()) {
            // delayed entries exists
            std::map<uint64_t, LogReplayEntry>::iterator i = delayed_entry_map.begin();
            if (last_directly_replayed_log_id_ + 1 == i->first) {
                LogReplayEntry& replay_entry(i->second);
                TRACE("Replay event (delay direct): " << replay_entry.DebugString());

                if (!ReplayDirectReplayEntry(replay_entry)) {
                    ERROR("Failed to replay direct replay: " << replay_entry.DebugString() <<
                        " last directly replayed log id " << last_directly_replayed_log_id_ <<
                        " last replayed log id " << replay_id_);
                }
                last_directly_replayed_log_id_ += replay_entry.log_id_count();
                consider_queue = false;
                delayed_entry_map.erase(i);
            }
        }
        if (likely(consider_queue)) {
            // here we cannot used pop directly as it would deadlock
            LogReplayEntry replay_entry;
            if (!this->replay_event_queue_.try_pop(replay_entry)) { // Note: non-const reference
                // no entries in queue
                if (!direct_replay_queue_empty_condition_.Broadcast()) {
                    WARNING("Failed to broadcast direct replay queue empty condition");
                }
                if (!log_condition_lock_.AcquireLock()) {
                    WARNING("Failed to acquire lock");
                }
                timed_bool t = this->log_condition_.ConditionWaitTimeout(&log_condition_lock_, 1);
                if (t == TIMED_FALSE) {
                    WARNING("Failed to wait for log entry");
                }
                // if timeout or new event, we simply continue
                if (!log_condition_lock_.ReleaseLock()) {
                    WARNING("Failed to release lock");
                }
                if (this->state_ == LOG_STATE_STOPPED) {
                    // we exit if there is no item to replay and if the log state changed.
                    should_exit = true;
                    continue;
                }
            } else {
                // replay entry set
                TRACE("Pop replay event: " << replay_entry.DebugString() << ", last directly replayed log id "
                                           << last_directly_replayed_log_id_);

                if (last_directly_replayed_log_id_ + 1 == replay_entry.log_id()) {
                    // this item is the next

                    TRACE("Replay event (direct): " << replay_entry.DebugString());
                    if (!ReplayDirectReplayEntry(replay_entry)) {
                        ERROR("Failed to replay direct replay: " << replay_entry.DebugString() <<
                            " last directly replayed log id " << last_directly_replayed_log_id_ <<
                            " last replayed log id " << replay_id_);
                    }
                    last_directly_replayed_log_id_ += replay_entry.log_id_count();
                } else if (last_directly_replayed_log_id_ >= replay_entry.log_id()) {
                    WARNING("Illegal last directly replayed log id: " << last_directly_replayed_log_id_
                                                                      << ", replay entry " << replay_entry.DebugString());
                } else {
                    TRACE("Place event in delayed event map: " << replay_entry.DebugString());
                    delayed_entry_map[replay_entry.log_id()] = replay_entry;
                }
            }
        }
    }
    DEBUG("Direct replay thread existed: state " << this->state_);
    return true;
}

bool Log::WriteNextEntry(const LogEventData& event_data, int64_t* log_id_given, uint32_t* log_id_count,
                         dedupv1::base::ErrorContext* ec) {
    CHECK(this->log_data_, "Log data not set");

    uint32_t id_count = 1;
    event_data.ByteSize();
    if (event_data.GetCachedSize() % this->max_log_value_size_per_bucket_ == 0) {
        id_count = event_data.GetCachedSize() / this->max_log_value_size_per_bucket_;
    } else {
        id_count = (event_data.GetCachedSize() / this->max_log_value_size_per_bucket_) + 1;
    }

    if (id_count == 0) {
        id_count = 1;
    } else if (id_count > 1) {
        stats_.multi_entry_event_count_++;
    }
    stats_.event_count_++;

    // large events need more than one id because there value is split over
    // multiple fixed length entries

    // get the id
    // TODO(fermat): should this be changed in uint64_t now?
    int64_t id = 0;
    {
        spin_mutex::scoped_lock l(this->lock_);
        id = this->log_id_;

        // acquire enough consecutive ids

        if (this->IsNearlyFull(id_count)) {
            if (ec) {
                ec->set_full();
            }
            // It is not possible to perform that operation now
            ERROR("Log full: " << "log id " << this->log_id_ << ", replay id " << this->replay_id_ << ", limit "
                               << this->log_data_->GetLimitId());
            return false;
        }

        in_progress_log_id_set_.insert(id);

        // We have to update the LogID within the lock, so we can be sure, that we only have to scan the
        // area between last saved log_id and last saved log_id + consistency area after a crash.
        int64_t new_log_id = id + id_count;
        if ((new_log_id / this->log_id_update_intervall_) > (id / this->log_id_update_intervall_)) {
            PersistLogID(new_log_id);
        }

        this->log_id_ = new_log_id;
    }
    if (log_id_given) {
        *log_id_given = id;
    }
    if (log_id_count) {
        *log_id_count = id_count;
    }
    if (!this->WriteEntry(id, id_count, event_data)) {
        ERROR("Failed to write log entry: " << "id " << id << ", partial count " << id_count << ", entry "
                                            << FriendlySubstr(event_data.ShortDebugString(), 0, 256, " ..."));
        spin_mutex::scoped_lock l(this->lock_);
        in_progress_log_id_set_.erase(id);
        return false;
    }

    spin_mutex::scoped_lock l(this->lock_);

    bool least_log_event = (*in_progress_log_id_set_.begin() == id);
    if (least_log_event) {
        TRACE("Least log event: log id " << id);
    }
    in_progress_log_id_set_.erase(id);

    if (likely(least_log_event)) {
        if (likely(id > last_fully_written_log_id_)) {
            last_fully_written_log_id_ = id;
            TRACE("Is last fully written log id " << id);
        }
    }
    return true;
}

Option<bool> Log::CheckLogId() {
    int64_t next_read_id = this->replay_id_;
    int64_t max_check_id = std::min(this->log_id_ + this->log_id_update_intervall_, replay_id_
        + this->log_data_->GetLimitId());
    int64_t real_log_id = 0;
    bool found_any_element = false;
    int64_t real_replay_id = max_check_id;
    INFO("Checking if replay id " << this->replay_id_ << " and log id " << this->log_id_ << " are correct.");
    int64_t last_fully_written_id = 0;
    list<int64_t> clear_ids;

    // I check the whole log here. This way I can remove possible events, which are not written completely, as this is not atomic.
    while (next_read_id < max_check_id) {
        TRACE("Checking id " << next_read_id);
        uint64_t checked_log_id = next_read_id;
        bool found_element = false;
        bool error = false;
        enum lookup_result result;
        LogEntryData event_data;
        int64_t next_read_posistion = GetLogPositionFromId(next_read_id);
        result = this->ReadEntryRaw(next_read_posistion, &event_data);
        CHECK(result != LOOKUP_ERROR, "Error reading log id " << next_read_id << " at position " << next_read_posistion);
        if (result == LOOKUP_FOUND) {
            TRACE("  Found entry at position " << next_read_posistion);
            CHECK(event_data.has_log_id(), "Event " << next_read_id << " has no log id: position " << next_read_posistion);
            if (event_data.log_id() >= replay_id_) {
                found_element = true;
                TRACE("    Entry at position " << next_read_posistion << " is not replayed");

                // update last fully written log id
                if (event_data.has_last_fully_written_log_id() && (event_data.last_fully_written_log_id()
                                                                   > last_fully_written_id)) {
                    last_fully_written_id = event_data.last_fully_written_log_id();
                }
                CHECK(event_data.log_id() == next_read_id, "Log event with illegal id found: log id " << event_data.log_id() << ", expected log id " << next_read_id);
                if ((event_data.has_partial_count()) && (event_data.partial_count() > 1)) {
                    TRACE("      Entry at position " << next_read_posistion << " is multi partitioned");
                    CHECK(event_data.has_partial_index(), "Event " << next_read_id << " at Position " << next_read_posistion << " has partial count " << event_data.partial_count() << " but no partial index");
                    CHECK(event_data.partial_index() < event_data.partial_count(), "Event " << next_read_id << " at Position " << next_read_posistion << " has partial count " << event_data.partial_count() << " but illegal partial index " << event_data.partial_index());
                    uint32_t first_entry = event_data.partial_index();
                    uint32_t read_part = first_entry + 1;
                    next_read_id -= first_entry;
                    bool error = (first_entry != 0); // TODO (dmeister) Am I wrong or do we run into problems where when this is true? We may delete to much at L771 right?
                    while (read_part < event_data.partial_count()) {
                        LogEntryData inner_event_data;
                        int64_t entry_id = next_read_id + read_part;
                        int64_t entry_pos = GetLogPositionFromId(entry_id);
                        result = this->ReadEntryRaw(entry_pos, &inner_event_data);
                        CHECK(result != LOOKUP_ERROR, "Error reading partition " << read_part <<
                            ", id " << entry_id <<
                            ", position " << entry_pos << " of log id " << next_read_id << " at position " << next_read_posistion);
                        if (result == LOOKUP_NOT_FOUND) {
                            INFO("Could not find partition " << read_part << ", id " << entry_id << ", position "
                                                             << entry_pos << " of log id " << next_read_id << " at position "
                                                             << next_read_posistion);
                            error = true;
                        } else {
                            // LOOKUP_FOUND
                            CHECK(inner_event_data.has_log_id(), "Partition " << read_part << ", id " << entry_id << ", position " << entry_pos << " of event " << next_read_id << " at Position " << next_read_posistion << " had no log_id");
                            if (inner_event_data.log_id() < replay_id_) {
                                INFO("Partition " << read_part << ", id " << entry_id << ", position " << entry_pos
                                                  << " of log id " << next_read_id << " at position " << next_read_posistion
                                                  << " has to old log_id: " << inner_event_data.log_id()
                                                  << " while replay id is " << replay_id_);
                                error = true;
                            } else {
                                CHECK(inner_event_data.log_id() == entry_id, "Partition " << read_part <<
                                    ", id " << entry_id << ", position " << entry_pos << " of log id " << next_read_id << " at position " << next_read_posistion << " had illegal log_id " << inner_event_data.log_id());
                                CHECK(inner_event_data.has_partial_count(), "Partition " << read_part <<
                                    ", id " << entry_id << ", position " << entry_pos << " of log id " << next_read_id << " at position " << next_read_posistion << " has no partial count.");
                                CHECK(inner_event_data.has_partial_index(), "Partition " << read_part <<
                                    ", id " << entry_id << ", position " << entry_pos << " of log id " << next_read_id << " at position " << next_read_posistion << " has no partial index.");
                                CHECK(inner_event_data.partial_count() == event_data.partial_count(),
                                    "Partition " << read_part << ", id " << entry_id << ", position " << entry_pos << " of log id " << next_read_id << " at position " << next_read_posistion << " had partial count " << inner_event_data.partial_count() << " but should have " << event_data.partial_count());
                                CHECK(inner_event_data.partial_index() == read_part, "Partition " << read_part <<
                                    ", id " << entry_id << ", position " << entry_pos << " of log id " << next_read_id << " at position " << next_read_posistion << " had partial index " << inner_event_data.partial_index() << " but should have " << read_part);
                            }
                        }
                        read_part++;
                    }
                    if (error) {
                        WARNING("Log ID " << next_read_id << " has not been written correctly.");
                        // so I will have to remote it
                        for (int32_t i = first_entry; i < event_data.partial_count(); i++) {
                            clear_ids.push_back(next_read_id + i);
                        }
                    }
                    next_read_id += event_data.partial_count() - 1; // -1, so i can use a general next_read_id++ at the end of the loop
                }
            } else {
                clear_ids.push_back(next_read_id); // TODO (dmeister) To we clear ids or positions???
            }
        } else {
            clear_ids.push_back(next_read_posistion);
            TRACE("  Found no entry at position " << next_read_posistion);
        }

        next_read_id++;
        if (found_element) {
            real_log_id = next_read_id;
            if (unlikely(!found_any_element)) {
                if (!error) {
                    found_any_element = true;
                    real_replay_id = checked_log_id;
                }
            }
        }
    }

    // We now have all failed elements in the area in clear_ids. The elements bigger real_log_id have to be ignored.
    // If there are elements small last_fully_written_id we have an error. Any other elements have to overwritten by None-events.
    for (std::list<int64_t>::iterator it = clear_ids.begin(); it != clear_ids.end(); it++) {
        CHECK(*it > last_fully_written_id, "Shall clear element " << *it << " but up to id " << last_fully_written_id << " anything should be o.k.");
        if (*it < real_log_id) {
            MakeValidEntry(*it);
        }
    }

    if (likely(real_log_id > this->log_id_)) {
        INFO("Corresponding to database we can use " << real_log_id << " as next log id, the saved next log id was "
                                                     << this->log_id_ << ". Will use the one from database.");
        PersistLogID(real_log_id);
        this->log_id_ = real_log_id;
    } else if (unlikely(real_log_id < this->log_id_)) {
        // In this case we keep the existing log_id, as there could be direct replayed events in this area. We just inform.
        INFO("Corresponding to database we can use " << real_log_id << " as next log id, the saved next log id was "
                                                     << this->log_id_ << ". Will use the saved next log id.");
    } else {
        INFO("Corresponding to database we can use " << real_log_id << " as next log id, the saved next log id was "
                                                     << this->log_id_ << ". As both are same, we use this value.");
    }

    if (real_replay_id > this->log_id_) {
        INFO("There are no elements to replay in the log.");
        // Corresponding to our old scheme, this is a problem...
        real_replay_id = this->log_id_;
    }
    if (unlikely(real_replay_id > this->replay_id_)) {
        INFO("Changing replay id from " << this->replay_id_ << " to " << real_replay_id
                                        << " as there were some places at the beginning, which we have not to replay.");
        PersistReplayID(real_replay_id);
        this->replay_id_ = real_replay_id;
    }

    return make_option(true);
}

bool Log::MakeValidEntry(int64_t id) {
    LogEventData log_value;
    log_value.set_event_type(EVENT_TYPE_NONE);

    if (!this->WriteEntry(id, 1, log_value)) {
        ERROR("Failed to mark event as event: " << id);
        return false;
    }
    return true;
}

bool Log::WriteEntry(int64_t first_id, int64_t id_count, const LogEventData& log_event) {
    DCHECK(this->log_data_, "Log data not set");
    ProfileTimer timer(this->stats_.write_time_);

    TRACE("Write log entry: type " << Log::GetEventTypeName(static_cast<enum event_type>(log_event.event_type()))
                                   << ", event value " << FriendlySubstr(log_event.ShortDebugString(), 0, 256, "...") << ", first id "
                                   << first_id << ", first position " << this->GetLogPositionFromId(first_id) << ", count " << id_count);

    bytestring buffer;
    CHECK(SerializeMessageToString(log_event, &buffer),
        "Failed to serialize log event: " << log_event.ShortDebugString());

    int64_t local_last_fully_committed_log_id = last_fully_written_log_id_;
    LogEntryData event_data;
    event_data.set_last_fully_written_log_id(local_last_fully_committed_log_id);
    if (id_count > 1) {
        event_data.set_partial_count(id_count);
    }

    size_t event_value_pos = 0;
    for (int64_t i = 0; i < id_count; i++) {
        if (id_count > 1) {
            event_data.set_partial_index(i);
        }
        event_data.set_log_id(first_id + i);

        size_t partial_value_size = (log_event.ByteSize() - event_value_pos);
        if (partial_value_size > this->max_log_value_size_per_bucket_) {
            partial_value_size = this->max_log_value_size_per_bucket_;
        }
        event_data.set_value(buffer.data() + event_value_pos, partial_value_size);

        int64_t position = this->GetLogPositionFromId(first_id + i);
        TRACE("Putting to position " << position << ": " << "log id " << first_id << ", entry " << FriendlySubstr(
                event_data.ShortDebugString(), 0, 64, "..."));
        enum put_result r = this->log_data_->Put(&position, sizeof(position), event_data);
        CHECK(r != PUT_ERROR, "Failed to write log data: " << event_data.ShortDebugString());
        event_value_pos += partial_value_size;
    }
    return true;
}

bool Log::RemoveEntry(int64_t pos) {
    CHECK(this->log_data_, "Log data not set");

    TRACE("Remove position: " << pos);

    CHECK(this->log_data_->Delete(&pos, sizeof(pos)) != DELETE_ERROR, "Cannot remove log entry: position " << pos);
    return true;
}

enum lookup_result Log::ReadEntryRaw(int64_t position, LogEntryData* data) {
    CHECK_RETURN(data, LOOKUP_ERROR, "Data not set");
    CHECK_RETURN(position >= 0, LOOKUP_ERROR, "Position is negative: position " << position);

    enum lookup_result result = this->log_data_->Lookup(&position, sizeof(position), data);
    CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR,
        "Error reading log entry: position " << position);
    if (result == LOOKUP_NOT_FOUND) {
        return result;
    }
    return LOOKUP_FOUND;
}

Log::log_read Log::ReadEntry(int64_t id, LogEntryData* log_entry, bytestring* log_value, uint32_t* partial_count) {
    DCHECK_RETURN(log_entry, LOG_READ_ERROR, "Log entry not set");
    DCHECK_RETURN(log_value, LOG_READ_ERROR, "Log value not set");

    int64_t pos = this->GetLogPositionFromId(id);
    LogEntryData event_data;
    TRACE("Reading id " << id << " from position " << pos);
    enum lookup_result result = this->ReadEntryRaw(pos, &event_data);
    CHECK_RETURN(result != LOOKUP_ERROR, LOG_READ_ERROR, "Error reading log entry: " <<
        "log id " << id <<
        ", log position " << pos);
    if (result == LOOKUP_NOT_FOUND) {
        // No log entry at that position
        WARNING("Illegal log entry at position " << id << " (next used log id is " << this->log_id_ << ")");
        return LOG_READ_NOENT;
    }
    if ((event_data.has_partial_count() && event_data.partial_index() != 0)) {
        DEBUG("Tried to read partial log entry: " <<
            "current id " << id <<
            ", current position " << pos <<
            ", partial index " << event_data.partial_index() <<
            ", partial count " << event_data.partial_count());
        return LOG_READ_PARTIAL;
    }

    TRACE("Read log entry " << " current id " << id << ", current position " << pos << ", partial index "
                            << event_data.partial_index() << ", partial count " << event_data.partial_count());

    if (event_data.has_partial_count()) { // we checked before that it is the first
        TRACE("Log entry has " << event_data.partial_count() << " elements: " << "current log id " << id
                               << ", position " << pos);
        log_value->append(reinterpret_cast<const byte*>(event_data.value().data()), event_data.value().size());
        if (partial_count) {
            *partial_count = event_data.partial_count();
        }

        uint32_t event_partial_count = event_data.partial_count();
        for (uint32_t i = 1; i < event_partial_count; i++) {
            int64_t position_key = (pos + i) % this->log_data_->GetLimitId();
            LogEntryData event_data;
            enum lookup_result result = this->log_data_->Lookup(&position_key, sizeof(position_key), &event_data);
            CHECK_RETURN(result == LOOKUP_FOUND, LOG_READ_ERROR, "Failed to find partial log data: " <<
                "position key " << position_key <<
                ", partial index " << i <<
                ", partial count " << event_partial_count <<
                ", log id " << id <<
                ", log position " << pos <<
                ", result " << (result == LOOKUP_NOT_FOUND ? "not found" : "error"));

            CHECK_RETURN(event_data.has_partial_index() && event_data.partial_index() == i, LOG_READ_ERROR,
                "Illegal partial index: " <<
                "position " << position_key <<
                ", i " << i <<
                ", partial index " << event_data.partial_index() <<
                ", partial count " << event_data.partial_count() <<
                ", log id " << event_data.log_id());

            log_value->append(reinterpret_cast<const byte*>(event_data.value().data()), event_data.value().size());
        }
    } else {
        if (partial_count) {
            *partial_count = 1;
        }
        log_value->append(reinterpret_cast<const byte*>(event_data.value().data()), event_data.value().size());
    }
    return LOG_READ_OK;
}

bool Log::Close() {
    bool failed = false;
    if (!this->Stop(dedupv1::StopContext::FastStopContext())) {
        WARNING("Failed to stop log");
    }

    if (this->consumer_list_.size() != 0) {
        vector<string> consumer_names;
        list<LogConsumerListEntry>::iterator i;
        for (i = this->consumer_list_.begin(); i != this->consumer_list_.end(); i++) {
            consumer_names.push_back(i->name());
        }
        WARNING("Log should not close with consumers: [" << Join(consumer_names.begin(), consumer_names.end(), ", ")
                                                         << "]");
    }

    DEBUG("Closing log: replay id " << this->replay_id_ << ", log id " << this->log_id_);

    if (state_ != LOG_STATE_CREATED) {
        // if the log is not started, there is nothing to dump
        if (!DumpMetaInfo()) {
            WARNING("Failed to dump meta info");
            failed = true;
        }
    }

    if (this->log_data_) {
        if (!this->log_data_->Close()) {
            WARNING("Error closing log data");
            failed = true;
        }
        this->log_data_ = NULL;
    }
    this->replay_event_queue_.clear();

    if (!this->log_info_store_.Close()) {
        ERROR("Cannot close log info store");
        failed = true;
    }
    delete this;
    return !failed;
}

namespace {

/**
 * Fill the correct sub field of the log event data
 */
bool FillEventData(LogEventData* event_data, const EventTypeInfo& event_type_info,
                   const google::protobuf::Message* message) {
    DCHECK(event_data, "Event data not set");

    int field_nummer = event_type_info.event_data_message_field();
    if (field_nummer == 0) {
        TRACE("Found no field number");
        return true;
    }
    DCHECK(message, "Mandatory event value not set"); // A message must be set, if the type as a field number

    const FieldDescriptor* field = event_data->GetDescriptor()->FindFieldByNumber(field_nummer);
    DCHECK(field, "Failed to find field data");
    DCHECK(field->message_type()->name() == message->GetTypeName(), "Type name mismatch: "
        "field " << field->DebugString() <<
        ", type " << field->message_type()->DebugString() <<
        ", message type " << message->GetTypeName());

    Message* type_message = event_data->GetReflection()->MutableMessage(event_data, field);
    DCHECK(type_message, "Type message not set");
    type_message->MergeFrom(*message);
    return true;
}
}

bool Log::CommitEvent(enum event_type event_type, const google::protobuf::Message* message, int64_t* commit_log_id,
                      LogAckConsumer* ack, dedupv1::base::ErrorContext* ec) {
    CHECK(!readonly_, "Log is in readonly mode");
    CHECK(IsStarted(), "Log not started"); // it is important that events can be committed after stop

    ProfileTimer timer(this->stats_.commit_time_);
    tbb::tick_count start_tick = tbb::tick_count::now();

#ifdef DEDUPV1_CORE_TEST
    // if the data is cleared during a test, we simply what to bring the system down
    if (this->data_cleared) {
        return true;
    }
#endif

    int64_t current_log_id = 0;
    uint32_t current_log_id_count = 0;

    TRACE("Prepare commit: " << Log::GetEventTypeName(event_type) <<
        ", event value " << (message ? FriendlySubstr(message->ShortDebugString(), 0, 256, " ...") : "null")
        << ", event size " << (message ? message->ByteSize() : 0));

    const EventTypeInfo& event_type_info(EventTypeInfo::GetInfo(event_type));
    LogEventData event_data;
    CHECK(FillEventData(&event_data, EventTypeInfo::GetInfo(event_type), message),
        "Failed to get event data " << event_type <<
        ", event value " << (message ? message->ShortDebugString() : "null"));

    if (event_type_info.is_persistent()) {
        event_data.set_event_type(event_type);
        if (!this->WriteNextEntry(event_data, &current_log_id, &current_log_id_count, ec)) {

            // they write failed, but we have to push the event as failed to the replay queue so that
            // the replay thread can update its data
            if (current_log_id != 0 && current_log_id_count != 0) {
                ERROR("Failed to write next log entry: event type " << Log::GetEventTypeName(event_type));
                if (this->state_ == LOG_STATE_RUNNING && event_type != EVENT_TYPE_LOG_EMPTY) {
                    LogReplayEntry replay_entry(current_log_id, event_type, event_data, true, current_log_id_count);
                    TRACE("Push replay event: " << replay_entry.DebugString());
                    this->replay_event_queue_.push(replay_entry);
                }
            }
            return false;
        }
        DEBUG("Committed event: "
            << "event log id " << current_log_id <<
            ", entry count " << current_log_id_count <<
            ", type " << Log::GetEventTypeName(event_type) <<
            ", event value " << (message ? FriendlySubstr(message->ShortDebugString(), 0, 256, " ...") : "null") <<
            ", event size " << (message ? message->ByteSize() : 0));
        if (commit_log_id) {
            *commit_log_id = current_log_id;
        }
    }
    FAULT_POINT("log.commit.before-ack");
    LogReplayContext replay_context(EVENT_REPLAY_MODE_DIRECT, current_log_id);

    if (ack) {
        tbb::tick_count ack_start_tick = tbb::tick_count::now();
        CHECK(ack->LogAck(event_type, message, replay_context), "Failed to acknowledge log event: " <<
            "event type " << Log::GetEventTypeName(event_type) <<
            ", message " << (message ? message->ShortDebugString() : ""));
        tbb::tick_count ack_end_tick = tbb::tick_count::now();
        this->stats_.average_ack_latency_.Add((ack_end_tick - ack_start_tick).seconds() * 1000);
    } else {
        this->stats_.average_ack_latency_.Add(0);
    }

    FAULT_POINT("log.commit.before-publish");
    // before the RUNNING state, the replay thread is not started so the data would never
    // be replayed
    if (this->state_ != LOG_STATE_RUNNING) {
        if (!this->PublishEvent(replay_context, event_type, event_data)) {
            WARNING("Cannot publish event: " << "event type " << Log::GetEventTypeName(event_type) << ", event data "
                                             << event_data.ShortDebugString());
        }
    } else if (event_type == EVENT_TYPE_LOG_EMPTY) {
        // Log empty events should (for some reason I do not remeber) be replayed outside the replay thread
        if (!this->PublishEvent(replay_context, event_type, event_data)) {
            WARNING("Cannot publish event: " << "event type " << Log::GetEventTypeName(event_type) << ", event data "
                                             << event_data.ShortDebugString());
        }
        // However, we inform the replay thread about the event so that the thread can managed its state correctly
        LogReplayEntry replay_entry(current_log_id, event_type, event_data, false, current_log_id_count);
        TRACE("Push replay event: " << replay_entry.DebugString());
        this->replay_event_queue_.push(replay_entry);
    } else {
        LogReplayEntry replay_entry(current_log_id, event_type, event_data, false, current_log_id_count);
        TRACE("Push replay event: " << replay_entry.DebugString());
        this->replay_event_queue_.push(replay_entry);
    }
    CHECK(this->log_condition_.Broadcast(), "Cannot broadcast log commit");
    tbb::tick_count end_tick = tbb::tick_count::now();
    this->stats_.average_commit_latency_.Add((end_tick - start_tick).seconds() * 1000);
    return true;
}

Option<bool> Log::IsRegistered(const std::string& consumer_name) {
    spin_rw_mutex::scoped_lock l(this->consumer_list_lock_);
    list<LogConsumerListEntry>::iterator i;
    for (i = this->consumer_list_.begin(); i != this->consumer_list_.end(); i++) {
        if (i->name() == consumer_name) {
            return make_option(true);
        }
    }
    return make_option(false);
}

bool Log::RegisterConsumer(const string& consumer_name, LogConsumer* consumer) {
    CHECK(consumer, "Consumer replay function not set");
    CHECK(consumer_name.size() < 128, "Consumer name too long");
    CHECK(consumer_name.size() > 0, "Consumer name too long");

    DEBUG("Register consumer: name " << consumer_name);

    // acquire the lock for writing without setting of lock into the
    // write lock waiting state.
    // If we would go into the write lock waiting state, further requests to acquire
    // the lock for reading would block and this might lead to a deadlock since
    // the log may acquire read locks during it holds read locks.
    spin_rw_mutex::scoped_lock l;
    while (!l.try_acquire(this->consumer_list_lock_)) {
        sleep(1);
    }

    list<LogConsumerListEntry>::iterator i;
    for (i = this->consumer_list_.begin(); i != this->consumer_list_.end(); i++) {
        if (i->name() == consumer_name) {
            ERROR("Found already registered consumer: " << consumer_name);
            return false;
        }
    }
    LogConsumerListEntry entry(consumer_name, consumer);
    this->consumer_list_.push_back(entry);
    return true;
}

bool Log::UnregisterConsumer(const string& consumer_name) {
    DEBUG("Remove log consumer " << consumer_name);

    // waiting until empty
    CHECK(this->WaitUntilDirectReplayQueueEmpty(0),
        "Failed to wait until direct replay finished");

    // acquire the lock for writing without setting of lock into the
    // write lock waiting state.
    // If we would go into the write lock waiting state, further requests to acquire
    // the lock for reading would block and this might lead to a deadlock since
    // the log may acquire read locks during it holds read locks.
    spin_rw_mutex::scoped_lock l;
    while (!l.try_acquire(this->consumer_list_lock_)) {
        sleep(1);
    }
    list<LogConsumerListEntry>::iterator i;
    for (i = this->consumer_list_.begin(); i != this->consumer_list_.end(); i++) {
        if (i->name() == consumer_name) {
            this->consumer_list_.erase(i);
            return true;
        }
    }
    ERROR("Cannot find log consumer: " << consumer_name);
    return false;
}

bool Log::PublishEvent(const LogReplayContext& replay_context, enum event_type event_type,
                       const LogEventData& event_data) {
    ProfileTimer timer(this->stats_.publish_time_);
    bool success = true;

    TRACE("Publish event: " << Log::GetEventTypeName(event_type) << ", replay mode " << Log::GetReplayModeName(
            replay_context.replay_mode()) << ", event log id " << replay_context.log_id());

    spin_rw_mutex::scoped_lock l(this->consumer_list_lock_, false);
    list<LogConsumerListEntry>::iterator i;
    for (i = consumer_list_.begin(); i != consumer_list_.end(); i++) {
        LogConsumer* consumer = i->consumer();
        CHECK(consumer, "Consumer not set");

        // This is a bad HACK to set the direct replay state. This
        // assumes that if PublishEvent is called in direct replay mode
        // the direct replay thread is used which is usually the case, but
        // I am not happy with this code
        if (replay_context.replay_mode() == EVENT_REPLAY_MODE_DIRECT && this->state_ == LOG_STATE_RUNNING && event_type
            != EVENT_TYPE_LOG_EMPTY) {
            direct_replay_state_.SetConsumer(i->name());
        }

        const string& consumer_name(i->name());
        if (!consumer->LogReplay(event_type, event_data, replay_context)) {
            WARNING("Replay failed: " << Log::GetEventTypeName(event_type) <<
                ", log consumer " << consumer_name <<
                ", log id " << replay_context.log_id() <<
                ", replay mode " << Log::GetReplayModeName(replay_context.replay_mode()));
            success = false;
        }
    }
    // RACE CONDITION
    // if a "publish" is active, a delete is pushed to a list
    return success;
}

bool Log::ReplayStart(enum replay_mode replay_mode, bool is_full_replay, bool commit_replay_event) {
    spin_mutex::scoped_lock scoped_replaying_lock(this->is_replaying_lock_);
    CHECK(!is_replaying_, "Log is already replaying");

    INFO("Started replay: mode " << GetReplayModeName(replay_mode) <<
        ", is full replay " << ToString(is_full_replay) <<
        ", replay id " << replay_id_ <<
        ", log id " << log_id_);

    if (commit_replay_event) {
        spin_mutex::scoped_lock l(this->lock_);
        ReplayStartEventData event_data;
        event_data.set_replay_type(replay_mode);
        event_data.set_log_id(log_id_);
        event_data.set_replay_id(replay_id_);
        l.release();

        CHECK(this->CommitEvent(EVENT_TYPE_REPLAY_STARTED, &event_data, NULL, NULL, NO_EC),
            "Cannot commit replay started event");
    }
    this->is_replaying_ = true;

    return true;
}

log_replay_result Log::Replay(enum replay_mode replay_mode,
                              uint32_t number_to_replay,
                              uint64_t* replayed_log_id,
                              uint32_t* number_replayed) {
    // We divide the problem in three tasks:
    // 0.) Some initialization...
    // 1.) Process Events
    // 2.) Delete Events (if Background run)

    // Initialization
    CHECK_RETURN(replay_mode != EVENT_REPLAY_MODE_DIRECT, LOG_REPLAY_ERROR, "Illegal replay mode");

    if (number_replayed) {
        (*number_replayed) = 0;
    }
    if (number_to_replay == 0) {
        WARNING("Replay called to process 0 events...");
        return LOG_REPLAY_OK;
    }

    spin_mutex::scoped_lock l(this->lock_);
    uint64_t current_log_id = this->log_id_;
    uint64_t current_last_empty_log_id = this->last_empty_log_id_;
    l.release();

    // We do not need a very actual state, so for performance reasons we keep this values here.
    uint64_t current_last_fully_written_log_id = this->last_fully_written_log_id_;
    int64_t current_replay_id = this->replay_id_;

    DEBUG("Shall replay " << number_to_replay << " elements in mode " << replay_mode << " beginning with id "
                          << current_replay_id << ", log id is " << current_log_id << ", last fully written log id is "
                          << current_last_fully_written_log_id << ", last empty log id " << current_last_empty_log_id);

    if (replayed_log_id) {
        // set the replay id as soon as possible
        *replayed_log_id = replay_id_;
    }

    // Process the events
    int32_t processed_entries = 0;
    int64_t last_processed_id = current_replay_id;
    log_replay_result result = LOG_REPLAY_OK;
    LogEntryData entry;
    bytestring buffer;
    int64_t next_replay_id = current_replay_id;
    event_type old_type = EVENT_TYPE_NONE;
    event_type new_type = EVENT_TYPE_NONE;

    bool log_empty = (next_replay_id > current_last_fully_written_log_id);
    if (log_empty) {
        TRACE("No elements to replay");
        return LOG_REPLAY_NO_MORE_EVENTS;
    }
    // if we are running (aka if we have direct replay) only replay items that are already replayed in the
    // direct replay queue. It is very hard to deal with this
    if (likely(this->state_ == LOG_STATE_RUNNING) && unlikely(next_replay_id > last_directly_replayed_log_id_)) {
        TRACE("Stop replay: next replay id " << next_replay_id <<
            ", last directly replayed id " << last_directly_replayed_log_id_);
        return LOG_REPLAY_NO_MORE_EVENTS;
    }

    tbb::tick_count start_tick = tbb::tick_count::now();
    ProfileTimer timer(this->stats_.replay_time_);

    enum Log::log_read read_result = LOG_READ_OK;
    if (!is_last_read_event_data_valid_) {
        read_result = this->ReadEvent(next_replay_id, &last_read_partial_count_, &last_read_event_data_);
    }
    if (read_result == LOG_READ_OK) {
        old_type = static_cast<enum event_type>(last_read_event_data_.event_type());
        new_type = old_type;
    }

    while (!log_empty && (read_result == LOG_READ_OK) && (result = LOG_REPLAY_OK) && (old_type == new_type)
           && (processed_entries < number_to_replay)) {
        TRACE("Will try to publish log id " << last_processed_id);
        LogReplayContext replay_context(replay_mode, next_replay_id);
        is_last_read_event_data_valid_ = false;
        ProfileTimer timer_publish(this->stats_.replay_publish_time_);
        bool publish_result = this->PublishEvent(replay_context, old_type, last_read_event_data_);
        timer_publish.stop();
        if (!publish_result) {
            ERROR("Failed to publish event: " << entry.ShortDebugString() << ", event id " << next_replay_id
                                              << ", replay mode " << Log::GetReplayModeName(replay_mode));
            result = LOG_REPLAY_ERROR;
        } else {
            DEBUG("Replayed log event " << next_replay_id);
            if (last_read_partial_count_ == 0) {
                last_read_partial_count_++;
            }
            last_processed_id = next_replay_id;
            next_replay_id += last_read_partial_count_;
            processed_entries++;

            log_empty = (next_replay_id > current_last_fully_written_log_id);
            DEBUG("Check empty state: " << log_empty <<
                ", next replay id " << next_replay_id <<
                ", current log id " << current_log_id <<
                ", current last empty log id " << current_last_empty_log_id <<
                ", current last fully written log id " << current_last_fully_written_log_id);

            if (!log_empty) {
                ProfileTimer timer_read(this->stats_.replay_read_time_);
                read_result = this->ReadEvent(next_replay_id, &last_read_partial_count_, &last_read_event_data_);
                new_type = static_cast<enum event_type>(last_read_event_data_.event_type());
                is_last_read_event_data_valid_ = true;
            }
        }
    }

    if (read_result != LOG_READ_OK) {
        is_last_read_event_data_valid_ = false;
        ERROR("Error while reading event " << next_replay_id <<
            ", result was " << read_result <<
            ", current log id is " << current_log_id <<
            ", current last fully written id is " <<
            current_last_fully_written_log_id <<
            ", current last empty log id is " << current_last_empty_log_id);
        result = LOG_REPLAY_ERROR;
    }

    TRACE("After publishing " << processed_entries << " events" <<
        ", next replay id " << next_replay_id <<
        ", last processed id is " << last_processed_id);
    // Delete events
    if ((replay_mode == EVENT_REPLAY_MODE_REPLAY_BG) && (next_replay_id != current_replay_id)) {
        // We persist the replay ID only when replaying in Background mode, not
        // during Dirty Restart.
        if (log_empty) {
            // I think it is a good idea to update the current log_id_ and the
            // last_fully_written_log_id_ here,
            // because the Publishing could have taken some time and therefore
            // it might be possible that the
            // log is no more empty.
            spin_mutex::scoped_lock l2(this->lock_);
            current_log_id = this->log_id_;
            current_last_empty_log_id = this->last_empty_log_id_;
            l2.release();
            current_last_fully_written_log_id = this->last_fully_written_log_id_;
            log_empty = (next_replay_id >= current_last_fully_written_log_id);
            DEBUG("Re-check empty state: " << log_empty <<
                ", next replay id " << next_replay_id <<
                ", current log id " << current_log_id <<
                ", current last empty log id " << current_last_empty_log_id <<
                ", current last fully written log id " << current_last_fully_written_log_id);
        }
        if (log_empty && next_replay_id > current_last_empty_log_id) {
            is_last_read_event_data_valid_ = false;
            TRACE("Will commit empty event");
            int64_t empty_log_id = 0;
            CHECK_RETURN(this->CommitEvent(EVENT_TYPE_LOG_EMPTY, NULL, &empty_log_id, NULL, NO_EC),
                LOG_REPLAY_ERROR, "Cannot commit log empty event");
            DEBUG("Write empty log event to id " << empty_log_id);
            // it is not probable to run into race conditions here, but so we are on the save side
            spin_mutex::scoped_lock l(this->lock_);
            if (empty_log_id > this->last_empty_log_id_) {
                this->last_empty_log_id_ = empty_log_id;
            }
        }
        TRACE("Will delete events: log id " << current_replay_id << " to log id " << (next_replay_id - 1));
        ProfileTimer timer_update_id(this->stats_.replay_update_id_time_);
        PersistReplayID(next_replay_id);
    }
    this->replay_id_ = next_replay_id;

    if (replayed_log_id) {
        *replayed_log_id = last_processed_id;
    }

    if (number_replayed) {
        *number_replayed = processed_entries;
    }

    if ((result == LOG_REPLAY_OK) && (log_empty)) {
        TRACE("All elements replayed");
        result = LOG_REPLAY_NO_MORE_EVENTS;
    }

    if ((processed_entries > 0) && (replay_mode == EVENT_REPLAY_MODE_REPLAY_BG)) {

        this->stats_.replayed_events_ += processed_entries;
        this->stats_.replayed_events_by_type_[old_type] += processed_entries;

        tbb::tick_count end_tick = tbb::tick_count::now();
        this->stats_.average_replay_events_latency_.Add((end_tick - start_tick).seconds() * 1000);
        this->stats_.average_replayed_events_per_step_.Add(processed_entries);
        this->stats_.average_replay_events_latency_by_type_[old_type].Add((end_tick - start_tick).seconds() * 1000);
        this->stats_.average_replayed_events_per_step_by_type_[old_type].Add(processed_entries);
    }
    return result;
}

Log::log_read Log::ReadEvent(int64_t replay_log_id, uint32_t* partial_count, LogEventData* event_data) {
    CHECK_RETURN(event_data, LOG_READ_ERROR, "ReadEvent needs an event data object");

    tbb::tick_count start_tick = tbb::tick_count::now();

    LogEntryData entry;
    bytestring buffer;
    uint32_t local_partial_count = 1;
    enum log_read read_result = this->ReadEntry(replay_log_id, &entry, &buffer, &local_partial_count);
    if (partial_count) {
        *partial_count = local_partial_count;
    }

    if (read_result == LOG_READ_OK) {
        if (!event_data->ParseFromArray(buffer.data(), buffer.size())) {
            ERROR("Failed to fill event data");
            read_result = LOG_READ_ERROR;
        } else {
            if (unlikely(!event_data->has_event_type())) {
                ERROR("Illegal event data: " << event_data->ShortDebugString() << ", log id " << replay_log_id);
                read_result = LOG_READ_ERROR;
            }
            event_type event_type = static_cast<enum event_type>(event_data->event_type());

            DEBUG("Read event from log: " << "event " << FriendlySubstr(event_data->ShortDebugString(), 0, 256, " ...")
                                          << ", event type " << GetEventTypeName(event_type) << ", id " << replay_log_id
                                          << ", last persisted replay id " << this->replay_id_ << ", partial count " << local_partial_count);
        }
    }

    tbb::tick_count end_tick = tbb::tick_count::now();
    this->stats_.average_read_event_latency_.Add((end_tick - start_tick).seconds() * 1000);
    return read_result;
}

bool Log::ReplayStop(enum replay_mode replay_mode,
    bool success,
    bool commit_replay_event) {

    spin_mutex::scoped_lock scoped_replaying_lock(this->is_replaying_lock_);
    CHECK(is_replaying_, "Log is not replaying");

    if (commit_replay_event) {
        spin_mutex::scoped_lock l(this->lock_);
        ReplayStopEventData event_data;
        event_data.set_replay_type(replay_mode);
        event_data.set_success(success);
        event_data.set_log_id(log_id_);
        event_data.set_replay_id(replay_id_);
        l.release(); // should not call CommitEvent with lock held

        CHECK(this->CommitEvent(EVENT_TYPE_REPLAY_STOPPED, &event_data, NULL, NULL, NO_EC),
            "Cannot commit event: " << event_data.ShortDebugString());
    }

    this->is_replaying_ = false;

    INFO("Stopped replay: mode " << GetReplayModeName(replay_mode) <<
        ", success " << ToString(success) <<
        ", replay id " << replay_id_ <<
        ", log id " << log_id_);

    // note here that the log id and replay id logged in the replay stopped event might differ from the
    // version dumped into the meta data index

    if (replay_mode == EVENT_REPLAY_MODE_REPLAY_BG) {
        CHECK(this->DumpMetaInfo(), "Cannot dump meta data");
    }
    return true;
}

bool Log::WaitUntilDirectReplayQueueEmpty(uint32_t timeout) {
    if (this->state_ != LOG_STATE_RUNNING) {
        return true; // as we have no queue here, everything is replayed directly
    }
    DEBUG("Wait until direct replay queue empty event: timeout " << timeout);
    ScopedLock scoped_lock(&direct_replay_queue_empty_lock_);
    CHECK(scoped_lock.AcquireLock(),
        "Failed to acquire direct replay empty queue condition lock");

    if (timeout == 0) {
        CHECK(direct_replay_queue_empty_condition_.ConditionWait(&direct_replay_queue_empty_lock_),
            "Failed to wait for direct replay empty queue condition");
    } else {
        dedupv1::base::timed_bool b =
            direct_replay_queue_empty_condition_.ConditionWaitTimeout(&direct_replay_queue_empty_lock_, timeout);
        CHECK(b != dedupv1::base::TIMED_FALSE,
            "Failed to wait for direct replay empty queue condition");
        if (b == dedupv1::base::TIMED_TIMEOUT) {
            DEBUG("Timeout while waiting for empty direct replay lock");
        }
        if (replay_event_queue_.unsafe_size() > 0) {
            DEBUG("Still un-replayed items in event queue: " << replay_event_queue_.unsafe_size());
        }
    }
    return true;
}

string Log::GetEventTypeName(enum event_type event_type) {
    switch (event_type) {
    case EVENT_TYPE_CONTAINER_OPEN:
        return "Container Open";
    case EVENT_TYPE_CONTAINER_COMMIT_FAILED:
        return "Container Commit Failed";
    case EVENT_TYPE_CONTAINER_COMMITED:
        return "Container Committed";
    case EVENT_TYPE_CONTAINER_MERGED:
        return "Container Merged";
    case EVENT_TYPE_CONTAINER_MOVED:
        return "Container Moved";
    case EVENT_TYPE_CONTAINER_DELETED:
        return "Container Deleted";
    case EVENT_TYPE_BLOCK_MAPPING_DELETED:
        return "Block Mapping Deleted";
    case EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED:
        return "Block Mapping Write Failed";
    case EVENT_TYPE_BLOCK_MAPPING_WRITTEN:
        return "Block Mapping Written";
    case EVENT_TYPE_REPLAY_STARTED:
        return "Replay Started";
    case EVENT_TYPE_REPLAY_STOPPED:
        return "Replay Stopped";
    case EVENT_TYPE_REPLAY_COMMIT:
        return "Replay Commit";
    case EVENT_TYPE_LOG_EMPTY:
        return "Log Empty";
    case EVENT_TYPE_LOG_NEW:
        return "Log New";
    case EVENT_TYPE_VOLUME_ATTACH:
        return "Volume Attached";
    case EVENT_TYPE_VOLUME_DETACH:
        return "Volume Detached";
    case EVENT_TYPE_SYSTEM_START:
        return "System Start";
    case EVENT_TYPE_SYSTEM_RUN:
        return "System Run";
    case EVENT_TYPE_LOG_BARRIER:
        return "Log Barrier";
    case EVENT_TYPE_OPHRAN_CHUNKS:
        return "Ophran Chunks";
    default:
        return "Unknown event type (" + ToString(event_type) + ")";
    }
}

string Log::GetReplayModeName(enum replay_mode replay_mode) {
    if (replay_mode == EVENT_REPLAY_MODE_DIRECT) {
        return "Replay Direct";
    } else if (replay_mode == EVENT_REPLAY_MODE_REPLAY_BG) {
        return "Replay Background";
    } else if (replay_mode == EVENT_REPLAY_MODE_DIRTY_START) {
        return "Replay Dirty";
    }
    return "Unknown Replay mode";
}

bool Log::DumpEvent(enum replay_mode replay_mode, enum event_type event_type, byte* event_value, size_t event_size) {
    INFO(Log::GetReplayModeName(replay_mode) << "-" << Log::GetEventTypeName(event_type) << "-" << event_size);
    return true;
}

bool Log::PersistLogID(int64_t logID) {
    LogLogIDData logID_data;
    logID_data.set_log_id(logID);
    CHECK(log_info_store_.PersistInfo("logID", logID_data),
        "Failed to persist log id in log info data: " << logID_data.ShortDebugString());
    return true;
}

bool Log::PersistReplayID(int64_t replayID) {
    LogReplayIDData replayID_data;
    replayID_data.set_replay_id(replayID);
    CHECK(log_info_store_.PersistInfo("replayID", replayID_data),
        "Failed to persist replay id in log info data: " << replayID_data.ShortDebugString());
    return true;
}

bool Log::DumpMetaInfo() {
#ifdef DEDUPV1_CORE_TEST
    if (!this->log_data_) {
        return true;
    }
#endif
    DCHECK(this->log_data_, "Log database not set");

    LogStateData logState;
    logState.set_limit_id(this->log_data_->GetLimitId());
    logState.set_log_entry_width(this->max_log_entry_width_);
    CHECK(this->log_info_store_.PersistInfo("state", logState),
        "Failed to persist state in log info data: " << logState.ShortDebugString());

    PersistLogID(this->log_id_);
    PersistReplayID(this->replay_id_);

    return true;
}

lookup_result Log::ReadMetaInfo(LogLogIDData* logID_data, LogReplayIDData* replayID_data, LogStateData* state_data) {
    CHECK_RETURN(logID_data, LOOKUP_ERROR, "Log ID data not set");
    CHECK_RETURN(replayID_data, LOOKUP_ERROR, "Replay ID data not set");
    CHECK_RETURN(state_data, LOOKUP_ERROR, "State data not set");

    bool error = false;
    bool all_found = true;

    lookup_result lr_log_id = log_info_store_.RestoreInfo("logID", logID_data);
    if (lr_log_id == LOOKUP_ERROR) {
        ERROR("Error reading log ID");
        error = true;
    } else if (lr_log_id == LOOKUP_NOT_FOUND) {
        INFO("Could not find log ID, this is o.k., if we are creating");
        all_found = false;
    }
    lookup_result lr_replay_id = log_info_store_.RestoreInfo("replayID", replayID_data);
    if (lr_replay_id == LOOKUP_ERROR) {
        ERROR("Error reading replay ID");
        error = true;
    } else if (lr_replay_id == LOOKUP_NOT_FOUND) {
        if (all_found) {
            ERROR("Could not find replay ID, but log ID has been found before");
            error = true;
        } else {
            INFO("Could not find replay ID, this is o.k., if we are creating");
        }
    } else if (!all_found) {
        ERROR("Found Replay ID, but Log ID has not been found before.");
        error = true;
    }
    lookup_result lr_state = log_info_store_.RestoreInfo("state", state_data);
    if (lr_state == LOOKUP_ERROR) {
        ERROR("Error reading state");
        error = true;
    } else if (lr_replay_id == LOOKUP_NOT_FOUND) {
        if (all_found) {
            ERROR("Could not find state, but log ID and replay ID have been found before");
            error = true;
        } else {
            INFO("Could not find state, this is o.k., if we are creating");
        }
    } else if (!all_found) {
        ERROR("Found State, but Log ID or Replay ID have not been found before.");
        error = true;
    }

    if (error) {
        return LOOKUP_ERROR;
    }

    if (all_found) {
        return LOOKUP_FOUND;
    }

    return LOOKUP_NOT_FOUND;
}

bool Log::PerformDirtyReplay() {
    // we try there a) to detect the case of an invalid log if the system crashes during a log write and
    // b) to do keep the log intact. If the log replay fails (except for last case), call the developer, support,
    // batman, whom ever

    FullReplayLogConsumer consumer(this->replay_id(), this->log_id());

    int64_t old_replay_log_id = replay_id();
    bool failed = false;

    // we should take care that the log consumer is unregistered even in case of errors
    CHECK(this->RegisterConsumer("replay", &consumer), "Failed to register replay consumer");

    INFO("Perform dirty replay: replay id " << replay_id() << ", log id " << log_id());

    if (!this->ReplayStart(EVENT_REPLAY_MODE_DIRTY_START, true)) {
        ERROR("Error while starting log replay");
        failed = true;
    } else {
        log_replay_result result = LOG_REPLAY_OK;
        uint64_t replay_log_id = 0;
        while (result == LOG_REPLAY_OK) {
            replay_log_id = 0;
            result = this->Replay(EVENT_REPLAY_MODE_DIRTY_START, max_area_size_dirty_replay_, &replay_log_id, NULL);

            if (result == LOG_REPLAY_ERROR) {
                // do we know that that log event was written correctly? If not, than it is the result of the crash and it is ok
                if (replay_log_id <= last_fully_written_log_id_) {
                    ERROR("Error during log replay: replayed id " << replay_log_id << ", log id " << this->log_id()
                                                                  << ", last fully written log id at startup " << last_fully_written_log_id_);
                    failed = true;
                } else {
                    INFO("Replay failed, but is not fully written log entry (that may happen after crashes): "
                        << "replayed id " << replay_log_id << ", log id " << this->log_id()
                        << ", last fully written log id at startup " << this->last_fully_written_log_id_);
                }
            }
        }
        if (!this->ReplayStop(EVENT_REPLAY_MODE_DIRTY_START, !failed)) {
            ERROR("Error while stopping log replay");
            failed = true;
        }
    }

    CHECK(this->UnregisterConsumer("replay"), "Failed to unregister replay consumer");

    this->SetReplayPosition(old_replay_log_id);

    return !failed;
}

bool Log::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    LogStatsData data;
    data.set_event_count(stats_.event_count_);
    data.set_replayed_event_count(stats_.replayed_events_);
    for (int i = 0; i < EVENT_TYPE_NEXT_ID; i++) {
        uint64_t val = this->stats_.replayed_events_by_type_[i];
        LogStatsData_LogTypeCounter* counter = data.add_logtype_count();
        counter->set_type(i);
        counter->set_count(val);
    }
    CHECK(ps->Persist(prefix, data), "Failed to persist log stats");
    return true;
}

bool Log::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    LogStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore log stats");

    if (data.has_event_count()) {
        stats_.event_count_ = data.event_count();
    }
    if (data.has_replayed_event_count()) {
        stats_.replayed_events_ = data.replayed_event_count();
    }
    if (data.logtype_count_size() > 0) {
        for (int i = 0; i < data.logtype_count_size(); i++) {
            const LogStatsData_LogTypeCounter& counter = data.logtype_count(i);
            if (counter.has_count() && counter.has_type()) {
                this->stats_.replayed_events_by_type_[counter.type()] = counter.count();
            }
        }
    }
    return true;
}

string Log::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"log data\": " << (this->log_data_ ? this->log_data_->PrintLockStatistics() : "null") << std::endl;
    sstr << "}";
    return sstr.str();
}

double Log::GetFillRatio() {
    if (log_data_ == NULL || log_data_->GetEstimatedMaxItemCount() == 0) {
        return 0.0;
    }
    uint64_t unreplayed_items = log_id_ - replay_id_;
    double fill_ratio = 1.0 * unreplayed_items / log_data_->GetEstimatedMaxItemCount();
    return fill_ratio;
}

string Log::PrintStatistics() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{";
    sstr << "\"fill ratio\": ";
    if (log_data_ && log_data_->GetEstimatedMaxItemCount() != 0) {
        sstr << GetFillRatio() << ",";
    } else {
        sstr << "null,";
    }

    sstr << "\"replayed log event count\": " << this->stats_.replayed_events_ << "," << std::endl;
    for (int i = 0; i < EVENT_TYPE_NEXT_ID; i++) {
        uint64_t val = this->stats_.replayed_events_by_type_[i];
        if (i > 0) {
            sstr << "\"replayed log event type " << i << " count\": " << val << "," << std::endl;
        }
    }
    sstr << "\"log event count\": " << this->stats_.event_count_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string Log::PrintTrace() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{";
    sstr << "\"log id\": " << this->log_id_ << "," << std::endl;
    sstr << "\"replay id\": " << this->replay_id_ << "," << std::endl;
    sstr << "\"max log size\": ";
    if (log_data_) {
        sstr << this->log_data_->GetLimitId() << ",";
    } else {
        sstr << "null" << "," << std::endl;
    }
    sstr << "\"multi-entry event count\": " << this->stats_.multi_entry_event_count_ << "," << std::endl;
    sstr << "\"throttle count\": " << this->stats_.throttle_count_ << "," << std::endl;
    sstr << "\"direct replay event\": ";
    if (direct_replay_state_.active()) {
        // Note that there might be inconsistent trace outputs under heavy load
        // but simply I don't care here. But you should be aware of it.
        sstr << "{";
        sstr << "\"event type\": \"" << GetEventTypeName(direct_replay_state_.type()) << "\"," << std::endl;
        sstr << "\"log id\": " << direct_replay_state_.log_id() << "," << std::endl;
        sstr << "\"consumer\": " << "\"" << direct_replay_state_.GetConsumer() << "\"" << std::endl;

        sstr << "},";
    } else {
        sstr << "null,";
    }
    sstr << "\"direct replay queue size\": " << this->replay_event_queue_.unsafe_size() << "," << std::endl;
    sstr << "\"direct replay count\": " << this->stats_.direct_replay_count_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string Log::PrintProfile() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);
    sstr << "{";
    sstr << "\"average commit latency\": " << this->stats_.average_commit_latency_.GetAverage() << "," << std::endl;
    sstr << "\"average ack latency\": " << this->stats_.average_ack_latency_.GetAverage() << "," << std::endl;
    sstr << "\"average read event latency\": " << this->stats_.average_read_event_latency_.GetAverage() << ","
         << std::endl;
    sstr << "\"average replay events latency\": " << this->stats_.average_replay_events_latency_.GetAverage() << ","
         << std::endl;
    sstr << "\"average replayed events per replay\": " << this->stats_.average_replayed_events_per_step_.GetAverage()
         << "," << std::endl;
    for (int i = 0; i < EVENT_TYPE_NEXT_ID; i++) {
        if (this->stats_.replayed_events_by_type_[i] > 0) {
            sstr << "\"average replay events latency type " << i << "\": "
                 << this->stats_.average_replay_events_latency_by_type_[i].GetAverage() << "," << std::endl;
            sstr << "\"average replayed events per replay type " << i << "\": "
                 << this->stats_.average_replayed_events_per_step_by_type_[i].GetAverage() << "," << std::endl;
        }
    }
    sstr << "\"write time\": " << this->stats_.write_time_.GetSum() << "," << std::endl;
    sstr << "\"commit time\": " << this->stats_.commit_time_.GetSum() << "," << std::endl;
    sstr << "\"replay time\": " << this->stats_.replay_time_.GetSum() << "," << std::endl;
    sstr << "\"replay publish time\": " << this->stats_.replay_publish_time_.GetSum() << "," << std::endl;
    sstr << "\"replay read time\": " << this->stats_.replay_read_time_.GetSum() << "," << std::endl;
    sstr << "\"replay update id time\": " << this->stats_.replay_update_id_time_.GetSum() << "," << std::endl;
    sstr << "\"publish time\": " << this->stats_.publish_time_.GetSum() << "," << std::endl;
    sstr << "\"throttle time\": " << this->stats_.throttle_time_.GetSum() << "," << std::endl;
    sstr << "\"log data\": " << (this->log_data_ ? this->log_data_->PrintProfile() : "null") << std::endl;
    sstr << "}";
    return sstr.str();
}

#ifdef DEDUPV1_CORE_TEST

void Log::ClearData() {
    this->Stop(StopContext::FastStopContext());

    if (log_data_) {
        log_data_->Close();
        log_data_ = NULL;
    }
    this->log_info_store_.ClearData();
    this->data_cleared = true;
}
#endif

bool Log::PerformFullReplayBackgroundMode(bool write_boundary_events) {
    bool failed = false;

    FullReplayLogConsumer consumer(this->replay_id(), this->log_id());
    // we should take care that the log consumer is unregistered even in case of errors
    CHECK(this->RegisterConsumer("replay", &consumer), "Failed to register replay consumer");

    if (!this->ReplayStart(EVENT_REPLAY_MODE_REPLAY_BG, true, write_boundary_events)) {
        ERROR("Error while starting log replay");
        failed = true;
    }

    // We wait here for the direct replay queue to prevent an early stop because the log has hit the
    // direct replay queue head
    if (!WaitUntilDirectReplayQueueEmpty(0)) {
        ERROR("Failed to wait for an empty direct replay queue");
        failed = true;
    }

    if (!failed) {
        enum log_replay_result result = LOG_REPLAY_OK;
        while (result == LOG_REPLAY_OK) {
            result = this->Replay(EVENT_REPLAY_MODE_REPLAY_BG, max_area_size_full_replay_, NULL, NULL);
        }
        if (result != LOG_REPLAY_NO_MORE_EVENTS) {
            ERROR("Error during log replay: replay mode " << Log::GetReplayModeName(EVENT_REPLAY_MODE_REPLAY_BG));
            failed = true;
        }
    }

    if (!this->ReplayStop(EVENT_REPLAY_MODE_REPLAY_BG, !failed, write_boundary_events)) {
        ERROR("Error while stopping log replay");
        failed = true;
    }

    CHECK(this->UnregisterConsumer("replay"), "Failed to unregister replay consumer");

    return !failed;
}

size_t Log::consumer_count() {
    return this->consumer_list_.size();
}

LogConsumerListEntry::LogConsumerListEntry() {
    consumer_ = NULL;
}

LogConsumerListEntry::LogConsumerListEntry(const std::string& name, LogConsumer* consumer) {
    this->name_ = name;
    this->consumer_ = consumer;
}

const std::string& LogConsumerListEntry::name() {
    return name_;
}

LogConsumer* LogConsumerListEntry::consumer() {
    return consumer_;
}

uint64_t Log::log_size() {
    return this->log_data_ ? this->log_data_->GetItemCount() : -1;
}

void Log::SetLogPosition(int64_t log_position) {
    this->log_id_ = log_position;
}

void Log::SetReplayPosition(int64_t replay_position) {
    this->replay_id_ = replay_position;
}

FullReplayLogConsumer::FullReplayLogConsumer(int64_t replay_id_at_start, int64_t log_id_at_start) {
    this->replay_id_at_start = replay_id_at_start;
    this->log_id_at_start = log_id_at_start;
    this->last_full_percent_progress = 0;
    this->start_time = tick_count::now();
}

bool FullReplayLogConsumer::LogReplay(dedupv1::log::event_type event_type, const LogEventData& data,
                                      const dedupv1::log::LogReplayContext& context) {
    // we are only interested in background events
    if (context.replay_mode() == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        return true;
    }

    int64_t total_replay_count = log_id_at_start - replay_id_at_start - 1;
    int64_t replayed_count = context.log_id() - replay_id_at_start;
    double ratio = (100.0 * replayed_count) / total_replay_count;

    TRACE("Replayed event: " << Log::GetEventTypeName(event_type) << ", log id " << context.log_id());

    if (ratio >= this->last_full_percent_progress + 1) {
        tick_count::interval_t run_time = tick_count::now() - this->start_time;

        this->last_full_percent_progress = ratio; // implicit cast
        if (last_full_percent_progress >= 0.0 && last_full_percent_progress <= 100.0) {
            INFO("Replayed " << this->last_full_percent_progress << "% of log, running time " << run_time.seconds()
                             << "s, mode " << Log::GetReplayModeName(context.replay_mode()));
        }
    }
    return true;
}

LogReplayEntry::LogReplayEntry() {
    log_id_ = 0;
    event_type_ = EVENT_TYPE_NONE;
    failed_ = false;
    log_id_count_ = 0;
}

LogReplayEntry::LogReplayEntry(uint64_t log_id, enum event_type event_type, const LogEventData& event_value,
                               bool failed, uint32_t log_id_count) {
    this->log_id_ = log_id;
    this->event_type_ = event_type;
    this->event_value_ = event_value;
    this->failed_ = failed;
    this->log_id_count_ = log_id_count;
}

string LogReplayEntry::DebugString() const {
    stringstream sstr;
    sstr << "[" << log_id_ << ", " << Log::GetEventTypeName(event_type_) << "]";
    return sstr.str();
}

}
}

