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
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>

#include <core/dedup.h>
#include <base/strutil.h>
#include <base/index.h>
#include <core/dedup_system.h>
#include <core/filter.h>
#include <core/chunker.h>
#include <core/fingerprinter.h>
#include <base/threadpool.h>
#include <core/dedup_volume.h>
#include <base/logging.h>
#include <base/bitutil.h>
#include <base/hashing_util.h>
#include <re2/re2.h>

#include "scst_handle.h"
#include "command_handler.h"
#include "dedupv1d.h"
#include "dedupv1d_volume.h"
#include "dedupv1d_session.h"
#include "monitor.h"
#include <signal.h>

#include <algorithm>
#include <iostream>
#include <iomanip>
#include <sstream>

using std::set;
using std::map;
using std::list;
using std::vector;
using std::string;
using std::pair;
using std::make_pair;
using std::stringstream;
using dedupv1::DedupVolume;
using dedupv1::DedupSystem;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::IsNumeric;
using dedupv1::base::strutil::Join;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::base::bits;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::ErrorContext;
using dedupv1::scsi::ScsiResult;

LOGGER("Dedupv1dVolume");

namespace dedupv1d {

Dedupv1dVolume::Dedupv1dVolume(bool preconfigured) {
    // the default block size is 512

    this->block_size_ = kDefaultBlockSize;
    this->block_shift_ = bits(block_size_); // here we use 512k
    this->block_count_ = 0;
    this->command_thread_count_ = kDefaultCommandThreadCount;
    this->preconfigured_ = preconfigured;
    this->maintenance_mode_ = false;
    this->info_store_ = NULL;
    this->state_ = DEDUPV1D_VOLUME_STATE_CREATED;
}

bool Dedupv1dVolume::Start(DedupSystem* system) {
    CHECK(this->state_ == DEDUPV1D_VOLUME_STATE_CREATED, "Illegal state: " << this->state_);
    CHECK(this->id() != DedupVolume::kUnsetVolumeId, "Volume id not set");
    CHECK(this->command_thread_count() > 0, "Command threads not set");

    CHECK(system, "System not set");

    info_store_ = system->info_store();
    CHECK(info_store_, "Info store not set");

    CHECK(this->logical_size() % this->block_size_ == 0,
        "The logical size " << this->logical_size() << " is no multiple of the sector size " << this->block_size_);

    this->block_count_ = this->volume()->GetLogicalSize() / this->block_size();
    CHECK(this->block_count() > 0, "Volume must have at least one block");

    CHECK(this->volume_.SetOption("session-count", ToString(this->command_thread_count_ * 2)), "Failed to set session count");
    CHECK(this->handle()->SetOption("device-name", device_name()),
        "Cannot set default device name " << device_name()); // set default device-name
    CHECK(this->volume()->Start(system, this->maintenance_mode()), "Cannot start volume: base volume " << volume()->DebugString());

    CHECK(this->command_handler()->Start(this, info_store_), "Cannot start command handler");

    this->state_ = DEDUPV1D_VOLUME_STATE_STARTED;

    INFO("Started dedupv1d volume " << this->device_name());
    return true;
}

bool Dedupv1dVolume::JoinGroupOption(const string& group, uint64_t lun, string* group_lun_pair) {
    CHECK(group_lun_pair, "Group lun pair not set");

    group_lun_pair->clear();
    group_lun_pair->append(group);
    group_lun_pair->append(":");
    group_lun_pair->append(ToString(lun));
    return true;
}

bool Dedupv1dVolume::SplitGroupOption(const string& option, string* group, uint64_t* lun) {
    CHECK(group, "Group not set");
    CHECK(lun, "Lun not set");

    size_t sep = option.rfind(':');
    CHECK(sep != string::npos, "Found no separator in " << option);
    string lun_part = option.substr(sep + 1);
    string group_part = option.substr(0, sep);
    CHECK(lun_part.size() > 0, "Lun is empty in " << lun_part);
    CHECK(group_part.size() > 0, "Group is empty in " << group_part);
    CHECK(IsNumeric(lun_part), "Lun is not numeric in " << lun_part);

    *group = group_part;
    CHECK(To<int>(lun_part).valid(), "Illegal lun: " << lun_part);
    *lun = To<uint64_t>(lun_part).value();
    return true;
}

bool Dedupv1dVolume::ParseFrom(const VolumeInfoData& volume_info) {
    // We redirect everything over the config system as some of these settings are delegated to other classes
    // and we have to duplicate the logic this way.

    CHECK(this->state() == DEDUPV1D_VOLUME_STATE_CREATED, "Illegal state: " << this->state());

    CHECK(this->SetOption("id", ToString(volume_info.volume_id())),
        "Failed to set id");
    CHECK(this->SetOption("logical-size", ToString(volume_info.logical_size())),
        "Failed to set logical size");

    if (volume_info.has_device_name()) {
        CHECK(this->SetOption("device-name", ToString(volume_info.device_name())),
            "Failed to set device name");
    }
    if (volume_info.has_command_thread_count()) {
        CHECK(this->SetOption("threads", ToString(volume_info.command_thread_count())),
            "Failed to set thread count");
    }
    if (volume_info.has_sector_size()) {
        CHECK(this->SetOption("sector-size", ToString(volume_info.sector_size())),
            "Failed to set sector size");
    }
    for (int i = 0; i < volume_info.groups_size(); i++) {
        CHECK(this->SetOption("group", volume_info.groups(i)),
            "Failed to set group");
    }
    for (int i = 0; i < volume_info.targets_size(); i++) {
        CHECK(this->SetOption("target",volume_info.targets(i)),
            "Failed to set target");
    }
    if (volume_info.has_state()) {
        if (volume_info.state() == VOLUME_STATE_MAINTENANCE) {
            CHECK(this->SetOption("maintenance", ToString(true)),
                "Failed to set maintenance mode");
        }
    }
    for (int i = 0; i < volume_info.filter_chain_options_size(); i++) {
        CHECK(this->SetOption(volume_info.filter_chain_options(i).option_name(),
                volume_info.filter_chain_options(i).option()),
            "Failed to set filter chain option");
    }
    for (int i = 0; i < volume_info.chunking_options_size(); i++) {
        CHECK(this->SetOption(volume_info.chunking_options(i).option_name(),
                volume_info.chunking_options(i).option()),
            "Failed to set chunking option");
    }
    return true;
}

bool Dedupv1dVolume::SetOption(const string& option_name, const string& option) {
    CHECK(this->state_ == DEDUPV1D_VOLUME_STATE_CREATED, "Illegal state: " << this->state_);
    if (option_name == "threads") {
        CHECK(To<uint16_t>(option).valid(), "Illegal option: " << option);
        this->command_thread_count_ = To<uint16_t>(option).value();
        CHECK(this->command_thread_count() > 0, "Illegal thread count: " << this->command_thread_count());
        return true;
    }
    if (option_name == "device-name") {
        CHECK(option.size() != 0, "Illegal device name (empty)");
        CHECK(option.size() <= 48, "Illegal device name (too long): " << option); // (tested max: 48)
        CHECK(RE2::FullMatch(option, "[a-zA-Z0-9\\.\\-_]+$"), "Illegal device name: " << option);
        this->device_name_ = option;
        return true;
    }
    if (option_name == "sector-size") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option: " << option);
        uint32_t sector_size = To<uint32_t>(option).value();
        CHECK(sector_size == 512 || sector_size == 1024 || sector_size == 2048 || sector_size == 4096, "Illegal sector size: " << sector_size);
        this->block_size_ = sector_size;
        this->block_shift_ = dedupv1::base::bits(this->block_size_);
        this->block_count_ = this->volume()->GetLogicalSize() / this->block_size();
        return true;
    }
    if (option_name == "group") {
        string group;
        uint64_t lun;
        CHECK(SplitGroupOption(option, &group, &lun),"Illegal group format " << option);

        vector< pair<string, uint64_t> >::const_iterator i;
        for (i = this->groups_.begin(); i != this->groups_.end(); i++) {
            if (i->first == group) {
                ERROR("Volume is already in group: " << option);
                return false;
            }
        }
        // group check is ok
        pair<string, uint64_t> group_lun_pair(group, lun);
        this->groups_.push_back(group_lun_pair);
        return true;
    }
    if (option_name == "target") {
        string target;
        uint64_t lun;
        CHECK(SplitGroupOption(option, &target, &lun), "Illegal target format " << option);

        vector< pair<string, uint64_t> >::const_iterator i;
        for (i = this->targets_.begin(); i != this->targets_.end(); i++) {
            if (i->first == target) {
                ERROR("Volume is already in target: " << option);
                return false;
            }
        }
        // target check is ok
        pair<string, uint16_t> target_lun_pair(target, lun);
        this->targets_.push_back(target_lun_pair);
        return true;
    }
    if (option_name == "maintenance") {
        CHECK(To<bool>(option).valid(), "Illegal option: " << option);
        this->maintenance_mode_ = To<bool>(option).value();
        return true;
    }
    if (StartsWith(option_name, "filter")) {
        this->filter_options_.push_back(make_pair(option_name, option));
        // fall through
    }
    if (StartsWith(option_name, "chunking")) {
        this->chunking_options_.push_back(make_pair(option_name, option));
        // fall through
    }
    bool r = this->volume_.SetOption(option_name, option);
    if (r) {
        this->block_count_ = this->volume()->GetLogicalSize() / this->block_size();
    }
    return r;
}

bool Dedupv1dVolume::Runner(int thread_index) {
    CHECK(this->command_handler(), "Command handler not set");
    CHECK(this->handle(), "SCST handle not set");

    DEBUG("Start dedupv1 command thread: volume " << this->DebugString() << ", thread index " << thread_index);

    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireReadLock(), "Failed to acquire volume lock: " <<
        "volume " << DebugString());

    bool failed = false;
    CommandHandlerSession* chs = this->command_handler()->CreateSession(thread_index);
    if (!chs) {
        ERROR("Failed to create command handler session: volume " << this->DebugString());
        failed = true;
    }

    while (this->state() == DEDUPV1D_VOLUME_STATE_RUNNING) {
        // do not hold lock during command processing
        CHECK(scoped_lock.ReleaseLock(), "Failed to release volume lock");

        Option<bool> throttleState = Throttle(thread_index, this->command_thread_count_);
        if (!throttleState.valid()) {
            ERROR("Failed to throttle volume: " << this->DebugString());
            failed = true;
        } else if (throttleState.value()) {
            DEBUG("Throttle command handler thread: " <<
                "volume " << DebugString() <<
                ", thread id " << thread_index);
        } else {
            if (!this->handle()->HandleProcessCommand(chs)) {
                ERROR("Failed to process next SCST command: volume " << this->DebugString());
                failed = true;
            }
        }
        // re-acquire before state check
        CHECK(scoped_lock.AcquireReadLock(),
            "Failed to acquire volume lock: volume " << this->DebugString());
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release volume lock: volume " << this->DebugString());
    if (chs) {
        delete chs;
        chs = NULL;
    }
    if (failed) {
        (void) scoped_lock.AcquireWriteLock();
        state_ = DEDUPV1D_VOLUME_STATE_FAILED;
        (void) scoped_lock.ReleaseLock();
    }
    DEBUG("Exit dedupv1 command thread: volume " << this->DebugString() <<
        ", thread index " << thread_index <<
        ", success " << ToString(!failed));
    return !failed;
}

bool Dedupv1dVolume::Run() {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire volume lock");

    CHECK(this->state() == DEDUPV1D_VOLUME_STATE_STARTED || this->state() == DEDUPV1D_VOLUME_STATE_STOPPED,
        "Illegal state: state " << this->state());

    DEBUG("Run volume " << this->DebugString());

    // We have to take this detour because volumes can have more than one Run/Stop cycle per life cycle
    if (this->handle()->state() == ScstHandle::SCST_HANDLE_STATE_CREATED) {
        CHECK(this->handle()->Start(this->block_size()), "Failed to start SCST handle");
    } else {
        CHECK(this->handle()->Restart(this->block_size()), "Failed to restart handle");
    }

    this->command_handler_threads_.resize(command_thread_count_);
    for (uint16_t i = 0; i < command_thread_count_; i++) {
        this->command_handler_threads_[i] = new Thread<bool>(
            NewRunnable(this, &Dedupv1dVolume::Runner, static_cast<int>(i)),
            this->device_name() + " " + ToString(i));
        CHECK(this->command_handler_threads_[i], "Failed to create command thread " << i);
    }

    this->state_ = DEDUPV1D_VOLUME_STATE_RUNNING;

    for (uint16_t i = 0; i < this->command_handler_threads_.size(); i++) {
        CHECK(this->command_handler_threads_[i]->Start(),
            "Cannot submit command thread");
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release volume lock");

    DEBUG("Volume " << this->DebugString() << " is running");
    return true;
}

bool Dedupv1dVolume::Stop(const dedupv1::StopContext& stop_context) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire volume lock");
    this->state_ = DEDUPV1D_VOLUME_STATE_STOPPED;
    CHECK(scoped_lock.ReleaseLock(), "Failed to release volume lock");

    DEBUG("Stopping volume " << this->DebugString());

    bool failed = false;
    for (uint16_t i = 0; i < this->command_handler_threads_.size(); i++) {
        if (command_handler_threads_[i]) {
            if (command_handler_threads_[i]->IsJoinable()) {
                bool result = false;
                if (!this->command_handler_threads_[i]->Join(&result)) {
                    ERROR("Failed to join command thread: thread index " << i);
                    failed = true;
                } else {
                    // only when join was ok
                    if (!result) {
                        WARNING(this->command_handler_threads_[i]->name() << " exited with error");
                    }
                }
            }
            delete this->command_handler_threads_[i];
            this->command_handler_threads_[i] = NULL;
        }
    }
    this->command_handler_threads_.clear();

    // now it is save to stop the handle
    CHECK(this->handle()->Stop(), "Cannot stop SCST handle");
    return !failed;
}

bool Dedupv1dVolume::ChangeLogicalSize(uint64_t new_logical_size) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire volume lock");

    DEBUG("Volume changed logical size: " <<
        "volume " << this->DebugString() <<
        ", new logical size " << new_logical_size);

    CHECK(logical_size() <= new_logical_size, "Cannot reduce logical size of a volume: " << DebugString());

    bool r = this->volume_.ChangeLogicalSize(new_logical_size);
    if (r) {
        this->block_count_ = this->volume()->GetLogicalSize() / this->block_size();
    }

    if (handle_.is_registered()) {
        CHECK(this->handle_.NotifyDeviceCapacityChanged(),
            "Failed to notify initiators about capacity change");
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release volume lock");
    return true;
}

bool Dedupv1dVolume::ChangeOptions(const list<pair<string, string> >& options) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire volume lock");

    CHECK(maintenance_mode(), "Volume is not in maintenance mode: " << DebugString());

    list<pair<string, string> > new_filter_options;
    list<pair<string, string> > new_chunking_options;

    list<pair<string, string> >::const_iterator i;
    for (i = options.begin(); i != options.end(); ++i) {
        if (StartsWith(i->first, "filter")) {
            new_filter_options.push_back(make_pair(i->first, i->second));
        }
        if (StartsWith(i->first, "chunking")) {
            new_chunking_options.push_back(make_pair(i->first, i->second));
        }
    }

    if (!volume_.ChangeOptions(options)) {
        ERROR("Failed to change volume options: " << DebugString());

        // revert to old settings
        if (!filter_options_.empty()) {
            if (!volume_.ChangeOptions(filter_options_)) {
                WARNING("Failed to revert to old filter options");
            }
        }

        if (!chunking_options_.empty()) {
            if (!volume_.ChangeOptions(chunking_options_)) {
                WARNING("Failed to revert to old chunking options");
            }
        }
        return false;
    }

    if (!new_filter_options.empty()) {
        filter_options_ = new_filter_options;
    }

    if (!new_chunking_options.empty()) {
        chunking_options_ = new_chunking_options;
    }

    CHECK(scoped_lock.ReleaseLock(), "Failed to release volume lock");
    return true;
}

bool Dedupv1dVolume::ChangeMaintenanceMode(bool maintaince_mode) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire volume lock");

    DEBUG("Volume changed maintenance mode: " <<
        "volume " << this->DebugString() <<
        ", new maintenance mode " << ToString(maintaince_mode));

    if (maintenance_mode() == maintaince_mode) {
        // already in that mode, nothing has to change
        return true;
    }

    CHECK(this->state() != DEDUPV1D_VOLUME_STATE_FAILED,
        "Volume is in failure state: " << this->DebugString());

    this->maintenance_mode_ = maintaince_mode;
    CHECK(volume_.ChangeMaintenanceMode(maintaince_mode), "Failed to change maintenance mode");

    // We have a write lock. No one can invalidate the iterator
    tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<dedupv1::scsi::ScsiResult> >::iterator i;
    for (i = this->session_unit_attention_map_.begin(); i != this->session_unit_attention_map_.end(); i++) {
        // OPERATING CONDITIONS CHANGED / REPORTED LUNS DATA CHANGED
        dedupv1::scsi::ScsiResult sr(dedupv1::scsi::SCSI_CHECK_CONDITION, dedupv1::scsi::SCSI_KEY_UNIT_ATTENTION, 0x3F, 0x0E);
        i->second.push(sr);
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release volume lock");
    return true;
}

Dedupv1dVolume::~Dedupv1dVolume() {
    DEBUG("Closing volume: " << this->DebugString());

    if (this->state() == DEDUPV1D_VOLUME_STATE_RUNNING || this->state() == DEDUPV1D_VOLUME_STATE_FAILED) {
        if (!this->Stop(dedupv1::StopContext::FastStopContext())) {
            ERROR("Cannot stop volume: " << this->DebugString());
        }
    }
    if (this->command_handler_threads_.size() > 0) {
        ERROR("Command threads not stopped: " << this->DebugString());
    }

    if (session_map_.size() > 0) {
        tbb::concurrent_hash_map<uint64_t, Dedupv1dSession>::iterator i;
        vector<string> session_names;
        for (i = this->session_map_.begin(); i != this->session_map_.end(); i++) {
            session_names.push_back(i->second.target_name());
        }
        WARNING("Closing volume with open sessions: [" << Join(session_names.begin(), session_names.end(), ", ") << "]");
        session_map_.clear();
    }
}

bool Dedupv1dVolume::AddSession(const Dedupv1dSession& session) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire volume lock");

    tbb::concurrent_hash_map<uint64_t, Dedupv1dSession>::accessor a;
    session_map_.insert(a,session.session_id());
    a->second = session; // overwrite if already in

    tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<dedupv1::scsi::ScsiResult> >::accessor a2;
    session_unit_attention_map_.insert(a2, session.session_id());
    // implicit construction here
    session_set_.insert(session.session_id());
    return true;
}

dedupv1::base::Option<set<uint64_t> > Dedupv1dVolume::GetSessionSet() {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireReadLock(), "Failed to acquire volume lock");

    set<uint64_t> s = session_set_;
    CHECK(scoped_lock.ReleaseLock(), "Failed to release volume lock");
    return make_option(s);
}

Option<Dedupv1dSession> Dedupv1dVolume::FindSession(uint64_t session_id) const {
    tbb::concurrent_hash_map<uint64_t, Dedupv1dSession>::const_accessor a;
    if (session_map_.find(a, session_id)) {
        return make_option(a->second);
    }
    return false;
}

bool Dedupv1dVolume::RemoveSession(uint64_t session_id) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire volume lock");

    tbb::concurrent_hash_map<uint64_t, Dedupv1dSession>::accessor a;
    if (session_map_.find(a, session_id)) {
        session_map_.erase(a);
        session_set_.erase(session_id);

        session_unit_attention_map_.erase(session_id);
        return true;
    }
    ERROR("Session not found: " << session_id);
    return false;
}

bool Dedupv1dVolume::AddTarget(const string& target, uint64_t lun) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire volume lock");

    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = this->targets_.begin(); i != this->targets_.end(); i++) {
        if (i->first == target) {
            ERROR("Volume is already in target: " << target);
            return false;
        }
    }

    pair<string, uint64_t> target_lun_pair(target, lun);
    this->targets_.push_back(target_lun_pair);
    CHECK(scoped_lock.ReleaseLock(), "Cannot lock volume lock");
    return true;
}

bool Dedupv1dVolume::AddGroup(const string& group, uint64_t lun) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire volume lock");

    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = this->groups_.begin(); i != this->groups_.end(); i++) {
        if (i->first == group) {
            ERROR("Volume is already in group: " << group);
            return false;
        }
    }

    pair<string, uint64_t> group_lun_pair(group, lun);
    this->groups_.push_back(group_lun_pair);
    CHECK(scoped_lock.ReleaseLock(), "Cannot lock volume lock");
    return true;
}

bool Dedupv1dVolume::RemoveTarget(const string& target) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire volume lock");

    vector< pair<string, uint64_t> >::iterator i;
    for (i = this->targets_.begin(); i != this->targets_.end(); i++) {
        if (i->first == target) {
            this->targets_.erase(i);
            CHECK(scoped_lock.ReleaseLock(), "Cannot lock volume lock");
            return true;
        }
    }
    ERROR("Failed to find target in volume: volume " << this->DebugString() << ", target " << target);
    return false;
}

bool Dedupv1dVolume::RemoveGroup(const string& group) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Failed to acquire volume lock");

    vector< pair<string, uint64_t> >::iterator i;
    for (i = this->groups_.begin(); i != this->groups_.end(); i++) {
        if (i->first == group) {
            this->groups_.erase(i);
            CHECK(scoped_lock.ReleaseLock(), "Cannot lock volume lock");
            return true;
        }
    }
    ERROR("Failed to find group in volume: volume " << this->DebugString() << ", group " << group);
    return false;
}

uint32_t Dedupv1dVolume::session_count() const {
    return this->session_map_.size();
}

string Dedupv1dVolume::device_name() const {
    if (this->device_name_.size() > 0) {
        return this->device_name_;
    }
    if (volume_.GetId() == DedupVolume::kUnsetVolumeId) {
        return "[Unknown device name]";
    }
    // default volume name
    return "dedupv1-" + ToString(volume_.GetId());
}

uint64_t Dedupv1dVolume::unique_serial_number() const {
    string device_name = this->device_name();
    uint64_t h = dedupv1::base::bj_hash(device_name.data(), device_name.size());
    return h;

}

uint32_t Dedupv1dVolume::id() const {
    return this->volume_.GetId();
}

uint64_t Dedupv1dVolume::logical_size() const {
    return this->volume_.GetLogicalSize();
}

string Dedupv1dVolume::DebugString() const {
    stringstream sstr;
    sstr << "[Volume: id " << this->id() <<
    ", name " << this->device_name() <<
    ", size " << this->logical_size() <<
    ", sector size " << this->block_size() <<
    ", sessions " << this->session_count();

    sstr << ", groups [";
    for (std::vector< std::pair<std::string, uint64_t> >::const_iterator i = groups_.begin(); i != groups_.end(); i++) {
        if (i != groups_.begin()) {
            sstr << ", ";
        }
        sstr << i->first << ":" << i->second;
    }
    sstr << "]";

    sstr << ", targets [";
    for (std::vector< std::pair<std::string, uint64_t> >::const_iterator i = targets_.begin(); i != targets_.end(); i++) {
        if (i != targets_.begin()) {
            sstr << ", ";
        }
        sstr << i->first << ":" << i->second;
    }
    sstr << "]"
    ", state " + ToString(this->state()) +
    ", maintenance mode " + (this->maintenance_mode() ? "maintenance" : "running") + "]";

    return sstr.str();
}

bool Dedupv1dVolume::SerializeTo(VolumeInfoData* data) const {
    CHECK(data, "Data not set");

    data->set_volume_id(this->id());
    if (this->device_name().size() > 0) {
        // if device name is manually set, we don't want the fallback device name to be persistent
        data->set_device_name(this->device_name());
    }

    data->set_logical_size(this->logical_size());
    data->set_command_thread_count(this->command_thread_count());
    if (this->block_size() != kDefaultBlockSize) {
        data->set_sector_size(this->block_size());
    }
    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = this->groups().begin(); i != this->groups().end(); i++) {
        string group_lun_pair;
        CHECK(JoinGroupOption(i->first, i->second, &group_lun_pair),
            "Failed to join group lun pair");
        data->add_groups(group_lun_pair);
    }
    for (i = this->targets().begin(); i != this->targets().end(); i++) {
        string target_lun_pair;
        CHECK(JoinGroupOption(i->first, i->second, &target_lun_pair),
            "Failed to join target lun pair");
        data->add_targets(target_lun_pair);
    }

    std::list<std::pair<std::string, std::string> >::const_iterator j;
    for (j = filter_options_.begin(); j != filter_options_.end(); j++) {
        OptionPair* op =  data->add_filter_chain_options();
        op->set_option_name(j->first);
        op->set_option(j->second);
    }
    for (j = chunking_options_.begin(); j != chunking_options_.end(); j++) {
        OptionPair* op =  data->add_chunking_options();
        op->set_option_name(j->first);
        op->set_option(j->second);
    }

    if (this->maintenance_mode()) {
        data->set_state(VOLUME_STATE_MAINTENANCE);
    } else {
        data->set_state(VOLUME_STATE_RUNNING);
    }

    return true;
}

Dedupv1dVolume::Statistics::Statistics() : throttle_time_average_(256) {
    throttled_thread_count_ = 0;
}

dedupv1::base::Option<bool> Dedupv1dVolume::Throttle(int thread_id, int thread_count) {
    tbb::tick_count start_tick = tbb::tick_count::now();
    this->stats_.throttled_thread_count_++;

    double average_response_time = ch_.average_response_time();
    if (average_response_time > 500.0) {
        double ratio = average_response_time / 2000.0;
        if (ratio > 1.0) {
            ratio = 1.0;
        }
        int threads_to_hold = (exp(ratio * log(command_thread_count_ - 2)) + 1);

        if (threads_to_hold > stats_.throttled_thread_count_) {
            DEBUG("Response time throttle: average response time " << average_response_time <<
                ", threads to throttle " << threads_to_hold <<
                ", throttled thread count " << stats_.throttled_thread_count_);
            sleep(average_response_time / 100.0); // sleep ten times as long as the average response time
        }
    }

    Option<bool> r = this->volume_.Throttle(thread_id, thread_count);

    this->stats_.throttled_thread_count_--;
    tbb::tick_count end_tick = tbb::tick_count::now();
    this->stats_.throttle_time_average_.Add((end_tick - start_tick).seconds() * 1000);
    return r;
}

ScsiResult Dedupv1dVolume::MakeRequest(dedupv1::request_type rw,
                                       uint64_t offset,
                                       uint64_t size,
                                       byte* buffer,
                                       ErrorContext* ec) {
    if (maintenance_mode()) {
        // return ERROR if in maintenance mode, but that should not happen anyway.
        // The error code says that the LUN is not ready and manual intervention is necessary
        return dedupv1::scsi::ScsiResult(dedupv1::scsi::SCSI_CHECK_CONDITION, dedupv1::scsi::SCSI_KEY_NOT_READY, 0x04, 0x03);
    }

    DEBUG("Request: offset " << offset << ", size " << size);

    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireReadLock();
    ScsiResult r = this->volume_.MakeRequest(rw, offset, size, buffer, ec);
    scoped_lock.ReleaseLock();

    if (ec && ec->is_full() && !maintenance_mode()) {
        WARNING("Force maintenance mode: " << DebugString());
        // here we acquire a write lock that means that all
        // already running requests run to completion
        if (!ChangeMaintenanceMode(true)) {
            WARNING("Failed to force maintenance mode: " << DebugString());
        }
    }

    return r;
}

ScsiResult Dedupv1dVolume::SyncCache() {
    if (maintenance_mode()) {
        // return ERROR if in maintenance mode, but that should not happen anyway.
        // The error code says that the LUN is not ready and manual intervention is necessary
        return dedupv1::scsi::ScsiResult(dedupv1::scsi::SCSI_CHECK_CONDITION, dedupv1::scsi::SCSI_KEY_NOT_READY, 0x04, 0x03);
    }
    ScopedReadWriteLock scoped_lock(&lock_);
    scoped_lock.AcquireReadLock();
    ScsiResult r =  this->volume_.SyncCache();
    scoped_lock.ReleaseLock();
    return r;
}

bool Dedupv1dVolume::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    CHECK(this->volume_.PersistStatistics(prefix, ps), "Failed to persist volume statistics");
    CHECK(this->ch_.PersistStatistics(prefix + ".ch", ps), "Failed to persist command handler statistics");
    return true;
}

bool Dedupv1dVolume::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    CHECK(this->volume_.RestoreStatistics(prefix, ps), "Failed to persist volume statistics");
    CHECK(this->ch_.RestoreStatistics(prefix + ".ch", ps), "Failed to persist command handler statistics");
    return true;
}

std::string Dedupv1dVolume::PrintStatistics() {
    stringstream sstr;
    sstr << "{" << std::endl;
    double throttle_time = this->stats_.throttle_time_average_.GetAverage();
    if (throttle_time < 1.0) {
        // a throttle time < 1.0 ms is not throttling, but a measurement artifact
        throttle_time = 0.0;
    }
    sstr << "\"average throttle time\": " << throttle_time << "," << std::endl;

    sstr << "\"commands\": " << ch_.PrintStatistics() << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

std::string Dedupv1dVolume::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"commands\": " << ch_.PrintLockStatistics() << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

std::string Dedupv1dVolume::PrintTrace() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"throttled thread count\": " << stats_.throttled_thread_count_ << "," << std::endl;
    sstr << "\"commands\": " << ch_.PrintTrace() << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

std::string Dedupv1dVolume::PrintProfile() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"commands\": " << ch_.PrintProfile() << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

#ifdef DEDUPV1D_TEST
void Dedupv1dVolume::ClearData() {
    Stop(dedupv1::StopContext::FastStopContext());
    handle()->ClearData();
}
#endif

}
