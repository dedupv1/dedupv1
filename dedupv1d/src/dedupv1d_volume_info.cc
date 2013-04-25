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

#include <tbb/spin_rw_mutex.h>

#include "dedupv1d_volume_info.h"
#include "dedupv1d_volume.h"
#include <base/index.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <core/dedup_volume.h>
#include <core/dedup_volume_info.h>
#include <core/dedup_system.h>
#include <sstream>
#include <algorithm>

#include "dedupv1d.pb.h"

using std::string;
using std::list;
using std::pair;
using std::multimap;
using std::vector;
using std::stringstream;
using std::make_pair;
using dedupv1::base::ScopedLock;
using dedupv1::DedupSystem;
using dedupv1::DedupVolumeInfo;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::lookup_result;
using dedupv1::base::IndexCursor;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::EndsWith;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::strutil::Contains;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::DELETE_ERROR;
using dedupv1::StartContext;
using dedupv1::base::Option;
using dedupv1::base::Index;
using dedupv1::base::make_option;

LOGGER("Dedupv1dVolumeInfo");

namespace dedupv1d {

Dedupv1dVolumeInfo::Dedupv1dVolumeInfo() {
    base_volume_info_ = NULL;
    base_dedup_system_ = NULL;
    info_ = NULL;
    started_ = false;
    running_ = false;
    this->info_store_ = NULL;
    detacher_ = new Dedupv1dVolumeDetacher(this);
    fast_copy_ = new Dedupv1dVolumeFastCopy(this);
    default_command_thread_count_ = Dedupv1dVolume::kDefaultCommandThreadCount;
}

bool Dedupv1dVolumeInfo::Start(const StartContext& start_context,
                               Dedupv1dGroupInfo* group_info,
                               Dedupv1dTargetInfo* target_info,
                               DedupSystem* system) {
    CHECK(!this->started_, "Volume info already started");

    CHECK(group_info, "Group info not set");
    CHECK(group_info->is_started(), "Group info not started");
    this->group_info_ = group_info;

    CHECK(target_info, "Target info not set");
    CHECK(target_info->is_started(), "Target info not started");
    this->target_info_ = target_info;

    CHECK(system, "System not set");
    DedupVolumeInfo* base_volume_info = system->volume_info();
    CHECK(base_volume_info, "Dedup subsystem volume info not set");
    CHECK(base_volume_info->IsStarted(), "Dedup subsystem volume info not started");

    info_store_ = system->info_store();
    CHECK(info_store_, "Info store not set");

    CHECK(detacher_ != NULL, "Detacher not set");
    CHECK(detacher_->Start(start_context), "Failed to start volume detacher");

    CHECK(fast_copy_ != NULL, "Cloner not set");
    CHECK(fast_copy_->Start(start_context, info_store_), "Failed to start volume cloner");

    CHECK(this->info_ != NULL, "Info storage not set");
    CHECK(this->info_->SupportsCursor(), "Index doesn't support cursor");
    CHECK(this->info_->IsPersistent(), "Volume info index should be persistent");

    DEBUG("Start dedupv1d volume info");

    this->base_volume_info_ = base_volume_info;
    this->base_dedup_system_ = system;

    CHECK(this->info_->Start(start_context), "Cannot start volume info");

    if (this->current_volume_options_.size() > 0) {
        volume_options_.push_back(this->current_volume_options_);
        this->current_volume_options_.clear();
    }

    // start pre configured volumes
    list< list < pair<string, string> > >::iterator i;
    for (i = volume_options_.begin(); i != volume_options_.end(); i++) {
        DEBUG("Found config " << DebugStringOptions(*i));
        list< pair<string, string> > concrete_volume_option = *i;
        Dedupv1dVolume* new_volume = ConfigureNewVolume(true, concrete_volume_option);
        CHECK(new_volume, "Failed to create new volume: " << DebugStringOptions(concrete_volume_option));
        if (!CheckNewVolume(new_volume)) {
            ERROR("Volume not valid: " << new_volume->DebugString());
            delete new_volume;
            return false;
        }
        if (!new_volume->Start(system)) {
            ERROR("Cannot start volume: " << new_volume->DebugString());
            delete new_volume;
            return false;
        }
        if (!this->RegisterVolume(new_volume, false)) {
            ERROR("Failed to register volume: " << new_volume->DebugString());
            delete new_volume;
            return false;
        }
        // here to do not close the volume directly as it is registered and therefore closed at the end

        DEBUG("Found volume " << new_volume->device_name() << " (id " << new_volume->id() << ", pre configured)");
    }

    IndexCursor* cursor = this->info_->CreateCursor();
    CHECK(cursor, "Cannot create cursor");

    // start dynamic volumes
    enum lookup_result ir = cursor->First();
    CHECK(ir != LOOKUP_ERROR, "Cannot read volume info storage");
    while (ir == LOOKUP_FOUND) {
        VolumeInfoData volume_info;
        CHECK(cursor->Get(NULL, NULL, &volume_info), "Get volume info value");

        Dedupv1dVolume* new_volume = new Dedupv1dVolume(false);
        CHECK(new_volume, "cannot create new volume");
        CHECK(new_volume->ParseFrom(volume_info), "Failed to configure volume: " << volume_info.DebugString());
        CHECK(this->CheckNewVolume(new_volume), "Volume not valid: " << new_volume->DebugString());
        CHECK(new_volume->Start(system), "Cannot start volume: " << new_volume->DebugString());
        CHECK(this->RegisterVolume(new_volume, false), "cannot register volume: " << new_volume->DebugString());
        DEBUG("Found volume " << new_volume->device_name() << " (id " << new_volume->id() << ", dynamic)");

        ir = cursor->Next();
        CHECK(ir != LOOKUP_ERROR, "Cannot read volume info storage");
    }
    delete cursor;
    this->started_ = true;
    return true;
}

bool Dedupv1dVolumeInfo::SetOption(const string& option_name, const string& option) {
    CHECK(this->started_ == false, "Illegal state");

    if (StartsWith(option_name, "volume.")) {
        string revised_option_name = option_name.substr(strlen("volume."));
        if (revised_option_name == "id") {
            if (this->current_volume_options_.size() > 0) {
                volume_options_.push_back(this->current_volume_options_);
                this->current_volume_options_.clear();
            }
        } else {
            CHECK(this->current_volume_options_.size() > 0, "No volume to configure set");
        }
        this->current_volume_options_.push_back(pair<string, string>(revised_option_name, option));
        return true;
    }

    if (StartsWith(option_name, "fast-copy.")) {
        CHECK(fast_copy_->SetOption(option_name.substr(strlen("fast-copy.")), option), "Failed to configure cloner");
        return true;
    }

    // we use the some options as for the info index for the detached state info index, a)
    // to keep the config file small and b) the be compatible with old config files
    // However, the rewrite all filename parameters

    if (option_name == "type") {
        CHECK(this->info_ == NULL, "Index type already set");
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Cannot create index: " << option);
        this->info_ = index->AsPersistentIndex();
        CHECK(this->info_, "Info should be persistent");
        CHECK(this->info_->SetOption("max-key-size", "8"), "Failed to set max key size");

        CHECK(this->detacher_ != NULL, "Volume detacher not set");
        CHECK(this->detacher_->SetOption(option_name, option), "Failed to configure detacher");
        return true;
    }
    CHECK(this->info_, "Index not set");

    if (option_name == "default-thread-count") {
        CHECK(To<uint16_t>(option).valid(), "Illegal option: " << option);
        this->default_command_thread_count_ = To<uint16_t>(option).value();
        return true;
    }

    // we later rewrite all filename options, to have an suffix. To avoid collisions, we forbid
    // the suffix for the info index.
    CHECK(!(Contains(option_name, "filename") && EndsWith(option, "_detaching_state")),
        "The suffix \"_detaching_state\" is forbidden for the info index");
    CHECK(!(Contains(option_name, "filename") && EndsWith(option, "_clone_state")),
        "The suffix \"_clone_state\" is forbidden for the info index");
    CHECK(this->info_->SetOption(option_name, option), "Failed to configure index");

    CHECK(this->detacher_ != NULL, "Volume detacher not set");  // this check is redundant, but for symmetry I keep it
    string detaching_info_option = option; // we have to make a copy because of the const modifier
    if (Contains(option_name, "filename")) { // we rewrite all filename options to avoid collisions
        detaching_info_option = detaching_info_option + "_detaching_state";
    }
    CHECK(this->detacher_->SetOption(option_name, detaching_info_option), "Failed to configure detacher");
    return true;
}

Dedupv1dVolumeInfo::~Dedupv1dVolumeInfo() {
    DEBUG("Closing dedupv1d volume info");

    // Close and unregister all volumes
    list<Dedupv1dVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        Dedupv1dVolume* v = *i;
        string device_name = v->device_name();
        if (base_volume_info_->FindVolume(v->id())) {
            if(!base_volume_info_->UnregisterVolume(v->volume())) {
              WARNING("Cannot unregister volume " << device_name);
          }
        }
        delete v;

    }
    if (info_) {
        delete info_;
        info_ = NULL;
    }
    if (this->detacher_) {
        delete this->detacher_;
        this->detacher_ = NULL;
    }
    if (this->fast_copy_) {
        delete this->fast_copy_;
        this->fast_copy_ = NULL;
    }
}

bool Dedupv1dVolumeInfo::RegisterVolume(Dedupv1dVolume* v, bool new_attachment) {
    CHECK(v, "Volume not set");

    if (new_attachment) {
        CHECK(this->base_volume_info_->AttachVolume(v->volume()),
            "Cannot attach volume info to base system");
    } else {
        CHECK(this->base_volume_info_->RegisterVolume(v->volume()), "Cannot register volume");
    }

    // from now on it is atomic
    volume_map_[v->id()] = v;
    volume_name_map_[v->device_name()] = v;
    volumes_.push_back(v);

    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = v->groups().begin(); i != v->groups().end(); i++) {
        string group_name = i->first;
        uint64_t lun = i->second;
        DEBUG("Add group entry " << group_name << ":" << lun << " for volume " << v->id());
        this->group_map_.insert( pair<string, pair<uint16_t, Dedupv1dVolume*> >(group_name, pair<uint16_t, Dedupv1dVolume*>(lun, v)));
    }
    for (i = v->targets().begin(); i != v->targets().end(); i++) {
        string target_name = i->first;
        uint64_t lun = i->second;
        DEBUG("Add target entry " << target_name << ":" << lun << " for volume " << v->id());
        this->target_map_.insert( pair<string, pair<uint16_t, Dedupv1dVolume*> >(target_name, pair<uint16_t, Dedupv1dVolume*>(lun, v)));
    }
    return true;
}

Dedupv1dVolume* Dedupv1dVolumeInfo::ConfigureNewVolume(bool preconfigured, const list< pair<string, string> >& options) {

    Dedupv1dVolume* new_volume = new Dedupv1dVolume(preconfigured);
    CHECK_RETURN(new_volume, NULL, "Memalloc of volume failed");
    if (!new_volume->SetOption("threads", ToString(default_command_thread_count_))) {
        ERROR("Cannot set default thread num for volume");
        delete new_volume;
        return NULL;
    }

    DEBUG("Create new volume: " << DebugStringOptions(options));
    list< pair<string, string> >::const_iterator i;
    for (i = options.begin(); i != options.end(); i++) {
        string option_name = i->first;
        string option = i->second;
        if (option_name == "id") {
            Option<uint32_t> volume_id = To<uint32_t>(option);
            if (!volume_id.valid()) {
                ERROR("Illegal volume id: " << volume_id.value());
                delete new_volume;
                return NULL;
            }
            if (this->FindVolumeLocked(volume_id.value()) != NULL) {
                ERROR("Volume " << volume_id.value() << " exists already");
                delete new_volume;
                return NULL;
            }
        }
        if (!new_volume->SetOption(option_name, option)) {
            ERROR("Cannot configure volume: " << option_name << "=" << option);
            delete new_volume;
            return NULL;
        }
    }
    return new_volume;
}

bool Dedupv1dVolumeInfo::CheckNewVolume(Dedupv1dVolume* new_volume) {
    CHECK(this->group_info_, "Group info not set");
    CHECK(this->target_info_, "Target info not set");
    CHECK(new_volume, "New volume not set");

    bool detaching_state = false;
    CHECK(this->detacher_->IsDetaching(new_volume->id(), &detaching_state), "Failed to check detaching state");

    CHECK(!detaching_state, "Volume with id " << new_volume->id() << " is in detaching state");

    CHECK(volume_name_map_.find(new_volume->device_name()) == volume_name_map_.end(),
        "Volume with name " << new_volume->device_name() << " exists already");
    vector< pair< string, uint64_t> >::const_iterator k;

    // check groups
    for (k = new_volume->groups().begin(); k != new_volume->groups().end(); k++) {
        string group = k->first;
        uint64_t lun = k->second;

        if (!this->group_info_->FindGroup(group).valid()) {
            ERROR("Group " << group << " not found");
            return false;
        }

        pair<multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator, multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator> r
            = this->group_map_.equal_range(group);
        multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator j = r.first;
        while (j != r.second) {
            pair<uint64_t, Dedupv1dVolume*>& group_lun(j->second);
            DEBUG("Check existing group entry " << j->first << ":" << group_lun.first);
            CHECK(group_lun.first != lun,
                "Group " << group << ":" << lun << " already assigned to volume " << group_lun.second->device_name());
            j++;
        }
    }

    // check targets
    for (k = new_volume->targets().begin(); k != new_volume->targets().end(); k++) {
        string target = k->first;
        uint64_t lun = k->second;

        if (!this->target_info_->FindTargetByName(target).valid()) {
            ERROR("Target " << target << " not found");
            return false;
        }

        pair<multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator, multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator> r
            = this->target_map_.equal_range(target);
        multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator j = r.first;
        while (j != r.second) {
            pair<uint64_t, Dedupv1dVolume*>& target_lun(j->second);
            DEBUG("Check existing target entry " << j->first << ":" << target_lun.first);
            CHECK(target_lun.first != lun,
                "Target " << target << ":" << lun << " already assigned to volume " << target_lun.second->device_name());
            j++;
        }
    }
    return true;
}

bool Dedupv1dVolumeInfo::ChangeLogicalSize(uint32_t volume_id, uint64_t new_logical_size) {
    CHECK(this->started_, "Volume info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    // we recheck the volume here, because of possible race conditions
    Dedupv1dVolume* volume = this->FindVolumeLocked(volume_id);
    CHECK(volume, "Volume not found");
    CHECK(volume->is_preconfigured() == false, "Cannot change logical size of pre-configured volume: " << volume->DebugString());
    CHECK(volume->logical_size() <= new_logical_size, "Cannot reduce logical size of a volume: " << volume->DebugString());

    INFO("Change volume logical size: volume " << volume->DebugString() <<
        ", logical size " << new_logical_size);

    CHECK(volume->ChangeLogicalSize(new_logical_size), "Failed to change logical size: " << volume->DebugString());

    VolumeInfoData volume_data;
    CHECK(volume->SerializeTo(&volume_data), "Failed to serialize volume data: " << volume->DebugString());

    int32_t volume_id_key = volume->id();
    CHECK(this->info_->Put(&volume_id_key, sizeof(volume_id_key), volume_data) != PUT_ERROR,
        "Failed to update volume info: volume " << volume->DebugString());

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeInfo::ChangeOptions(uint32_t volume_id, const std::list<std::pair<std::string, std::string> >& options) {
    CHECK(this->started_, "Volume info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    // we recheck the volume here, because of possible race conditions
    Dedupv1dVolume* volume = this->FindVolumeLocked(volume_id);
    CHECK(volume, "Volume not found");
    CHECK(volume->is_preconfigured() == false, "Cannot change volume option of pre-configured volume: " << volume->DebugString());
    CHECK(volume->maintenance_mode(), "Cannot change volume options of running volune " << volume->DebugString());

    INFO("Change volume options: volume " << volume->DebugString() <<
        ", options size " << DebugStringOptions(options));

    CHECK(volume->ChangeOptions(options), "Failed to change logical size: volume " << volume->DebugString() <<
        ", options size " << DebugStringOptions(options));

    VolumeInfoData volume_data;
    CHECK(volume->SerializeTo(&volume_data), "Failed to serialize volume data: " << volume->DebugString());

    int32_t volume_id_key = volume->id();
    CHECK(this->info_->Put(&volume_id_key, sizeof(volume_id_key), volume_data) != PUT_ERROR,
        "Failed to update volume info: volume " << volume->DebugString());

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeInfo::FastCopy(uint32_t src_volume_id,
                                  uint32_t target_volume_id,
                                  uint64_t source_offset,
                                  uint64_t target_offset,
                                  uint64_t size) {
    CHECK(this->started_, "Volume info is not started");

    CHECK(src_volume_id != target_volume_id, "Invalid clone source: src volume id " << src_volume_id << ", target volume id " << target_volume_id);

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    // we recheck the volume here, because of possible race conditions
    Dedupv1dVolume* target_volume = this->FindVolumeLocked(target_volume_id);
    CHECK(target_volume, "Target volume not found: volume id " << target_volume_id);
    CHECK(target_volume->maintenance_mode(), "Cannot clone to running volume: " << target_volume->DebugString());

    Dedupv1dVolume* src_volume = this->FindVolumeLocked(src_volume_id);
    CHECK(src_volume, "Source volume not found: volume id " << src_volume_id);
    CHECK(src_volume->maintenance_mode(), "Cannot clone from running volume: " << src_volume->DebugString());

    CHECK(source_offset + size <= src_volume->logical_size(), "Illegal clone source");
    CHECK(target_offset + size <= target_volume->logical_size(), "Illegal clone target");

    CHECK(fast_copy_->StartNewFastCopyJob(src_volume_id, target_volume_id,
            source_offset, target_offset,
            size), "Failed to start clone operation");

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeInfo::ChangeMaintainceMode(uint32_t volume_id, bool maintenance_mode) {
    CHECK(this->started_, "Volume info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    // we recheck the volume here, because of possible race conditions
    Dedupv1dVolume* volume = this->FindVolumeLocked(volume_id);
    CHECK(volume, "Volume not found");
    CHECK(volume->is_preconfigured() == false, "Cannot change state of pre-configured volume: " << volume->DebugString());

    if (maintenance_mode) {
        CHECK(volume->session_count() == 0, "Cannot detach change state of volume with sessions: " << volume->DebugString());
    }
    CHECK(!fast_copy_->IsFastCopySource(volume_id),
        "Cannot change state of in-progress fast-copy source: " << volume->DebugString());
    CHECK(!fast_copy_->IsFastCopyTarget(volume_id),
        "Cannot change state of in-progress fast-copy target: " << volume->DebugString());

    INFO("Change volume state: volume " << volume->DebugString() << ", state " << (maintenance_mode ? "maintenance" : "running"));

    CHECK(volume->ChangeMaintenanceMode(maintenance_mode), "Failed to change maintenance mode: " << volume->DebugString());

    VolumeInfoData volume_data;
    CHECK(volume->SerializeTo(&volume_data), "Failed to serialize volume data: " << volume->DebugString());

    int32_t volume_id_key = volume->id();
    CHECK(this->info_->Put(&volume_id_key, sizeof(volume_id_key), volume_data) != PUT_ERROR,
        "Failed to update volume info: volume " << volume->DebugString());

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeInfo::AddToTarget(uint32_t volume_id, std::string target_lun_pair) {
    CHECK(this->started_, "Volume info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    // we recheck the volume here, because of possible race conditions
    Dedupv1dVolume* volume = this->FindVolumeLocked(volume_id);
    CHECK(volume, "Volume not found");
    CHECK(volume->is_preconfigured() == false, "Cannot add pre-configured volume to target: " << volume->DebugString());

    INFO("Add volume to target: volume " << volume->DebugString() << ", target " << target_lun_pair);

    string target_name;
    uint64_t lun;

    CHECK(Dedupv1dVolume::SplitGroupOption(target_lun_pair, &target_name, &lun),
        "Failed to parse target data: " << target_lun_pair);

    Dedupv1dVolume* assigned_volume = FindVolumeByTargetLocked(target_name, lun);
    CHECK(assigned_volume == NULL,
        "Target entry " << target_name << ":" << lun << " already assigned: volume " << assigned_volume->DebugString());
    // assigned_volume == NULL => target:lun can be used

    CHECK(volume->AddTarget(target_name, lun), "Failed to add volume to target: " << target_name << ":" << lun);

    DEBUG("Add target entry " << target_name << ":" << lun << " for volume " << volume->id());
    this->target_map_.insert( pair<string, pair<uint16_t, Dedupv1dVolume*> >(target_name, pair<uint16_t, Dedupv1dVolume*>(lun, volume)));

    VolumeInfoData volume_data;
    CHECK(volume->SerializeTo(&volume_data), "Failed to serialize volume data");

    int32_t volume_id_key = volume->id();
    CHECK(this->info_->Put(&volume_id_key, sizeof(volume_id_key), volume_data) != PUT_ERROR,
        "Failed to update volume info: volume " << volume->DebugString());

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeInfo::AddToGroup(uint32_t volume_id, string group_lun_pair) {
    CHECK(this->started_, "Volume info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    // we recheck the volume here, because of possible race conditions
    Dedupv1dVolume* volume = this->FindVolumeLocked(volume_id);
    CHECK(volume, "Volume not found");
    CHECK(volume->is_preconfigured() == false, "Cannot remove pre-configured volume from group: " << volume->DebugString());

    INFO("Add volume to group: volume " << volume->DebugString() << ", group " << group_lun_pair);

    string group_name;
    uint64_t lun;

    CHECK(Dedupv1dVolume::SplitGroupOption(group_lun_pair, &group_name, &lun),
        "Failed to parse group data: " << group_lun_pair);

    Dedupv1dVolume* assigned_volume = FindVolumeByGroupLocked(group_name, lun);
    CHECK(assigned_volume == NULL,
        "Group entry " << group_name << ":" << lun << " already assigned: volume " << assigned_volume->DebugString());
    // assigned_volume == NULL => group:lun can be used

    CHECK(volume->AddGroup(group_name, lun), "Failed to add volume to group: " << group_name << ":" << lun);

    DEBUG("Add group entry " << group_name << ":" << lun << " for volume " << volume->id());
    this->group_map_.insert( pair<string, pair<uint16_t, Dedupv1dVolume*> >(group_name, pair<uint16_t, Dedupv1dVolume*>(lun, volume)));

    VolumeInfoData volume_data;
    CHECK(volume->SerializeTo(&volume_data), "Failed to serialize volume data");

    int32_t volume_id_key = volume->id();
    CHECK(this->info_->Put(&volume_id_key, sizeof(volume_id_key), volume_data) != PUT_ERROR,
        "Failed to update volume info: volume " << volume->DebugString());

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeInfo::RemoveFromTarget(uint32_t volume_id, std::string target) {
    CHECK(this->started_, "Volume info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    Dedupv1dVolume* volume = this->FindVolumeLocked(volume_id);
    CHECK(volume, "Volume not found");
    CHECK(volume->is_preconfigured() == false, "Cannot remove pre-configured volume from target: " << volume->DebugString());

    INFO("Remove volume from target: volume " << volume->DebugString() << ", target " << target);

    CHECK(volume->RemoveTarget(target), "Failed to remove target from volume");

    // search and delete volume target entry in target map
    pair<multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator, multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator> r
        = this->target_map_.equal_range(target);
    multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator i = r.first;
    while (i != r.second) {
        std::pair<uint64_t, Dedupv1dVolume*> p = i->second;

        multimap<string, std::pair<uint64_t, Dedupv1dVolume*> >::iterator erase_iter = i++;
        if (p.second == volume) {
            this->target_map_.erase(erase_iter);
        }
    }

    VolumeInfoData volume_data;
    CHECK(volume->SerializeTo(&volume_data), "Failed to serialize volume data");

    int32_t volume_id_key = volume->id();
    CHECK(this->info_->Put(&volume_id_key, sizeof(volume_id_key), volume_data) != PUT_ERROR,
        "Failed to update volume info: volume " << volume->DebugString());

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dVolumeInfo::RemoveFromGroup(uint32_t volume_id, std::string group) {
    CHECK(this->started_, "Volume info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    Dedupv1dVolume* volume = this->FindVolumeLocked(volume_id);
    CHECK(volume, "Volume not found");
    CHECK(volume->is_preconfigured() == false, "Cannot remove pre-configured volume from group: " << volume->DebugString());

    INFO("Remove volume from group: volume " << volume->DebugString() << ", group " << group);

    CHECK(volume->RemoveGroup(group), "Failed to remove group from volume: " << volume->DebugString());

    // search and delete volume group entry in group map
    pair<multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator, multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator> r
        = this->group_map_.equal_range(group);
    multimap<string, pair<uint64_t, Dedupv1dVolume*> >::iterator i = r.first;
    while (i != r.second) {
        std::pair<uint64_t, Dedupv1dVolume*> p = i->second;

        multimap<string, std::pair<uint64_t, Dedupv1dVolume*> >::iterator erase_iter = i++;
        if (p.second == volume) {
            this->group_map_.erase(erase_iter);
        }
    }

    VolumeInfoData volume_data;
    CHECK(volume->SerializeTo(&volume_data), "Failed to serialize volume data");

    int32_t volume_id_key = volume->id();
    CHECK(this->info_->Put(&volume_id_key, sizeof(volume_id_key), volume_data) != PUT_ERROR,
        "Failed to update volume info: volume " << volume->DebugString());

    DEBUG("Finished removing volume from group: volume " << volume->DebugString() << ", group " << group);
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

Dedupv1dVolume* Dedupv1dVolumeInfo::AttachVolume(list< pair< string, string> > options) {
    CHECK_RETURN(this->started_, NULL, "Volume info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireLock(), NULL, "Cannot acquire lock");

    INFO("Attach volume: " << DebugStringOptions(options));

    Dedupv1dVolume* new_volume = ConfigureNewVolume(false, options);
    CHECK_RETURN(new_volume, NULL, "Cannot create new volume: " << DebugStringOptions(options));

    if (!CheckNewVolume(new_volume)) {
        ERROR("New volume is not valid: " << new_volume->DebugString());
        delete new_volume;
        return NULL;
    }
    if (!new_volume->Start(base_dedup_system_)) {
        ERROR("Cannot start volume " << new_volume->DebugString());
        delete new_volume;
        return NULL;
    }
    if (!this->RegisterVolume(new_volume, true)) {
        ERROR("Failed to register volume: " << new_volume->DebugString());
        delete new_volume;
        return NULL;
    }
    // do not delete the volume from now on as it is registered
    VolumeInfoData volume_info;
    CHECK_RETURN(new_volume->SerializeTo(&volume_info), NULL,
        "Cannot fill volume info settings: " << new_volume->DebugString());

    uint32_t volume_id_key = new_volume->id();
    CHECK_RETURN(info_->Put(&volume_id_key, sizeof(uint32_t), volume_info), NULL,
        "Cannot store volume info: " << new_volume->DebugString());

    if (running_) {
        CHECK_RETURN(new_volume->Run(), NULL, "Cannot run volume " << new_volume->DebugString());
    }
    CHECK_RETURN(scoped_lock.ReleaseLock(), NULL, "Failed to release lock");
    return new_volume;
}

Dedupv1dVolume* Dedupv1dVolumeInfo::FindVolumeByGroup(const string& group, uint64_t lun, dedupv1::base::MutexLock** lock) {
    Dedupv1dVolume* volume = NULL;
    CHECK_RETURN(this->lock_.AcquireLock(), NULL, "Cannot acquire lock");
    volume = FindVolumeByGroupLocked(group, lun);
    if (volume == NULL) {
        CHECK_RETURN(this->lock_.ReleaseLock(), NULL, "Cannot release lock");
        return NULL;
    }
    if (lock) {
        *lock = &this->lock_;
    } else {
        CHECK_RETURN(this->lock_.ReleaseLock(), NULL, "Cannot release lock");
    }
    return volume;
}

Option<list<pair<uint32_t, uint64_t> > > Dedupv1dVolumeInfo::FindVolumesInGroup(const string& group_name) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    DEBUG("Search volumes in group " << group_name);

    list<pair<uint32_t, uint64_t> > volume_list;
    for (std::multimap<std::string, std::pair<uint64_t, Dedupv1dVolume*> >::iterator i = group_map_.begin();
         i != group_map_.end(); i++) {
        if (i->first != group_name) {
            continue;
        }
        pair<uint64_t, Dedupv1dVolume*> p = i->second;
        DEBUG("Found volume " << p.second->DebugString() << ", lun" << p.first << ", group " << i->first);
        CHECK(p.second, "Volume not set");
        volume_list.push_back(make_pair(p.second->id(), p.first));
    }

    CHECK(scoped_lock.ReleaseLock(), "Cannot release lock");
    return make_option(volume_list);
}

Option<list<pair<uint32_t, uint64_t> > > Dedupv1dVolumeInfo::FindVolumesInTarget(const string& target_name) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    TRACE("Find volumes in target " << target_name);

    list<pair<uint32_t, uint64_t> > volume_list;
    for (std::multimap<std::string, std::pair<uint64_t, Dedupv1dVolume*> >::iterator i = target_map_.begin();
         i != target_map_.end(); i++) {
        if (i->first != target_name) {
            continue;
        }
        pair<uint64_t, Dedupv1dVolume*> p = i->second;
        CHECK(p.second, "Volume not set");
        volume_list.push_back(make_pair(p.second->id(), p.first));
    }

    CHECK(scoped_lock.ReleaseLock(), "Cannot release lock");
    return make_option(volume_list);
}

Dedupv1dVolume* Dedupv1dVolumeInfo::FindVolumeByTarget(const string& target, uint64_t lun, dedupv1::base::MutexLock** lock) {
    Dedupv1dVolume* volume = NULL;
    CHECK_RETURN(this->lock_.AcquireLock(), NULL, "Cannot acquire lock");
    volume = FindVolumeByTargetLocked(target, lun);
    if (volume == NULL) {
        CHECK_RETURN(this->lock_.ReleaseLock(), NULL, "Cannot release lock");
        return NULL;
    }

    if (lock) {
        *lock = &this->lock_;
    } else {
        CHECK_RETURN(this->lock_.ReleaseLock(), NULL, "Cannot release lock");
    }
    return volume;
}

Dedupv1dVolume* Dedupv1dVolumeInfo::FindVolumeByGroupLocked(const string& group, uint64_t lun) {
    multimap<string, std::pair<uint64_t, Dedupv1dVolume*> >::iterator i;

    for (i = this->group_map_.find(group); i != this->group_map_.end(); i++) {
        std::pair<uint64_t, Dedupv1dVolume*> p = i->second;
        if (p.first == lun) {
            return p.second;
        }
    }
    return NULL;
}

Dedupv1dVolume* Dedupv1dVolumeInfo::FindVolumeByTargetLocked(const string& target, uint64_t lun) {
    multimap<string, std::pair<uint64_t, Dedupv1dVolume*> >::iterator i;

    for (i = this->target_map_.find(target); i != this->target_map_.end(); i++) {
        std::pair<uint64_t, Dedupv1dVolume*> p = i->second;
        if (p.first == lun) {
            return p.second;
        }
    }
    return NULL;
}

Dedupv1dVolume* Dedupv1dVolumeInfo::FindVolume(uint32_t id, dedupv1::base::MutexLock** lock) {
    Dedupv1dVolume* volume = NULL;
    CHECK_RETURN(this->lock_.AcquireLock(), NULL, "Cannot acquire lock");
    volume = FindVolumeLocked(id);
    if (volume == NULL) {
        CHECK_RETURN(this->lock_.ReleaseLock(), NULL, "Cannot release lock");
        return NULL;
    }
    if (lock) {
        *lock = &this->lock_;
    } else {
        CHECK_RETURN(this->lock_.ReleaseLock(), NULL, "Cannot release lock");
    }
    return volume;
}

dedupv1::base::Option<std::list<Dedupv1dVolume*> >  Dedupv1dVolumeInfo::GetVolumes(dedupv1::base::MutexLock** lock) {
    if (lock) {
        CHECK(this->lock_.AcquireLock(), "Cannot acquire lock");
        *lock = &this->lock_;
    }
    return make_option(volumes_);
}

Dedupv1dVolume* Dedupv1dVolumeInfo::FindVolumeLocked(uint32_t id) {
    if (this->volume_map_.find(id) != this->volume_map_.end()) {
        return this->volume_map_[id];
    }
    return NULL;
}

bool Dedupv1dVolumeInfo::DetachVolume(uint32_t id) {
    CHECK(this->started_, "Volume info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireLock(), NULL, "Cannot acquire lock");

    // We recheck here, because of possible race conditions
    Dedupv1dVolume* volume = FindVolumeLocked(id);
    CHECK(volume, "Cannot find volume " << id);

    INFO("Detach volume: " << volume->DebugString());
    CHECK(volume->is_preconfigured() == false, "Cannot detach pre-configured volume " << volume->id());
    CHECK(volume->groups().size() == 0, "Cannot detach volume: " << volume->DebugString());
    CHECK(volume->targets().size() == 0, "Cannot detach volume: " << volume->DebugString());
    CHECK(volume->session_count() == 0, "Cannot detach volume " << volume->id() << " with sessions: volume " << volume->DebugString());
    CHECK(!fast_copy_->IsFastCopySource(id), "Cannot detach in-progress fast-copy source: " << volume->DebugString());
    CHECK(!fast_copy_->IsFastCopyTarget(id), "Cannot detach in-progress fast-copy target: " << volume->DebugString());

    // stop volume so that no further requests are processed
    if (this->running_) {
        CHECK(volume->Stop(dedupv1::StopContext::FastStopContext()), "Cannot stop volume: " << volume->DebugString());
    }
    // detach at base volume info: We than have a VOLUME_DETACH event logged
    CHECK(base_volume_info_->DetachVolume(volume->volume()), "Detaching dedup volume failed: " << volume->DebugString());

    uint32_t volume_id_key = volume->id();
    CHECK(info_->Delete(&volume_id_key, sizeof(volume_id_key)) != DELETE_ERROR, "Cannot store volume info");

    volume_map_.erase(id);
    volume_name_map_.erase(volume->device_name());
    volumes_.erase(::std::find(volumes_.begin(), volumes_.end(), volume));

    CHECK(this->detacher_->DetachVolume(*volume), "Failed to inform detacher about detached volume: " << volume->DebugString());
    delete volume;
    volume = NULL;
    return true;
}

bool Dedupv1dVolumeInfo::Stop(const dedupv1::StopContext& stop_context) {
    INFO("Stopping dedupv1d volume info");

    list<Dedupv1dVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        Dedupv1dVolume* v = *i;
        CHECK(base_volume_info_->UnregisterVolume(v->volume()), "Cannot unregister volume");
        CHECK(v->Stop(stop_context), "Cannot stop volume " << v->device_name());
    }
    CHECK(this->detacher_->Stop(stop_context), "Failed to stop volume detacher");
    CHECK(this->fast_copy_->Stop(stop_context), "Failed to stop volume cloner");

    running_ = false;
    return true;
}

bool Dedupv1dVolumeInfo::Run() {
    CHECK(started_, "Volume info not started");
    CHECK(!running_, "Volume info already running");

    DEBUG("Run volumes");

    list<Dedupv1dVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        Dedupv1dVolume* v = *i;
        CHECK(v->Run(), "Cannot run volume: " << v->DebugString());
    }
    CHECK(this->detacher_->Run(), "Failed to run volume detacher");
    CHECK(this->fast_copy_->Run(), "Failed to run volume cloner");
    running_ = true;
    return true;
}

string Dedupv1dVolumeInfo::DebugStringOptions(const list< pair< string, string> >& options) {
    string s = "[";
    list< pair< string, string> >::const_iterator i;
    for (i = options.begin(); i != options.end(); i++) {
        if (i != options.begin()) {
            s += ", ";
        }
        s += i->first + ":" + i->second;
    }
    s += "]";
    return s;
}

bool Dedupv1dVolumeInfo::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    list<Dedupv1dVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        Dedupv1dVolume* v = *i;
        CHECK(v->PersistStatistics(prefix + ".volume-" + ToString(v->id()), ps),
            "Failed to persist statistics of volume: " << v->DebugString());
    }
    CHECK(scoped_lock.ReleaseLock(), "Cannot acquire lock");
    return true;
}

bool Dedupv1dVolumeInfo::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    list<Dedupv1dVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        Dedupv1dVolume* v = *i;
        CHECK(v->RestoreStatistics(prefix + ".volume-" + ToString(v->id()), ps),
            "Failed to restore statistics of volume: " << v->DebugString());
    }
    CHECK(scoped_lock.ReleaseLock(), "Cannot acquire lock");
    return true;
}

std::string Dedupv1dVolumeInfo::PrintTrace() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireLock(), "", "Cannot acquire lock");

    stringstream sstr;
    sstr << "{";
    list<Dedupv1dVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        Dedupv1dVolume* v = *i;
        if (i != volumes_.begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" + ToString(v->id()) + "\": " + v->PrintTrace();
    }
    sstr << "}";
    CHECK_RETURN(scoped_lock.ReleaseLock(), "", "Cannot acquire lock");
    return sstr.str();
}

std::string Dedupv1dVolumeInfo::PrintLockStatistics() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireLock(), "", "Cannot acquire lock");

    stringstream sstr;
    sstr << "{";
    list<Dedupv1dVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        Dedupv1dVolume* v = *i;
        if (i != volumes_.begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" + ToString(v->id()) + "\": " + v->PrintLockStatistics();
    }
    sstr << "}";
    CHECK_RETURN(scoped_lock.ReleaseLock(), "", "Cannot acquire lock");
    return sstr.str();
}

std::string Dedupv1dVolumeInfo::PrintProfile() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireLock(), "", "Cannot acquire lock");

    stringstream sstr;
    sstr << "{";
    list<Dedupv1dVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        Dedupv1dVolume* v = *i;
        if (i != volumes_.begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" + ToString(v->id()) + "\": " + v->PrintProfile();
    }
    sstr << "}";
    CHECK_RETURN(scoped_lock.ReleaseLock(), "", "Cannot acquire lock");
    return sstr.str();
}

std::string Dedupv1dVolumeInfo::PrintStatistics() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireLock(), "", "Cannot acquire lock");

    stringstream sstr;
    sstr << "{";
    list<Dedupv1dVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        Dedupv1dVolume* v = *i;
        if (i != volumes_.begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" + ToString(v->id()) + "\": " + v->PrintStatistics();
    }
    sstr << "}";
    CHECK_RETURN(scoped_lock.ReleaseLock(), "", "Cannot acquire lock");
    return sstr.str();
}

/*
 * sums up the volume statistics and returns them json-formatted
 */
std::string Dedupv1dVolumeInfo::PrintStatisticSummary() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireLock(), "", "Cannot acquire lock");

    uint64_t cumulative_scsi_command_count = 0;
    uint64_t cumulative_sector_read_count = 0;
    uint64_t cumulative_sector_write_count = 0;
    uint64_t cumulative_retry_count = 0;
    double cumulative_write_throughput = 0.0;
    double cumulative_read_throughput = 0.0;

    list<Dedupv1dVolume*>::iterator i;
    Dedupv1dVolume* v = NULL;
    CommandHandler::Statistics* s = NULL;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        v = *i;
        s = v->command_handler()->stats();
        cumulative_scsi_command_count += s->scsi_command_count_;
        cumulative_sector_read_count += s->sector_read_count_;
        cumulative_sector_write_count += s->sector_write_count_;
        cumulative_retry_count += s->retry_count_;
        cumulative_write_throughput += s->average_write_throughput();
        cumulative_read_throughput += s->average_read_throughput();
    }

    stringstream sstr;
    sstr << "{";
    sstr << "\"cumulative scsi command count\": " << cumulative_scsi_command_count << "," << std::endl;
    sstr << "\"cumulative sector read count\": " << cumulative_sector_read_count << "," << std::endl;
    sstr << "\"cumulative sector write count\": " << cumulative_sector_write_count << "," << std::endl;
    sstr << "\"cumulative retry count\": " << cumulative_retry_count << "," << std::endl;
    sstr << "\"cumulative write throughput\": " << cumulative_write_throughput << "," << std::endl;
    sstr << "\"cumulative read throughput\": " << cumulative_read_throughput << std::endl;
    sstr << "}";

    CHECK_RETURN(scoped_lock.ReleaseLock(), "", "Cannot acquire lock");
    return sstr.str();
}

#ifdef DEDUPV1D_TEST
void Dedupv1dVolumeInfo::ClearData() {
    if (this->info_) {
        delete info_;
        this->info_ = NULL;
    }
    if (this->detacher_) {
        this->detacher_->ClearData();
    }
    for (std::list<Dedupv1dVolume*>::iterator i = volumes_.begin(); i != volumes_.end(); i++) {
        Dedupv1dVolume* v = *i;
        v->ClearData();
    }
}
#endif

}

