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
#include <list>
#include <sstream>

#include "dedupv1.pb.h"

#include <core/dedup.h>
#include <core/log_consumer.h>
#include <base/locks.h>
#include <core/dedup_volume.h>
#include <core/dedup_system.h>
#include <core/log.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <core/dedup_volume_info.h>

using std::stringstream;
using std::make_pair;
using std::string;
using std::list;
using std::pair;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::StartsWith;
using dedupv1::log::Log;
using dedupv1::log::EVENT_TYPE_VOLUME_ATTACH;
using dedupv1::log::EVENT_TYPE_VOLUME_DETACH;

LOGGER("DedupVolumeInfo");

namespace dedupv1 {

DedupVolumeInfo::DedupVolumeInfo() {
    this->log_ = NULL;
}

bool DedupVolumeInfo::Start(DedupSystem* system) {
    CHECK(system, "System not set");
    this->log_ = system->log();
    CHECK(log_, "Log not set");

    DEBUG("Starting core volume info");

    for (std::list<DedupVolume*>::iterator i = raw_configured_volume_.begin();
         i != raw_configured_volume_.end();
         ++i) {
        DedupVolume* volume = *i;
        CHECK(volume->Start(system, false), "Failed to start volume: " << volume->DebugString());
        CHECK(RegisterVolume(volume), "Failed to register volume: " << volume->DebugString());
    }

    return true;
}

DedupVolumeInfo::~DedupVolumeInfo() {
    for (list<DedupVolume*>::iterator i = raw_configured_volume_.begin();
         i != raw_configured_volume_.end();
         ++i) {
        DedupVolume* volume = *i;
        if (volume == NULL) {
            continue;
        }
        if (FindVolume(volume->GetId())) {
            UnregisterVolume(volume);
        }
        delete volume;
        *i = NULL;
    }
    raw_configured_volume_.clear();

    if (volumes_.size() > 0) {
        string volume_ids = "[";
        list<DedupVolume*>::iterator i;
        for (i = this->volumes_.begin(); i != this->volumes_.end(); i++) {
            DedupVolume* volume = *i;
            if (i != this->volumes_.begin()) {
                volume_ids += ",";
            }
            volume_ids += ToString(volume->GetId());
        }
        volume_ids += "]";
        WARNING("Volumes " << volume_ids << " is still registered");
    }
}

bool DedupVolumeInfo::SetOption(const std::string& option_name, const std::string& option) {
    if (option_name == "id") {
        DedupVolume* volume = new DedupVolume();
        CHECK(volume, "Failed to allocate volume");
        raw_configured_volume_.push_back(volume);
        // fall through
    }
    CHECK(raw_configured_volume_.size() > 0, "No raw configured volume available");
    DedupVolume* volume = raw_configured_volume_.back();
    CHECK(volume->SetOption(option_name, option), "Failed to configure volume: " << volume->DebugString());
    return true;
}

DedupVolume* DedupVolumeInfo::FindVolumeLocked(uint32_t id) {
    list<DedupVolume*>::iterator i;
    for (i = this->volumes_.begin(); i != this->volumes_.end(); i++) {
        DedupVolume* volume = *i;
        if (volume->GetId() == id) {
            return *i;
        }
    }
    return NULL;
}

bool DedupVolumeInfo::IsStarted() {
    return this->log_ != NULL;
}

DedupVolume* DedupVolumeInfo::FindVolume(uint32_t id) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireReadLock(), NULL, "Cannot acquire lock");
    // The lock is automatically released

    return FindVolumeLocked(id);
}

bool DedupVolumeInfo::RegisterVolume(DedupVolume* volume) {
    CHECK(this->log_, "Volume info not started");
    CHECK(volume, "Volume not set");

    CHECK(volume->is_started(), "Volume not started: " << volume->DebugString());

    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Cannot acquire lock");
    // The lock is automatically released

    CHECK(FindVolumeLocked(volume->GetId()) == NULL, "Volume with id " << volume->GetId() << " already registered");

    volumes_.push_back(volume);
    INFO("Register volume " << volume->DebugString());
    return true;
}

bool DedupVolumeInfo::UnregisterVolume(DedupVolume* volume) {
    CHECK(this->log_, "Volume info not started");

    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Cannot acquire lock");
    // The lock is automatically released

    DedupVolume* v = FindVolumeLocked(volume->GetId());
    CHECK(v, "Volume with id " << volume->GetId() << " not existing");
    CHECK(v == volume, "Volume with id " << volume->GetId() << " is different volume object");

    volumes_.erase(::std::find(volumes_.begin(), volumes_.end(), volume));
    DEBUG("Unregister volume " << volume->DebugString());

    return true;
}

bool DedupVolumeInfo::AttachVolume(DedupVolume* volume) {
    CHECK(this->log_, "Volume info not started");
    CHECK(volume, "Volume not set");

    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Cannot acquire lock");
    // The lock is automatically released

    CHECK(FindVolumeLocked(volume->GetId()) == NULL, "Volume with id " << volume->GetId() << " already registered");

    volumes_.push_back(volume);
    DEBUG("Register volume " << volume->GetId());

    VolumeAttachedEventData event_data;
    event_data.set_volume_id(volume->GetId());
    INFO("Attach volume " << volume->GetId());

    CHECK(this->log_->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &event_data, NULL, NULL, NO_EC),
        "Cannot commit volume attach for volume " << volume->GetId());
    return true;
}

bool DedupVolumeInfo::DetachVolume(DedupVolume* volume) {
    CHECK(this->log_, "Volume info not started");
    CHECK(volume, "Volume not set");

    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireWriteLock(), "Cannot acquire lock");
    // The lock is automatically released

    CHECK(FindVolumeLocked(volume->GetId()) == volume, "Volume with id " << volume->GetId() << " not registered");

    volumes_.erase(::std::find(volumes_.begin(), volumes_.end(), volume));
    DEBUG("Unregister volume " << volume->GetId());

    VolumeDetachedEventData event_data;
    event_data.set_volume_id(volume->GetId());

    INFO("Detach volume " << volume->GetId());
    CHECK(this->log_->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &event_data, NULL, NULL, NO_EC),
        "Cannot commit volume detach for volume " << volume->GetId());
    return true;
}

uint32_t DedupVolumeInfo::GetVolumeCount() {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireReadLock(), 0, "Cannot acquire lock");

    return this->volumes_.size();
}

bool DedupVolumeInfo::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireReadLock(), "Cannot acquire lock");

    list<DedupVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        DedupVolume* v = *i;
        CHECK(v->PersistStatistics(prefix + ".volume-" + ToString(v->GetId()), ps),
            "Failed to persist statistics of volume: " << v->DebugString());
    }
    CHECK(scoped_lock.ReleaseLock(), "Cannot acquire lock");
    return true;
}

bool DedupVolumeInfo::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireReadLock(), "Cannot acquire lock");

    list<DedupVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        DedupVolume* v = *i;
        CHECK(v->RestoreStatistics(prefix + ".volume-" + ToString(v->GetId()), ps),
            "Failed to restore statistics of volume: " << v->DebugString());
    }
    CHECK(scoped_lock.ReleaseLock(), "Cannot acquire lock");
    return true;
}

std::string DedupVolumeInfo::PrintTrace() {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireReadLock(), "null", "Cannot acquire lock");

    stringstream sstr;
    sstr << "{";
    list<DedupVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        DedupVolume* v = *i;
        if (i != volumes_.begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" + ToString(v->GetId()) + "\": " + v->PrintTrace();
    }
    sstr << "}";
    CHECK_RETURN(scoped_lock.ReleaseLock(), "null", "Cannot acquire lock");
    return sstr.str();
}

std::string DedupVolumeInfo::PrintLockStatistics() {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireReadLock(), "null", "Cannot acquire lock");

    stringstream sstr;
    sstr << "{";
    list<DedupVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        DedupVolume* v = *i;
        if (i != volumes_.begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" + ToString(v->GetId()) + "\": " + v->PrintLockStatistics();
    }
    sstr << "}";
    CHECK_RETURN(scoped_lock.ReleaseLock(), "", "Cannot acquire lock");
    return sstr.str();
}

std::string DedupVolumeInfo::PrintProfile() {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireReadLock(), "null", "Cannot acquire lock");

    stringstream sstr;
    sstr << "{";
    list<DedupVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        DedupVolume* v = *i;
        if (i != volumes_.begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" + ToString(v->GetId()) + "\": " + v->PrintProfile();
    }
    sstr << "}";
    CHECK_RETURN(scoped_lock.ReleaseLock(), "null", "Cannot acquire lock");
    return sstr.str();
}

std::string DedupVolumeInfo::PrintStatistics() {
    ScopedReadWriteLock scoped_lock(&this->lock_);
    CHECK_RETURN(scoped_lock.AcquireReadLock(), "", "Cannot acquire lock");

    stringstream sstr;
    sstr << "{";
    list<DedupVolume*>::iterator i;
    for (i = volumes_.begin(); i != volumes_.end(); i++) {
        DedupVolume* v = *i;
        if (i != volumes_.begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" + ToString(v->GetId()) + "\": " + v->PrintStatistics();
    }
    sstr << "}";
    CHECK_RETURN(scoped_lock.ReleaseLock(), "", "Cannot acquire lock");
    return sstr.str();
}

}
