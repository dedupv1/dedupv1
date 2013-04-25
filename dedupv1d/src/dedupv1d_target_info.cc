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
#include "dedupv1d_target_info.h"

#include <base/index.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <sstream>
#include <algorithm>

#include "dedupv1d.pb.h"

#include "dedupv1d_volume_info.h"
#include "dedupv1d_user_info.h"

using std::string;
using std::list;
using std::map;
using std::pair;
using std::multimap;
using std::vector;
using std::stringstream;
using dedupv1::base::ScopedLock;
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

LOGGER("Dedupv1dTargetInfo");

namespace dedupv1d {

Dedupv1dTargetInfo::Dedupv1dTargetInfo() {
    info_ = NULL;
    started_ = false;
    volume_info_ = NULL;
    user_info_ = NULL;
}

bool Dedupv1dTargetInfo::Start(const StartContext& start_context,
                               Dedupv1dVolumeInfo* volume_info,
                               Dedupv1dUserInfo* user_info) {
    CHECK(this->started_ == false, "Target info already started");

    CHECK(this->info_ != NULL, "Info storage not set");
    CHECK(this->info_->SupportsCursor(), "Index doesn't support cursor");
    CHECK(this->info_->IsPersistent(), "Target info index should be persistent");

    CHECK(volume_info, "Volume info not set");
    volume_info_ = volume_info;

    user_info_ = user_info;

    INFO("Start dedupv1d target info");

    CHECK(this->info_->Start(start_context), "Cannot start target info");

    if (this->current_target_options_.size() > 0) {
        target_options_.push_back(this->current_target_options_);
        this->current_target_options_.clear();
    }

    // add pre-configured target
    list< list < pair<string, string> > >::iterator i;
    for (i = target_options_.begin(); i != target_options_.end(); i++) {
        DEBUG("Found config " << DebugStringOptions(*i));
        list< pair<string, string> > concrete_target_option = *i;
        Dedupv1dTarget new_target(true);
        CHECK(ConfigureNewTarget(concrete_target_option, &new_target),
            "Failed to create new target: " << DebugStringOptions(concrete_target_option));
        if (!CheckTarget(new_target)) {
            ERROR("Target not valid: " << new_target.DebugString());
            return false;
        }
        if (!this->RegisterTarget(new_target)) {
            ERROR("Failed to register target: " << new_target.DebugString());
            return false;
        }
        INFO("Found target " << new_target.name() << " (pre configured)");
    }

    IndexCursor* cursor = this->info_->CreateCursor();
    CHECK(cursor, "Cannot create cursor");

    // start dynamic targets
    enum lookup_result ir = cursor->First();
    CHECK(ir != LOOKUP_ERROR, "Cannot read target info");
    while (ir == LOOKUP_FOUND) {
        TargetInfoData target_info;
        CHECK(cursor->Get(NULL, NULL, &target_info), "Get target info value");

        Dedupv1dTarget new_target(false);
        CHECK(new_target.ParseFrom(target_info), "Failed to configure target: " << new_target.DebugString());
        CHECK(this->CheckTarget(new_target), "Target not valid: " << new_target.DebugString());
        CHECK(this->RegisterTarget(new_target), "cannot register target: " << new_target.DebugString());
        INFO("Found target " << new_target.name() << " (dynamic)");

        ir = cursor->Next();
        CHECK(ir != LOOKUP_ERROR, "Cannot read volume info storage");
    }
    delete cursor;
    this->started_ = true;
    return true;
}

Dedupv1dTargetInfo::~Dedupv1dTargetInfo() {
    this->target_list_.clear();
    this->target_map_.clear();
    if (info_) {
        delete info_;
        info_ = NULL;
    }
}

bool Dedupv1dTargetInfo::SetOption(const string& option_name, const string& option) {
    CHECK(this->started_ == false, "Illegal state");

    if (option_name == "target") {
        if (this->current_target_options_.size() > 0) {
            target_options_.push_back(this->current_target_options_);
            this->current_target_options_.clear();
        }
        this->current_target_options_.push_back(pair<string, string>("tid", option));
        return true;
    }
    if (StartsWith(option_name, "target.")) {
        string revised_option_name = option_name.substr(strlen("target."));
        CHECK(revised_option_name != "tid", "Illegal option: target.tid");

        CHECK(this->current_target_options_.size() > 0, "No target to configure set");
        this->current_target_options_.push_back(pair<string, string>(revised_option_name, option));
        return true;
    }

    if (option_name == "type") {
        CHECK(this->info_ == NULL, "Index type already set");
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Cannot create index: " << option);
        this->info_ = index->AsPersistentIndex();
        CHECK(this->info_, "Info should be persistent");
        CHECK(this->info_->SetOption("max-key-size", "4"), "Failed to set max key size");
        return true;
    }
    CHECK(this->info_, "Index not set");
    CHECK(this->info_->SetOption(option_name, option), "Failed to configure index");
    return true;
}

bool Dedupv1dTargetInfo::ConfigureNewTarget(std::list< std::pair< std::string, std::string> > target_options,
                                            dedupv1d::Dedupv1dTarget* new_target) {
    CHECK(new_target, "New target not set");

    DEBUG("Create new target: " << DebugStringOptions(target_options));
    list< pair<string, string> >::const_iterator i;
    for (i = target_options.begin(); i != target_options.end(); i++) {
        string option_name = i->first;
        string option = i->second;
        if (!new_target->SetOption(option_name, option)) {
            ERROR("Cannot configure target: " << option_name << "=" << option);
            return false;
        }
    }
    return true;
}

bool Dedupv1dTargetInfo::CheckTarget(const Dedupv1dTarget& target) {
    CHECK(target.name().size() > 0, "Target name not set");
    CHECK(target.tid() > 0, "Target id not set");
    CHECK(target_map_.find(target.name()) == target_map_.end(),
        "Target with name " << target.name() << " exists already");
    CHECK(target_id_map_.find(target.tid()) == target_id_map_.end(),
        "Target with tid " << target.tid() << " exists already");
    return true;
}

bool Dedupv1dTargetInfo::RegisterTarget(const Dedupv1dTarget& target) {
    TRACE("Register target: " << target.DebugString());
    // from now on it is atomic
    target_list_.push_back(target);
    target_id_map_[target.tid()] = --target_list_.end();
    target_map_[target.name()] = --target_list_.end();
    return true;
}

Option<Dedupv1dTarget> Dedupv1dTargetInfo::FindTargetByName(const string& target_name) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");
    map<string, list<dedupv1d::Dedupv1dTarget>::iterator>::const_iterator i = this->target_map_.find(target_name);
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");

    if (i == this->target_map_.end()) {
        return false; // no target found
    }
    return make_option(*(i->second));
}

Option<Dedupv1dTarget> Dedupv1dTargetInfo::FindTargetByNameLocked(const string& target_name) {
    map<string, list<dedupv1d::Dedupv1dTarget>::iterator>::const_iterator i = this->target_map_.find(target_name);
    if (i == this->target_map_.end()) {
        return false; // no target found
    }
    return make_option(*(i->second));
}

Option<Dedupv1dTarget> Dedupv1dTargetInfo::FindTarget(uint32_t tid) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");
    map<uint32_t, list<dedupv1d::Dedupv1dTarget>::iterator>::const_iterator i = this->target_id_map_.find(tid);
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");

    if (i == this->target_id_map_.end()) {
        return false; // no target found
    }
    return make_option(*(i->second));
}

bool Dedupv1dTargetInfo::RemoveTarget(uint32_t tid) {
    CHECK(this->started_, "Target info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Remove target: tid " << tid);

    map<uint32_t, list<dedupv1d::Dedupv1dTarget>::iterator>::iterator i = this->target_id_map_.find(tid);
    CHECK(i != this->target_id_map_.end(), "Target not found: tid " << tid);

    CHECK(!i->second->is_preconfigured(), "Cannot remove preconfigured target: tid " << tid);
    string target_name = i->second->name();

    // now delete
    this->target_id_map_.erase(i);
    this->target_map_.erase(target_name);
    for (std::list<dedupv1d::Dedupv1dTarget>::iterator j = this->target_list_.begin(); j != this->target_list_.end(); j++) {
        if (j->name() == target_name) {
            this->target_list_.erase(j);
            break;
        }
    }

    CHECK(info_->Delete(&tid, sizeof(tid)), "Cannot delete target info");

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dTargetInfo::AddTarget(list< pair< string, string> > options) {
    CHECK(this->started_, "Target info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Add target: " << DebugStringOptions(options));

    Dedupv1dTarget new_target(false);
    CHECK(ConfigureNewTarget(options, &new_target), "Cannot create new target: " << DebugStringOptions(options));

    if (!CheckTarget(new_target)) {
        ERROR("New target is not valid: " << new_target.DebugString());
        return false;
    }
    if (!this->RegisterTarget(new_target)) {
        ERROR("Failed to register target: " << new_target.DebugString());
        return false;
    }
    // do not delete the volume from now on as it is registered
    TargetInfoData target_info;

    CHECK(new_target.SerializeTo(&target_info), "Cannot fill target info settings");

    uint32_t tid = new_target.tid();
    CHECK(info_->Put(&tid, sizeof(tid), target_info), "Cannot store target info");
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dTargetInfo::ChangeTargetParams(uint32_t tid, const list< pair< string, string> >& param_options) {
    CHECK(this->started_, "Target info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Change target params: tid " << tid << ", options " << DebugStringOptions(param_options));
    map<uint32_t, list<dedupv1d::Dedupv1dTarget>::iterator>::iterator i = this->target_id_map_.find(tid);
    CHECK(i != this->target_id_map_.end(), "Target not found: tid " << tid);

    Dedupv1dTarget target(*(i->second));
    CHECK(!target.is_preconfigured(),
        "Cannot change params of preconfigured target: tid " << tid);

    string old_target_name;
    list< pair< string, string> >::const_iterator j;
    for (j = param_options.begin(); j != param_options.end(); j++) {

        if (j->first == "name") {
            if (target.name() == j->second) {
                continue;
            }
            Option<Dedupv1dTarget> name_target = FindTargetByNameLocked(j->second);
            CHECK(name_target.valid() == false, "Failed to change target param: " <<
                "target " << target.DebugString() <<
                ", param " << j->first << "=" << j->second);
            old_target_name = target.name();
        }
        CHECK(target.ChangeParam(j->first, j->second),
            "Failed to change target param: " <<
            "target " << target.DebugString() <<
            ", param " << j->first << "=" << j->second);
    }

    // here the target_list_ is updated
    *(i->second) = target;

    bool failed = false;
    if (!old_target_name.empty()) { // name of the target changed
        Option<list<pair<uint32_t, uint64_t> > > volumes = volume_info_->FindVolumesInTarget(old_target_name);
        CHECK(volumes.valid(), "Failed to get volumes for target");

        list<pair<uint32_t, uint64_t> >::const_iterator k;
        bool failed = false;
        for (k = volumes.value().begin(); k != volumes.value().end(); k++) {
            if (!volume_info_->RemoveFromTarget(k->first, old_target_name)) {
                ERROR("Failed to remove volume from target");
                failed = true;
            } else {
                if (!volume_info_->AddToTarget(k->first, target.name() + ":" + ToString(k->second))) {
                    ERROR("Failed to re-add volume to target");
                    failed = true;
                }
            }
        }

        if (!failed) {
            Option<list<string> > users = user_info_->GetUsersInTarget(old_target_name);
            CHECK(users.valid(), "Failed to get users for target");

            list<string>::const_iterator l;
            for (l = users.value().begin(); l != users.value().end() && !failed; ++l) {
                if (!user_info_->RemoveUserFromTarget(*l, old_target_name)) {
                    ERROR("Failed to remove user from target: " <<
                        "old target name " << old_target_name <<
                        ", new target name " << target.name() <<
                        ", user " << *l);
                    failed = true;
                } else {
                    // remove as ok
                    if (!user_info_->AddUserToTarget(*l, target.name())) {
                        ERROR("Failed to re-add user to target: " <<
                            "old target name " << old_target_name <<
                            ", new target name " << target.name() <<
                            ", user " << *l);
                        failed = true;
                    }
                }
            }
        }

        // if the old target name has changed, we have to update the map
        if (!failed) {
            target_map_[target.name()] = i->second;
            target_map_.erase(old_target_name);
        }
    }

    if (!failed) {
        TargetInfoData target_info;
        CHECK(target.SerializeTo(&target_info), "Cannot fill target info settings: " << target.DebugString());
        CHECK(info_->Put(&tid, sizeof(tid), target_info), "Cannot store target info: " << target.DebugString() <<
            ", data " << target_info.ShortDebugString());
    }
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return !failed;
}

string Dedupv1dTargetInfo::DebugStringOptions(const list< pair< string, string> >& options) {
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

Option<list<Dedupv1dTarget> > Dedupv1dTargetInfo::GetTargets() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    list<dedupv1d::Dedupv1dTarget> copy = target_list_;
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return make_option(copy);
}

#ifdef DEDUPV1D_TEST
void Dedupv1dTargetInfo::ClearData() {
    if (this->info_) {
        delete info_;
        this->info_ = NULL;
    }
}
#endif

}
