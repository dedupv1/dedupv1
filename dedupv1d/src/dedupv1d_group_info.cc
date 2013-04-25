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

#include "dedupv1d_group_info.h"

#include <base/index.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <sstream>
#include <algorithm>

#include "dedupv1d.pb.h"

using std::string;
using std::list;
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

LOGGER("Dedupv1dGroupInfo");

namespace dedupv1d {

Dedupv1dGroupInfo::Dedupv1dGroupInfo() {
    info_ = NULL;
    started_ = false;
}

bool Dedupv1dGroupInfo::Start(const StartContext& start_context) {
    CHECK(this->started_ == false, "Group info already started");

    CHECK(this->info_ != NULL, "Info storage not set");
    CHECK(this->info_->SupportsCursor(), "Index doesn't support cursor");
    CHECK(this->info_->IsPersistent(), "Group info index should be persistent");

    INFO("Start dedupv1d group info");

    CHECK(this->info_->Start(start_context), "Cannot start group info");

    if (this->current_group_options_.size() > 0) {
        group_options_.push_back(this->current_group_options_);
        this->current_group_options_.clear();
    }
    // add pre-configured group
    list< list < pair<string, string> > >::iterator i;
    for (i = group_options_.begin(); i != group_options_.end(); i++) {
        DEBUG("Found config " << DebugStringOptions(*i));
        list< pair<string, string> > concrete_group_option = *i;
        Dedupv1dGroup new_group(true);
        CHECK(ConfigureNewGroup(concrete_group_option, &new_group),
            "Failed to create new group: " << DebugStringOptions(concrete_group_option));
        if (!CheckGroup(new_group)) {
            ERROR("Group not valid: " << new_group.DebugString());
            return false;
        }
        if (!this->RegisterGroup(new_group)) {
            ERROR("Failed to register group: " << new_group.DebugString());
            return false;
        }
        INFO("Found group " << new_group.name() << " (pre configured)");
    }

    IndexCursor* cursor = this->info_->CreateCursor();
    CHECK(cursor, "Cannot create cursor");

    // start dynamic groups
    enum lookup_result ir = cursor->First();
    CHECK(ir != LOOKUP_ERROR, "Cannot read group info");
    while (ir == LOOKUP_FOUND) {
        GroupInfoData group_info;
        CHECK(cursor->Get(NULL, NULL, &group_info), "Get group info value");

        Dedupv1dGroup new_group(false);
        CHECK(new_group.ParseFrom(group_info), "Failed to configure group: " << new_group.DebugString());
        CHECK(this->CheckGroup(new_group), "Group not valid: " << new_group.DebugString());
        CHECK(this->RegisterGroup(new_group), "Failed to register group: " << new_group.DebugString());
        INFO("Found group " << new_group.name() << " (dynamic): " << new_group.DebugString());

        ir = cursor->Next();
        CHECK(ir != LOOKUP_ERROR, "Cannot read volume info storage");
    }
    delete cursor;
    this->started_ = true;
    return true;
}

Dedupv1dGroupInfo::~Dedupv1dGroupInfo() {
    DEBUG("Closing dedupv1d group info");

    this->group_list_.clear();
    this->group_map_.clear();
    if (info_) {
        delete info_;
        info_ = NULL;
    }
}

bool Dedupv1dGroupInfo::SetOption(const string& option_name, const string& option) {
    CHECK(this->started_ == false, "Illegal state");

    if (option_name == "group") {
        if (this->current_group_options_.size() > 0) {
            group_options_.push_back(this->current_group_options_);
            this->current_group_options_.clear();
        }
        this->current_group_options_.push_back(pair<string, string>("name", option));
        return true;
    }
    if (StartsWith(option_name, "group.")) {
        string revised_option_name = option_name.substr(strlen("group."));
        CHECK(revised_option_name != "name", "Illegal option: group.name");

        CHECK(this->current_group_options_.size() > 0, "No group to configure set");
        this->current_group_options_.push_back(pair<string, string>(revised_option_name, option));
        return true;
    }

    if (option_name == "type") {
        CHECK(this->info_ == NULL, "Index type already set");
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Cannot create index: " << option);
        this->info_ = index->AsPersistentIndex();
        CHECK(this->info_, "Info should be persistent");
        return true;
    }
    CHECK(this->info_, "Index not set");
    CHECK(this->info_->SetOption(option_name, option), "Failed to configure index");
    return true;
}

bool Dedupv1dGroupInfo::ConfigureNewGroup(std::list< std::pair< std::string, std::string> > group_options,
                                          dedupv1d::Dedupv1dGroup* new_group) {
    CHECK(new_group, "New group not set");

    DEBUG("Create new group: " << DebugStringOptions(group_options));
    list< pair<string, string> >::const_iterator i;
    for (i = group_options.begin(); i != group_options.end(); i++) {
        string option_name = i->first;
        string option = i->second;
        if (!new_group->SetOption(option_name, option)) {
            ERROR("Cannot configure group: " << option_name << "=" << option);
            return false;
        }
    }
    return true;
}

bool Dedupv1dGroupInfo::CheckGroup(const Dedupv1dGroup& group) {
    CHECK(group.name().size() > 0, "Group name not set");
    CHECK(group_map_.find(group.name()) == group_map_.end(),
        "Group with name " << group.name() << " exists already: " << group.DebugString());

    if (StartsWith(group.name(), "Default")) {
        if (!(group.name() == "Default" && group.is_preconfigured())) {
            // a preconfigured group Default is an exception
            ERROR("Groups are not allowed to start with Default: " << group.DebugString());
            return false;
        }
    }

    return true;
}

bool Dedupv1dGroupInfo::RegisterGroup(const Dedupv1dGroup& group) {
    // from now on it is atomic
    group_map_[group.name()] = group;
    group_list_.push_back(group.name());
    return true;
}

Option<Dedupv1dGroup> Dedupv1dGroupInfo::FindGroupLocked(std::string group_name) {
    std::map<std::string, dedupv1d::Dedupv1dGroup>::const_iterator i = this->group_map_.find(group_name);
    if (i == this->group_map_.end()) {
        return false; // no group found
    }
    return make_option(i->second);
}

Option<Dedupv1dGroup> Dedupv1dGroupInfo::FindGroup(std::string group_name) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");
    Option<Dedupv1dGroup> r = FindGroupLocked(group_name);
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return r;
}

bool Dedupv1dGroupInfo::AddInitiatorPattern(std::string group_name, std::string initiator_pattern) {
    CHECK(this->started_, "Group info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Add initiator pattern: group " << group_name << ", initiator pattern " << initiator_pattern);

    Option<Dedupv1dGroup> group = this->FindGroupLocked(group_name);
    CHECK(group.valid(), "Group not existing: " << group_name);
    CHECK(!group.value().is_preconfigured(), "Cannot remove initiator pattern from preconfigured group: group name " << group_name);

    Dedupv1dGroup new_group = group.value();
    CHECK(new_group.AddInitiatorPattern(initiator_pattern), "Failed to add initiator pattern to group");

    group_map_[new_group.name()] = new_group;

    GroupInfoData group_info;
    CHECK(new_group.SerializeTo(&group_info), "Cannot fill group info settings");

    CHECK(info_->Put(new_group.name().data(), new_group.name().size(), group_info), "Cannot store group info");
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dGroupInfo::RemoveInitiatorPattern(std::string group_name, std::string initiator_pattern) {
    CHECK(this->started_, "Group info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Remove initiator pattern: group " << group_name << ", initiator pattern " << initiator_pattern);

    Option<Dedupv1dGroup> group = this->FindGroupLocked(group_name);
    CHECK(group.valid(), "User not existing: " << group_name);
    CHECK(!group.value().is_preconfigured(), "Cannot add initiator pattern to preconfigured group: group name " << group_name);

    Dedupv1dGroup new_group = group.value();
    CHECK(new_group.RemoveInitiatorPattern(initiator_pattern), "Failed to remove initiator pattern from group");

    group_map_[new_group.name()] = new_group;

    GroupInfoData group_info;
    CHECK(new_group.SerializeTo(&group_info), "Cannot fill group info settings");

    CHECK(info_->Put(new_group.name().data(), new_group.name().size(), group_info), "Cannot store group info");
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dGroupInfo::RemoveGroup(string group_name) {
    CHECK(this->started_, "Group info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Remove group: group name " << group_name);

    std::map<string, dedupv1d::Dedupv1dGroup>::iterator i = this->group_map_.find(group_name);
    CHECK(i != this->group_map_.end(), "Group not found: group name " << group_name);

    CHECK(!i->second.is_preconfigured(), "Cannot remove preconfigured group: group name " << group_name);

    // now delete
    this->group_map_.erase(group_name);
    for (list<string>::iterator j = this->group_list_.begin(); j != this->group_list_.end(); j++) {
        if (*j == group_name) {
            this->group_list_.erase(j);
            break;
        }
    }

    CHECK(info_->Delete(group_name.data(), group_name.size()), "Cannot delete group info");

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dGroupInfo::AddGroup(list< pair< string, string> > options) {
    CHECK(this->started_, "Group info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Add group: " << DebugStringOptions(options));

    Dedupv1dGroup new_group(false);
    CHECK(ConfigureNewGroup(options, &new_group), "Cannot create new group: " << DebugStringOptions(options));

    if (!CheckGroup(new_group)) {
        ERROR("New group is not valid: " << new_group.DebugString());
        return false;
    }
    if (!this->RegisterGroup(new_group)) {
        ERROR("Failed to register group: " << new_group.DebugString());
        return false;
    }
    // do not delete the volume from now on as it is registered
    GroupInfoData group_info;

    CHECK(new_group.SerializeTo(&group_info), "Cannot fill group info settings");

    CHECK(info_->Put(new_group.name().data(), new_group.name().size(), group_info), "Cannot store group info");
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

Option<list<string> > Dedupv1dGroupInfo::GetGroupNames() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    list<string> copy = group_names();
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return make_option(copy);
}

string Dedupv1dGroupInfo::DebugStringOptions(const list< pair< string, string> >& options) {
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

#ifdef DEDUPV1D_TEST
void Dedupv1dGroupInfo::ClearData() {
    if (this->info_) {
        delete info_;
        this->info_ = NULL;
    }
}
#endif

}

