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
#include "dedupv1d_user_info.h"

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
using dedupv1::base::Index;
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
using dedupv1::base::make_option;

LOGGER("Dedupv1dUserInfo");

namespace dedupv1d {

Dedupv1dUserInfo::Dedupv1dUserInfo() {
    info_ = NULL;
    started_ = false;
}

bool Dedupv1dUserInfo::Start(const StartContext& start_context) {
    CHECK(this->started_ == false, "User info already started");

    CHECK(this->info_ != NULL, "Info storage not set");
    CHECK(this->info_->SupportsCursor(), "Index doesn't support cursor");
    CHECK(this->info_->IsPersistent(), "User info index should be persistent");

    INFO("Start dedupv1d user info");

    CHECK(this->info_->Start(start_context), "Cannot start user info");

    if (this->current_user_options_.size() > 0) {
        user_options_.push_back(this->current_user_options_);
        this->current_user_options_.clear();
    }
    // add pre-configured user
    list< list < pair<string, string> > >::iterator i;
    for (i = user_options_.begin(); i != user_options_.end(); i++) {
        DEBUG("Found config " << DebugStringOptions(*i));
        list< pair<string, string> > concrete_user_option = *i;
        Dedupv1dUser new_user(true);
        CHECK(ConfigureNewUser(concrete_user_option, &new_user),
            "Failed to create new user: " << DebugStringOptions(concrete_user_option));
        if (!CheckUser(new_user)) {
            ERROR("User not valid: " << new_user.DebugString());
            return false;
        }
        if (!this->RegisterUser(new_user)) {
            ERROR("Failed to register user: " << new_user.DebugString());
            return false;
        }
        INFO("Found user " << new_user.name() << " (pre configured)");
    }

    IndexCursor* cursor = this->info_->CreateCursor();
    CHECK(cursor, "Cannot create cursor");

    // start dynamic users
    enum lookup_result ir = cursor->First();
    CHECK(ir != LOOKUP_ERROR, "Cannot read user info");
    while (ir == LOOKUP_FOUND) {
        UserInfoData user_info;
        CHECK(cursor->Get(NULL, NULL, &user_info), "Get user info value");

        Dedupv1dUser new_user(false);
        CHECK(new_user.ParseFrom(user_info), "Failed to configure user: " << new_user.DebugString());
        CHECK(this->CheckUser(new_user), "User not valid: " << new_user.DebugString());
        CHECK(this->RegisterUser(new_user), "cannot register user: " << new_user.DebugString());
        INFO("Found user " << new_user.name() << " (dynamic)");

        ir = cursor->Next();
        CHECK(ir != LOOKUP_ERROR, "Cannot read volume info storage");
    }
    delete cursor;
    this->started_ = true;
    return true;
}

Dedupv1dUserInfo::~Dedupv1dUserInfo() {
    DEBUG("Closing dedupv1d user info");

    this->user_list_.clear();
    this->user_map_.clear();
    if (info_) {
        delete info_;
        info_ = NULL;
    }
}

bool Dedupv1dUserInfo::SetOption(const string& option_name, const string& option) {
    CHECK(this->started_ == false, "Illegal state");

    if (option_name == "user") {
        if (this->current_user_options_.size() > 0) {
            user_options_.push_back(this->current_user_options_);
            this->current_user_options_.clear();
        }
        this->current_user_options_.push_back(pair<string, string>("name", option));
        return true;
    }
    if (StartsWith(option_name, "user.")) {
        string revised_option_name = option_name.substr(strlen("user."));
        CHECK(revised_option_name != "name", "Illegal option: user.name");

        CHECK(this->current_user_options_.size() > 0, "No user to configure set");
        this->current_user_options_.push_back(pair<string, string>(revised_option_name, option));
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

bool Dedupv1dUserInfo::ConfigureNewUser(std::list< std::pair< std::string, std::string> > user_options,
                                        dedupv1d::Dedupv1dUser* new_user) {
    CHECK(new_user, "New user not set");

    DEBUG("Create new user: " << DebugStringOptions(user_options));
    bool secret_set = false;
    list< pair<string, string> >::const_iterator i;
    for (i = user_options.begin(); i != user_options.end(); i++) {
        string option_name = i->first;
        string option = i->second;

        if (option_name == "secret") {
            CHECK(secret_set == false, "Secret can only be set once");
            secret_set = true;
            INFO("Unencoded user secrets are depreciated");
        }
        if (option_name == "secret-hash") {
            CHECK(secret_set == false, "Secret can only be set once");
            secret_set = true;
        }

        if (!new_user->SetOption(option_name, option)) {
            ERROR("Cannot configure user: " << option_name << "=" << option);
            return false;
        }
    }
    CHECK(secret_set, "User password not set");
    return true;
}

bool Dedupv1dUserInfo::CheckUser(const Dedupv1dUser& user) {
    CHECK(user.name().size() > 0, "User name not set");
    CHECK(user_map_.find(user.name()) == user_map_.end(),
        "User with name " << user.name() << " exists already");
    return true;
}

bool Dedupv1dUserInfo::RegisterUser(const Dedupv1dUser& user) {
    // from now on it is atomic
    user_map_[user.name()] = user;
    user_list_.push_back(user.name());

    for (list<string>::const_iterator i = user.targets().begin(); i != user.targets().end(); i++) {
        target_user_map_.insert(make_pair(*i, user.name()));
    }

    return true;
}

Option<Dedupv1dUser> Dedupv1dUserInfo::FindUser(std::string user_name) {
    ScopedLock scoped_lock(&this->lock_);

    TRACE("Find user " << user_name);
    CHECK(scoped_lock.AcquireLock(), "Failed to acquire lock");
    Option<Dedupv1dUser> user = FindUserLocked(user_name);
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");

    return user;
}

Option<Dedupv1dUser> Dedupv1dUserInfo::FindUserLocked(std::string user_name) {
    std::map<std::string, dedupv1d::Dedupv1dUser>::const_iterator i = this->user_map_.find(user_name);
    if (i == this->user_map_.end()) {
        return false; // no user found
    }
    return make_option(i->second);
}

bool Dedupv1dUserInfo::AddUserToTarget(std::string user_name, std::string target_name) {
    CHECK(this->started_, "User info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Add user to target: user " << user_name << ", target " << target_name);

    Option<Dedupv1dUser> user = this->FindUserLocked(user_name);
    CHECK(user.valid(), "User not existing: " << user_name);
    CHECK(!user.value().is_preconfigured(), "Cannot add preconfigured user to target");

    Dedupv1dUser new_user = user.value();
    CHECK(new_user.AddTarget(target_name), "Failed to add target to user");

    user_map_[new_user.name()] = new_user;
    target_user_map_.insert(make_pair(target_name, new_user.name()));

    UserInfoData user_info;

    CHECK(new_user.SerializeTo(&user_info), "Cannot fill user info settings");

    CHECK(info_->Put(new_user.name().data(), new_user.name().size(), user_info), "Cannot store user info");
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dUserInfo::RemoveUserFromTarget(std::string user_name, std::string target_name) {
    CHECK(this->started_, "User info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Remove user from target: user " << user_name << ", target " << target_name);

    Option<Dedupv1dUser> user = this->FindUserLocked(user_name);
    CHECK(user.valid(), "User not existing: " << user_name);
    CHECK(!user.value().is_preconfigured(), "Cannot add preconfigured user to target");

    Dedupv1dUser new_user = user.value();
    CHECK(new_user.RemoveTarget(target_name), "Failed to remove user from target");

    user_map_[new_user.name()] = new_user;
    for (multimap<string, string>::iterator i = target_user_map_.find(target_name); i != target_user_map_.end(); i++) {
        if (i->second == user_name) {
            target_user_map_.erase(i);
        }
    }

    UserInfoData user_info;

    CHECK(new_user.SerializeTo(&user_info), "Cannot fill user info settings");

    CHECK(info_->Put(new_user.name().data(), new_user.name().size(), user_info), "Cannot store user info");
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dUserInfo::RemoveUser(string user_name) {
    CHECK(this->started_, "User info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Remove user: user name " << user_name);

    std::map<string, dedupv1d::Dedupv1dUser>::iterator i = this->user_map_.find(user_name);
    CHECK(i != this->user_map_.end(), "User not found: user name " << user_name);

    dedupv1d::Dedupv1dUser user = i->second;
    CHECK(!user.is_preconfigured(), "Cannot remove preconfigured user: user name " << user_name);

    CHECK(user.targets().size() == 0, "Cannot remove user that is assigned to targets: user name " << user_name);

    // now delete
    this->user_map_.erase(user_name);
    // do not access i after this

    for (list<string>::iterator j = this->user_list_.begin(); j != this->user_list_.end(); j++) {
        if (*j == user_name) {
            this->user_list_.erase(j);
            break;
        }
    }
    for (list<string>::const_iterator k = user.targets().begin(); k != user.targets().end(); k++) {
        // j is a target name
        DEBUG("Remove target entries for " << user_name << "/" << *k);
        for (multimap<string, string>::iterator l = target_user_map_.find(*k); l != target_user_map_.end(); l++) {
            DEBUG("Check entry " << l->first << "/" << l->second);
            if (*k == l->second) {
                target_user_map_.erase(l);
                break; // break inner loop
            }
        }
    }

    CHECK(info_->Delete(user_name.data(), user_name.size()), "Cannot delete user info");

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

Option<list<string> > Dedupv1dUserInfo::GetUsersInTarget(const std::string& target_name) {
    CHECK(this->started_, "User info is not started");

    TRACE("Find user in target " << target_name);

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    list<string> user_names;
    for (multimap<string, string>::iterator i = target_user_map_.find(target_name); i != target_user_map_.end(); i++) {
        user_names.push_back(i->second);
    }

    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return make_option(user_names);
}

bool Dedupv1dUserInfo::AddUser(list< pair< string, string> > options) {
    CHECK(this->started_, "User info is not started");

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Add user: " << DebugStringOptions(options));

    Dedupv1dUser new_user(false);
    CHECK(ConfigureNewUser(options, &new_user), "Cannot create new user: " << DebugStringOptions(options));

    CHECK(CheckUser(new_user), "New user is not valid: " << new_user.DebugString());
    CHECK(RegisterUser(new_user), "Failed to register user: " << new_user.DebugString());

    UserInfoData user_info;
    CHECK(new_user.SerializeTo(&user_info), "Cannot fill user info settings");

    CHECK(info_->Put(new_user.name().data(), new_user.name().size(), user_info), "Cannot store user info");
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

bool Dedupv1dUserInfo::ChangeUser(list< pair< string, string> > options) {
    CHECK(this->started_, "User info is not started");
    CHECK(options.size() >= 1, "Illegal options: " << DebugStringOptions(options));
    CHECK(options.front().first == "name", "Illegal options: " << DebugStringOptions(options));
    string user_name = options.front().second;

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    INFO("Change user: " << DebugStringOptions(options));

    Option<Dedupv1dUser> user = this->FindUserLocked(user_name);
    CHECK(user.valid(), "User not existing: " << user_name);
    CHECK(!user.value().is_preconfigured(), "Cannot add preconfigured user to target");

    Dedupv1dUser new_user = user.value();
    for (list< pair< string, string> >::const_iterator i = options.begin(); i != options.end(); i++) {
        if (i == options.begin()) {
            continue; // the first is the name
        }
        CHECK(new_user.SetOption(i->first, i->second), "Failed to update user");
    }

    user_map_[new_user.name()] = new_user;

    UserInfoData user_info;
    CHECK(new_user.SerializeTo(&user_info), "Cannot fill user info settings");

    CHECK(info_->Put(new_user.name().data(), new_user.name().size(), user_info), "Cannot store user info");
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

Option<list<string> > Dedupv1dUserInfo::GetUserNames() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLock(), "Cannot acquire lock");

    list<string> copy = user_names();
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return make_option(copy);
}

string Dedupv1dUserInfo::DebugStringOptions(const list< pair< string, string> >& options) {
    string s = "[";
    list< pair< string, string> >::const_iterator i;
    for (i = options.begin(); i != options.end(); i++) {
        if (i != options.begin()) {
            s += ", ";
        }
        if (i->first == "secret" || i->first == "secret-hash") {
            s += i->first + ":" + "******";
        } else {
            s += i->first + ":" + i->second;
        }
    }
    s += "]";
    return s;
}

#ifdef DEDUPV1D_TEST
void Dedupv1dUserInfo::ClearData() {
    if (this->info_) {
        delete info_;
        this->info_ = NULL;
    }
}
#endif

}

