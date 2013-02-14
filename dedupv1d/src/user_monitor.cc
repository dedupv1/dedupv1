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

#include "user_monitor.h"
#include <core/dedup_system.h>
#include <base/logging.h>
#include "dedupv1d.h"
#include "monitor.h"
#include "dedupv1d_user.h"
#include "dedupv1d_user_info.h"
#include "dedupv1d_target_info.h"
#include "dedupv1d_session.h"
#include <base/strutil.h>
#include <base/option.h>
#include "default_monitor.h"

#include <sstream>

using std::list;
using std::vector;
using std::string;
using std::pair;
using std::stringstream;
using dedupv1::base::strutil::To;
using dedupv1::base::Option;

LOGGER("UserMonitorAdapter");

namespace dedupv1d {
namespace monitor {

UserMonitorAdapter::UserMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

UserMonitorAdapter::~UserMonitorAdapter() {

}

MonitorAdapterRequest* UserMonitorAdapter::OpenRequest() {
    return new UserMonitorAdapterRequest(this);
}

UserMonitorAdapterRequest::UserMonitorAdapterRequest(UserMonitorAdapter* adapter) {
    this->adapter_ = adapter;
}

UserMonitorAdapterRequest::~UserMonitorAdapterRequest() {
}

bool UserMonitorAdapterRequest::ParseParam(const string& key, const string& value) {
    if (key == "op") {
        this->operation_ = value;
    } else {
        // We care about the ordering in the user monitor, but strangely MHD processes
        // the options in the reserved direction. So we have to insert the option at the
        // beginning of the list to have to original ordering
        this->options_.push_front( pair<string, string>(key, value));
    }
    return true;
}

string UserMonitorAdapterRequest::WriteUser(const Dedupv1dUser& user) {
    stringstream sstr;
    sstr << "\"" << user.name() << "\": {";
    sstr << "\"secret hash\": \"" << user.secret_hash() << "\"," << std::endl;
    sstr << "\"targets\": [";
    for (list<string>::const_iterator i = user.targets().begin(); i != user.targets().end(); i++) {
        if (i != user.targets().begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" << *i << "\"";
    }
    sstr << "]";

    sstr << "}";
    return sstr.str();
}

string UserMonitorAdapterRequest::Monitor() {
    dedupv1d::Dedupv1d* ds = adapter_->ds_;
    CHECK_RETURN_JSON(ds, "ds not set");
    Dedupv1dUserInfo* user_info = ds->user_info();
    CHECK_RETURN_JSON(user_info, "User info not set");
    Dedupv1dTargetInfo* target_info = ds->target_info();
    CHECK_RETURN_JSON(target_info, "Target info not set");

    stringstream sstr;
    sstr << "{";

    if (options_.size() > 0 && ds->state() != Dedupv1d::RUNNING) {
        WARNING("User change request in illegal state: " << ds->state());
        sstr << "\"ERROR\": \"Illegal dedupv1d state\"";
    } else if (options_.size() > 0 && ds->state() == Dedupv1d::RUNNING) {
        if (this->operation_ == "") {
            WARNING("Operation not set: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            sstr << "\"ERROR\": \"Operation not set\"" << std::endl;
        } else if (this->operation_ == "add") {
            DEBUG("Perform add: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));

            CHECK_RETURN_JSON(this->options_.size() >= 1, "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            CHECK_RETURN_JSON(this->options_.front().first == "name", "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            string user_name = this->options_.front().second;

            // Check the targets
            for (list< pair< string, string> >::const_iterator i = options_.begin();
                 i != options_.end(); i++) {
                if (i->first == "target") {
                    if (!target_info->FindTargetByName(i->second).valid()) {
                        WARNING("Target not existing: " << i->second);
                        WARNING("Cannot create user: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
                        sstr << "\"ERROR\": \"Cannot create user\"" << std::endl;
                    }
                }
            }

            Option<Dedupv1dUser> user = user_info->FindUser(user_name);
            if (user.valid()) {
                sstr << "\"ERROR\": \"User already existing\"" << std::endl;
            } else {
                if (!user_info->AddUser(this->options_)) {
                    WARNING("Cannot create user: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
                    sstr << "\"ERROR\": \"Cannot create user\"" << std::endl;
                } else {
                    Option<Dedupv1dUser> user = user_info->FindUser(user_name);
                    if (!user.valid()) {
                        sstr << "\"ERROR\": \"User not created\"" << std::endl;
                    }

                    sstr << WriteUser(user.value());
                }
            }
        } else if (this->operation_ == "change") {
            DEBUG("Perform change: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));

            CHECK_RETURN_JSON(this->options_.size() >= 1, "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            CHECK_RETURN_JSON(this->options_.front().first == "name", "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            string user_name = this->options_.front().second;

            Option<Dedupv1dUser> user = user_info->FindUser(user_name);
            if (!user.valid()) {
                sstr << "\"ERROR\": \"User not existing\"" << std::endl;
            } else {
                if (!user_info->ChangeUser(this->options_)) {
                    WARNING("Cannot change user: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
                    sstr << "\"ERROR\": \"Cannot change user\"" << std::endl;
                }
            }
        } else if (this->operation_ == "addtotarget") {
            DEBUG("Perform addtotarget: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));

            CHECK_RETURN_JSON(this->options_.size() == 2, "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            CHECK_RETURN_JSON(this->options_.front().first == "name", "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            CHECK_RETURN_JSON(this->options_.back().first == "target", "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            string user_name = this->options_.front().second;
            string target_name = this->options_.back().second;

            Option<Dedupv1dTarget> target = target_info->FindTargetByName(target_name);
            CHECK_RETURN_JSON(target.valid(), "Target not existing");

            Option<Dedupv1dUser> user = user_info->FindUser(user_name);
            if (!user.valid()) {
                sstr << "\"ERROR\": \"User not existing\"" << std::endl;
            } else {
                if (!user_info->AddUserToTarget(user_name, target_name)) {
                    WARNING("Cannot add user to target: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
                    sstr << "\"ERROR\": \"Cannot add user to target\"" << std::endl;
                }
            }
        } else if (this->operation_ == "rmfromtarget") {
            DEBUG("Perform rmfromtarget: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));

            CHECK_RETURN_JSON(this->options_.size() == 2, "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            CHECK_RETURN_JSON(this->options_.front().first == "name", "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            CHECK_RETURN_JSON(this->options_.back().first == "target", "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            string user_name = this->options_.front().second;
            string target_name = this->options_.back().second;

            Option<Dedupv1dTarget> target = target_info->FindTargetByName(target_name);
            CHECK_RETURN_JSON(target.valid(), "Target not existing");

            Option<Dedupv1dUser> user = user_info->FindUser(user_name);
            if (!user.valid()) {
                sstr << "\"ERROR\": \"User not existing\"" << std::endl;
            } else {
                if (!user_info->RemoveUserFromTarget(user_name, target_name)) {
                    WARNING("Cannot remove user from target: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
                    sstr << "\"ERROR\": \"Cannot remove user from target\"" << std::endl;
                }
            }
        } else if (this->operation_ == "remove") {
            DEBUG("Perform remove: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));

            CHECK_RETURN_JSON(this->options_.size() == 1, "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            CHECK_RETURN_JSON(this->options_.front().first == "name", "Illegal options: " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            string user_name = this->options_.front().second;

            Option<Dedupv1dUser> user = user_info->FindUser(user_name);
            if (!user.valid()) {
                WARNING("Cannot find user " << user_name);
                sstr << "\"ERROR\": \"Cannot find user " << user_name << "\"" << std::endl;
            } else {
                if (!user_info->RemoveUser(user_name)) {
                    WARNING("Cannot remove user " << user_name);
                    sstr << "\"ERROR\": \"Cannot detach user " << user_name << "\"" << std::endl;
                }
            }
        } else {
            WARNING("Illegal operation " << this->operation_ << ": " << Dedupv1dUserInfo::DebugStringOptions(this->options_));
            sstr << "\"ERROR\": \"Illegal operation\"" << std::endl;
        }
        options_.clear();
    } else {
        // no options: Give all users
        Option<list<string> > user_names = user_info->GetUserNames();
        CHECK_RETURN_JSON(user_names.valid(), "Failed to get user names");

        bool first = true;
        list<string>::const_iterator i;
        for (i = user_names.value().begin(); i != user_names.value().end(); i++) {
            Option<Dedupv1dUser> user = user_info->FindUser(*i);
            if (user.valid()) {
                if (first) {
                    first = false;
                } else {
                    sstr << "," << std::endl;
                }
                sstr << WriteUser(user.value());
            }
        }
    }
    sstr << "}";
    return sstr.str();
}

}
}
