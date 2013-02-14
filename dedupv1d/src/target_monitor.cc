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

#include "target_monitor.h"
#include <core/dedup_system.h>
#include <base/logging.h>
#include "dedupv1d.h"
#include "monitor.h"
#include "dedupv1d_target.h"
#include "dedupv1d_target_info.h"
#include "dedupv1d_volume_info.h"
#include "dedupv1d_volume.h"
#include "dedupv1d_user_info.h"
#include "dedupv1d_session.h"
#include <base/strutil.h>
#include <base/option.h>
#include "default_monitor.h"

#include <sstream>

using std::list;
using std::vector;
using std::string;
using std::pair;
using std::make_pair;
using std::stringstream;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::Option;

LOGGER("TargetMonitorAdapter");

namespace dedupv1d {
namespace monitor {

TargetMonitorAdapter::TargetMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

TargetMonitorAdapter::~TargetMonitorAdapter() {

}

MonitorAdapterRequest* TargetMonitorAdapter::OpenRequest() {
    return new TargetMonitorAdapterRequest(this);
}

TargetMonitorAdapterRequest::TargetMonitorAdapterRequest(TargetMonitorAdapter* adapter) {
    this->adapter = adapter;
}

TargetMonitorAdapterRequest::~TargetMonitorAdapterRequest() {

}

bool TargetMonitorAdapterRequest::ParseParam(const string& key, const string& value) {
    if (key == "op") {
        this->operation = value;
        DEBUG("Found operation param: " << value);
    } else {
        // We care about the ordering in the volume monitor, but strangely MHD processes
        // the options in the reserved direction. So we have to insert the option at the
        // beginning of the list to have to original ordering
        this->options.push_front(std::make_pair(key, value));
        DEBUG("Found option param: " << key << "=" << value);
    }
    return true;
}

string TargetMonitorAdapterRequest::WriteTarget(const Dedupv1dTarget& target) {
    dedupv1d::Dedupv1d* ds = adapter->ds_;
    CHECK_RETURN_JSON(ds, "dedupv1d not set");
    Dedupv1dUserInfo* user_info = ds->user_info();
    CHECK_RETURN_JSON(user_info, "user info not set");
    Dedupv1dVolumeInfo* volume_info = ds->volume_info();
    CHECK_RETURN_JSON(volume_info, "Volume info not set");

    DEBUG("Write JSON info about target: " << target.DebugString());

    stringstream sstr;
    sstr << "\"" << target.tid() << "\": {";
    sstr << "\"name\": \"" << target.name() << "\"," << std::endl;

    TRACE("Get user info about target: " << target.DebugString());

    sstr << "\"users\":";
    Option<list<string > > user_list;
    if (user_info->is_started()) {
        user_list = user_info->GetUsersInTarget(target.name());
    }
    if (user_list.valid()) {
        sstr << "[";
        for (list<string >::const_iterator i = user_list.value().begin(); i != user_list.value().end(); i++) {
            if (i != user_list.value().begin()) {
                sstr << ", " << std::endl;
            }
            Option<Dedupv1dUser> user = user_info->FindUser(*i);
            if (user.valid()) {
                sstr << "\"" << user.value().name() << "\"";
            } else {
                sstr << "null";
            }

        }
        sstr << "]";
    } else {
        sstr << "null";
    }
    sstr << "," << std::endl;

    TRACE("Get volume info about target: " << target.DebugString());
    sstr << "\"volumes\":";
    Option<list<pair<uint32_t, uint64_t> > > volume_list = volume_info->FindVolumesInTarget(target.name());
    if (volume_list.valid()) {
        sstr << "[";
        for (list<pair<uint32_t, uint64_t> >::const_iterator i = volume_list.value().begin(); i != volume_list.value().end(); i++) {
            if (i != volume_list.value().begin()) {
                sstr << ", " << std::endl;
            }
            uint32_t volume_id = i->first;
            uint64_t lun = i->second;

            dedupv1::base::MutexLock* lock = NULL;
            Dedupv1dVolume* volume = volume_info->FindVolume(volume_id, &lock);
            if (volume) {
                sstr << "\"" << volume->device_name() << ":" << lun << "\"";
                lock->ReleaseLock();
                lock = NULL;
            } else {
                sstr << "null";
            }

        }
        sstr << "]";
    } else {
        sstr << "null";
    }
    sstr << "," << std::endl;

    TRACE("Get params info about target: " << target.DebugString());
    sstr << "\"params\":";
    sstr << "[";
    for (list<pair<string, string> >::const_iterator i = target.params().begin(); i != target.params().end(); i++) {
        if (i != target.params().begin()) {
            sstr << ", " << std::endl;
        }
        sstr << "\"" << i->first << "=" << i->second << "\"";
    }
    sstr << "]";

    if (!target.auth_username().empty() or !target.auth_secret_hash().empty()) {
        sstr << ",";

        sstr << "\"auth\": { ";
        sstr << "\"name\": \"" << target.auth_username() << "\",";
        sstr << "\"secret\": \"" << target.auth_secret_hash() << "\"";
        sstr << "}";
    }

    sstr << "}";
    return sstr.str();
}

string TargetMonitorAdapterRequest::Monitor() {
    dedupv1d::Dedupv1d* ds = adapter->ds_;
    CHECK_RETURN_JSON(ds, "Daemon not set");

    stringstream sstr;
    sstr << "{";

    if (options.size() > 0 && ds->state() != Dedupv1d::RUNNING) {
        WARNING("Target change request in illegal state: " << ds->state());
        sstr << "\"ERROR\": \"Illegal dedupv1d state\"";
    } else if (options.size() > 0 && ds->state() == Dedupv1d::RUNNING) {
        if (this->operation == "") {
            WARNING("Operation not set: " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
            sstr << "\"ERROR\": \"Operation not set\"" << std::endl;
        } else if (this->operation == "add") {
            DEBUG("Perform add: " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
            Dedupv1dTargetInfo* target_info = ds->target_info();
            CHECK_RETURN_JSON(target_info, "Target info not set");

            CHECK_RETURN_JSON(this->options.size() >= 2, "Illegal options: " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "tid", "Illegal options: " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(To<uint32_t>(this->options.front().second).valid(), "Illegal option: " << this->options.front().second);
            uint32_t tid = To<uint32_t>(this->options.front().second).value();

            Option<Dedupv1dTarget> target = target_info->FindTarget(tid);
            if (target.valid()) {
                sstr << "\"ERROR\": \"Target already existing\"" << std::endl;
            } else {
                if (!target_info->AddTarget(this->options)) {
                    WARNING("Cannot create target: " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
                    sstr << "\"ERROR\": \"Cannot create target\"" << std::endl;
                } else {
                    Option<Dedupv1dTarget> target = target_info->FindTarget(tid);
                    if (!target.valid()) {
                        sstr << "\"ERROR\": \"Target not created\"" << std::endl;
                    }
                    sstr << WriteTarget(target.value());
                }
            }
        } else if (this->operation == "remove") {
            DEBUG("Perform remove: " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
            Dedupv1dTargetInfo* target_info = ds->target_info();
            CHECK_RETURN_JSON(target_info, "Target info not set");

            CHECK_RETURN_JSON(this->options.size() == 1, "Illegal options: " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "tid", "Illegal options: " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(To<uint32_t>(this->options.front().second).valid(), "Illegal option: " << this->options.front().second);
            uint32_t tid = To<uint32_t>(this->options.front().second).value();

            Option<Dedupv1dTarget> target = target_info->FindTarget(tid);
            if (!target.valid()) {
                WARNING("Cannot find target " << tid);
                sstr << "\"ERROR\": \"Cannot find target " << tid << "\"" << std::endl;
            } else {
                if (!target_info->RemoveTarget(tid)) {
                    WARNING("Cannot remove target " << tid);
                    sstr << "\"ERROR\": \"Cannot detach target " << tid << "\"" << std::endl;
                }
            }
        } else if (this->operation == "change-param") {
            DEBUG("Perform change-param: " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
            Dedupv1dTargetInfo* target_info = ds->target_info();
            CHECK_RETURN_JSON(target_info, "Target info not set");

            CHECK_RETURN_JSON(this->options.size() > 1, "Illegal options: " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "tid", "Illegal options: " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
            uint32_t tid = To<uint32_t>(this->options.front().second).value();

            list< pair<string, string> > param_options;
            list< pair<string, string> >::iterator i = ++options.begin(); // on second entry
            for (; i != options.end(); i++) {
                bool is_param_option = StartsWith(i->first, "param.") ||
                        StartsWith(i->first, "auth.") ||
                        i->first == "name";
                CHECK_RETURN_JSON(is_param_option, "Illegal option: " << i->first);
                param_options.push_back(make_pair(i->first, i->second));
            }
            Option<Dedupv1dTarget> target = target_info->FindTarget(tid);
            if (!target.valid()) {
                WARNING("Cannot find target " << tid);
                sstr << "\"ERROR\": \"Cannot find target " << tid << "\"" << std::endl;
            } else {
                if (!target_info->ChangeTargetParams(tid, param_options)) {
                    WARNING("Cannot change target params: target " << tid);
                    sstr << "\"ERROR\": \"Cannot change target params: target " << tid << "\"" << std::endl;
                }
            }
        } else {
            WARNING("Illegal operation " << this->operation << ": " << Dedupv1dTargetInfo::DebugStringOptions(this->options));
            sstr << "\"ERROR\": \"Illegal operation\"" << std::endl;
        }
        options.clear();
    } else {
        // no options: Give all targets
        CHECK_RETURN_JSON(ds->target_info(), "Target info not set");

        Option<list<Dedupv1dTarget> > targets = ds->target_info()->GetTargets();
        CHECK_RETURN_JSON(targets.valid(), "Failed to get targets");

        list<Dedupv1dTarget>::const_iterator i;
        for (i = targets.value().begin(); i != targets.value().end(); i++) {
            if (i != targets.value().begin()) {
                sstr << "," << std::endl;
            }
            sstr << WriteTarget(*i);
        }

    }
    sstr << "}";
    return sstr.str();
}

}
}
