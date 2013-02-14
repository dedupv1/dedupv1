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

#include "group_monitor.h"
#include <core/dedup_system.h>
#include <base/logging.h>
#include "dedupv1d.h"
#include "monitor.h"
#include "dedupv1d_group.h"
#include "dedupv1d_group_info.h"
#include "dedupv1d_volume.h"
#include "dedupv1d_volume_info.h"
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
using dedupv1::base::Option;

LOGGER("GroupMonitorAdapter");

namespace dedupv1d {
namespace monitor {

GroupMonitorAdapter::GroupMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds = ds;
}

GroupMonitorAdapter::~GroupMonitorAdapter() {

}

MonitorAdapterRequest* GroupMonitorAdapter::OpenRequest() {
    return new GroupMonitorAdapterRequest(this);
}

GroupMonitorAdapterRequest::GroupMonitorAdapterRequest(GroupMonitorAdapter* adapter) {
    this->adapter = adapter;
}

GroupMonitorAdapterRequest::~GroupMonitorAdapterRequest() {

}

bool GroupMonitorAdapterRequest::ParseParam(const string& key, const string& value) {
    if (key == "op") {
        this->operation = value;
        DEBUG("Found operation param: " << value);
    } else {
        // We care about the ordering in the group monitor, but strangely MHD processes
        // the options in the reserved direction. So we have to insert the option at the
        // beginning of the list to have to original ordering
        this->options.push_front( make_pair(key, value));
        DEBUG("Found option param: " << key << "=" << value);
    }
    return true;
}

string GroupMonitorAdapterRequest::WriteGroup(const Dedupv1dGroup& group) {
    dedupv1d::Dedupv1d* ds = adapter->ds;
    Dedupv1dVolumeInfo* volume_info = ds->volume_info();

    stringstream sstr;
    sstr << "\"" << group.name() << "\": {";
    sstr << "\"initiators\":";
    sstr << "[";
    for (list<string>::const_iterator i = group.initiator_pattern().begin(); i != group.initiator_pattern().end(); i++) {
        if (i != group.initiator_pattern().begin()) {
            sstr << ", " << std::endl;
        }
        sstr << "\"" << *i << "\"";
    }
    sstr << "]";

    sstr << ", " << std::endl;
    sstr << "\"volumes\":";
    if (volume_info) {
        Option<list<pair<uint32_t, uint64_t> > > volume_list = volume_info->FindVolumesInGroup(group.name());
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
    } else {
        sstr << "null";
    }

    sstr << "}";
    return sstr.str();
}

string GroupMonitorAdapterRequest::Monitor() {
    dedupv1d::Dedupv1d* ds = adapter->ds;
    CHECK_RETURN_JSON(ds, "dedupv1d not set");

    Dedupv1dGroupInfo* group_info = ds->group_info();
    CHECK_RETURN_JSON(group_info, "Group info not set");
    Dedupv1dVolumeInfo* volume_info = ds->volume_info();
    CHECK_RETURN_JSON(volume_info, "Volume info not set");

    stringstream sstr;
    sstr << "{";

    if (options.size() > 0 && ds->state() != Dedupv1d::RUNNING) {
        WARNING("Group change request in illegal state: " << ds->state());
        sstr << "\"ERROR\": \"Illegal dedupv1d state\"";
    } else if (options.size() > 0 && ds->state() == Dedupv1d::RUNNING) {
        if (this->operation == "") {
            WARNING("Operation not set: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            sstr << "\"ERROR\": \"Operation not set\"" << std::endl;
        } else if (this->operation == "add") {
            DEBUG("Perform add: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            Dedupv1dGroupInfo* group_info = ds->group_info();
            CHECK_RETURN(group_info, "{\"ERROR\": \"Group info not set\"}", "Group info not set");

            CHECK_RETURN_JSON(this->options.size() >= 1, "Illegal options: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "name", "Illegal options: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            string group_name = this->options.front().second;

            Option<Dedupv1dGroup> group = group_info->FindGroup(group_name);
            if (group.valid()) {
                sstr << "\"ERROR\": \"Group already existing\"" << std::endl;
            } else {
                if (!group_info->AddGroup(this->options)) {
                    WARNING("Cannot create group: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
                    sstr << "\"ERROR\": \"Cannot create group\"" << std::endl;
                } else {
                    Option<Dedupv1dGroup> group = group_info->FindGroup(group_name);
                    if (!group.valid()) {
                        sstr << "\"ERROR\": \"Group not created\"" << std::endl;
                    }
                    sstr << WriteGroup(group.value());
                }
            }
        } else if (this->operation == "remove") {
            DEBUG("Perform remove: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            Dedupv1dGroupInfo* group_info = ds->group_info();
            CHECK_RETURN_JSON(group_info, "Group info not set");

            CHECK_RETURN_JSON(this->options.size() == 1, "Illegal options: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "name", "Illegal options: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            string group_name = this->options.front().second;

            Option<Dedupv1dGroup> group = group_info->FindGroup(group_name);
            if (!group.valid()) {
                WARNING("Cannot find group " << group_name);
                sstr << "\"ERROR\": \"Cannot find group " << group_name << "\"" << std::endl;
            } else {
                if (!group_info->RemoveGroup(group_name)) {
                    WARNING("Cannot remove group " << group_name);
                    sstr << "\"ERROR\": \"Cannot detach group " << group_name << "\"" << std::endl;
                }
            }
        } else if (this->operation == "addinitiator") {
            DEBUG("Perform addinitiator: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(this->options.size() == 2, "Illegal options: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "name", "Illegal options: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.back().first == "initiator", "Illegal options: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            string group_name = this->options.front().second;
            string initiator_pattern = this->options.back().second;

            Option<Dedupv1dGroup> group = group_info->FindGroup(group_name);
            if (!group.valid()) {
                sstr << "\"ERROR\": \"Group not existing\"" << std::endl;
            } else {
                if (!group_info->AddInitiatorPattern(group_name, initiator_pattern)) {
                    WARNING("Cannot add initiator pattern: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
                    sstr << "\"ERROR\": \"Cannot add initiator pattern\"" << std::endl;
                }
            }
        } else if (this->operation == "rminitiator") {
            DEBUG("Perform rminitiator: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(this->options.size() == 2, "Illegal options: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "name", "Illegal options: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.back().first == "initiator", "Illegal options: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            string group_name = this->options.front().second;
            string initiator_pattern = this->options.back().second;

            Option<Dedupv1dGroup> group = group_info->FindGroup(group_name);
            if (!group.valid()) {
                sstr << "\"ERROR\": \"Group not existing\"" << std::endl;
            } else {
                if (!group_info->RemoveInitiatorPattern(group_name, initiator_pattern)) {
                    WARNING("Cannot remove initiator pattern: " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
                    sstr << "\"ERROR\": \"Cannot remove initiator pattern\"" << std::endl;
                }
            }
        } else {
            WARNING("Illegal operation " << this->operation << ": " << Dedupv1dGroupInfo::DebugStringOptions(this->options));
            sstr << "\"ERROR\": \"Illegal operation\"" << std::endl;
        }
        options.clear();
    } else {
        // no options: Give all groups
        Option<list<string> > group_names = ds->group_info()->GetGroupNames();
        CHECK_RETURN_JSON(group_names.valid(), "Failed to get group names");

        bool first = true;
        list<string>::const_iterator i;
        for (i = group_names.value().begin(); i != group_names.value().end(); i++) {
            Option<Dedupv1dGroup> group = ds->group_info()->FindGroup(*i);
            if (group.valid()) {
                if (first) {
                    first = false;
                } else {
                    sstr << "," << std::endl;
                }
                sstr << WriteGroup(group.value());
            }
        }

    }
    sstr << "}";
    return sstr.str();
}

}
}
