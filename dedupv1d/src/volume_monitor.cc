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

#include "volume_monitor.h"
#include <core/dedup_system.h>
#include <base/logging.h>
#include "dedupv1d.h"
#include "monitor.h"
#include "scst_handle.h"
#include "dedupv1d_volume.h"
#include "dedupv1d_volume_info.h"
#include "dedupv1d_session.h"
#include <base/strutil.h>
#include "default_monitor.h"
#include <base/option.h>

#include "dedupv1d.pb.h"

#include <sstream>

using std::make_pair;
using std::list;
using std::set;
using std::vector;
using std::string;
using std::pair;
using std::stringstream;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::Option;

LOGGER("VolumeMonitorAdapter");

namespace dedupv1d {
namespace monitor {

VolumeMonitorAdapter::VolumeMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

VolumeMonitorAdapter::~VolumeMonitorAdapter() {

}

MonitorAdapterRequest* VolumeMonitorAdapter::OpenRequest() {
    return new VolumeMonitorAdapterRequest(this);
}

VolumeMonitorAdapterRequest::VolumeMonitorAdapterRequest(VolumeMonitorAdapter* adapter) {
    this->adapter = adapter;
}

VolumeMonitorAdapterRequest::~VolumeMonitorAdapterRequest() {

}

bool VolumeMonitorAdapterRequest::ParseParam(const string& key, const string& value) {
    if (key == "op") {
        this->operation = value;
        DEBUG("Found operation param: " << value);
    } else {
        // We care about the ordering in the volume monitor, but strangely MHD processes
        // the options in the reserved direction. So we have to insert the option at the
        // beginning of the list to have to original ordering
        this->options.push_front( make_pair(key, value));
        DEBUG("Found option param: " << key << "=" << value);
    }
    return true;
}

string VolumeMonitorAdapterRequest::WriteVolume(Dedupv1dVolumeInfo* info, Dedupv1dVolume* volume) {
    CHECK_RETURN(volume, "null", "Volume not set");

    DEBUG("Write volume " << volume->DebugString());

    stringstream sstr;
    sstr << "\"" << volume->id() << "\": {";
    sstr << "\"name\": \"" << volume->device_name() << "\"," << std::endl;
    sstr << "\"sector size\": " << volume->block_size() << "," << std::endl;
    sstr << "\"logical size\": " << volume->logical_size() << "," << std::endl;
    sstr << "\"unique serial number\": \"" << volume->unique_serial_number() << "\"," << std::endl;

    uint64_t start_block_id = 0;
    uint64_t end_block_id = 0;
    if (volume->volume()->GetBlockInterval(&start_block_id, &end_block_id)) {
        sstr << "\"blocks\": [" << start_block_id << "," << end_block_id << "], " << std::endl;
    } else {
        sstr << "\"blocks\": null," << std::endl;
    }

    sstr << "\"groups\": [";
    vector< pair<string, uint64_t> >::const_iterator i;
    for (i = volume->groups().begin(); i != volume->groups().end(); i++) {
        if (i != volume->groups().begin()) {
            sstr << ",";
        }
        string group = i->first;
        uint16_t lun = i->second;
        sstr << "{";
        sstr << "\"name\": \"" << group << "\"," << std::endl;
        sstr << "\"lun\": \"" << lun << "\"" << std::endl;
        sstr << "}";
    }
    sstr << "],";

    sstr << "\"targets\": [";
    for (i = volume->targets().begin(); i != volume->targets().end(); i++) {
        if (i != volume->targets().begin()) {
            sstr << ",";
        }
        string target = i->first;
        uint16_t lun = i->second;
        sstr << "{";
        sstr << "\"name\": \"" << target << "\"," << std::endl;
        sstr << "\"lun\": \"" << lun << "\"" << std::endl;
        sstr << "}";
    }
    sstr << "],";

    sstr << "\"sessions\": " << volume->session_count() << "," << std::endl;
    if (volume->state() == Dedupv1dVolume::DEDUPV1D_VOLUME_STATE_FAILED) {
        sstr << "\"state\": \"failure\"" << std::endl;
    } else {
        if (volume->maintenance_mode()) {
            sstr << "\"state\": \"maintenance\"" << std::endl;
        } else {
            sstr << "\"state\": \"running\"" << std::endl;
        }
    }
    sstr << ",";
    sstr << "\"filter\": [";
    {
        set<string>::const_iterator i;
        for (i = volume->volume()->enabled_filter_names().begin();
             i != volume->volume()->enabled_filter_names().end();
             ++i) {
            if (i != volume->volume()->enabled_filter_names().begin()) {
                sstr << ",";
            }
            sstr << "\"" << *i << "\"";
        }
    }
    sstr << "],";
    sstr << "\"chunking\":";
    if (!volume->volume()->chunking_config().empty()) {
        sstr << "{";
        list<pair<string, string> >::const_iterator j;
        for (j = volume->volume()->chunking_config().begin();
             j != volume->volume()->chunking_config().end();
             ++j) {
            if (j != volume->volume()->chunking_config().begin()) {
                sstr << ",";
            }
            sstr << "\"" << j->first << "\": \"" << j->second << "\"";
        }
        sstr << "}";
    } else {
        sstr << "null";
    }
    sstr << ",";
    sstr << "\"fast copy\": [";
    Option<VolumeFastCopyJobData> fast_copy_job = info->fast_copy()->GetFastCopyJob(volume->id());
    if (fast_copy_job.valid()) {
        sstr << "{";
        sstr << "\"source id\": \"" << fast_copy_job.value().src_volume_id() << "\"," << std::endl;
        sstr << "\"source start offset\": " << fast_copy_job.value().src_start_offset() << "," << std::endl;
        sstr << "\"target start offset\": " << fast_copy_job.value().target_start_offset() << "," << std::endl;
        if (fast_copy_job.value().job_failed()) {
            sstr << "\"state\": \"failed\"," << std::endl;
        } else {
            sstr << "\"state\": \"running\"," << std::endl;
        }
        sstr << "\"size\": " << fast_copy_job.value().size() << "," << std::endl;
        sstr << "\"current\": " << fast_copy_job.value().current_offset() << std::endl;

        sstr << "}";
    }
    sstr << "]";

    sstr << "}";
    return sstr.str();
}

string VolumeMonitorAdapterRequest::WriteAllVolumes(dedupv1d::Dedupv1d* ds) {
    stringstream sstr;
    if (ds->volume_info() == NULL) {
        WARNING("Volume info not set");
        sstr << "\"ERROR\": \"Volume info not set\"" << std::endl;
    }  else{
        dedupv1::base::MutexLock* lock = NULL;
        Option<list<Dedupv1dVolume*> > volumes = ds->volume_info()->GetVolumes(&lock);
        CHECK_RETURN_JSON(volumes.valid(), "Failed to get volumes");

        list<Dedupv1dVolume*>::const_iterator i;
        for (i = volumes.value().begin(); i != volumes.value().end(); i++) {
            if (i != volumes.value().begin()) {
                sstr << "," << std::endl;
            }
            Dedupv1dVolume* volume = *i;
            sstr << WriteVolume(ds->volume_info(), volume);
        }

        // it is save to make this inside the lock
        Option<list<uint32_t> > detaching_list = ds->volume_info()->detacher()->GetDetachingVolumeList();
        if (!detaching_list.valid()) {
            ERROR("Failed to gather detaching volume list");
        }   else{
            list<uint32_t>::const_iterator j;
            for (j = detaching_list.value().begin(); j != detaching_list.value().end(); j++) {
                if (j != detaching_list.value().begin() || volumes.value().size() > 0) {
                    sstr << ",";
                }
                sstr << "\"" << *j << "\": null";
            }
        }
        lock->ReleaseLock();
    }
    return sstr.str();
}

string VolumeMonitorAdapterRequest::Monitor() {
    dedupv1d::Dedupv1d* ds = adapter->ds_;
    CHECK_RETURN_JSON(ds, "Volume info not set");
    Dedupv1dVolumeInfo* volume_info = ds->volume_info();
    CHECK_RETURN_JSON(volume_info, "Volume info not set");
    stringstream sstr;
    sstr << "{";

    if (options.size() > 0 && ds->state() != Dedupv1d::RUNNING) {
        WARNING("Volume change request in illegal state: " << ds->state());
        sstr << "\"ERROR\": \"Illegal dedupv1d state\"";
    } else if (options.size() > 0 && ds->state() == Dedupv1d::RUNNING) {
        if (this->operation == "") {
            WARNING("Operation not set: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            sstr << "\"ERROR\": \"Operation not set\"" << std::endl;
        } else if (this->operation == "attach") {
            DEBUG("Perform attachment: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            Dedupv1dVolume* volume = volume_info->AttachVolume(this->options);
            if (!volume) {
                WARNING("Cannot create volume: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
                sstr << "\"ERROR\": \"Cannot create volume\"" << std::endl;
            } else {
                sstr << WriteVolume(volume_info, volume);
            }
        } else if (this->operation == "detach") {
            DEBUG("Perform detachment: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(this->options.size() == 1, "Illegal options");
            CHECK_RETURN_JSON(this->options.front().first == "id", "Illegal options");
            CHECK_RETURN_JSON(To<uint32_t>(this->options.front().second).valid(), "Illegal option: " << this->options.front().second);
            uint32_t id = To<uint32_t>(this->options.front().second).value();

            dedupv1::base::MutexLock* lock = NULL;
            Dedupv1dVolume* volume = volume_info->FindVolume(id, &lock);
            if (!volume) {
                WARNING("Cannot find volume " << id);
                sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
            } else {
                lock->ReleaseLock();
                lock = NULL;
                volume = NULL; // pointer is invalid after detachment

                if (!volume_info->DetachVolume(id)) {
                    WARNING("Cannot detach volume " << id);
                    sstr << "\"ERROR\": \"Cannot detach volume " << id << "\"" << std::endl;
                }
            }
        } else if (this->operation == "addtogroup") {
            DEBUG("Perform addtogroup operations: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(this->options.size() == 2, "Illegal options");
            CHECK_RETURN_JSON(this->options.front().first == "id", "Illegal options");
            CHECK_RETURN_JSON(this->options.back().first == "group", "Illegal options");
            CHECK_RETURN_JSON(To<uint32_t>(this->options.front().second).valid(), "Illegal option: " << this->options.front().second);
            uint32_t id = To<uint32_t>(this->options.front().second).value();
            string group = this->options.back().second;

            dedupv1::base::MutexLock* lock = NULL;
            Dedupv1dVolume* volume = volume_info->FindVolume(id, &lock);
            if (!volume) {
                WARNING("Cannot find volume " << id);
                sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
            } else {
                lock->ReleaseLock();
                lock = NULL;
                volume = NULL;

                if (!volume_info->AddToGroup(id, group)) {
                    WARNING("Cannot add group: volume " << id << ", group " << group);
                    sstr << "\"ERROR\": \"Cannot add group: volume " << id << ", group " << group << "\"" << std::endl;
                } else {
                    volume = volume_info->FindVolume(id, &lock);
                    if (!volume) {
                        WARNING("Cannot find volume " << id);
                        sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
                    } else {
                        sstr << WriteVolume(volume_info, volume);
                        lock->ReleaseLock();
                    }
                }
            }
        } else if (this->operation == "rmfromgroup") {
            DEBUG("Perform rmfromgroup operation: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(this->options.size() == 2,
                "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "id",
                "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.back().first == "group",
                "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(To<uint32_t>(this->options.front().second).valid(), "Illegal option: " << this->options.front().second);
            uint32_t id = To<uint32_t>(this->options.front().second).value();
            string group = this->options.back().second;

            dedupv1::base::MutexLock* lock = NULL;
            Dedupv1dVolume* volume = volume_info->FindVolume(id, &lock);
            if (!volume) {
                WARNING("Cannot find volume " << id);
                sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
            } else {
                lock->ReleaseLock();
                lock = NULL;
                volume = NULL;

                bool b = volume_info->RemoveFromGroup(id, group);
                DEBUG("Finished rmfromgroup operation: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
                if (!b) {
                    WARNING("Cannot remove group: volume " << id << ", group " << group);
                    sstr << "\"ERROR\": \"Cannot remove group: volume " << id << ", group " << group << "\"" << std::endl;
                } else {
                    volume = volume_info->FindVolume(id, &lock);
                    if (!volume) {
                        WARNING("Cannot find volume " << id);
                        sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
                    } else {
                        sstr << WriteVolume(volume_info, volume);
                        lock->ReleaseLock();
                    }
                }
            }
        } else if (this->operation == "addtotarget") {
            DEBUG("Perform addtotarget operations: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(this->options.size() == 2, "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "id", "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.back().first == "target", "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(To<uint32_t>(this->options.front().second).valid(), "Illegal option: " << this->options.front().second);
            uint32_t id = To<uint32_t>(this->options.front().second).value();
            string target = this->options.back().second;

            dedupv1::base::MutexLock* lock = NULL;
            Dedupv1dVolume* volume = volume_info->FindVolume(id, &lock);
            if (!volume) {
                WARNING("Cannot find volume " << id);
                sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
            } else {
                lock->ReleaseLock();
                lock = NULL;
                volume = NULL;

                if (!volume_info->AddToTarget(id, target)) {
                    WARNING("Cannot add target: volume " << id << ", target " << target);
                    sstr << "\"ERROR\": \"Cannot add target: volume " << id << ", target " << target << "\"" << std::endl;
                } else {
                    volume = volume_info->FindVolume(id, &lock);
                    if (!volume) {
                        WARNING("Cannot find volume " << id);
                        sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
                    } else {
                        sstr << WriteVolume(volume_info, volume);
                        lock->ReleaseLock();
                    }
                }
            }
        } else if (this->operation == "rmfromtarget") {
            DEBUG("Perform rmfromtarget operations: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(this->options.size() == 2, "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "id", "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.back().first == "target", "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(To<uint32_t>(this->options.front().second).valid(), "Illegal option: " << this->options.front().second);
            uint32_t id = To<uint32_t>(this->options.front().second).value();
            string target = this->options.back().second;

            dedupv1::base::MutexLock* lock = NULL;
            Dedupv1dVolume* volume = volume_info->FindVolume(id, &lock);
            if (!volume) {
                WARNING("Cannot find volume " << id);
                sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
            } else {
                lock->ReleaseLock();
                lock = NULL;
                volume = NULL;

                if (!volume_info->RemoveFromTarget(id, target)) {
                    WARNING("Cannot remove target: volume " << id << ", target " << target);
                    sstr << "\"ERROR\": \"Cannot remove target: volume " << id << ", target " << target << "\"" << std::endl;
                } else {
                    volume = volume_info->FindVolume(id, &lock);
                    if (!volume) {
                        WARNING("Cannot find volume " << id);
                        sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
                    } else {
                        sstr << WriteVolume(volume_info, volume);
                        lock->ReleaseLock();
                    }
                }
            }
        } else if (this->operation == "change-state") {
            DEBUG("Perform changestate operation: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(this->options.size() == 2, "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "id", "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.back().first == "state", "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(To<uint32_t>(this->options.front().second).valid(), "Illegal option: " << this->options.front().second);
            uint32_t id = To<uint32_t>(this->options.front().second).value();
            string new_state = this->options.back().second;

            bool new_mainteinance_state = false;
            if (new_state == "running") {
                new_mainteinance_state = false;
            } else if (new_state == "maintenance") {
                new_mainteinance_state = true;
            } else {
                CHECK_RETURN_JSON(false, "Illegal state: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            }

            dedupv1::base::MutexLock* lock = NULL;
            Dedupv1dVolume* volume = volume_info->FindVolume(id, &lock);
            if (!volume) {
                WARNING("Cannot find volume " << id);
                sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
            } else {
                lock->ReleaseLock();
                lock = NULL;
                volume = NULL;

                if (!volume_info->ChangeMaintainceMode(id, new_mainteinance_state)) {
                    WARNING("Cannot change state: volume " << id << ", state " << new_mainteinance_state);
                    sstr << "\"ERROR\": \"Cannot change volume: volume " << id << ", state " << new_mainteinance_state << "\"" << std::endl;
                } else {
                    volume = volume_info->FindVolume(id, &lock);
                    if (!volume) {
                        WARNING("Cannot find volume " << id);
                        sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
                    } else {
                        sstr << WriteVolume(volume_info, volume);
                        lock->ReleaseLock();
                    }
                }
            }
        } else if (this->operation == "change-size") {
            DEBUG("Perform change-size operation: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(this->options.size() == 2, "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "id", "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.back().first == "logical-size", "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(To<uint32_t>(this->options.front().second).valid(), "Illegal option: " << this->options.front().second);
            uint32_t id = To<uint32_t>(this->options.front().second).value();

            // Logical sizes are allowed to end with "B". There is the hack to fix this
            string logical_size_option = this->options.back().second;
            Option<int64_t> ls = ToStorageUnit(logical_size_option);
            if (!ls.valid()) {
                // try with "B" prefix
                if (logical_size_option.size() > 1 &&
                    (logical_size_option[logical_size_option.size() - 1] == 'B' || logical_size_option[logical_size_option.size() - 1] == 'b')) {
                    ls = ToStorageUnit(logical_size_option.substr(0, logical_size_option.size() - 1));
                    CHECK_RETURN_JSON(ls.valid(), "Illegal option " << logical_size_option);
                } else {
                    CHECK_RETURN_JSON(false, "Illegal option " << logical_size_option);
                }
            }
            CHECK_RETURN_JSON(ls.value() > 0, "Illegal logical size " << logical_size_option);
            uint64_t new_logical_size = ls.value();

            dedupv1::base::MutexLock* lock = NULL;
            Dedupv1dVolume* volume = volume_info->FindVolume(id, &lock);
            if (!volume) {
                WARNING("Cannot find volume " << id);
                sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
            } else {
                lock->ReleaseLock();
                lock = NULL;
                volume = NULL;

                if (!volume_info->ChangeLogicalSize(id, new_logical_size)) {
                    WARNING("Cannot change size: volume " << id << ", new size " << new_logical_size);
                    sstr << "\"ERROR\": \"Cannot change volume: volume " << id << ", new size " << new_logical_size << "\"" << std::endl;
                } else {
                    volume = volume_info->FindVolume(id, &lock);
                    if (!volume) {
                        WARNING("Cannot find volume " << id);
                        sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
                    } else {
                        sstr << WriteVolume(volume_info, volume);
                        lock->ReleaseLock();
                    }
                }
            }
        } else if (this->operation == "change-options") {
            DEBUG("Perform change-options operation: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(this->options.size() >= 2, "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(this->options.front().first == "id", "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(To<uint32_t>(this->options.front().second).valid(), "Illegal option: " << this->options.front().second);
            uint32_t id = To<uint32_t>(this->options.front().second).value();

            dedupv1::base::MutexLock* lock = NULL;
            Dedupv1dVolume* volume = volume_info->FindVolume(id, &lock);
            if (!volume) {
                WARNING("Cannot find volume " << id);
                sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
            } else {

                lock->ReleaseLock();
                lock = NULL;
                volume = NULL;

                list< pair<string, string> > tail_options = options;
                tail_options.erase(tail_options.begin()); // remove id entry

                if (!volume_info->ChangeOptions(id, tail_options)) {
                    WARNING("Cannot change options: volume " << id << ", options " << Dedupv1dVolumeInfo::DebugStringOptions(tail_options));
                    sstr << "\"ERROR\": \"Cannot change options: volume " << id << ", options " << Dedupv1dVolumeInfo::DebugStringOptions(tail_options) << "\"";
                } else {
                    volume = volume_info->FindVolume(id, &lock);
                    if (!volume) {
                        WARNING("Cannot find volume " << id);
                        sstr << "\"ERROR\": \"Cannot find volume " << id << "\"" << std::endl;
                    } else {
                        sstr << WriteVolume(volume_info, volume);
                        lock->ReleaseLock();
                    }
                }
            }
        } else if (this->operation == "fast-copy") {
            DEBUG("Perform fast copy operation: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            CHECK_RETURN_JSON(this->options.size() >= 2, "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));

            list< pair<string, string> >::iterator i = options.begin();

            // 1. parameter
            CHECK_RETURN_JSON(i->first == "src-id", "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(To<uint32_t>(i->second).valid(), "Illegal option: " << i->first << "=" << i->second);
            uint32_t src_id = To<uint32_t>(i->second).value();

            // 2. parameter
            i++;
            CHECK_RETURN_JSON(i->first == "target-id", "Illegal options: " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            CHECK_RETURN_JSON(To<uint32_t>(i->second).valid(), "Illegal option: " << i->first << "=" << i->second);
            uint32_t target_id = To<uint32_t>(i->second).value();

            i++;
            uint64_t size = 0;
            uint64_t src_offset = 0;
            uint64_t target_offset = 0;

            for (; i != options.end(); i++) {
                if (i->first == "size") {
                    CHECK_RETURN_JSON(ToStorageUnit(i->second).valid(), "Illegal option: " << i->first << "=" << i->second);
                    size = ToStorageUnit(i->second).value();
                } else if (i->first == "src-offset") {
                    CHECK_RETURN_JSON(ToStorageUnit(i->second).valid(), "Illegal option: " << i->first << "=" << i->second);
                    src_offset = ToStorageUnit(i->second).value();
                } else if (i->first == "target-offset") {
                    CHECK_RETURN_JSON(ToStorageUnit(i->second).valid(), "Illegal option: " << i->first << "=" << i->second);
                    target_offset = ToStorageUnit(i->second).value();
                } else {
                    CHECK_RETURN_JSON(false, "Illegal option: " << i->first << "=" << i->second);
                }
            }

            CHECK_RETURN_JSON(size > 0, "Fast copy size not set");

            dedupv1::base::MutexLock* lock = NULL;
            Dedupv1dVolume* volume = volume_info->FindVolume(src_id, &lock);
            CHECK_RETURN_JSON(volume, "Cannot find volume " << src_id);

            lock->ReleaseLock();
            lock = NULL;
            volume = NULL;

            volume = volume_info->FindVolume(target_id, &lock);
            CHECK_RETURN_JSON(volume, "Cannot find volume " << target_id);

            lock->ReleaseLock();
            lock = NULL;
            volume = NULL;

            CHECK_RETURN_JSON(volume_info->FastCopy(src_id, target_id, src_offset, target_offset, size),
                "Cannot perform fast copy: " << Dedupv1dVolumeInfo::DebugStringOptions(options));

            sstr << WriteAllVolumes(ds);
        } else {
            WARNING("Illegal operation " << this->operation << ": " << Dedupv1dVolumeInfo::DebugStringOptions(this->options));
            sstr << "\"ERROR\": \"Illegal operation\"" << std::endl;
        }
        options.clear();
    } else {
        // no options: Give all volumes
        sstr << WriteAllVolumes(ds);
    }
    sstr << "}";
    TRACE(sstr.str());
    return sstr.str();
}

}
}
