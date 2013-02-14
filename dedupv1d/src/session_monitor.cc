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

#include "session_monitor.h"
#include <core/dedup_system.h>
#include <base/logging.h>
#include "dedupv1d.h"
#include "monitor.h"
#include "default_monitor.h"
#include "base/strutil.h"
#include "dedupv1d_volume.h"
#include "dedupv1d_volume_info.h"
#include "dedupv1d_session.h"

LOGGER("SessionMonitorAdapter");

using std::stringstream;
using std::string;
using std::list;
using std::set;
using dedupv1::base::strutil::ToString;
using dedupv1d::Dedupv1d;
using dedupv1d::Dedupv1dVolume;
using dedupv1d::Dedupv1dVolumeInfo;
using dedupv1d::Dedupv1dSession;
using dedupv1::base::Option;

namespace dedupv1d {
namespace monitor {

SessionMonitorAdapter::SessionMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

bool SessionMonitorAdapter::WriteVolumeSessionReport(stringstream* sstr, Dedupv1dVolume* volume) {
    Option<set<uint64_t> > session_set = volume->GetSessionSet();

    *sstr << "\"" << volume->id() << "\": {";
    *sstr << "\"name\": \"" << volume->device_name() << "\"," << std::endl;
    *sstr << "\"session\": " << std::endl;
    if (!session_set.valid()) {
        *sstr << "null";
    } else {
        *sstr << "[" << std::endl;
        set<uint64_t>::const_iterator i;
        for (i = session_set.value().begin(); i != session_set.value().end(); i++) {
            if (i != session_set.value().begin()) {
                *sstr << "," << std::endl;
            }
            Option<Dedupv1dSession> s = volume->FindSession(*i);
            if (s.valid()) {
                *sstr << "{";
                *sstr << "\"session id\": \"" << s.value().session_id() << "\"," << std::endl;
                *sstr << "\"target name\": \"" << s.value().target_name() << "\"," << std::endl;
                *sstr << "\"lun\": " << s.value().lun() << "," << std::endl;
                *sstr << "\"initiator name\": \"" << s.value().initiator_name() << "\"" << std::endl;
                *sstr << "}";
            }
        }
        *sstr << "]" << std::endl;
    }
    *sstr << "}";
    return true;
}

string SessionMonitorAdapter::Monitor() {
    stringstream sstr;
    sstr << "{";
    Dedupv1dVolumeInfo* volume_info = this->ds_->volume_info();
    CHECK_RETURN_JSON(volume_info, "Volume info not found");

    dedupv1::base::MutexLock* lock = NULL;
    Option<list<Dedupv1dVolume*> > volumes = volume_info->GetVolumes(&lock);
    CHECK_RETURN_JSON(volumes.valid(), "Failed to get volumes");

    list<Dedupv1dVolume*>::const_iterator i;
    for (i = volumes.value().begin(); i != volumes.value().end(); i++) {
        if (i != volumes.value().begin()) {
            sstr << "," << std::endl;
        }
        Dedupv1dVolume* volume = *i;
        CHECK_RETURN_JSON(WriteVolumeSessionReport(&sstr, volume), "Failed to write volume report: " << volume->DebugString());
    }
    CHECK_RETURN_JSON(lock->ReleaseLock(), "Failed to release lock");
    sstr << "}";
    return sstr.str();
}

}
}
