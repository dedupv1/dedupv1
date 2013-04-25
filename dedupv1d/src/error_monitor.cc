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

#include <sstream>

#include <core/dedup_system.h>
#include <base/logging.h>
#include <core/storage.h>
#include <base/strutil.h>

#include "dedupv1d.h"
#include "monitor.h"
#include "dedupv1d_volume.h"
#include "dedupv1d_volume_info.h"
#include "command_handler.h"

#include "error_monitor.h"

using std::string;
using std::stringstream;
using std::endl;
using std::list;
using dedupv1::DedupSystem;
using dedupv1::chunkstore::Storage;
using dedupv1d::Dedupv1dVolume;
using dedupv1d::Dedupv1dVolumeInfo;
using dedupv1::base::Option;

LOGGER("ErrorMonitorAdapter");

namespace dedupv1d {
namespace monitor {

ErrorMonitorAdapter::ErrorMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

bool ErrorMonitorAdapter::WriteSCSIResult(stringstream* sstr, const dedupv1::scsi::ScsiResult& result) {
    int sense_key = result.sense_key();
    int asc = result.asc();
    int ascq = result.ascq();

    stringstream localsstr;
    *sstr << "\"";
    *sstr << "sense key 0x" << std::hex << sense_key << std::dec <<
    ", asc 0x" << std::hex << asc << std::dec <<
    ", ascq 0x" << std::hex << ascq << std::dec;
    *sstr << "\"";

    *sstr << localsstr.str();
    return true;
}

bool ErrorMonitorAdapter::WriteVolumeReport(stringstream* sstr, Dedupv1dVolume* volume) {
    CommandHandler* ch = volume->command_handler();
    const CommandHandler::Statistics* stats = ch->stats();

    *sstr << "\"" << volume->id() << "\": {";
    *sstr << "\"name\": \"" << volume->device_name() << "\"," << std::endl;
    *sstr << "\"scsi task mgmt\": " << std::endl;
    *sstr << "{" << endl;

    tbb::concurrent_unordered_map<byte, tbb::atomic<uint64_t> >::const_iterator i;
    for (i = stats->scsi_task_mgmt_map_.begin(); i != stats->scsi_task_mgmt_map_.end(); i++) {
        byte tmcode = i->first;
        uint64_t count = i->second;
        if (i != stats->scsi_task_mgmt_map_.begin()) {
            *sstr << "," << std::endl;
        }
        *sstr << "\"" << CommandHandler::GetTaskMgmtFunctionName(tmcode) << "\": \"" << count << "\"";
    }
    *sstr << "}," << endl;
    *sstr << "\"errors\": " << std::endl;
    *sstr << "{" << endl;
    for (i = stats->error_count_map_.begin(); i != stats->error_count_map_.end(); i++) {
        byte opcode = i->first;
        uint64_t count = i->second;
        if (i != stats->error_count_map_.begin()) {
            *sstr << "," << std::endl;
        }
        *sstr << "\"" << CommandHandler::GetOpcodeName(opcode) << "\": " << count;
    }
    *sstr << "}," << endl;

    *sstr << "\"recent error details\": [";
    std::list<CommandErrorReport> reports = ch->GetErrorReports();
    std::list<CommandErrorReport>::const_iterator j;
    for (j = reports.begin(); j != reports.end(); j++) {
        if (j != reports.begin()) {
            *sstr << ",";
        }
        time_t t = j->time();
        struct tm ts;
        gmtime_r(&t, &ts);
        char time_buf[128];
        strftime(time_buf, sizeof(time_buf), "%a %Y-%m-%d %H:%M:%S %Z", &ts);

        *sstr << "{";
        *sstr << "\"opcode\": \"" << CommandHandler::GetOpcodeName(j->opcode()) << "\"," << std::endl;
        *sstr << "\"sector\": \"" << j->sector() << "\"," << std::endl;
        *sstr << "\"time\": \"" << time_buf << "\"," << std::endl;
        *sstr << "\"details\": \"" << j->details() << "\"," << std::endl;
        *sstr << "\"result\": ";
        WriteSCSIResult(sstr, j->result());
        *sstr << "" << std::endl;
        *sstr << "}";
    }
    *sstr << "]";
    *sstr << "}";
    return true;
}

string ErrorMonitorAdapter::Monitor() {
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
        CHECK_RETURN_JSON(WriteVolumeReport(&sstr, volume), "Failed to write volume report: " << volume->DebugString());
    }
    CHECK_RETURN_JSON(lock->ReleaseLock(), "Failed to release lock");
    sstr << "}";
    return sstr.str();
}

}
}

