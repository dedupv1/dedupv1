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

#include "command_handler.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#ifndef NO_SCST
#include <malloc.h>
#endif
#include <sstream>
#include <tbb/tick_count.h>

#include <base/bitutil.h>
#include <core/dedup_system.h>
#include <core/dedup_volume.h>
#include <base/crc32.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <base/hashing_util.h>
#include <core/dedupv1_scsi.h>

#include "dedupv1d_stats.pb.h"
#include "dedupv1d.pb.h"

#include "dedupv1d_volume.h"
#include "dedupv1d_session.h"

using std::string;
using std::stringstream;
using std::endl;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::ToHexString;
using dedupv1::scsi::SCSI_OK;
using dedupv1::scsi::ScsiResult;
using dedupv1::scsi::SCSI_KEY_MEDIUM_ERROR;
using dedupv1::scsi::SCSI_KEY_ILLEGAL_REQUEST;
using dedupv1::scsi::SCSI_KEY_NOT_READY;
using dedupv1::scsi::SCSI_KEY_MISCOMPARE;
using dedupv1::REQUEST_READ;
using dedupv1::REQUEST_WRITE;
using dedupv1::scsi::SCSI_CHECK_CONDITION;
using dedupv1::scsi::SCSI_KEY_VENDOR_SPECIFIC;
using dedupv1::scsi::SCSI_KEY_RECOVERD;
using dedupv1::base::bit_set;
using dedupv1::base::bit_clear;
using dedupv1::base::bit_test;
using dedupv1::base::ErrorContext;
using dedupv1::base::Option;

namespace dedupv1d {

const string CommandHandler::kVendorName("DEDUPV1");
const string CommandHandler::kProductName("DEDUPV1");

LOGGER("CommandHandler");

CommandErrorReport::CommandErrorReport(std::time_t time, int opcode, uint64_t sector,
                                       const dedupv1::scsi::ScsiResult& result, const std::string& details) {
    time_ = time;
    sector_ = sector;
    result_ = result;
    details_ = details;
    opcode_ = opcode;
}

CommandHandler::CommandHandler() :
    response_time_average_(256) {
    this->volume_ = NULL;
    this->started = false;
    this->session_count_ = 0;
    max_error_count_ = 5;
    this->info_store_ = NULL;
    error_reports_last_store_ = tbb::tick_count::now();
}

bool CommandHandler::Start(Dedupv1dVolume* volume, dedupv1::InfoStore* info_store) {
    CHECK(!this->started, "Command handler already started");
    CHECK(volume, "Dedupv1 volume not set");

    CHECK(info_store, "Info store not set");
    info_store_ = info_store;

    DEBUG("Starting command handler: volume " << volume->DebugString());

    this->volume_ = volume;
    this->started = true;

    if (!RestoreErrorReports()) {
        WARNING("Failed to restore error reports");
    }
    return true;
}

CommandHandler::~CommandHandler() {
    if (this->session_count_ > 0) {
        WARNING("Open command handler sessions: session count " << this->session_count_);
    }
}

bool CommandHandler::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    CommandHandlerStatsData data;
    data.set_scsi_command_count(this->stats_.scsi_command_count_);
    data.set_sector_read_count(this->stats_.sector_read_count_);
    data.set_sector_write_count(this->stats_.sector_write_count_);
    data.set_retry_count(this->stats_.retry_count_);
    tbb::concurrent_unordered_map<byte, tbb::atomic<uint64_t> >::iterator i;
    for (i = this->stats_.scsi_command_map_.begin(); i != this->stats_.scsi_command_map_.end(); i++) {
        byte opcode = i->first;
        uint64_t count = i->second;

        CommandHandlerOpcodeStatsData* opcode_data = data.add_opcode_stats();
        opcode_data->set_opcode(opcode);
        opcode_data->set_count(count);
    }
    for (i = this->stats_.scsi_task_mgmt_map_.begin(); i != this->stats_.scsi_task_mgmt_map_.end(); i++) {
        byte tmcode = i->first;
        uint64_t count = i->second;

        CommandHandlerTaskMgmtStatsData* task_mgmt_data = data.add_task_mgmt_stats();
        task_mgmt_data->set_tmcode(tmcode);
        task_mgmt_data->set_count(count);
    }
    for (i = this->stats_.error_count_map_.begin(); i != this->stats_.error_count_map_.end(); i++) {
        byte opcode = i->first;
        uint64_t count = i->second;

        CommandHandlerErrorStatsData* error_data = data.add_error_stats();
        error_data->set_opcode(opcode);
        error_data->set_count(count);
    }
    CHECK(ps->Persist(prefix, data), "Failed to persist command handler stats");
    return true;
}

bool CommandHandler::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    CommandHandlerStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to load command handler stats");
    this->stats_.scsi_command_count_ = data.scsi_command_count();
    if (data.has_sector_read_count()) {
        this->stats_.sector_read_count_ = data.sector_read_count();
    }
    if (data.has_sector_write_count()) {
        this->stats_.sector_write_count_ = data.sector_write_count();
    }
    if (data.has_retry_count()) {
        this->stats_.retry_count_ = data.retry_count();
    }
    for (int i = 0; i < data.opcode_stats_size(); i++) {
        this->stats_.scsi_command_map_[data.opcode_stats(i).opcode()] += data.opcode_stats(i).count();
    }
    for (int i = 0; i < data.task_mgmt_stats_size(); i++) {
        this->stats_.scsi_task_mgmt_map_[data.task_mgmt_stats(i).tmcode()] += data.task_mgmt_stats(i).count();
    }
    for (int i = 0; i < data.error_stats_size(); i++) {
        this->stats_.error_count_map_[data.error_stats(i).opcode()] += data.error_stats(i).count();
    }
    return true;
}

string CommandHandler::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"thread states\": " << std::endl;
    sstr << "{" << endl;
    tbb::concurrent_unordered_map<int, CommandHandlerThreadState>::const_iterator i;
    for (i = this->runner_states_.begin(); i != this->runner_states_.end(); i++) {
        int thread_id = i->first;
        const CommandHandlerThreadState& state = i->second;
        if (i != this->runner_states_.begin()) {
            sstr << "," << std::endl;
        }
        if (state.command() == 0) {
            sstr << "\"" << thread_id << "\": null";
        } else {
            sstr << "\"" << thread_id << "\": {";
            sstr << "\"cmd id\": \"" << state.cmd_id() << "\"," << std::endl;
            sstr << "\"command\": \"" << GetOpcodeName(state.command()) << "\"," << std::endl;
            sstr << "\"session\": \"" << state.session() << "\"" << std::endl;
            sstr << "}";
        }
    }
    sstr << "}";
    sstr << "}";
    return sstr.str();
}

string CommandHandler::PrintStatistics() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{";
    sstr << "\"scsi command count\": " << this->stats_.scsi_command_count_ << "," << std::endl;
    sstr << "\"sector read count\": " << this->stats_.sector_read_count_ << "," << std::endl;
    sstr << "\"sector write count\": " << this->stats_.sector_write_count_ << "," << std::endl;
    sstr << "\"retry count\": " << this->stats_.retry_count_ << "," << std::endl;
    sstr << "\"average response time\": " << this->response_time_average_.GetAverage() << "," << std::endl;
    sstr << "\"average write response time\": " << this->response_time_write_average_.GetAverage() << "," << std::endl;
    sstr << "\"average write throughput\": " << this->stats_.average_write_throughput() << "," << std::endl;
    sstr << "\"average read throughput\": " << this->stats_.average_read_throughput() << "," << std::endl;
    sstr << "\"scsi commands\": " << std::endl;
    sstr << "{" << endl;
    tbb::concurrent_unordered_map<byte, tbb::atomic<uint64_t> >::iterator i;
    for (i = this->stats_.scsi_command_map_.begin(); i != this->stats_.scsi_command_map_.end(); i++) {
        byte opcode = i->first;
        uint64_t count = i->second;
        if (i != this->stats_.scsi_command_map_.begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" << GetOpcodeName(opcode) << "\": " << count;
    }
    sstr << "}," << endl;
    sstr << "\"scsi task mgmt\": " << std::endl;
    sstr << "{" << endl;
    for (i = this->stats_.scsi_task_mgmt_map_.begin(); i != this->stats_.scsi_task_mgmt_map_.end(); i++) {
        byte tmcode = i->first;
        uint64_t count = i->second;
        if (i != this->stats_.scsi_task_mgmt_map_.begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" << GetTaskMgmtFunctionName(tmcode) << "\": " << count;
    }
    sstr << "}," << endl;
    sstr << "\"errors\": " << std::endl;
    sstr << "{" << endl;
    for (i = this->stats_.error_count_map_.begin(); i != this->stats_.error_count_map_.end(); i++) {
        byte opcode = i->first;
        uint64_t count = i->second;
        if (i != this->stats_.error_count_map_.begin()) {
            sstr << "," << std::endl;
        }
        sstr << "\"" << GetOpcodeName(opcode) << "\": " << count;
    }
    sstr << "}" << endl;
    sstr << "}";
    return sstr.str();
}

bool CommandErrorReport::ParseFrom(const CommandErrorReportData& data) {
    time_ = data.time();
    details_ = data.details();
    opcode_ = data.opcode();
    sector_ = data.sector();

    result_ = ScsiResult((enum dedupv1::scsi::scsi_result) data.result().result(),
        (enum dedupv1::scsi::scsi_sense_key) data.result().sense_key(), data.result().asc(), data.result().ascq());
    return true;
}

bool CommandErrorReport::SerializeTo(CommandErrorReportData* data) const {
    DCHECK(data, "Data not set");

    data->set_time(time());
    data->set_details(details());
    data->set_opcode(opcode());
    data->set_sector(sector());

    data->mutable_result()->set_result(result().result());
    data->mutable_result()->set_sense_key(result().sense_key());
    data->mutable_result()->set_asc(result().asc());
    data->mutable_result()->set_ascq(result().ascq());
    return true;
}

bool CommandHandler::RestoreErrorReports() {
    DCHECK(this->info_store_, "Info store not set");

    CommandErrorReportsData reports_data;
    stringstream sstr;
    sstr << "volume." << this->volume_->id() << ".ch.error";

    CHECK(info_store_->RestoreInfo(sstr.str(), &reports_data), "Failed to restore error report data");

    for (int i = 0; i < reports_data.report_size(); i++) {
        const CommandErrorReportData& report_data(reports_data.report(i));

        CommandErrorReport report;
        CHECK(report.ParseFrom(report_data), "Failed to parse error report data: " << report_data.ShortDebugString());

        error_reports_.push_back(report);
    }
    return true;
}

bool CommandHandler::AddErrorReport(int opcode, uint64_t sector, const dedupv1::scsi::ScsiResult& result) {
    tbb::spin_mutex::scoped_lock scoped_lock(this->error_report_lock_);

    CommandErrorReport report(std::time(NULL), opcode, sector, result, string(""));

    this->error_reports_.push_front(report);

    if (this->error_reports_.size() > max_error_count_) {
        this->error_reports_.pop_back();
    }

    // persist the error data

    tbb::tick_count now = tbb::tick_count::now();
    if ((now - error_reports_last_store_).seconds() > 1) {
        error_reports_last_store_ = now;
        CommandErrorReportsData reports_data;
        std::list<CommandErrorReport>::const_iterator i;
        for (i = error_reports_.begin(); i != error_reports_.end(); ++i) {
            CommandErrorReportData* report = reports_data.add_report();
            report->set_time(i->time());
            report->set_details(i->details());
            report->set_opcode(i->opcode());
            report->set_sector(i->sector());

            report->mutable_result()->set_result(i->result().result());
            report->mutable_result()->set_sense_key(i->result().sense_key());
            report->mutable_result()->set_asc(i->result().asc());
            report->mutable_result()->set_ascq(i->result().ascq());
        }
        scoped_lock.release(); // do not access error_reports after here

        stringstream sstr;
        sstr << "volume." << this->volume_->id() << ".ch.error";
        if (!this->info_store_->PersistInfo(sstr.str(), reports_data)) {
            WARNING("Failed to persist error report data: " << reports_data.ShortDebugString());
        }
    }

    return true;
}

string CommandHandler::GetTaskMgmtFunctionName(uint32_t fn) {
    switch (fn) {
#ifndef NO_SCST
    case SCST_ABORT_TASK:
        return "Abort Task";
    case SCST_ABORT_TASK_SET:
        return "Abort Task Set";
    case SCST_CLEAR_ACA:
        return "Clear ACA";
    case SCST_CLEAR_TASK_SET:
        return "Clear Task Set";
    case SCST_LUN_RESET:
        return "LUN Reset";
    case SCST_TARGET_RESET:
        return "Target Reset";
    case SCST_NEXUS_LOSS_SESS:
        return "Nexus Loss Session";
    case SCST_ABORT_ALL_TASKS_SESS:
        return "Abort All Tasks Session";
    case SCST_NEXUS_LOSS:
        return "Nexus Loss";
    case SCST_ABORT_ALL_TASKS:
        return "Abort All Tasks";
    case SCST_UNREG_SESS_TM:
        return "Unreg Session Task";
#ifdef SCST_PR_ABORT_ALL
    case SCST_PR_ABORT_ALL:
        return "PR Abort All";
#endif
#endif
    default:
        return "Unknown Task (" + ToString(fn) + ")";
    }
    return "";
}

#ifndef NO_SCST
bool CommandHandlerSession::AttachSession(uint32_t cmd_h, struct scst_user_sess* sess) {
    CHECK(sess, "Session not set");

    INFO("Attach session: target " << sess->target_name << ", lun " << sess->lun << ", session " << sess->sess_h);

    CommandHandler* ch = this->GetCommandHandler();
    CHECK(ch, "Command handler not set");
    Dedupv1dVolume* volume_ = ch->GetVolume();
    CHECK(volume_, "Volume not set");

    Dedupv1dSession new_session(sess->sess_h, sess->target_name, sess->initiator_name, sess->lun);
    if (!volume_->AddSession(new_session)) {
        ERROR("Cannot add session: " << volume_->DebugString());
        return false;
    }
    INFO("Attach session: " << new_session.DebugString());

    return true;
}

void CommandHandlerSession::DetachSession(uint32_t cmd_h, uint64_t sess_h) {
    CommandHandler* ch = this->GetCommandHandler();

    if (!ch) {
        WARNING("Command handler not set");
        return;
    }

    Dedupv1dVolume* volume_ = ch->GetVolume();
    if (!volume_) {
        WARNING("Volume not set");
        return;
    }
    if (!volume_->RemoveSession(sess_h)) {
        WARNING("Failed to remove session: " << sess_h);
    }

}

int CommandHandlerSession::TaskMgmt(uint32_t cmd_h, uint64_t sess_h, struct scst_user_tm* tm) {
    if (this->ch) {
        this->ch->stats_.scsi_task_mgmt_map_[tm->fn]++;
    }

    Dedupv1dVolume* volume_ = ch->GetVolume();
    if (!volume_) {
        WARNING("Volume not set");
        return SCST_MGMT_STATUS_FAILED;
    }
    Option<Dedupv1dSession> session = volume_->FindSession(sess_h);
    if (!session.valid()) {
        WARNING("Failed to find session: session " << sess_h);
        return SCST_MGMT_STATUS_FAILED;
    } else {
        if (tm->fn == SCST_UNREG_SESS_TM) {
            INFO("Session " << session.value().DebugString() << ": Task Management " << CommandHandler::GetTaskMgmtFunctionName(tm->fn));
        } else if (tm->fn == SCST_ABORT_TASK) {
            DEBUG("Abort command " << tm->cmd_h_to_abort);
            std::string trace = ch->PrintTrace();
            std::string statistics = ch->PrintStatistics();
            WARNING("Session " << session.value().DebugString() << ": Task Management " << CommandHandler::GetTaskMgmtFunctionName(tm->fn) << "\nStatistics:\n" << statistics << "\nTrace:\n" << trace);
        } else {
            std::string trace = ch->PrintTrace();
            std::string statistics = ch->PrintStatistics();
            WARNING("Session " << session.value().DebugString() << ": Task Management " << CommandHandler::GetTaskMgmtFunctionName(tm->fn) << "\nStatistics:\n" << statistics << "\nTrace:\n" << trace);
        }
    }
    return SCST_MGMT_STATUS_SUCCESS;
}
#endif

static const char* opcode_names[255] = {
/* 00 NOLINT */ "TEST UNIT READY",
/* 01 NOLINT */ "REZERO UNIT", NULL,
/* 03 NOLINT */ "REQUEST SENSE",
/* 04 NOLINT */ "FORMAT UNIT",
/* 05 NOLINT */ "READ BLOCK LIMITS", NULL,
/* 07 NOLINT */ "REASSIGN BLOCKS",
/* 08 NOLINT */ "READ (6)", NULL,
/* 0A NOLINT */ "WRITE (6)",
/* 0B NOLINT */ "SEEK (6)", NULL, NULL, NULL,
/* 0F NOLINT */ "READ REVERSE",
/* 10 NOLINT */ "WRITE FILEMARKS",
/* 11 NOLINT */ "SPACE (6)",
/* 12 NOLINT */ "INQUIRY", NULL,
/* 14 NOLINT */ "RECOVER BUFFERED DATA",
/* 15 NOLINT */ "MODE SELECT (6)",
/* 16 NOLINT */ "RESERVE (6)",
/* 17 NOLINT */ "RELEASE (6)",
/* 18 NOLINT */ "COPY",
/* 19 NOLINT */ "ERASE",
/* 1A NOLINT */ "MODE SENSE (6)",
/* 1B NOLINT */ "START/STOP UNIT",
/* 1C NOLINT */ "RECEIVE DIAGNOSTIC RESULTS",
/* 1D NOLINT */ "SEND DIAGNOSTIC",
/* 1E NOLINT */ "PREVENT/ALLOW MEDIUM REMOVAL", NULL, NULL, NULL, NULL,
/* 23 NOLINT */ "READ FORMAT CAPACITIES (MMC)", NULL,
/* 25 NOLINT */ "READ CAPACITY (10)", NULL, NULL,
/* 28 NOLINT */ "READ (10)", NULL,
/* 2A NOLINT */ "WRITE (10)",
/* 2B NOLINT */ "SEEK (10)", NULL, NULL,
/* 2E NOLINT */ "WRITE AND VERIFY",
/* 2F NOLINT */ "VERIFY (10)",
/* 30 NOLINT */ "SEARCH DATA HIGH",
/* 31 NOLINT */ "SEARCH DATA EQUAL",
/* 32 NOLINT */ "SEARCH DATA LOW",
/* 33 NOLINT */ "SET LIMITS (10)",
/* 34 NOLINT */ "PRE-FETCH",
/* 35 NOLINT */ "SYNCHRONIZE CACHE (10)",
/* 36 NOLINT */ "LOCK/UNLOCK CACHE",
/* 37 NOLINT */ "READ DEFECT DATA", NULL,
/* 39 NOLINT */ "COMPARE",
/* 3A NOLINT */ "COPY AND VERIFY",
/* 3B NOLINT */ "WRITE BUFFER",
/* 3C NOLINT */ "READ BUFFER", NULL,
/* 3E NOLINT */ "READ LONG",
/* 3F NOLINT */ "WRITE LONG",
/* 40 NOLINT */ "CHANGE DEFINITION",
/* 41 NOLINT */ "WRITE SAME", NULL, NULL, NULL,
/* 45 NOLINT */ NULL, NULL, NULL, NULL, NULL,
/* 4A NOLINT */ NULL, NULL,
/* 4C NOLINT */ "LOG SELECT",
/* 4D NOLINT */ "LOG SENSE", NULL, NULL,
/* 50 NOLINT */ "XDWRITE",
/* 51 NOLINT */ "XPWRITE",
/* 52 NOLINT */ "XDREAD", NULL, NULL,
/* 55 NOLINT */ "MODE SELECT (10)",
/* 56 NOLINT */ "RESERVE (10)",
/* 57 NOLINT */ "RELEASE (10)", NULL, NULL,
/* 5A NOLINT */ "MODE SENSE (10)", NULL, NULL, NULL,
/* 5E NOLINT */ "PERSISTENT RESERVE IN",
/* 5F NOLINT */ "PERSISTENT RESERVE OUT", NULL, NULL, NULL, NULL, NULL,
/* 65 NOLINT */ NULL, NULL, NULL, NULL, NULL,
/* 6A NOLINT */ NULL, NULL, NULL, NULL, NULL, NULL,
/* 70 NOLINT */ NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
/* 80 NOLINT */ "XDWRITE EXTENDED", NULL,
/* 82 NOLINT */ "REGENERATE", NULL, NULL,
/* 85 NOLINT */ "ATA COMMAND PASS THROUGH(16)", NULL, NULL,
/* 88 NOLINT */ "READ (16)", NULL,
/* 8A NOLINT */ "WRITE (16)", NULL, NULL, NULL, NULL,
/* 8F NOLINT */ "VERIFY (16)",
/* 90 NOLINT */ NULL,
/* 91 NOLINT */ "SYNCHRONIZE CACHE (16)", NULL, NULL, NULL,
/* 95 NOLINT */ NULL, NULL, NULL, NULL, NULL,
/* 9A NOLINT */ NULL, NULL, NULL, NULL,
/* 9E NOLINT */ "READ CAPACITY (16)", NULL,
/* A0 NOLINT */ "REPORT LUNS", NULL, NULL,
/* A3 NOLINT */ "REPORT SUPPORTED OPCODES", NULL,
/* A5 NOLINT */ "MOVE MEDIUM", NULL, NULL,
/* A8 NOLINT */ "READ (12)", NULL,
/* AA NOLINT */ "WRITE (12)", NULL, NULL, NULL, NULL,
/* AF NOLINT */ "VERIFY (12)",
/* B0 NOLINT */ NULL, NULL, NULL,
/* B3 NOLINT */ "SET LIMITS (12)",
/* B4 NOLINT */ "READ ELEMENT STATUS", NULL, NULL,
/* B7 NOLINT */ "READ DEFECT DATA (12)"
};

string CommandHandler::GetOpcodeName(byte opcode) {
    const char* opcode_name = NULL;
    if (opcode <= 0xB7) {
        opcode_name = opcode_names[opcode];
    }
    if (opcode_name) {
        return opcode_name;
    }
    return "Opcode 0x" + ToHexString(static_cast<int>(opcode));
}

#ifndef NO_SCST
dedupv1::scsi::ScsiResult CommandHandlerSession::ExtractOffset(struct scst_user_scsi_cmd_exec* cmd, uint64_t* offset) {
    byte opcode = cmd->cdb[0];
    uint64_t lba = 0;
    uint64_t off = 0;
    switch (opcode) {
    case READ_6:
    case WRITE_6:
        lba = (((uint64_t) cmd->cdb[1] & 0x1f) << 16) + ((uint64_t) cmd->cdb[2] << 8) + ((uint64_t) cmd->cdb[3]
                                                                                         << 0);
        break;
    case READ_10:
    case WRITE_10:
    case VERIFY_10:
        lba = ((uint64_t) cmd->cdb[2] << 24) + ((uint64_t) cmd->cdb[3] << 16) + ((uint64_t) cmd->cdb[4] << 8)
              + ((uint64_t) cmd->cdb[5] << 0);
        break;
    case READ_16:
    case WRITE_16:
    case VERIFY_16:
        lba = ((uint64_t) cmd->cdb[2] << 56) + ((uint64_t) cmd->cdb[3] << 48) + ((uint64_t) cmd->cdb[4] << 40)
              + ((uint64_t) cmd->cdb[5] << 32) + ((uint64_t) cmd->cdb[6] << 24) + ((uint64_t) cmd->cdb[7] << 16)
              + ((uint64_t) cmd->cdb[8] << 8) + ((uint64_t) cmd->cdb[9] << 0);
        break;
    default:
        return ScsiResult::kIllegalMessage;

    }
    off = lba << ch->GetVolume()->block_shift();
    *offset = off;
    return ScsiResult::kOk;
}

dedupv1::scsi::ScsiResult CommandHandlerSession::ExecuteVerify(struct scst_user_scsi_cmd_exec* cmd, uint64_t offset,
                                                               uint64_t size) {
    // TODO(fermat): We have to ensure here that the data in the verify area is synchronized, so the accessed containers have to be written to storage.
    CommandHandler* ch = this->GetCommandHandler();

    byte control_byte = cmd->cdb[1];
    bool byte_check = dedupv1::base::bit_test(control_byte, 1);

    DEBUG("Verify offset " << offset << ", size " << size << ", byte check " << ToString(byte_check));

    byte* application_buffer = (byte *) cmd->pbuf;
    byte* own_buffer = NULL;
    own_buffer = new byte[size];
    CHECK_RETURN(own_buffer, ScsiResult::kDefaultNotReady, "Memalloc failed");
    ErrorContext ec;

    ScsiResult result = ch->GetVolume()->MakeRequest(REQUEST_READ, offset, size, own_buffer, &ec);
    if (!result) {
        ERROR("Reading data for verification failed: offset " << offset <<
            ", size " << size <<
            ", volume " << this->ch->volume_->DebugString() <<
            ", error " << result.DebugString());
        return result;
    }
    // reading was ok

    if (byte_check) {
        // a byte-by-byte comparison should be done
        bool ok = memcmp(application_buffer, own_buffer, size) == 0;
        if (!ok) {
            return ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_MISCOMPARE, 0x1D, 0x00);
        }
    }

    return ScsiResult::kOk;
}

dedupv1::scsi::ScsiResult CommandHandlerSession::ExecuteRead(struct scst_user_scsi_cmd_exec* cmd, uint64_t offset,
                                                             uint64_t size) {
    CommandHandler* ch = this->GetCommandHandler();

    DEBUG("Read offset " << offset << ", size " << size);
    ErrorContext ec;

    byte* buffer = (byte *) cmd->pbuf;
    ScsiResult result = ch->GetVolume()->MakeRequest(REQUEST_READ, offset, size, buffer, &ec);
    if (!result) {
        ERROR("Execute read failed: offset " << offset <<
            ", size " << size <<
            ", volume " << this->ch->volume_->DebugString() <<
            ", error " << result.DebugString());
        return result;
    }
    if (!ch->stats_.UpdateRead(size)) {
        WARNING("Failed to update statistics");
    }

    this->ch->stats_.sector_read_count_ += (size / this->ch->GetVolume()->block_size());

    return ScsiResult::kOk;
}

dedupv1::scsi::ScsiResult CommandHandlerSession::ExecuteWrite(struct scst_user_scsi_cmd_exec* cmd, uint64_t offset,
                                                              uint64_t size) {
    tbb::tick_count start_tick_ = tbb::tick_count::now();
    byte* buffer = NULL;
    CommandHandler* ch = this->GetCommandHandler();

    CHECK_RETURN(ch, ScsiResult::kIllegalMessage, "Command handler not set");
    CHECK_RETURN(cmd, ScsiResult::kIllegalMessage, "Command not set");
    CHECK_RETURN(cmd->pbuf, ScsiResult::kIllegalMessage, "Command buffer not set");

    ErrorContext ec;

    buffer = (byte *) cmd->pbuf;
    DEBUG("Write offset " << offset << ", size " << size);
    ScsiResult result = ch->GetVolume()->MakeRequest(REQUEST_WRITE, offset, size, buffer, &ec);
    if (!result) {
        ERROR("Execute write failed: offset " << offset <<
            ", size " << size <<
            ", volume " << this->ch->volume_->DebugString() <<
            ", error " << result.DebugString());
        tbb::tick_count end_tick = tbb::tick_count::now();
        this->ch->response_time_write_average_.Add((end_tick - start_tick_).seconds() * 1000);
        return result;
    }
    if (!ch->stats_.UpdateWrite(size)) {
        WARNING("Failed to update statistics");
    }
    this->ch->stats_.sector_write_count_ += (size / this->ch->GetVolume()->block_size());

    tbb::tick_count end_tick = tbb::tick_count::now();
    this->ch->response_time_write_average_.Add((end_tick - start_tick_).seconds() * 1000);
    return ScsiResult::kOk;
}

dedupv1::scsi::ScsiResult CommandHandlerSession::ExecuteSynchronizeCache(struct scst_user_scsi_cmd_exec* cmd,
                                                                         struct scst_user_scsi_cmd_reply_exec* reply) {

    bool sync_nv = cmd->cdb[1] & 0x04;
    bool immed = cmd->cdb[1] & 0x02;
    uint64_t logical_block_address = 0;
    uint32_t group_number = 0;
    uint32_t block_numbers = 0;
    if (cmd->cdb[0] == SYNCHRONIZE_CACHE) {
        logical_block_address = 0;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[2]) << 24;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[3]) << 16;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[4]) << 8;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[5]) << 0;
        group_number = cmd->cdb[6];
        block_numbers = 0;
        block_numbers += static_cast<uint32_t>(cmd->cdb[7]) << 8;
        block_numbers += static_cast<uint32_t>(cmd->cdb[8]) << 0;
    } else if (cmd->cdb[0] == SYNCHRONIZE_CACHE_16) {
        // 16 byte variant
        logical_block_address = 0;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[2]) << 56;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[3]) << 48;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[4]) << 40;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[5]) << 32;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[6]) << 24;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[7]) << 16;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[8]) << 8;
        logical_block_address += static_cast<uint64_t>(cmd->cdb[9]) << 0;
        block_numbers = 0;
        block_numbers += static_cast<uint32_t>(cmd->cdb[10]) << 24;
        block_numbers += static_cast<uint32_t>(cmd->cdb[11]) << 16;
        block_numbers += static_cast<uint32_t>(cmd->cdb[12]) << 8;
        block_numbers += static_cast<uint32_t>(cmd->cdb[13]) << 0;
        group_number = cmd->cdb[14];
    } else {
        ERROR("Illegal request: cdb op code " << cmd->cdb[1]);
        return dedupv1::scsi::ScsiResult::kIllegalMessage;
    }

    DEBUG("Synchronize Cache: sync " << dedupv1::base::strutil::ToString(sync_nv) <<
        ", immed " << dedupv1::base::strutil::ToString(immed) <<
        ", logical_block_address " << logical_block_address <<
        ", group number " << group_number <<
        ", block numbers " << block_numbers);

    if (immed) {
        // we do not support the immed bit
        return ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00); // Illegal field in CDB
    }

    return ch->volume_->SyncCache();
}

dedupv1::scsi::ScsiResult CommandHandlerSession::ExecuteReadCapacity16(struct scst_user_scsi_cmd_exec* cmd,
                                                                       struct scst_user_scsi_cmd_reply_exec* reply) {
    CHECK_RETURN(ch, ScsiResult::kIllegalMessage, "Command handler not set");
    CHECK_RETURN(cmd, ScsiResult::kIllegalMessage, "Command not set");
    CHECK_RETURN(cmd->pbuf, ScsiResult::kIllegalMessage, "Command buffer not set");
    CHECK_RETURN(reply, ScsiResult::kIllegalMessage, "Reply not set");

    uint32_t block_size = ch->volume_->block_size();
    uint64_t blocks = ch->volume_->block_count();
    byte* result_buffer = (byte *) cmd->pbuf;

    uint64_t logical_block_address;
    memcpy(&logical_block_address, &cmd->cdb[2], sizeof(logical_block_address));
    uint32_t allocation_length;
    memcpy(&allocation_length, &cmd->cdb[10], sizeof(allocation_length));
    bool pmi_set = (cmd->cdb[14] & 1);

    DEBUG("Read Capacity (16): lba " << logical_block_address <<
        ", allocation length " << allocation_length <<
        ", pmi " << dedupv1::base::strutil::ToString(pmi_set) <<
        ", logical size " << ch->volume_->logical_size() <<
        ", blocks " << blocks <<
        "," << block_size);

    int len = 32;
    byte buffer[32];
    memset(buffer, 0, len);
    buffer[0] = ((blocks - 1) >> 56) & 0xFF;
    buffer[1] = ((blocks - 1) >> 48) & 0xFF;
    buffer[2] = ((blocks - 1) >> 40) & 0xFF;
    buffer[3] = ((blocks - 1) >> 32) & 0xFF;
    buffer[4] = ((blocks - 1) >> 24) & 0xFF;
    buffer[5] = ((blocks - 1) >> 16) & 0xFF;
    buffer[6] = ((blocks - 1) >> 8) & 0xFF;
    buffer[7] = ((blocks - 1) >> 0) & 0xFF;

    buffer[8] = (block_size >> 24) & 0xFF;
    buffer[9] = (block_size >> 16) & 0xFF;
    buffer[10] = (block_size >> 8) & 0xFF;
    buffer[11] = (block_size >> 0) & 0xFF;

    if (cmd->bufflen < len) {
        len = cmd->bufflen;
    }
    memcpy(result_buffer, buffer, len);
    reply->resp_data_len = len;
    return ScsiResult::kOk;
}

dedupv1::scsi::ScsiResult CommandHandlerSession::ExecuteReadCapacity(struct scst_user_scsi_cmd_exec* cmd,
                                                                     struct scst_user_scsi_cmd_reply_exec* reply) {
    DCHECK_RETURN(ch, ScsiResult::kIllegalMessage, "Command handler not set");
    DCHECK_RETURN(cmd, ScsiResult::kIllegalMessage, "Command not set");
    DCHECK_RETURN(reply, ScsiResult::kIllegalMessage, "Reply not set");

    CHECK_RETURN(cmd->pbuf, ScsiResult::kIllegalMessage, "Command buffer not set");
    CHECK_RETURN(cmd->cdb_len >= 10, ScsiResult::kIllegalMessage, "Illegal cdb len " << cmd->cdb_len);

    uint32_t block_size = ch->volume_->block_size();
    uint64_t blocks = ch->volume_->block_count();
    byte* result_buffer = (byte *) cmd->pbuf;

    uint32_t logical_block_address;
    memcpy(&logical_block_address, &cmd->cdb[2], sizeof(logical_block_address));
    bool pmi_set = (cmd->cdb[8] & 1);
    bool overflow = (blocks >> 32);

    DEBUG("Read Capacity: lba " << logical_block_address <<
        ", pmi " << dedupv1::base::strutil::ToString(pmi_set) <<
        ", logical size " << ch->volume_->logical_size() <<
        ", blocks " << blocks << (overflow ? " (overflow)" : "") <<
        ", block size " << block_size);

    int len = 8;
    byte buffer[8];
    memset(buffer, 0, len);
    if (overflow) {
        buffer[0] = 0xFF;
        buffer[1] = 0xFF;
        buffer[2] = 0xFF;
        buffer[3] = 0xFF;
    } else {
        buffer[0] = ((blocks - 1) >> 24) & 0xFF;
        buffer[1] = ((blocks - 1) >> 16) & 0xFF;
        buffer[2] = ((blocks - 1) >> 8) & 0xFF;
        buffer[3] = ((blocks - 1) >> 0) & 0xFF;
    }
    buffer[4] = (block_size >> 24) & 0xFF;
    buffer[5] = (block_size >> 16) & 0xFF;
    buffer[6] = (block_size >> 8) & 0xFF;
    buffer[7] = (block_size >> 0) & 0xFF;
    if (cmd->bufflen < len) {
        len = cmd->bufflen;
    }
    memcpy(result_buffer, buffer, len);
    reply->resp_data_len = len;
    return ScsiResult::kOk;
}

namespace {
size_t ModeSenseCachingPage(byte* buffer) {
    buffer[0] = 0x08; // ps set to 0
    buffer[1] = 0x12; // page length

    buffer[2] = 0;
    buffer[2] |= 0x04; // write cache enabled
    buffer[2] |= 0x01; // read cache disabled

    buffer[4] = 0xFF; // we allow PREFETCH as much as possible
    buffer[5] = 0xFF;
    buffer[6] = 0x0; // no minimal prefetch size
    buffer[7] = 0x0;
    buffer[8] = 0xFF; // maximal prefetch size
    buffer[9] = 0xFF;
    buffer[10] = 0xFF; // maximal prefetch size
    buffer[11] = 0xFF;

    buffer[12] = 0; // FSW set to 0: We are allowed to reorder writes

    return 20;
}

size_t ModeSenseRecoveryPage(byte* buffer) {
    buffer[0] = 0x01; // page code, saveable disabled
    buffer[1] = 0x0A; // length
    buffer[2] = 0x0C; // allow much recovery operations
    buffer[3] = 0xFF; // read retry count
    buffer[8] = 0xFF; // write retry count
    buffer[10] = 0xFF; // recovery time limit
    buffer[11] = 0xFF; // recovery time limit
    return 12;
}

size_t ModeSenseDisconnectPage(byte* buffer) {
    buffer[0] = 0x02; // page code, savable disabled
    buffer[1] = 0x0E; // page length
    buffer[2] = 0xFF; // buffer full ratio TODO (dmeister): WTF?
    buffer[3] = 0xFF; // buffer empty ratio TODO(dmeister): WTF?

    buffer[12] = 0x80; // EMDP set to 1
    return 16;
}

size_t ModeSenseControlModePage(byte* buffer) {
    buffer[0] = 0x0A; // page code, savable disabled
    buffer[1] = 0x0A; // page length
    buffer[3] = 0x10; // Unrestricted queuing

    return 12;
}

size_t ModeSenseInformationExceptionsPage(byte* buffer) {
    buffer[0] = 0x1C; // page code, savable disabled
    buffer[1] = 0x0A; // page length
    buffer[2] = 0x08; // disable informations exceptions
    return 12;
}
}

dedupv1::scsi::ScsiResult CommandHandlerSession::ExecuteModeSense(struct scst_user_scsi_cmd_exec* cmd,
                                                                  struct scst_user_scsi_cmd_reply_exec* reply) {

    CHECK_RETURN(ch, ScsiResult::kIllegalMessage, "Command handler not set");
    CHECK_RETURN(cmd, ScsiResult::kIllegalMessage, "Command not set");
    CHECK_RETURN(cmd->pbuf, ScsiResult::kIllegalMessage, "Command buffer not set");
    CHECK_RETURN(reply, ScsiResult::kIllegalMessage, "Reply not set");

    byte buffer[1024];
    memset(buffer, 0, 1024);
    byte* result_buffer = (byte *) cmd->pbuf;

    bool dbd = cmd->cdb[1] & 0x04;
    int pc = (cmd->cdb[2] & 0xC0) >> 6; // bits 6 and 7
    int page_code = cmd->cdb[2] & 0x3F; // bits 0 - 5
    int subpage_code = cmd->cdb[3];

    DEBUG("Mode sense: dbd " << dedupv1::base::strutil::ToString(dbd) <<
        ", pc 0x" << std::hex << pc <<
        ", page code 0x" << std::hex << page_code <<
        ", sub page code 0x" << std::hex << subpage_code <<
        ", buffer length " << std::dec << cmd->bufflen);

    if (pc == 0x03) {
        // Saved values are not supported
        // ASC/SCQ set to SAVING PARAMETERS NOT SUPPORTED
        return ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x39, 0x00);
    }

    uint32_t offset = 4;
    // buffer[0] is set later
    buffer[1] = 0x00; // disk
    if (dbd) {
        // similar to read capacity (but only with 24 bits for blocks)
        buffer[3] = 8;

        uint32_t block_size = ch->volume_->block_size();
        uint64_t blocks = ch->volume_->block_count();
        bool overflow = (blocks >> 24);

        buffer[4] = 0; // density code
        if (overflow) {
            buffer[5] = 0xFF;
            buffer[6] = 0xFF;
            buffer[7] = 0xFF;
        } else {
            buffer[5] = ((blocks - 1) >> 16) & 0xFF;
            buffer[6] = ((blocks - 1) >> 8) & 0xFF;
            buffer[7] = ((blocks - 1) >> 0) & 0xFF;
        }
        buffer[8] = (block_size >> 24) & 0xFF;
        buffer[9] = (block_size >> 16) & 0xFF;
        buffer[10] = (block_size >> 8) & 0xFF;
        buffer[11] = (block_size >> 0) & 0xFF;

        offset += 8;
    }

    if (page_code == 0x01) {
        // recovery page code
        offset += ModeSenseRecoveryPage(buffer + offset);
    } else if (page_code == 0x02) {
        offset += ModeSenseDisconnectPage(buffer + offset);
    } else if (page_code == 0x08) {
        // caching page code
        offset += ModeSenseCachingPage(buffer + offset);
    } else if (page_code == 0x0A) {
        offset += ModeSenseControlModePage(buffer + offset);
    } else if (page_code == 0x1C) {
        offset += ModeSenseInformationExceptionsPage(buffer + offset);
    } else if (page_code == 0x3f) {
        offset += ModeSenseRecoveryPage(buffer + offset);
        offset += ModeSenseDisconnectPage(buffer + offset);
        offset += ModeSenseCachingPage(buffer + offset);
        offset += ModeSenseControlModePage(buffer + offset);
        offset += ModeSenseInformationExceptionsPage(buffer + offset);
    } else {
        WARNING("Unsupported sense page: page code 0x" << std::hex << page_code <<
            ", sub page code 0x" << std::hex << subpage_code);
        return ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00);
    }

    buffer[0] = offset - 1; // length of mode data without byte 0

    if (cmd->bufflen < offset) {
        offset = cmd->bufflen;
    }
    memcpy(result_buffer, buffer, offset);
    reply->resp_data_len = offset;

    return ScsiResult::kOk;
}

dedupv1::scsi::ScsiResult CommandHandlerSession::ExecuteInquiry(struct scst_user_scsi_cmd_exec* cmd,
                                                                struct scst_user_scsi_cmd_reply_exec* reply) {
    CHECK_RETURN(cmd, ScsiResult::kIllegalMessage, "Command not set");
    CHECK_RETURN(reply, ScsiResult::kIllegalMessage, "Reply not set");
    CHECK_RETURN(cmd->pbuf, ScsiResult::kIllegalMessage, "Result buffer not set");

    byte buffer[96];
    memset(buffer, 0, 96);
    byte* result_buffer = (byte *) cmd->pbuf;
    int len = 0;

    bool evpd = cmd->cdb[1] & 1;
    bool cmddt = cmd->cdb[1] & 2;
    byte page_code = cmd->cdb[2];

    DEBUG("Inquiry: evpd " << dedupv1::base::strutil::ToString(evpd) <<
        ", cmddt " << dedupv1::base::strutil::ToString(cmddt) <<
        ", page code 0x" << std::hex << static_cast<int>(page_code));

    if (evpd && cmddt) {
        return ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00);
    } else if (evpd) {
        if (page_code == 0) {
            buffer[3] = 2;
            buffer[4] = 0;
            buffer[5] = 0x80;
            buffer[6] = 0x83;
            len = 7;
        } else if (page_code == 0x80) {
            buffer[0] = 0x00; // disk
            buffer[1] = page_code;
            buffer[3] = 8;

            uint64_t usn = ch->GetVolume()->unique_serial_number();
            string usn_string = ToHexString(&usn, sizeof(usn));
            size_t s = usn_string.size();
            if (s > 8) {
                s = 8;
            }
            memcpy(&buffer[4], usn_string.data(), s);
            len = 8 + 4;
        } else if (page_code == 0x83) {
            len = 8 + ch->GetVolume()->device_name().size() + 4 + 8;
            buffer[0] = 0x00; // disk
            buffer[1] = page_code;
            buffer[3] = len - 3;

            // first identification descriptor
            buffer[4 + 0] = 0x02; // ASCII values
            // PIV set to 0 => no protocol data set
            // Association set to 0
            // Identifier type set to 0
            buffer[4 + 3] = ch->GetVolume()->device_name().size() + 1; // we are sure that device name is less than 256 bytes
            memcpy(&buffer[4 + 4], ch->GetVolume()->device_name().c_str(), ch->GetVolume()->device_name().size());

            // second identification descriptor
            size_t id_start = 8 + ch->GetVolume()->device_name().size();
            buffer[id_start + 0] = 0x02;
            buffer[id_start + 1] = 0x01; // IDENTIFIER: VENDOR ID
            buffer[id_start + 3] = 9;
            memcpy(&buffer[id_start + 4], "CHRISTMA", 8);

        } else {
            WARNING("Unsupported EVPD page " << std::hex << static_cast<int>(page_code));
            return ScsiResult::kIllegalMessage;
        }
    } else if (cmddt) {
        return ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00);
    } else {
        if (page_code != 0) {
            // page code should be zero
            return ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00);
        }

        if (ch->volume_->maintenance_mode()) {
            buffer[0] = 3 << 5; // disk, but PQ = 3
        } else {
            buffer[0] = 0x00; // disk
        }
        buffer[1] = 0x00; // not removable
        buffer[2] = 0x05; // we claim to fully support
        buffer[3] = 0x12; // response data format 2 + HiSup
        // HiSup means that we support LUNs
        buffer[4] = 36 - 5; // additional length
        bit_clear(&buffer[5], 3); // no 3rd party copy TODO (dmeister): Support third party copy later
        bit_clear(&buffer[5], 0); // no protection support
        bit_set(&buffer[7], 1); // full queue support TODO (dmeister): Do we really support that?

        // VENDOR
        len = CommandHandler::kVendorName.size();
        if (len > 8) {
            len = 8;
        }
        memcpy(buffer + 8, CommandHandler::kVendorName.c_str(), len);

        // PRODUCT
        len = CommandHandler::kProductName.size();
        if (len > 16) {
            len = 16;
        }
        memcpy(buffer + 16, CommandHandler::kProductName.c_str(), len);

        // REVISION LEVEL
        memcpy(buffer + 32, " 001", 4);

        buffer[58] = 0x00; // SAM-3
        buffer[59] = 0x60;
        buffer[60] = 0x03; // SBC-2
        buffer[61] = 0x20;
        buffer[62] = 0x02; // SPC-2
        buffer[63] = 0x60;
        buffer[64] = 0x09; // iSCSI
        buffer[65] = 0x60;

        len = 66;
        buffer[4] = len - 5; // additional length
    }
    if (cmd->bufflen < len) {
        len = cmd->bufflen;
    }
    memcpy(result_buffer, buffer, len);
    reply->resp_data_len = len;
    return ScsiResult::kOk;
}

bool CommandHandlerSession::SetScsiError(struct scst_user_scsi_cmd_reply_parse* reply, const ScsiResult& result) {
    CHECK(reply, "Reply not set");
    CHECK(reply->psense_buffer, "Reply sense buffer not set");

    DEBUG("Set SCSI sense: " << result.DebugString() <<
        ", volume " << this->ch->volume_->DebugString());

    byte* buffer = (byte *) reply->psense_buffer;
    Option<size_t> s = result.SerializeTo(buffer, 18);
    CHECK(s.valid(), "Failed to serialize scsi result: " << result.DebugString());
    reply->sense_len = s.value();
    reply->status = result.result();
    return true;
}

bool CommandHandlerSession::SetScsiError(struct scst_user_scsi_cmd_reply_exec* reply, const ScsiResult& result) {
    CHECK(reply, "Reply not set");
    CHECK(reply->psense_buffer, "Reply sense buffer not set");

    DEBUG("Set SCSI sense: " << result.DebugString() <<
        ", volume " << this->ch->volume_->DebugString());

    byte* buffer = (byte *) reply->psense_buffer;
    Option<size_t> s = result.SerializeTo(buffer, 18);
    CHECK(s.valid(), "Failed to serialize scsi result: " << result.DebugString());
    reply->sense_len = s.value();
    reply->status = result.result();
    return true;
}

void CommandHandlerSession::ExecuteSCSICommand(uint32_t cmd_h, uint64_t sess_h, struct scst_user_scsi_cmd_exec* cmd,
                                               struct scst_user_scsi_cmd_reply_exec* reply) {

    tbb::tick_count start_tick_ = tbb::tick_count::now();

    uint64_t offset = 0;
    uint64_t size = 0;

    // set error sense buffer
    reply->psense_buffer = (uint64_t) this->GetErrorSenseBuffer();

    byte opcode = cmd->cdb[0];

    this->ch->runner_state(thread_id_).set_command(opcode);
    this->ch->runner_state(thread_id_).set_session(sess_h);
    this->ch->runner_state(thread_id_).set_cmd_id(cmd_h);
    DEBUG(CommandHandler::GetOpcodeName(opcode) << ": session " << cmd->sess_h << ", command " << cmd_h);
    reply->reply_type = SCST_EXEC_REPLY_COMPLETED;

    // Allocate memory if necessary
    if (cmd->pbuf == (uint64_t) NULL && cmd->alloc_len > 0) {
        cmd->pbuf = (uint64_t) memalign(PAGE_SIZE, cmd->alloc_len); // need to be aligned memory

        if (cmd->pbuf == 0) {
            ERROR("Alloc command buffer failed");

            this->SetScsiError(reply, ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_VENDOR_SPECIFIC, 0x80, 0x00));
            return;
        } else {
            reply->pbuf = (uint64_t) cmd->pbuf;
            TRACE("Allocates " << cmd->alloc_len << " bytes reply buffer");
        }
    }

    ScsiResult result;
    bool result_set = false;
    tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<dedupv1::scsi::ScsiResult> >::accessor a;
    Dedupv1dVolume* volume = this->ch->volume_;
    if (!volume->session_unit_attention_map().find(a, sess_h)) {
        TRACE("Failed to find session: " << sess_h);
        result = ScsiResult::kDefaultNotReady;
        result_set = true;
    }
    if (!result_set) {
        ScsiResult unit_attention_result;
        if (a->second.try_pop(unit_attention_result)) {

            INFO("Found unit attention: " << unit_attention_result.DebugString() <<
                ", session " << sess_h);
            // there are unit attention messages for this session
            result = unit_attention_result;
            result_set = true;
            a.release();
        } else {
            a.release();
        }
    }
    if (!result_set) {
        switch (opcode) {
        case WRITE_6:
        case WRITE_10:
        case WRITE_12:
        case WRITE_16:
            if (volume->maintenance_mode()) {
                result = ScsiResult::kNotReadyManualIntervention;
                result_set = true;
            } else {
                offset = 0;
                result = this->ExtractOffset(cmd, &offset);
                if (!result) {
                    ERROR("Failed to extract offset");
                } else {
                    size = cmd->bufflen;
                    result = this->ExecuteWrite(cmd, offset, size);
                }
            }
            break;
        case READ_6:
        case READ_10:
        case READ_12:
        case READ_16:
            if (volume->maintenance_mode()) {
                result = ScsiResult::kNotReadyManualIntervention;
                result_set = true;
            } else {
                offset = 0;
                result = this->ExtractOffset(cmd, &offset);
                if (!result) {
                    ERROR("Failed to extract offset");
                } else {
                    size = cmd->bufflen;
                    result = this->ExecuteRead(cmd, offset, size);
                    if (result) {
                        reply->resp_data_len = size;
                    }
                }
            }
            break;
        case VERIFY_10:
        case VERIFY_16:
            if (volume->maintenance_mode()) {
                result = ScsiResult::kNotReadyManualIntervention;
                result_set = true;
            } else {
                offset = 0;
                result = this->ExtractOffset(cmd, &offset);
                if (!result) {
                    ERROR("Failed to extract offset");
                } else {
                    size = cmd->bufflen;
                    result = this->ExecuteVerify(cmd, offset, size);
                }
            }
            break;
        case SYNCHRONIZE_CACHE:
        case SYNCHRONIZE_CACHE_16:
            if (volume->maintenance_mode()) {
                result = ScsiResult::kNotReadyManualIntervention;
                result_set = true;
            } else {
                result = this->ExecuteSynchronizeCache(cmd, reply);
            }
            break;
        case INQUIRY:
            result = this->ExecuteInquiry(cmd, reply);
            break;
        case READ_CAPACITY:
            result = this->ExecuteReadCapacity(cmd, reply);
            break;
        case READ_CAPACITY_16:
            result = this->ExecuteReadCapacity16(cmd, reply);
            break;
        case TEST_UNIT_READY:
            if (volume->maintenance_mode()) {
                result = ScsiResult::kNotReadyManualIntervention;
                result_set = true;
            } else {
                // do nothing, the default value is ok
            }
            break;
        case MODE_SENSE:
            result = this->ExecuteModeSense(cmd, reply);
            break;
        default:
            INFO("Unknown opcode " << CommandHandler::GetOpcodeName(opcode))
            ;
            result = ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x20, 0x00);
            break;
        }
    }
    if (!result && result.sense_key() != SCSI_KEY_ILLEGAL_REQUEST) {
        this->ch->stats_.error_count_map_[opcode]++;
    }
    if (result.sense_key() == SCSI_KEY_RECOVERD) {
        this->ch->stats_.retry_count_++;
    }
    tbb::tick_count end_tick = tbb::tick_count::now();
    double used_time = (end_tick - start_tick_).seconds() * 1000;
    this->ch->response_time_average_.Add(used_time);
    if (used_time > 100.0) {
        // TODO(fermat): remove Warning, it is for testing only
        uint64_t used_time_ms = static_cast<uint64_t>(used_time);
        DEBUG("Long Running request found. It took " << ToHexString(&used_time_ms, sizeof(used_time_ms)) << "ms and had opcode " << ToHexString(opcode));
    }

    this->ch->stats_.scsi_command_map_[opcode]++;
    this->ch->stats_.scsi_command_count_++;

    TRACE("Executed " << CommandHandler::GetOpcodeName(opcode) << ": " << result.DebugString());
    if (result.result() != SCSI_OK && reply->sense_len == 0) {
        this->SetScsiError(reply, result);

        if (!result) {
            this->ch->AddErrorReport(opcode, offset / this->ch->GetVolume()->block_size(), result);
        }
    }
    this->ch->runner_state(thread_id_).clear();
}

bool CommandHandlerSession::AllocMem(uint32_t cmd_h, uint64_t sess_h, struct scst_user_scsi_cmd_alloc_mem* cmd,
                                     struct scst_user_scsi_cmd_reply_alloc_mem* reply) {
    DCHECK(cmd, "cmd not set");
    DCHECK(reply, "reply not set");

    void* buffer = memalign(PAGE_SIZE, cmd->alloc_len); // memory needs to be aligned
    CHECK(buffer, "Alloc buffer failed: alloc length " << cmd->alloc_len);
#ifndef NDEBUG
    memset(buffer, 0, cmd->alloc_len);
#endif
    reply->pbuf = (uint64_t) buffer;

    this->ch->stats_.memory_allocation_count_++;

    TRACE("Allocated " << cmd->alloc_len << " bytes for caching buffer ");
    return true;
}

bool CommandHandlerSession::OnFreeMemory(uint32_t cmd_h, uint64_t sess_h, struct scst_user_scsi_on_free_cmd* cmd) {
    if (!cmd->buffer_cached && cmd->pbuf) {
        void* buffer = (void *) cmd->pbuf;
        TRACE("Free buffer");
        free(buffer);
    }
    return true;
}

void CommandHandlerSession::OnParse(uint32_t cmd_h, uint64_t sess_h, struct scst_user_scsi_cmd_parse* cmd,
                                    struct scst_user_scsi_cmd_reply_parse* reply) {
    if (cmd == NULL || reply == NULL) {
        ERROR("Command or reply not set");
        return;
    }

    if (cmd->cdb_len == 0) {
        WARNING("Bad cdb len: " << cmd->cdb_len);

    } else {
        int len = 0;
        byte opcode = cmd->cdb[0];
        switch (opcode) {
        case WRITE_6:
        case WRITE_10:
        case WRITE_12:
        case WRITE_16:
        case READ_6:
        case READ_10:
        case READ_12:
        case READ_16:
        case VERIFY_10:
        case VERIFY_16:
        case SYNCHRONIZE_CACHE:
        case SYNCHRONIZE_CACHE_16:
        case INQUIRY:
        case READ_CAPACITY:
        case READ_CAPACITY_16:
        case TEST_UNIT_READY:
        case MODE_SENSE:
            len = cmd->cdb_len;
            if (len > 256) {
                len = 256;
            }
            WARNING("Parsing error for known command: " << ToHexString(cmd->cdb, len))
            ;
            break;
        default:
            DEBUG("Unhandled opcode " << CommandHandler::GetOpcodeName(opcode));
        }
    }

    reply->psense_buffer = (uint64_t) this->GetErrorSenseBuffer();
    this->SetScsiError(reply, ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x20, 0x00));
}

bool CommandHandlerSession::OnFreeCachedMemory(uint32_t cmd_h, uint64_t sess_h,
                                               struct scst_user_on_cached_mem_free* cmd) {
    void* buffer = NULL;

    buffer = (void *) cmd->pbuf;
    TRACE("Free cached buffer");
    free(buffer);
    this->ch->stats_.memory_release_count_++;
    return true;
}
#endif

CommandHandlerSession::CommandHandlerSession(CommandHandler* ch, int thread_id) {
    this->ch = ch;
    this->thread_id_ = thread_id;
}

CommandHandlerSession::~CommandHandlerSession() {
    if (ch) {
      ch->session_count_--;
    }
}

CommandHandlerSession* CommandHandler::CreateSession(int thread_id) {
    CHECK_RETURN(this->started, NULL, "Command handler not started");

    CommandHandlerSession* chs = new CommandHandlerSession(this, thread_id);
    CHECK_RETURN(chs, NULL, "Alloc command handler session failed");

    TRACE("Creating command handler thread: volume " << this->volume_->DebugString());

    this->session_count_.fetch_and_increment();

    return chs;
}

CommandHandler::Statistics::Statistics() :
    write_througput_average_(5), read_througput_average_(5) {
    scsi_command_count_ = 0;
    memory_allocation_count_ = 0;
    memory_release_count_ = 0;
    sector_read_count_ = 0;
    sector_write_count_ = 0;
    retry_count_ = 0;
    start_tick_ = tbb::tick_count::now();
}

bool CommandHandler::Statistics::UpdateWrite(uint64_t size) {
    tbb::spin_mutex::scoped_lock scoped_lock(this->throughput_average_lock_);

    uint32_t second_since_start = (tbb::tick_count::now() - start_tick_).seconds();
    this->write_througput_average_.Add(second_since_start, size);
    return true;
}

bool CommandHandler::Statistics::UpdateRead(uint64_t size) {
    tbb::spin_mutex::scoped_lock scoped_lock(this->throughput_average_lock_);

    uint32_t second_since_start = (tbb::tick_count::now() - start_tick_).seconds();
    this->read_througput_average_.Add(second_since_start, size);
    return true;
}

double CommandHandler::Statistics::average_write_throughput() {
    tbb::spin_mutex::scoped_lock scoped_lock(this->throughput_average_lock_);

    uint32_t second_since_start = (tbb::tick_count::now() - start_tick_).seconds();
    return this->write_througput_average_.GetAverage(second_since_start);
}

double CommandHandler::Statistics::average_read_throughput() {
    tbb::spin_mutex::scoped_lock scoped_lock(this->throughput_average_lock_);

    uint32_t second_since_start = (tbb::tick_count::now() - start_tick_).seconds();
    return this->read_througput_average_.GetAverage(second_since_start);
}

}
