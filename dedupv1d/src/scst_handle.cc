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
#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <signal.h>
#include <sys/types.h>
#ifndef NO_SCST
#include <sys/user.h>
#include <sys/poll.h>
#endif

#include <core/dedup.h>
#include <base/strutil.h>
#include <core/storage.h>
#include <core/dedup_system.h>
#include <core/filter.h>
#include <core/chunker.h>
#include <core/fingerprinter.h>
#include <base/timer.h>
#include <base/logging.h>

#include "scst_handle.h"

using std::string;
using dedupv1::base::strutil::ToString;
using dedupv1::scsi::ScsiResult;

LOGGER("ScstHandle");

namespace dedupv1d {

#ifndef NO_SCST

bool ScstCommandHandler::AttachSession(uint32_t cmd_h, struct scst_user_sess* sess) {
    return true;
}

void ScstCommandHandler::DetachSession(uint32_t cmd_h, uint64_t sess_h) {
}

int ScstCommandHandler::TaskMgmt(uint32_t cmd_h, uint64_t sess_h, struct scst_user_tm* tm) {
    return SCST_MGMT_STATUS_SUCCESS;
}

#endif

/*
 * SCSI Disk Device Type
 */
#define TYPE_DISK   0x00
#define DEDUPV1_SCST_LICENSE "GPL"

bool ScstHandle::Register(uint32_t block_size) {
    CHECK(this->state_ == SCST_HANDLE_STATE_STARTED, "SCST Handle not started");
    CHECK(!this->registered_, "SCST Handle already registered");
#ifndef NO_SCST
    CHECK(this->file >= 0, "SCST file not open");
#endif
    DEBUG("Register scst handle: " << this->DebugString());

#ifndef NO_SCST
    struct scst_user_dev_desc desc;
    memset(&desc, 0, sizeof(struct scst_user_dev_desc));
    desc.version_str = (unsigned long) DEV_USER_VERSION;
    desc.license_str = (unsigned long) DEDUPV1_SCST_LICENSE;
    desc.type = TYPE_DISK;
    desc.opt.parse_type = SCST_USER_PARSE_EXCEPTION;
    desc.opt.on_free_cmd_type = SCST_USER_ON_FREE_CMD_CALL;
    desc.opt.memory_reuse_type = SCST_USER_MEM_NO_REUSE;
    desc.opt.partial_transfers_type = SCST_USER_PARTIAL_TRANSFERS_NOT_SUPPORTED;
    desc.opt.tst = SCST_CONTR_MODE_SEP_TASK_SETS;
    desc.opt.queue_alg = SCST_CONTR_MODE_QUEUE_ALG_UNRESTRICTED_REORDER;
    desc.opt.d_sense = SCST_CONTR_MODE_FIXED_SENSE;
    desc.block_size = block_size;
    strcpy(desc.name, this->device_name_.c_str());

    CHECK_ERRNO(ioctl(this->file, SCST_USER_REGISTER_DEVICE, &desc),
        "Failed to register SCST handle: " << this->DebugString() << ", message ");
#endif
    this->registered_ = true;
    return true;
}

bool ScstHandle::Unregister() {
    CHECK(this->registered_, "SCST handle is not registered: " << this->DebugString());

    DEBUG("Unregister scst handle: " << this->DebugString());
#ifndef NO_SCST
    CHECK_ERRNO(ioctl(this->file, SCST_USER_UNREGISTER_DEVICE), "Failed to unregister SCST handle: " << this->DebugString());
#endif
    this->registered_ = false;
    return true;
}

ScstHandle::ScstHandle() {
    this->device_name_ = "";
#ifndef NO_SCST
    this->file = -1;
#endif
    this->registered_ = false;
    this->state_ = SCST_HANDLE_STATE_CREATED;
    this->scst_user_filename_ = "/dev/scst_user";
}

ScstHandle::~ScstHandle() {
#ifndef NO_SCST
    if (this->file >= 0) {
        close(this->file);
        this->file = -1;
    }
#endif
}

bool ScstHandle::SetOption(const string& option_name, const string& option) {
    if (option_name == "device-name") {
        CHECK(option.size() + 1 < 50, "Illegal device name");
        this->device_name_ = option;
        return true;
    }
    ERROR("Illegal option: " << option);
    return false;
}

bool ScstHandle::Restart(uint32_t block_size) {
    CHECK(this->state_ == SCST_HANDLE_STATE_STOPPED, "SCST handle not started and stopped before");

    this->state_ = SCST_HANDLE_STATE_STARTED;
    CHECK(this->Register(block_size), "Cannot register driver");

    DEBUG("Restarted scst handle: " << this->DebugString());
    return true;
}

bool ScstHandle::Start(uint32_t block_size) {
    CHECK(this->state_ == SCST_HANDLE_STATE_CREATED, "SCST Handle already started");
    CHECK(this->device_name_.size() > 0, "Device name not set");

#ifndef NO_SCST
    this->file = open(this->scst_user_filename_.c_str(), O_RDWR | O_NONBLOCK);
    CHECK_ERRNO(this->file, "Failed to open SCST user file: " << this->DebugString() <<
        ", filename " << this->scst_user_filename_ <<
        ", message ");
#endif
    this->state_ = SCST_HANDLE_STATE_STARTED;
    CHECK(this->Register(block_size), "Cannot register driver");

    DEBUG("Started scst handle: " << this->DebugString());
    return true;
}

string ScstHandle::GetSubcommandName(uint32_t subcode) {
#ifdef NO_SCST
    return "";
#else
    switch (subcode) {
    case SCST_USER_ATTACH_SESS: return "Session Attach";
    case SCST_USER_DETACH_SESS: return "Session Detach";
    case SCST_USER_PARSE:       return "Parse";
    case SCST_USER_ALLOC_MEM:   return "Memory Alloc";
    case SCST_USER_EXEC:        return "Exec";
    case SCST_USER_ON_FREE_CMD: return "On Free";
    case SCST_USER_ON_CACHED_MEM_FREE: return "Cached Memory Free";
    case SCST_USER_TASK_MGMT_RECEIVED:   return "Task Mgmt Received";
    case SCST_USER_REPLY_CMD:   return "Reply";
    case SCST_USER_FLUSH_CACHE: return "Flush Cache";
    case SCST_USER_DEVICE_CAPACITY_CHANGED: return "Device Capacity Changed";
    case SCST_USER_GET_EXTENDED_CDB: return "Get Extended CDB";
    default: return "Unknown Command";
    }
#endif
}

bool ScstHandle::HandleCommand(ScstCommandHandler* handler, struct scst_user_get_cmd* cmd, struct scst_user_reply_cmd* response) {
    CHECK(handler, "Handler not set");
    CHECK(cmd, "Command not set");
    CHECK(response, "Response not set");

#ifdef NO_SCST
    return true;
#else
    ScsiResult result;
    DEBUG("Command " << cmd->cmd_h << " - " << ScstHandle::GetSubcommandName(cmd->subcode));
    switch (cmd->subcode) {
    case SCST_USER_ATTACH_SESS:
        if (!handler->AttachSession(cmd->cmd_h, &cmd->sess)) {
            response->result = -1;
        }
        break;
    case SCST_USER_DETACH_SESS:
        handler->DetachSession(cmd->cmd_h, cmd->sess.sess_h);
        // no error code
        break;
    case SCST_USER_TASK_MGMT_RECEIVED:
        response->result = handler->TaskMgmt(cmd->cmd_h, cmd->sess.sess_h, &cmd->tm_cmd);
        break;
    case SCST_USER_TASK_MGMT_DONE:
	response->result = 0;
	break;
    case SCST_USER_EXEC:
        handler->ExecuteSCSICommand(cmd->cmd_h, cmd->sess.sess_h, &cmd->exec_cmd, &response->exec_reply);
        // error reporting is done by SENSE handling
        break;
    case SCST_USER_ALLOC_MEM:
        if (!handler->AllocMem(cmd->cmd_h, cmd->sess.sess_h, &cmd->alloc_cmd, &response->alloc_reply)) {
            // Error reporting should be deferred until the USER_EXEC call
            response->result = 0;
        }
        break;
    case SCST_USER_ON_FREE_CMD:
        if (!handler->OnFreeMemory(cmd->cmd_h, cmd->sess.sess_h, &cmd->on_free_cmd)) {
            response->result = 0; // no error code
        }
        break;
    case SCST_USER_ON_CACHED_MEM_FREE:
        if (!handler->OnFreeCachedMemory(cmd->cmd_h, cmd->sess.sess_h, &cmd->on_cached_mem_free)) {
            response->result = 0; // no error code
        }
        break;
    case SCST_USER_PARSE:
        handler->OnParse(cmd->cmd_h, cmd->sess.sess_h, &cmd->parse_cmd, &response->parse_reply);
        // error reporting is done by SENSE handling. This feature is currently not documented in the official SCST documentation
        break;
    default:
        ERROR("Illegal command: subcode " << cmd->subcode);
        return false;
    }
    DEBUG("Command " << response->cmd_h << " - Sending Reply");
    return true;
#endif
}

bool ScstHandle::HandleProcessCommand(ScstCommandHandler* handler) {
    CHECK(this->state_ == SCST_HANDLE_STATE_STARTED,
        "SCST handle not started: " << this->DebugString() <<
        ", state " << state_);
    CHECK(handler, "Command handle not set");

#ifdef NO_SCST
    sleep(2); // sleep 2 seconds
    return true;
#else
    int err = 0;
    struct scst_user_get_cmd cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.preply = 0; // get command

    struct pollfd pollfd = {this->file, POLLIN | POLLPRI, 0};
    err = poll(&pollfd, 1, 2000); // poll for 2 seconds
    if (err == 0) {
        // No events
        return true;
    }
    if (err == -1 && errno == EINTR) {
        // Interrupted system call
        return true;
    }
    CHECK_ERRNO(err, "Failed to poll SCST: " << this->DebugString() << ", message ");
    err = ioctl(this->file, SCST_USER_REPLY_AND_GET_CMD, &cmd);
    if (err == -1 && errno == EAGAIN) {
        return true;
    }
    if (err == -1 && errno == EINTR) {
        return true;
    }
    if (err == -1 && errno == ENODEV) {
        return true;
    }
    CHECK_ERRNO(err, "Failed to get command from SCST: " << this->DebugString() << ", message ");

    struct scst_user_reply_cmd response;
    memset(&response, 0, sizeof(response));
    response.cmd_h = cmd.cmd_h;
    response.subcode = cmd.subcode;

    CHECK(HandleCommand(handler, &cmd, &response), "Cannot handle command: " << this->DebugString());
    CHECK_ERRNO(ioctl(this->file, SCST_USER_REPLY_CMD, &response), "Failed to send reply to SCST: " << this->DebugString() << ", message ");
    return true;
#endif
}

bool ScstHandle::NotifyDeviceCapacityChanged() {
    CHECK(registered_, "Not registered at SCST");

    DEBUG("Notify SCST about device capacity change");
#ifndef NO_SCST
    CHECK_ERRNO(ioctl(this->file, SCST_USER_DEVICE_CAPACITY_CHANGED),
        "Failed to notify SCST about device capacity change: ");
#endif
    return true;
}

std::string ScstHandle::DebugString() const {
    string s = "[SCST handle: name " + this->device_name_;
#ifndef NO_SCST
    s += ", file " + ToString(this->file);
#endif
    s += ", registered " + ToString(this->registered_) +
         "]";
    return s;
}

bool ScstHandle::Stop() {
#ifdef NO_SCST
    if (state_ == SCST_HANDLE_STATE_STARTED) {
        if (registered_) {
            CHECK(this->Unregister(), "Failed to unregister handle: " << this->DebugString());
        }
        this->state_ = SCST_HANDLE_STATE_STOPPED;
    }
#else
    if (state_ == SCST_HANDLE_STATE_STARTED && this->file >= 0) {
        DEBUG("Stop scst handle: " << this->DebugString());
        CHECK_ERRNO(ioctl(this->file, SCST_USER_FLUSH_CACHE), "Failed to flush SCST cache: " << this->DebugString() << ", message ");
        if (registered_) {
            CHECK(this->Unregister(), "Failed to unregister handle: " << this->DebugString());
        }
        this->state_ = SCST_HANDLE_STATE_STOPPED;
    }
#endif
    return true;
}

string ScstHandle::device_name() const {
    return device_name_;
}

ScstHandle::scst_handle_state ScstHandle::state() const {
    return this->state_;
}

#ifdef DEDUPV1_TEST
bool ScstHandle::ClearData() {
    return true;
}
#endif

}
