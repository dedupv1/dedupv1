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

#include <base/strutil.h>
#include <base/locks.h>
#include <base/logging.h>

#include "dedupv1d.h"
#include "monitor.h"
#include "default_monitor.h"

#include "logging_monitor.h"

using std::string;
using std::stringstream;
using dedupv1::log::Log;

LOGGER("LoggingMonitorAdapter");

namespace dedupv1d {
namespace monitor {

LoggingMonitorAdapter::LoggingMonitorAdapter() {
}

string LoggingMonitorAdapter::Monitor() {
    if (message_ != "") {
        string logger_name = this->logger_;
        if (logger_name == "") {
            logger_name = "External";
        }
        LOGGER_CLASS log = GET_LOGGER(logger_name);

        if (this->level_ == "") {
            this->level_ = "INFO";
        }
        if (this->level_ == "DEBUG") {
            DEBUG_LOGGER(log, this->cmd_ << ": " << this->message_);
        } else if (this->level_ == "INFO") {
            INFO_LOGGER(log, this->cmd_ << ": " << this->message_);
        } else if (this->level_ == "WARNING") {
            WARNING_LOGGER(log, this->cmd_ << ": " << this->message_);
        } else if (this->level_ == "ERROR") {
            ERROR_LOGGER(log, this->cmd_ << ": " << this->message_ << (trace_.empty() ? "" : ", " + trace_));
        } else {
            CHECK_RETURN_JSON(false, "Illegal level: " << this->level_);
        }
    }
    return "{}";
}

bool LoggingMonitorAdapter::ParseParam(const string& key, const string& value) {
    if (key == "message") {
        this->message_ = value;
        return true;
    } else if (key == "logger") {
        this->logger_ = value;
        return true;
    } else if (key == "level") {
        this->level_ = value;
        return true;
    } else if (key == "cmd") {
        this->cmd_ = value;
        return true;
    } else if (key == "trace") {
        this->trace_ = value;
        return true;
    }
    WARNING("Illegal log monitor param " << key << "=" << value);
    return false;
}

}
}
