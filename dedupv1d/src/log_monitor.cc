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
#include <core/log_consumer.h>
#include <core/log.h>
#include <base/logging.h>

#include "dedupv1d.h"
#include "monitor.h"
#include "log_replayer.h"
#include "default_monitor.h"

#include "log_monitor.h"

using std::string;
using std::stringstream;
using dedupv1::log::Log;
using dedupv1::base::strutil::ToString;

LOGGER("LogMonitorAdapter");

namespace dedupv1d {
namespace monitor {

LogMonitorAdapter::LogMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
    if (ds_) {
        this->log_replayer_ = ds_->log_replayer();
    }
    this->message_ = "";
}

string LogMonitorAdapter::Monitor() {
    CHECK_RETURN_JSON(this->log_replayer_, "Log replayer not set");
    stringstream sstr;

    string message = "";
    if (this->message_.empty()) {
        message = "null";
    } else {
        message = "\"" + this->message_ + "\"";
    }

    Log* log = log_replayer_->log();
    if (!log) {
        return "null";
    }
    uint64_t open_event_count = log->log_id() - log->replay_id();
    int64_t remaining_free_log = 0;
    CHECK_RETURN_JSON(log->GetRemainingFreeLogPlaces(&remaining_free_log),
        "Failed to get remaining free log places");

    sstr << "{" << std::endl;

    if (log_replayer_->is_failed()) {
        sstr << "\"state\": \"failed\"," << std::endl;
    } else {
        sstr << "\"state\": \"" << log_replayer_->state_name() << "\"," << std::endl;
    }
    sstr << "\"replaying\": \"" << ToString(log_replayer_->is_replaying()) << "\"," << std::endl;
    sstr << "\"open events\": " << open_event_count << "," << std::endl;
    sstr << "\"free event places\": " << remaining_free_log << "," << std::endl;
    sstr << "\"last message\": " << message << std::endl;
    sstr << "}" << std::endl;

    return sstr.str();
}

bool LogMonitorAdapter::ParseParam(const string& key, const string& value) {
    if (!log_replayer_) {
        WARNING("Log replayer not set");
        return false;
    }
    if (key == "state" && value == "pause") {
        if (!log_replayer_->Pause()) {
            this->message_ = "Error pausing log replay. Check log";
            WARNING("Error pausing log replay");
        }
        return true;
    }
    if (key == "state" && value == "resume") {
        if (!log_replayer_->Resume()) {
            this->message_ = "Error resume log replay. Check log";
            WARNING("Error resume log replay");
        }
        return true;
    }
    WARNING("Illegal log monitor param " << key << "=" << value);
    return false;
}

}
}
