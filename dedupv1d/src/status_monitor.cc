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

#include "status_monitor.h"
#include <core/dedup_system.h>
#include <base/logging.h>
#include "dedupv1d.h"
#include "monitor.h"
#include "default_monitor.h"
#include "base/strutil.h"

LOGGER("StatusMonitorAdapter");

using std::string;
using dedupv1::base::strutil::ToString;
using dedupv1d::Dedupv1d;

namespace dedupv1d {
namespace monitor {

StatusMonitorAdapter::StatusMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

bool StatusMonitorAdapter::ParseParam(const string& key, const string& value) {
    if (key == "change-state" && value == "writeback-stop") {
        if (this->ds_) {
            CHECK(this->ds_->Shutdown(dedupv1::StopContext::WritebackStopContext()), "Failed to shutdown dedupv1d (writeback)");
        }
    } else if (key == "change-state" && value == "stop") {
        if (this->ds_) {
            CHECK(this->ds_->Shutdown(dedupv1::StopContext::FastStopContext()), "Failed to shutdown dedupv1d");
        }
    }
    return true;
}

string StatusMonitorAdapter::Monitor() {
    string state = "{";
    if (ds_->state() == Dedupv1d::CREATED) {
        state += "\"state\": \"init\"";
    } else if (ds_->state() == Dedupv1d::STARTING) {
        state += "\"state\": \"starting\"";
    } else if (ds_->state() == Dedupv1d::DIRTY_REPLAY) {
        state += "\"state\": \"starting\"";
    } else if (ds_->state() == Dedupv1d::STARTED) {
        state += "\"state\": \"started\"";
    } else if (ds_->state() == Dedupv1d::RUNNING) {
        state += "\"state\": \"ok\"";
    } else if (ds_->state() == Dedupv1d::STOPPED) {
        state += "\"state\": \"shutting down\"";
        if (ds_->GetStopContext().mode() == dedupv1::StopContext::WRITEBACK) {
            state += ",\"shutdown type\": \"writeback\"";
        } else {
            state += ",\"shutdown type\": \"default\"";
        }
    } else {
        state += "\"state\": \"unknown state\"";
    }
    state += ",\n\"pid\": \"" + ToString(getpid()) + "\"";
    state += "}";
    return state;
}

}
}
