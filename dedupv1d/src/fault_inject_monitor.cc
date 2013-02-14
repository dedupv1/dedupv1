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

#include "fault_inject_monitor.h"

#include <base/fault_injection.h>
#include <base/strutil.h>
#include <core/dedup_system.h>
#include <base/logging.h>
#include "dedupv1d.h"
#include "monitor.h"
#include "default_monitor.h"

using std::string;
using dedupv1::base::strutil::To;
using dedupv1::base::Option;

LOGGER("FaultInjectMonitorAdapter");

namespace dedupv1d {
namespace monitor {

FaultInjectMonitorAdapter::FaultInjectMonitorAdapter() {
    WARNING("Fault injection monitor enabled");
}

MonitorAdapterRequest* FaultInjectMonitorAdapter::OpenRequest() {
    return new FaultInjectMonitorAdapterRequest();
}

FaultInjectMonitorAdapterRequest::FaultInjectMonitorAdapterRequest() {
    fault_id_ = "";
    hit_points_ = 1;
    failed_ = false;
}

FaultInjectMonitorAdapterRequest::~FaultInjectMonitorAdapterRequest() {

}

bool FaultInjectMonitorAdapterRequest::ParseParam(const string& key, const string& value) {
#ifdef FAULT_INJECTION
    if (key == "crash" || key == "fault-id") {
        fault_id_ = value;
    } else if (key == "hit-points") {
        Option<uint32_t> o = To<uint32_t>(value);
        if (!o) {
            failed_ = true;
        } else {
            hit_points_ = *o;
        }
    }  else{
        WARNING("Failed to inject crash fault: Illegal key " << key);
    }
#else
    WARNING("Failed to inject crash fault: fault injection disabled");
#endif
    return true;
}

string FaultInjectMonitorAdapterRequest::Monitor() {
    CHECK_RETURN_JSON(!failed_, "Illegal parameters");

    string content = "{";
#ifndef FAULT_INJECTION
    content += "\"fault injection\": false";
#else
    if (!fault_id_.empty()) {
        dedupv1::base::FaultInjection::ActivateFaultPoint(fault_id_, hit_points_);
    }

    content += "\"fault injection\": true";
#endif
    content += "}";

    return content;
}

}
}
