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
#include <core/idle_detector.h>
#include <base/strutil.h>

#include "dedupv1d.h"
#include "monitor.h"

#include "monitor_monitor.h"

using std::string;
using std::stringstream;
using std::endl;
using dedupv1::DedupSystem;
using dedupv1::gc::GarbageCollector;
using dedupv1::base::strutil::To;

LOGGER("MonitorMonitorAdapter");

namespace dedupv1d {
namespace monitor {

MonitorMonitorAdapter::MonitorMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

bool MonitorMonitorAdapter::ParseParam(const string& key, const string& value) {
    return false;
}

string MonitorMonitorAdapter::Monitor() {
    stringstream sstr;
    bool first = true;
    std::vector<std::string> names = ds_->monitor()->GetMonitorNames();
    sstr << "{";
    sstr << "\"adapters\":";
    sstr << "[";
    for (std::vector<std::string>::iterator it = names.begin(); it != names.end(); it++) {
        if (!first) {
            sstr << ",";
        }
        sstr << "\"" << *it << "\"";
        first = false;
    }
    sstr << "]";
    sstr << "}";
    return sstr.str();
}

}
}
