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

#include "gc_monitor.h"

using std::string;
using std::stringstream;
using std::endl;
using dedupv1::DedupSystem;
using dedupv1::gc::GarbageCollector;
using dedupv1::base::strutil::To;

LOGGER("GcMonitorAdapter");

namespace dedupv1d {
namespace monitor {

GCMonitorAdapter::GCMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

bool GCMonitorAdapter::ParseParam(const string& key, const string& value) {
    if (!ds_) {
        return true;
    }
    DedupSystem* system = this->ds_->dedup_system();
    if (!system) {
        return true;
    }
    GarbageCollector* gc = system->garbage_collector();
    if (!gc) {
        return true;
    }
    if (key == "force-processing" && value == "true") {
        CHECK(gc->StartProcessing(), "Failed to force processing");
        return true;
    }
    if (key == "force-processing" && value == "false") {
        CHECK(gc->StopProcessing(), "Failed to force stop");
        return true;
    }
    return false;
}

string GCMonitorAdapter::Monitor() {
    stringstream sstr;
    sstr << "{";
    DedupSystem* system = this->ds_->dedup_system();
    if (system == NULL) {
        sstr << "\"ERROR\": \"System not found\"" << std::endl;
    } else {
        GarbageCollector* gc = system->garbage_collector();
        if (gc == NULL) {
            sstr << "\"ERROR\": \"GC not found\"" << std::endl;
        } else {
            if (gc->IsProcessing()) {
                sstr << "\"state\": \"processing\"" << std::endl;
            } else {
                sstr << "\"state\": \"stopped\"" << std::endl;
            }
        }
    }
    sstr << "}";
    return sstr.str();
}

}
}
