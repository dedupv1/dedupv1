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

#include "idle_monitor.h"

using std::string;
using std::stringstream;
using std::endl;
using dedupv1::DedupSystem;
using dedupv1::IdleDetector;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::Option;

LOGGER("IdleMonitorAdapter");

namespace dedupv1d {
namespace monitor {

IdleMonitorAdapter::IdleMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

bool IdleMonitorAdapter::ParseParam(const string& key, const string& value) {
    if (key == "force-idle") {
        if (this->ds_) {
            DedupSystem* system = this->ds_->dedup_system();
            if (!system) {
                return true;
            }
            IdleDetector* id = system->idle_detector();
            if (!id) {
                return true;
            }
            Option<bool> b = To<bool>(value);
            CHECK(b.valid(), "Illegal value");
            CHECK(id->ForceIdle(b.value()), "Failed to force idle");
        }
    }
    if (key == "force-busy" && value == "true") {
        if (this->ds_) {
            DedupSystem* system = this->ds_->dedup_system();
            if (!system) {
                return true;
            }
            IdleDetector* id = system->idle_detector();
            if (!id) {
                return true;
            }
            Option<bool> b = To<bool>(value);
            CHECK(b.valid(), "Illegal value");
            CHECK(id->ForceBusy(b.value()), "Failed to force busy");
        }
    }
    if (key == "change-idle-tick-interval") {
        if (this->ds_) {
            DedupSystem* system = this->ds_->dedup_system();
            if (!system) {
                return true;
            }
            IdleDetector* id = system->idle_detector();
            if (!id) {
                return true;
            }
            CHECK(To<uint32_t>(value).valid(), "Illegal value: " << value);
            uint32_t seconds = To<uint32_t>(value).value();
            if (seconds > 0) {
                CHECK(id->ChangeIdleTickInterval(seconds), "Failed to set idle tick time");
            }
        }
    }
    return true;
}

string IdleMonitorAdapter::Monitor() {
    DedupSystem* system = this->ds_->dedup_system();
    CHECK_RETURN_JSON(system, "System not found");
    IdleDetector* id = system->idle_detector();
    CHECK_RETURN_JSON(id, "Idle detector not found");

    stringstream sstr;
    sstr << "{";

    if (id->IsIdle()) {
        sstr << "\"state\": \"idle\"," << std::endl;
    } else {
        sstr << "\"state\": \"busy\"," << std::endl;
    }

    sstr << "\"forced idle\": " << ToString(id->is_forced_idle()) << ",";
    sstr << "\"forced busy\": " << ToString(id->is_forced_busy());
    sstr << "}";
    return sstr.str();
}

}
}
