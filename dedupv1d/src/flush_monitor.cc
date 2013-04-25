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

#include "dedupv1d.h"
#include "monitor.h"

#include "flush_monitor.h"

using std::string;
using std::stringstream;
using std::endl;
using dedupv1::DedupSystem;
using dedupv1::chunkstore::Storage;

LOGGER("FlushMonitorAdapter");

namespace dedupv1d {
namespace monitor {

FlushMonitorAdapter::FlushMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

bool FlushMonitorAdapter::ParseParam(const string& key, const string& value) {
    if (key == "flush" && value == "true") {
        DedupSystem* system = this->ds_->dedup_system();
        if (system != NULL) {
            Storage* storage = system->storage();
            if (storage != NULL) {
                dedupv1::base::ErrorContext ec;
                CHECK(storage->Flush(&ec), "Failed to flush storage: " << ec.DebugString());
            }
        }
    }
    return true;
}

string FlushMonitorAdapter::Monitor() {
    stringstream sstr;
    sstr << "{";
    DedupSystem* system = this->ds_->dedup_system();
    if (system == NULL) {
        sstr << "\"ERROR\": \"System not found\"" << std::endl;
    } else {
        Storage* storage = system->storage();
        if (storage == NULL) {
            sstr << "\"error\": \"Storage not found\"" << std::endl;
        } else {
            sstr << "\"flush\": \"ok\"" << std::endl;
        }
    }
    sstr << "}";
    return sstr.str();
}

}
}

