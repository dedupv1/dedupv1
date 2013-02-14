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
#include <core/container_storage.h>
#include <core/container_storage_gc.h>
#include <base/strutil.h>

#include "dedupv1d.h"
#include "monitor.h"

#include "container_gc_monitor.h"

using std::string;
using std::stringstream;
using std::endl;
using dedupv1::DedupSystem;
using dedupv1::chunkstore::ContainerStorage;
using dedupv1::chunkstore::ContainerGCStrategy;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::Split;
using dedupv1::base::Option;

LOGGER("ContainerGCMonitorAdapter");

namespace dedupv1d {
namespace monitor {

ContainerGCMonitorAdapter::ContainerGCMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

bool ContainerGCMonitorAdapter::ParseParam(const string& key, const string& value) {
    DCHECK(ds_, "Ds not set");

    DedupSystem* system = this->ds_->dedup_system();
    CHECK(system, "System not ready");
    
    ContainerStorage* storage = dynamic_cast<ContainerStorage*>(system->storage());
    CHECK(storage, "System not ready");

    ContainerGCStrategy* gc = storage->GetGarbageCollection();
    CHECK(gc, "System not ready");

    if (key == "force-storage-pressure" && value == "true") {
        if (!gc->OnStoragePressure()) {
            ERROR("Failed to force storage pressure");
            error_message_ = "Failed to force storage pressure";
        }
        return true;
    } else if (key == "merge") {
        string value1;
        string value2;
        CHECK(dedupv1::base::strutil::Split(value, ":", &value1, &value2),
                "Failed to split the merge values");

        Option<uint64_t> container_id1 = To<uint64_t>(value1);
        Option<uint64_t> container_id2 = To<uint64_t>(value2);
        CHECK(container_id1.valid(), "Illegal parameter: " << value);
        CHECK(container_id2.valid(), "Illegal parameter: " << value);

        DEBUG("Force merge: container id " << container_id1.value() << ", container id " << container_id2.value());

        bool aborted = false;
        if(!storage->TryMergeContainer(container_id1.value(), container_id2.value(), &aborted)) {
            ERROR("Failed to merge container id " << (container_id1.value()) << ", container id " << (container_id2.value()));
            error_message_ = "Failed to merge container id " + ToString(container_id1.value()) + ", container id " + ToString(container_id2.value());
        }
        if (aborted) {
            WARNING("Merge aborted: container id " << (container_id1.value()) << ", container id " << (container_id2.value()));
            error_message_ = "Merge aborted: container id " + ToString(container_id1.value()) + ", container id " + ToString(container_id2.value());
        }
    }
    return true;
}

string ContainerGCMonitorAdapter::Monitor() {
    CHECK_RETURN_JSON (error_message_.empty(), error_message_);

    stringstream sstr;
    sstr << "{";
    sstr << "}";
    return sstr.str();
}

}
}
