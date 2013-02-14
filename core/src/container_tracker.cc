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

#include <core/container_tracker.h>
#include <core/storage.h>
#include <base/logging.h>
#include <core/container_storage.h>
#include <base/strutil.h>

#include <sstream>

using std::string;
using std::stringstream;
using std::set;
using dedupv1::base::strutil::Join;
using dedupv1::chunkstore::Storage;
using dedupv1::chunkstore::Container;

LOGGER("ContainerTracker");

namespace dedupv1 {

ContainerTracker::ContainerTracker() {
    Clear();
}

void ContainerTracker::CopyFrom(const ContainerTracker& tracker) {
    least_non_processed_container_id_ = tracker.least_non_processed_container_id_;
    processed_containers_ = tracker.processed_containers_;
    highest_seen_container_id_ = tracker.highest_seen_container_id_;
    currently_processed_containers_ = tracker.currently_processed_containers_;
}

void ContainerTracker::Clear() {
    this->highest_seen_container_id_ = Storage::ILLEGAL_STORAGE_ADDRESS; // we have no
    // knowledge about non-processes containers
    this->least_non_processed_container_id_ = Container::kLeastValidContainerId;

    processed_containers_.clear();
    currently_processed_containers_.clear();
}

std::string ContainerTracker::DebugString() const {
    stringstream sstr;
    sstr << "least non processed container id " << least_non_processed_container_id_;
    sstr << ", highest seen container id " << highest_seen_container_id_;
    sstr << ", processed container [" << Join(this->processed_containers_.begin(), this->processed_containers_.end(), ", ") << "]";
    sstr << ", currently processing container [" << Join(this->currently_processed_containers_.begin(), this->currently_processed_containers_.end(), ", ") << "]";
    return sstr.str();
}

bool ContainerTracker::ShouldProcessContainerTest(uint64_t id) const {
    if (id < least_non_processed_container_id_) {
        return false; // if id less than least non processed id => id must be processed before
    } else if (id == least_non_processed_container_id_) {
        return true; // if id == least non processed id => id should be processed
    } else {
        // if id > least non processed id => check if id should be processed
        set<uint64_t>::const_iterator i = this->processed_containers_.find(id);
        return (i == this->processed_containers_.end());
    }
}

bool ContainerTracker::ShouldProcessContainer(uint64_t id) {
    bool r = ShouldProcessContainerTest(id);

    TRACE("Should process container: container id " << id <<
            ", tracker " << DebugString());

    if (id > highest_seen_container_id_ || highest_seen_container_id_ == Storage::ILLEGAL_STORAGE_ADDRESS) {
        highest_seen_container_id_ = id;
    }
    return r;
}

bool ContainerTracker::AbortProcessingContainer(uint64_t id) {
    DEBUG("Abort processing container " << id);
    this->currently_processed_containers_.erase(id);
    return true;
}

bool ContainerTracker::ProcessingContainer(uint64_t id) {
    DEBUG("Processing container " << id);
    this->currently_processed_containers_.insert(id);
    return true;
}

bool ContainerTracker::ProcessedContainer(uint64_t id) {
    CHECK(id >= Container::kLeastValidContainerId, "Illegal container id");

    this->currently_processed_containers_.erase(id);

    DEBUG("Processed container: " << id << ", tracker " << this->DebugString());
    bool see_as_least = id == this->least_non_processed_container_id_;

    if (least_non_processed_container_id_ == Storage::ILLEGAL_STORAGE_ADDRESS &&
            id == Container::kLeastValidContainerId) {
        least_non_processed_container_id_ = Container::kLeastValidContainerId;
        see_as_least = true;
    }

    if (see_as_least) {
        DEBUG("container id " << id << " was least non processed container => Find next");
        // find next
        uint64_t j = this->least_non_processed_container_id_ + 1;
        for(;;j++) {
            if (this->ShouldProcessContainerTest(j)) {
                this->least_non_processed_container_id_ = j;
                break;
            }
        }

        set<uint64_t>::iterator k = processed_containers_.lower_bound(least_non_processed_container_id_);
        processed_containers_.erase(processed_containers_.begin(), k);
        DEBUG("New least non-processed container id: container id " << this->least_non_processed_container_id_ <<
                ", processed containers [" << Join(this->processed_containers_.begin(), this->processed_containers_.end(), ", ") << "]");

    } else {
        DEBUG("container id " << id << " was not least non processed container => Inserting into processed list")
                        this->processed_containers_.insert(id);

    }
    return true;
}

bool ContainerTracker::Reset() {
    TRACE("Reset: " << this->DebugString());
    if (this->processed_containers_.size() > 0) {
        set<uint64_t>::iterator i;
        uint64_t max = this->least_non_processed_container_id_;
        for (i = this->processed_containers_.begin(); i != this->processed_containers_.end(); i++) {
            if (*i > max) {
                max = *i;
            }
        }

        // Situation: least 216, highest 215
        // we do want the result least 216, highest 215 as least 217 highest 216 would render 216 invalid

        // Situation: least 210, highest 215
        // we want the result least 211, highest 215
        if (least_non_processed_container_id_ <= highest_seen_container_id_) {
            this->least_non_processed_container_id_ = (max + 1);
        }

        DEBUG("reset: least non processed container: " << this->least_non_processed_container_id_ <<
                ", max processed container id " << max);
    } else {
        DEBUG("reset: least non processed container: " << this->least_non_processed_container_id_);
    }
    this->processed_containers_.clear();
    return true;
}

bool ContainerTracker::ParseFrom(const ContainerTrackerData& data) {
    if (data.container_id_size() > 0) {
        this->least_non_processed_container_id_ = data.container_id(0);
    }
    if (data.has_highest_seen_container_id()) {
        this->highest_seen_container_id_ = data.highest_seen_container_id();
    } else {
        // fallback mode
        if (data.container_id_size() > 0) {
            this->highest_seen_container_id_ = data.container_id(data.container_id_size() - 1);
        }
    }
    for (int i = 1; i < data.container_id_size(); i++) {
        this->processed_containers_.insert(data.container_id(i));
    }
    TRACE("Parsed tracker: " << this->DebugString());
    return true;
}

bool ContainerTracker::SerializeTo(ContainerTrackerData* data) const {
    CHECK(data, "container tracker data not set");

    TRACE("Serialize tracker: " << this->DebugString());
    if (this->least_non_processed_container_id_ != Storage::ILLEGAL_STORAGE_ADDRESS) {
        data->add_container_id(this->least_non_processed_container_id_);
    }
    set<uint64_t>::iterator i;
    for (i = this->processed_containers_.begin(); i != this->processed_containers_.end(); i++) {
        data->add_container_id(*i);
    }
    data->set_highest_seen_container_id(highest_seen_container_id_);
    return true;
}

uint64_t ContainerTracker::GetNextProcessingContainer() const {
    uint64_t id = GetLeastNonProcessedContainerId();
    for (;; id++) {
        if (this->ShouldProcessContainerTest(id) && this->currently_processed_containers_.find(id) == this->currently_processed_containers_.end()) {
            break;
        }
    }
    DEBUG("Next processing container id " << id << ", highest seen container id " << highest_seen_container_id_);
    if (id > highest_seen_container_id_ || highest_seen_container_id_ == Storage::ILLEGAL_STORAGE_ADDRESS) {
        return Storage::ILLEGAL_STORAGE_ADDRESS;
    }
    return id;
}

}
