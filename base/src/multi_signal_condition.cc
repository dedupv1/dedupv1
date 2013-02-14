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

#include <base/multi_signal_condition.h>

#include <errno.h>

#include <base/logging.h>

LOGGER("MultiSignalCondition");

namespace dedupv1 {
namespace base {

MultiSignalCondition::MultiSignalCondition(uint32_t count) {
    this->count_ = count;
    this->current_ = 0;
    signaled_ = false;
}

MultiSignalCondition::~MultiSignalCondition() {
}

bool MultiSignalCondition::Wait() {
    CHECK(lock_.AcquireLock(), "Failed to acquire lock");

    while (!signaled_) {
        CHECK(this->condition_.ConditionWait(&lock_), "Failed to wait for condition");
    }
    CHECK(lock_.ReleaseLock(), "Failed to release lock");
    return true;
}

bool MultiSignalCondition::Signal() {
    CHECK(lock_.AcquireLock(), "Failed to acquire lock");
    this->current_++;
    if (this->current_ == this->count_) {
        // We have the right number of threads, unlock all
        signaled_ = true;
        this->condition_.Broadcast();
    }
    CHECK(lock_.ReleaseLock(), "Failed to release lock");
    return true;
}

}

}
