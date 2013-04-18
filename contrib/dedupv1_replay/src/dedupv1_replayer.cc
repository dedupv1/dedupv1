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

#include "dedupv1_replayer.h"

#include <iostream>

#include <core/block_index.h>
#include <core/block_mapping.h>
#include <core/chunk_index.h>
#include <core/container.h>
#include <core/container_storage.h>
#include <core/dedup_system.h>
#include <base/hashing_util.h>
#include <base/index.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <base/startup.h>
#include <base/memory.h>
#include <core/fingerprinter.h>
#include <base/bitutil.h>
#include <core/chunk.h>

#include <dedupv1.pb.h>

using std::set;
using std::vector;
using dedupv1::DedupSystem;
using dedupv1::StartContext;
using dedupv1d::Dedupv1d;
LOGGER("Dedupv1Replayer");

namespace dedupv1 {
namespace contrib {
namespace replay {

Dedupv1Replayer::Dedupv1Replayer() {
    system_ = NULL;
    started_ = false;
    gc_paused_ = false;
}

bool Dedupv1Replayer::Initialize(const std::string& filename) {
    CHECK(!started_, "Dedupv1 replayer already started");
    system_ = new Dedupv1d();

    CHECK(system_, "Error creating dedup system");
    CHECK(system_->Init(),"Error initializing dedup system");
    CHECK(system_->LoadOptions(filename), "Error loading options");

    CHECK(system_->OpenLockfile(), "Failed to acquire lock on lockfile");

    StartContext start_context(StartContext::NON_CREATE,
                               StartContext::DIRTY,
                               StartContext::NO_FORCE,
                               false);
    CHECK(system_->Start(start_context), "Failed to start dedupv1 system");
    // Will force system busy, so no bg replay will start
    CHECK(system_->dedup_system()->idle_detector()->ForceBusy(true), "Failed to force system busy");

    if (gc_paused_) {
        DEBUG("Will pause GC before running it.");
        CHECK(system_->dedup_system()->garbage_collector()->PauseProcessing(),
            "Failed to pause Garbage Collector");
    }

    CHECK(system_->dedup_system()->Run(), "Failed to run dedupv1 system");

    started_ = true;
    return true;
}

bool Dedupv1Replayer::PauseGC() {
    DEBUG("GC will be paused");
    gc_paused_ = true;
    if (started_) {
        DEBUG("GC is startet, so it will be paused");
        return system_->dedup_system()->garbage_collector()->PauseProcessing();
    }
    return true;
}

bool Dedupv1Replayer::UnPauseGC() {
    DEBUG("GC will not be paused");
    gc_paused_ = false;
    if (started_) {
        DEBUG("GC is startet, so it will be unpaused");
        return system_->dedup_system()->garbage_collector()->ResumeProcessing();
    }
    return true;
}

bool Dedupv1Replayer::Replay() {
    DCHECK(started_, "Dedupv1 replayer not started");
    DCHECK(system_ != NULL, "Dedup System is null");

    // The idea behind doing both replays (on in Start if the system is dirty) directly after each other is that it is easier
    // to program the background replay if the state is already in memory as it is during a usual replay
    // instead of having to think about an additional special case.

    CHECK(system_->dedup_system()->log()->PerformFullReplayBackgroundMode(), "Failed to perform full replay");

    return true;
}

bool Dedupv1Replayer::Close() {
    DEBUG("Closing dedupv1 replayer");
    if (system_) {
        CHECK(system_->Shutdown(dedupv1::StopContext::FastStopContext()), "Failed to start dedupv1 shutdown");
        CHECK(system_->Stop(), "Failed to stop dedupv1 system");
        CHECK(system_->Close(), "Failed to close system");
    }
    return true;
}

}
}
}
