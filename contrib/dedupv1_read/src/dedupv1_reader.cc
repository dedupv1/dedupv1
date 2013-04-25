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
#include "dedupv1_reader.h"

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
#include "dedupv1d_volume_info.h"
#include "dedupv1d_volume.h"
#include <dedupv1.pb.h>

using std::set;
using std::vector;
using dedupv1::DedupSystem;
using dedupv1::StartContext;
using dedupv1d::Dedupv1d;

LOGGER("Dedupv1Reader");

namespace dedupv1 {
namespace contrib {
namespace reader {

Dedupv1Reader::Dedupv1Reader() {
    system_ = NULL;
    started_ = false;
}

bool Dedupv1Reader::Initialize(const std::string& filename) {
    CHECK(!started_, "Dedupv1 reader already started");
    system_ = new Dedupv1d();
    CHECK(system_->LoadOptions(filename), "Error loading options");

    CHECK(system_->OpenLockfile(), "Failed to acquire lock on lockfile");

    StartContext start_context(StartContext::NON_CREATE, StartContext::DIRTY, StartContext::NO_FORCE, false);
    CHECK(system_->Start(start_context), "Failed to start dedupv1 system");

    CHECK(system_->Run(), "Failed to run dedupv1 system");
    started_ = true;
    return true;
}

bool Dedupv1Reader::Read(uint32_t volume_id, uint64_t offset, uint64_t size) {
    DCHECK(started_, "Dedupv1 reader not started");
    DCHECK(system_ != NULL, "Dedup System is null");

    dedupv1d::Dedupv1dVolume* volume = system_->volume_info()->FindVolume(volume_id, NULL);
    CHECK(volume, "Volume " << volume_id << " not found");

    if (size == 0) {
        size = volume->logical_size();
    }

    DEBUG("Read volume: " << volume_id << ", offset " << offset << ", size " << size);

    uint64_t request_size = 0;
    uint64_t request_offset = offset;
    dedupv1::base::ScopedArray<byte> buffer(new byte[system_->dedup_system()->block_size()]);
    while (size > 0) {
        request_size = size;
        if (request_size > system_->dedup_system()->block_size()) {
            request_size = system_->dedup_system()->block_size();
        }

        TRACE("Make request: request offset " << request_offset << ", request size " << request_size);
        dedupv1::scsi::ScsiResult result = volume->MakeRequest(REQUEST_READ, request_offset, request_size, buffer.Get(), NO_EC);
        if (!result) {
            ERROR("Failed to read: offset " << request_offset << ", request size " << request_size << ", result " << result.DebugString());
            break;
        }

        uint32_t write_size = request_size;
        while (write_size > 0) {
            ssize_t w = write(1, buffer.Get(), request_size);
            CHECK(w > 0, "Failed to output: offset " << request_offset <<
                ", request size " << request_size <<
                ", result " << strerror(errno));
            write_size -= w;
        }
        request_offset += request_size;
        size -= request_size;
    }

    if (size > 0) {
        ERROR("Read aborted");
        return false;
    }
    return true;
}

bool Dedupv1Reader::Close() {
    DEBUG("Closing dedupv1 reader");
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
