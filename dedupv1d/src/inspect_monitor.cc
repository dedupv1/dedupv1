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

#include "inspect_monitor.h"

#include <sstream>

#include <core/dedup_system.h>
#include <base/logging.h>
#include <core/container.h>
#include <core/storage.h>
#include <core/container_storage.h>
#include <base/strutil.h>
#include <core/log.h>
#include <core/log_consumer.h>
#include <core/block_index.h>
#include <core/block_mapping.h>
#include <core/chunk_index.h>
#include <core/chunk_mapping.h>
#include <core/dedupv1_scsi.h>
#include <base/memory.h>
#include <base/crc32.h>
#include "dedupv1d.h"
#include "monitor.h"
#include "default_monitor.h"

#include <time.h>

using dedupv1::base::strutil::To;
using std::string;
using std::pair;
using std::stringstream;
using std::endl;
using std::list;
using std::vector;
using std::set;
using dedupv1::base::make_bytestring;
using dedupv1::base::CRC;
using dedupv1::DedupSystem;
using dedupv1::Fingerprinter;

using std::make_pair;

LOGGER("InspectMonitorAdapter");

namespace dedupv1d {
namespace monitor {

InspectMonitorAdapter::InspectMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

InspectMonitorAdapter::~InspectMonitorAdapter() {
}

MonitorAdapterRequest* InspectMonitorAdapter::OpenRequest() {
    return new InspectMonitorAdapterRequest(this);
}

InspectMonitorAdapterRequest::InspectMonitorAdapterRequest(InspectMonitorAdapter* adapter) : inspect_(adapter->ds_){
    this->adapter_ = adapter;
}

InspectMonitorAdapterRequest::~InspectMonitorAdapterRequest() {
}

bool InspectMonitorAdapterRequest::ParseParam(const string& key, const string& value) {
    this->options_.push_back( make_pair(key,value));

    DEBUG(key << "=" << value);
    return true;
}

string InspectMonitorAdapterRequest::Monitor() {
    stringstream sstr;

    DEBUG("Monitor: " << options_.size());

    if (options_.size() != 1) {
        sstr << "{\"ERROR\": \"Illegal option\"}";
    } else {

        string option = this->options_[0].second;
        if (options_[0].first == "container") {
            CHECK_RETURN_JSON(To<uint64_t>(option).valid(), "Illegal option: " << option);
            uint64_t container_id = To<uint64_t>(this->options_[0].second).value();
            sstr << this->inspect_.ShowContainer(container_id, NULL);
            options_.clear();
        } else if (options_[0].first == "container-head") {
            CHECK_RETURN_JSON(To<uint64_t>(option).valid(), "Illegal option: " << option);
            uint64_t container_id = To<uint64_t>(this->options_[0].second).value();
            sstr << this->inspect_.ShowContainerHeader(container_id);
            options_.clear();
        } else if (options_[0].first == "log" && options_[0].second == "info") {
            sstr << this->inspect_.ShowLogInfo();
            options_.clear();
        } else if (options_[0].first == "log") {
            CHECK_RETURN_JSON(To<uint64_t>(option).valid(), "Illegal option: " << option);
            uint64_t log_position = To<uint64_t>(this->options_[0].second).value();
            sstr << this->inspect_.ShowLog(log_position);
            options_.clear();
        } else if (options_[0].first == "block") {
            CHECK_RETURN_JSON(To<uint64_t>(option).valid(), "Illegal option: " << option);
            uint64_t block_id = To<uint64_t>(this->options_[0].second).value();
            sstr << this->inspect_.ShowBlock(block_id);
            options_.clear();
        } else if (options_[0].first == "chunk") {
            string hex_fp = this->options_[0].second;
            bytestring fp;
            CHECK_RETURN_JSON(Fingerprinter::FromDebugString(hex_fp, &fp),
                "Failed to parse fingerprint: " << hex_fp);
            CHECK_RETURN_JSON(fp.size() == 20, "Illegal fp size");
            sstr << this->inspect_.ShowChunk(fp);
            options_.clear();
        } else {
            sstr << "{\"ERROR\": \"Illegal option\"}";
        }
    }
    return sstr.str();
}

}
}
