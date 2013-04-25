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

#include <core/request.h>

#include <sstream>

#include <base/logging.h>
#include <core/block_mapping.h>

LOGGER("Request");

namespace dedupv1 {

Request::Request(enum request_type request_type,
                 uint64_t block_id,
                 uint64_t offset,
                 uint64_t size,
                 byte* buffer,
                 uint32_t block_size) :
    request_type_(request_type),
    block_id_(block_id), offset_(offset), size_(size), buffer_(buffer), block_size_(block_size) {
}

bool Request::IsValid() const {
    CHECK(block_id_ != dedupv1::blockindex::BlockMapping::ILLEGAL_BLOCK_ID, "Illegal block id");
    CHECK(buffer_, "Data not set");
    CHECK(block_size_ > 0, "Illegal block size: block size" << block_size_);
    CHECK(offset_ + size_ <= block_size_,
        "Illegal offset size: offset " << offset_ << ", size " << size_ << ", block size " << block_size_);
    CHECK(offset_ % 512 == 0, "Illegal offset: offset " << offset_);
    CHECK(size_ % 512 == 0, "Illegal size: size " << size_);
    CHECK(size_ > 0, "Illegal size: size " << size_);
    return true;
}

std::string Request::DebugString() {
    std::stringstream sstr;
    sstr << "[" << (request_type_ == REQUEST_READ ? "r" : "w")  <<
    ", block id " << block_id_ <<
    ", offset " << offset_ <<
    ", size " << size_ << "]";
    return sstr.str();
}

RequestStatistics::RequestStatistics() {
}

void RequestStatistics::Start(enum ProfileComponent c) {
    this->start_tick_map_[c] = tbb::tick_count::now();
}

void RequestStatistics::Finish(enum ProfileComponent c) {
    tbb::concurrent_unordered_map<enum ProfileComponent, tbb::tick_count>::iterator i = this->start_tick_map_.find(c);
    if (i == this->start_tick_map_.end()) {
        return;
    }
    tbb::tick_count start = i->second;
    double t = (tbb::tick_count::now() - start).seconds() * 1000;
    this->latency_map_[c] += t;
}

uint64_t RequestStatistics::latency(enum ProfileComponent c) {
    return this->latency_map_[c];
}

std::string RequestStatistics::DebugString() {
    std::stringstream s;
    s << "[";
    s << "total " << latency(TOTAL);
    s << ", waiting " << latency(WAITING);
    s << ", processing " << latency(PROCESSING);
    s << ", chunking " << latency(CHUNKING);
    s << ", fingerprinting " << latency(FINGERPRINTING);
    s << ", filter chain " << latency(FILTER_CHAIN);
    s << ", open request handling " << latency(OPEN_REQUEST_HANDLING);
    s << "]";
    return s.str();
}

}
