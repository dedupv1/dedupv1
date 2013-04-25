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

#include <core/storage.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <base/bitutil.h>
#include <base/logging.h>

using std::map;
using std::string;
using dedupv1::log::Log;
using dedupv1::IdleDetector;

LOGGER("Storage");

namespace dedupv1 {
namespace chunkstore {

MetaFactory<Storage> Storage::factory_("Storage", "storage");

MetaFactory<Storage>& Storage::Factory() {
    return factory_;
}

const uint64_t Storage::EMPTY_DATA_STORAGE_ADDRESS = (uint64_t) -2;
const uint64_t Storage::ILLEGAL_STORAGE_ADDRESS = (uint64_t) -1;

Storage::Storage() {
}

Storage::~Storage() {
}

bool Storage::Start(const StartContext& start_context, DedupSystem* system) {
    return true;
}

bool Storage::Run() {
    return true;
}

bool Storage::Stop(const dedupv1::StopContext& stop_context) {
    return true;
}

bool Storage::Close() {
    delete this;
    return true;
}

bool Storage::Flush(dedupv1::base::ErrorContext* ec) {
    return true;
}

bool Storage::SetOption(const string& option_name, const string& option) {
    ERROR("Illegal option: " << option_name);
    return false;
}

bool Storage::CheckIfFull() {
    return false;
}

bool Storage::DeleteChunk(uint64_t address, const byte* key, size_t key_size, dedupv1::base::ErrorContext* ec) {
    std::list<bytestring> key_list;
    bytestring s;
    s.assign(key, key_size);
    key_list.push_back(s);
    return DeleteChunks(address, key_list, ec);
}


bool Storage::IsValidAddress(uint64_t address, bool allow_empty) {
    return (address != 0) &&
           (address != Storage::ILLEGAL_STORAGE_ADDRESS) &&
           (allow_empty || address != Storage::EMPTY_DATA_STORAGE_ADDRESS);
}

}
}
