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

#include <core/statistics.h>
#include <base/logging.h>
#include <core/info_store.h>

using std::string;
using std::map;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::Index;
using dedupv1::base::lookup_result;

LOGGER("InfoStore");

namespace dedupv1 {

InfoStore::InfoStore() {
}

InfoStore::~InfoStore() {
}

bool InfoStore::Start(const dedupv1::StartContext& start_context) {
    return true;
}

bool InfoStore::SetOption(const std::string& option_name, const std::string& option) {
    ERROR("Invalid option: " << option_name);
    return false;
}

#ifdef DEDUPV1_CORE_TEST
void InfoStore::ClearData() {
}
#endif

IndexInfoStore::IndexInfoStore() {
    index_ = NULL;
    started_ = false;
#ifdef DEDUPV1_CORE_TEST
    data_cleared_ = false;
#endif
}

IndexInfoStore::~IndexInfoStore() {
  if (index_) {
    delete index_;
  }
}

bool IndexInfoStore::Start(const dedupv1::StartContext& start_context) {
    CHECK(!started_, "Info store already started");
    DEBUG("Starting info store");

    CHECK(index_, "Index not configured");
    CHECK(index_->Start(start_context), "Failed to start index");

    started_ = true;
    return true;
}

bool IndexInfoStore::SetOption(const std::string& option_name, const std::string& option) {
    CHECK(!started_, "Info store already started");
    if (option_name == "type") {
        CHECK(this->index_ == NULL, "Index type already set");
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Cannot create index: " << option);
        this->index_ = index->AsPersistentIndex();
        CHECK(this->index_, "Info should be persistent");
        return true;
    }
    CHECK(this->index_, "Index not set");
    CHECK(this->index_->SetOption(option_name, option), "Failed to configure index");
    return true;
}

bool IndexInfoStore::PersistInfo(std::string key, const google::protobuf::Message& message) {
    CHECK(started_, "Info store not started");
#ifdef DEDUPV1_CORE_TEST
    if (data_cleared_) {
        return true;
    }
#endif
    DEBUG("Persisting info: key " << key << ", message " << message.ShortDebugString());

    CHECK(this->index_->Put(key.data(), key.size(), message) != PUT_ERROR,
        "Failed to write info data: key " << key << ", message " << message.ShortDebugString());
    return true;
}

lookup_result IndexInfoStore::RestoreInfo(std::string key, google::protobuf::Message* message) {
    CHECK_RETURN(started_, LOOKUP_ERROR, "Info store not started");
#ifdef DEDUPV1_CORE_TEST
    if (data_cleared_) {
        return LOOKUP_NOT_FOUND;
    }
#endif
    lookup_result lr = this->index_->Lookup(key.data(), key.size(), message);
    CHECK_RETURN(lr != LOOKUP_ERROR, LOOKUP_ERROR, "Failed to lookup info data: key " << key);
    if (lr == LOOKUP_NOT_FOUND) {
        DEBUG("Restoring info: key " << key << ", message <not found>");
        return lr;
    }
    DEBUG("Restoring info: key " << key << ", message " << message->ShortDebugString());
    return LOOKUP_FOUND;
}

#ifdef DEDUPV1_CORE_TEST
void IndexInfoStore::ClearData() {
    if (this->index_) {
        delete index_;
        this->index_ = NULL;
    }
    data_cleared_ = true;
}
#endif

bool MemoryInfoStore::PersistInfo(std::string key, const google::protobuf::Message& message) {
    string s;
    CHECK(message.SerializePartialToString(&s), "Failed to serialize data");
    stats_[key] = s;
    return true;
}

lookup_result MemoryInfoStore::RestoreInfo(std::string key, google::protobuf::Message* message) {
    CHECK_RETURN(message, LOOKUP_ERROR, "Message not set");
    map<string, string>::iterator i = stats_.find(key);
    if (i == stats_.end()) {
        DEBUG("Restoring info: key " << key << ", message <not found>");
        return LOOKUP_NOT_FOUND;
    }
    CHECK_RETURN(message->ParseFromString(i->second), LOOKUP_ERROR, "Failed to parse data");
    DEBUG("Restoring info: key " << key << ", message " << message->ShortDebugString());
    return LOOKUP_FOUND;
}

}
