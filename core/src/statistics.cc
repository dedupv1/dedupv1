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
#include <base/thread.h>

using std::string;
using std::map;
using dedupv1::base::make_option;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::Index;
using dedupv1::base::ThreadUtil;

LOGGER("Statistics");

namespace dedupv1 {

PersistStatistics::PersistStatistics() {
}

PersistStatistics::~PersistStatistics() {
}

bool StatisticProvider::PersistStatistics(string prefix, dedupv1::PersistStatistics* ps) {
    return true;
}

bool StatisticProvider::RestoreStatistics(string prefix, dedupv1::PersistStatistics* ps) {
    return true;
}

string StatisticProvider::PrintStatistics() {
    return "null";
}

string StatisticProvider::PrintProfile() {
    return "null";
}

string StatisticProvider::PrintLockStatistics() {
    return "null";
}

string StatisticProvider::PrintTrace() {
    return "null";
}

IndexPersistentStatistics::IndexPersistentStatistics() {
    index_ = NULL;
    started_ = false;
#ifdef DEDUPV1_CORE_TEST
    data_cleared_ = false;
#endif
}

bool IndexPersistentStatistics::Start(const dedupv1::StartContext& start_context) {
    CHECK(!started_, "Statistics already started");
    DEBUG("Starting persistent statistics");

    CHECK(index_, "Index not configured");
    CHECK(index_->Start(start_context), "Failed to start index");

    started_ = true;
    return true;
}

bool IndexPersistentStatistics::SetOption(const std::string& option_name, const std::string& option) {
    CHECK(!started_, "Statistics already started");
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

IndexPersistentStatistics::~IndexPersistentStatistics() {
    if (index_) {
        delete index_;
        index_ = NULL;
    }
}

bool IndexPersistentStatistics::Persist(const std::string& key, const google::protobuf::Message& message) {
    CHECK(started_, "Statistics not started");
#ifdef DEDUPV1_CORE_TEST
    if (data_cleared_) {
        return true;
    }
#endif
    DEBUG("Persisting statistics: key " << key << ", message " << message.ShortDebugString());

    CHECK(this->index_->Put(key.data(), key.size(), message) != PUT_ERROR,
        "Failed to write stats data: key " << key << ", message " << message.ShortDebugString());
    return true;
}

bool IndexPersistentStatistics::Restore(const std::string& key, google::protobuf::Message* message) {
    CHECK(started_, "Statistics not started");
#ifdef DEDUPV1_CORE_TEST
    if (data_cleared_) {
        return true;
    }
#endif
    CHECK(this->index_->Lookup(key.data(), key.size(), message) != LOOKUP_ERROR, "Failed to lookup stats data: key " << key);

    DEBUG("Restoring statistics: key " << key << ", message " << message->ShortDebugString());
    return true;
}

dedupv1::base::Option<bool> IndexPersistentStatistics::Exists(const std::string& key) {
    CHECK(started_, "Statistics not started");

    dedupv1::base::lookup_result lr = this->index_->Lookup(key.data(), key.size(), NULL);
    CHECK(lr != LOOKUP_ERROR, "Failed to lookup key " << key);
    return make_option(lr == dedupv1::base::LOOKUP_FOUND);
}

#ifdef DEDUPV1_CORE_TEST
void IndexPersistentStatistics::ClearData() {
    data_cleared_ = true;
    ThreadUtil::Sleep(1, ThreadUtil::SECONDS);
    if (this->index_) {
        delete index_;
        this->index_ = NULL;
    }
}
#endif

bool MemoryPersistentStatistics::Persist(const std::string& key, const google::protobuf::Message& message) {
    string s;
    CHECK(message.SerializePartialToString(&s), "Failed to serialize data");
    stats_[key] = s;
    return true;
}

bool MemoryPersistentStatistics::Restore(const std::string& key, google::protobuf::Message* message) {
    CHECK(message, "Message not set");
    map<string, string>::iterator i = stats_.find(key);
    if (i == stats_.end()) {
        return true;
    }
    CHECK(message->ParseFromString(i->second), "Failed to parse data");
    return true;
}

dedupv1::base::Option<bool> MemoryPersistentStatistics::Exists(const std::string& key) {
    map<string, string>::iterator i = stats_.find(key);
    return make_option(i != stats_.end());
}

}
