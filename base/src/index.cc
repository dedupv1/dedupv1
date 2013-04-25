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
#include <base/index.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include <base/base.h>
#include <base/bitutil.h>
#include <base/logging.h>

LOGGER("Index");

using std::map;
using std::string;
using google::protobuf::Message;

namespace dedupv1 {
namespace base {

MetaFactory<Index> Index::factory_("Index", "index");

MetaFactory<Index>& Index::Factory() {
    return factory_;
}

Index::Index(bool persistent, int capabilities) {
    this->persistent_ = persistent;
    capabilities_ = capabilities;

}

Index::~Index() {
}

IndexIterator* Index::CreateIterator() {
    return NULL;
}

IndexCursor::IndexCursor() {
}

IndexCursor::~IndexCursor() {
}

IndexIterator::IndexIterator() {
}

IndexIterator::~IndexIterator() {
}

MemoryIndex::MemoryIndex(int capabilities)
    : Index(false, capabilities) {
}

PersistentIndex::PersistentIndex(int capabilities)
    : Index(true, capabilities | HAS_ITERATOR) {
}

PersistentIndex* Index::AsPersistentIndex() {
    if (!IsPersistent()) {
        return NULL;
    }
    return static_cast<PersistentIndex*>(this);
}

MemoryIndex* Index::AsMemoryIndex() {
    if (IsPersistent()) {
        return NULL;
    }
    return static_cast<MemoryIndex*>(this);
}

bool Index::SetOption(const string& option_name, const string& option) {
    ERROR("Illegal option: " << option_name);
    (void) option;

    return false;
}

bool PersistentIndex::SetOption(const string& option_name, const string& option) {
    (void) option;
    // no persistent index should fail if configured with one of
    // these options even if they do not make any use of it.
    if (option_name == "sync" ||
        option_name == "lazy-sync" ||
        option_name == "max-key-size" ||
        option_name == "min-key-size" ||
        option_name == "max-value-size" ||
        option_name == "min-value-size") {
        return true;
    }
    ERROR("Illegal option: " << option_name);
    return false;
}

bool PersistentIndex::SupportsCursor() {
    return false;
}

IndexCursor* PersistentIndex::CreateCursor() {
    return NULL;
}

bool Index::IsPersistent() {
    return this->persistent_;
}

bool Index::Close() {
    delete this;
    return true;
}

std::string Index::PrintLockStatistics() {
    return "null";
}

std::string Index::PrintProfile() {
    return "null";
}

std::string Index::PrintTrace() {
    return "null";
}

put_result Index::PutIfAbsent(const void* key, size_t key_size,
                              const Message& message) {
    ERROR("Index doesn't support put-if-absent operations.");
    return PUT_ERROR;
}

enum put_result Index::RawPutIfAbsent(
    const void* key, size_t key_size,
    const void* value, size_t value_size) {
    ERROR("Index doesn't support raw usage");
    return PUT_ERROR;
}

enum put_result Index::RawPut(
    const void* key, size_t key_size,
    const void* value, size_t value_size) {
    ERROR("Index doesn't support raw usage");
    return PUT_ERROR;
}

enum lookup_result Index::RawLookup(const void* key, size_t key_size,
                                    void* value, size_t* value_size) {
    ERROR("Index doesn't support raw usage");
    return LOOKUP_ERROR;
}

enum put_result Index::CompareAndSwap(const void* key, size_t key_size,
                                      const google::protobuf::Message& message,
                                      const google::protobuf::Message& compare_message,
                                      google::protobuf::Message* result_message) {
    ERROR("Index doesn't support compare-and-swap");
    return PUT_ERROR;
}

enum lookup_result PersistentIndex::LookupDirty(const void* key, size_t key_size,
                                                enum cache_lookup_method cache_lookup_type,
                                                enum cache_dirty_mode dirty_mode,
                                                google::protobuf::Message* message) {
    if (cache_lookup_type == CACHE_LOOKUP_ONLY) {
        return LOOKUP_NOT_FOUND;
    }
    return Lookup(key, key_size, message);
}

enum put_result PersistentIndex::PutDirty(const void* key, size_t key_size,
                                          const google::protobuf::Message& message, bool pin) {
    CHECK_RETURN(!pin, PUT_ERROR, "Index doesn't support pinning");
    return Put(key, key_size, message);
}

enum put_result Index::RawPutBatch(
    const std::vector<std::tr1::tuple<bytestring, bytestring> >& data) {

    std::vector<std::tr1::tuple<bytestring, bytestring> >::const_iterator i;
    for (i = data.begin(); i != data.end(); i++) {
        put_result r = RawPut(
            std::tr1::get<0>(*i).data(),
            std::tr1::get<0>(*i).size(),
            std::tr1::get<1>(*i).data(),
            std::tr1::get<1>(*i).size()
            );
        if (r == PUT_ERROR) {
            return r;
        }
    }
    return PUT_OK;
}

enum put_result Index::PutBatch(
    const std::vector<std::tr1::tuple<bytestring, const google::protobuf::Message*> >& data) {
    std::vector<std::tr1::tuple<bytestring, const google::protobuf::Message*> >::const_iterator i;
    for (i = data.begin(); i != data.end(); i++) {
        put_result r = Put(
            std::tr1::get<0>(*i).data(),
            std::tr1::get<0>(*i).size(),
            *(std::tr1::get<1>(*i)));
        if (r == PUT_ERROR) {
            return r;
        }
    }
    return PUT_OK;
}

bool PersistentIndex::DropAllPinned() {
    ERROR("Index doesn't support pinning");
    return false;
}

bool PersistentIndex::PersistAllDirty() {
    return true;
}

enum put_result PersistentIndex::EnsurePersistent(const void* key, size_t key_size, bool* pinned) {
    return PUT_OK;
}

bool PersistentIndex::IsWriteBackCacheEnabled() {
    return false;
}

uint64_t PersistentIndex::GetEstimatedMaxCacheItemCount() {
    return 0;
}

bool PersistentIndex::TryPersistDirtyItem(
    uint32_t max_batch_size,
    uint64_t* resume_handle,
    bool* persisted) {
    DCHECK(persisted, "Persisted not set");
    *persisted = false;
    return true;
}

enum lookup_result PersistentIndex::ChangePinningState(const void* key, size_t key_size, bool new_pin_state) {
    ERROR("Index doesn't support pinning");
    return LOOKUP_ERROR;
}

uint64_t PersistentIndex::GetDirtyItemCount() {
    return 0;
}

uint64_t PersistentIndex::GetTotalItemCount() {
    return GetItemCount();
}

bool Index::HasCapability(enum index_capability cap) {
    return (capabilities_ & cap) == cap;
}

uint64_t IDBasedIndex::GetEstimatedMaxItemCount() {
    return this->GetLimitId();
}

IDBasedIndex::IDBasedIndex(int capabilities) : PersistentIndex(capabilities) {
}

IDBasedIndex::~IDBasedIndex() {
}

}
}

