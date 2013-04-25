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

#include <sstream>

#include <core/dedup.h>
#include <base/logging.h>
#include <core/container.h>
#include <core/log_consumer.h>
#include <base/locks.h>
#include <core/container_storage.h>
#include <core/container_storage_gc.h>
#include <base/strutil.h>
#include <base/fault_injection.h>

#include "dedupv1.pb.h"

#include <core/log.h>

using std::string;
using std::pair;
using std::list;
using std::make_pair;
using std::stringstream;
using dedupv1::base::strutil::To;
using dedupv1::base::ScopedLock;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::put_result;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::Index;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using tbb::recursive_mutex;
using dedupv1::base::delete_result;
using dedupv1::base::DELETE_ERROR;
using dedupv1::base::ProfileTimer;

LOGGER("ContainerGC");

namespace dedupv1 {
namespace chunkstore {

ContainerGCStrategy::ContainerGCStrategy() {
}

ContainerGCStrategy::~ContainerGCStrategy() {
}

bool ContainerGCStrategy::Start(const StartContext& start_context, ContainerStorage* storge) {
    return true;
}

bool ContainerGCStrategy::Stop(const dedupv1::StopContext& stop_context) {
    return true;
}

bool ContainerGCStrategy::Close() {
    delete this;
    return true;
}

bool ContainerGCStrategy::OnIdle() {
    return true;
}

bool ContainerGCStrategy::OnDeleteContainer(const Container& container) {
    return true;
}

bool ContainerGCStrategy::OnMove(const ContainerMoveEventData& data) {
    return true;
}

bool ContainerGCStrategy::OnMerge(const ContainerMergedEventData& data) {
    return true;
}

bool ContainerGCStrategy::OnStoragePressure() {
    return true;
}

#ifdef DEDUPV1_CORE_TEST
void ContainerGCStrategy::ClearData() {
}
#endif

ContainerGCStrategyFactory ContainerGCStrategyFactory::factory;

bool ContainerGCStrategyFactory::Register(const string& name, ContainerGCStrategy*(*factory)(void)) {
    factory_map[name] = factory;
    return true;
}

ContainerGCStrategyFactory::ContainerGCStrategyFactory() {

}

ContainerGCStrategy* ContainerGCStrategyFactory::Create(const string& name) {
    ContainerGCStrategy* (*f)(void) = factory.factory_map[name];
    if (f) {
        ContainerGCStrategy* gc = f();
        CHECK_RETURN(gc, NULL, "Cannot create new gc strategy" << name);
        return gc;
    }
    ERROR("Cannot find gc strategy: " << name);
    return NULL;
}

void GreedyContainerGCStrategy::RegisterGC() {
    ContainerGCStrategyFactory::GetFactory()->Register("greedy", &GreedyContainerGCStrategy::CreateGC);
}

ContainerGCStrategy* GreedyContainerGCStrategy::CreateGC() {
    return new GreedyContainerGCStrategy();
}

GreedyContainerGCStrategy::GreedyContainerGCStrategy() : touched_set_(0) {
    this->merge_candidates_ = NULL;
    this->merge_candidate_data_size_threshold_ = 0;
    this->merge_candidate_item_count_threshold_ = 0;
    this->bucket_size_ = kDefaultBucketSize;
    this->storage_ = NULL;
    this->started_ = false;
    this->eviction_timeout_ = 5;
    merge_candidate_count_ = 0;
}

GreedyContainerGCStrategy::~GreedyContainerGCStrategy() {

}

bool GreedyContainerGCStrategy::Start(const StartContext& start_context, ContainerStorage* storage) {
    CHECK(this->started_ == false, "GC already started");
    CHECK(this->merge_candidates_, "GC not configured");
    CHECK(storage, "Storage not set");

    DEBUG("Starting greedy container storage gc");

    touched_set_.SetSeconds(this->eviction_timeout_);

    this->storage_ = storage;
    container_size_ = storage->GetContainerSize();
    container_data_size_ = storage->GetContainerSize() - Container::kMetaDataSize;

    if (merge_candidate_data_size_threshold_ == 0) {
        this->merge_candidate_data_size_threshold_ = (0.40 * container_data_size_);
    } else {
        CHECK(merge_candidate_data_size_threshold_ < container_size_,
            "Illegal threshold: threshold " << merge_candidate_data_size_threshold_ <<
            ", container size " << container_size_);
    }

    if (this->merge_candidate_item_count_threshold_ == 0) {
        this->merge_candidate_item_count_threshold_ = storage->GetMaxItemsPerContainer() * 0.4;
    }

    CHECK(this->merge_candidates_->Start(start_context), "Cannot start merge candidate index");

    this->maximal_bucket_ = (this->merge_candidate_data_size_threshold_ / this->bucket_size_) + 1;

    // recover merge candidate count
    lookup_result r = LOOKUP_NOT_FOUND;
    for (uint64_t bucket = 0; bucket <= this->maximal_bucket_; bucket++) {
        ContainerGreedyGCCandidateData candidate_data;
        r = this->merge_candidates_->Lookup(&bucket, sizeof(bucket), &candidate_data);
        CHECK(r != LOOKUP_ERROR, "Failed to lookup merge candidates");

        if (r == LOOKUP_FOUND) {
            merge_candidate_count_ += candidate_data.item_size();
        }
    }

    this->started_ = true;
    return true;
}

bool GreedyContainerGCStrategy::Close() {
    DEBUG("Closing greedy container storage gc");
    bool failed = false;
    if (this->merge_candidates_) {
        if (!this->merge_candidates_->Close()) {
            ERROR("Failed to close gc merge candidates index");
            failed = true;
        }
        this->merge_candidates_ = NULL;
    }
    if (!ContainerGCStrategy::Close()) {
        failed = true;
    }
    return !failed;
}

bool GreedyContainerGCStrategy::SetOption(const string& option_name, const string& option) {
    CHECK(this->started_ == false, "GC already started");
    if (option_name == "type") {
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Index creation failed: " << option);
        CHECK(index->IsPersistent(), "Index not persistent");
        this->merge_candidates_ = index->AsPersistentIndex();
        CHECK(this->merge_candidates_->SetOption("max-key-size", "8"), "Failed to set max key size");
        return true;
    }
    if (option_name == "threshold") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->merge_candidate_data_size_threshold_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "item-count-threshold") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->merge_candidate_item_count_threshold_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "bucket-size") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->bucket_size_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "eviction-timeout") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->eviction_timeout_ = To<uint32_t>(option).value();
        return true;
    }
    CHECK(this->merge_candidates_, "Merge candidate index not set");
    CHECK(this->merge_candidates_->SetOption(option_name, option), "Configuration failed: " << option_name << " - " << option);
    return true;
}

bool GreedyContainerGCStrategy::OnMerge(const ContainerMergedEventData& data) {
    ProfileTimer gc_timer(this->stats_.gc_time_);

    CHECK(this->started_, "GC not started");
    return ProcessCommit(data.new_primary_id(), data.new_item_count(), data.new_active_data_size(), false);
}

uint64_t GreedyContainerGCStrategy::GetBucket(uint64_t active_data_size) {
    if (this->bucket_size_ == 0) {
        return 0;
    }
    return active_data_size / this->bucket_size_;
}

bool GreedyContainerGCStrategy::ProcessCommit(uint64_t primary_container_id, uint32_t item_count, uint32_t active_data_size, bool new_commit) {
    recursive_mutex::scoped_lock scoped_lock(this->lock_);

    TRACE("Process commit: " <<
        "container id " << primary_container_id <<
        ", item count " << item_count <<
        ", active data size " << active_data_size);

    if (active_data_size > this->merge_candidate_data_size_threshold_) {
        TRACE("Container is no merge candidate: container id " << primary_container_id <<
            ", threshold " << this->merge_candidate_data_size_threshold_);
        return true;
    }
    if (item_count > this->merge_candidate_item_count_threshold_) {
        TRACE("Container is no merge candidate: container id " << primary_container_id <<
            ", item threshold " << this->merge_candidate_item_count_threshold_);
        return true;
    }

    // container is a merge candidate

    if (this->eviction_timeout_ > 0) {
        touched_set_.Insert(primary_container_id);
    }

    ContainerGreedyGCCandidateData candidate_data;
    uint64_t data_size_bucket = GetBucket(active_data_size);

    DEBUG("Insert merge candidate: " <<
        "container id " << primary_container_id <<
        ", bucket " << data_size_bucket <<
        ", threshold " << this->merge_candidate_data_size_threshold_);

    ContainerGreedyGCCandidateItemData* item = NULL;
    enum lookup_result r = merge_candidates_->Lookup(&data_size_bucket, sizeof(data_size_bucket), &candidate_data);
    CHECK(r != LOOKUP_ERROR, "Failed to lookup merge candidate info: data size bucket " << data_size_bucket);
    if (r == LOOKUP_FOUND) {
        // we found the data size bucket

        for (int i = 0; i < candidate_data.item_size(); i++) {
            if (candidate_data.mutable_item(i)->address() == primary_container_id) {
                item = candidate_data.mutable_item(i);
                break;
            }
        }
    } else {
        TRACE("Found no merge candidate bucket for container: " <<
            "container id " << primary_container_id <<
            ", total candidate count " << merge_candidates_->GetItemCount());
    }

    // r == LOOKUP_NOT_FOUND or similar
    if (item == NULL) {
        item = candidate_data.add_item();
        merge_candidate_count_++; // we have a new item
    }
    DCHECK(item, "Item not set");
    // ok, we have a new merge candidate container, add it
    item->set_address(primary_container_id);
    item->set_active_data_size(active_data_size);
    item->set_active_item_count(item_count);
    CHECK(merge_candidates_->Put(&data_size_bucket, sizeof(data_size_bucket), candidate_data), "Cannot store merge candidate data");

    return true;
}

bool GreedyContainerGCStrategy::OnCommit(const ContainerCommittedEventData& data) {
    ProfileTimer gc_timer(this->stats_.gc_time_);

    CHECK(this->started_, "GC not started");
    return ProcessCommit(data.container_id(), data.item_count(), data.active_data_size(), true);
}

bool GreedyContainerGCStrategy::OnDeleteContainer(const ContainerDeletedEventData& data) {
    return true;
}

bool GreedyContainerGCStrategy::OnMove(const ContainerMoveEventData& data) {
    ProfileTimer gc_timer(this->stats_.gc_time_);

    uint64_t container_id = data.container_id();
    uint64_t active_data_size = data.active_data_size();
    uint64_t old_active_data_size = data.old_active_data_size();
    uint64_t item_count = data.item_count();

    recursive_mutex::scoped_lock scoped_lock(this->lock_);
    CHECK(this->started_, "GC not started");
    if (this->storage_->IsCommitted(container_id) != STORAGE_ADDRESS_COMMITED) {
        return true; // greedy gc doesn't care about non-committed containers
    }
    // the container is committed
    if (active_data_size > this->merge_candidate_data_size_threshold_) {
        TRACE("Container is no merge candidate: " << data.DebugString() <<
            ", threshold " << this->merge_candidate_data_size_threshold_);
        return true;
    }
    if (item_count > this->merge_candidate_item_count_threshold_) {
        TRACE("Container is no merge candidate: " << data.DebugString() <<
            ", item threshold " << this->merge_candidate_item_count_threshold_);
        return true;
    }
    // container is a merge candidate
    if (this->eviction_timeout_ > 0) {
        touched_set_.Insert(container_id);
    }

    uint64_t old_data_size_bucket = GetBucket(old_active_data_size);
    uint64_t data_size_bucket = GetBucket(active_data_size);

    if (old_data_size_bucket != data_size_bucket) {
        // the data size bucket changed => an entry about that container may be stored in that bucket.

        ContainerGreedyGCCandidateData old_candidate_data;
        // delete the old entry, the container has moved between the buckets
        enum lookup_result r = merge_candidates_->Lookup(&old_data_size_bucket, sizeof(data_size_bucket), &old_candidate_data);
        CHECK(r != LOOKUP_ERROR, "Failed to lookup merge candidate info");
        if (r == LOOKUP_FOUND) {

            bool found_in_old_bucket = false;
            // search for the container id and remove the entry
            for (int i = 0; i < old_candidate_data.item_size(); i++) {
                ContainerGreedyGCCandidateItemData* item = old_candidate_data.mutable_item(i);
                if (item->address() == container_id) {
                    // a protobuf repeated type doesn't allow to remove elements from the middle
                    // of the variable, but we can to something similar via swapping
                    old_candidate_data.mutable_item()->SwapElements(i, old_candidate_data.item_size() - 1);
                    old_candidate_data.mutable_item()->RemoveLast();
                    DEBUG("Delete from merge candidate: " <<
                        "event data " << data.ShortDebugString() <<
                        ", old active data size " << old_active_data_size <<
                        ", candidate data " << old_candidate_data.ShortDebugString() <<
                        ", bucket " << old_data_size_bucket);
                    found_in_old_bucket = true;
                    break;
                }
            }
            if (found_in_old_bucket) {
                // delete or update the other data size bucket
                if (old_candidate_data.item_size() == 0) {
                    CHECK(merge_candidates_->Delete(&old_data_size_bucket, sizeof(old_data_size_bucket)),
                        "Failed to delete merge candidate data: bucket " << old_data_size_bucket);
                } else {
                    CHECK(merge_candidates_->Put(&old_data_size_bucket, sizeof(old_data_size_bucket), old_candidate_data),
                        "Cannot store merge candidate data: " << old_candidate_data.ShortDebugString() <<
                        ", bucket " << old_data_size_bucket);
                }
            }
            // else: If we haven't found anything, there is no need to update or delete the index

        }
    }
    // now add or update the candidate data in the correct data size bucket
    ContainerGreedyGCCandidateData candidate_data;
    enum lookup_result r = merge_candidates_->Lookup(&data_size_bucket, sizeof(data_size_bucket), &candidate_data);
    CHECK(r != LOOKUP_ERROR, "Failed to lookup merge candidate info");
    bool found = false;
    if (r == LOOKUP_FOUND) {
        for (int i = 0; i < candidate_data.item_size(); i++) {
            ContainerGreedyGCCandidateItemData* item = candidate_data.mutable_item(i);
            if (item->address() == container_id) {
                found = true;
                item->set_active_data_size(data.active_data_size());
                item->set_active_item_count(data.item_count());
                DEBUG("Update merge candidate: " <<
                    "event data " << data.ShortDebugString() <<
                    ", candidate data " << candidate_data.ShortDebugString() <<
                    ", bucket " << data_size_bucket);
            }
        }
    } else {
        // if r == LOOKUP_NOT_FOUND:
        // We found no merge candidate bucket, the candidate_data is still empty
        // we will add the element to it and create a new bucket
        // this is done using the normal process in the !found branch
    }
    // the candidate data has been filled with the correct entries either because
    // it is new or the old entry has been correct
    if (!found) {
        // add a new entry if necessary
        ContainerGreedyGCCandidateItemData* item = candidate_data.add_item();
        item->set_address(container_id);
        item->set_active_data_size(data.active_data_size());
        item->set_active_item_count(data.item_count());
        DEBUG("Insert merge candidate: " <<
            "event data " << data.ShortDebugString() <<
            ", candidate data " << candidate_data.ShortDebugString() <<
            ", bucket " << data_size_bucket);
        merge_candidate_count_++;
    }
    CHECK(merge_candidates_->Put(&data_size_bucket, sizeof(data_size_bucket), candidate_data), "Cannot store merge candidate data");
    return true;
}

bool GreedyContainerGCStrategy::OnRead(const Container& container, const void* key, size_t key_size) {
    return true;
}

bool GreedyContainerGCStrategy::DeleteFromBucket(uint64_t bucket, uint64_t address) {
    ContainerGreedyGCCandidateData candidate_data;
    lookup_result r = this->merge_candidates_->Lookup(&bucket, sizeof(bucket), &candidate_data);
    CHECK(r == LOOKUP_FOUND, "Failed to lookup bucket " << bucket << ", result " << r);
    bool found = false;
    for (int i = 0; i < candidate_data.item_size(); i++) {
        if (candidate_data.item(i).address() == address) {
            found = true;
            // remove the entry
            if (candidate_data.item_size() > 1) {
                *candidate_data.mutable_item(i) = candidate_data.item(candidate_data.item_size() - 1);
                candidate_data.mutable_item()->RemoveLast();
            } else {
                candidate_data.mutable_item()->RemoveLast();
            }
        }
    }
    CHECK(found, "Failed to find address: address " << address << ", bucket " << candidate_data.ShortDebugString());

    merge_candidate_count_--;
    if (candidate_data.item_size() > 0) {
        put_result pr = this->merge_candidates_->Put(&bucket, sizeof(bucket), candidate_data);
        CHECK(pr != PUT_ERROR, "Failed to put bucket " << bucket << ", result " << r);
    } else {
        delete_result dr = this->merge_candidates_->Delete(&bucket, sizeof(bucket));
        CHECK(dr != DELETE_ERROR, "Failed to delete bucket " << bucket << ", result " << r);
    }
    return true;
}

Option<bool> GreedyContainerGCStrategy::CheckIfPrimaryContainerId(uint64_t container_id) {
    CHECK(storage_, "Storage not set");

    TRACE("Check if primary: container id " << container_id);
    pair<lookup_result, ContainerStorageAddressData> address_data =
        storage_->LookupContainerAddress(container_id, NULL, false);
    if (address_data.first == LOOKUP_ERROR) {
        WARNING("Lookup of container address failed: " << container_id << ", result " << address_data.first);
        return false;
    } else if (address_data.first == LOOKUP_NOT_FOUND) {
        WARNING("Container address not found: " << container_id << ", result " << address_data.first);
        return make_option(false);
    } else if (address_data.second.has_primary_id() && address_data.second.primary_id() != container_id) {
        WARNING("Merge candidate id is now only secondary: " <<
            "merge candidate id " << container_id <<
            ", primary id " << address_data.second.primary_id());
        return make_option(false);
    }
    return make_option(true);
}

bool GreedyContainerGCStrategy::ProcessMergeCandidates() {
    FAULT_POINT("container-storage.gc.process.pre");

    // TODO (dmeister): why is this lock recursive?
    recursive_mutex::scoped_lock scoped_lock(this->lock_);
    CHECK(this->merge_candidates_, "Merge candidates not set");

    // search two containers that can be merged
    TRACE("Search mergable containers");

    ContainerGreedyGCCandidateItemData merge_items[2];

    // the delete item list will contain all items to remove either because
    // the item has been selected or because some other in-consistency that
    // make the item an invalid or bad merge candidate
    list<pair<uint64_t, uint64_t> > delete_item_list;

    // search an not really filled container

    lookup_result r = LOOKUP_NOT_FOUND;
    for (uint64_t bucket = 0; bucket <= this->maximal_bucket_; bucket++) {
        ContainerGreedyGCCandidateData candidate_data;
        r = this->merge_candidates_->Lookup(&bucket, sizeof(bucket), &candidate_data);
        CHECK(r != LOOKUP_ERROR, "Failed to lookup merge candidates");

        if (r == LOOKUP_FOUND) {
            TRACE("Read bucket: bucket " << bucket << ", data " << candidate_data.ShortDebugString());

            // found a bucket with candidates
            uint32_t item_count = candidate_data.item_size();
            CHECK(item_count > 0, "Illegal item count: item count " << item_count);

            uint32_t selected_item = -1;
            for (uint32_t i = 0; i < candidate_data.item_size(); i++) {
                ContainerGreedyGCCandidateItemData item_data = candidate_data.item(i);

                if (this->eviction_timeout_ > 0 && touched_set_.Contains(item_data.address())) {
                    DEBUG("Container has been changed recently: item " << item_data.ShortDebugString());
                    continue;
                }

                if (GetBucket(item_data.active_data_size()) != bucket) {
                    WARNING("Item is in wrong bucket: bucket " << bucket << ", item " << item_data.ShortDebugString());
                    delete_item_list.push_back(make_pair(bucket, item_data.address()));
                    continue;
                }

                Option<bool> check_result = CheckIfPrimaryContainerId(item_data.address());
                CHECK(check_result.valid(),
                    "Failed to check of container id is primary: container id " << item_data.address());
                if (!check_result.value()) {
                    TRACE("container id is either not yet committed, already deleted or not primary: " <<
                        "item " << item_data.ShortDebugString());
                    delete_item_list.push_back(make_pair(bucket, item_data.address()));
                    continue;
                }

                selected_item = i;
                break;
            }

            if (selected_item != -1) {
                TRACE("Found first merge candidate: " << candidate_data.item(selected_item).ShortDebugString());

                merge_items[0] = candidate_data.item(selected_item);
                delete_item_list.push_back(make_pair(bucket, candidate_data.item(selected_item).address()));
                break;
            }
        }
    }

    if (r == LOOKUP_NOT_FOUND) {
        // if there is no bucket with an item, there is no need to search for more
        return true;
    }
    // r == LOOKUP_FOUND
    // we have found a first possible container

    if (merge_items[0].active_data_size() == 0 && merge_items[0].active_item_count() == 0) {
        // we found a container that should be deleted without a merge
    } else {
        // we have to search a merge partner

        uint64_t maximal_matching_bucket = this->maximal_bucket_ - GetBucket(merge_items[0].active_data_size());
        // signed matching bucket here
        for (int64_t matching_bucket = maximal_matching_bucket; matching_bucket >= 0; matching_bucket--) {

            ContainerGreedyGCCandidateData candidate_data;
            r = this->merge_candidates_->Lookup(&matching_bucket, sizeof(matching_bucket), &candidate_data);
            CHECK(r != LOOKUP_ERROR, "Failed to lookup merge candidates");

            if (r == LOOKUP_FOUND) {
                TRACE("Read bucket: bucket " << matching_bucket << ", data " << candidate_data.ShortDebugString());

                // may be we find a matching container here
                uint32_t item_count = candidate_data.item_size();
                CHECK(item_count > 0, "Illegal item count: item count " << item_count);

                uint32_t selected_item = -1;
                for (uint32_t i = 0; i < candidate_data.item_size() && selected_item == -1; i++) {
                    ContainerGreedyGCCandidateItemData item_data = candidate_data.item(i);

                    if ((item_data.active_data_size() + merge_items[0].active_data_size()) >= container_data_size_
                        || (item_data.active_item_count() + merge_items[0].active_item_count()) >= storage_->GetMaxItemsPerContainer()
                        || item_data.address() == merge_items[0].address()) {
                        TRACE("Skip merge candidate: " << item_data.ShortDebugString() <<
                            ", partner " << merge_items[0].ShortDebugString());
                        continue;
                    }

                    if (this->eviction_timeout_ > 0 && touched_set_.Contains(item_data.address())) {
                        DEBUG("Container has been changed recently: item " << item_data.ShortDebugString());
                        continue;
                    }

                    if (GetBucket(item_data.active_data_size()) != matching_bucket) {
                        WARNING("Item is in wrong bucket: bucket " << matching_bucket << ", item " << item_data.ShortDebugString());
                        delete_item_list.push_back(make_pair(matching_bucket, item_data.address()));
                        continue;
                    }

                    Option<bool> check_result = CheckIfPrimaryContainerId(item_data.address());
                    CHECK(check_result.valid(), "Failed to check of container id is primary: container id " << item_data.address());
                    if (!check_result.value()) {
                        // container id is either not yet committed, already deleted or not primary
                        delete_item_list.push_back(make_pair(matching_bucket, item_data.address()));
                        continue;
                    }

                    selected_item = i;

                }

                if (selected_item != -1) {
                    TRACE("Found second merge candidate: " << candidate_data.item(selected_item).ShortDebugString());
                    merge_items[1] = candidate_data.item(selected_item);
                    delete_item_list.push_back(make_pair(matching_bucket, candidate_data.item(selected_item).address()));
                    break;
                } else {
                    r = LOOKUP_NOT_FOUND;
                }
            }
        }

        if (r == LOOKUP_NOT_FOUND) {
            return true;
        }
    }

    // we found a second container
    if (merge_items[0].has_address() && merge_items[1].has_address()) {
        DEBUG("Found merge candidates " <<
            merge_items[0].ShortDebugString() << ", " <<
            merge_items[1].ShortDebugString());

        if (merge_items[1].address() < merge_items[0].address()) {
            // to have everything a bit more predictable we normalize the ordering
            ContainerGreedyGCCandidateItemData exchange =  merge_items[0];
            merge_items[0] = merge_items[1];
            merge_items[1] = exchange;
        }

        // remove the items from the bucket before the merge as the worst thing that can happen
        // is to "lose" a merge candidate. While this should not happen constantly, we can life with that
        for (list<pair<uint64_t, uint64_t> >::iterator i = delete_item_list.begin(); i != delete_item_list.end(); i++) {
            TRACE("Delete merge address from bucket: address " << i->second << ", bucket " << i->first);
            CHECK(DeleteFromBucket(i->first, i->second),
                "Failed to remove merge address from bucket: address " << i->second << ", bucket " << i->first);
        }

        // release lock to avoid deadlock
        scoped_lock.release();

        FAULT_POINT("container-storage.gc.process.before-container-merge");
        bool aborted = false;
        CHECK(storage_->TryMergeContainer(
                merge_items[0].address(),
                merge_items[1].address(),
                &aborted),
            "Failed to merge container " << merge_items[0].address() <<
            ", container " << merge_items[1].address() << ":"
            ", data of container " << merge_items[0].ShortDebugString() <<
            ", data of container " << merge_items[1].ShortDebugString());

        if (aborted) {
            DEBUG("Aborted to merge container " << merge_items[0].address() <<
                ", container " << merge_items[1].address() << ":"
                ", data of container " << merge_items[0].ShortDebugString() <<
                ", data of container " << merge_items[1].ShortDebugString());
        }

        TRACE("Finished processing merge candidates " <<
            merge_items[0].ShortDebugString() << ", " <<
            merge_items[1].ShortDebugString());
    } else if (merge_items[0].has_address()) {
        CHECK(merge_items[0].active_data_size() == 0 && merge_items[0].active_item_count() == 0,
            "Illegal delete candidate container " << merge_items[0].ShortDebugString());
        // a single deletion container
        DEBUG("Found delete candidate " <<
            merge_items[0].ShortDebugString());

        // remove the items from the bucket before the merge as the worst thing that can happen
        // is to "lose" a merge candidate. While this should not happen constantly, we can life with that
        for (list<pair<uint64_t, uint64_t> >::iterator i = delete_item_list.begin(); i != delete_item_list.end(); i++) {
            TRACE("Delete merge address from bucket: address " << i->second << ", bucket " << i->first);
            CHECK(DeleteFromBucket(i->first, i->second),
                "Failed to remove merge address from bucket: address " << i->second << ", bucket " << i->first);
        }

        // release lock to avoid deadlock
        scoped_lock.release();

        FAULT_POINT("container-storage.gc.process.before-container-delete");
        bool aborted = false;
        CHECK(storage_->TryDeleteContainer(
                merge_items[0].address(),
                &aborted),
            "Failed to delete container " << merge_items[0].address() <<
            ", data of container " << merge_items[0].DebugString());
        if (aborted) {
            DEBUG("Aborted to delete container " << merge_items[0].address() <<
                ", data of container " << merge_items[0].ShortDebugString());
        }
        TRACE("Finished processing delete candidate " <<
            merge_items[0].ShortDebugString());
    }
    FAULT_POINT("container-storage.gc.process.post");
    return true;
}

bool GreedyContainerGCStrategy::OnIdle() {
    CHECK(this->started_, "GC not started");

    CHECK(ProcessMergeCandidates(),
        "Failed to process merge candidates");
    return true;
}

bool GreedyContainerGCStrategy::OnStoragePressure() {
    return this->OnIdle();
}

string GreedyContainerGCStrategy::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    if (merge_candidates_) {
        sstr << "\"merge candidate count\": " << merge_candidate_count_ << std::endl;
    } else {
        sstr << "\"merge candidate count\": null" << std::endl;
    }
    sstr << "}";
    return sstr.str();
}

string GreedyContainerGCStrategy::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"gc time\": " << this->stats_.gc_time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

#ifdef DEDUPV1_CORE_TEST
void GreedyContainerGCStrategy::ClearData() {
    if (this->merge_candidates_) {
        this->merge_candidates_->Close();
        this->merge_candidates_ = NULL;
    }
}
#endif

}
}
