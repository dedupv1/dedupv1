/*
 * dedupv1 - iSCSI based Deduplication System for Linux
 *
 * (C) 2008 Dirk Meister
 * (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
 * (C) 2012, Dirk Meister, Johannes Gutenberg University Mainz
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
#include <core/block_chunk_cache.h>
#include <base/strutil.h>
#include <base/hashing_util.h>
#include <base/logging.h>
#include "dedupv1_stats.pb.h"

using dedupv1::blockindex::BlockIndex;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;

using dedupv1::base::ScopedLock;
using dedupv1::base::make_bytestring;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::LOOKUP_FOUND;
using std::set;
using std::string;
using std::map;
using std::list;
using std::stringstream;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::To;
using dedupv1::base::Option;
using dedupv1::base::ProfileTimer;
using std::tr1::unordered_map;
using dedupv1::base::bytestring_fp_murmur_hash;

LOGGER("BlockChunkCache");

namespace dedupv1 {

namespace filter {

BlockChunkCache::BlockChunkCache() {
    block_index_ = NULL;
    prefetchWindow_ = 0;
    min_diff_value_ = 0;
    diff_cache_size_ = 16;
    block_cache_size_ = 256;
    prefetchWindow_ = 0;
}

bool BlockChunkCache::Start(BlockIndex* block_index) {
    block_index_ = block_index;
    return true;
}

bool BlockChunkCache::Close() {
    return true;
}

bool BlockChunkCache::SetOption(const string& option_name, const string& option) {
    if (option_name == "diff-cache-size") {
        Option<uint32_t> b = To<uint32_t>(option);
        CHECK(b.valid(), "Illegal option value: " << option_name << "=" << option);
        diff_cache_size_ = b.value();
        return true;
    }
    if (option_name == "block-cache-size") {
        Option<uint32_t> b = To<uint32_t>(option);
        CHECK(b.valid(), "Illegal option value: " << option_name << "=" << option);
        block_cache_size_ = b.value();
        return true;
    }
    if (option_name == "min-diff-hit-count") {
        Option<uint32_t> b = To<uint32_t>(option);
        CHECK(b.valid(), "Illegal option value: " << option_name << "=" << option);
        min_diff_value_ = b.value();
        return true;
    }
    if (option_name == "prefetch-window") {
        Option<uint32_t> b = To<uint32_t>(option);
        CHECK(b.valid(), "Illegal option value: " << option_name << "=" << option);
        prefetchWindow_ = b.value();
        return true;
    }
    return false;
}

bool BlockChunkCache::EvictBlock(uint64_t event_block_id) {

    TRACE("Evict block from cache: block id " << event_block_id);
    int assoc = event_block_id % 4;
    std::set<bytestring> fp_set;
    tbb::concurrent_hash_map<uint64_t, std::set<bytestring> >::accessor acc;
    if (block_map_[assoc].find(acc, event_block_id)) {
        fp_set = acc->second;
        block_map_[assoc].erase(acc);
        acc.release();

        set<bytestring>::iterator i;
        for (i = fp_set.begin(); i != fp_set.end(); i++) {

            tbb::concurrent_hash_map<bytestring, ChunkMapData, dedupv1::base::bytestring_fp_murmur_hash>::accessor chunk_acc;
            if (block_chunk_map_.find(chunk_acc, *i)) {
                chunk_acc->second.block_set.erase(event_block_id);

                if (chunk_acc->second.block_set.empty()) {

                    TRACE("Evict fingerprint: " << ToHexString(i->data(), i->size()));

                    block_chunk_map_.erase(chunk_acc);
                }
            }
        }
    }
    return true;
}

lookup_result BlockChunkCache::FetchBlockIntoCache(uint64_t fetch_block_id) {
    ProfileTimer timer(this->stats_.fetch_time_);
    int assoc = fetch_block_id % 4;
    ScopedLock scoped_fetch_lock(&fetch_lock_[assoc]);
    scoped_fetch_lock.AcquireLock();

    // A re-check because of out-dated information
    tbb::concurrent_hash_map<uint64_t, std::set<bytestring> >::const_accessor block_acc;
    if (block_map_[assoc].find(block_acc, fetch_block_id)) {
        block_acc.release();
        return LOOKUP_FOUND;
    }
    block_acc.release();

    TRACE("Fetch block into cache: block id " << fetch_block_id);

    stats_.fetch_++;

    BlockMapping block_mapping(fetch_block_id, block_index_->block_size());
    BlockIndex::read_result r = block_index_->ReadBlockInfo(NULL, &block_mapping, NO_EC);
    CHECK_RETURN(r != BlockIndex::READ_RESULT_ERROR, LOOKUP_ERROR, "Failed to read block index");

    if (r == BlockIndex::READ_RESULT_NOT_FOUND) {
        stats_.block_lookup_missing_++;
        // hhm
        TRACE("Block not found in block index: block id " << fetch_block_id);
        return LOOKUP_NOT_FOUND;
    }

    tbb::spin_mutex::scoped_lock scoped_lock(block_map_cache_mutex_[assoc]);
    if (this->block_cache_size_ <= block_map_cache_strategy_[assoc].size()) {
        stats_.block_evict_count_++;
        uint64_t replacement_block_id;
        if (block_map_cache_strategy_[assoc].Replace(&replacement_block_id)) {
            EvictBlock(replacement_block_id);
        }
    }
    block_map_cache_strategy_[assoc].Touch(fetch_block_id);
    scoped_lock.release();

    tbb::concurrent_hash_map<uint64_t, std::set<bytestring> >::accessor block_acc_insert;
    block_map_[assoc].insert(block_acc_insert, fetch_block_id);

    std::list<BlockMappingItem>::iterator i;
    for (i = block_mapping.items().begin(); i != block_mapping.items().end(); i++) {

        if (Fingerprinter::IsEmptyDataFingerprint(i->fingerprint(), i->fingerprint_size())) {
            continue;
        }

        const bytestring& fp(i->fingerprint_string());
        TRACE("Add fingerprint to cache: block id " << fetch_block_id <<
            ", fp " << ToHexString(fp.data(), fp.size()));

        block_acc_insert->second.insert(fp);

        tbb::concurrent_hash_map<bytestring, ChunkMapData, dedupv1::base::bytestring_fp_murmur_hash>::accessor acc;
        block_chunk_map_.insert(acc, fp);
        acc->second.block_set.insert(fetch_block_id);
        acc->second.data_adress = i->data_address();
    }
    return LOOKUP_FOUND;
}

bool BlockChunkCache::Contains(const ChunkMapping* mapping,
                               uint64_t current_block_id,
                               uint64_t* data_address) {
    ProfileTimer timer(this->stats_.time_);

    DCHECK(mapping, "Mapping not set");

    if (Fingerprinter::IsEmptyDataFingerprint(mapping->fingerprint(), mapping->fingerprint_size())) {
        return true;
    }

    TRACE("Block chunk cache check: " <<
        "chunk " << mapping->DebugString() <<
        ", block id " << current_block_id);

    bool result = false;

    bytestring fp = dedupv1::base::make_bytestring(mapping->fingerprint(), mapping->fingerprint_size());
    tbb::concurrent_hash_map<bytestring, ChunkMapData, bytestring_fp_murmur_hash>::accessor acc;

    if (!block_chunk_map_.find(acc, fp)) {
        TRACE("Fingerprint not in block chunk cache: " << ToHexString(fp.data(), fp.size()));
        acc.release();

        // fingerprint not found

        std::list<int64_t>::const_iterator diff_iterator;

        tbb::spin_mutex::scoped_lock scoped_diff_lock2(diff_mutex_);
        std::list<int64_t> diff_list;
        for (diff_iterator = diff_map_cache_strategy_.ordered_objects().begin();
             diff_iterator != diff_map_cache_strategy_.ordered_objects().end();
             diff_iterator++) {
            if (diff_map_[*diff_iterator] >= min_diff_value_) {
                diff_list.push_back(*diff_iterator);
            } else {
                TRACE("Skip diff " << (*diff_iterator) <<
                    ", diff value " << diff_map_[*diff_iterator] <<
                    ", min diff value not reached");
            }
        }
        scoped_diff_lock2.release();

        ProfileTimer iter_timer(this->stats_.diff_iteration_time_);

        std::set<int64_t> diff_to_remove;
        for (diff_iterator = diff_list.begin();
             diff_iterator != diff_list.end();
             diff_iterator++) {

            int64_t check_block_id = current_block_id + *diff_iterator;

            TRACE("Check for fetching: check block id " << check_block_id <<
                ", diff " << *diff_iterator);
            if (check_block_id < 0) {
                continue;
            }

            int check_assoc = check_block_id % 4;
            tbb::concurrent_hash_map<uint64_t, std::set<bytestring> >::const_accessor block_acc;
            if (block_map_[check_assoc].find(block_acc, check_block_id)) {
                block_acc.release();
                // block already in cache
                TRACE("Check block already in map: check block id " << check_block_id);
                continue;
            }
            block_acc.release();

            lookup_result lr = this->FetchBlockIntoCache(check_block_id);
            CHECK(lr != LOOKUP_ERROR, "Failed to fetch block: " << check_block_id);

            if (lr == LOOKUP_NOT_FOUND) {
                TRACE("Decrease diff due to failed block lookup: " << (*diff_iterator));
                diff_to_remove.insert(*diff_iterator);
            } else {
                if (prefetchWindow_ > 0) {
                    for (int k = 0; k < prefetchWindow_; k++) {
                        lr = this->FetchBlockIntoCache(check_block_id + k);
                        CHECK(lr != LOOKUP_ERROR, "Failed to fetch block: " << check_block_id + k);
                        if (lr == LOOKUP_NOT_FOUND) {
                            break; // no more prefetching
                        }
                    }
                }

                // re-check
                tbb::concurrent_hash_map<bytestring, ChunkMapData, bytestring_fp_murmur_hash>::const_accessor acc2;

                if (block_chunk_map_.find(acc2, fp)) {
                    // we have a hit now
                    *data_address = acc2->second.data_adress;
                    result = true;

                    TRACE("Block hit after fetching: " << ToHexString(fp.data(), fp.size()) <<
                        ", data address " << (*data_address) <<
                        ", diff " << (*diff_iterator));

                    break;
                }
            }
        }
        iter_timer.stop();

        if (!diff_to_remove.empty()) {
            tbb::spin_mutex::scoped_lock scoped_lock(diff_mutex_);
            set<int64_t>::iterator remove_iterator;
            for (remove_iterator = diff_to_remove.begin(); remove_iterator != diff_to_remove.end(); remove_iterator++) {

                stats_.diff_evict_count_++;
                diff_map_.erase(*remove_iterator);
                diff_map_cache_strategy_.Delete(*remove_iterator);
            }
        }

    } else {
        // fingerprint found
        *data_address = acc->second.data_adress;
        std::set<uint64_t> block_set(acc->second.block_set);
        acc.release();

        TRACE("Fingerprint found in block chunk cache: " << ToHexString(fp.data(), fp.size()) <<
            ", data address " << (*data_address));

        bool touched_one = false;
        std::set<uint64_t>::iterator block_set_iterator;
        for (block_set_iterator = block_set.begin(); block_set_iterator != block_set.end(); block_set_iterator++) {
            uint64_t block_id = *block_set_iterator;
            int64_t diff = block_id - current_block_id;

            TRACE("Touch block: " << block_id << ", diff " << diff);

            TouchBlock(block_id);
            if (TouchDiff(diff, false)) {
                touched_one = true;
            }
        }

        if (!touched_one && block_set.size() == 1) {
            int64_t diff = (*block_set.begin()) - current_block_id;
            TouchDiff(diff, true);
        }

        result = true;
    }
    if (result) {
        stats_.hits_++;
    } else {
        stats_.miss_++;
    }
    return result;
}

BlockChunkCache::Statistics::Statistics() {
    fetch_ = 0;
    hits_ = 0;
    miss_ = 0;
    block_lookup_missing_ = 0;
    block_evict_count_ = 0;
    diff_evict_count_ = 0;
    no_hint_count_ = 0;
}

bool BlockChunkCache::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    BlockChunkCacheStatsData data;
    data.set_fetch_count(stats_.fetch_);
    data.set_hit_count(stats_.hits_);
    data.set_miss_count(stats_.miss_);
    CHECK(ps->Persist(prefix, data), "Failed to persist block chunk stats");
    return true;
}

bool BlockChunkCache::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    BlockChunkCacheStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore block chunk stats");
    stats_.fetch_ = data.fetch_count();
    stats_.hits_ = data.hit_count();
    stats_.miss_ = data.miss_count();
    return true;
}

string BlockChunkCache::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"no block hint count\": " << this->stats_.no_hint_count_ << "," << std::endl;
    sstr << "\"block lookup missing count\": " << this->stats_.block_lookup_missing_ << "," << std::endl;
    sstr << "\"block evict count\": " << this->stats_.block_evict_count_ << "," << std::endl;
    sstr << "\"diff evict count\": " << this->stats_.diff_evict_count_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string BlockChunkCache::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"fetch count\": " << this->stats_.fetch_ << "," << std::endl;
    if (stats_.fetch_ == 0) {
        sstr << "\"hit to fetch ratio\": null," << std::endl;
    } else {
        sstr << "\"hit to fetch ratio\": " << (1.0 * stats_.hits_ / stats_.fetch_) << "," << std::endl;
    }
    sstr << "\"hits\": " << this->stats_.hits_ << "," << std::endl;
    sstr << "\"miss\": " << this->stats_.miss_ << "," << std::endl;

    uint64_t r = stats_.hits_ + this->stats_.miss_;
    if (r == 0) {
        sstr << "\"hit ratio\": null" << std::endl;
    } else {
        sstr << "\"hit ratio\": " << (1.0 * stats_.hits_ / r) << std::endl;
    }

    sstr << "}";
    return sstr.str();
}

string BlockChunkCache::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"time\": " << this->stats_.time_.GetSum() << "," << std::endl;
    sstr << "\"lock time\": " << this->stats_.lock_time_.GetSum() << "," << std::endl;
    sstr << "\"fetch time\": " << this->stats_.fetch_time_.GetSum() << "," << std::endl;
    sstr << "\"diff touch time\": " << this->stats_.diff_handling_time_.GetSum() << "," << std::endl;
    sstr << "\"diff iteration time\": " << this->stats_.diff_iteration_time_.GetSum() << "," << std::endl;
    sstr << "\"block touch time\": " << this->stats_.block_handling_time_.GetSum() << std::endl;

    sstr << "}";
    return sstr.str();
}

bool BlockChunkCache::TouchBlock(uint64_t block_id) {
    ProfileTimer timer2(this->stats_.block_handling_time_);

    int assoc = block_id % 4;
    tbb::spin_mutex::scoped_lock scoped_lock(block_map_cache_mutex_[assoc]);

    block_map_cache_strategy_[assoc].Touch(block_id);
    return true;
}

bool BlockChunkCache::TouchDiff(int64_t diff, bool allow_insert) {
    ProfileTimer timer2(this->stats_.diff_handling_time_);

    tbb::spin_mutex::scoped_lock scoped_lock(diff_mutex_);

    if (this->diff_map_.find(diff) == diff_map_.end()) {
        // diff not found
        if (!allow_insert) {
            return false;
        }
        if (diff_cache_size_ <= diff_map_cache_strategy_.size()) {
            int64_t replacement_diff = 0;
            diff_map_cache_strategy_.Replace(&replacement_diff);

            diff_map_.erase(replacement_diff);
            diff_map_[diff] = 1;
            diff_map_cache_strategy_.Touch(diff);

            stats_.diff_evict_count_++;

            DEBUG("Touch diff: " << diff <<
                ", evited diff " << replacement_diff <<
                ", cache size " << diff_map_cache_strategy_.size());
        } else {
            // Space left
            TRACE("Touch diff: " << diff);

            diff_map_cache_strategy_.Touch(diff);
            diff_map_[diff] = 1;
        }
        return false;
    } else {
        // diff found
        diff_map_[diff]++;
        diff_map_cache_strategy_.Touch(diff);

        TRACE("Touch diff: " << diff << ", diff value " << diff_map_[diff]);
        return true;
    }
}

bool BlockChunkCache::UpdateKnownChunk(const ChunkMapping* mapping, uint64_t current_block_id) {
    ProfileTimer timer(this->stats_.time_);

    int64_t block_diff = 0;
    if (mapping->has_block_hint()) {
        block_diff = mapping->block_hint() - current_block_id;
        TRACE("Block hint diff: " << block_diff << ", mapping " << mapping->DebugString());

        TouchDiff(block_diff, true);
    } else {
        stats_.no_hint_count_++;
    }

    return true;
}

} // namespace
} // namespace
