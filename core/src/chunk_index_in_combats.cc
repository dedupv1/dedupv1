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

#include <core/chunk_index_in_combat.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <base/bitutil.h>

#include <cmath>
#include <sstream>

using dedupv1::base::make_bytestring;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::make_option;
using dedupv1::base::Option;
using dedupv1::base::BloomSet;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START;
using dedupv1::log::EVENT_REPLAY_MODE_DIRECT;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_DELETED;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED;
using dedupv1::base::ProfileTimer;
using tbb::spin_rw_mutex;
using std::map;
using std::stringstream;

LOGGER("ChunkIndex");

namespace dedupv1 {
namespace chunkindex {

ChunkIndexInCombats::ChunkIndexInCombats() {
    size_ = kDefaultMaxSize;
    capacity_ = kDefaultMaxSize;
    k_ = 0;
    error_rate_ = 0.01;
    in_combat_chunks_ = NULL;
    in_combat_count_ = 0;
}

ChunkIndexInCombats::~ChunkIndexInCombats() {
    if (in_combat_chunks_) {
        delete in_combat_chunks_;
        in_combat_chunks_ = NULL;
    }
}

bool ChunkIndexInCombats::SetOption(const std::string& option_name,
    const std::string& option) {
    if (option_name == "size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->size_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "k") {
        dedupv1::base::Option<uint32_t> o = To<uint32_t>(option);
        CHECK(o.valid(), "Illegal k: " << option);
        CHECK(o.value() >= 1 && o.value() < 256, "Illegal k: " << option);
        this->k_ = o.value();
        return true;
    }
    if (option_name == "error-rate") {
        dedupv1::base::Option<double> o = To<double>(option);
        CHECK(o.valid(), "Illegal error-rate: " << option);
        CHECK(o.value() > 0.0 && o.value() < 1.0, "Illegal error rate: " << option);
        this->error_rate_ = o.value();
        return true;
    }
    if (option_name == "capacity") {
        dedupv1::base::Option<int64_t> o = ToStorageUnit(option);
        CHECK(o.valid(), "Illegal capacity: " << option);
        CHECK(o.value() > 0, "Illegal capacity (must be greater 0): " << option);
        this->capacity_ = o.value();
        return true;
    }
    return false;
}

bool ChunkIndexInCombats::Start(const StartContext& start_context,
    dedupv1::log::Log* log) {
    CHECK(log, "Log not set");
    this->log_ = log;

    if (error_rate_ > 0.0) {
        // auto configuration
        this->in_combat_chunks_ =
          BloomSet::NewOptimizedBloomSet(capacity_, error_rate_);
        CHECK(this->in_combat_chunks_, "Failed to alloc bloom set");
        k_ = in_combat_chunks_->hash_count();
        size_ = in_combat_chunks_->size();
        INFO("Auto configure in-combat store: " <<
            "k " << static_cast<uint32_t>(k_) <<
            ", size " << size_);
    } else {
        this->in_combat_chunks_ = new BloomSet(this->size_, this->k_);
        CHECK(this->in_combat_chunks_, "Failed to alloc bloom set");
    }

    CHECK(this->in_combat_chunks_, "Failed to alloc bloom set");
    CHECK(this->in_combat_chunks_->Init(), "Failed to init bloom set");

    DEBUG("Starting in-combat store");
    return true;
}

bool ChunkIndexInCombats::Touch(const void* fp, size_t fp_size) {
    ProfileTimer timer(this->stats_.touch_time_);

    CHECK(this->in_combat_chunks_->Put(fp, fp_size),
        "Cannot update bloom filter: " << ToHexString(fp, fp_size));

    in_combat_count_++;
    return true;
}

Option<bool> ChunkIndexInCombats::Contains(const void* fp, size_t fp_size) {
    ProfileTimer timer(this->stats_.contains_time_);

    Option<bool> result = false;
    lookup_result lr = this->in_combat_chunks_->Contains(fp, fp_size);
    if (lr == LOOKUP_FOUND) {
        result = make_option(true);
    } else if (lr == LOOKUP_NOT_FOUND) {
        result = make_option(false);
    }

    TRACE("Check in-combat state: " << ToHexString(fp, fp_size) <<
        ", result " << result.valid() << "/" << result.value());
    return result;
}

bool ChunkIndexInCombats::LogReplay(dedupv1::log::event_type event_type,
                                    const LogEventData& event_value,
                                    const dedupv1::log::LogReplayContext& context) {
    DCHECK(log_, "Log not set");

    TRACE("Replay log entry: " <<
        "event type " << dedupv1::log::Log::GetEventTypeName(event_type) <<
        ", replay mode " << dedupv1::log::Log::GetReplayModeName(context.replay_mode()) <<
        ", log id " << context.log_id());

    if (event_type == EVENT_TYPE_BLOCK_MAPPING_WRITTEN && context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
        // rebuild
        BlockMappingWrittenEventData event_data = event_value.block_mapping_written_event();

        // we touch the original and the updated mappings because of situations where
        // a -1 block mapping is still not replayed and fast gc is active. The usage count might already be zero, but the
        // -1 block mapping is not yet replayed. Then the chunk might be removed based on the uc value, but the replay
        // fails because the chunk is no in the chunk index
        BlockMappingPairData mapping_pair_data = event_data.mapping_pair();
        for (int i = 0; i < mapping_pair_data.items_size(); i++) {
            CHECK(Touch(mapping_pair_data.items(i).fp().data(), mapping_pair_data.items(i).fp().size()),
                "Failed to update in-combat data: " << mapping_pair_data.items(i).DebugString());
        }
    }
    if (event_type == EVENT_TYPE_BLOCK_MAPPING_DELETED && context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
        // rebuild
        BlockMappingDeletedEventData event_data = event_value.block_mapping_deleted_event();

        // we touch the original because of situations where
        // a -1 block mapping is still not replayed and fast gc is active. The usage count might already be zero, but the
        // -1 block mapping is not yet replayed. Then the chunk might be removed based on the uc value, but the replay
        // fails because the chunk is no in the chunk index
        BlockMappingData mapping_data = event_data.original_block_mapping();
        for (int i = 0; i < mapping_data.items_size(); i++) {
            CHECK(Touch(mapping_data.items(i).fp().data(), mapping_data.items(i).fp().size()),
                "Failed to update in-combat data: " << mapping_data.items(i).DebugString());
        }
    }
    if (event_type == EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED && context.replay_mode() == EVENT_REPLAY_MODE_DIRTY_START) {
        BlockMappingWriteFailedEventData event_data = event_value.block_mapping_write_failed_event();

        BlockMappingPairData mapping_pair_data = event_data.mapping_pair();
        for (int i = 0; i < mapping_pair_data.items_size(); i++) {
            CHECK(Touch(mapping_pair_data.items(i).fp().data(), mapping_pair_data.items(i).fp().size()),
                "Failed to update in-combat data: " << mapping_pair_data.items(i).DebugString());
        }
    }
    if ((event_type == dedupv1::log::EVENT_TYPE_LOG_EMPTY || event_type == dedupv1::log::EVENT_TYPE_LOG_NEW)
        && context.replay_mode() == EVENT_REPLAY_MODE_DIRECT) {
        // log is still empty aka the log empty event is the last event logged
        if (context.log_id() + 1 == log_->log_id()) {
            CHECK(Clear(), "Failed to clear in combat chunks");
        } else {
            TRACE("New events: context log id " << context.log_id() << ", current log id" << log_->log_id());
            // The log is not empty in this moment. As there are already new events
        }
    }
    return true;
}

bool ChunkIndexInCombats::Clear() {
    DEBUG("Clear in-combat list: " << in_combat_count_);
    this->in_combat_chunks_->Clear();
    this->in_combat_count_ = 0;

    return true;
}

std::string ChunkIndexInCombats::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"contains time\": " << stats_.contains_time_.GetSum() << "," << std::endl;
    sstr << "\"touch time\": " << stats_.touch_time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

std::string ChunkIndexInCombats::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"in combat count\": " << in_combat_count_ << std::endl;
    sstr << "}";
    return sstr.str();
}

}
}

