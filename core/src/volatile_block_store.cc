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

#include <core/volatile_block_store.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <core/dedup.h>
#include <base/bitutil.h>
#include <base/index.h>
#include <base/locks.h>
#include <base/profile.h>

#include <core/log_consumer.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/timer.h>
#include <core/storage.h>
#include <core/container_storage.h>
#include <core/dedup_system.h>
#include <core/chunk_store.h>
#include <core/open_request.h>
#include <core/session.h>

#include <sstream>
#include <algorithm>
#include <iostream>
#include <iomanip>

using std::make_pair;
using std::pair;
using std::multimap;
using std::set;
using std::map;
using std::list;
using std::string;
using std::stringstream;
using dedupv1::base::Walltimer;
using dedupv1::base::ScopedLock;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::ProfileTimer;
using dedupv1::chunkstore::Storage;
using std::tr1::tuple;
using std::tr1::make_tuple;

LOGGER("VolatileBlockStore");

namespace dedupv1 {
namespace blockindex {

UncommitedBlockEntry::UncommitedBlockEntry(const BlockMapping& o,
        const BlockMapping& m,
        google::protobuf::Message* extra_message,
        uint64_t block_mapping_written_event_log_id,
        uint32_t open_container_count,
        uint32_t open_predecessor_count)
: original_mapping_(o.block_size()),  modified_mapping_(m.block_size()) {
    if (!this->original_mapping_.CopyFrom(o)) {
        WARNING("Cannot copy original block mapping: " << o.DebugString());
    }
    if (!this->modified_mapping_.CopyFrom(m)) {
        WARNING("Cannot copy modified block mapping: " << m.DebugString());
    }
    this->block_mapping_written_event_log_id_ = block_mapping_written_event_log_id;
    this->open_container_count_ = open_container_count;
    this->open_predecessor_count_ = open_predecessor_count;
    extra_message_ = extra_message;
}

VolatileBlockStore::VolatileBlockStore() {
    this->stats_.lock_busy_ = 0;
    this->stats_.lock_free_ = 0;
    commit_state_callback_ = NULL;
}

bool VolatileBlockStore::ResetTracker() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_free_),
            "Failed to acquire lock");
    CHECK(this->container_tracker_.Reset(), "Failed to mark container as processed");
    CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    return true;
}

VolatileBlockStore::~VolatileBlockStore() {
    Clear();
}

bool VolatileBlockStore::Clear() {
    DEBUG("Clear volatile block store");

    std::multimap<uint64_t, UncommitedBlockEntry>::iterator i;
    for (i = uncommited_block_map_.begin(); i != uncommited_block_map_.end(); i++) {
        const google::protobuf::Message* extra_message = i->second.extra_message();
        if (extra_message) {
            delete extra_message;
        }
    }
    this->uncommited_block_map_.clear();
    this->uncommited_container_map_.clear();
    return true;
}

void VolatileBlockStore::HandleVolatileFailChange(list<tuple<BlockMapping, BlockMapping, const google::protobuf::Message*, int64_t> >* callback_ubes,
        const std::multimap<uint64_t, UncommitedBlockEntry>::iterator& bi) {
    tuple<BlockMapping, BlockMapping, const google::protobuf::Message*, int64_t> tuple = make_tuple(
            bi->second.original_mapping(),
            bi->second.modified_mapping(),
            bi->second.extra_message(),
            bi->second.block_mapping_written_event_log_id());
    callback_ubes->push_back(tuple);
    
    DEBUG("Fail change: block " << bi->second.modified_mapping().block_id() << ", version " << bi->second.modified_mapping().version());

    // fail all later versions of the same block
    std::list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::const_iterator j;
    for (j = bi->second.block_list().begin(); j != bi->second.block_list().end(); j++) {
        const std::multimap<uint64_t, UncommitedBlockEntry>::iterator& i(*j);

        DEBUG(i->second.original_mapping().DebugString() << " >> " << i->second.modified_mapping().DebugString() <<
                ": open predecessor count " <<
                i->second.open_predecessor_count());
        HandleVolatileFailChange(callback_ubes, i);
    }
    this->uncommited_block_map_.erase(bi);
}

bool VolatileBlockStore::Abort(uint64_t container_id, VolatileBlockCommitCallback* callback) {
    Walltimer timer;
    bool failed = false;
    list<tuple<BlockMapping, BlockMapping, const google::protobuf::Message*,int64_t> > callback_ubes;
    {
        ScopedLock scoped_lock(&this->lock_);
        CHECK(scoped_lock.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_free_),
                "Failed to acquire lock");

        DEBUG("Abort container " << container_id);

        map<uint64_t, UncommitedContainerEntry>::iterator j = this->uncommited_container_map_.find(container_id);
        if (j == this->uncommited_container_map_.end()) {
            // No blocks rely on this container;
            this->stats_.total_time_.Add(&timer);
            return true;
        }

        // container
        std::list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::iterator i;
        for (i = j->second.block_list_.begin(); i != j->second.block_list_.end(); i++) {
            std::multimap<uint64_t, UncommitedBlockEntry>::iterator& bi(*i);
            DEBUG("Found failing block mapping: " << bi->second.original_mapping().DebugString() << " >> " <<
                    bi->second.modified_mapping().DebugString());

            HandleVolatileFailChange(&callback_ubes, bi);
        }
        this->uncommited_container_map_.erase(container_id);
        CHECK(this->container_tracker_.ProcessedContainer(container_id), "Failed to mark container as processed");

        CHECK(scoped_lock.ReleaseLock(), "Failed to release lock");
    }
    // we defer the commit callback to avoid holding the volatile block store lock.
    Walltimer callback_timer;
    for (list<tuple<BlockMapping, BlockMapping, const google::protobuf::Message*, int64_t> >::iterator j = callback_ubes.begin(); j != callback_ubes.end(); j++) {
        DEBUG("Fail block: " << std::tr1::get<0>(*j).DebugString() << " => " << std::tr1::get<1>(*j).DebugString());
        if (callback) {
            if (!callback->FailVolatileBlock(std::tr1::get<0>(*j),std::tr1::get<1>(*j), std::tr1::get<2>(*j), std::tr1::get<3>(*j))) {
                WARNING("Fail function reports an error: " <<
                        "container id " << container_id <<
                        ", original block mapping " << std::tr1::get<0>(*j).DebugString() <<
                        ", modified block mapping " << std::tr1::get<1>(*j).DebugString());
                failed = true;
            }
            const google::protobuf::Message* message = std::tr1::get<2>(*j);
            if (message) {
                delete message;
            }
        }
    }
    this->stats_.total_time_.Add(&timer);
    return !failed;
}

bool VolatileBlockStore::Commit(uint64_t container_id, VolatileBlockCommitCallback* callback) {
    ProfileTimer total_timer(this->stats_.total_time_);
    bool failed = false;

    // OK, this processing is the horror, but I don't see another way that is more elegant
    // the main reason for the code is that a block is not allowed to be removed from the block map
    // before the block mapping is logged. Because otherwise there might be a lot of race conditions, e.g.
    // an block mapping version (i+1,i+2) might be stored after (i,i+1) is ready (removed from the block map), but
    // before it is committed. Then the block mapping (i+1,i+2) is committed before (i,i+1). We call this
    // situations that the block mapping is out-runned. This caused trouble for the gc, esp. when the system crashes or
    // when the chunk index must be restored.

    // search for all block changed that are committed when this container is ready
    list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator> callback_ubes;
    {
        ScopedLock scoped_lock(&this->lock_);
        CHECK(scoped_lock.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_busy_),
                "Failed to acquire lock");

        DEBUG("Commit container " << container_id);
        map<uint64_t, UncommitedContainerEntry>::iterator j = this->uncommited_container_map_.find(container_id);
        if (j == this->uncommited_container_map_.end()) {
            // No blocks rely on this container;

            TRACE("No blocks rely on container " << container_id);

            CHECK(this->container_tracker_.ProcessedContainer(container_id),
                    "Failed to mark container as processed");
            return true;
        }

        TRACE("Some blocks rely on container " << container_id);

        // container
        std::list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::iterator i;
        for (i = j->second.block_list_.begin(); i != j->second.block_list_.end(); i++) {
            std::multimap<uint64_t, UncommitedBlockEntry>::iterator& bi(*i);
            bi->second.open_container_count_--;
            DEBUG("Updated uncommitted block entry: " <<
                    "block " << bi->second.modified_mapping().block_id() <<
                    ", version " << bi->second.modified_mapping().version() <<
                    ", open container count " << bi->second.open_container_count() <<
                    ", open predecessor count " << bi->second.open_predecessor_count() <<
                    ", committed container " << container_id);

            if  (bi->second.open_container_count() == 0 && bi->second.open_predecessor_count() == 0) {
                // call back the currently handled block change
                callback_ubes.push_back(bi);
                TRACE("Ready uncommited block entry: " <<
                        bi->second.DebugString());
            }
        }
        this->uncommited_container_map_.erase(container_id);
        CHECK(this->container_tracker_.ProcessedContainer(container_id), "Failed to mark container as processed");
    }

    list<list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::iterator> failed_callbacks;

    while (!callback_ubes.empty()) {
        // we defer the commit callback to avoid holding the volatile block store lock.

        // callback phase
        {
            TRACE("Callback phase: " << callback_ubes.size());

            ProfileTimer callback_timer(this->stats_.callback_time_);
            for (list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::iterator j = callback_ubes.begin(); j != callback_ubes.end(); j++) {
                std::multimap<uint64_t, UncommitedBlockEntry>::iterator& bi(*j);
                UncommitedBlockEntry& block_entry(bi->second);
                DEBUG("Commit block: " << block_entry.ShortDebugString());
                if (callback) {
                    if (!callback->CommitVolatileBlock(block_entry.original_mapping(),
                            block_entry.modified_mapping(),
                            block_entry.extra_message(),
                            block_entry.block_mapping_written_event_log_id(), false)) {
                        WARNING("Commit function reports an error: " <<
                                "container id " << container_id <<
                                ", " << block_entry.DebugString());
                        failed_callbacks.push_back(j);
                        failed = true;
                    }
                    if (block_entry.extra_message()) {
                        delete block_entry.extra_message();
                        block_entry.clear_extra_message();
                    }
                }
            }
        }

        // search phase
        TRACE("Search phase: " << callback_ubes.size());
        {
            ScopedLock scoped_lock(&this->lock_);
            CHECK(scoped_lock.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_free_),
                    "Failed to acquire lock");

            // remove failed callbacks
            // the depended block changed of a failed callback should not be processed.
            list<list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::iterator>::iterator k;
            for (k = failed_callbacks.begin(); k != failed_callbacks.end(); k++) {
                list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::iterator& l(*k);
                std::multimap<uint64_t, UncommitedBlockEntry>::iterator& m(*l);
                callback_ubes.erase(l);
                this->uncommited_block_map_.erase(m);
                // TODO (dmeister): We should here fail all depended block changes
            }
            failed_callbacks.clear();


            list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator> new_callback_ubes;

            // search for each committed block entry all blocks that are now commitable
            for (list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::iterator j = callback_ubes.begin();
                    j != callback_ubes.end();
                    j++) {
                std::multimap<uint64_t, UncommitedBlockEntry>::iterator& bi(*j);
                UncommitedBlockEntry& block_change(bi->second);
                DEBUG("Process committed block: " << block_change.DebugString());

                std::list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::iterator i;
                for (i = block_change.block_list_.begin(); i != block_change.block_list_.end(); i++) {
                    std::multimap<uint64_t, UncommitedBlockEntry>::iterator& bi(*i);
                    bi->second.open_predecessor_count_--;

                    DEBUG("Updated uncommitted block entry: " <<
                        bi->second.ShortDebugString() <<
                            ", predecessor change");

                    if  (bi->second.open_container_count() == 0 && bi->second.open_predecessor_count() == 0) {
                        // call back the currently handled block change
                        TRACE("Ready uncommited block entry: " <<
                                bi->second.DebugString());
                        new_callback_ubes.push_back(bi);
                    }
                }
            }

            for (list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>::iterator j = callback_ubes.begin(); j != callback_ubes.end(); j++) {
                std::multimap<uint64_t, UncommitedBlockEntry>::iterator& bi(*j);
                // only delete from block map after it is committed
                // here we delete the successful and the failed entries
                TRACE("Delete from block map " << bi->second.DebugString());
                this->uncommited_block_map_.erase(bi);
            }

            callback_ubes = new_callback_ubes;
        }
    }
    return !failed;
}

bool VolatileBlockStore::AddBlock(
        const BlockMapping& o,
        const BlockMapping& m,
        google::protobuf::Message* extra_message,
        const set<uint64_t>& container_id_set,
        int64_t block_mapping_written_event_log_id,
        VolatileBlockCommitCallback* callback) {
    ProfileTimer total_timer(this->stats_.total_time_);
    ProfileTimer add_timer(this->stats_.add_time_);

    DEBUG("Add block: " <<
        "block " << m.block_id() << 
        ", version " << m.version() <<
        ", event log id " << block_mapping_written_event_log_id);

    // build a list of containers
    list<uint64_t> container_id_list;
    for (set<uint64_t>::iterator j = container_id_set.begin(); j != container_id_set.end(); j++) {
        container_id_list.push_back(*j);
    }

    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_free_),
            "Failed to acquire lock");

    // recheck the container list
    for (list<uint64_t>::iterator i = container_id_list.begin(); i != container_id_list.end(); ) {
        bool should_process = *i != Storage::EMPTY_DATA_STORAGE_ADDRESS &&
                this->container_tracker_.ShouldProcessContainer(*i);
        if (should_process && commit_state_callback_) {
            TRACE("Check commit state by callback: container id " << *i);
            Option<bool> commit_state = commit_state_callback_->Call(*i);
            CHECK(commit_state.valid(), "Failed to to check commit state by callback: container id " << *i);
            if (commit_state.value()) {
                TRACE("Mark container as committed by callback: container id " << *i);
                container_tracker_.ProcessedContainer(*i);
                should_process = false;
            }
        }
        if (!should_process) {
            // already processed (or the empty chunk container)
            i = container_id_list.erase(i);
        } else {
            TRACE("Container " << *i << " is still open");
            i++;
        }
    }

    // Check for open blocks changed for the same block with a lower version
    list<multimap<uint64_t, UncommitedBlockEntry>::iterator> earlier_versions;
    pair<multimap<uint64_t, UncommitedBlockEntry>::iterator, multimap<uint64_t, UncommitedBlockEntry>::iterator> ret =
            this->uncommited_block_map_.equal_range(m.block_id());

    for (multimap<uint64_t, UncommitedBlockEntry>::iterator j = ret.first; j != ret.second; j++) {
        if (j->second.modified_mapping().version() < m.version()) {
            earlier_versions.push_back(j);
        }
    }
    bool hasOpenPredecessors = !earlier_versions.empty();

    // We only call the callback if there are not open container ids
    if (container_id_list.size() == 0 && !hasOpenPredecessors) {
        DEBUG("No open containers or predecessors: "
            "block " << m.block_id() <<
            ", version " << m.version());
        scoped_lock.ReleaseLock();
        if (callback) {
            Walltimer callback_timer;
            CHECK(callback->CommitVolatileBlock(o, m, extra_message, block_mapping_written_event_log_id, true),
                    "Failed to commit volatile block: " << o.DebugString() << ", " << m.DebugString());
            this->stats_.callback_time_.Add(&callback_timer);
        }
        if (extra_message) {
            delete extra_message;
        }
        return true;
    }

    UncommitedBlockEntry ube(o, m, extra_message, block_mapping_written_event_log_id, container_id_list.size(), earlier_versions.size());
    DEBUG("Add block list entry: " <<
            ube.ShortDebugString() <<            
            ", container list [" << dedupv1::base::strutil::Join(container_id_list.begin(), container_id_list.end(), ",") << "]");

    std::multimap<uint64_t, UncommitedBlockEntry>::iterator nbi = this->uncommited_block_map_.insert(make_pair(ube.modified_mapping().block_id(), ube));

    /*
     * Now copy of block mapping is correctly inserted into the uncommitted block list
     */
    for (list<uint64_t>::iterator i = container_id_list.begin(); i != container_id_list.end(); i++) {
        uint64_t container_id = *i;
        TRACE("Search container entry " << container_id);
        this->uncommited_container_map_[container_id].block_list_.push_back(nbi);
    }

    // insert new block to all block changed for the same block with a block id
    list<multimap<uint64_t, UncommitedBlockEntry>::iterator>::iterator evi;
    for (evi = earlier_versions.begin(); evi != earlier_versions.end(); evi++) {
        (*evi)->second.block_list_.push_back(nbi);
    }

    return true;
}

Option<bool> VolatileBlockStore::IsVolatileBlock(uint64_t block_id) {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_free_),
            "Failed to acquire lock");
    bool v = this->uncommited_block_map_.find(block_id) != this->uncommited_block_map_.end();
    return make_option(v);
}

uint32_t VolatileBlockStore::GetBlockCount() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_free_),
            "Failed to acquire lock");
    uint32_t block_count = this->uncommited_block_map_.size();
    return block_count;
}

uint32_t VolatileBlockStore::GetContainerCount() {
    ScopedLock scoped_lock(&this->lock_);
    CHECK(scoped_lock.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_free_),
            "Failed to acquire lock");
    uint32_t container_count = this->uncommited_container_map_.size();
    return container_count;
}

string VolatileBlockStore::PrintTrace() {
    uint32_t block_count = 0;
    uint32_t container_count = 0;
    {
        ScopedLock scoped_lock(&this->lock_);
        CHECK_RETURN(scoped_lock.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_free_),
                "{}", "Failed to acquire lock");
        block_count = this->uncommited_block_map_.size();
        container_count = this->uncommited_container_map_.size();
    }
    stringstream sstr;
    sstr << "{";
    sstr << "\"block count\": " << block_count << "," << std::endl;
    sstr << "\"container count\": " << container_count << std::endl;
    sstr << "}";
    return sstr.str();
}

string VolatileBlockStore::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"lock free\": " << this->stats_.lock_free_ << "," << std::endl;
    sstr << "\"lock busy\": " << this->stats_.lock_busy_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string VolatileBlockStore::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"total time\": " << this->stats_.total_time_.GetSum() << "," << std::endl;
    sstr << "\"add time\": " << this->stats_.add_time_.GetSum() << "," << std::endl;
    sstr << "\"callback time\": " << this->stats_.callback_time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

#ifdef DEDUPV1_TEST
void VolatileBlockStore::ClearData() {
    uncommited_block_map_.clear();
    uncommited_container_map_.clear();
}
#endif

VolatileBlockCommitCallback::VolatileBlockCommitCallback() {
}

VolatileBlockCommitCallback::~VolatileBlockCommitCallback() {
}

string UncommitedBlockEntry::DebugString() const {
    stringstream sstr;
    sstr << "[" << original_mapping_.DebugString() << " >> " << modified_mapping_.DebugString() <<
            ", open container count " << open_container_count_<<
            ", open predecessor block count " << open_predecessor_count_ << "]";
    return sstr.str();
}

string UncommitedBlockEntry::ShortDebugString() const {
    stringstream sstr;
    sstr << "block " << modified_mapping().block_id() <<
            ", version " << modified_mapping().version() <<
            ", open container count " << open_container_count_<<
            ", open predecessor block count " << open_predecessor_count_ << "]";
    return sstr.str();
}

}
}
