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

#ifndef VOLATILE_BLOCK_STORE_H__
#define VOLATILE_BLOCK_STORE_H__

#include <list>
#include <string>
#include <map>
#include <set>
#include <tr1/tuple>

#include <gtest/gtest_prod.h>

#include <tbb/atomic.h>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/block_mapping.h>
#include <base/locks.h>
#include <core/container_tracker.h>
#include <base/profile.h>
#include <core/log.h>
#include <core/chunk_mapping.h>
#include <base/option.h>
#include <core/statistics.h>
#include <base/callback.h>

namespace dedupv1 {

class DedupSystem;

namespace blockindex {

/**
 * This callback is for each block mapping with all referenced container
 * are now committed as the VolatileBlockStore processes a newly committed container.
 */
class VolatileBlockCommitCallback {
    private:
        DISALLOW_COPY_AND_ASSIGN(VolatileBlockCommitCallback);
    public:
        /**
         * Constructor
         * @return
         */
        VolatileBlockCommitCallback();

        /**
         * Destructor
         * @return
         */
        virtual ~VolatileBlockCommitCallback();

        /**
         * The block mapping is now ready in the sense that all referenced containers
         * are now committed.
         *
         * @param original_mapping
         * @param modified_mapping
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool CommitVolatileBlock(const BlockMapping& original_mapping,
                const BlockMapping& modified_mapping,
                const google::protobuf::Message* extra_message,
                int64_t event_log_id,
                bool direct) = 0;

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool FailVolatileBlock(const BlockMapping& original_mapping,
                const BlockMapping& modified_mapping,
                const google::protobuf::Message* extra_message,
                int64_t event_log_id) = 0;
};

/**
 * Internal class for the VolatileBlockStore that saves
 * the metadata about a block mapping with uncommitted containers.
 */
class UncommitedBlockEntry {
    private:
        /**
         * Number of containers referenced by the block mapping
         * that are not-committed yet.
         * if the container count reaches zero, the block mapping can be
         * committed.
         */
        uint32_t open_container_count_;

        /**
         * Number of version of the same block not yet committed.
         * A block should not be committed unless all earlier versions are also committed.
         * If a earlier version fails, this block should also be marked as failed.
         */
        uint32_t open_predecessor_count_;

        /**
         * Copy of the original block mapping
         */
        BlockMapping original_mapping_;

        /**
         * Copy of the modified block mapping that is open
         */
        BlockMapping modified_mapping_;

        google::protobuf::Message* extra_message_;

        /**
         * event log id of the Block Mapping Written event associated with this
         * update
         */
        uint64_t block_mapping_written_event_log_id_;

        /**
         * list of uncommitted block entries that reference
         * this block
         */
        std::list< std::multimap<uint64_t, UncommitedBlockEntry>::iterator> block_list_;
    public:
        /**
         * Constructor.
         *
         * @param original_mapping original version of the uncommitted block mapping
         * @param modified_mapping modified version of the block mapping.
         * @param open_container_count
         * @param predecessor_count number of earlier block mapping versions of the same block that are currently not
         * committable.
         *
         * @return
         */
        UncommitedBlockEntry(const BlockMapping& original_mapping,
                const BlockMapping& modified_mapping,
                google::protobuf::Message* extra_message,
                uint64_t block_mapping_written_event_log_id,
                uint32_t open_container_count,
                uint32_t open_predecessor_count);

        /**
         * returns the number of open containers that the
         * modified block mapping is waiting to be committed
         * @return
         */
        inline uint32_t open_container_count() const;

        /**
         * returns the number of open predecessor blocks that the
         * modified block mapping is waiting to be committed
         * @return
         */
        inline uint32_t open_predecessor_count() const;

        /**
         * returns the modified block mapping
         * @return
         */
        inline const BlockMapping& modified_mapping() const;

        /**
         * returns the original block mapping.
         * @return
         */
        inline const BlockMapping& original_mapping() const;

        inline const std::list< std::multimap<uint64_t, UncommitedBlockEntry>::iterator>& block_list() const;

        inline const google::protobuf::Message* extra_message() const;

        inline void clear_extra_message();

        inline uint64_t block_mapping_written_event_log_id() const;

        /**
         * returns a develop-readable representation of the uncommitted block entry.
         */
        std::string DebugString() const;
        
        std::string ShortDebugString() const;

        friend class VolatileBlockStore;
};

/**
 * internal class of the volatile block mapping that
 * represents a uncommitted container that is
 * referenced by an open block mapping.
 */
class UncommitedContainerEntry {
    private:

        /**
         * list of uncommitted block entries that reference
         * this container
         */
        std::list< std::multimap<uint64_t, UncommitedBlockEntry>::iterator> block_list_;
    public:

        /**
         * returns a list of uncommitted block entry
         * @return
         */
        inline const std::list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>& block_list() const;

        inline std::list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>& mutable_block_list();

        friend class VolatileBlockStore;
};

/**
 * Statistics about the volatile block store
 */
struct volatile_block_store_statistics {
        /**
         * Number of times that the volatile block store lock
         * is free.
         */
        tbb::atomic<uint32_t> lock_free_;

        /**
         * Number of times that the volatile block store lock
         * is busy.
         */
        tbb::atomic<uint32_t> lock_busy_;

        /**
         * Total time spend in the volatile block store
         */
        dedupv1::base::Profile total_time_;

        dedupv1::base::Profile add_time_;

        /**
         * Total time spend during callbacks
         */
        dedupv1::base::Profile callback_time_;
};

/**
 * The volatile block store is used by the block index to
 * manage which blocks have open aka uncommitted chunks.
 *
 * Thread safety:
 * The volatile block store can be used from
 * multiple threads concurrently.
 */
class VolatileBlockStore : public dedupv1::StatisticProvider {
    private:
    friend class VolatileBlockStorageTest;
    FRIEND_TEST(VolatileBlockStorageTest, SimpleAddBlock);
    FRIEND_TEST(VolatileBlockStorageTest, SimpleAddRemoveBlock);
    FRIEND_TEST(VolatileBlockStorageTest, AddBlockTwoContainers);
    FRIEND_TEST(VolatileBlockStorageTest, AddTwoBlockTwoContainers);

    DISALLOW_COPY_AND_ASSIGN(VolatileBlockStore);

    /**
     * Map from a block id to UncommitedBlockEntry entries for all uncommitted blocks
     */
    std::multimap<uint64_t, UncommitedBlockEntry> uncommited_block_map_;

    /**
     * Map from the container id to a pointers to UncommitedContainerEntry entries for all referenced uncommitted containers
     */
    std::map<uint64_t, UncommitedContainerEntry> uncommited_container_map_;

    /**
     * Mutex to ensure mutual exclusion.
     * Protected the uncommited block map and the uncommitted container map
     */
    dedupv1::base::MutexLock lock_;

    /**
     * Statistics about the volatile block store
     */
    struct volatile_block_store_statistics stats_;

    /**
     * tracks which containers are processed by the
     * container tracker
     */
    ContainerTracker container_tracker_;

    /**
     * Optional callback that can be used to perform an additional check if a container is already committed.
     * This is done when the container tracker do not contain an up-to-date state. We use it during the dirty replay
     */
    dedupv1::base::Callback1<dedupv1::base::Option<bool>, uint64_t>* commit_state_callback_;

    void HandleVolatileFailChange(std::list<std::tr1::tuple<BlockMapping, BlockMapping, const google::protobuf::Message*, int64_t> >* callback_ubes,
            const std::multimap<uint64_t, UncommitedBlockEntry>::iterator& bi);
    public:
    /**
     * Constructor.
     *
     * @return
     */
    VolatileBlockStore();

    virtual ~VolatileBlockStore();

    /**
     * Clears the volatile block store
     *
     * @return
     */
    bool Clear();

    bool ResetTracker();

    /**
     * Marks a block mapping as using uncommited data and stored a copy of the mapping to save the data when the container becomes committed.
     *
     * TODO (dmeister): The error conditions are not handled correctly.
     * @param extra_message Ownership goes over the volatile block store if true is returned
     * @return true iff ok, otherwise an error has occurred
     */
    bool AddBlock(const BlockMapping& original_mapping,
            const BlockMapping& modified_mapping,
            google::protobuf::Message* extra_message,
            const std::set<uint64_t>& container_id_set,
            int64_t block_mapping_written_event_log_id_,
            VolatileBlockCommitCallback* callback);

    /**
     * Should be called when a container is committed.
     * The callback method is called when a block hasn't any
     * uncommitted chunk given that the container_id is now
     * committed.
     *
     * @param container_id
     * @param callback
     * @return true iff ok, otherwise an error has occurred
     */
    bool Commit(uint64_t container_id, VolatileBlockCommitCallback* callback);

    /**
     * Should be called when a container is aborted because
     * the commit failed.
     *
     * @param container_id
     * @param callback
     * @return true iff ok, otherwise an error has occurred
     */
    bool Abort(uint64_t container_id, VolatileBlockCommitCallback* callback);

    /**
     * returns trace statistics about the volatile block store
     * @return
     */
    std::string PrintTrace();

    /**
     * returns lock statistics about the volatile block store
     * @return
     */
    std::string PrintLockStatistics();

    /**
     * results profile information about the volatile block store
     * @return
     */
    std::string PrintProfile();

    /**
     * returns the container tracker of the volatile block store
     * @return
     */
    inline ContainerTracker* GetContainerTracker();

    /**
     * Checks if a given block id is currently volatile that is an open
     * block mapping of the block id is currently stored in the volatile
     * block store.
     *
     * The call of this method is thread-safe, but it might block.
     *
     * @param block_id
     * @return
     */
    dedupv1::base::Option<bool> IsVolatileBlock(uint64_t block_id);

    /**
     * May block
     * @return
     */
    uint32_t GetBlockCount();

    /**
     * May block
     * @return
     */
    uint32_t GetContainerCount();

    /**
     * returns the map of uncommitted block mappings
     */
    const std::multimap<uint64_t, UncommitedBlockEntry>& uncommited_block_map() const {
        return uncommited_block_map_;
    }

    /**
     * Sets the commit state check callback.
     * The ownership on the callback is not transferred
     * Note: The method is called in a context where the container tracker lock is held, be aware of deadlocks
     */
    dedupv1::base::Callback1<dedupv1::base::Option<bool>, uint64_t>* set_commit_state_check_callback(dedupv1::base::Callback1<dedupv1::base::Option<bool>, uint64_t>* callback) {
        dedupv1::base::Callback1<dedupv1::base::Option<bool>, uint64_t>* old_callback = commit_state_callback_;
        commit_state_callback_ = callback;
        return old_callback;
    }

    dedupv1::base::Callback1<dedupv1::base::Option<bool>, uint64_t>* commit_state_check_callback() const{
        return commit_state_callback_;
    }

#ifdef DEDUPV1_TEST
    void ClearData();
#endif
};

uint32_t UncommitedBlockEntry::open_container_count() const {
    return open_container_count_;
}

uint32_t UncommitedBlockEntry::open_predecessor_count() const {
    return open_predecessor_count_;
}

const BlockMapping& UncommitedBlockEntry::modified_mapping() const {
    return modified_mapping_;
}

const BlockMapping& UncommitedBlockEntry::original_mapping() const {
    return original_mapping_;
}


const std::list< std::multimap<uint64_t, UncommitedBlockEntry>::iterator>& UncommitedBlockEntry::block_list() const {
    return block_list_;
}

const google::protobuf::Message* UncommitedBlockEntry::extra_message() const {
    return extra_message_;
}

void UncommitedBlockEntry::clear_extra_message() {
    extra_message_ = NULL;
}

uint64_t UncommitedBlockEntry::block_mapping_written_event_log_id() const {
    return block_mapping_written_event_log_id_;
}

ContainerTracker* VolatileBlockStore::GetContainerTracker() {
    return &this->container_tracker_;
}

const std::list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>& UncommitedContainerEntry::block_list() const {
    return block_list_;
}

std::list<std::multimap<uint64_t, UncommitedBlockEntry>::iterator>& UncommitedContainerEntry::mutable_block_list() {
    return block_list_;
}

}
}

#endif  // VOLATILE_BLOCK_STORE_H__
