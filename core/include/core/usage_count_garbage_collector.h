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

#ifndef USAGE_COUNT_GARBAGE_COLLECTOR_H__
#define USAGE_COUNT_GARBAGE_COLLECTOR_H__

#include <core/garbage_collector.h>
#include <core/container_storage.h>
#include <core/statistics.h>
#include <core/block_index.h>
#include <core/volatile_block_store.h>
#include <base/thread.h>
#include <base/locks.h>
#include <base/profile.h>
#include <base/threadpool.h>

#include <gtest/gtest_prod.h>

#include <set>
#include <string>
#include <tbb/atomic.h>
#include <tbb/task_scheduler_init.h>

using dedupv1::base::Profile;

namespace dedupv1 {
namespace gc {

/**
 * Reference-Counting Garbage collection of the dedup system.
 *
 * The reference-counting garbage collection is not compatible with
 * a sparse chunk index filter configuration.
 *
 * The deduplication garbage collection works in the following steps:
 * - If a block is written and all containers of it are committed, a
 *   BlockMappingWritten with a commit flag is written. If a block mapping
 *   is written, but not all container of the block chunks are already
 *   committed, a BlockMappingComitted event is committed at a later point.
 *   These messages contain the original block mapping and the new block mapping.
 *
 * - The gc processes these events during a background log processing and
 *   calculates a diff. The diff contains informations about the chunks used
 *   more in the new mapping (usage count increase) or are not used or used less
 *   in the new block mapping (usage count decrease). These update usage
 *   counts are written to the chunk index.
 *
 * -
 */
class UsageCountGarbageCollector : public GarbageCollector,
    public dedupv1::log::LogConsumer,
    public dedupv1::IdleTickConsumer {
private:
    friend class UsageCountGarbageCollectorTest;
    friend class ProcessMappingDiffTask;
    friend class ProcessMappingDirtyDiffTask;
    friend class ProcessMappingDiffParentTask;
    FRIEND_TEST(UsageCountGarbageCollectorTest, TriggerByStartProcessing);
    FRIEND_TEST(UsageCountGarbageCollectorTest, TriggerByIdleStart);
    DISALLOW_COPY_AND_ASSIGN(UsageCountGarbageCollector);

    static const int kMaxWaitingTime = 60;

    /**
     * Enumeration about the states of the gc.
     */
    enum state {
        CREATED, // !< CREATED
        STARTED, // !< STARTED
        RUNNING, // !< RUNNING
        CANDIDATE_PROCESSING, // !< CANDIDATE_PROCESSING
        STOPPING,
        STOPPED
        // !< STOPPED
    };

    /**
     * Statistics about the gc
     */
    class Statistics {
public:
        /**
         * Constructor
         */
        Statistics();

        /**
         * number of processed blocks
         */
        tbb::atomic<uint64_t> processed_blocks_;

        /**
         * number of processed gc candidates
         */
        tbb::atomic<uint64_t> processed_gc_candidates_;

        /**
         * number of skipped chunks in blocks because of the references
         * container is not available.
         */
        tbb::atomic<uint64_t> skipped_chunk_mapping_count_;

        /**
         * time spend active in the gc thread
         */
        Profile gc_thread_time_;

        /**
         * time spend with log replay
         */
        Profile log_replay_time_;

        /**
         * time spend with direct log replay
         */
        Profile direct_log_replay_time_;

        /**
         * time spend with dirty start log replay
         */
        Profile dirty_start_log_replay_time_;

        /**
         * time spend processing the diff data
         */
        Profile diff_replay_time_;

        /**
         * time spend in the Put method
         */
        Profile update_index_time_;

        tbb::atomic<uint64_t> already_processed_chunk_count_;

        tbb::atomic<uint64_t> processed_chunk_count_;
    };

    dedupv1::InfoStore* info_store_;

    /**
     * Reference to the chunk index
     */
    dedupv1::chunkindex::ChunkIndex* chunk_index_;

    /**
     * Reference to the storage subsystem
     */
    dedupv1::chunkstore::ContainerStorage* storage_;

    /**
     * Reference to the idle detector
     */
    IdleDetector* idle_detector_;

    /**
     * Reference to the log
     */
    dedupv1::log::Log* log_;

    /**
     * Block size of all blocks in the system
     */
    size_t block_size_;

    /**
     * index of all garbage collection candidate.
     * The key is a combination of the container id and the fingerprint
     * of the candidate. The size of therefore
     * fingerprint size + 8 bytes. the container id is stored in the
     * higher-value bits. This is done to
     * process all fingerprints of a container in a series, which increases
     * the caching.
     *
     * Being a gc candidate does not mean that the chunk will be removed
     * because of other log entries that
     * will increase the reference counter and because of concurrent
     * requests. The gc has to recheck the
     * candidate state later in a safe situation (e.g. no concurrent requests).
     *
     */
    dedupv1::base::PersistentIndex* candidate_info_;

    /**
     * Lock used to protect the candidate infos against concurrent access
     */
    dedupv1::base::MutexLock candidate_info_lock_;

    /**
     * state of the garbage collection system
     */
    tbb::atomic<enum state> state_;

    /**
     * if true the Garbage Collector is paused and will not start
     * processing in idle time
     */
    tbb::atomic<bool> paused_;

    /**
     * Background thread that performs the garbage collection
     */
    dedupv1::base::Thread<bool> gc_thread_;

    /**
     * Condition that is fired on state changed.
     * The condition system is used to prevent that state changed are only
     * recognized after at most
     * kMaxWaitingTime seconds.
     */
    dedupv1::base::Condition gc_condition_;

    /**
     * Lock used by the gc condition
     */
    dedupv1::base::MutexLock gc_lock_;

    /**
     * Statistics about the gc
     */
    Statistics stats_;

    /**
     * maximal time in seconds that the gc might use for processing a gc
     * candidate container.
     */
    double max_candidate_processing_time_;

    /**
     * pointer to the thread pool.
     * NULL before Start().
     */
    dedupv1::base::Threadpool* tp_;

    /**
     * A set of block failed event that are replayed. The set value is the
     * log id of the matching block mapping written event. This set is used
     * to ensure
     * exactly-once semantics for block mapping failed events.
     *
     * The data structure has to be persisted between calls. The data
     * structure is cleared when
     * the log is empty.
     */
    std::set<int64_t> replayed_block_failed_event_set_;

    /**
     * Thread main loop of the gc.
     */
    bool GCLoop();
    bool TriggerGC();

    /**
     * processes a GC candidate.
     * The method requires that gc candidate index lock is held.
     *
     * @param candidate_data
     * @return true iff ok, otherwise an error has occurred.
     */
    bool ProcessGCCandidate(GarbageCollectionCandidateData* candidate_data,
                            bool* changed);

    /**
     * Core handling of the candidate processing.
     * The interaction of the candidate processing with the candidate info
     * database is done
     * in the ProcessGCCandidate that usually calls this method.
     */
    bool DoProcessGCCandidate(GarbageCollectionCandidateData* candidate_data,
                              bool* changed);

    /**
     * Processes a given candidate item.
     * @param candidate_data full data of the candidate
     * @param item item to process
     * @param del: if the first item is true, the item should be deleted from the candidate data, if the second
     * value is true, the item should be deleted from the storage system.
     * @return true iff ok, otherwise an error has occurred
     */
    bool ProcessGCCandidateItem(
        const GarbageCollectionCandidateData& candidate_data,
        const GarbageCollectionCandidateItemData& item,
        std::pair<bool, bool>* del);

    /**
     * Processes a given list of possible ophran chunks.
     * We call chunks ophrans when they are involved in write request resulting in an error. Often
     * these chunks have not representation in a block mapping event of some kind. Therefore these
     * chunks are not seen by the gc. We use this explicit ophran chunk event to process these chunks.
     */
    bool
    ProcessOphranChunks(const OphranChunksEventData& event_data,
                        const dedupv1::log::LogReplayContext& context);

    enum dedupv1::base::lookup_result ProcessGCCandidates();

    bool ProcessBlockMapping(
        const dedupv1::blockindex::BlockMappingPair& mapping_pair,
        const dedupv1::log::LogReplayContext& context);

    /**
     * Processes a deleted block mapping.
     *
     * processing a delete block mapping is (from the GC standpoint)
     * equivalent to processing
     * the original mapping with an empty block mapping (zeros).
     * @param orig_mapping
     * @param context log context
     * @return true iff ok, otherwise an error has occurred
     */
    bool ProcessDeletedBlockMapping(
        const dedupv1::blockindex::BlockMapping& orig_mapping,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessDeletedBlockMappingDirect(
        const dedupv1::blockindex::BlockMapping& orig_mapping,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessDeletedBlockMappingDirtyStart(
        const dedupv1::blockindex::BlockMapping& orig_mapping,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessFailedBlockMapping(
        const dedupv1::blockindex::BlockMappingPair& mapping_pair,
        dedupv1::base::Option<int64_t> write_event_committed,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessFailedBlockMappingDirect(
        const dedupv1::blockindex::BlockMappingPair& mapping_pair,
        dedupv1::base::Option<int64_t> write_event_committed,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessFailedBlockMappingDirtyStart(
        const dedupv1::blockindex::BlockMappingPair& mapping_pair,
        dedupv1::base::Option<int64_t> write_event_committed,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessBlockMappingDirect(
        const dedupv1::blockindex::BlockMappingPair& mapping_pair,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessBlockMappingDirtyStart(
        const dedupv1::blockindex::BlockMappingPair& mapping_pair,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessDiffDirtyStart(
        dedupv1::chunkindex::ChunkMapping* mapping,
        uint64_t block_id,
        int usage_modifier,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessDiffDirect(
        dedupv1::chunkindex::ChunkMapping* mapping,
        uint64_t block_id,
        int usage_modifier,
        const dedupv1::log::LogReplayContext& context);

    /**
     * Calculates the difference between the two block mappings.
     *
     * @param original_block_mapping
     * @param modified_block_mapping
     * @param diff
     * @return true iff ok, otherwise an error has occurred
     */
    bool Diff(const dedupv1::blockindex::BlockMapping& original_block_mapping,
              const dedupv1::blockindex::BlockMapping& modified_block_mapping,
              std::map<bytestring, std::pair<int,
                                             uint64_t> >* diff);

    bool ProcessBlockMappingParallel(
        const std::map<bytestring, std::pair<int, uint64_t> >& diff,
        bool invert_failed_write,
        const dedupv1::log::LogReplayContext& context);

    bool DumpMetaInfo();

    bool ReadMetaInfo();
public:
    /**
     * Constructor
     * @return
     */
    UsageCountGarbageCollector();

    /**
     * Destructor
     * @return
     */
    virtual ~UsageCountGarbageCollector();

    static GarbageCollector* CreateGC();

    static void RegisterGC();

    /**
     * Starts the gc.
     *
     * @param system
     * @param start_context
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool Start(const dedupv1::StartContext& start_context, DedupSystem* system);

    /**
     * Runs the gc background thread(s)
     * @return true iff ok, otherwise an error has occurred
     */
    bool Run();

    /**
     * Stops the gc background thread(s)
     * @return true iff ok, otherwise an error has occurred
     */
    bool Stop(const dedupv1::StopContext& stop_context);

    /**
     * Configures the gc
     *
     * Available options:
     * - type: String
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Closes the gc and frees all its resources
     * @return true iff ok, otherwise an error has occurred
     */
    bool Close();

    /**
     * Log event listener method. The gc listens to all events that chance a block mapping.
     *
     * @param event_type
     * @param event_value
     * @param event_size
     * @param context
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool LogReplay(dedupv1::log::event_type event_type,
                           const LogEventData& event_value,
                           const dedupv1::log::LogReplayContext& context);

    /**
     * Checks if the given fingerprint is a gc candidate. This method is e.g. used by
     * dedupv1 check
     */
    virtual dedupv1::base::Option<bool> IsGCCandidate(uint64_t address,
                                                      const void* fp,
                                                      size_t fp_size);

    /**
     * returns the gc candidate info
     * @return
     */
    dedupv1::base::PersistentIndex* candidate_info();

    virtual bool PersistStatistics(std::string prefix,
                                   dedupv1::PersistStatistics* ps);

    virtual bool RestoreStatistics(std::string prefix,
                                   dedupv1::PersistStatistics* ps);

    /**
     * prints statistics about the gc
     * @return
     */
    virtual std::string PrintStatistics();

    /**
     * prints trace statistics about the gc
     * @return
     */
    virtual std::string PrintTrace();

    /**
     * prints profile statistics
     * @return
     */
    virtual std::string PrintProfile();

    /**
     * prints lock statistics
     * @return
     */
    virtual std::string PrintLockStatistics();

    /**
     * Manually starts the processing
     */
    bool StartProcessing();

    /**
     * Manually stops the processing
     */
    bool StopProcessing();

    /**
     * Set the garbage collector in pause mode.
     *
     * If dedupv1 is running and processing, processing will be stopped. It will not be started in idle time.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool PauseProcessing();

    /**
     * Leave the pause mode.
     *
     * This method does not change the State of the collector, so it will stay as before. If the dedupv1 is
     * idle while this method is called, it will not start processing until the next idle time starts.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool ResumeProcessing();

    /**
     * the gc is currently processing, iff true is returned.
     */
    bool IsProcessing();

    /**
     * Called when the system is idle. The gc then usually start processing gc candidates
     */
    virtual void IdleStart();

    /**
     * Called when the idle time of the system ended. The gc then usually should stop
     */
    virtual void IdleEnd();

    /**
     * Stores new gc candidates
     *
     * @param gc_chunks
     * @return true iff ok, otherwise an error has occurred
     */
    bool PutGCCandidates(
        const std::multimap<uint64_t, dedupv1::chunkindex::ChunkMapping>& gc_chunks,
        bool failed_mode);

#ifdef DEDUPV1_CORE_TEST
    /**
     * Closes all indexes to allow crash-like tests.
     */
    void ClearData();

    /**
     * if true there were no GC Candidates during last run in GCLoop
     *
     * This used for Testing only, as it gives only an insight, if the system is idle and the Log is completly replayed before.
     */
    tbb::atomic<bool> no_gc_candidates_during_last_try_;
#endif
};

}
}

#endif  // USAGE_COUNT_GARBAGE_COLLECTOR_H__
