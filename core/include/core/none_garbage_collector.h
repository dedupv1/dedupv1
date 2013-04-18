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

#ifndef NONE_GARBAGE_COLLECTOR_H__
#define NONE_GARBAGE_COLLECTOR_H__

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
 * None Garbage collection of the dedup system.
 *
 * The only think it does is to update the block hint if
 * the chunk is already in the chunk index.
 */
class NoneGarbageCollector : public GarbageCollector,
    public dedupv1::log::LogConsumer,
    public dedupv1::IdleTickConsumer {
private:
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

    };

    dedupv1::InfoStore* info_store_;

    /**
     * Reference to the chunk index
     */
    dedupv1::chunkindex::ChunkIndex* chunk_index_;

    /**
     * Reference to the idle detector
     */
    IdleDetector* idle_detector_;

    /**
     * Reference to the log
     */
    dedupv1::log::Log* log_;

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
     * Lock used by the gc condition
     */
    dedupv1::base::MutexLock gc_lock_;

    /**
     * Statistics about the gc
     */
    Statistics stats_;

    /**
     * pointer to the thread pool.
     * NULL before Start().
     */
    dedupv1::base::Threadpool* tp_;

    uint32_t block_size_;

    bool ProcessBlockMappingDirect(
        const dedupv1::blockindex::BlockMappingPair& mapping_pair,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessBlockMappingDirtyStart(
        const dedupv1::blockindex::BlockMappingPair& mapping_pair,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessDiffDirtyStart(
        dedupv1::chunkindex::ChunkMapping* mapping,
        uint64_t block_id,
        const dedupv1::log::LogReplayContext& context);

    bool ProcessDiffDirect(
        dedupv1::chunkindex::ChunkMapping* mapping,
        uint64_t block_id,
        const dedupv1::log::LogReplayContext& context);

public:
    /**
     * Constructor
     * @return
     */
    NoneGarbageCollector();

    /**
     * Destructor
     * @return
     */
    virtual ~NoneGarbageCollector();

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

#endif
};

}
}

#endif  // NONE_COUNT_GARBAGE_COLLECTOR_H__
