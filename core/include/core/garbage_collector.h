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

#ifndef GARBAGE_COLLECTOR_H__
#define GARBAGE_COLLECTOR_H__

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
 * Abstract class for all garbage collection implementations
 */
class GarbageCollector : public dedupv1::StatisticProvider {
public:
    DISALLOW_COPY_AND_ASSIGN(GarbageCollector);
    enum gc_concept {
        NONE,
        USAGE_COUNT,
        MARK_AND_SWEEP
    };

    static MetaFactory<GarbageCollector>& Factory();

    /**
     * Constructor
     */
    GarbageCollector(enum gc_concept concept);

    /**
     * Destructor
     */
    virtual ~GarbageCollector();

    virtual bool Init();

    /**
     * Starts the gc.
     *
     * @param system
     * @param start_context
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(const dedupv1::StartContext& start_context,
                       DedupSystem* system);

    /**
     * Runs the gc background thread(s)
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Run();

    /**
     * Stops the gc background thread(s)
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Stop(const dedupv1::StopContext& stop_context);

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
    virtual bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Closes the gc and frees all its resources
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Close();

    virtual bool StartProcessing();

    virtual bool StopProcessing();

    /**
     * Set the garbage collector in pause mode.
     *
     * If dedupv1 is running and processing, processing will be stopped. It will not be started in idle time.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool PauseProcessing();

    /**
     * Leave the pause mode.
     *
     * This method does not change the State of the collector, so it will stay as before. If the dedupv1 is
     * idle while this method is called, it will not start processing until the next idle time starts.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool ResumeProcessing();

    virtual bool IsProcessing();

    /**
     * Checks if the given fingerprint is a gc candidate. This method is e.g. used by
     * dedupv1 check.
     *
     * It is unclear what the meaning of a GC candidate is for a different
     * garbage collection implementation.
     */
    virtual dedupv1::base::Option<bool> IsGCCandidate(
        uint64_t address,
        const void* fp,
        size_t fp_size);

    /**
     * Stores new gc candidates.
     *
     * It is unclear what the meaning of a GC canidate is for a different
     * garbage collection implementation.
     *
     * @param gc_chunks
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool PutGCCandidates(
        const std::multimap<uint64_t, dedupv1::chunkindex::ChunkMapping>& gc_chunks,
        bool failed_mode);

    virtual dedupv1::base::PersistentIndex* candidate_info();

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

#ifdef DEDUPV1_CORE_TEST
    /**
     * Closes all indexes to allow crash-like tests.
     */
    virtual void ClearData();
#endif

    inline enum gc_concept gc_concept() const;
private:
    const enum gc_concept gc_concept_;

    static MetaFactory<GarbageCollector> factory_;
};

enum GarbageCollector::gc_concept GarbageCollector::gc_concept() const {
    return gc_concept_;
}

}
}

#endif  // GARBAGE_COLLECTOR_H__
