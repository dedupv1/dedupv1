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

#ifndef DEDUP_SYSTEM_H__
#define DEDUP_SYSTEM_H__

#include <tbb/atomic.h>
#include <tbb/spin_rw_mutex.h>

#include <core/dedup.h>
#include <core/dedupv1_scsi.h>
#include <base/locks.h>
#include <core/request.h>
#include <core/block_locks.h>
#include <base/profile.h>
#include <core/block_index.h>
#include <core/storage.h>
#include <core/chunk_store.h>
#include <core/filter_chain.h>
#include <core/content_storage.h>
#include <core/session_management.h>
#include <base/resource_management.h>
#include <core/session.h>
#include <core/log.h>
#include <core/chunk.h>
#include <core/chunk_index.h>
#include <core/dedup_volume_info.h>
#include <core/garbage_collector.h>
#include <core/idle_detector.h>
#include <core/statistics.h>
#include <core/info_store.h>
#include <base/threadpool.h>

#include <string>

namespace dedupv1 {

/**
 * This class represents a DedupSystem as whole.
 *
 * DedupSystem is the heart of the dedupv1 system. I holds the references to the subsystems.
 *
 * This class is designed to be used as singleton. It follows the rules of \ref life_cycle
 * with own threads.
 */
class DedupSystem : public dedupv1::StatisticProvider {
    public:
    DISALLOW_COPY_AND_ASSIGN(DedupSystem);

    static const uint32_t kDefaultLogFullPauseTime;

    static const dedupv1::scsi::ScsiResult kFullError;
    static const dedupv1::scsi::ScsiResult kReadChecksumError;

    /**
     * Default: 256KB
     */
    static const uint32_t kDefaultBlockSize;

    static const uint32_t kDefaultSessionCount;


    /**
     * State of the dedup system
     */
    enum state {
        CREATED,
        STARTED,
        RUNNING,
        STOPPED
    };

    /**
     * Type for statistics about the dedup system
     */
    class Statistics {
        public:
            Statistics();
            /**
             * Profiling information
             */
            dedupv1::base::Profile profiling_total_;

            /**
             * Number of concurrently active dedup sessions, aka active request threads.
             */
            tbb::atomic<uint32_t> active_session_count_;

            /**
             * Active sessions, with a valid block lock
             */
            tbb::atomic<uint32_t> processsed_session_count_;

            /**
             * Number of requests running longer than 1s
             */
            tbb::atomic<uint64_t> long_running_request_count_;

            /**
             * Average number of ms a request is waiting for a block lock.
             */
            dedupv1::base::SimpleSlidingAverage average_waiting_time_;
    };
    private:
    /**
     * Reference to the chunk index.
     * The chunk index is initially NULL and it is set during the configuration phase.
     * It is ensures that a started deduplication system has a chunk index.
     */
    dedupv1::chunkindex::ChunkIndex* chunk_index_;

    /**
     * Reference to the block index.
     * The block index is initially NULL and it is set during the configuration phase.
     * It is ensures that a started deduplication system has a block index.
     */
    dedupv1::blockindex::BlockIndex* block_index_;

    /**
     * Reference to the chunk store.
     * The chunk store is initially NULL and it is set during the configuration phase.
     * It is ensures that a started deduplication system has a chunk store.
     */
    dedupv1::chunkstore::ChunkStore* chunk_store_;

    /**
     * Reference to the filter chain.
     * The filter chain is set in the Init method. The filters
     * are added during the configuration phase.
     */
    dedupv1::filter::FilterChain* filter_chain_;

    /**
     * Reference to the content storage.
     * The content storage is set in the Init method.
     */
    dedupv1::ContentStorage* content_storage_;

    /**
     * Reference to the dedup system log.
     * The log is set in the Init method.
     */
    dedupv1::log::Log* log_;

    /**
     * Size of the internal block size.
     * This block size is different from the block size an SCSI device.
     * the block size must be larger than 0.
     */
    uint32_t block_size_;

    /**
     * Information about the current volumes in the system.
     * TODO (dmeister) Not clear if the volume management should be part of the base system. Consider
     * a refactoring.
     */
    DedupVolumeInfo* volume_info_;

    /**
     * Reference to the garbage collector.
     *
     * The garbage collector observes the block mappings written, and calculates usage of chunks in the chunk index.
     * If a chunk is not used anymore, the chunk might eventually be removed from the chunk index and the storage
     * might eventually be freed.
     *
     * The garbage collection is optional and it is not the default value.
     */
    dedupv1::gc::GarbageCollector* gc_;

    /**
     * detects if the system can be seen as idle.
     */
    IdleDetector idle_detector_;

    /**
     * Statistics about the dedup system.
     */
    Statistics stats_;

    /**
     * State of the deduplication system.
     */
    enum state state_;

    /**
     * locks to protect the blocks against concurrent accesses
     */
    dedupv1::BlockLocks block_locks_;

    /**
     * Default: false
     */
    bool disable_sync_cache_;

    dedupv1::InfoStore* info_store_;

    /**
     * iff true, the system is readonly. No user-visible changed should be possible.
     */
    bool readonly_;

    /**
     * Threadpool to use by the dedup system and all its children
     */
    dedupv1::base::Threadpool* tp_;

    /**
     * number of times a write request should be retried after an error.
     * Default: 0
     */
    uint32_t write_retry_count_;

    /**
     * number of times a read request should be retried after an error.
     * Default: 0
     */
    uint32_t read_retry_count_;

    bool report_long_running_requests_;

    bool FastBlockCopy(
            uint64_t src_block_id,
            uint64_t src_offset,
            uint64_t target_block_id,
            uint64_t target_offset,
            uint64_t size,
            dedupv1::base::ErrorContext* ec);

	/**
     * Makes a read or write request on the given block
     *
     * @param session session to use
     * @param block_request request information, incl. the block id
     * @param last_block_request true iff this is the last block request in the iSCSI request
     * @param ec error contest (may be NULL)
     */
    bool MakeBlockRequest(
            dedupv1::Session* session,
            Request* block_request,
            bool last_block_request,
            dedupv1::base::ErrorContext* ec);

    dedupv1::scsi::ScsiResult DoMakeRequest(
            dedupv1::Session* session,
            enum request_type rw,
            uint64_t request_index,
            uint64_t request_offset,
            uint64_t size,
            byte* buffer,
            dedupv1::base::ErrorContext* ec);

    public:
    /**
     * Constructor.
     * @return
     */
    DedupSystem();

    /**
     * Destructor
     * @return
     */
    virtual ~DedupSystem();

    /**
     * Loads a configuration from a file. The
     * file is parsed line by line and and not out-commented line (# prefix)
     * is passed to the SetOption method. Each configuration line is assumed
     * to have the pattern OPTION_NAME=OPTION
     *
     * @param filename
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool LoadOptions(const std::string& filename);

    /**
     * Configures the dedup system.
     *
     * Most options are delegates to the specific subsystem.
     *
     * Available options:
     * - block-size: StorageUnit
     * - disable-sync-cache: Boolean
     * - write-retries: uint32_t
     * - read-retries: uint32_t
     * - session-count (is depreciated)
     * - block-index.*
     * - chunk-index
     * - chunk-index.*
     * - storage
     * - storage.*
     * - filter: String
     * - filter.*: String
     * - chunking: String, delete default chunking information
     * - fingerprinting: String, is trimmed by content_storage
     * - content-storage.*: String, is trimmed by content_storage
     * - log.*
     * - gc (is depreciated)
     * - gc.*
     * - idle-detection.*
     * - block-locks.*
     * - report-long-running-requests: Boolean
     * - raw-volume.*
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Starts the deduplication system.
     * A configuration is not allowed after the start. After the start
     * the system should be able to process requests.
     *
     * @param start_context start context
     * @param info_store info store to use
     * @param tp threadpool to use
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(const dedupv1::StartContext& start_context,
            dedupv1::InfoStore* info_store, dedupv1::base::Threadpool* tp);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Run();

    /**
     * Stops the deduplication system.
     *
     * In particular all threads of the dedup system (and its sub systems) should be stopped
     * after this method end.
     *
     * After a system is once stopped, the system must not be able to be restarted.
     * The method may fail if the system was not started before.
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Stop(const dedupv1::StopContext& stop_context);

    /**
     * The make request method splits up the external (iSCSI) request into
     * several internal requests that have a most a size of a internal block.
     * The internal requests are delegated to the content storage component and processed
     * (chunked, fingerprintes, ...) there.
     *
     * @param type indicates if the request is a read or a write request
     * @param request_index index at which the request starts (in terms of an internal block index)
     * @param request_offset offset inside the first index at which the request starts
     * @param size size of the request. the size can (and often is) greater than a single internal block
     * @param buffer buffer that holds (read) or to hold (write) the data. The buffer is assumed to have a size of at
     * least size.
     * @param ec error context (can be NULL):
     * @return
     */
    virtual dedupv1::scsi::ScsiResult MakeRequest(
            dedupv1::Session* session,
            enum request_type type,
            uint64_t request_index,
            uint64_t request_offset,
            uint64_t size,
            byte* buffer,
            dedupv1::base::ErrorContext* ec);

    virtual dedupv1::scsi::ScsiResult FastCopy(
            uint64_t src_block_id,
            uint64_t src_offset,
            uint64_t target_block_id,
            uint64_t target_offset,
            uint64_t size,
            dedupv1::base::ErrorContext* ec);

    virtual dedupv1::base::Option<bool> Throttle(int thread_id, int thread_count);

    virtual dedupv1::scsi::ScsiResult SyncCache();

    virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * Returns statistics about the lock contention as JSON string.
     * @return
     */
    virtual std::string PrintLockStatistics();

    /**
     * Returns statistics about the system as JSON string.
     * @return
     */
    virtual std::string PrintStatistics();

    /**
     * Returns profiling information about the system as JSON string.
     * @return
     */
    virtual std::string PrintProfile();

    /**
     * Returns trace information about the system as JSON strin.g
     * @return
     */
    virtual std::string PrintTrace();

    /**
     * Returns the chunk index.
     * Maybe NULL before the start.
     * @return
     */
    virtual dedupv1::chunkindex::ChunkIndex* chunk_index();

    /**
     * Returns the block index.
     * Maybe NULL before start.
     * @return
     */
    virtual dedupv1::blockindex::BlockIndex* block_index();

    /**
     * Returns the chunk store (aka thin layer above the storage).
     * Maybe NULL before start.
     * @return
     */
    virtual dedupv1::chunkstore::ChunkStore* chunk_store();

    /**
     * Returns the storage directly.
     * The normal write and read requests should always used the chunk store (because of
     * additional checks and processing).
     * Often this method is used if a component bypasses the chunk store if it uses
     * specific functions of the current implementation, e.g. the ContainerStorage.
     *
     * @return
     */
    virtual dedupv1::chunkstore::Storage* storage();

    /**
     * Returns information about the currently configured volumes.
     * Is NULL before start.
     *
     * @return
     */
    virtual DedupVolumeInfo* volume_info();

    /**
     * Returns the content storage component.
     * Should never be NULL.
     *
     * @return
     */
    virtual dedupv1::ContentStorage* content_storage();

    /**
     * Returns the log system.
     * Should never be NULL.
     *
     * @return
     */
    virtual dedupv1::log::Log* log();

    /**
     * Returns the filter chain
     * Should never be NULL
     */
    virtual dedupv1::filter::FilterChain* filter_chain();

    /**
     * Returns the idle detector.
     * May be NULL because a deactivation of the idle detection
     * sets the instance to NULL.
     *
     * @return
     */
    virtual IdleDetector* idle_detector();

    /**
     * Returns the size of the internal block size in bytes.
     *
     * @return
     */
    virtual uint32_t block_size() const;

    /**
     * Register the default implementations of all
     * filter, index, storage implementations.
     */
    static void RegisterDefaults();

    /**
     * returns a valid pointer to block locks.
     * @return
     */
    virtual dedupv1::BlockLocks* block_locks();

    /**
     * returns a valid pointer to the gc.
     * @return
     */
    virtual dedupv1::gc::GarbageCollector* garbage_collector();

    virtual dedupv1::InfoStore* info_store();

    /**
     * Sets the info store.
     * Used for testing.
     *
     * @param info_store
     * @return
     */
    inline bool set_info_store(dedupv1::InfoStore* info_store);

    /**
     * Sets the threadpool
     * Used for testing.
     *
     * @param tp the new thread pool to use
     * @return
     */
    inline bool set_threadpool(dedupv1::base::Threadpool* tp);

    /**
     * Returns the threadpool
     * @return
     */
    inline dedupv1::base::Threadpool* threadpool();

    /**
     * returns the volume with the given id
     *
     * @return if volume with given id doesn't exists or volume info not started, NULL is returned
     */
    inline DedupVolume* GetVolume(uint64_t volume_id) {
        if (volume_info_ == NULL) {
            return NULL;
        }
        return volume_info_->FindVolume(volume_id);
    }

#ifdef DEDUPV1_CORE_TEST
    void ClearData();
#endif
};

dedupv1::base::Threadpool* DedupSystem::threadpool() {
    return tp_;
}

bool DedupSystem::set_threadpool(dedupv1::base::Threadpool* tp) {
    if (tp_) {
        return false;
    }
    tp_ = tp;
    return true;
}

bool DedupSystem::set_info_store(dedupv1::InfoStore* info_store) {
    if (info_store_) {
        return false;
    }
    info_store_ = info_store;
    return true;
}

}

#endif  // DEDUP_SYSTEM_H__
