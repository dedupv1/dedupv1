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

#ifndef BLOCK_INDEX_H__
#define BLOCK_INDEX_H__

#include <core/dedup.h>
#include <core/log.h>
#include <core/volatile_block_store.h>
#include <base/index.h>
#include <core/container_tracker.h>
#include <core/block_index_bg.h>
#include <core/block_locks.h>
#include <base/fileutil.h>
#include <core/storage.h>
#include <core/session.h>
#include <core/chunk_index_in_combat.h>
#include <core/info_store.h>
#include <base/sliding_average.h>
#include <base/startup.h>
#include <core/throttle_helper.h>
#include <core/block_mapping_pair.h>

#include <list>
#include <string>
#include <map>
#include <set>
#include <tr1/tuple>

#include <tbb/concurrent_queue.h>
#include <tbb/atomic.h>
#include <tbb/tick_count.h>

namespace dedupv1 {

class DedupSystem;

/**
 * \namespace dedupv1::blockindex
 * Namespace for classes related to the block index
 */
namespace blockindex {

class BlockIndexBackgroundCommitter;

/**
 * \defgroup blockindex Block Index
 *
 * The block index stores a mapping from an (internal) block index
 * to a chunks that (in the most current) version from the data of the
 * block. In addition, to the fingerprint of the chunks, it stores
 * other chunk metadata.
 */

/**
 * \ingroup blockindex
 *
 * The block index stores a mapping from an (internal) block index
 * to a chunks that (in the most current) version from the data of the
 * block. In addition, to the fingerprint of the chunks, it stores
 * other chunk metadata.
 *
 * The block index is generally thread-safe in the STARTED state. However,
 * there are race conditions for the usage on individual block mappings between
 * the read and the update operations. Use the BlockLocks to protect block mappings
 * against concurrent access.
 *
 * Be careful about deadlocks.
 *
 */
class BlockIndex : public dedupv1::log::LogConsumer, public VolatileBlockCommitCallback, public dedupv1::StatisticProvider {
    DISALLOW_COPY_AND_ASSIGN(BlockIndex);
    friend class BlockIndexBackgroundCommitter;

    static const uint64_t kDefaultHardLimitFactor = 2;
    static const uint64_t KMinimalHardLimit = 32 * 1024;
    static const int kDefaultImportBatchSize = 256;

    /**
     * \ingroup blockindex
     * Structure for the statistics collection of the block index.
     */
    class Statistics {
        public:
            /**
             * Constructor
             */
            Statistics();

            /**
             * Number of block index reads
             */
            tbb::atomic<uint64_t> index_reads_;

            /**
             * Number of block index writes
             */
            tbb::atomic<uint64_t> index_writes_;
            tbb::atomic<uint64_t> index_real_writes_;

            tbb::atomic<uint32_t> ready_map_lock_free_;
            tbb::atomic<uint32_t> ready_map_lock_busy_;

            /**
             * Time spent with reading entries
             */
            dedupv1::base::Profile read_time_;

            /**
             * Time spent with writing entries
             */
            dedupv1::base::Profile write_time_;

            /**
             * Time spent with logging data
             */
            dedupv1::base::Profile log_time_;

            dedupv1::base::Profile open_new_block_check_time_;

            /**
             * Time spent to check the container commit state
             */
            dedupv1::base::Profile check_time_;

            /**
             * Time spent with log replay
             */
            dedupv1::base::Profile replay_time_;

            /**
             * Number of imported blocks
             */
            tbb::atomic<uint64_t> imported_block_count_;

            tbb::atomic<uint64_t> incomplete_imports_;

            /**
             * Number of times a request is throttled down
             */
            tbb::atomic<uint64_t> throttle_count_;

            dedupv1::base::Profile throttle_time_;

            /**
             * Number of failed block writes
             */
            tbb::atomic<uint64_t> failed_block_write_count_;

            /**
             * Average import latency in ms over the last 1k imported blocks.
             */
            dedupv1::base::SimpleSlidingAverage import_latency_;
    };
    public:
    /**
     * Enumerations for the results of internal read functions.
     */
    enum read_result {
        READ_RESULT_ERROR,       //!< BLOCK_INDEX_READ_ERROR
        READ_RESULT_MAIN,     //!< BLOCK_INDEX_MAIN
        READ_RESULT_NOT_FOUND,//!< BLOCK_INDEX_NOT_FOUND
        READ_RESULT_AUX,       //!< BLOCK_INDEX_AUX
        READ_RESULT_SESSION
    };

    /**
     * Enumeration of the states of the block index
     */
    enum state {
        CREATED,//!< BLOCK_INDEX_CREATED
        STARTED,//!< BLOCK_INDEX_STARTED
        RUNNING,
        STOPPED //!< BLOCK_INDEX_STOPPED
    };
    private:

    dedupv1::chunkindex::ChunkIndexInCombats* chunk_in_combats_;

    /**
     * Normal persistent block index.
     *
     * A consistency requirement is that every chunk referenced using the
     * main block index, must be persisted by the chunk index and in the storage.
     */
    dedupv1::base::PersistentIndex* block_index_;

    /**
     * Persistent index containing an entry for all block/version pairs that are failed, but whose
     * failed event is not yet replayed (in the background).
     *
     * The key is a 12-byte combination of the block id and the version. The value
     * is an instance of BlockWriteFailedData.
     *
     * The index is only requested and updated to handle errors.
     */
    dedupv1::base::PersistentIndex* failed_block_write_index_;

    /**
     * In-Memory block index with non-confirmed blocks and for blocks that are confirmed, but not yet
     * persisted to disk.
     *
     * If an block entry in the auxiliary index has the event log id set, it is committable.
     * The event log id is the log id of the associated BlockMappingWritten event.
     *
     * An block mapping is deleted from the auxiliary index in the following situations:
     * - When ImportModifiedBlockMapping is called with the version that is currently
     *   in the auxiliary index. ImportModifiedBlockMapping is called in the background and
     *   when the block index is shutdown.
     * - When a volatile block fails and the version in the auxiliary index is the same
     *   version as of the failed block.
     * - When LogReplayBlockMappingDeleted is called and the log event id is less than
     *   of the deleted block.
     * - When a block mapping write event is replayed
     */
    dedupv1::base::MemoryIndex* auxiliary_block_index_;

    /**
     * Maximal size of the auxiliary block index.
     * The limit should be seen as a soft limit above the system
     * should try to (if possible) to reduce the size of the auxiliary block index,
     * usually by importing already committed container chunks.
     * A value of 0 (default) means that every committed container should
     * be imported immediately.
     */
    uint64_t max_auxiliary_block_index_size_;

    uint64_t auxiliary_block_index_hard_limit_;

    /**
     * Internal block size of the dedup system.
     */
    size_t block_size_;

    /**
     * Reference to the system log.
     * Set inside the start method
     */
    dedupv1::log::Log* log_;

    /**
     * Block statistics
     */
    class Statistics stats_;

    /**
     * Reference to the storage system.
     */
    dedupv1::chunkstore::Storage* storage_;

    /**
     * Helper structure that manages all uncommitted block mappings.
     */
    VolatileBlockStore volatile_blocks_;

    /**
     * Helper structure that manages all uncommitted block mappings during a dirty replay.
     * We cannot know for sure if a container has been committed, when the block mapping is replayed in
     * dirty mode. The problem is that the container metadata is not consistent at the time of the
     * block mapping written event.
     * If should be noted that we use the same callback method
     */
    VolatileBlockStore dirty_volatile_blocks_;

    /**
     * Contains all block id than can be imported from the auxiliary index to the persistent index.
     * The first element is the block id, the second is the version number.
     */
    tbb::concurrent_queue<std::tr1::tuple<uint64_t, uint32_t> > ready_queue_;

    /**
     * A condition that is fired every time the ready map is changed
     */
    dedupv1::base::Condition ready_map_change_condition_;

    /**
     * Block index background committer.
     * Used to commit ready blocks from the auxiliary index to the persistent index.
     */
    BlockIndexBackgroundCommitter bg_committer_;

    /**
     * Reference to the block locks that protect block against
     * concurrent accesses
     */
    dedupv1::BlockLocks* block_locks_;

    /**
     * State of the block index
     */
    enum state state_;

    /**
     * number of currently open blocks that have not been stored in the block index
     * before.
     * This value is used to calculate if the block index is full or if another new block
     * fits into the block index.
     */
    tbb::atomic<uint64_t> open_new_block_count_;

    /**
     * Info store to use
     */
    dedupv1::InfoStore* info_store_;

    /**
     * Number of threads used during the stop process
     */
    uint32_t stop_thread_count_;

    /**
     * If set to true, the block index is imported if the system is idle
     */
    bool import_if_idle_;

    /**
     * Reference to the idle detector.
     * NULL before start.
     */
    dedupv1::IdleDetector* idle_detector_;

    /**
     * True iff the log is currently replaying. To improve the performance of the
     * replay, multiple bg threads import block mappings so that the work must not be done
     * by the single-threaded log replay.
     */
    tbb::atomic<bool> is_replaying_;

    tbb::atomic<bool> is_full_log_replay_;


    /**
     * Iff set to true, the block index is importing if the system is replaying log entries.
     */
    bool import_if_replaying_;

    /**
     * Delay in ms between two block imports in an import thread in situations where the log is replayed
     */
    uint32_t log_replay_import_delay_;

    /**
     * Delay in ms between two block imports in an import thread in situations when a
     * full log replay is done, e.g. via dedupv1_replay or dedupv1_check
     */
    uint32_t full_log_replay_import_delay_;

    /**
     * Delay in ms between two block imports in an import thread in situations where the
     * hard limit of the auxilary index is reached
     */
    uint32_t hard_limit_import_delay_;

    /**
     * Delay in ms between two block imports in an import thread in situations where the
     * soft limit of the auxilary index is reached
     */
    uint32_t soft_limit_import_delay_;

    /**
     * Delay in ms between two block imports during system is idle.
     */
    uint32_t idle_import_delay_;

    /**
     * Delay in ms between two block imports in an import thread in normal request situations
     */
    uint32_t default_import_delay_;

    /**
     * Number of background importing threads
     */
    uint32_t import_thread_count_;

    /**
     * throttling helper object
     */
    ThrottleHelper throttling_;

    /**
     * TODO (dmeister) Remove this.
     */
    uint32_t minimal_replay_import_size_;

    int import_batch_size_;

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool ImportReadyBlockLoop();

    /**
     * Reads the block information from an index.
     * At first the auxiliary index is checked, if the block mapping
     * is not found there, the persistent index is checked.
     *
     * @param block_mappings
     * @return
     */
    enum read_result ReadBlockInfoFromIndex(BlockMapping* block_mappings);

    /**
     * Replays a log event.
     * - Block mapping written events are observed during background
     * replay to check to import them into the persistent index
     * - Container Commit events are observed during direct replay
     * to check update its state about which blocks are allowed
     * to be moved from the auxiliary index to the persistent index
     * eventually.
     *
     * @param event_type type of the event
     * @param event_value value of the event.
     * @param context context information about the event, e.g. the event log id or the replay mode
     * @return true iff ok, otherwise an error has occurred
     */
    bool LogReplay(dedupv1::log::event_type event_type,
            const LogEventData& event_value,
            const dedupv1::log::LogReplayContext& context);

    /**
     * Dumps meta information about the block index into a info store.
     * The meta information is dumped during the shutdown and additional points
     * after the start. The block index can rely that the a read will return
     * the last dumped information, it cannot rely that the meta info is uptodate.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool DumpMetaInfo();

    /**
     * Reads meta information, e.g. parts of the state about the block index from a info store.
     *
     * Additionally, the method performs a basic
     * verification if the stored info is compatible
     * to the configuration.
     *
     * @return
     */
    dedupv1::base::lookup_result ReadMetaInfo();

    /**
     * Writes a block mapping deleted event entry.
     *
     * @param previous_block_mapping
     * @param event_log_id out parameter that contains the event log id of the block mapping
     * deleted event that is written.
     * @param ec optional error context whose values are set, e.g. if the log is full.
     * @return true iff ok, otherwise an error has occurred
     */
    bool WriteDeleteLogEntry(const BlockMapping& previous_block_mapping,
            int64_t* event_log_id,
            dedupv1::base::ErrorContext* ec);

    /**
     * Writes a block mapping written event entry
     * related to the block mapping change.
     *
     * @param previous_block_mapping
     * @param updated_block_mapping
     * @param event_log_id out parameter that contains the event log id of the block mapping
     * written event that is written.
     * @param ec optional error context whose values are set, e.g. if the log is full.
     * @return true iff ok, otherwise an error has occurred
     */
    bool WriteLogEntry(const BlockMapping& previous_block_mapping,
            const BlockMapping& updated_block_mapping,
            int64_t* event_log_id,
            dedupv1::base::ErrorContext* ec);

    /**
     * Returns a set of used containers that are not committed.
     * We check the original block mapping and the modified block mapping to ensure that no chunk that might
     * be checked during the gc goes to disk. Neither in the modified block mapping nor in the original block mapping.
     * @return true iff ok, otherwise an error has occurred
     */
    bool BlockMappingStorageCheck(const BlockMapping& modified_block_mapping,
            std::set<uint64_t>* address_set);

    /**
     * Imports the next ready batch of blocks.
     *
     * @return LOOKUP_NOT_FOUND: there is no ready blocks left to be imported. However, it may be the
     * case that new blocks are ready after the check has been done.
     */
    dedupv1::base::lookup_result ImportNextBlockBatch();

    /**
     * Imports a list of modified block mapping.
     * It is assumes that all containers of the block mapping are committed and that the
     * block mapping is available in the auxiliary chunk index.
     *
     * It also assumes that all block ids in the list are unique. No two versions for the same block
     *
     * The methods blocks to acquire the block locks.
     *
     * @return
     */
    bool ImportModifiedBlockMappings(const std::list<std::tr1::tuple<uint64_t, uint32_t> >& ready_infos,
            std::list<std::tr1::tuple<uint64_t, uint32_t> >* unimported_ready_infos);

    /**
     * Callback of the volatile block store called when a container used by
     * a volatile block mapping is marked as failed.
     *
     * The callback is also called if block mappings of the same but an earlier version
     * are marked as failed.
     *
     * @param original_mapping
     * @param modified_mapping
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool FailVolatileBlock(const BlockMapping& original_mapping,
            const BlockMapping& modified_mapping,
            const google::protobuf::Message* extra_message,
            int64_t block_mapping_written_event_log_id);

    /**
     * Callback of the volatile store called when the last container referenced by a updates block mapping item
     * is committed (that means the block mapping item can be imported into the main index).
     *
     * This should be notes that the calling thread might hold a block index lock (including the
     * block index lock of the modified mapping).
     *
     * @param original_mapping original block mapping
     * @param modified_mapping changed block mapping
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool CommitVolatileBlock(const BlockMapping& original_mapping,
            const BlockMapping& modified_mapping,
            const google::protobuf::Message* extra_message,
            int64_t event_log_id,
            bool direct);

    /**
     * Called when a block mapping deleted event is replayed.
     *
     * @param replay_mode
     * @param event_value
     * @param context
     * @return true iff ok, otherwise an error has occurred
     */
    bool LogReplayBlockMappingDeleted(dedupv1::log::replay_mode replay_mode,
            const LogEventData& event_value,
            const dedupv1::log::LogReplayContext& context);

    /**
     * Listens the all log entries and processes, e.g. CONTAINER_COMMIT
     * events.
     *
     * @param replay_mode replay mode of the currently replayed event
     * @param event_value value of the log event
     * @param context context of the replayed log event.
     * @return true iff ok, otherwise an error has occurred
     */
    bool LogReplayBlockMappingWritten(dedupv1::log::replay_mode replay_mode,
            const LogEventData& event_value,
            const dedupv1::log::LogReplayContext& context);

    /**
     * Tries to import blocks into the persistent index.
     *
     * @param max_import_batch_count
     * @param imported outgoing parameter indication if some block has been imported
     * @return true iff ok, otherwise an error has occurred
     */
    dedupv1::base::Option<bool> TryImportBlock(bool with_wait_period);

    /**
     * returns the size of the error queue.
     * @return
     */
    inline size_t ready_queue_size() const;

    /**
     * Reads a block mapping from a specific index.
     *
     * @param index
     * @param block_mappings
     * @return
     */
    dedupv1::base::lookup_result ReadBlockInfoFromIndex(dedupv1::base::Index* index, BlockMapping* block_mappings);

    /**
     * Called in the volatile block store callback
     * @return true iff ok, otherwise an error has occurred
     */
    bool ProcessCommittedBlockWrite(const BlockMapping& original_mapping, const BlockMapping& modified_mapping, int64_t event_log_id);

    /**
     * Called when a dirty log replay finishes.
     * Here we look at all blocks that are still in the volatile block store. These blocks cannot be recovered because
     * they reference lost containers. We mark these blocks as failed (if this has not been done before)
     */
    bool FinishDirtyLogReplay();

    /**
     * Replays a block mapping written event during background replay.
     */
    bool LogReplayBlockMappingWrittenBackground(dedupv1::log::replay_mode replay_mode,
                                                        const LogEventData& event_value,
                                                        const dedupv1::log::LogReplayContext& context);

    /**
     * Replays a block mapping written event during a dirty replay.
     */
    bool LogReplayBlockMappingWrittenDirty(dedupv1::log::replay_mode replay_mode,
                                                   const LogEventData& event_value,
                                                   const dedupv1::log::LogReplayContext& context);

    bool LogReplayBlockMappingWriteFailedDirty(dedupv1::log::replay_mode replay_mode,
                                                    const LogEventData& event_value,
                                                    const dedupv1::log::LogReplayContext& context);

    bool LogReplayBlockMappingWriteFailedBackground(dedupv1::log::replay_mode replay_mode,
                                                    const LogEventData& event_value,
                                                    const dedupv1::log::LogReplayContext& context);

    dedupv1::base::Option<bool> IsKnownAsFailedBlock(uint64_t block_id, uint32_t version);

    /**
     * Checks if the given container is is committed.
     */
    dedupv1::base::Option<bool> CheckContainerAddress(uint64_t container_id);
    public:

    /**
     * Constructor
     * @return
     */
    BlockIndex();

    /**
     * Destructor
     */
    virtual ~BlockIndex();

    /**
     * Stops the block index.
     *
     * The block index should not emit log entries during or after this call as the log is usually
     * already stopped.
     *
     * @return
     */
    bool Stop(dedupv1::StopContext stop_context);

    /**
     * Configures the block index.
     *
     * Available options:
     * - auxiliary: String creates and auxiliary index of the given type
     * - auxiliary.*: String configures the auxiliary index.
     * - max-auxiliary-size: StorageUnit
     * - auxiliary-size-threshold: StorageUnit
     * - auxiliary-size-hard-limit: StorageUnit
     * - idle-import: Boolean
     * - replaying-import: Boolean
     * - persistent: String
     * - persistent.*: String
     * - import-thread-count: uint32_t
     * - import-batch-size: int
     * - log-replay-import-delay: uint32_t
     * - full-log-replay-import-delay: uint32_t
     * - hard-limit-import-delay: uint32_t
     * - soft-limit-import-delay: uint32_t
     * - default-import-delay: uint32_t
     * - throttle.*: String
     *
     * @param option_name
     * @param option
     * @return
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Starts the block index by starting the index implementation.
     * @return true if no error occurred, otherwise false.
     */
    bool Start(const dedupv1::StartContext& start_context, DedupSystem* system);

    /**
     *
     * @return true if no error occurred, otherwise false.
     */
    bool Run();

    /**
     * Stores the given block mapping in the index.
     *
     * Issues a block mapping written event containing the previous block mapping and the update
     * block mapping, even when not all used containers are already committed.
     *
     * The method should only return false iff block mapping written event is not written.
     *
     * @param previous_block_mapping block mapping as it has been before the current request
     * @param updated_block_mapping changed block mapping that should be stored.
     * @param ec Error context that can be filled if case of special errors
     * @return
     */
    virtual bool StoreBlock(const BlockMapping& previous_block_mapping, const BlockMapping& updated_block_mapping,
            dedupv1::base::ErrorContext* ec);

    /**
     * When in doubt, the method should return false.
     *
     * @param previous_block_mapping
     * @param updated_block_mapping
     * @return
     */
    virtual bool CheckIfFullWith(const BlockMapping& previous_block_mapping, const BlockMapping& updated_block_mapping);

    /**
     * Reads the block info of the given mapping (id of the mapping)
     * from
     * a) the cache
     * b) the auxiliary index
     * c) the block index.
     *
     * Reads the block information for the given block_id and write the data to the block_mapping structure.
     * If the block_id is currently "open" for the current session the data from the open request is used. This ensured consistency for
     * the session. However, sessions are never aware of open request of other sessions.
     * If the block_id isn't open the information is read from the index.
     *
     * @param session Current session object (can be NULL, but is passed as const)
     * @param block_mapping block mapping to fill (id must be set; it
     * determines which mapping to read).
     * @param ec Error context that can be filled if case of special errors
     * @return
     */
    virtual read_result ReadBlockInfo(
            const dedupv1::Session* session,
            BlockMapping* block_mapping,
            dedupv1::base::ErrorContext* ec);

    /**
     * Deletes the block info from the index.
     * If a block mapping for that block id exists, a EVENT_TYPE_BLOCK_MAPPING_DELETED
     * event is written and the block mapping data is deleted. After the call, the
     * block id should be treated as is would contain only zeros (similar to the initial state).
     *
     * The caller has to ensure that no concurrent operations operate on that block id.
     * The only good reason to call this method, is after the volume of that block has been detached.
     *
     * We avoid deleting block info for block ids that are currently
     * volatile as it is very, very hard to do it right. You cannot
     * simply write a BLOCK_MAPPING_DELETE event with the new block mapping has
     * the previous is not logged, and other problems and race conditions. The call
     * fails if the block is volatile.
     *
     * @param block_id
     * @param ec Error context that can be filled if case of special errors
     * @return
     */
    virtual dedupv1::base::delete_result DeleteBlockInfo(uint64_t block_id,
            dedupv1::base::ErrorContext* ec);

    /**
     * Persist the statistics of the block index
     */
    virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * Restores the statistics of the block index
     */
    virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * See also the comments in dedupv1.proto
     */
    virtual bool MarkBlockWriteAsFailed(const BlockMapping& previous_block_mapping,
            const BlockMapping& updated_block_mapping,
            dedupv1::base::Option<int64_t> write_event_log_id,
            dedupv1::base::ErrorContext* ec);

    /**
     * See also the comments in dedupv1.proto
     */
    virtual bool MarkBlockWriteAsFailed(const BlockMappingPair& mapping_pair,
            dedupv1::base::Option<int64_t> write_event_log_id,
            dedupv1::base::ErrorContext* ec);

    /**
     * Prints statistics about the locks used by the block index.
     * @return
     */
    std::string PrintLockStatistics();

    /**
     * Prints statistics about the block index
     * @return
     */
    std::string PrintStatistics();

    /**
     * Print profile information about the block index
     * @return
     */
    std::string PrintProfile();

    /**
     * Print trace information about the block index
     */
    std::string PrintTrace();

    /**
     * Returns the persistent index. This is not thread safe.
     * (Or more accurately: The returned index is not thread safe)
     */
    inline dedupv1::base::PersistentIndex* persistent_block_index();

    /**
     * Returns the volatile block store.
     */
    inline VolatileBlockStore* volatile_blocks();

    /**
     * Returns the current state of the block index
     */
    inline state state() const;

    /**
     * returns true iff the soft- or the hard limit on the size of the auxiliary index
     * are reached.
     *
     * @return
     */
    bool IsSoftLimitReached();

    /**
     * returns true iff the hard limit on the size of the auxiliary index
     * is reached.
     *
     * @return
     */
    bool IsHardLimitReached();

    /**
     * checks if a minimal auxiliary index size is reached for the importing of block entries
     * into the persistent index because the log is replaying.
     *
     * The reason is that we want to avoid locking because of too early block imports. It is often
     * the case that a block that is written is rewritten fast. This method helps here.
     */
    bool IsMinimalImportSizeReached();

    /**
     * Waits until the hard limit of the auxiliary index isn't reached longer.
     * This is used to throttle the system down.
     *
     * @return true if no error occurred, otherwise false.
     */
    dedupv1::base::Option<bool> Throttle(int thread_id, int thread_count);

    /**
     * returns the number of active blocks.
     * We mean by active blocks blocks, block (ids) that are either in the persistent index and additional(!)
     * blocks in the auxiliary index that are not yet written to the persistent index.
     */
    uint64_t GetActiveBlockCount();

    /**
     * Imports all ready blocks from the ready queue.
     *
     * Only in non-concurrent situations like a shutdown
     * @return true iff ok, otherwise an error has occurred
     */
    bool ImportAllReadyBlocks();

    /**
     * Returns the block size of the blocks in the block index.
     */
    inline uint32_t block_size() const;

#ifdef DEDUPV1_CORE_TEST
    void ClearData();
#endif
};

enum BlockIndex::state BlockIndex::state() const {
    return state_;
}

size_t BlockIndex::ready_queue_size() const {
    return this->ready_queue_.unsafe_size();
}

dedupv1::base::PersistentIndex* BlockIndex::persistent_block_index() {
    return block_index_;
}

VolatileBlockStore* BlockIndex::volatile_blocks() {
    return &volatile_blocks_;
}

uint32_t BlockIndex::block_size() const {
  return block_size_;
}


}
}

#endif  // BLOCK_INDEX_H__
