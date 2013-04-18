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
#ifndef CHUNK_INDEX_H__
#define CHUNK_INDEX_H__

#include <tbb/atomic.h>

#include <core/chunk_mapping.h>
#include <core/log_consumer.h>
#include <base/index.h>
#include <base/disk_hash_index.h>
#include <core/container_tracker.h>
#include <core/chunk_index_bg.h>
#include <base/profile.h>
#include <base/fileutil.h>
#include <core/storage.h>
#include <base/sliding_average.h>
#include <base/factory.h>
#include <base/error.h>
#include <core/chunk_locks.h>
#include <core/chunk_index_in_combat.h>
#include <core/chunk_index_sampling_strategy.h>
#include <core/info_store.h>
#include <core/container.h>
#include <base/threadpool.h>
#include <core/throttle_helper.h>

#include <string>
#include <list>
#include <tbb/tick_count.h>

namespace dedupv1 {

class DedupSystem;

/**
 * \namespace dedupv1::chunkindex
 * Namespace for classes related to the chunk index
 */
namespace chunkindex {

class ChunkMapping;
class ChunkIndexBackgroundCommitter;

/**
 * The chunk index stores all known chunk fingerprints as well as a mapping to the
 * address of the chunk in the storage system. The chunk index data is actually a subset
 * of the ChunkMapping data.
 *
 * The key of the index is the fingerprint, the value is the storage address and other metadata as
 * the usage count.
 *
 * We use an auxiliary chunk index to store all fingerprint chunks that are not committed by the storage
 * subsystem.
 *
 * The size of the chunk index grows with the amount of non-redundant data.
 * Per terabyte non-redundant data (2^40 byte) we have to store the metadata of
 * 2^27 chunks (assuming a chunk size of 8KB). Without any overhead, the chunk index has
 * therefore at least a size of 3,5 GB per TB non-redundant data.
 *
 * The chunk index has a delay-write mechanism. Assuming a working logging system
 * there is no need for the chunk index to ever update its persistent index during runtime.
 * However, for two reasons the persistent index is updated:
 * - reducing the recovery time
 * - reducing the in-memory requirements.
 * With the max-auxiliary-size option, the client can configure the maximal number of
 * items that should be stored in the auxiliary index (soft limit). If the
 * auxiliary index stored more than that, the chunk index should move items from the
 * auxiliary index to the main index. This moving should consider the container commit
 * ordering. If an item from a container x is stored in the main index, all items from
 * container y < x should be stored in the main index.
 *
 */
class ChunkIndex : public dedupv1::log::LogConsumer, public dedupv1::StatisticProvider {
    friend class ChunkIndexBackgroundCommitter;
    friend class ImportTask;
private:
    DISALLOW_COPY_AND_ASSIGN(ChunkIndex);

    /**
     * Factory for all chunk index instance
     */
    static MetaFactory<ChunkIndex> factory_;
public:

    static MetaFactory<ChunkIndex>& Factory();

    /**
     * States of the chunk index
     */
    enum chunk_index_state {
        CREATED, // !< CREATED
        STARTED, // !< STARTED
        STOPPED
        // !< STOPPED
    };

    /**
     * Type for statistics about the chunk index
     */
    class Statistics {
public:
        Statistics();
        /**
         * Profiling information
         */
        dedupv1::base::Profile profiling_;

        dedupv1::base::Profile update_time_;

        dedupv1::base::Profile lookup_time_;

        dedupv1::base::Profile replay_time_;

        dedupv1::base::Profile import_time_;

        tbb::atomic<uint64_t> index_full_failure_count_;

        tbb::atomic<uint64_t> imported_container_count_;

        /**
         * How often have we tried to import a container into the
         * persistent index from the log replay while at the same time
         * a bg thread is already active importing the same container. In
         * these situation, we pause the log replay to ensure that
         * the container is fully committed.
         */
        tbb::atomic<uint64_t> bg_container_import_wait_count_;

        tbb::atomic<uint32_t> lock_free_;

        tbb::atomic<uint32_t> lock_busy_;

        dedupv1::base::SimpleSlidingAverage average_lookup_latency_;

        tbb::atomic<uint64_t> throttle_count_;

        dedupv1::base::Profile throttle_time_;
    };
private:

    /**
     * State of the chunk index.
     */
    chunk_index_state state_;

    /**
     * Reference to the persistent chunk index.
     */
    dedupv1::base::PersistentIndex* chunk_index_;

    /**
     * Reference to the system log
     */
    dedupv1::log::Log* log_;

    /**
     * Maintains statistics about the chunk index
     */
    Statistics stats_;

    /**
     * Reference to the storage.
     *
     * NULL before Start, and set afterwards.
     */
    dedupv1::chunkstore::Storage* storage_;

    /**
     * Protected by lock
     * The container tracker tracks which container
     * has been imported into the chunk index or currently
     * are imported into the chunk index.
     */
    ContainerTracker container_tracker_;

    /**
     * Non-persistent container tracker to track which containers should be imported
     * into the auxiliary index during the dirty replay.
     */
    ContainerTracker dirty_import_container_tracker_;

    bool dirty_import_container_exists_;

    bool dirty_import_finished_;

    /**
     * lock to protect the values of last_container_id and
     * last_ready_container_id.
     */
    dedupv1::base::MutexLock lock_;

    /**
     * Chunk index background committer.
     * Used to commit ready chunks from the auxiliary index to the persistent index.
     */
    ChunkIndexBackgroundCommitter* bg_committer_;

    /**
     * Number of background committing threads;
     */
    uint32_t bg_thread_count_;

    /**
     * locks for the chunks
     */
    ChunkLocks chunk_locks_;

    ChunkIndexInCombats in_combats_;

    /**
     * Info store
     */
    dedupv1::InfoStore* info_store_;

    /**
     * Threadpool to use for the parallel import
     */
    dedupv1::base::Threadpool* tp_;

    /**
     * True iff the log is currently replaying. To improve the performance of the
     * replay, multiple bg threads import chunk mappings so that the work must not be done
     * by the single-threaded log replay.
     */
    tbb::atomic<bool> is_replaying_;

    /**
     * Iff set to true, the chunk index is importing if the system is replaying log entries.
     */
    bool import_if_replaying_;

    /**
     * Import delay (in ms)
     */
    uint32_t import_delay_;

    ChunkIndexSamplingStrategy* sampling_strategy_;

    ThrottleHelper throttling_;

    /**
     * threshold after that the system begins writing dirty data back to disk.
     * At first, it imports containers, after that it forces the index to write-back dirty pages
     */
    uint64_t dirty_chunk_count_threshold_;

    /**
     * Flag denoting if the start of an container import/dirty data import was already reported by an
     * INFO log message. Used to log message if importing starts and stops.
     */
    tbb::atomic<bool> has_reported_importing_;

    /**
     * Dumps meta information about the chunk index into a info store.
     * The meta information is dumped during the shutdown and additional points
     * after the start. The chunk index can rely that the a read will return
     * the last dumped information, it cannot rely that the meta info is uptodate.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool DumpMetaInfo();

    /**
     * Reads meta information, e.g. parts of the state about the chunk index from a info store.
     *
     * Additionally, the method performs a basic
     * verification if the stored info is compatible
     * to the configuration.
     *
     * @return
     */
    dedupv1::base::lookup_result ReadMetaInfo();

    /**
     * Imports the container by moving all fingerprints in that
     * container from the auxiliary index to the main index.
     * Note: The container must be committed.
     *
     * The chunk index lock must NOT be hold when calling this method.
     *
     * @param container_id The ID if the container to be imported
     * @param ec Error context that can be filled if case of special errors
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool ImportContainer(uint64_t container_id, dedupv1::base::ErrorContext* ec);

    /**
     * Enumeration to denote different result states when
     * trying to import containers.
     */
    enum import_result {
        IMPORT_ERROR = 0, // !< IMPORT_ERROR
        IMPORT_NO_MORE = 1, // !< IMPORT_NO_MORE
        IMPORT_BATCH_FINISHED = 2
                                // !< IMPORT_BATCH_FINISHED
    };

    /**
     * Checks the commit state of the container and import it is possible.
     *
     * @return false if an error occurred, Some(true) if the importation has been done, Some(false) if the container could not be imported, but
     * it was not an error.
     */
    dedupv1::base::Option<bool> DoImport(uint64_t container_id);

    /**
     * Tries to import a container into the chunk index
     *
     * @return
     */
    enum import_result TryImportContainer();

    enum import_result TryImportDirtyChunks(uint64_t* resume_handle);

    /**
     * Handles a directly replayed container commit event
     */
    bool HandleContainerCommit(const ContainerCommittedEventData& event_data);

    /**
     * Handled a failed container commit.
     *
     * @param event_data
     * @return true iff ok, otherwise an error has occurred
     */
    bool HandleContainerCommitFailed(const ContainerCommitFailedEventData& event_data);
protected:

    /**
     * Returns the storage
     * @return
     */
    inline dedupv1::chunkstore::Storage* storage();

    /**
     * Returns the lock
     * @return
     */
    inline dedupv1::base::MutexLock* lock();

    /**
     * Returns the statistics variable about the chunk index
     * @return
     */
    inline Statistics* statistics();

    /**
     * Returns the background committer of the chunk index
     * @return
     */
    inline ChunkIndexBackgroundCommitter* GetBackgroundCommitter();

    /**
     * returns the state of the chunk index
     * @return
     */
    inline chunk_index_state state() const;

    /**
     * Sets the state of the chunk index.
     * Used by subclasses.
     *
     * @param new_state
     */
    void set_state(chunk_index_state new_state);

    /**
     *
     * Note: We do not care about the version of the chunk index entry for the container item, it should only
     * be some version of the item on disk when the method finishes.
     *
     * @param item
     * @param ec
     * @return true iff ok, otherwise an error has occurred
     */
    bool ImportContainerItem(const dedupv1::chunkstore::ContainerItem& item, dedupv1::base::ErrorContext* ec);

    bool ImportContainerParallel(uint64_t container_id, const dedupv1::chunkstore::Container& container,
                                 dedupv1::base::ErrorContext* ec);

#ifdef DEDUPV1_CORE_TEST
public:
#endif

    /**
     * Loads a container that has not been imported into the persistent storage into the auxiliary index.
     *
     * The method is quite similar to ImportContainer but it works on the auxiliary index instead the persistent index,
     * and the container is not marked as imported.
     *
     * This method is used during a fast-crash replay. It is in the current form only applicable in that context as, e.g.
     * no locking is used.
     */
    bool LoadContainerIntoCache(uint64_t container_id, dedupv1::base::ErrorContext* ec);
public:
    /**
     * Creates a new chunk index
     * @return
     */
    static ChunkIndex* CreateIndex();

    /**
     * Registers the chunk index implementation.
     *
     */
    static void RegisterChunkIndex();

    /**
     * Constructor
     * @return
     */
    ChunkIndex();

    /**
     * Destructor
     * @return
     */
    virtual ~ChunkIndex();

    /**
     * Inits the chunk index.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Init();

    /**
     * Configures the chunk index.
     * The configuration should happen after the call of Init() and before the start. It is not
     * possible to change the configuration after the start.
     *
     * Available options:
     * - persistent: index type of the auxiliary index
     * - persistent.*: Forwards the option suffix to the auxiliary index. See there for more information.
     *   If the default (0) is used, every chunk data is stored in the main index as soon
     *   as the container is committed.
     * - parallel-import: If set to false, the chunk items within a container are not imported in parallel.
     *   The parallel import improves the performance to the the concurrency, but this feature was only
     *   recently introduced. The default is true. The option is depreciated and might be removed in the future.
     * - in-combats.*: Forwards the option suffix to the in combats object. See there for more information.
     * - bg-thread-count: Number of background importing threads. Default: 4.
     * - dirty-chunks-threshold: sets the dirty chunk threashold (storage unit)
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Starts the chunk index.
     *
     * @param system
     * @param start_context
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(const dedupv1::StartContext& start_context, DedupSystem* system);

    /**
     * Runs the threads of the chunk index, e.g. the chunk index background committer
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Run();

    /**
     * Closes the chunk index and frees all its resources.
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Close();

    /**
     * Stops the chunk index and especially the chunk index
     * background committer if used.
     *
     * The chunk index should not emit log entries during or after this call.
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Stop(dedupv1::StopContext stop_context);

    /**
     * Deletes the chunk mapping from the chunk index
     *
     * @param mapping
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Delete(const ChunkMapping& mapping);

    /**
     * Lookups a chunk mapping from the chunk index.
     * The fp of the mapping must be set. The values for known_chunk, usage_count, and data_address
     * are fetched from the index.
     *
     * @param mapping chunk mapping with a filled fingerprint to look up.
     * @param add_as_in_combat iff true, the chunk is marked as in-combat.
     * @param ec Error context that can be filled if case of special errors (can be NULL)
     *
     * @return
     */
    virtual dedupv1::base::lookup_result Lookup(ChunkMapping* mapping,
                                                bool add_as_in_combat,
                                                dedupv1::base::ErrorContext* ec);

    /**
     * Performs a lookup limited to the auxiliary index.
     *
     * Note: There might be situations where this method is necessary, but in general
     *       you are better of using the normal lookup.
     *
     * Note: To access aux or persistent Index directly use LookupAuxiliaryIndex or
     *       LookupPersistentIndex. This method is only used for startup and will therefore
     *       result in Aux-Index entries.
     *
     * @param index index to look in
     * @param mapping mapping to look up
     * @param ec error context (can be NULL)
     * @return
     */
    dedupv1::base::lookup_result LookupIndex(dedupv1::base::Index* index,
                                             ChunkMapping* mapping,
                                             dedupv1::base::ErrorContext* ec);

    /**
     * Performs a lookup limited to the persistent index.
     *
     * Note: There might be situations where this method is necessary, but in general<Merge Conflict>
     * you are better of using the normal lookup.
     *
     * @param index index to look in
     * @param mapping mapping to look up
     * @param ec error context (can be NULL)
     * @return
     */
    dedupv1::base::lookup_result LookupPersistentIndex(ChunkMapping* mapping,
                                                       dedupv1::base::cache_lookup_method cache_lookup_type,
                                                       dedupv1::base::cache_dirty_mode dirty_mode,
                                                       dedupv1::base::ErrorContext* ec);
    /**
     * Note: Currently only the garbage collector is every overwriting chunk mappings,
     * If gc is therefore save to do it without locking. However, if this assumption becomes
     * wrong, the gc must be changed.
     *
     * @param mapping
     * @param ensure_persistence Ensures that the mapping is persistent on disk when the call returns (successfully). Otherwise
     * the mapping is put as dirty item into the write-back cache and is written back eventually.
     * @param pin iff true, the data is pinned to memory and should never be written back before the pin state is changed. Only
     * ensure_persistance or pin can be specified.
     * @param ec Error context that can be filled if case of special errors
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool PutPersistentIndex(const ChunkMapping& mapping, bool ensure_persistence,
                                    bool pin,
                                    dedupv1::base::ErrorContext* ec);

    /**
     * Ensures that any dirty version of the fingerprint of the mapping is persistent.
     * It doesn't enforce the persistence of actual data of the mapping given, but the last written
     * version of the mapping with the given chunk fingerprint.
     *
     * If the chunk is still pinned, PUT_KEEP with pinned = true is returned. For what the chunk index knows,
     * this chunk has not been committed yet.
     */
    virtual dedupv1::base::put_result EnsurePersistent(const ChunkMapping& mapping, bool* pinned);

    /**
     * Changes the pinning state of the chunk with the given fingerprint key.
     */
    virtual dedupv1::base::lookup_result ChangePinningState(const void* key, size_t key_size, bool new_pin_state);

    virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * Print trace about the chunk index
     */
    virtual std::string PrintTrace();

    /**
     * Print statistics about the locks of the chunk index
     * @return
     */
    virtual std::string PrintLockStatistics();

    /**
     * Print general statistics about the chunk index
     * @return
     */
    virtual std::string PrintStatistics();

    /**
     * Print profile data about the chunk index
     * @return
     */
    virtual std::string PrintProfile();

    /**
     * Callback method that is called if a log
     * event should be replayed
     *
     * @param event_type type of the event
     * @param event_value value of the event.
     * @param context context information about the event, e.g. the event log id or the replay mode
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool LogReplay(dedupv1::log::event_type event_type, const LogEventData& event_value,
                           const dedupv1::log::LogReplayContext& context);

    bool FinishDirtyLogReplay();

    virtual dedupv1::base::Option<bool> IsContainerImported(uint64_t container_id);

    /**
     * returns the log system
     * @return
     */
    inline dedupv1::log::Log* log();

    /**
     * Puts a new chunk into the chunk index. Usually the mapping is marked as dirty and not
     * directly persistent on disk.
     *
     * @param mapping
     * @param ec Error context that can be filled if case of special errors
     * @return true iff ok, otherwise an error has occurred
     */
    bool Put(const ChunkMapping& mapping, dedupv1::base::ErrorContext* ec);

    /**
     * Puts the chunk into the persistent index.
     *
     * Note: Usually the Put method should be used.
     *
     * @param mapping
     * @param ec Error context that can be filled if case of special errors
     * @return true iff ok, otherwise an error has occurred
     */
    bool PutOverwrite(ChunkMapping& mapping, dedupv1::base::ErrorContext* ec);

    /**
     * Returns the number of persisting chunk index entries.
     * @return
     */
    inline uint64_t GetPersistentCount();

    /**
     * Returns the number of dirty chunk index entries.
     */
    inline uint64_t GetDirtyCount();

    /**
     * Create an Iterator to run over the persistent Index.
     *
     * Here we should try to find a better way in future, as the calling function
     * access the Protobuf Messages here directly.
     *
     * @return the created iterator
     */
    inline dedupv1::base::IndexIterator* CreatePersistentIterator();

    /**
     * Checks that all indices are set correctly.
     *
     * @return true if persistent and chunk index are set, else false.
     */
    bool CheckIndeces();

    /**
     * Put and possibly overwrites the chunk (mapping) to the given index.
     *
     * Note: Usually the PutOverwrite method should be used.
     *
     * Note: To access the Aux Index or Persistent Index use PutOverwriteAuxiliaryIndex
     *       or PutOverwritePersistentIndex. This method is only used to access the
     *       startup index, which is treated as aux index.
     *
     * @param index
     * @param mapping
     * @param ec Error context that can be filled if case of special errors
     * @return true iff ok, otherwise an error has occurred
     */
    bool PutIndex(dedupv1::base::Index* index, const ChunkMapping& mapping,
                  dedupv1::base::ErrorContext* ec);

    /**
     * Returns the container tracker of the chunk index
     * @return
     */
    inline ContainerTracker* container_tracker();

    /**
     * throttles the system down by waiting for at most ms milliseconds if the hard limit
     * size of the auxiliary index is reached.
     */
    dedupv1::base::Option<bool> Throttle(int thread_id, int thread_count);

    /**
     * Imports all ready (aka committed) container into the persistent chunk index.
     *
     * This method is usually called during a writeback stop and depending on the actual situation
     * it might block for some minutes
     */
    bool ImportAllReadyContainer();

    /**
     * returns the chunk locks
     */
    inline ChunkLocks& chunk_locks();

    /**
     * returns the in-combat chunks data
     */
    inline ChunkIndexInCombats& in_combats();

    /**
     * If false, the chunk index would be full if we already consider the items currently in the auxiliary index
     * In such situations, the system should stop accepting new data
     */
    bool IsAcceptingNewChunks();

    /**
     * @param it
     * @param mapping
     * @return
     */
    dedupv1::base::lookup_result LookupNextIterator(dedupv1::base::IndexIterator* it, ChunkMapping* mapping);

    bool dirty_import_finished() const {
        return dirty_import_finished_;
    }

    /**
     * Direct access to the underlying index data structure.
     * The direct access should be avoided.
     */
    dedupv1::base::PersistentIndex* persistent_index() {
        return chunk_index_;
    }

    inline ChunkIndexSamplingStrategy* sampling_strategy();
#ifdef DEDUPV1_CORE_TEST
    /**
     * Test if the persistent Index is a DiskHashImage. This is used for unit tests.
     *
     * @return
     */
    inline bool TestPersistentIndexIsDiskHashIndex();

    /**
     * Returns the max key size of the persistent index if it is a disk hash index.
     * This is used for unit tests.
     *
     * @return
     */
    inline size_t TestPersistentIndexAsDiskHashIndexMaxKeySize();

    void ClearData();
#endif
};

/**
 * @ingroup chunk index
 *
 * Factory for chunk index implementations.
 * Used by the configuration system to inject different implementations.
 */
class ChunkIndexFactory {
    DISALLOW_COPY_AND_ASSIGN(ChunkIndexFactory);
public:
    ChunkIndexFactory();
    bool Register(const std::string & name, ChunkIndex * (*factory)(void));
    static ChunkIndex* Create(const std::string& name);

    static ChunkIndexFactory* GetFactory() {
        return &factory;
    }
private:
    std::map<std::string, ChunkIndex*(*)(void)> factory_map;

    static ChunkIndexFactory factory;
};

ChunkIndexSamplingStrategy* ChunkIndex::sampling_strategy() {
    return sampling_strategy_;
}

dedupv1::chunkstore::Storage* ChunkIndex::storage() {
    return this->storage_;
}

dedupv1::base::MutexLock* ChunkIndex::lock() {
    return &this->lock_;
}

ContainerTracker* ChunkIndex::container_tracker() {
    return &this->container_tracker_;
}

ChunkIndex::Statistics* ChunkIndex::statistics() {
    return &this->stats_;
}

dedupv1::log::Log* ChunkIndex::log() {
    return this->log_;
}

inline ChunkIndexBackgroundCommitter* ChunkIndex::GetBackgroundCommitter() {
    return this->bg_committer_;
}

ChunkIndex::chunk_index_state ChunkIndex::state() const {
    return this->state_;
}

inline uint64_t ChunkIndex::GetDirtyCount() {
    return this->chunk_index_->GetDirtyItemCount();
}

inline uint64_t ChunkIndex::GetPersistentCount() {
    return this->chunk_index_->GetItemCount();
}

inline dedupv1::base::IndexIterator* ChunkIndex::CreatePersistentIterator() {
    return this->chunk_index_->CreateIterator();
}

#ifdef DEDUPV1_CORE_TEST
inline bool ChunkIndex::TestPersistentIndexIsDiskHashIndex() {
    return dynamic_cast<dedupv1::base::DiskHashIndex*>(this->chunk_index_);
}

inline size_t ChunkIndex::TestPersistentIndexAsDiskHashIndexMaxKeySize() {
    dedupv1::base::DiskHashIndex* di = dynamic_cast<dedupv1::base::DiskHashIndex*>(this->chunk_index_);
    if (!di) {
        return 0;
    }
    return di->max_key_size();
}
#endif

ChunkLocks& ChunkIndex::chunk_locks() {
    return chunk_locks_;
}

ChunkIndexInCombats& ChunkIndex::in_combats() {
    return in_combats_;
}

}
}

#endif  // CHUNK_INDEX_H__
