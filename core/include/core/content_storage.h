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

#ifndef CONTENT_STORAGE_H__
#define CONTENT_STORAGE_H__

#include <string>
#include <map>
#include <list>
#include <vector>
#include <tr1/tuple>
#include <tbb/atomic.h>

#include <core/dedup.h>
#include <base/crc32.h>
#include <core/block_locks.h>
#include <core/block_index.h>
#include <core/chunk_index.h>
#include <core/block_mapping.h>
#include <core/chunk_store.h>
#include <core/filter_chain.h>
#include <core/log.h>
#include <core/chunk.h>
#include <core/chunk_mapping.h>
#include <core/dedupv1_scsi.h>
#include <base/error.h>
#include <base/threadpool.h>
#include <base/multi_signal_condition.h>
#include <base/sliding_average.h>

namespace dedupv1 {

/**
 * The responsibility of the ContentStorage is to processes client requests.
 * This is done in the following high-level steps:
 *
 * - On write requests, the data is split into chunks and each chunk is processed
 *   indepentenly.
 *
 * - Each chunk is fingerprinted
 *
 * - Then each chunk is delegated to the filter chain. The filter chain checks if
 *   the chunk is already known or not. At the end of the filter chain and if the
 *   result is at least a STRONG_MAYBE, the data address of the chunk is known.
 *
 * - If a chunk is classified as unknown, the chunk data is given to the
 *   storage subsystem to be stored on disk.
 *
 * - If the data is stored, we process the filter chain a second time, to that
 *   the different filters can update there metadata. However, the filters
 *   must be aware, that a written chunk is not necessarily on disk.
 *
 * - At the end, we update the block mapping entry and write it into the block index.+
 *
 * Despite its name the content storage is not a subclass of storage. The name is a
 * typo. The equivalent class in the Data Domain File System (DDFS) according to Zhu et al.,
 * is content store. The first version tried to align its naming on DDFS, but used a wrong
 * name.
 *
 */
class ContentStorage : public dedupv1::StatisticProvider {
    private:

    static const uint32_t kDefaultChecksumSize = 32;

    /**
     * Name of the fingerprinting method
     */
    std::string fingerprinter_name_;

    /**
     * Reference to the block index
     */
    dedupv1::blockindex::BlockIndex* block_index_;

    dedupv1::chunkindex::ChunkIndex* chunk_index_;

    /**
     * Reference to the filter chain
     */
    dedupv1::filter::FilterChain* filter_chain_;

    /**
     * Reference to the chunk store
     */
    dedupv1::chunkstore::ChunkStore* chunk_store_;

    /**
     * Pointer to the block locks.
     */
    dedupv1::BlockLocks* block_locks_;

    dedupv1::Chunker* default_chunker_;

    /**
     * resource management to avoid chunk allocations.
     */
    dedupv1::base::ResourceManagement<Chunk>* chunk_management_;

    /**
     * if true, the filter chain for chunks of a request is executed in parallel
     * thread pool jobs.
     * If false, the filter chain for the chunks of a request are executed
     * in order.
     */
    bool parallel_filter_chain_;

    class Statistics {
        public:
            Statistics();

            tbb::atomic<uint64_t> reads_;
            tbb::atomic<uint64_t> read_size_;
            tbb::atomic<uint64_t> writes_;
            tbb::atomic<uint64_t> write_size_;
            tbb::atomic<uint64_t> sync_;

            /**
             * Profiling data
             */
            dedupv1::base::Profile profiling_;

            dedupv1::base::Profile fingerprint_profiling_;

            dedupv1::base::Profile chunking_time_;

            dedupv1::base::Profile checksum_time_;

            tbb::atomic<uint64_t> threads_in_filter_chain_;

            dedupv1::base::SimpleSlidingAverage average_write_block_latency_;

            dedupv1::base::SimpleSlidingAverage average_processing_time_;

            dedupv1::base::SimpleSlidingAverage average_filter_chain_time_;

            dedupv1::base::SimpleSlidingAverage average_chunking_latency_;

            dedupv1::base::SimpleSlidingAverage average_fingerprint_latency_;

            dedupv1::base::SimpleSlidingAverage average_chunk_store_latency_;

            dedupv1::base::SimpleSlidingAverage average_block_read_latency_;

            dedupv1::base::SimpleSlidingAverage average_sync_latency_;

            dedupv1::base::SimpleSlidingAverage average_open_request_handling_latency_;

            dedupv1::base::SimpleSlidingAverage average_block_storing_latency_;

            dedupv1::base::SimpleSlidingAverage average_process_chunk_filter_chain_latency_;

            dedupv1::base::SimpleSlidingAverage average_process_filter_chain_barrier_wait_latency_;

            dedupv1::base::SimpleSlidingAverage average_process_chunk_filter_chain_read_chunk_info_latency_;

            dedupv1::base::SimpleSlidingAverage average_process_chunk_filter_chain_write_block_latency_;

            dedupv1::base::SimpleSlidingAverage average_process_chunk_filter_chain_store_chunk_info_latency_;
    };

    /**
     * Statistics about the content storage.
     */
    Statistics stats_;

    /**
     * Reference to the log system.
     */
    dedupv1::log::Log* log_;

    /**
     * block size of the dedup system
     */
    uint32_t block_size_;

    /**
     * Threadpool to use
     */
    dedupv1::base::Threadpool* tp_;

    tbb::atomic<bool> reported_full_block_index_before_;

    tbb::atomic<bool> reported_full_chunk_index_before_;

    tbb::atomic<bool> reported_full_storage_before_;

	/**
	 * Merged to chunk mappings into the current request.
     *
     * @param block_id block id of the current request
     * @param already_failed if true, the request already failed, but we should update the block mapping and
     * mark it as failed. However, we should not try to do this request.
     * @param session session to use
     * @param original_block_mapping
     * @param updated_block_mapping
     * @param chunk_mappings chunks to process
     * @param ec error context (may be NULL)
     */
    bool MergeChunksIntoCurrentRequest(
            uint64_t block_id,
            RequestStatistics* request_stats,
            unsigned int block_offset,
            unsigned long open_chunk_pos,
            bool already_failed,
            Session* session,
            const dedupv1::blockindex::BlockMapping* original_block_mapping,
            const dedupv1::blockindex::BlockMapping* updated_block_mapping,
            std::vector<dedupv1::chunkindex::ChunkMapping>* chunk_mappings,
            dedupv1::base::ErrorContext* ec);

	/**
	 * Merged to chunk mappings into the open request and the current request.
     *
     * @param block_id block id of the current request
     * @param session session to use
     * @param request current request
     * @param request_stats statistics about the request (may be NULL)
     * @param original_block_mapping
     * @param updated_block_mapping
     * @param chunk_mappings chunks to process
     * @param ec error context (may be NULL)
     */
    bool MergeChunksIntoOpenRequests(
            uint64_t block_id,
            Session* session,
            Request* request,
            RequestStatistics* request_stats,
            const dedupv1::blockindex::BlockMapping* original_block_mapping,
            const dedupv1::blockindex::BlockMapping* updated_block_mapping,
            std::vector<dedupv1::chunkindex::ChunkMapping>* chunk_mappings,
            dedupv1::base::ErrorContext* ec);

    /**
     * Runs through the filter chain for all chunk mappings.
     * Each chunk mapping is processed in an own thread from the global thread pool
     *
     * @param session session to use
     * @param request current request
     * @param request_stats statistics about the current request (may be NULL)
     * @param block mapping current block mapping
     * @param chunk mappings all chunk mappings that should be processed by the filter chain
     * @param ec error context (may be NULL)
     * @return true iff ok, otherwise an error has occurred
     */
    bool ProcessFilterChain(Session* session,
            Request* request,
            RequestStatistics* request_stats,
            const dedupv1::blockindex::BlockMapping* block_mapping,
            std::vector<dedupv1::chunkindex::ChunkMapping>* chunk_mappings,
            dedupv1::base::ErrorContext* ec);

    /**
     * Runs through the filter chain for a given chunk mappings.
     *
     * @param t a tuple consisting of the session, the current block mapping, the chunk mapping and the error context
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool ProcessChunkFilterChain(std::tr1::tuple<Session*,
            const dedupv1::blockindex::BlockMapping*,
            dedupv1::chunkindex::ChunkMapping*,
            dedupv1::base::MultiSignalCondition*,
            tbb::atomic<bool>*,
            dedupv1::base::ErrorContext*> t);

    /**
     * Marks the given chunks as possible ophran chunks
     * An ohpran chunk is a chunks that is not used by a block mapping because
     * an error happened during the processing.
     * Without a special handling, these chunks will never be "seen" by the
     * garbage collector and therefore will never be removed.
     *
     * @param chunk_mappings chunks that might be ophrans.
     */
    bool MarkChunksAsOphran(const std::vector<dedupv1::chunkindex::ChunkMapping>& chunk_mappings);
    public:

    /**
     * Constructor
     * @return
     */
    ContentStorage();

    /**
     * Destructor
     */
    virtual ~ContentStorage();

    /**
     * Starts the content storage system.
     *
     * @param block_index block index to use
     * @param filter_chain filter chain to use
     * @param chunk_store chunk store to use
     * @param chunk_management chunk management to use
     * @param log log to use
     * @param block_locks block locks to use.
     * @param block_size block size to use
     * @param tp thread pool to use
     * @return true iff ok, otherwise an error has occurred
     */
    bool Start(
            dedupv1::base::Threadpool* tp,
            dedupv1::blockindex::BlockIndex* block_index,
            dedupv1::chunkindex::ChunkIndex* chunk_index,
            dedupv1::chunkstore::ChunkStore* chunk_store,
            dedupv1::filter::FilterChain* filter_chain,
            dedupv1::base::ResourceManagement<Chunk>* chunk_management,
            dedupv1::log::Log* log,
            dedupv1::BlockLocks* block_locks,
            uint32_t block_size);

    /**
     * Creates a new session object.
     */
    virtual Session* CreateSession(Chunker* chunker,
            const std::set<std::string>* enabled_filter_names);

    /**
     * Configures the content storage.
     *
     * The possible options are:
     * - chunking: Sets the type of the chunking system. It is not allowed to
     *             set this parameter multiple times
     * - chunking.*: Delegates the parameter (without the prefix) to the chunking
     *              implementation.
     * - fingerprinting: Set the fingerprinter implementation.
     * - content-storage.checksum: (none, min-adler)
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Writes a block.
     *
     * @param session: Session used to write.
     * @param request request to process. The request must be a write request.
     * @param request_stats statistics about the request (may be NULL)
     * @param last_write true iff this is the last write block request in the iSCSI request. All data, e.g. all chunk data
     *        must be processed at the end of the method.
     * @param ec error context (may be NULL)
     * @return false if an error occurred, otherwise true
     */
    bool WriteBlock(Session* session,
            Request* request,
            RequestStatistics* request_stats,
            bool last_write,
            dedupv1::base::ErrorContext* ec);

    /**
     * performs a fast copy operation from a given block in one volume to another block in possibly another volume
     *
     * @param src_block_id block id to copy from
     * @param src_offset offset within the source block
     * @param target_block_id block id to copy to
     * @param target_offset offset within the target block
     * @param size size of the data to copy
     * @param ec error context (may be NULL)
     * @return false if an error occurred, otherwise true
     */
    bool FastCopyBlock(
            uint64_t src_block_id,
            uint64_t src_offset,
            uint64_t target_block_id,
            uint64_t target_offset,
            uint64_t size,
            dedupv1::base::ErrorContext* ec);

    /**
     * Reads the block with the given block_id.
     *
     * @param session Session used to read
     * @param request The request to be performed
     * @param request_stats Statistics about the request (can be NULL)
     * @param ec error context (can be NULL)
     * @return false if an error occurred, otherwise true
     */
    bool ReadBlock(Session* session,
            Request* request,
            RequestStatistics* request_stats,
            dedupv1::base::ErrorContext* ec);

    /**
     *
     * @param session
     * @param ec error context (can be NULL)
     * @return true iff ok, otherwise an error has occurred
     */
    bool CloseRequest(Session* session, dedupv1::base::ErrorContext* ec);

    /**
     * Closes the content storage and frees all its resources.
     * @return true iff ok, otherwise an error has occurred
     */
    bool Close();

    /**
     * Persists the statistics of the content storage.
     *
     * @param prefix
     * @param ps
     * @return
     */
    virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * Restores the statistics of the content storage.
     *
     * @param prefix
     * @param ps
     * @return
     */
    virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * print statistics about the content storage as JSON string
     * @return
     */
    virtual std::string PrintStatistics();

    /**
     * print profile data about the content storage
     * @return
     */
    virtual std::string PrintProfile();

    /**
     * prints statistics about the locks of the content storage
     * @return
     */
    virtual std::string PrintLockStatistics();

    virtual std::string PrintTrace();

    /**
     * Returns the name of the fingerprinter
     * @return
     */
    inline const std::string& fingerprinter_name();

    /**
     * Calculates the fingerprint of the chunk filled with zeros.
     * This method is usually called during the start of the content
     * storage.
     *
     * @return
     */
    static bool InitEmptyFingerprint(Chunker* chunker, Fingerprinter* fp_gen, bytestring* empty_fp);

    private:

    /**
     * Handles the chunks for the current request.
     * Every parts of the deduplication process after the chunking happens here.
     *
     * If called without a request, the method is called to clean up to
     * last chunks open in the session.

     * INVARIANT: Each block in the request is locked before by dedup system.
     *
     * @param session current session
     * @param original_block_mapping unchanged,block mapping of the same block before the change.
     * The original block mapping should be of the same block id and have the version i if the
     * updated block mapping has the version 1+i. Can be null, but if not all values in the mapping are preloaded
     * @param updated_block_mapping current, and valid block mapping (equals the original with version + 1), the
     * block mapping can be NULL, but if not all values in the mapping are preloaded
     * @param request current dedup request (may be null if called by the Sync method)
     * @param request_stats object holding statistics about the request (can be null if called
     * by the sync request).
     * @param chunks references to chunk data that should be handled.
     * @param ec error context (can be NULL)
     */
    bool HandleChunks(Session* session, Request* request,
            RequestStatistics* request_stats,
            const dedupv1::blockindex::BlockMapping* original_block_mapping,
            const dedupv1::blockindex::BlockMapping* updated_block_mapping,
            const std::list<Chunk*>& chunks,
            dedupv1::base::ErrorContext* ec);

    /**
     * Computes the CRC checksum for a given block mapping.
     */
    bool ComputeCRCChecksum(std::string* checksum, Session* session,
            dedupv1::blockindex::BlockMapping* block_mapping, dedupv1::base::ErrorContext* ec);

    /**
     * Reads data and computes the crc checksum for a block mapping item
     *
     * @param item The block mapping item to be read from
     * @param session The current session
     * @param data_buffer A buffer to store data in
     * @param data_pos Current position in the data buffer
     * @param count The number of bytes to read from this item. If 0 no bytes are read and only the crc is calculated.
     * @param offset Offset of reading within this item.
     * @param ec error context (Can be NULL)
     */
    bool ReadDataForItem(dedupv1::blockindex::BlockMappingItem* item,
            Session* session,
            byte* data_buffer,
            unsigned int data_pos, int count,
            int offset, dedupv1::base::ErrorContext* ec);

    /**
     * Fingerprint the given chunks.
     *
     * @param request current request (Can be NULL)
     * @param request_stats statistics about the current request (Can be NULL)
     * @param fingerprinter Fingerprinter instance to use
     * @param chunks Chunks to fingerprint. Every item of the chunks list should not be NULL:
     * @param chunk_mappings Output parameter to hold the created chunk mappings
     * @param ec error context (Can be NULL)
     * @return
     */
    bool FingerprintChunks(
            Session* session,
            Request* request,
            RequestStatistics* request_stats,
            Fingerprinter* fingerprinter,
            const std::list<Chunk*>& chunks,
            std::vector<dedupv1::chunkindex::ChunkMapping>* chunk_mappings,
            dedupv1::base::ErrorContext* ec);

    DISALLOW_COPY_AND_ASSIGN(ContentStorage);
};

const std::string& ContentStorage::fingerprinter_name() {
    return this->fingerprinter_name_;
}

}

#endif  // CONTENT_STORAGE_H__
