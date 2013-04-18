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

#ifndef SPARSE_CHUNK_INDEX_FILTER_H__
#define SPARSE_CHUNK_INDEX_FILTER_H__

#include <tbb/atomic.h>
#include "tbb/recursive_mutex.h"

#include <core/dedup.h>
#include <core/filter.h>
#include <base/profile.h>
#include <core/chunk_index.h>
#include <core/block_mapping.h>
#include <base/sliding_average.h>

#include <string>

namespace dedupv1 {
namespace filter {

/**
 * The sparse-chunk-index-filter is the main index for a deduplication system.
 * If checks if a new is a duplicate by asking the chunk index if the fingerprint
 * is an anchor.
 *
 * An addition to the original chunk index filter design, is the special handing of
 * the fingerprint of the empty chunk. The chunk index filter will return "EXISTING"
 * as the result.
 *
 * \ingroup filterchain
 */
class SparseChunkIndexFilter : public Filter {
private:
    DISALLOW_COPY_AND_ASSIGN(SparseChunkIndexFilter);

    static const size_t kDefaultChunkLockCount = 512;

    /**
     * Type for statistics about the chunk index filter
     */
    class Statistics {
public:
        Statistics();

        tbb::atomic<uint64_t> reads_;
        tbb::atomic<uint64_t> writes_;
        tbb::atomic<uint64_t> strong_hits_;
        tbb::atomic<uint64_t> weak_hits_;
        tbb::atomic<uint64_t> miss_;
        tbb::atomic<uint64_t> failures_;

        tbb::atomic<uint64_t> anchor_count_;

        /**
         * Profiling information about the filter.
         */
        dedupv1::base::Profile time_;

        /**
         * Profiling information (filter latency in ms)
         */
        dedupv1::base::SimpleSlidingAverage average_latency_;
    };

    /**
     * Reference to the chunk index
     */
    dedupv1::chunkindex::ChunkIndex* chunk_index_;

    /**
     * structure to holds statistics about the filter
     */
    Statistics stats_;

    uint32_t sampling_factor_;

    uint64_t sampling_mask_;

    /**
     * Releases the lock on the fingerprint of the chunk
     */
    bool ReleaseChunkLock(const dedupv1::chunkindex::ChunkMapping& mapping);

    /**
     * Acquire the lock on the fingerprint of the chunk.
     *
     * May block if the chunk is used. A aquired lock must be released later.
     */
    bool AcquireChunkLock(const dedupv1::chunkindex::ChunkMapping& mapping);

    bool IsAnchor(const dedupv1::chunkindex::ChunkMapping& mapping);
public:
    /**
     * Constructor
     * @return
     */
    SparseChunkIndexFilter();

    /**
     * Destructor
     * @return
     */
    virtual ~SparseChunkIndexFilter();

    /**
     * Starts the chunk index filter
     *
     * @param system
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(DedupSystem* system);

    virtual bool SetOption(const std::string& option_name,
                           const std::string& option);

    /**
     * Checks the chunk index for the chunk mapping.
     * If we find an entry in the chunk index with the same fingerprint,
     * STRONG_MAYBE is returned. Otherwise NOT_EXISTING is returned.
     *
     * If an auxiliary index is configured, the auxiliary index is checked
     * first.
     *
     * @param session
     * @param block_mapping
     * @param mapping
     * @param ec Error context that can be filled if case of special errors
     * @return
     */
    virtual enum filter_result Check(dedupv1::Session* session,
        const dedupv1::blockindex::BlockMapping* block_mapping,
        dedupv1::chunkindex::ChunkMapping* mapping,
        dedupv1::base::ErrorContext* ec);

    /**
     * Updates the chunk index for the newly found chunk.
     * If the chunk is already committed in the storage system,
     * the chunk is written to the main index otherwise the chunk
     * is only added to the in-memory auxiliary index.
     *
     * @param session
     * @param mapping
     * @param ec Error context that can be filled if case of special errors
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Update(dedupv1::Session* session,
        const dedupv1::blockindex::BlockMapping* block_mapping,
        dedupv1::chunkindex::ChunkMapping* mapping,
        dedupv1::base::ErrorContext* ec);

    /**
     * Calls when the processing of the started filtering should be aborted.
     *
     * A filtering is started, when the Check method was called for this
     * chunk mapping. The chunk index filter is releasing the chunk lock
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Abort(dedupv1::Session* session,
        const dedupv1::blockindex::BlockMapping* block_mapping,
        dedupv1::chunkindex::ChunkMapping* chunk_mapping,
        dedupv1::base::ErrorContext* ec);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * Prints statistics about the chunk index filter.
     * @return
     */
    virtual std::string PrintStatistics();

    /**
     * Prints profile information about the chunk index filter.
     * @return
     */
    virtual std::string PrintProfile();

    /**
     * Create a new chunk index filter object
     */
    static Filter* CreateFilter();

    /**
     * Registers the chunk-index-filter
     */
    static void RegisterFilter();

    inline uint32_t sampling_factor() const;
};

uint32_t SparseChunkIndexFilter::sampling_factor() const {
    return sampling_factor_;
}

}
}

#endif  // SPARSE_CHUNK_INDEX_FILTER_H__
