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

#ifndef SAMPLING_FILTER_H__
#define SAMPLING_FILTER_H__

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
 * The sampling filter sets if a chunk should be indexed or not.
 * It always returns WEAK_MAYBE.
 *
 * It is optional for full chunk index configurations. However, it
 * must be used in sampling configurations.
 *
 * \ingroup filterchain
 */
class SamplingFilter : public Filter {
private:
    DISALLOW_COPY_AND_ASSIGN(SamplingFilter);

    /**
     * Type for statistics about the chunk index filter
     */
    class Statistics {
public:
        Statistics();

        tbb::atomic<uint64_t> reads_;
        tbb::atomic<uint64_t> weak_hits_;

        /**
         * Profiling information about the filter.
         */
        dedupv1::base::Profile time_;

    };

    /**
     * Reference to the chunk index
     */
    dedupv1::chunkindex::ChunkIndex* chunk_index_;

    /**
     * structure to holds statistics about the filter
     */
    Statistics stats_;

    dedupv1::base::Option<bool> IsAnchor(const dedupv1::chunkindex::ChunkMapping& mapping);
public:
    /**
     * Constructor
     * @return
     */
    SamplingFilter();

    /**
     * Destructor
     * @return
     */
    virtual ~SamplingFilter();

    /**
     * Starts the sampling filter
     *
     * @param system
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(DedupSystem* system);

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
     * Create a new sampling filter object
     */
    static Filter* CreateFilter();

    /**
     * Registers the sampling-filter
     */
    static void RegisterFilter();
};

}
}

#endif  // SAMPLING_FILTER_H__
