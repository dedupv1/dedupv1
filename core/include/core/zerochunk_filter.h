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

#ifndef ZEROCHUNK_FILTER_H__
#define ZEROCHUNK_FILTER_H__

#include <tbb/atomic.h>

#include <core/dedup.h>
#include <core/filter.h>
#include <base/profile.h>
#include <base/sliding_average.h>

#include <string>

namespace dedupv1 {
namespace filter {

/**
 * The zero-chunk filter is a special handling of the zero-chunk.
 *
 * The zero-chunk filter is usually the first filter in the filter chain.
 * \ingroup filterchain
 */
class ZeroChunkFilter: public Filter {
    private:

        /**
         * Type for statistics about the zero-chunk filter
         */
        class Statistics {
            public:
                Statistics();
                tbb::atomic<uint64_t> reads_;
                tbb::atomic<uint64_t> existing_hits_;
                tbb::atomic<uint64_t> weak_hits_;
        };

        /**
         * Statistics about the zero-chunk filter
         */
        Statistics stats_;

    public:

        /**
         * Constructor
         * @return
         */
        ZeroChunkFilter();

        /**
         * Destructor.
         *
         * @return
         */
        virtual ~ZeroChunkFilter();

        /**
         * Checks if the chunk (mapping) is the zero-chunk.
         * If the chunk is the zero-chunk, FILTER_EXISTING is returned.
         * Otherwise, the filter is not making any statement about a chunk and
         * FILTER_WEAK_MAYBE is returned.
         *
         * @param session
         * @param block_mapping
         * @param mapping
         * @return
         */
        virtual enum filter_result Check(dedupv1::Session* session,
                const dedupv1::blockindex::BlockMapping* block_mapping,
                dedupv1::chunkindex::ChunkMapping* mapping,
                dedupv1::base::ErrorContext* ec);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        /**
         * Restores the statistics data
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        /**
         * Prints statistics about the zero-chunk filter.
         * @return
         */
        virtual std::string PrintStatistics();

        /**
         * Create a new instance of the filter
         * @return
         */
        static Filter* CreateFilter();

        /**
         * Registers the filter at the filter type registery.
         */
        static void RegisterFilter();

        DISALLOW_COPY_AND_ASSIGN(ZeroChunkFilter);
};

}
}

#endif  // ZEROCHUNK_FILTER_H__
