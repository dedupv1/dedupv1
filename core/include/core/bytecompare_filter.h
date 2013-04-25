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

#ifndef BYTECOMPARE_FILTER_H__
#define BYTECOMPARE_FILTER_H__

#include <tbb/atomic.h>

#include <core/dedup.h>
#include <core/filter.h>
#include <base/profile.h>
#include <base/sliding_average.h>
#include <core/storage.h>

#include <string>

namespace dedupv1 {
namespace filter {

/**
 * The bytecompare filter is a security related filter to check
 * the chunk date byte-by-byte to assure that a chunk, marked
 * as known by other filters, is really a duplicate.
 *
 * The bytecompare-filter is usually the last filter in a filter chain.
 *
 * \ingroup filterchain
 */
class ByteCompareFilter: public Filter {
    private:

        /**
         * Type for statistics about the bytecompare filter
         */
        class Statistics {
            public:
                Statistics();
                tbb::atomic<uint64_t> reads_;
                tbb::atomic<uint64_t> hits_;
                tbb::atomic<uint64_t> miss_;

                dedupv1::base::Profile time_;

                /**
                 * Profiling information (filter latency in ms)
                 */
                dedupv1::base::SimpleSlidingAverage average_latency_;
        };

        /**
         * Buffer for the byte compare filter.
         * The buffer has to be at least as large as the maximal chunk size.
         */
        size_t buffer_size_;

        /**
         * Statistics about the byte-compare filter
         */
        Statistics stats_;

        dedupv1::chunkstore::Storage* storage_;

    public:

        /**
         * Constructor
         * @return
         */
        ByteCompareFilter();

        /**
         * Destructor.
         *
         * @return
         */
        virtual ~ByteCompareFilter();

        virtual bool Start(dedupv1::DedupSystem* dedup_system);

        /**
         * Checks if the chunk (mapping) is really a duplicate.
         * It reads the chunk date from the storage, and performs
         * a byte-wise comparison. If the comparison fails,
         * NOT_EXISTING is returned and the data is newly stored.
         * If the comparison succeeds, EXISTING is returned and the
         * execution of the filter chain is finished.
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
         * Prints profiling information about the bytecompare filter.
         *
         * @return
         */
        virtual std::string PrintProfile();

        /**
         * Prints statistics about the bytecompare filter.
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

        DISALLOW_COPY_AND_ASSIGN(ByteCompareFilter);
};

}
}

#endif  // BYTECOMPARE_FILTER_H__
