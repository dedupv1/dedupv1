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

#ifndef DEDUPV1_CHECKER_H_
#define DEDUPV1_CHECKER_H_

#include <string>

#include <core/chunk_index.h>
#include <core/container_storage.h>

#include "dedupv1d.h"
#include <google/sparse_hash_map>
#include <tr1/unordered_map>

namespace dedupv1 {
namespace contrib {
namespace check {

/**
 * Class that checks the data integrity of the deduplication system as good as possible.
 *
 * The operations log must be replayed to run the checker. As the integrity
 * of an index might not be complete, when the log is not replayed.
 *
 * It is important to note that no background operations should be performed during the checking and
 * during an possible log replay before the check.
 *
 */
class Dedupv1Checker {
    public:
        /**
         * Constructor
         */
        Dedupv1Checker(bool check_log, bool repair);

        /**
         * Destructor
         */
        ~Dedupv1Checker();

        /**
         * Initialize the checker
         */
        bool Initialize(const std::string& filename);

        /**
         * Performs a full replay of the log
         */
        bool ReplayLog();

        /**
         * Performs the check (and easy repairs) on the chunk index, the block index and other dedupv1 data structures.
         *
         * If check_log_only_ is set, the call returns immediately with the return code true.
         * If repair_ is set, the check tries to repair easy errors.
         */
        bool Check();

        /**
         * Closes the system.
         */
        bool Stop();

        /**
         * returns the number of reported errors
         */
        inline uint32_t reported_errors() const;

        /**
         * returns the number of fixed (repaired) errors
         */
        inline uint32_t fixed_errors() const;

        /**
         * set the number of passes, in which the chunks will be divided.
         *
         * 0 means, it will be calculated. Maximum is 2^15. If passes is not a power of 2
         * it will be expanded to the next power of 2.
         *
         * @param passes the number of passes
         */
        bool set_passes(const uint32_t passes);

        /**
         * get the number of passes, in which the chunks will be divided. 0 means, it will be calculated.
         *
         * @return the number of passes
         */
        inline uint32_t passes() const;

        /**
         * Number of Chunks that have been skipped over all passes.
         *
         * This method is only for testing, especially in the unit tests it is helpful.
         * After a call of Check this method should deliver (run-passes - 1) * #Chunks in System.
         *
         * @return Number of Chunks that have been skipped over all passes.
         */
        inline uint64_t get_all_pass_skipped_chunks() const;

        /**
         * Number of Chunks that have been processed over all passes.
         *
         * This method is only for testing, especially in the unit tests it is helpful.
         * After a call of Check this method should deliver #Chunks in System.
         *
         * @return Number of Chunks that have been processed over all passes.
         */
        inline uint64_t get_all_pass_processed_chunks() const;

        /**
         * The Reference to the internal used dedupv1.
         *
         * This method is only for testing.
         *
         * @return the reference to the internal used dedupv1.
         */
        inline dedupv1d::Dedupv1d* dedupv1d();

    private:

        /**
         * For each prefix we want to know the difference in usage between block and chunk index
         * and the number of chunks in it.
         */
        struct usage_data {
            usage_data() : usage_count(0), usage_chunks(0) {
            }

            int32_t usage_count;
            uint8_t usage_chunks;
        };

        /**
         * Size a chunk takes in memory
         */
        static const uint16_t kChunkSize = 16;

        /**
         * Pointer to the daemon class
         */
        dedupv1d::Dedupv1d* system_;

        /**
         * Pointer to the core system
         */
        dedupv1::DedupSystem* dedup_system_;

        /**
         * iff true, the system should try to repair found errors. It is not
         * possible to correct all kinds of errors.
         */
        bool repair_;

        /**
         * iff true, only the consistency of the log should be checked
         */
        bool check_log_only_;

        /**
         * iff true, the system has been started.
         */
        bool started_;

        /**
         * number of found errors.
         *
         */
        uint32_t reported_errors_;

        /**
         * number of repairs errors.
         */
        uint32_t fixed_errors_;

        /**
         * Number of passes in which we divide the Chunk Indices
         */
        uint32_t run_passes_;

        /**
         * The pass in which we are in now
         * (between 0 and run_passes_, where 0 means we are not Checking at the moment)
         */
        uint32_t actual_run_pass_;

        /**
         * Bitmask to decide if a chunk is in actual pass or not.
         */
        uint64_t pass_bitmask_;

        /**
         *
         * this map hashes from the 64-bit prefix of a fp, to the summed
         * usage count.
         *
         * On important point to consider is the checking as the usage counter as
         * the naive approach may not fit in memory, there I will use an n-byte prefix
         * of the fp to save RAM. The probability of a prefix collision and that here
         * are multiple gc errors whose errors hide themselves is low.
         **/
        std::tr1::unordered_map<uint64_t, usage_data> usage_count_prefix_map_;

        /**
         * prefixes, that reached INT32_MAX hits in block index. This ones have to be checked sperately.
         */
        std::tr1::unordered_map<uint64_t, uint8_t> overrun_prefix_map_;

        /**
         * prefixes, that reached INT32_MIN while reading Chunk Index.
         *
         * Here we have to repair the usage count, but we do not know the exact difference.
         * Therefore another run over the block index is necessary.
         */
        std::tr1::unordered_map<uint64_t, uint8_t> underrun_prefix_map_;

        /**
         * prefixes, where we found a difference.
         *
         * If we have a usage_chunk of 1 here, we do not need another run over the block index.
         */
        std::tr1::unordered_map<uint64_t, usage_data> error_prefix_map_;

        /**
         * this map hashes from the (container file index, file offset) to the container at that place.
         */
        std::map<uint32_t, google::sparse_hash_map<uint64_t, uint64_t> > container_address_inverse_map_;

        /**
         * Called by ReadContainerData
         *
         * @return
         */
        bool CheckContainerItem(dedupv1::chunkindex::ChunkIndex* chunk_index, dedupv1::Fingerprinter* fp_gen,
                dedupv1::chunkstore::Container* container, const dedupv1::chunkstore::ContainerItem* item);

        /**
         * Call after ReadBlockIndex and ReadChunkIndex
         * @return
         */
        bool CheckUsageCount();

        /**
         * Called by CheckUsageCount if there are deep checks or repairs necessary.
         *
         * If repair_ is false this method only has a look at the orrun_prefix_map_. For this elements we
         * do not know, if there has an error occured or not. Therefore the method scans this entries deeply
         * and increases reported_errors_ if one is found.
         *
         * If repair_ is true this errors are also repaired. I also scans for the entries in uderrin_prefix_map_
         * and error_prefix_map_ and repairs demeaged usage counts.
         *
         * Note: This method uses ChunkIndex->PutOverride without holding a lock, therefore it may not be used while the
         * Garbage Collection is running.
         *
         * @return false on error, else true
         */
        bool RepairChunkCount();

        /**
         * Scans through the complete block index and check its consistency
         */
        bool ReadBlockIndex();

        /**
         * Scans through the complete chunk index and checks its consistency
         */
        bool ReadChunkIndex();

        /**
         * Scans through all containers and checks the consistency of the container storage
         */
        bool ReadContainerData();

        /**
         * Calculate the number of passes the check has to be split in
         */
        bool CalcPasses();

        /**
         * Number of chunks, that have been skipped because they were not part of the actual pass.
         * This should be #Chunks * (run_passes - 1)
         */
        uint64_t all_pass_skipped_chunks_;

        /**
         * Number of Chunks that have been processed. This should be #Chunks.
         */
        uint64_t all_pass_processed_chunks_;
};

inline dedupv1d::Dedupv1d* Dedupv1Checker::dedupv1d() {
    return system_;
}

inline uint64_t Dedupv1Checker::get_all_pass_skipped_chunks() const {
    return all_pass_skipped_chunks_;
}

inline uint64_t Dedupv1Checker::get_all_pass_processed_chunks() const {
    return all_pass_processed_chunks_;
}

inline uint32_t Dedupv1Checker::passes() const {
    return this->run_passes_;
}

inline uint32_t Dedupv1Checker::reported_errors() const {
    return reported_errors_;
}

inline uint32_t Dedupv1Checker::fixed_errors() const {
    return fixed_errors_;
}

}
}
}

#endif /* DEDUPV1_CHECKER_H_ */
