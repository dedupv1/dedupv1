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

#ifndef DEDUPV1D_VOLUME_CLONER_H_
#define DEDUPV1D_VOLUME_CLONER_H_

#include "dedupv1d.pb.h"

#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_hash_map.h>

namespace dedupv1d {

class Dedupv1dVolumeInfo;

/**
 * The volume cloner is used to
 * clone a volume (or parts of it) into a new volume in the background.
 */
class Dedupv1dVolumeFastCopy {
    private:
        DISALLOW_COPY_AND_ASSIGN(Dedupv1dVolumeFastCopy);

        static const uint32_t kQueueRetryTimeout = 20;

        static const uint64_t kFastCopyStepSize = 64 * 1024 * 1024;

        /**
         * Enumeration of the states of the detacher
         */
        enum state {
            CREATED,//!< STATE_CREATED
            STARTED,//!< STATE_STARTED
            RUNNING,//!< STATE_RUNNING
            STOPPED //!< STATE_STOPPED
        };

        /**
         * Pointer to the volume info. Should always be
         * non-NULL.
         */
        Dedupv1dVolumeInfo* volume_info_;

        /**
         * Lock to protect the state of the detacher.
         */
        dedupv1::base::MutexLock lock_;

        /**
         * Map from a source volume id to all target volume ids to which clone operations are currently
         * in progress.
         *
         * Protected by lock_
         */
        std::multimap<uint32_t, uint32_t> source_map_;

        /**
         * A lock on a single target should not be hold for a long time
         */
        tbb::concurrent_hash_map<uint32_t, VolumeFastCopyJobData> fastcopy_map_;

        dedupv1::base::Thread<bool> fastcopy_thread_;

        tbb::concurrent_queue<uint32_t> fastcopy_queue_;

        dedupv1::base::Condition change_condition_;

        dedupv1::InfoStore* info_store_;

        VolumeInfoFastCopyData fastcopy_data_;

        /**
         * State of the cloner.
         */
        enum state state_;

        /**
         * Loop method for a background thread.
         *
     * @return true iff ok, otherwise an error has occurred
         */
        bool FastCopyThreadRunner();

        bool DeleteFromFastCopyData(int target_id);

        bool ProcessFastCopyStep(VolumeFastCopyJobData* data);

        bool PersistFastCopyData();

        bool UpdateFastCopyData(const VolumeFastCopyJobData& data);

    public:
        /**
         *
         * @param volume_info volume_info that might not be initialzed at that point in time
         * @return
         */
        explicit Dedupv1dVolumeFastCopy(Dedupv1dVolumeInfo* volume_info);

        virtual ~Dedupv1dVolumeFastCopy();

        /**
         * Configures the cloner.
         *
         * No available options
         *
         * @param option_name
         * @param option
     * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Starts the cloner.
     * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context, dedupv1::InfoStore* info_store);

        /**
         * Starts the background threads of the cloner.
     * @return true iff ok, otherwise an error has occurred
         */
        bool Run();

        /**
         * Stops the background threads.
     * @return true iff ok, otherwise an error has occurred
         */
        bool Stop(const dedupv1::StopContext& stop_context);

        /**
         * The parameter has been validated, the dedupv1d volume info ensures that both volumes
         * are currently in maintenance mode and will stay in this mode until the clone operation finishes
         *
     * @return true iff ok, otherwise an error has occurred
         */
        bool StartNewFastCopyJob(uint32_t src_volume_id,
                uint32_t target_volume_id,
                uint64_t source_offset,
                uint64_t target_offset,
                uint64_t size);

        bool IsFastCopySource(uint32_t volume_id);

        bool IsFastCopyTarget(uint32_t volume_id);

        dedupv1::base::Option<VolumeFastCopyJobData> GetFastCopyJob(uint32_t target_id);

        /**
         * returns the pointer to the volume info
         * @return
         */
        inline Dedupv1dVolumeInfo* volume_info();

#ifdef DEDUPV1D_TEST
        void ClearData();
#endif
};

Dedupv1dVolumeInfo* Dedupv1dVolumeFastCopy::volume_info() {
    return this->volume_info_;
}

}

#endif /* DEDUPV1D_VOLUME_CLONER_H_ */
