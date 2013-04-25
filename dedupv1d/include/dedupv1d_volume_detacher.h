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

#ifndef DEDUPV1D_VOLUME_DETACHER_H__
#define DEDUPV1D_VOLUME_DETACHER_H__

#include <map>

#include <core/dedup.h>
#include <base/thread.h>
#include <base/locks.h>
#include <base/index.h>
#include <base/option.h>
#include "dedupv1d_volume.h"

namespace dedupv1d {

class Dedupv1dVolumeInfo;

/**
 * The volume detacher is used to
 * free the resources of a detached volume in the background.
 *
 * It stores all the volumes in the detached state and also
 * stores which blocks are freed. The blocks are freed from the
 * beginning to the end.
 */
class Dedupv1dVolumeDetacher {
    private:
        DISALLOW_COPY_AND_ASSIGN(Dedupv1dVolumeDetacher);

        /**
         * Sleep time in microseconds.
         * 1,000,000 us = 1s
         */
        static const int kDefaultBusyDetachSleepTime = 1 * 1000 * 1000;

        /**
         * size of detachment batches if the system is busy.
         */
        static const int kDefaultBusyBatchSize = 4;

        /**
         * size of detachment batches if the system is idle.
         */
        static const int kDefaultIdleBatchSize = 256;

        /**
         * Default sleep time between detachment batches in idle
         * phases in microseconds.
         */
        static const int kDefaultIdleDetachSleepTime = 20 * 1000;

        /**
         * Enumeration of the states of the detacher
         */
        enum dedupv1d_volume_detacher_state {
            STATE_CREATED,//!< STATE_CREATED
            STATE_STARTED,//!< STATE_STARTED
            STATE_RUNNING,//!< STATE_RUNNING
            STATE_STOPPED //!< STATE_STOPPED
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
         * Note: No one except the thread that has called the Stop() method is allowed to
         * modify this map if detacher is stopped (aka in STATE_STOPPED state)
         */
        std::map<uint32_t, dedupv1::base::Thread<bool>* > detaching_threads_;

        /**
         * Persistent index that stores data about volumes that are detached and are currently
         * vanished from the system. Vanishing means to delete all block mappings of the volume so that
         * the data can be freed. For the time a volume id is used by a volume in the detaching state, the
         * volume is not allowed to be reused.
         *
         * The index stores uint32 volume ids as keys and VolumeInfoDetachingData values
         */
        dedupv1::base::PersistentIndex* detaching_info_;

        /**
         * State of the detacher.
         */
        enum dedupv1d_volume_detacher_state state;

        /**
         * Loop method for a background thread.
         * The detacher uses a background thread for each
         * volume in the detachted state.
         *
         * @param volume_id
     * @return true iff ok, otherwise an error has occurred
         */
        bool DetachingThreadRunner(uint32_t volume_id);

        /**
         * returns the threads that do the detaching.
         * @return
         */
        inline std::map<uint32_t, dedupv1::base::Thread<bool>* >* detaching_threads();

    public:
        /**
         *
         * @param volume_info volume_info that might not be initialzed at that point in time
         * @return
         */
        explicit Dedupv1dVolumeDetacher(Dedupv1dVolumeInfo* volume_info);

        virtual ~Dedupv1dVolumeDetacher();

        /**
         * Configures the detacher.
         *
         * Available options:
         * - type: String
         *
         * @param option_name
         * @param option
     * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Starts the detacher.
     * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context);

        /**
         * Starts the background threads of the detacher.
       * @return true iff ok, otherwise an error has occurred
         */
        bool Run();

        /**
         * Stops the background threads.
     * @return true iff ok, otherwise an error has occurred
         */
        bool Stop(const dedupv1::StopContext& stop_context);

        /**
         * Detaches the given volume.
         * @param volume
     * @return true iff ok, otherwise an error has occurred
         */
        bool DetachVolume(const Dedupv1dVolume& volume);

        /**
         * Checks if a given volume (by id) is currently
         * in detaching state.
         *
         * @param volume_id
         * @param detaching_state
         * @return
         */
        bool IsDetaching(uint32_t volume_id, bool* detaching_state);

        /**
         * Declares that a volume that is in detaching mode is fully detached.
         *
         * Acquires a lock. This method should usually not be called.
         * @param volume
         * @return
         */
        bool DeclareFullyDetached(uint32_t volume);

        /**
         * returns the detaching data.
         * @return
         */
        inline dedupv1::base::PersistentIndex* detaching_info();

        /**
         * returns the pointer to the volume info
         * @return
         */
        inline Dedupv1dVolumeInfo* volume_info();

        dedupv1::base::Option<std::list<uint32_t> > GetDetachingVolumeList();

#ifdef DEDUPV1D_TEST
        void ClearData();
#endif
};

Dedupv1dVolumeInfo* Dedupv1dVolumeDetacher::volume_info() {
    return this->volume_info_;
}

dedupv1::base::PersistentIndex* Dedupv1dVolumeDetacher::detaching_info() {
    return this->detaching_info_;
}

std::map<uint32_t, dedupv1::base::Thread<bool>* >* Dedupv1dVolumeDetacher::detaching_threads() {
    return &this->detaching_threads_;
}

}

#endif  // DEDUPV1D_VOLUME_DETACHER_H__
