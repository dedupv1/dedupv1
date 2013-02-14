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

#ifndef DEDUP_VOLUME_H__
#define DEDUP_VOLUME_H__

#include <string>
#include <set>
#include <list>

#include <core/dedup.h>
#include <core/dedupv1_scsi.h>
#include <core/request.h>
#include <core/filter_chain.h>
#include <core/session.h>
#include <base/resource_management.h>
#include <core/chunker.h>
#include <base/error.h>
#include <core/statistics.h>

namespace dedupv1 {

class DedupSystem;

/**
 * The class represents a virtual volume.
 *
 * Note: If a volume has been registered at the DedupVolumeInfo, the volume
 * should not be closed before it is unregistered.
 */
class DedupVolume : public dedupv1::StatisticProvider {
    public:
        static const uint32_t kVolumeBits = 16;
        static const uint32_t kMaxVolumeId = (64 * 1024);
        static const uint32_t kUnsetVolumeId = static_cast<uint32_t>(-1);
    private:
        DISALLOW_COPY_AND_ASSIGN(DedupVolume);

        /**
         * id of the volume
         */
        uint32_t id_;

        /**
         * logical size of the volume
         */
        uint64_t logical_size_;

        /**
         * Reference to the dedup system.
         * Only set after the start
         */
        DedupSystem* system_;

        std::set<std::string> enabled_filter_names_;

        std::list<std::pair<std::string, std::string> > chunking_config_;

        dedupv1::Chunker* chunker_;

        /**
         * Session management.
         *
         * Sessions are very expensive to allocate and free.
         * We therefore use (and reuse) a fixed number of sessions.
         */
        dedupv1::base::ResourceManagement<dedupv1::Session>* session_management_;

        /**
         * Number of sessions for the session management.
         * This is also the maximal number of concurrency in the system.
         * TODO: If there are no open sessions, the result should not be an error, but the
         * thread should wait.
         */
        unsigned int session_count_;

        bool maintainance_mode_;

        bool ChangePerVolumeOption(const std::string& option_name, const std::string& option);
    protected:
        /**
        * Transforms the volume offset into a dedupv1 block id and request offset.
        *
        * @param offset
        * @param request_block_id
        * @param request_offset
        * @return returns false if either a out parameter is not set of the volume is not started
        */
        bool MakeIndex(uint64_t offset, uint64_t* request_block_id, uint64_t* request_offset) const;
    public:
        /**
         * Constructor
         */
        DedupVolume();

        /**
         * Destructor
         */
        virtual ~DedupVolume();

        /**
        * Configures the volume.
        *
        * Available options:
        * - logical-size: StorageUnit
        * - id: uint32_t
        * - session-count: StorageUnit
        * - chunking: String
        * - chunking.*: String
        * - filter: String
        *
        * @param option_name
        * @param option
        * @return true iff ok, otherwise an error has occurred
        */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
        * Starts the volume.
        *
        * After the volume is started, requests can be made.
        *
        * @param system Reference to the dedupe system object
        * @return true iff ok, otherwise an error has occurred
        */
        bool Start(DedupSystem* system, bool initial_maintaince_mode);

        bool Close();

        bool ChangeMaintenanceMode(bool maintaince_mode);

        bool ChangeOptions(const std::list<std::pair<std::string, std::string> >& options);

        dedupv1::scsi::ScsiResult FastCopyTo(
                DedupVolume* target_volume,
                uint64_t src_offset,
                uint64_t target_offset,
                uint64_t size,
                dedupv1::base::ErrorContext* ec);

        /**
        * performs a request on the volume.
        *
        * The request is delegated the the dedup system.
        *
        * @param rw
        * @param offset
        * @param size
        * @param buffer
        * @param ec error context (can be NULL)
        * @return
        */
        dedupv1::scsi::ScsiResult MakeRequest(enum request_type rw,
                uint64_t offset,
                uint64_t size,
                byte* buffer,
                dedupv1::base::ErrorContext* ec);


        dedupv1::base::Option<bool> Throttle(int thread_id, int thread_count);

        dedupv1::scsi::ScsiResult SyncCache();

        /**
        * returns the unique id of the volume
        * @return
        */
        inline uint32_t GetId() const;

        /**
        * returns the logical size of the volume
        * @return
        */
        inline uint64_t GetLogicalSize() const;

        /**
        * returns the block interval of the volume.
        * the start block id is the first block of this volume. The end block id is the first block id that belongs
        * not to the valid interval (similar to the usage in for loops, etc.). So
        * start_block_id() and end_block_id() form an half-open interval.
        *
        * The interval can only be calculated if the volume is started.
        *
        * Note: We do not use normal getter methods because the call might fail.
        *
        * @param start_block_id
        * @param end_block_id
     * @return true iff ok, otherwise an error has occurred
        */
        bool GetBlockInterval(uint64_t* start_block_id, uint64_t* end_block_id) const;

        bool ChangeLogicalSize(uint64_t new_logical_size);

        virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        /**
         * Returns statistics about the lock contention as JSON string.
         * @return
         */
        virtual std::string PrintLockStatistics();

        /**
         * Returns statistics about the system as JSON string.
         * @return
         */
        virtual std::string PrintStatistics();

        /**
         * Returns profiling information about the system as JSON string.
         * @return
         */
        virtual std::string PrintProfile();

        /**
         * Returns trace information about the system as JSON strin.g
         * @return
         */
        virtual std::string PrintTrace();

        /**
         * Returns the chunker
         */
        inline dedupv1::Chunker* chunker() {
            return chunker_;
        }

        /**
         * Returns true iff the volume has been started
         */
        inline bool is_started() {
            return system_ != NULL;
        }

        inline const std::set<std::string>& enabled_filter_names() const {
            return enabled_filter_names_;
        }

        inline const std::list<std::pair<std::string, std::string> >& chunking_config() const {
            return chunking_config_;
        }

        inline const dedupv1::base::ResourceManagement<dedupv1::Session>* session_management() const {
            return session_management_;
        }

        /**
         * Returns a developer-readable representation of the volume
         */
        std::string DebugString() const;
};

uint32_t DedupVolume::GetId() const {
    return this->id_;
}

uint64_t DedupVolume::GetLogicalSize() const {
    return this->logical_size_;
}

}

#endif  // DEDUP_VOLUME_H__
