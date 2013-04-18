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

#ifndef DEDUPV1D_VOLUME_H__
#define DEDUPV1D_VOLUME_H__

#include <stdint.h>

#include <core/dedup.h>
#include <base/locks.h>
#include <base/thread.h>
#include <core/dedup_volume.h>
#include <core/dedupv1_scsi.h>
#include <core/statistics.h>

#include "dedupv1d.pb.h"

#include <set>
#include <list>
#include <vector>

#include "scst_handle.h"
#include "dedupv1d_session.h"
#include "command_handler.h"

#include <tbb/concurrent_hash_map.h>

namespace dedupv1d {

/**
 *
 * The life cycle is special as a volume can have more than one Run/Stop lifecycle because
 * volumes can be into a maintenance mode and restart from that mode.
 *
 * \ingroup dedupv1d
 */
class Dedupv1dVolume : public dedupv1::StatisticProvider {
    DISALLOW_COPY_AND_ASSIGN(Dedupv1dVolume);
    friend class Dedupv1dVolumeInfo;
public:

    /**
     * Default block size.
     * Here we mean the device sector size with the block size.
     * It can be changed to e.g. 4K, but current Windows and Linux system might have problems with that.
     */
    static const uint32_t kDefaultBlockSize = 512;

    /**
     * Type for the state of a dedupv1d volume.
     * A typical life cycle is CREATED -> STARTED -> RUNNING -> STOPPED
     * If a volume is in failed mode, the running state failed.
     * If a volume is in maintenance mode, the running state is skipped.
     * We do not represent the maintenance mode as a state (in the sense of this
     * enumeration) as the state captures more the life-cycle of the object during the
     * program and not the life-cycle of the volume.
     */
    enum dedupv1d_volume_state {
        DEDUPV1D_VOLUME_STATE_CREATED, // !< DEDUPV1D_VOLUME_STATE_CREATED
        DEDUPV1D_VOLUME_STATE_STARTED, // !< DEDUPV1D_VOLUME_STATE_STARTED
        DEDUPV1D_VOLUME_STATE_RUNNING, // !< DEDUPV1D_VOLUME_STATE_RUNNING
        DEDUPV1D_VOLUME_STATE_STOPPED, // !< DEDUPV1D_VOLUME_STATE_STOPPED
        DEDUPV1D_VOLUME_STATE_FAILED // !< DEDUPV1D_VOLUME_STATE_FAILED
    };

    class Statistics {
public:
        Statistics();
        /**
         * number of currently throttled threads
         */
        tbb::atomic<uint32_t> throttled_thread_count_;

        /**
         * Statistics about the average throttle time
         */
        dedupv1::base::SimpleSlidingAverage throttle_time_average_;
    };
private:

    /**
     * Handle for the connection with SCST
     */
    ScstHandle handle_;

    /**
     * Pointer to a command handler that handles all requests
     * for this volume
     */
    CommandHandler ch_;

    /**
     * Pointer to the base dedup volume.
     * TODO (dmeister): Refactor?
     */
    dedupv1::DedupVolume volume_;

    /**
     * number of bits of the block size.
     * That is the number of bits a offset has to be
     * shifted to get the block id.
     */
    uint32_t block_shift_;

    /**
     * External visible block size (aka sector size)
     * Usually set to 512 bytes.
     */
    uint32_t block_size_;

    /**
     * Number of blocks in the volume.
     * block_count * block_size gives the overall size of the
     * volume.
     */
    uint64_t block_count_;

    /**
     * Name of the volume.
     */
    std::string device_name_;

    Statistics stats_;

    /**
     * Vector of group entries of the volume.
     * Each group entry consists of the group name and
     * the LUN index of this volume.
     *
     * Note: Each group needs a LUN 0.
     */
    std::vector<std::pair<std::string, uint64_t> > groups_;

    /**
     * Vector of target entries of the volume.
     */
    std::vector<std::pair<std::string, uint64_t> > targets_;

    /**
     * Flag if the volume is preconfigured.
     * A preconfigured volume cannot be modified during runtime,
     * especially it cannot be deleted.
     * Preconfigured volume should rarely be used in production, but
     * they are useful for development and testing.
     *
     */
    bool preconfigured_;

    /**
     * The number of command handling threads of this volume
     */
    uint16_t command_thread_count_;

    /**
     * State of the volume
     */
    enum dedupv1d_volume_state state_;

    /**
     * Lock to protect all members.
     */
    dedupv1::base::ReadWriteLock lock_;

    /**
     * Lists of current sessions
     */
    tbb::concurrent_hash_map<uint64_t, Dedupv1dSession> session_map_;

    /**
     * Map of all not delivers unit attentions.
     * The map maps from the session id for a queue of unit attentions SCSI results
     */
    tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<dedupv1::scsi::ScsiResult> >
    session_unit_attention_map_;

    /**
     * protected by lock.
     */
    std::set<uint64_t> session_set_;

    /**
     * flag that denotes if the volume is in maintenance mode.
     */
    bool maintenance_mode_;

    /**
     * vector of command handler threads.
     */
    std::vector<dedupv1::base::Thread<bool>*> command_handler_threads_;

    /**
     * List of per-volume filter options
     */
    std::list<std::pair<std::string, std::string> > filter_options_;

    /**
     * List of per-volume chunking options
     */
    std::list<std::pair<std::string, std::string> > chunking_options_;

    /**
     * Info store
     */
    dedupv1::InfoStore* info_store_;

    /**
     * method for a command handler thread.
     *
     * @param thread_index
     * @return true iff ok, otherwise an error has occurred
     */
    bool Runner(int thread_index);
public:
    /**
     * Default number of Threads, if nothing else is set
     */
    static const uint16_t kDefaultCommandThreadCount = 16;

    /**
     * Constructor for a volume.
     * @param preconfigured flag that denotes if the volume is preconfigured
     * @return
     */
    explicit Dedupv1dVolume(bool preconfigured);

    /**
     * Destructor
     */
    virtual ~Dedupv1dVolume();

    /**
     * Inits the volume.
     * @return true iff ok, otherwise an error has occurred
     */
    bool Init();

    /**
     * Starts the volume
     * @param system Should never be NULL
     * @return true iff ok, otherwise an error has occurred
     */
    bool Start(dedupv1::DedupSystem* system);

    /**
     * Configures the volume.
     *
     * Available options:
     * - threads: uint16_t
     * - device-name: String
     * - sector-size: uint_32
     * - group: String
     * - target: String
     * - maintenance: Boolean
     * - filter: String
     * - chunking: String
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Runs the volume and starts the command handler threads.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool Run();

    /**
     * Stops the volume and its command handler threads
     * @return true iff ok, otherwise an error has occurred
     */
    bool Stop(const dedupv1::StopContext& stop_context);

    /**
     * Closes the volume
     * @return true iff ok, otherwise an error has occurred
     */
    bool Close();

    /**
     * Adds a new session.
     *
     * @param session
     * @return true iff ok, otherwise an error has occurred
     */
    bool AddSession(const Dedupv1dSession& session);

    /**
     * Remove a session.
     * @param session
     * @return true iff ok, otherwise an error has occurred
     */
    bool RemoveSession(uint64_t session_id);

    /**
     * Finds a session with the given session id
     * @param session_id
     * @return
     */
    dedupv1::base::Option<Dedupv1dSession> FindSession(uint64_t session_id) const;

    dedupv1::base::Option<std::set<uint64_t> > GetSessionSet();

    /**
     * returns the block shift
     * @return
     */
    inline uint32_t block_shift() const;

    /**
     * returns the block size
     * @return
     */
    inline uint32_t block_size() const;

    /**
     * returns the number of blocks of the volume.
     * @return
     */
    inline uint64_t block_count() const;

    /**
     * returns the number of sessions
     * @return
     */
    uint32_t session_count() const;

    /**
     * returns the volume id
     * @return
     */
    uint32_t id() const;

    /**
     * returns the logical volume size
     * @return
     */
    uint64_t logical_size() const;

    /**
     * returns the base volume
     * @return
     */
    inline dedupv1::DedupVolume* volume();

    /**
     * returns the base volume
     * @return
     */
    inline const dedupv1::DedupVolume* volume() const;

    /**
     * returns the scst handle
     * @return
     */
    inline ScstHandle* handle();

    /**
     * returns the state of the volume
     * @return
     */
    inline Dedupv1dVolume::dedupv1d_volume_state state() const;

    /**
     * returns the command handler
     * @return
     */
    inline CommandHandler* command_handler();

    /**
     * returns the command handler thread count
     * @return
     */
    inline uint16_t command_thread_count() const;

    /**
     * returns the (maybe auto generated) device name of the volume
     * @return
     */
    std::string device_name() const;

    /**
     * returns the unique serial number of the volume.
     * @return
     */
    uint64_t unique_serial_number() const;

    /**
     * returns the groups the volume is assigned to
     * @return
     */
    inline const std::vector<std::pair<std::string, uint64_t> >& groups() const;

    /**
     * returns the targets the volume is assigned to
     * @return
     */
    inline const std::vector<std::pair<std::string, uint64_t> >& targets() const;

    /**
     * returns a list of all current sessions.
     * @return
     */
    inline const tbb::concurrent_hash_map<uint64_t, Dedupv1dSession>& session_map() const {
        return session_map_;
    }

    inline tbb::concurrent_hash_map<uint64_t, Dedupv1dSession>& session_map() {
        return session_map_;
    }

    inline tbb::concurrent_hash_map<uint64_t, tbb::concurrent_queue<dedupv1::scsi::ScsiResult> >& session_unit_attention_map() {
        return session_unit_attention_map_;
    }

    /**
     * Add the volume to a new group with a given lun (within the lun).
     *
     * @param group
     * @param lun
     * @return true if no error occurred, otherwise false.
     */
    bool AddGroup(const std::string& group, uint64_t lun);

    /**
     *
     * @param target
     * @param lun
     * @return true if no error occurred, otherwise false.
     */
    bool AddTarget(const std::string& target, uint64_t lun);

    /**
     * removes the volume from a given group
     * @param group
     * @return true if no error occurred, otherwise false.
     */
    bool RemoveGroup(const std::string& group);

    /**
     *
     * @param target
     * @return true if no error occurred, otherwise false.
     */
    bool RemoveTarget(const std::string& target);

    /**
     * returns the flag if the volume is preconfigured or dynamic
     * @return true if no error occurred, otherwise false.
     */
    inline bool is_preconfigured() const;

    /**
     * returns if the volume is in maintenance mode
     * @return true if no error occurred, otherwise false.
     */
    inline bool maintenance_mode() const;

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    static bool SplitGroupOption(const std::string& option, std::string* group, uint64_t* lun);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    static bool JoinGroupOption(const std::string& group, uint64_t lun, std::string* group_lun_pair);

    /**
     * returns a developer-readable representation of the
     * volume.
     *
     * @return
     */
    std::string DebugString() const;

    /**
     * Serializes the data of a volume (including group membership)
     * to a protobuf message.
     *
     * @param data
     * @return true iff ok, otherwise an error has occurred
     */
    bool SerializeTo(VolumeInfoData* data) const;

    /**
     * Parses the configuration of a volume from a
     * protobuf message. This method should only be
     * called in the configuration phase.
     *
     * @param data
     * @return true if no error occurred, otherwise false.
     */
    bool ParseFrom(const VolumeInfoData& data);

    /**
     * Note: We don't use the normal set_maintainance_mode here as the
     * method has to do much more than a normal setter
     *
     * @param maintaince_mode
     * @return true if no error occurred, otherwise false.
     */
    bool ChangeMaintenanceMode(bool maintaince_mode);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool ChangeLogicalSize(uint64_t new_logical_size);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool ChangeOptions(const std::list<std::pair<std::string, std::string> >& options);

    /**
     * Makes a request to the volume.
     *
     * @param type
     * @param offset
     * @param size
     * @param buffer
     * @return
     */
    dedupv1::scsi::ScsiResult MakeRequest(dedupv1::request_type type,
                                          uint64_t offset,
                                          uint64_t size,
                                          byte* buffer,
                                          dedupv1::base::ErrorContext* ec);

    dedupv1::base::Option<bool> Throttle(int thread_id, int thread_count);

    dedupv1::scsi::ScsiResult SyncCache();

    /**
     *
     * @param prefix
     * @param ps
     * @return true if no error occurred, otherwise false.
     */
    bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     *
     * @param prefix
     * @param ps
     * @return true if no error occurred, otherwise false.
     */
    bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    std::string PrintTrace();
    std::string PrintLockStatistics();
    std::string PrintProfile();
    std::string PrintStatistics();

#ifdef DEDUPV1D_TEST
    void ClearData();
#endif
};

uint32_t Dedupv1dVolume::block_shift() const {
    return this->block_shift_;
}

uint32_t Dedupv1dVolume::block_size() const {
    return this->block_size_;
}

uint64_t Dedupv1dVolume::block_count() const {
    return this->block_count_;
}

Dedupv1dVolume::dedupv1d_volume_state Dedupv1dVolume::state() const {
    return this->state_;
}

uint16_t Dedupv1dVolume::command_thread_count() const {
    return this->command_thread_count_;
}

const std::vector<std::pair<std::string, uint64_t> >& Dedupv1dVolume::groups() const {
    return this->groups_;
}

const std::vector<std::pair<std::string, uint64_t> >& Dedupv1dVolume::targets() const {
    return this->targets_;
}

dedupv1::DedupVolume* Dedupv1dVolume::volume() {
    return &this->volume_;
}

const dedupv1::DedupVolume* Dedupv1dVolume::volume() const {
    return &this->volume_;
}

ScstHandle* Dedupv1dVolume::handle() {
    return &this->handle_;
}

CommandHandler* Dedupv1dVolume::command_handler() {
    return &this->ch_;
}

bool Dedupv1dVolume::is_preconfigured() const {
    return this->preconfigured_;
}

inline bool Dedupv1dVolume::maintenance_mode() const {
    return this->maintenance_mode_;
}

}

#endif  // DEDUPV1D_VOLUME_H__
