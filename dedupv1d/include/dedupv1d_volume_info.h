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

#ifndef DEDUPV1D_VOLUME_INFO_H__
#define DEDUPV1D_VOLUME_INFO_H__

#include <core/dedup.h>
#include <base/locks.h>
#include <core/dedup_volume_info.h>
#include <base/startup.h>
#include <base/option.h>

#include <string>
#include <list>
#include <map>

#include "dedupv1d_group_info.h"
#include "dedupv1d_target_info.h"
#include "dedupv1d_volume_detacher.h"
#include "dedupv1d_volume_fastcopy.h"
#include "dedupv1d_volume.h"

namespace dedupv1d {

/**
 * Note: To carry the options of a volume, we use a list of string pairs.
 * While a map seems to be the better fit, we preserve the ordering of the options when using
 * a list.
 */
class Dedupv1dVolumeInfo: public dedupv1::StatisticProvider {
    private:
        DISALLOW_COPY_AND_ASSIGN(Dedupv1dVolumeInfo);

        /**
         * a list of volumes
         */
        std::list<Dedupv1dVolume*> volumes_;

        /**
         * a map from the volume id to the volume instance
         *
         * TODO (dmeister): is the alternative to map the list<T>::iterator better?
         */
        std::map<uint32_t, Dedupv1dVolume*> volume_map_;

        /**
         * a map from the name of a volume to the volume instance
         */
        std::map<std::string, Dedupv1dVolume*> volume_name_map_;

        /**
         * map from a group name to the combinations of a LUN id (first) and a volume instance (second).
         */
        std::multimap<std::string, std::pair<uint64_t, Dedupv1dVolume*> > group_map_;

        /**
         * map from a target name to the combinations of a LUN id and a volume instance.
         */
        std::multimap<std::string, std::pair<uint64_t, Dedupv1dVolume*> > target_map_;

        dedupv1::DedupVolumeInfo* base_volume_info_;

        /**
         * Persistent index that stores the volume information.
         * The index stores uint32 volume ids as keys and VolumeInfoData values.
         */
        dedupv1::base::PersistentIndex* info_;

        /**
         * Reference to the base dedup system.
         * The pointer is set in the start method and used to
         * start new volumes that are attached at a later point in time.
         */
        dedupv1::DedupSystem* base_dedup_system_;

        /**
         * Each volume gets this number of threads, if nothing else is specified.
         */
        uint16_t default_command_thread_count_;

        /**
         * flag that denotes if the volume info is started.
         */
        bool started_;

        /**
         * flag that denotes if the volume info is running.
         */
        bool running_;

        /**
         * Lock for the dedupv1d volume info.
         * TODO (dmeister): The lock protects which members?
         */
        dedupv1::base::MutexLock lock_;

        dedupv1d::Dedupv1dGroupInfo* group_info_;

        dedupv1d::Dedupv1dTargetInfo* target_info_;

        /**
         * Pointer to the volume detacher.
         */
        Dedupv1dVolumeDetacher* detacher_;

        Dedupv1dVolumeFastCopy* fast_copy_;

        /**
         * Configures a new volume given the configuration.
         *
         * @param preconfigured
         * @param options
         * @return
         */
        Dedupv1dVolume* ConfigureNewVolume(bool preconfigured,
                const std::list<std::pair<std::string, std::string> >& options);

        /**
         * Checks if the new volume is valid.
         *
         * Note that the detacher must be started, when this method is called as otherwise it is
         * not possible to check if a volume id is currently used by a volume in detaching mode.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool CheckNewVolume(Dedupv1dVolume* new_volume);

        /**
         * Registers a volume at SCST and the base system volume info.
         *
         * @param v
         * @param new_attachment
         * @return true iff ok, otherwise an error has occurred
         */
        bool RegisterVolume(Dedupv1dVolume* v, bool new_attachment);

        /**
         * A version of FindVolume that assumes to hold the lock already.
         *
         * @param id
         * @return
         */
        Dedupv1dVolume* FindVolumeLocked(uint32_t id);

        /**
         * Finds a volume by the group name. The method
         * assumes that the lock is already held.
         *
         * @param group
         * @param lun
         * @return
         */
        Dedupv1dVolume* FindVolumeByGroupLocked(const std::string& group, uint64_t lun);

        /**
         * Finds a volume by the target name. The method
         * assumes that the lock is already held.
         *
         * @param target
         * @param lun
         * @return
         */
        Dedupv1dVolume* FindVolumeByTargetLocked(const std::string& target, uint64_t lun);

        /**
         * Used during the configuration phase. Should not be used after the start.
         */
        std::list<std::pair<std::string, std::string> > current_volume_options_;

        /**
         * Used during the configuration phase. Should not be used after the start.
         */
        std::list<std::list<std::pair<std::string, std::string> > > volume_options_;

        /**
         * Info store
         */
        dedupv1::InfoStore* info_store_;

    public:
        /**
         * Constructor for the volume info
         * @return
         */
        Dedupv1dVolumeInfo();

        /**
         * Destructor
         */
        virtual ~Dedupv1dVolumeInfo();

        /**
         * Starts the volume info.
         *
         * @param system
         * @param group_info
         * @param target_info
         * @param start_context
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context, Dedupv1dGroupInfo* group_info,
                Dedupv1dTargetInfo* target_info, dedupv1::DedupSystem* system);

        /**
         * Configures the volume info.
         *
         * Available options:
         * - volume.*: String
         * - fast-copy.*
         * - type: String
         * - default-thread-count: uint16_t
         *
         * @param option_name
         * @param option
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Starts the volume info and may be start background threads.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Run();

        /**
         * Stops the volume info and stops background threads.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Stop(const dedupv1::StopContext& stop_context);

        /**
         *
         * Note that the detacher must be started, when this method is called as otherwise it is
         * not possible to check if a volume id is currently used by a volume in detaching mode.
         *
         * TODO (dmeister) This method asks for trouble (race condition). Call it with care
         *
         * @param options
         * @return
         */
        Dedupv1dVolume* AttachVolume(std::list<std::pair<std::string, std::string> > options);

        /**
         * TODO (dmeister): This method asks for trouble (race condition). Call it with care
         * @param id
         * @return
         */
        Dedupv1dVolume* FindVolume(uint32_t id, dedupv1::base::MutexLock** lock);

        /**
         * Searches the volume assigned to a given group/LUN combination.
         *
         * @param group group to search from
         * @param lun logical unit number of the volume in the given group
         * @return
         */
        Dedupv1dVolume* FindVolumeByGroup(const std::string& group, uint64_t lun, dedupv1::base::MutexLock** lock);

        /**
         * Returns all volumes in the given group
         * @param group_name
         * @return
         */
        dedupv1::base::Option<std::list<std::pair<uint32_t, uint64_t> > > FindVolumesInGroup(
                const std::string& group_name);

        /**
         * Returns all volumes in the given target
         * @param target_name
         * @return
         */
        dedupv1::base::Option<std::list<std::pair<uint32_t, uint64_t> > > FindVolumesInTarget(
                const std::string& target_name);

        /**
         * TODO (dmeister): This method asks for trouble (race condition). Call it with care
         * @param target
         * @param lun
         * @return
         */
        Dedupv1dVolume* FindVolumeByTarget(const std::string& target, uint64_t lun, dedupv1::base::MutexLock** lock);

        /**
         * Detaches a volume from the system.
         *
         * If volume is detached, the volume is deregistered from SCST, the command threads are terminated, and
         * the volume is moved to the detaching state. The volume data is than stored in the detaching info index.
         *
         * It is not possible to detach a preconfigured volume.
         *
         * @param volume_id
         * @return true iff ok, otherwise an error has occurred
         */
        bool DetachVolume(uint32_t volume_id);

        /**
         * Adds a volume to a group with a given LUN number. The group and the lun are encoded in
         * the format "<group>:<lun>".
         *
         * It is not possible to add a preconfigured volume to a group.
         *
         * @param volume_id
         * @param group_lun_pair
         * @return true iff ok, otherwise an error has occurred
         */
        bool AddToGroup(uint32_t volume_id, std::string group_lun_pair);

        /**
         * Adds a volume to a target with a given LUN number. The target and the lun are encoded in
         * the format "<target>:<lun>".
         *
         * It is not possible to add a preconfigured volume to a target.
         *
         * @param volume_id
         * @param target_lun_pair
         * @return true iff ok, otherwise an error has occurred
         */
        bool AddToTarget(uint32_t volume_id, std::string target_lun_pair);

        /**
         * Removes a volume from a group.
         *
         * It is not possible to remove a preconfigured volume from a group.
         *
         * @param volume_id
         * @param group
         * @return true iff ok, otherwise an error has occurred
         */
        bool RemoveFromGroup(uint32_t volume_id, std::string group);

        /**
         * Removes a volume from a target.
         *
         * It is not possible to remove a preconfigured volume from a target.
         *
         * @param volume_id
         * @param target
         * @return true iff ok, otherwise an error has occurred
         */
        bool RemoveFromTarget(uint32_t volume_id, std::string target);

        /**
         * Changes the maintenance mode of a given volume
         * TODO (dmeister): Spelling error.
         *
         * @param volume_id
         * @param maintaince_mode
         * @return true iff ok, otherwise an error has occurred
         */
        bool ChangeMaintainceMode(uint32_t volume_id, bool maintaince_mode);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool ChangeOptions(uint32_t volume_id, const std::list<std::pair<std::string, std::string> >& options);

        /**
         * Changes the logical size mode of a given volume.
         * The volume must be in the maintenance mode.
         *
         * @param volume_id
         * @param new_logical_size
         * @return true iff ok, otherwise an error has occurred
         */
        bool ChangeLogicalSize(uint32_t volume_id, uint64_t new_logical_size);

        bool FastCopy(uint32_t src_volume_id, uint32_t target_volume_id, uint64_t source_offset,
                uint64_t target_offset, uint64_t size);

        /**
         * returns a list of all volumes
         * @return
         */
        dedupv1::base::Option<std::list<Dedupv1dVolume*> > GetVolumes(dedupv1::base::MutexLock** lock);

        /**
         * returns a pointer to the volume detacher.
         * @return
         */
        inline Dedupv1dVolumeDetacher* detacher();

        /**
         * returns a developer-readable representation of
         * a volume configuration.
         *
         * @param options
         * @return
         */
        static std::string DebugStringOptions(const std::list<std::pair<std::string, std::string> >& options);

        /**
         * returns a pointer to the base deduplication system.
         * @return
         */
        inline dedupv1::DedupSystem* base_dedup_system();

        inline Dedupv1dVolumeFastCopy* fast_copy() {
            return fast_copy_;
        }

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        std::string PrintTrace();
        std::string PrintProfile();
        std::string PrintStatistics();
        std::string PrintStatisticSummary();
        std::string PrintLockStatistics();

#ifdef DEDUPV1D_TEST
        void ClearData();
#endif
};

Dedupv1dVolumeDetacher* Dedupv1dVolumeInfo::detacher() {
    return detacher_;
}

dedupv1::DedupSystem* Dedupv1dVolumeInfo::base_dedup_system() {
    return base_dedup_system_;
}

}

#endif  // DEDUPV1D_VOLUME_INFO_H__
