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

#ifndef DEDUP_VOLUME_INFO_H__
#define DEDUP_VOLUME_INFO_H__

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log.h>
#include <core/dedup_volume.h>
#include <string>
#include <list>

namespace dedupv1 {

class DedupSystem;

/**
* The DedupVolumeInfo is the volume or LUN information system of the
* core dedup system.
*
* It provides a list of a currently active volumes and provides means to
* report the permanent creation and deletion of volumes.
*
* This info instance does not store persistent information, the info instance
* has to be restored by an external system. In the context of the dedupv1d system,
* the Dedupv1dVolumeInfo restores the information.
*
* This class is designed to be used as singleton. It follows the rules of \ref life_cycle
* without own threads.
*/
class DedupVolumeInfo {
    private:
        DISALLOW_COPY_AND_ASSIGN(DedupVolumeInfo);

        std::list<DedupVolume*> volumes_;

        dedupv1::log::Log* log_;

        dedupv1::base::ReadWriteLock lock_;

        std::list<DedupVolume*> raw_configured_volume_;

        /**
        * Version of the FindVolume method that assumes that the lock is already
        * acquired.
        *
        * @param id
        * @return
        */
        DedupVolume* FindVolumeLocked(uint32_t id);
    public:
        DedupVolumeInfo();

        virtual ~DedupVolumeInfo();

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        bool Start(dedupv1::DedupSystem* system);

        /**
         *
         * Available options:
         * - id: String
         *
     * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
        * Registers a new volume.
        * After the registration the volume is allowed to make requests.
        *
        * The volume info does not take over the ownership of the volume.
        *
        * @param volume
     * @return true iff ok, otherwise an error has occurred
        */
        bool RegisterVolume(DedupVolume* volume);

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        bool UnregisterVolume(DedupVolume* volume);

        DedupVolume* FindVolume(uint32_t id);

        /**
        * Notifies about the newly, initial creation of an volume.
        * The volume is attached permanently. However, the call has to assure
        * that the id is unique.
        *
        * The volume should not be registered. The volume is registered
        * afterwards.
        *
        * @param volume
     * @return true iff ok, otherwise an error has occurred
        */
        bool AttachVolume(DedupVolume* volume);

        /**
        * Notifies about the permanent deletion of an volume.
        * However, the caller has to assure that the id existed.
        *
        * The volume should be registered. The volume is unregistered
        * afterwards.
        *
        * @param volume
     * @return true iff ok, otherwise an error has occurred
        */
        bool DetachVolume(DedupVolume* volume);

        uint32_t GetVolumeCount();

        bool IsStarted();

        virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        std::string PrintTrace();
        std::string PrintProfile();
        std::string PrintStatistics();
        std::string PrintLockStatistics();
};

}

#endif  // DEDUP_VOLUME_INFO_H__
