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

#ifndef DEDUPV1D_TARGET_INFO_H__
#define DEDUPV1D_TARGET_INFO_H__

#include <core/dedup.h>
#include <base/locks.h>
#include <base/option.h>
#include <base/index.h>

#include <string>
#include <list>
#include <map>

#include "dedupv1d_target.h"

namespace dedupv1d {

class Dedupv1dVolumeInfo;
class Dedupv1dUserInfo;

/**
 * Database of the targets managed by dedupv1d
 */
class Dedupv1dTargetInfo {
    private:
        DISALLOW_COPY_AND_ASSIGN(Dedupv1dTargetInfo);

        /**
         * a list of all targets
         * Should only be accessed if the target info lock is held.
         */
        std::list<dedupv1d::Dedupv1dTarget> target_list_;

        /**
         * Should only be accessed if the target info lock is held.
         */
        std::map<uint32_t, std::list<dedupv1d::Dedupv1dTarget>::iterator> target_id_map_;

        /**
         * a map from the target name to the target
         * Should only be accessed if the target info lock is held.
         */
        std::map<std::string, std::list<dedupv1d::Dedupv1dTarget>::iterator> target_map_;

        /**
         * Persistent index that stores the target information.
         * The index stores string target name as keys and TargetInfoData values.
         */
        dedupv1::base::PersistentIndex* info_;

        /**
         * flag that denotes if the target info is started.
         */
        bool started_;

        /**
         * Lock for the dedupv1d target info.
         */
        dedupv1::base::MutexLock lock_;

        /**
         * Used during the configuration phase. Should not be used after the start.
         */
        std::list< std::pair< std::string, std::string> > current_target_options_;

        /**
         * Used during the configuration phase. Should not be used after the start.
         */
        std::list< std::list< std::pair< std::string, std::string> > > target_options_;

        /**
         * Pointer to the volume info object
         */
        Dedupv1dVolumeInfo* volume_info_;

        /**
         * The target info may call the user info while the target info lock is held. This introduces a lock ordering
         * constraint: Target info method should never by called while the user info lock is held.
         */
        Dedupv1dUserInfo* user_info_;

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool ConfigureNewTarget(std::list< std::pair< std::string, std::string> > target_option,
                dedupv1d::Dedupv1dTarget* new_target);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool CheckTarget(const dedupv1d::Dedupv1dTarget& target);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool RegisterTarget(const dedupv1d::Dedupv1dTarget& target);

        /**
         * Finds a target with a given name.
         *
         * The method assumes that the target info lock is held
         *
         * @return false iff the target is not existing. If a target with the given name exists, the target is returned
         */
        dedupv1::base::Option<Dedupv1dTarget> FindTargetByNameLocked(const std::string& target_name);

        /**
         * Returns a list of all targets.
         *
         * The method assumes that the target info lock is held
         *
         * @return
         */
        inline const std::list<dedupv1d::Dedupv1dTarget>& targets() const;

    public:
        /**
         * Constructor for the target info
         * @return
         */
        Dedupv1dTargetInfo();

        virtual ~Dedupv1dTargetInfo();

        /**
         * Starts the target info.
         *
         * @param start_context
         * @param volume_info Reference to the volume info. The volume info is used
         * during the renaming of targets. The volume info may be be started at this point in time.
         * @param user_info Reference to the user info. The user info is used
         * during the renaming of targets. The user info may be be started at this point in time.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context,
                Dedupv1dVolumeInfo* volume_info,
                Dedupv1dUserInfo* user_info);

        /**
         * Configures the target info.
         *
         * Available options:
         * - target: String
         * - target.*: String
         * - type: String
         *
         * @param option_name
         * @param option
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         *
         * @param options
         * @return
         */
        bool AddTarget(std::list< std::pair< std::string, std::string> > options);

        /**
         * Find a target by the name of the target.
         *
         * @param target_name
         * @return
         */
        dedupv1::base::Option<dedupv1d::Dedupv1dTarget> FindTargetByName(const std::string& target_name);

        /**
         * Find a target by target id.
         * @param tid
         * @return
         */
        dedupv1::base::Option<dedupv1d::Dedupv1dTarget> FindTarget(uint32_t tid);

        /**
         * Removes a target from the system.
         *
         * It is not possible to remove a preconfigured target.
         *
         * @param tid
         * @return true iff ok, otherwise an error has occurred
         */
        bool RemoveTarget(uint32_t tid);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool ChangeTargetParams(uint32_t tid, const std::list< std::pair< std::string, std::string> >& param_options);

        /**
         * Returns a copy of a list with all targets
         *
         * Be aware that the targets might have already changed when the list is processed.
         *
         * @return
         */
        dedupv1::base::Option<std::list<dedupv1d::Dedupv1dTarget> > GetTargets();

        /**
         * Returns a developer-readable representation of
         * a volume configuration.
         *
         * @param options
         * @return
         */
        static std::string DebugStringOptions(const std::list< std::pair< std::string, std::string> >& options);

        /**
         * Returns true iff the target info is started.
         * @return
         */
        inline bool is_started() const;

#ifdef DEDUPV1D_TEST
        void ClearData();
#endif
};

const std::list<dedupv1d::Dedupv1dTarget>& Dedupv1dTargetInfo::targets() const {
    return this->target_list_;
}

inline bool Dedupv1dTargetInfo::is_started() const {
    return this->started_;
}

}

#endif /* DEDUPV1D_TARGET_INFO_H_ */
