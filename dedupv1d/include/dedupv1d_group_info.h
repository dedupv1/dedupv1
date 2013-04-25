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

#ifndef DEDUPV1D_GROUP_INFO_H__
#define DEDUPV1D_GROUP_INFO_H__

#include <core/dedup.h>
#include <base/locks.h>
#include <base/option.h>
#include <base/index.h>

#include <string>
#include <list>
#include <map>

#include "dedupv1d_group.h"

namespace dedupv1d {

/**
 * Database of the groups managed by dedupv1d.
 *
 * The groups are SCST security groups.
 * A group has assigned volumes. If a initiator matches one of the
 * initiator pattern, the volumes assigned to the group are used as
 * volumes instead of the volumes assigned to the target (or the Default group)
 * the initiator connected to. This is the initiator-based LUN masking.
 *
 * The target related groups called Default_[TARGETNAME] are not managed
 * by the targets not by the group info. This is also the reason why groups
 * with the prefix Default_ are not allowed.
 */
class Dedupv1dGroupInfo {
    private:
        DISALLOW_COPY_AND_ASSIGN(Dedupv1dGroupInfo);

        /**
         * a list of all group names
         */
        std::list<std::string> group_list_;

        /**
         * a map from the group name to a group instance
         */
        std::map<std::string, dedupv1d::Dedupv1dGroup> group_map_;

        /**
         * Persistent index that stores the group information.
         * The index stores string group name as keys and GroupInfoData values.
         */
        dedupv1::base::PersistentIndex* info_;

        /**
         * flag that denotes if the group info is started.
         */
        bool started_;

        /**
         * Lock for the dedupv1d group info.
         */
        dedupv1::base::MutexLock lock_;

        /**
         * Used during the configuration phase. Should not be used after the start.
         */
        std::list< std::pair< std::string, std::string> > current_group_options_;

        /**
         * Used during the configuration phase. Should not be used after the start.
         */
        std::list< std::list< std::pair< std::string, std::string> > > group_options_;

        /**
         * Configures a new group based on the given group options.
         *
         * @param group_option
         * @param new_group
         * @return true iff ok, otherwise an error has occurred
         */
        bool ConfigureNewGroup(std::list< std::pair< std::string, std::string> > group_option,
                dedupv1d::Dedupv1dGroup* new_group);

        /**
         * Checks if the group given is valid
         * @param group
         * @return true iff ok, otherwise an error has occurred
         */
        bool CheckGroup(const dedupv1d::Dedupv1dGroup& group);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool RegisterGroup(const dedupv1d::Dedupv1dGroup& group);

        /**
         * Finds a group with the group lock held.
         * @param group_name
         * @return
         */
        dedupv1::base::Option<dedupv1d::Dedupv1dGroup> FindGroupLocked(std::string group_name);

        /**
         * returns a list of all group names.
         * The method assumes that the group info lock is held.
         *
         * @return
         */
        inline const std::list<std::string>& group_names() const;
    public:
        /**
         * Constructor for the group info
         * @return
         */
        Dedupv1dGroupInfo();

        virtual ~Dedupv1dGroupInfo();

        /**
         * Starts the group info.
         * @param start_context
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context);

        /**
         * Configures the group info.
         *
         * Available options:
         * - group: String
         * - group.*: String
         * - type: String
         *
         * @param option_name
         * @param option
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Adds a new group based on the group options.
         *
         * If successful, the group is added and stored persistently.
         * @param options
         * @return true iff ok, otherwise an error has occurred
         */
        bool AddGroup(std::list< std::pair< std::string, std::string> > options);

        /**
         * Adds a new initiator pattern to a group.
         *
         * @param group_name
         * @param initiator_pattern
         * @return true iff ok, otherwise an error has occurred
         */
        bool AddInitiatorPattern(std::string group_name, std::string initiator_pattern);

        /**
         * Removes a initiator pattern from a group.
         *
         * @param group_name
         * @param initiator_pattern
         * @return true iff ok, otherwise an error has occurred
         */
        bool RemoveInitiatorPattern(std::string group_name, std::string initiator_pattern);

        /**
         * Searches a group with the given name
         * @param group_name
         * @return
         */
        dedupv1::base::Option<dedupv1d::Dedupv1dGroup> FindGroup(std::string group_name);

        /**
         * Removes a group from the system.
         *
         * It is not possible to remove a preconfigured group.
         *
         * @param group_name
         * @return true iff ok, otherwise an error has occurred
         */
        bool RemoveGroup(std::string group_name);

        /**
         * returns a developer-readable representation of
         * a group configuration.
         *
         * @param options
         * @return
         */
        static std::string DebugStringOptions(const std::list< std::pair< std::string, std::string> >& options);

        /**
         * return true iff the group inof is started
         * @return
         */
        inline bool is_started() const;

        /**
         * Returns a copy of the current names of the groups.
         *
         * Be aware that the original list might be changed by other threads when this copy is processed.
         * The information in the list returned might be out of date.
         *
         * The method acquired and releases the groups info lock.
         */
        dedupv1::base::Option<std::list<std::string> > GetGroupNames();

#ifdef DEDUPV1D_TEST
        /**
         * Closes all threads and index data structures to allow a crash simulation during
         * testing
         */
        void ClearData();
#endif
};

const std::list<std::string>& Dedupv1dGroupInfo::group_names() const {
    return this->group_list_;
}

inline bool Dedupv1dGroupInfo::is_started() const {
    return this->started_;
}

}

#endif /* DEDUPV1D_GROUP_INFO_H_ */
