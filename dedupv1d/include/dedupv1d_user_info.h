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
#ifndef DEDUPV1D_USER_INFO_H__
#define DEDUPV1D_USER_INFO_H__

#include <core/dedup.h>
#include <base/locks.h>
#include <base/option.h>
#include <base/index.h>

#include <string>
#include <list>
#include <map>

#include "dedupv1d_user.h"
#include "dedupv1d_target_info.h"

namespace dedupv1d {

/**
 * Database of the iSCSI users managed by dedupv1d
 */
class Dedupv1dUserInfo {
    private:
        DISALLOW_COPY_AND_ASSIGN(Dedupv1dUserInfo);

        /**
         * a list of all user names
         */
        std::list<std::string> user_list_;

        /**
         * a map from the user name to a user instance
         */
        std::map<std::string, dedupv1d::Dedupv1dUser> user_map_;

        /**
         * a map from a target name to all user names
         */
        std::multimap<std::string, std::string> target_user_map_;

        /**
         * Persistent index that stores the user information.
         * The index stores string user name as keys and UserInfoData values.
         */
        dedupv1::base::PersistentIndex* info_;

        /**
         * flag that denotes if the user info is started.
         */
        bool started_;

        /**
         * Lock for the dedupv1d user info.
         */
        dedupv1::base::MutexLock lock_;

        /**
         * Used during the configuration phase. Should not be used after the start.
         */
        std::list< std::pair< std::string, std::string> > current_user_options_;

        /**
         * Used during the configuration phase. Should not be used after the start.
         */
        std::list< std::list< std::pair< std::string, std::string> > > user_options_;


        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool ConfigureNewUser(std::list< std::pair< std::string, std::string> > user_option,
                dedupv1d::Dedupv1dUser* new_user);

        /**
         * Checks if the user name is valid and that the user doesn't exists.
         *
         * The method assumes that the suer info lock is held.
         *
         * @returns false iff the user is not valid
         */
        bool CheckUser(const dedupv1d::Dedupv1dUser& user);

        /**
         * Adds the to the user info data structures (user map, user list).
         *
         * The methods assumes that the user info lock is held.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool RegisterUser(const dedupv1d::Dedupv1dUser& user);

        /**
         * The method assumes that the user info lock is held.
         *
         * @return
         */
        dedupv1::base::Option<dedupv1d::Dedupv1dUser> FindUserLocked(std::string user_name);

        /**
         * returns a list of all user names
         *
         * The method assumes that the user info lock is held.
         *
         * @return
         */
        inline const std::list<std::string>& user_names() const;
    public:
        /**
         * Constructor for the user info
         * @return
         */
        Dedupv1dUserInfo();

        virtual ~Dedupv1dUserInfo();

        /**
         * Starts the user info.
         *
         * @param start_context
         * @param target_info
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context);

        /**
         * Configures the user info.
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
         * @return true iff ok, otherwise an error has occurred
         */
        bool AddUser(std::list< std::pair< std::string, std::string> > options);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool ChangeUser(std::list< std::pair< std::string, std::string> > options);

        /**
         * @param user_name
         * @return
         */
        dedupv1::base::Option<dedupv1d::Dedupv1dUser> FindUser(std::string user_name);

        /**
         * Adds the given user to the given target.
         *
         * @user_name The user that should be added to the target. It is checked that the user exists, if
         * the user doesn't exists it is an error
         * @target_name Name of the target the user is added to. It is assumed that the target exists.
         *
         * Acquired the user info lock.
         * @return true iff ok, otherwise an error has occurred
         */
        bool AddUserToTarget(std::string user_name, std::string target_name);

        /**
         * Removes a user from the given target
         * @user_name Name of the user that should be removed from the target. It is checked if the user
         * exists and is assigned to the target. If that is not the case, it is an error.
         *
         * Acquires the user info lock
         * @return true iff ok, otherwise an error has occurred.
         */
        bool RemoveUserFromTarget(std::string user_name, std::string target_name);

        /**
         * Removes a user from the system.
         *
         * It is not possible to remove a preconfigured user.
         *
         * @param user_name
         * @return true iff ok, otherwise an error has occurred
         */
        bool RemoveUser(std::string user_name);

        /**
         * Returns the users assigned to the given target.
         *
         * Be aware that the original list might be changed by other threads when this copy is processed.
         * The information in the list returned might be out of date.
         */
        dedupv1::base::Option<std::list<std::string> > GetUsersInTarget(const std::string& target_name);

        /**
         * Returns a developer-readable representation of
         * a volume configuration.
         *
         * @param options
         * @return
         */
        static std::string DebugStringOptions(const std::list< std::pair< std::string, std::string> >& options);


        /**
         * Returns a copy of the current names of the users.
         *
         * Be aware that the original list might be changed by other threads when this copy is processed.
         * The information in the list returned might be out of date.
         *
         * The method acquired and releases the user info lock.
         */
        dedupv1::base::Option<std::list<std::string> > GetUserNames();

        /**
         * returns true iff the user info has been started
         */
        inline bool is_started() const;

#ifdef DEDUPV1D_TEST
        void ClearData();
#endif
};

const std::list<std::string>& Dedupv1dUserInfo::user_names() const {
    return this->user_list_;
}

inline bool Dedupv1dUserInfo::is_started() const {
    return this->started_;
}

}

#endif /* DEDUPV1D_USER_INFO_H_ */
