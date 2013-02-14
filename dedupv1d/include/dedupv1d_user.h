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

#ifndef DEDUPV1D_USER_H__
#define DEDUPV1D_USER_H__

#include <stdint.h>

#include <core/dedup.h>

#include "dedupv1d.pb.h"

#include <string>
#include <list>

namespace dedupv1d {

/**
 *
 * Represents a SCST user managed by dedupv1d.
 *
 * \ingroup dedupv1d
 */
class Dedupv1dUser {
        // Copy and assignment is ok
    private:
        /**
         * true iff the user is pre configured
         */
        bool preconfigured_;

        /**
         * name of the user
         */
        std::string name_;

        /**
         * user password (encoded)
         * It is not real a hash, but a base64 string of a rot13 encoded password.
         * As we have to submit the password in clear text to iscsi-scst-adm
         */
        std::string secret_hash_;

        /**
         * targets to which the user is assigned
         */
        std::list<std::string> targets_;

    public:
        /**
         * Constructor
         * @param preconfigured
         * @return
         */
        explicit Dedupv1dUser(bool preconfigured);

        /**
         * Default constructor
         * @return
         */
        Dedupv1dUser();

        /**
         * Destructor
         * @return
         */
        ~Dedupv1dUser();

        /**
         *
         * Available options:
         * - name: String
         * - secret-hash: String
         * - target: String
         *
         * Configures the user
         * @param option_name
         * @param option
     * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Parses the user from the user info data message.
         *
         * @param data
     * @return true iff ok, otherwise an error has occurred
         */
        bool ParseFrom(const UserInfoData& data);

        /**
         * Serializes the user to a user info data message.
         *
         * @param data
     * @return true iff ok, otherwise an error has occurred
         */
        bool SerializeTo(UserInfoData* data);

        /**
         * returns the name of the user
         */
        inline const std::string& name() const;

        /**
         * returns the encoded user password
         */
        inline const std::string& secret_hash() const;

        /**
         * returns a list of the names of the targets the user
         * is assigned too
         */
        inline const std::list<std::string>& targets() const;

        /**
         * returns true iff the user is preconfigured.
         *
         */
        inline bool is_preconfigured() const;

        /**
         * returns a developer-readable version of the user
         */
        std::string DebugString() const;

        /**
         * Adds a new targets to the user
         * @param target_name
         * @return
         */
        bool AddTarget(std::string target_name);

        /**
         * Removes a target from the user
         * @param target_name
         * @return
         */
        bool RemoveTarget(std::string target_name);

        /**
         * Encode a clear text password using the current version
         * of the dedupv1d user password encoding scheme.
         *
         * Currently there is only a single encoding scheme.
         * @return a base64 encoded version of the encoding user password
         */
        static std::string EncodePassword(const std::string& clear_text);
};

const std::string& Dedupv1dUser::name() const {
    return this->name_;
}

const std::string& Dedupv1dUser::secret_hash() const {
    return this->secret_hash_;
}

const std::list<std::string>& Dedupv1dUser::targets() const {
    return this->targets_;
}

bool Dedupv1dUser::is_preconfigured() const {
    return this->preconfigured_;
}

}

#endif /* DEDUPV1D_USER_H_ */
