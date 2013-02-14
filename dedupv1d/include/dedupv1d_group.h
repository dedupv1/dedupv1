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

#ifndef DEDUPV1D_GROUP_H__
#define DEDUPV1D_GROUP_H__

#include <stdint.h>

#include <core/dedup.h>

#include "dedupv1d.pb.h"

#include <list>
#include <string>

namespace dedupv1d {

/**
 *
 * Represents a SCST group managed by dedupv1d.
 *
 * \ingroup dedupv1d
 */
class Dedupv1dGroup {
        // Copy and assignment is ok
    private:
        /**
         * flag that is true iff the group is preconfigured.
         * It is not possible to add or remove groups or add initiator pattern
         * to a preconfigured group.
         */
        bool preconfigured_;

        /**
         * name of the group
         */
        std::string name_;

        /**
         * list of initiator pattern assigned to the group
         */
        std::list<std::string> initiator_pattern_;

    public:
        /**
         * Constructor
         * @param preconfigured
         * @return
         */
        explicit Dedupv1dGroup(bool preconfigured);

        /**
         * Default constructor
         * @return
         */
        Dedupv1dGroup();

        /**
         * Destructor
         * @return
         */
        ~Dedupv1dGroup();

        /**
         * Configures a group.
         *
         * Available options:
         * - name: String
         * - initiator: String
         *
         * @param option_name
         * @param option
     * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Configures a group from the given group data
         * @param data
     * @return true iff ok, otherwise an error has occurred
         */
        bool ParseFrom(const GroupInfoData& data);

        /**
         * Serializes the group to the given group data.
         * @param data
     * @return true iff ok, otherwise an error has occurred
         */
        bool SerializeTo(GroupInfoData* data);

        /**
         * returns the name.
         *
         * @return
         */
        inline const std::string& name() const;

        /**
         * returns true iff the group is preconfigured
         * @return
         */
        inline bool is_preconfigured() const;

        /**
         * returns the list of initiator pattern.
         *
         * @return
         */
        inline const std::list<std::string>& initiator_pattern() const;

        /**
         * returns a developer readable version of the group.
         *
         * @return
         */
        std::string DebugString() const;

        /**
         * Adds a new initiator pattern.
         *
         * @param pattern
     * @return true iff ok, otherwise an error has occurred
         */
        bool AddInitiatorPattern(std::string pattern);

        /**
         * Removes a initiator pattern.
         *
         * @param pattern
     * @return true iff ok, otherwise an error has occurred
         */
        bool RemoveInitiatorPattern(std::string pattern);
};

const std::string& Dedupv1dGroup::name() const {
    return this->name_;
}

bool Dedupv1dGroup::is_preconfigured() const {
    return this->preconfigured_;
}

inline const std::list<std::string>& Dedupv1dGroup::initiator_pattern() const {
    return initiator_pattern_;
}

}

#endif // DEDUPV1D_GROUP_H__
