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
#ifndef DEDUPV1_UUID_H_
#define DEDUPV1_UUID_H_

#include <base/base.h>
#include <base/option.h>

#include <string>
#include <apr-1/apr_uuid.h>

namespace dedupv1 {
namespace base {

/**
 * UUID
 *
 * Copy and assignment is ok
 */
class UUID {
    private:
        apr_uuid_t uuid_;

        static const UUID kNull;
    public:
        /**
         * Constructor. The uuid set here is the
         * NULL UUID.
         */
        UUID();
        UUID(const apr_uuid_t& uuid);

        UUID& operator=(const UUID& rhs);

        bool IsNull() const;

        /**
         * Parses a UUID froma  string
         */
        static dedupv1::base::Option<UUID> FromString(const std::string& s);

        void CopyFrom(const UUID& rhs);
        bool Equals(const UUID& rhs) const;

        std::string ToString() const;

        /**
         * Generate a new UUID
         */
        static UUID Generate();
};

}
}

#endif /* UUID_H_ */
