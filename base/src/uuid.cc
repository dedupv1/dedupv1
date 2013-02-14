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

#include <base/uuid.h>
#include <base/base.h>

using std::string;
using dedupv1::base::Option;
using dedupv1::base::make_option;

namespace dedupv1 {
namespace base {

UUID::UUID() {
    memset(&uuid_, 0, sizeof(uuid_));
}

UUID::UUID(const apr_uuid_t& uuid) {
    memcpy(&uuid_, &uuid, sizeof(uuid_));
}

bool UUID::IsNull() const {
    UUID zero;
    return Equals(zero);
}

UUID & UUID::operator=(const UUID& rhs) {
    memcpy(&uuid_, &rhs.uuid_, sizeof(uuid_));
    return *this;
}

Option<UUID> UUID::FromString(const std::string& s) {
    apr_uuid_t uu;
    apr_status_t r = apr_uuid_parse(&uu, s.c_str());
    if (r != 0) {
        return false;
    }
    return make_option(UUID(uu));
}

void UUID::CopyFrom(const UUID& rhs) {
    *this = rhs;
}

bool UUID::Equals(const UUID& rhs) const {
    return memcmp(&uuid_, &rhs.uuid_, sizeof(uuid_)) == 0;
}

std::string UUID::ToString() const {
    char s[36];
    apr_uuid_format(s, &uuid_);
    return s;
}

UUID UUID::Generate() {
    apr_uuid_t u;
    apr_uuid_get(&u);
    return u;
}

}
}
