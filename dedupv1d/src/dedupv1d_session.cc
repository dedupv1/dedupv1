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

#include "dedupv1d_session.h"

#include <base/strutil.h>

#include <sstream>

using dedupv1::base::strutil::ToString;
using std::string;
using std::stringstream;

namespace dedupv1d {

Dedupv1dSession::Dedupv1dSession(uint64_t session_id, const string& target_name,  const std::string& initiator_name, uint64_t lun) {
    this->session_id_ = session_id;
    this->target_name_ = target_name;
    this->initiator_name_ = initiator_name;
    this->lun_ = lun;
}

Dedupv1dSession::Dedupv1dSession() {
    session_id_ = 0;
    lun_ = 0;
}

std::string Dedupv1dSession::DebugString() const {
    stringstream sstr;
    sstr << "[Session: session id " << this->session_id() <<
    ", target name " << this->target_name() <<
    ", initiator name " << this->initiator_name() <<
    ", lun " << this->lun() << "]";
    return sstr.str();
}

}

