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

#include <core/session_management.h>
#include <base/logging.h>

#include <set>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>

#include <base/strutil.h>
#include <base/timer.h>
#include <core/content_storage.h>
#include <core/session.h>
#include <core/dedup_volume.h>

LOGGER("SessionManagement");

using std::set;
using std::string;

namespace dedupv1 {

SessionResourceType::SessionResourceType(DedupVolume* volume) {
    this->volume_ = volume;
}

SessionResourceType::~SessionResourceType() {
}

Session* SessionResourceType::Create() {
    DCHECK_RETURN(this->volume_, NULL, "Volume not set");
    Session* sess = new Session();
    if (!sess->Init(volume_)) {
      ERROR("Failed to init session");
      delete sess;
      return NULL;
    }
    return sess;
}

void SessionResourceType::Reinit(Session* sess) {
    if (!sess->Clear()) {
        WARNING("Failed to clear content storage session");
    }
}

void SessionResourceType::Close(Session* sess) {
    if (sess) {
        delete sess;
        sess = NULL;
    }
}

}
