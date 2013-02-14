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

#include <base/fault_injection.h>
#include <base/logging.h>

#ifdef FAULT_INJECTION

LOGGER("FaultInjection");

namespace dedupv1 {
namespace base {

tbb::concurrent_hash_map<std::string, uint32_t> FaultInjection::injection_data;

bool FaultInjection::ShouldFail(const std::string& id) {
    tbb::concurrent_hash_map<std::string, uint32_t>::accessor acc;
    TRACE("Test fault point: id " << id);
    bool r = false;
    if (injection_data.find(acc, id)) {
        r = acc->second == 0;
        if (r) {
            WARNING("Hit fault point: id " << id);
            injection_data.erase(acc);
        } else {
            acc->second--;
        }

    }
    return r;
}

void FaultInjection::ActivateFaultPoint(const std::string& id, uint32_t hits_until_abort) {
    WARNING("Activate fault point: id " << id);
    injection_data.insert(std::make_pair(id, hits_until_abort));
}

}

}

#endif
