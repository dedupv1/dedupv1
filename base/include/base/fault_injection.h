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
/**
 * @file fault_injection.h
 * Methods and defines for the fault injection framework.
 * The fault injection is used to simulate crash errors in specifc
 * places in the source code.
 */
#ifndef FAULT_INJECTION_H__
#define FAULT_INJECTION_H__

#include <base/base.h>
#include <tbb/concurrent_hash_map.h>

#ifdef FAULT_INJECTION

namespace dedupv1 {
namespace base {

/**
 * The fault injection framework is used during the QA to force the
 * system to crash in specific situations. While random "kill -9" provide
 * some coverage, it is nearly impossible to test certain problematic areas of the
 * code base.
 *
 * A user can declare a a fault point using the FAULT_POINT macro using a unique id. A
 * fault point can be activated using the ActivateFaultPoint method. The next time a
 * thread of execution passes the fault point, the daemon process crashes.
 *
 * The fault injection framework MUST NOT be used in release software. The framework
 * is only available when the system is compiled with the FAULT_INJECTION definition
 * using "scons <xy> --fault-inject".
 */
class FaultInjection {
    private:
        /**
         * Map of all activated injection points
         */
        static tbb::concurrent_hash_map<std::string, uint32_t> injection_data;
    public:
        /**
         * Checks if the fault point with the given id should fail
         * @return true iff ok, otherwise an error has occurred
         */
        static bool ShouldFail(const std::string& id);

        /**
         * Called to activate (and deactivate) a fault point
         */
        static void ActivateFaultPoint(const std::string& id, uint32_t hits_until_abort = 1);
};

}
}

#define FAULT_POINT(id) if (1) { if (dedupv1::base::FaultInjection::ShouldFail(id)) { abort(); } };

#else
// normal debug and release builds

/**
 * FAULT_POINT is a no-op i normal debug and release builds.
 */
#define FAULT_POINT(id)

#endif

#endif  // FAULT_INJECTION_H__
