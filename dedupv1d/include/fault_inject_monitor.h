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

#ifndef FAULT_INJECT_MONITOR_H__
#define FAULT_INJECT_MONITOR_H__

#include <string>

#include <core/dedup.h>
#include "default_monitor.h"

namespace dedupv1d {
namespace monitor {

/**
 * The fault inject monitor is used to crash the system for QA purposes.
 * While the fault inject monitor should compile in all situations, the crash is only
 * allowed to occur if the FAULT_INJECTION macro is defined.
 *
 * \ingroup monitor
 */
class FaultInjectMonitorAdapter : public MonitorAdapter {
    public:
    /**
     * Constructor
     * @return
     */
    FaultInjectMonitorAdapter();

    /**
     * Creates a new group monitor request.
     * @return
     */
    virtual MonitorAdapterRequest* OpenRequest();
};


/**
 * A fault inject adapter request.
 *
 * \ingroup monitor
 */
class FaultInjectMonitorAdapterRequest : public MonitorAdapterRequest {
    private:
        /**
         * key of the operation.
         */
        std::string fault_id_;

        uint32_t hit_points_;

        /**
         * true iff an error occurred during the parsing of parameters
         */
        bool failed_;
    public:
        /**
         * Constructor.
         * @param adapter
         * @return
         */
        explicit FaultInjectMonitorAdapterRequest();

        /**
         * Destructor.
         * @return
         */
        virtual ~FaultInjectMonitorAdapterRequest();

        /**
         * injects the fault into the system and returns information
         * about the fault injection.
         * @return
         */
        virtual std::string Monitor();

        /**
         * parse the fault injection parameters.
         * @param key
         * @param value
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool ParseParam(const std::string& key, const std::string& value);
};

}
}

#endif  // FAULT_INJECT_MONITOR_H__
