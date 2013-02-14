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

#ifndef INSPECT_MONITOR_H__
#define INSPECT_MONITOR_H__

#include <core/dedup.h>
#include "default_monitor.h"
#include "dedupv1d.h"
#include "inspect.h"
#include <vector>
#include <string>

namespace dedupv1d {
namespace monitor {

/**
 * The inspect monitor allows to view details of the running system.
 * Similar to the dedupv1_debug utility application, but directly at runtime.
 *
 * \ingroup monitor
 */
class InspectMonitorAdapter : public MonitorAdapter {
    private:
    dedupv1d::Dedupv1d* ds_;

    public:
    /**
     * Constructor.
     * @param ds
     * @return
     */
    explicit InspectMonitorAdapter(dedupv1d::Dedupv1d* ds);

    /**
     * Destructor
     * @return
     */
    virtual ~InspectMonitorAdapter();

    /**
     * Opens a new request
     * @return
     */
    virtual MonitorAdapterRequest* OpenRequest();

    friend class InspectMonitorAdapterRequest;
};

/**
 * \ingroup monitor
 *
 * Request on the inspect monitor
 */
class InspectMonitorAdapterRequest : public MonitorAdapterRequest {
    private:
		/**
		 * Shared inspect class
		 */
        Inspect inspect_;

		/**
		 * Adapter of the request
		 */
        InspectMonitorAdapter* adapter_;

		/**
		 * Configured options for the inspect request
		 */
        std::vector< std::pair<std::string, std::string> > options_;
    public:
	
		/**
		 * Constructor
		 */
        explicit InspectMonitorAdapterRequest(InspectMonitorAdapter* adapter);

		/**
		 * Destructor
		 */
        virtual ~InspectMonitorAdapterRequest();

        virtual std::string Monitor();
        virtual bool ParseParam(const std::string& key, const std::string& value);
};

}
}

#endif  // INSPECT_MONITOR_H__
