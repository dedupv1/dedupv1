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

#ifndef DEFAULT_MONITOR_H__
#define DEFAULT_MONITOR_H__

#include <core/dedup.h>
#include "monitor.h"

namespace dedupv1d {
namespace monitor {

class DefaultMonitorAdapterRequest;

/**
 * The default monitor is used to make the
 * development of monitor adapters much easier if
 * there is no need for a monitor to distinguish between
 * different monitor request. This is usually the case
 * when the monitor itself doesn't do any "active" work.
 *
 * The default monitor moves the method of the
 * monitor request to the adapter class.
 *
 * \ingroup monitor
 */
class DefaultMonitorAdapter : public MonitorAdapter {
    protected:
        /**
         * Abstract Monitor method
         */
        virtual std::string Monitor() = 0;

        /**
         * Default implementation to parse parameters.
         *
         * @param key
         * @param value
     * @return true iff ok, otherwise an error has occurred
         */
        virtual bool ParseParam(const std::string& key, const std::string& value);
    public:
        /**
         * Constructor
         * @return
         */
        DefaultMonitorAdapter();

        /**
         * Destructor.
         * @return
         */
        virtual ~DefaultMonitorAdapter();

        /**
         * Creates a new request that delegates the call back to the adapter. Should not
         * be virtual
         */
        MonitorAdapterRequest* OpenRequest();

        friend class DefaultMonitorAdapterRequest;
};

/**
 * Request for the default monitor request.
 * Delegates the monitor calls back to the adapter.
 * Should not be overwritten.
 *
 * \ingroup monitor
 */
class DefaultMonitorAdapterRequest : public MonitorAdapterRequest {
    private:
        /**
         * Pointer to the parent adapter
         */
        DefaultMonitorAdapter* parentAdapter;
    public:
        /**
         * Constructor.
         * @param adapter
         * @return
         */
        explicit DefaultMonitorAdapterRequest(DefaultMonitorAdapter* adapter);

        /**
         * Destructor.
         * @return
         */
        virtual ~DefaultMonitorAdapterRequest();

        /**
         * delegates the Monitor call back to the parent adapter.
         * @return
         */
        virtual std::string Monitor();

        /**
         * delegates the ParseParam call back to the parent adapter.
         * @param key
         * @param value
         * @return
         */
        virtual bool ParseParam(const std::string& key, const std::string& value);
};

}
}

#endif  // DEFAULT_MONITOR_H__
