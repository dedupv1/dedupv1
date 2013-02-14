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

#ifndef TARGET_MONITOR_H__
#define TARGET_MONITOR_H__

#include <core/dedup.h>

#include "default_monitor.h"
#include "dedupv1d.h"
#include "dedupv1d_target.h"

#include <list>
#include <string>


namespace dedupv1d {
namespace monitor {

/**
 * The target monitor reports informations about the currently
 * configured target to the user.
 *
 * \ingroup monitor
 */
class TargetMonitorAdapter : public MonitorAdapter {
    friend class TargetMonitorAdapterRequest;
    private:
    /**
     * Reference to the dedupv1d daemon
     */
    dedupv1d::Dedupv1d* ds_;
    public:
    /**
     * Constructor.
     *
     * @param ds
     * @return
     */
    explicit TargetMonitorAdapter(dedupv1d::Dedupv1d* ds);

    /**
     * Destructor.
     * @return
     */
    virtual ~TargetMonitorAdapter();

    /**
     * Creates a new volume monitor request.
     * @return
     */
    virtual MonitorAdapterRequest* OpenRequest();
};

/**
 * A target adapter request
 */
class TargetMonitorAdapterRequest : public MonitorAdapterRequest {
    private:
        /**
         * pointer to the parent monitor
         */
        TargetMonitorAdapter* adapter;

        /**
         * list of options pairs.
         */
        std::list< std::pair<std::string, std::string> > options;

        /**
         * key of the operation.
         */
        std::string operation;

        /**
         * writes informations about a target in a JSON
         * format.
         *
         * @param target
         * @return
         */
        std::string WriteTarget(const Dedupv1dTarget& target);
    public:
        /**
         * Constructor.
         * @param adapter
         * @return
         */
        explicit TargetMonitorAdapterRequest(TargetMonitorAdapter* adapter);

        /**
         * Destructor.
         * @return
         */
        virtual ~TargetMonitorAdapterRequest();

        virtual std::string Monitor();
        virtual bool ParseParam(const std::string& key, const std::string& value);
};

}
}

#endif /* TARGET_MONITOR_H_ */
