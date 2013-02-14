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

#ifndef USER_MONITOR_H__
#define USER_MONITOR_H__

#include <core/dedup.h>

#include "default_monitor.h"
#include "dedupv1d.h"
#include "dedupv1d_user.h"

#include <list>
#include <string>


namespace dedupv1d {
namespace monitor {

/**
 * The user monitor reports informations about the currently
 * configured user to the user.
 *
 * \ingroup monitor
 */
class UserMonitorAdapter : public MonitorAdapter {
    friend class UserMonitorAdapterRequest;
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
    explicit UserMonitorAdapter(dedupv1d::Dedupv1d* ds);

    /**
     * Destructor.
     * @return
     */
    virtual ~UserMonitorAdapter();

    /**
     * Creates a new volume monitor request.
     * @return
     */
    virtual MonitorAdapterRequest* OpenRequest();
};

/**
 * A user adapter request.
 *
 * \ingroup monitor
 */
class UserMonitorAdapterRequest : public MonitorAdapterRequest {
    private:
        /**
         * pointer to the parent monitor
         */
        UserMonitorAdapter* adapter_;

        /**
         * list of options pairs.
         */
        std::list< std::pair<std::string, std::string> > options_;

        /**
         * key of the operation.
         */
        std::string operation_;

        /**
         * writes informations about a user in a JSON
         * format.
         *
         * @param user
         * @return
         */
        std::string WriteUser(const Dedupv1dUser& user);
    public:
        /**
         * Constructor.
         * @param adapter
         * @return
         */
        explicit UserMonitorAdapterRequest(UserMonitorAdapter* adapter);

        /**
         * Destructor.
         * @return
         */
        virtual ~UserMonitorAdapterRequest();

        virtual std::string Monitor();
        virtual bool ParseParam(const std::string& key, const std::string& value);
};

}
}

#endif /* USER_MONITOR_H_ */
