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

#ifndef DAEMON_UTIL_H__
#define DAEMON_UTIL_H__

#include <base/base.h>
#include <base/fileutil.h>

#include <string>

namespace dedupv1 {
namespace base {

#ifndef __APPLE__

/**
 * Makes a daemon from the current process.
 *
 * This code is based on the Wikipedia entry and
 * Advanced Programming in the UNIX Environment (Stevens, Rago)
 *
 * This function is not available on Mac
 *
 * @param daemon_name name of the daemon
 * @param daemon_user name of the user the daemon should run
 * @param daemon_group name of the group used to administer the daemon
 * @param lockfile name of the lock file
 * @param lock_fd outgoing parameter that holds the lock file
 *
 * @return true iff ok, otherwise an error has occurred
 * TODO (dmeister): Describe what the state is if this call failed
 */
bool daemonize(const std::string& daemon_name,
        const std::string& daemon_group,
        const std::string& daemon_user,
        const std::string& lockfile,
        dedupv1::base::File** lock_fd);

#endif

}
}



#endif  // DAEMON_UTIL_H__
