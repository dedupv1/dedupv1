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
#include <base/aprutil.h>

#include <apr-1/apr.h>
#include <apr-1/apu.h>
#include <apr-1/apr_pools.h>
#include <apr-1/apr_errno.h>

namespace dedupv1 {
namespace base {
namespace apr {

/**
 * Internal utility class that is used for automatically
 * initialize and shutdown the apr and apr_util libraries
 */
class AprInit {
public:
    /**
     * Constructor
     * @return
     */
    AprInit() {
        apr_initialize();
    }

    ~AprInit() {
        apr_terminate();
    }
};

// the static object ensures that apr_initilize and apr_terminate are called
// before the main method.
// However, the aware that there is no specific ordering of the static initiation. You
// cannot rely on an inited apr_init_ object in that phase.
static AprInit apr_init_;

std::string apr_error(apr_status_t state) {
    char buf[1024];
    return apr_strerror(state, buf, 1024);
}

}
}
}
