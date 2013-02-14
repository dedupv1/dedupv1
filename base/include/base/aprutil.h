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
 * @file aprutil.h
 * @brief APR-UTIL Utility functions
 *
 * The apr life-cycle functions apr_initialize and
 * apr_terminiate are automatically called using a static utility
 * object.
 */

#ifndef APRUTIL_H__
#define APRUTIL_H__

#include <string>

#include <apr-1/apr_errno.h>

namespace dedupv1 {
namespace base {
namespace apr {

/**
 * returns a human-readable string that describes an apr_status value.
 *
 * @param state state flag for that the error string should be returned
 * @return error message for the apr state
 */
std::string apr_error(apr_status_t state);

}
}
}

#endif  // APRUTIL_H__
