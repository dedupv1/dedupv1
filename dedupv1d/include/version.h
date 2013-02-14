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

#ifndef VERSION_H__
#define VERSION_H__

#ifdef __cplusplus

#include <base/base.h>
#include <string>

namespace dedupv1d {
/**
 * returns version informations in a C++ way.
 * @return
 */
std::string ReportVersion();
}
#endif

#ifdef __cplusplus
extern "C" {
#endif

/**
 * returns version informations in a C way.
 * @param string
 * @param size
 * @return
 */
size_t dedupv1d_report_version(char* string, size_t size);

#ifdef __cplusplus
}
#endif


#endif  // VERSION_H__
