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
#include "version_monitor.h"

#include <core/dedup_system.h>
#include <base/logging.h>

#include "version.h"
#include "monitor.h"
#include "default_monitor.h"

using std::string;

LOGGER("VersionMonitorAdapter");

namespace dedupv1d {
namespace monitor {

string VersionMonitorAdapter::Monitor() {
    char buf[4096];
    int i = dedupv1d_report_version(buf, 4096);
    string s;
    s.append(buf, i);
    return s;
}

}
}
