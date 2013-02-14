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

#include <base/base.h>
#include <base/logging.h>
#include <base/strutil.h>

#include "stdio.h"
#include "pthread.h"
#include <stdarg.h>
#include <string>

using dedupv1::base::strutil::ToString;
using std::string;

LOGGER("Logging");

namespace dedupv1 {
namespace base {

LoggingStatistics LoggingStatistics::instance_;

LoggingStatistics::LoggingStatistics() {
    error_count_ = 0;
    warn_count_ = 0;
}

std::string LoggingStatistics::PrintStatistics() {
    return "{ \"error count\": " + ToString(error_count_) + ",\"warn count\": " + ToString(warn_count_) + "}";
}

LoggingStatistics& LoggingStatistics::GetInstance() {
    return instance_;
}

}
}

const char* file_basename(const char* file) {
    if (file == NULL) {
        return file;
    }
    const char* c =  strrchr(file, '/');
    if (c) {
        return c + 1;
    }
    return file;
}
