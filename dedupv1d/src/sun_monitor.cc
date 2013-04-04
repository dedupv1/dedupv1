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
#include <sstream>

#include <core/dedup_system.h>
#include <base/logging.h>
#include <core/idle_detector.h>
#include <base/strutil.h>

#include "dedupv1d.h"
#include "monitor.h"

#include <unicode/astro.h>
#include <unicode/datefmt.h>

#include "sun_monitor.h"

using std::string;
using std::stringstream;
using std::endl;
using dedupv1::DedupSystem;
using dedupv1::gc::GarbageCollector;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;

LOGGER("SunMonitorAdapter");

namespace dedupv1d {
namespace monitor {

namespace {
string FormatICUDate(DateFormat* date_format, UDate date) {
    UnicodeString unicode;
    date_format->format(date, unicode);
    string s;
    unicode.toUTF8String(s);
    return s;
}
}

SunMonitorAdapter::SunMonitorAdapter() {
}

string SunMonitorAdapter::Monitor() {
    icu::CalendarAstronomer ilsede_astronomer(10.2, 52.2666667);

    UDate current_date = ilsede_astronomer.getTime();
    UDate sunrise_date = ilsede_astronomer.getSunRiseSet(true);
    UDate sunset_date = ilsede_astronomer.getSunRiseSet(false);

    DateFormat* date_format = DateFormat::createInstance();

    bool sun_is_up =  (current_date > sunrise_date && current_date < sunset_date);

    stringstream sstr;
    sstr << "{";
    sstr << "\"current\": \"" << FormatICUDate(date_format, current_date) << "\",";
    sstr << "\"sunrise\": \"" << FormatICUDate(date_format, sunrise_date) << "\",";
    sstr << "\"sunset\": \"" << FormatICUDate(date_format, sunset_date) << "\",";
    sstr << "\"sun state\": \"" << ToString(sun_is_up) << "\"";
    sstr << "}";

    delete date_format;
    return sstr.str();
}

}
}
