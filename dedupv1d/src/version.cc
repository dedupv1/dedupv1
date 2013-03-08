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

#include "version.h"
#include <tbb/spin_rw_mutex.h>
#include <apr-1/apr_version.h>
#include <apr-1/apu_version.h>
#include <tcutil.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/select.h>
#include <microhttpd.h>
#include <sqlite3.h>
#ifndef NO_SCST
#include <scst_user.h>
#endif
#include <tbb/tbb_stddef.h>
#include <string>
#include <sstream>
#include <list>
#include <map>

#include <base/logging.h>
#include <base/strutil.h>
#include <base/shell.h>

#include "dedupv1d.h"

using std::string;
using std::stringstream;
using std::endl;
using std::list;
using std::map;
using std::pair;

using dedupv1::base::strutil::ToString;
using dedupv1::base::RunUntilCompletion;
using dedupv1::base::Option;

LOGGER("Version");

namespace dedupv1d {

static string GetUnameOutput() {
    // here we use the high-level system because in-process buffering helps here
    Option<pair<int, bytestring> > result = dedupv1::base::RunUntilCompletion("uname -a");
    CHECK_RETURN(result.valid(), "", "Failed to run uname");

    const char* uname_output = reinterpret_cast<const char*>(result.value().second.data());
    return dedupv1::base::strutil::Trim(uname_output);
}

string ReportVersion() {
    list<string> version_list;
    map<string, string> version_map;

    version_list.push_back("dedupv1d");
    version_map["dedupv1d"] = DEDUPV1_VERSION_STR;
#ifndef NDEBUG
    version_map["dedupv1d"] += " (DEBUG)";
#endif
    version_list.push_back("dedupv1d-rev");
    version_map["dedupv1d-rev"] = DEDUPV1_REVISION_STR;
    version_list.push_back("dedupv1d-rev-date");
    version_map["dedupv1d-rev-date"] = DEDUPV1_REVISION_DATE_STR;
    version_list.push_back("tokyo-cabinet");
    version_map["tokyo-cabinet"] = tcversion;
#ifdef LOGGING_LOG4CXX
    version_list.push_back("log4cxx");
    version_map["log4cxx"] = "0.10.0 (patched)";
#endif
    version_list.push_back("apr");
    version_map["apr"] = apr_version_string();
    version_list.push_back("apr-util");
    version_map["apr-util"] = apu_version_string();
    version_list.push_back("protobuf");
    version_map["protobuf"] = "2.3.0";
    version_list.push_back("scst");
#ifdef NO_SCST
    version_map["scst"] = "<not installed>";
#else
    version_map["scst"] = DEV_USER_VERSION;
#endif
    version_list.push_back("microhttpd");
    version_map["microhttpd"] = "0.4.5";
    version_list.push_back("cryptopp");
    version_map["cryptopp"] = "5.6.0";
    version_list.push_back("sqlite");
    version_map["sqlite"] = sqlite3_libversion();
    version_list.push_back("tbb");
    version_map["tbb"] = ToString(TBB_VERSION_MAJOR) + "." +  ToString(TBB_VERSION_MINOR) + "." + ToString(TBB_INTERFACE_VERSION);
    version_list.push_back("linux");
    version_map["linux"] = GetUnameOutput();

    stringstream sstr;
    sstr << "{" << std::endl;

    list<string>::iterator i;
    for (i = version_list.begin(); i != version_list.end(); i++) {
        string component_name = *i;
        string component_version = version_map[component_name];
        if (i != version_list.begin()) {
            sstr << "," << endl;
        }
        sstr << "\"" << component_name << "\":\"" << component_version << "\"";

    }
    sstr << "}" << endl;
    return sstr.str();
}

}

size_t dedupv1d_report_version(char* c, size_t s) {
    string version = dedupv1d::ReportVersion();
    strncpy(c, version.c_str(), s);
    return strlen(c);
}
