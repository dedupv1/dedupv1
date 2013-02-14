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

#include <vector>

#include <core/dedup_system.h>
#include <base/logging.h>
#include <base/fileutil.h>
#include <base/strutil.h>

#include "config_monitor.h"
#include "dedupv1d.h"
#include "monitor.h"
#include "default_monitor.h"

#include <json/json.h>

using std::string;
using std::vector;
using dedupv1::base::File;
using Json::Value;
using Json::StyledWriter;
using dedupv1::base::strutil::Split;

LOGGER("ConfigMonitorAdapter");

namespace dedupv1d {
namespace monitor {

ConfigMonitorAdapter::ConfigMonitorAdapter(dedupv1d::Dedupv1d* ds) {
    this->ds_ = ds;
}

string ConfigMonitorAdapter::Monitor() {
    CHECK_RETURN_JSON(ds_->config_data().size() > 0, "Config data not set");

    vector<string> config_array;
    Split(ds_->config_data(), "\n", &config_array);

    Value config_value(Json::arrayValue);
    for (vector<string>::iterator i = config_array.begin(); i != config_array.end(); i++) {
        config_value.append(*i);
    }

    Value root;
    root["config"] = config_value;
    Json::StyledWriter writer;
    return writer.write(root);
}

}
}
