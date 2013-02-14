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

#include <test/index_test_util.h>
#include <base/index.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/fixed_index.h>
#include <base/thread.h>
#include <base/runnable.h>

#include <string>
#include <vector>
#include <set>

using std::set;
using std::string;
using std::vector;
using dedupv1::base::strutil::Split;

LOGGER("IndexTestUtil");

namespace dedupv1 {
namespace testing {
dedupv1::base::Index* CreateIndex(const std::string& config_option) {
    vector<string> options;
    CHECK_RETURN(Split(config_option, ";", &options), NULL, "Failed to split: " << config_option);

    dedupv1::base::Index* index = dedupv1::base::Index::Factory().Create(options[0]);
    CHECK_RETURN(index, NULL, "Failed to create index type: " << options[0]);

    for (size_t i = 1; i < options.size(); i++) {
        if (options[i].empty()) {
            continue;
        }
        string option_name;
        string option;
        CHECK_RETURN(Split(options[i], "=", &option_name, &option), NULL, "Failed to split " << options[i]);
        CHECK_RETURN(index->SetOption(option_name, option), NULL, "Failed set option: " << options[i]);
    }
    return index;
}
}
}
