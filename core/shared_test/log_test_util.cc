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

#include <test/log_test_util.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <core/log.h>

using std::string;
using std::vector;
using dedupv1::base::PersistentIndex;
using dedupv1::base::Index;
using dedupv1::base::strutil::Split;
using dedupv1::base::strutil::ToString;
using dedupv1::log::Log;

LOGGER("LogTestUtil");

namespace dedupv1 {
namespace testutil {

PersistentIndex* OpenLogIndex(const string& config_option) {
    vector<string> options;
    CHECK_RETURN(Split(config_option, ";", &options), NULL, "Failed to split: " << config_option);

    dedupv1::base::PersistentIndex* index = NULL;

    for (size_t i = 0; i < options.size(); i++) {
        string option_name;
        string option;
        CHECK_RETURN(Split(options[i], "=", &option_name, &option), NULL, "Failed to split " << options[i]);
        if (option_name == "type") {
            CHECK_RETURN(index == NULL, NULL, "Index already created");
            Index* i = Index::Factory().Create(option);
            CHECK_RETURN(i, NULL, "Failed to create index");
            index = i->AsPersistentIndex();
            CHECK_RETURN(index, NULL, "Index is not persistent");
            CHECK_RETURN(index->SetOption("width", ToString(Log::kDefaultLogEntryWidth)), NULL, "Failed to set width");
        }
        if (option_name == "max-log-size") {
            if (index == NULL) {
                Index* i = Index::Factory().Create(Log::kDefaultLogIndexType);
                CHECK_RETURN(i, NULL, "Failed to create index");
                index = i->AsPersistentIndex();
                CHECK_RETURN(index, NULL, "Index is not persistent");
                CHECK_RETURN(index->SetOption("width", ToString(Log::kDefaultLogEntryWidth)), NULL, "Failed to set width");
            }
            CHECK_RETURN(index->SetOption("size", option), NULL, "Failed set option: " << options[i]);
        }
        if (option_name == "filename") {
            if (index == NULL) {
                Index* i = Index::Factory().Create(Log::kDefaultLogIndexType);
                CHECK_RETURN(i, NULL, "Failed to create index");
                index = i->AsPersistentIndex();
                CHECK_RETURN(index, NULL, "Index is not persistent");
                CHECK_RETURN(index->SetOption("width", ToString(Log::kDefaultLogEntryWidth)), NULL, "Failed to set width");
            }
            CHECK_RETURN(index->SetOption(option_name, option), NULL, "Failed set option: " << options[i]);
        }
    }
    return index;
}

}
}
