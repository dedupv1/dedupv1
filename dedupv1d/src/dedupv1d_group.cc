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

#include "dedupv1d_group.h"

#include <base/strutil.h>
#include <base/logging.h>
#include <re2/re2.h>

using std::string;
using std::list;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::To;

LOGGER("Dedupv1dGroup");

namespace dedupv1d {

Dedupv1dGroup::Dedupv1dGroup() {
    name_ = "";
    preconfigured_ = false;
}

Dedupv1dGroup::Dedupv1dGroup(bool preconfigured) {
    name_ = "";
    preconfigured_ = preconfigured;
}

Dedupv1dGroup::~Dedupv1dGroup() {
}

bool Dedupv1dGroup::SerializeTo(GroupInfoData* data) {
    DCHECK(data, "Data not set");
    data->set_group_name(name());

    for (list<string>::iterator i = initiator_pattern_.begin(); i != initiator_pattern_.end(); i++) {
        data->add_initiator_pattern(*i);
    }

    return true;
}

bool Dedupv1dGroup::ParseFrom(const GroupInfoData& info) {
    // We redirect everything over the config system as some of these settings are delegated to other classes
    // and we have to duplicate the logic this way.

    DCHECK(info.has_group_name(), "Invalid group data: " << info.ShortDebugString());
    CHECK(this->SetOption("name", info.group_name()),
            "Failed to set name");
    for (int i = 0; i < info.initiator_pattern_size(); i++) {
        CHECK(this->SetOption("initiator", info.initiator_pattern(i)), "Failed to add initiator pattern");
    }
    return true;
}

bool Dedupv1dGroup::SetOption(const string& option_name, const string& option) {
    if (option_name == "name") {
        CHECK(option.size() != 0, "Illegal group name (empty)");
        CHECK(option.size() <= 512, "Illegal group name (too long): " << option); // (tested max: 1014)
        CHECK(RE2::FullMatch(option, "^[a-zA-Z0-9\\.\\-:_]+$"), "Illegal group name: " << option);
        this->name_ = option;
        return true;
    }
    if (option_name == "initiator") {
        CHECK(option.size() <= 223, "Illegal initiator pattern (too long)"); // see http://tools.ietf.org/html/rfc3720#section-3.2.6.1
        CHECK(AddInitiatorPattern(option), "Failed to add initiator pattern");
        return true;
    }
    ERROR("Illegal option: " << option_name << "=" << option);
    return false;
}

string Dedupv1dGroup::DebugString() const {
    return "[name " + name() + "]";
}

bool Dedupv1dGroup::AddInitiatorPattern(std::string pattern) {
    for( list<string>::const_iterator i = initiator_pattern_.begin();
            i != initiator_pattern_.end();
            i++) {
        CHECK(*i != pattern, "Pattern already added");
    }
    this->initiator_pattern_.push_back(pattern);
    return true;
}

bool Dedupv1dGroup::RemoveInitiatorPattern(std::string pattern) {
    for (list<string>::iterator i = initiator_pattern_.begin(); i != initiator_pattern_.end(); i++) {
        if (*i == pattern) {
            initiator_pattern_.erase(i);
            return true;
        }
    }
    ERROR("Failed to find pattern: " << pattern);
    return false;
}

}


