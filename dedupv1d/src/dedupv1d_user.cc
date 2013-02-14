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

#include "dedupv1d_user.h"

#include <base/strutil.h>
#include <base/logging.h>
#include <base/rot13.h>
#include <base/base64.h>
#include <re2/re2.h>

using std::string;
using std::list;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::To;
using dedupv1::base::ToRot13;
using dedupv1::base::FromRot13;
using dedupv1::base::ToBase64;
using dedupv1::base::FromBase64;
using dedupv1::base::make_bytestring;

LOGGER("Dedupv1dUser");

namespace dedupv1d {

Dedupv1dUser::Dedupv1dUser() {
    name_ = "";
    preconfigured_ = false;
}

Dedupv1dUser::Dedupv1dUser(bool preconfigured) {
    name_ = "";
    preconfigured_ = preconfigured;
}

Dedupv1dUser::~Dedupv1dUser() {
}

bool Dedupv1dUser::SerializeTo(UserInfoData* data) {
    CHECK(data, "Data not set");
    data->set_user_name(name());

    if (secret_hash().size() > 0) {
        // Encoded with null-terminating string via rot13 via base64
        data->set_secret(secret_hash());
        data->set_encoding_version(1);
    }

    for (list<string>::iterator i = targets_.begin(); i != targets_.end(); i++) {
        data->add_targets(*i);
    }
    return true;
}

string Dedupv1dUser::EncodePassword(const string& clear_text) {
    // treat char array as byte array. Safe as sizeof(char) == sizeof(byte) == 1
    const byte* clear_text_as_bytes = reinterpret_cast<const byte*>(clear_text.data());
    size_t len = clear_text.size() + 1;

    // encode
    string secret = ToBase64(ToRot13(make_bytestring(clear_text_as_bytes, len)));
    return secret;
}

bool Dedupv1dUser::ParseFrom(const UserInfoData& info) {
    // We redirect everything over the config system as some of these settings are delegated to other classes
    // and we have to duplicate the logic this way.

    if (info.has_user_name()) {
        CHECK(this->SetOption("name", info.user_name()),
            "Failed to set name");
    }
    if (info.has_secret()) {
        string secret = info.secret();
        if (!info.has_encoding_version() || info.encoding_version() == 0) {
            // upgrade to current encoding schema
            secret = EncodePassword(secret);
        }
        CHECK(this->SetOption("secret-hash", secret), "Failed to set secret");
    }
    for (int i = 0; i < info.targets_size(); i++) {
        CHECK(this->SetOption("target", info.targets(i)), "Failed to add target");
    }
    return true;
}

bool Dedupv1dUser::AddTarget(std::string target_name) {
    for (list<string>::const_iterator i = targets_.begin(); i != targets_.end(); i++) {
        CHECK(*i != target_name, "Target already added");
    }
    this->targets_.push_back(target_name);
    return true;
}

bool Dedupv1dUser::RemoveTarget(std::string target_name) {
    for (list<string>::iterator i = targets_.begin(); i != targets_.end(); i++) {
        if (*i == target_name) {
            targets_.erase(i);
            return true;
        }
    }
    ERROR("Failed to find target: " << target_name);
    return false;
}

bool Dedupv1dUser::SetOption(const string& option_name, const string& option) {
    if (option_name == "name") {
        CHECK(option.size() != 0, "Illegal user name (empty)");
        CHECK(option.size() <= 512, "Illegal user name (too long): " << option); // (tested max: 2048+)
        CHECK(RE2::FullMatch(option, "^[a-zA-Z0-9\\.\\-:_]+$"), "Illegal user name: " << option);
        this->name_ = option;
        return true;
    }
    if (option_name == "secret-hash") {
        string secret = option;
        CHECK(secret.size() >= 12, "Password hash not long enough: size " << secret.size());

        // validate size with the real data
        string raw_secret;
        bytestring s = FromRot13(FromBase64(secret));
        raw_secret.assign(reinterpret_cast<const char*>(s.data()), s.size() - 1);
        CHECK(raw_secret.size() <= 256, "Password too long: size " << raw_secret.size());
        CHECK(raw_secret.size() >= 12, "Password not long enough: size " << raw_secret.size());
        raw_secret.clear();

        this->secret_hash_ = option;
        return true;
    }
    if (option_name == "target") {
        CHECK(option.size() < 256, "Illegal target name");
        CHECK(AddTarget(option), "Failed to add target to user");
        return true;
    }
    ERROR("Illegal option: " << option_name << "=" << option);
    return false;
}

string Dedupv1dUser::DebugString() const {
    return "[name " + name() + "]";
}

}

