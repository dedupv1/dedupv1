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

#include "dedupv1d_target.h"

#include <base/strutil.h>
#include <base/logging.h>
#include <base/option.h>
#include <re2/re2.h>
#include <base/rot13.h>
#include <base/base64.h>

#include <sstream>

using std::string;
using std::stringstream;
using std::set;
using std::list;
using std::pair;
using std::make_pair;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::Join;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::ToRot13;
using dedupv1::base::FromRot13;
using dedupv1::base::ToBase64;
using dedupv1::base::FromBase64;
using dedupv1::base::make_bytestring;

LOGGER("Dedupv1dTarget");

namespace dedupv1d {

const std::set<std::string> Dedupv1dTarget::kAllowedParams = Dedupv1dTarget::MakeAllowedParamSet();

Dedupv1dTarget::Dedupv1dTarget() {
    name_ = "";
    tid_ = 0;
    preconfigured_ = false;
}

Dedupv1dTarget::Dedupv1dTarget(bool preconfigured) {
    name_ = "";
    tid_ = 0;
    preconfigured_ = preconfigured;
}

Dedupv1dTarget::~Dedupv1dTarget() {
}

bool Dedupv1dTarget::SerializeTo(TargetInfoData* data) {
    DCHECK(data, "Data not set");

    data->set_target_name(name());
    data->set_tid(tid());

    for (list<pair <string, string> >::const_iterator i = params_.begin(); i != params_.end(); i++) {
        data->add_params(i->first + "=" + i->second);
    }

    if (!auth_username_.empty() or !auth_secret_hash_.empty()) {
        // auth info is set
        DCHECK(data->mutable_auth(), "Auth data not set");

        data->mutable_auth()->set_secret(auth_secret_hash_);
        data->mutable_auth()->set_username(auth_username_);
    }

    return true;
}

bool Dedupv1dTarget::ParseFrom(const TargetInfoData& info) {
    // We redirect everything over the config system as some of these settings are delegated to other classes
    // and we have to duplicate the logic this way.

    if (info.has_target_name()) {
        CHECK(this->SetOption("name", info.target_name()),
            "Failed to set name: " << info.ShortDebugString());
    }
    if (info.has_tid()) {
        CHECK(this->SetOption("tid", ToString(info.tid())),
            "Failed to set tid: " << info.ShortDebugString());
    }

    for (int i = 0; i < info.params_size(); i++) {
        string param_name;
        string param_value;
        CHECK(dedupv1::base::strutil::Split(info.params(i), "=", &param_name, &param_value),
            "Failed to split " << info.params(i) << ": " << info.ShortDebugString());

        CHECK(this->SetOption("param." + param_name, param_value), "Failed to set parameter: " << info.ShortDebugString());
    }

    if (info.has_auth()) {
        if (info.auth().has_username()) {
            CHECK(this->SetOption("auth.name", info.auth().username()), "Failed to set target name: " << info.ShortDebugString());
        }
        if (info.auth().has_secret()) {
            CHECK(this->SetOption("auth.secret", info.auth().secret()), "Failed to set target secret: " << info.ShortDebugString());
        }
    }

    return true;
}

Option<string> Dedupv1dTarget::param(const std::string& param) const {
    TRACE("Search param: " << DebugString() << ", param " << param);
    for (list<pair <string, string> >::const_iterator i = params_.begin(); i != params_.end(); i++) {
        if (i->first == param) {
            return make_option(i->second);
        }
    }
    return false;
}

bool Dedupv1dTarget::ChangeParam(const std::string& param_key, const std::string& param_value) {
    if (param_key == "name" || param_key == "auth.name" || param_key == "auth.secret") {
        return SetOption(param_key, param_value);
    }

    CHECK(StartsWith(param_key, "param."), "Illegal param key: " << param_key);
    string param = param_key.substr(strlen("param."));
    CHECK(kAllowedParams.find(param) != kAllowedParams.end(),
        "Illegal param: " << param_key);

    // allowed parameter
    for (list<pair <string, string> >::iterator i = params_.begin(); i != params_.end(); i++) {
        if (i->first == param) {
            i->second = param_value;

            TRACE("Updated param: " << DebugString() << ", param " << param_key);
            return true;
        }
    }
    // not found before
    this->params_.push_back(make_pair(param, param_value));
    TRACE("Added param: " << DebugString() << ", param " << param_key);
    return true;
}

bool Dedupv1dTarget::SetOption(const string& option_name, const string& option) {
    if (option_name == "name") {
        CHECK(option.size() != 0, "Illegal target name (empty)");
        CHECK(option.size() <= 223, "Illegal target name (too long): " << option); // (tested max: 256) see http://tools.ietf.org/html/rfc3720#section-3.2.6.1
        CHECK(RE2::FullMatch(option, "^[a-z0-9\\.\\-:]+$"), "Illegal target name: " << option);
        this->name_ = option;
        return true;
    }
    if (option_name == "tid") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option: " << option);
        this->tid_ = To<uint32_t>(option).value();
        CHECK(tid_ > 0, "Illegal tid: " << option);
        return true;
    }
    if (StartsWith(option_name, "param.")) {
        return ChangeParam(option_name, option);
    }
    if (option_name == "auth.name") {
        this->auth_username_ = option;
        return true;
    }
    if (option_name == "auth.secret") {
        string secret = option;
        CHECK(secret.size() >= 12, "Password hash not long enough: size " << secret.size());

        // validate size with the real data
        string raw_secret;
        bytestring s = FromRot13(FromBase64(secret));
        raw_secret.assign(reinterpret_cast<const char*>(s.data()), s.size() - 1);
        CHECK(raw_secret.size() <= 256, "Password too long: size " << raw_secret.size());
        CHECK(raw_secret.size() >= 12, "Password not long enough: size " << raw_secret.size());
        raw_secret.clear();

        this->auth_secret_hash_ = option;
        return true;
    }
    ERROR("Illegal option: " << option_name << "=" << option);
    return false;
}

string Dedupv1dTarget::DebugString() const {
    stringstream sstr;
    sstr << "[" <<
    "tid " << tid_ <<
    ", name " << name() <<
    ", params [";

    for (list<pair <string, string> >::const_iterator i = params_.begin(); i != params_.end(); i++) {
        if (i != params_.begin()) {
            sstr << ",";
        }
        sstr << i->first << "=" << i->second;
    }
    sstr << "]";

    if (!auth_username_.empty() or !auth_secret_hash_.empty()) {
        sstr << ", authname " << auth_username_ << ", auth secret " << auth_secret_hash_;
    }

    return sstr.str();
}

set<string>  Dedupv1dTarget::MakeAllowedParamSet() {
    set<string> s;
    // copied from iscsi-scst.conf example file
    s.insert("RspTimeout");
    s.insert("NopInInterval");
    s.insert("MaxSessions");
    s.insert("MaxConnections");
    s.insert("InitialR2T");
    s.insert("ImmediateData");
    s.insert("MaxRecvDataSegmentLength");
    s.insert("MaxXmitDataSegmentLength");
    s.insert("MaxBurstLength");
    s.insert("FirstBurstLength");
    s.insert("DefaultTime2Wait");
    s.insert("DefaultTime2Retain");
    s.insert("MaxOutstandingR2T");
    s.insert("DataPDUInOrder");
    s.insert("DataSequenceInOrder");
    s.insert("ErrorRecoveryLevel");
    s.insert("HeaderDigest");
    s.insert("DataDigest");
    s.insert("QueuedCommands");
    return s;
}
}

