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

#include <core/filter.h>
#include <base/logging.h>
#include <base/option.h>
#include <base/strutil.h>

#include <stdlib.h>
#include <string.h>

LOGGER("Filter");

using std::map;
using std::string;
using dedupv1::base::strutil::To;
using dedupv1::base::Option;
using dedupv1::Session;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::ErrorContext;

namespace dedupv1 {
namespace filter {

MetaFactory<Filter> Filter::factory_("Filter", "filter");

MetaFactory<Filter>& Filter::Factory() {
    return factory_;
}

Filter::Filter(const string& name, enum filter_result max_filter_level) {
    this->max_filter_level_ = max_filter_level;
    this->name_ = name;
    enabled_by_default_ = true;
}

Filter::~Filter() {
}

bool Filter::Init() {
    return true;
}

bool Filter::Start(DedupSystem* system) {
    return true;
}

bool Filter::Update(Session* session,
                    const dedupv1::blockindex::BlockMapping* block_mapping,
                    ChunkMapping* mapping,
                    ErrorContext* ec) {
    return true;
}

bool Filter::UpdateKnownChunk(Session* session,
                              const dedupv1::blockindex::BlockMapping* block_mapping,
                              ChunkMapping* mapping,
                              ErrorContext* ec) {
    return Abort(session, block_mapping, mapping, ec);
}

bool Filter::Abort(Session* session,
                   const dedupv1::blockindex::BlockMapping* block_mapping,
                   ChunkMapping* chunk_mapping,
                   ErrorContext* ec) {
    return true;
}

bool Filter::Close() {
    delete this;
    return true;
}

bool Filter::SetOption(const string& option_name, const string& option) {
    if (option_name == "enabled") {
        Option<bool> b = To<bool>(option);
        CHECK(b.valid(), "Illegal option: " << option_name << "=" << option);
        enabled_by_default_ = b.value();
        return true;
    }
    ERROR("Illegal option: " << option_name << "=" << option <<
        " for filter " << name_);
    return false;
}

string Filter::GetFilterResultName(enum filter_result fr) {
    switch (fr) {
    case FILTER_EXISTING: return "Existing";
    case FILTER_STRONG_MAYBE: return "Strong Maybe";
    case FILTER_WEAK_MAYBE: return "Weak Maybe";
    case FILTER_NOT_EXISTING: return "Not Existing";
    case FILTER_ERROR: return "Error";
    default: return "Unknown";
    }
    return "";
}

}
}
