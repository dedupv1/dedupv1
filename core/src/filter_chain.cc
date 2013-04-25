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

#include <core/filter_chain.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include <base/index.h>
#include <core/storage.h>
#include <core/chunker.h>
#include <core/filter.h>
#include <core/dedup_volume.h>
#include <base/logging.h>
#include <base/strutil.h>

#include <sstream>
#include <set>
#include <map>

using std::string;
using std::set;
using std::map;
using std::list;
using std::vector;
using std::stringstream;
using dedupv1::base::Option;
using dedupv1::base::ProfileTimer;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::chunkstore::Storage;
using dedupv1::Session;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::ErrorContext;

LOGGER("FilterChain");

namespace dedupv1 {
namespace filter {

bool FilterChain::SetOption(const string& option_name, const string& option) {

    CHECK(option_name.size() > 0, "Option name not set");
    CHECK(option.size() > 0, "Option value not set")

    CHECK(this->last_filter_, "No filter set");

    return last_filter_->SetOption(option_name, option);
}

bool FilterChain::AddFilter(const string& filter_type) {
    Filter* new_filter = NULL;

    CHECK(filter_type.size() > 0, "Filter Type not set");

    // check if the filter type is already configured
    std::list<Filter*>::const_iterator i;
    for (i = chain_.begin(); i != chain_.end(); i++) {
        Filter* existing_filter = *i;
        CHECK(existing_filter->GetName() != filter_type,
            "Filter " << filter_type << " already configured");
    }

    new_filter = Filter::Factory().Create(filter_type);
    CHECK(new_filter, "Create Filter failed");

    this->chain_.push_back(new_filter);
    this->last_filter_ = new_filter;
    return true;
}

FilterChain::FilterChain() {
    last_filter_ = NULL;
}

FilterChain::Statistics::Statistics() {
    index_reads_ = 0;
    index_writes_ = 0;
}

bool FilterChain::Start(DedupSystem* system) {
    INFO("Starting filter chain");

    if (this->chain_.size() == 0) {
        // Use defaults
        CHECK(this->AddFilter("block-index-filter"),
            "Failed to add default filter");
        CHECK(this->AddFilter("chunk-index-filter"),
            "Failed to add default filter");
    }

    list<Filter*>::iterator i;
    for (i = this->chain_.begin(); i != this->chain_.end(); i++) {
        Filter* filter = *i;
        CHECK(filter->Start(system), "Cannot start filter " << filter->GetName());
    }
    this->last_filter_ = NULL;
    return true;
}

bool FilterChain::StoreChunkInfo(Session* session,
                                 const BlockMapping* block_mapping,
                                 ChunkMapping* chunk_mapping,
                                 ErrorContext* ec) {
    DCHECK(session, "Session not set");
    DCHECK(chunk_mapping, "Chunk mapping not set");

    ProfileTimer update_timer(stats_.update_time_);
    bool failed = false;

    DEBUG("Update filer chain: chunk " << chunk_mapping->DebugString());

    if (chunk_mapping->fingerprint_size() == 0) {
        ERROR("Illegal fingerprint for chunk: " << chunk_mapping->DebugString());
        failed = true;
    }

    const list<Filter*>& chain(session->volume()->enabled_filter_list());

    if (!failed && chunk_mapping->is_known_chunk() == false) {
        for (list<Filter*>::const_iterator j = chain.begin(); j != chain.end(); j++) {
            Filter* filter = *j;
                if (!filter->Update(session, block_mapping, chunk_mapping, ec)) {
                    ERROR("Update of filter index failed: " << chunk_mapping->DebugString());
                    failed = true;
                }
        }
        this->stats_.index_writes_++;
    } else if (!failed && chunk_mapping->is_known_chunk()) {
        for (list<Filter*>::const_iterator j = chain.begin(); j != chain.end(); j++) {
            Filter* filter = *j;
                if (!filter->UpdateKnownChunk(session, block_mapping, chunk_mapping, ec)) {
                    ERROR("Update of filter index failed: " << chunk_mapping->DebugString());
                    failed = true;
                }
        }
    } else {
        for (list<Filter*>::const_iterator j = chain.begin(); j != chain.end(); j++) {
            Filter* filter = *j;
                if (!filter->Abort(session, block_mapping, chunk_mapping, ec)) {
                    if (ec) {
                        ERROR("Abort of filter failed: " << chunk_mapping->DebugString() << " because " << ec->DebugString());
                    } else {
                        ERROR("Abort of filter failed: " << chunk_mapping->DebugString());
                    }
                    failed = true;
                }
        }
    }
    // now everything should have a valid address
    CHECK(Storage::IsValidAddress(chunk_mapping->data_address(), true),
        "Fingerprint has illegal data address: " << chunk_mapping->DebugString());
    return !failed;
}

bool FilterChain::CheckChunk(Session* session,
                             const BlockMapping* block_mapping,
                             ChunkMapping* chunk_mapping,
                             ErrorContext* ec) {
    // session not always set
    // block mapping not always set
    DCHECK_RETURN(chunk_mapping, Filter::FILTER_ERROR, "Chunk mapping not set");
    chunk_mapping->set_data_address(Storage::ILLEGAL_STORAGE_ADDRESS);

    const list<Filter*>& chain(session->volume()->enabled_filter_list());

    Filter::filter_result result = Filter::FILTER_WEAK_MAYBE;
    list<Filter*>::const_iterator j;
    for (j = chain.begin(); j != chain.end() &&
         (result == Filter::FILTER_WEAK_MAYBE || result == Filter::FILTER_STRONG_MAYBE); j++) {
        Filter* filter = *j;
            if (result == Filter::FILTER_WEAK_MAYBE
                || filter->GetMaxFilterLevel() == Filter::FILTER_EXISTING) {
                result = filter->Check(session, block_mapping, chunk_mapping, ec);
                if (result == Filter::FILTER_ERROR) {
                    if (ec && ec->is_full()) {
                        DEBUG("Filter result: full " <<
                            "block mapping " << (block_mapping ? block_mapping->DebugString() : "null") <<
                            ", chunk mapping " << chunk_mapping->DebugString());
                    } else {
                        ERROR("Filter lookup failed: " <<
                            "block mapping " << (block_mapping ? block_mapping->DebugString() : "null") <<
                            ", chunk mapping " << chunk_mapping->DebugString());
                    }
                    break;
                } else {
                  TRACE("Filter result: filter " << filter->GetName() <<
                      ", chunk mapping " << chunk_mapping->DebugString() <<
                      ", result " << Filter::GetFilterResultName(result));
                }
            }
    }
    if (result == Filter::FILTER_ERROR) {
        // Abort every that has to be aborted
        // j is the iterator of the filter where it failed
        for (list<Filter*>::const_iterator k = chain.begin(); k != j; k++) {
            Filter* filter = *k;
            DCHECK_RETURN(filter, Filter::FILTER_ERROR, "Filter not set");
                    if (!filter->Abort(session, block_mapping, chunk_mapping, ec)) {
                        WARNING("Failed to abort filter: " << filter->GetName() << ", chunk " << chunk_mapping->DebugString());
                    }
        }
    } else {
        // result is fine
        bool is_known_chunk = chunk_mapping->data_address() != Storage::ILLEGAL_STORAGE_ADDRESS;
        chunk_mapping->set_known_chunk(is_known_chunk);
    }
    return result != Filter::FILTER_ERROR;
}

bool FilterChain::ReadChunkInfo(Session* session,
                                const BlockMapping* block_mapping,
                                ChunkMapping* chunk_mapping,
                                ErrorContext* ec) {
    DCHECK(session, "Session not set");
    DCHECK(chunk_mapping, "Chunk mapping not set");

    ProfileTimer check_timer(stats_.check_time_);

    bool failed = false;
    DEBUG("Check for chunk " << chunk_mapping->DebugString());
    if (!this->CheckChunk(session, block_mapping, chunk_mapping, ec)) {
        if (!ec->is_full()) {
            ERROR("Failed to check chunk: " << chunk_mapping->DebugString());
        }
        failed = true;
    }
    this->stats_.index_reads_++;
    return !failed;
}

bool FilterChain::AbortChunkInfo(
    Session* session,
    const BlockMapping* block_mapping,
    ChunkMapping* chunk_mapping,
    ErrorContext* ec) {
    DCHECK(session, "Session not set");
    DCHECK(chunk_mapping, "Chunk mapping not set");

    const list<Filter*>& chain(session->volume()->enabled_filter_list());

    bool failed = false;
    DEBUG("Abort filter chain: chunk " << chunk_mapping->DebugString());
    list<Filter*>::const_iterator j;
    for (j = chain.begin(); j != chain.end(); j++) {
        Filter* filter = *j;
        DCHECK(filter, "Filter not set");
            if (!filter->Abort(session, block_mapping, chunk_mapping, ec)) {
                ERROR("Abort of filter failed: " << chunk_mapping->DebugString());
                failed = true;
            }
    }
    return !failed;
}

dedupv1::filter::Filter* FilterChain::GetFilterByName(const std::string& name) {
    list<Filter*> f = GetChain();
    for (list<Filter*>::iterator i = f.begin(); i != f.end(); ++i) {
        Filter* filter = *i;
        if (filter && (filter->GetName() == name)) {
            return filter;
        }
    }
    return NULL;
}

FilterChain::~FilterChain() {
    list<Filter*>::iterator i;
    for (i = this->chain_.begin(); i != this->chain_.end(); i++) {
        Filter* filter = *i;
        if (filter) {
            delete filter;
        }
    }
}

bool FilterChain::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    for (list<Filter*>::iterator i = this->chain_.begin(); i != this->chain_.end(); i++) {
        Filter* filter = *i;
        CHECK(filter, "Filter not set");
        CHECK(filter->PersistStatistics(prefix + "." + filter->GetName(), ps),
            "Failed to persist statistics of filter " << filter->GetName());
    }
    return true;
}

bool FilterChain::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    for (list<Filter*>::iterator i = this->chain_.begin(); i != this->chain_.end(); i++) {
        Filter* filter = *i;
        CHECK(filter, "Filter not set");

        string filter_key = prefix + "." + filter->GetName();
        // we changed at some point the name of the key
        Option<string> old_filter_key = dedupv1::base::strutil::ReplaceAll(filter_key, "-", " ");
        CHECK(old_filter_key.valid(), "Failed to get old filter name");

        Option<bool> o = ps->Exists(filter_key);
        CHECK(o.valid(), "Failed to check for key " << filter_key);
        if (o.value()) {
            CHECK(filter->RestoreStatistics(filter_key, ps),
                "Failed to restore statistics of filter " << filter->GetName());
        } else {
            // TODO (dmeister) Remove if not needed anymore
            // The old names are used up to version DEDUPE 0.3, newer versions
            // do not need this branch

            // here we use the old name, the next time the statistics are stored,
            // the new name is used and this branch can be phased out
            CHECK(filter->RestoreStatistics(old_filter_key.value(), ps),
                "Failed to restore statistics of filter " << filter->GetName());
        }
    }
    return true;
}

string FilterChain::PrintLockStatistics() {
    list<Filter*>::iterator i;
    stringstream sstr;
    sstr << "{";
    for (i = this->chain_.begin(); i != this->chain_.end(); i++) {
        if (i != this->chain_.begin()) {
            sstr << "," << std::endl;
        }
        Filter* filter = *i;
        sstr << "\"" << filter->GetName() << "\": " << filter->PrintLockStatistics() << "" << std::endl;
    }
    sstr << std::endl;
    sstr << "}";
    return sstr.str();
}

string FilterChain::PrintStatistics() {
    list<Filter*>::iterator i;
    stringstream sstr;
    sstr << "{";
    for (i = this->chain_.begin(); i != this->chain_.end(); i++) {
        if (i != this->chain_.begin()) {
            sstr << "," << std::endl;
        }
        Filter* filter = *i;
        sstr << "\"" << filter->GetName() << "\": " << filter->PrintStatistics() << "" << std::endl;
    }
    sstr << std::endl;
    sstr << "}";
    return sstr.str();
}

string FilterChain::PrintProfile() {
    list<Filter*>::iterator i;
    stringstream sstr;

    sstr << "{";
    sstr << "\"check time\": " << this->stats_.check_time_.GetSum() << "," << std::endl;
    sstr << "\"update time\": " << this->stats_.update_time_.GetSum() << std::endl;
    for (i = this->chain_.begin(); i != this->chain_.end(); i++) {
        sstr << "," << std::endl;
        Filter* filter = *i;
        sstr << "\"" << filter->GetName() << "\": " << filter->PrintProfile() << "" << std::endl;
    }
    sstr << std::endl;
    sstr << "}";
    return sstr.str();
}

string FilterChain::PrintTrace() {
    list<Filter*>::iterator i;
    stringstream sstr;
    sstr << "{";
    for (i = this->chain_.begin(); i != this->chain_.end(); i++) {
        if (i != this->chain_.begin()) {
            sstr << "," << std::endl;
        }
        Filter* filter = *i;
        sstr << "\"" << filter->GetName() << "\": " << filter->PrintTrace() << "" << std::endl;
    }
    sstr << std::endl;
    sstr << "}";
    return sstr.str();
}

}
}
