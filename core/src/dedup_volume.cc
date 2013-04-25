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

#include <core/dedup_volume.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sstream>

#include <core/dedup_system.h>
#include <base/strutil.h>
#include <base/logging.h>

using std::list;
using std::pair;
using std::set;
using std::make_pair;
using std::stringstream;
using std::string;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::StartsWith;
using dedupv1::scsi::SCSI_CHECK_CONDITION;
using dedupv1::scsi::SCSI_KEY_ILLEGAL_REQUEST;
using dedupv1::scsi::SCSI_KEY_NOT_READY;
using dedupv1::scsi::ScsiResult;
using dedupv1::base::Option;
using dedupv1::base::ErrorContext;
using dedupv1::base::ResourceManagement;
using dedupv1::SessionResourceType;

LOGGER("DedupVolume");

namespace dedupv1 {

DedupVolume::DedupVolume() {
    this->id_ = kUnsetVolumeId;
    this->logical_size_ = 0;
    this->system_ = NULL;
    this->chunker_ = NULL;
    this->session_management_ = NULL;
    session_count_ = DedupSystem::kDefaultSessionCount;
    maintainance_mode_ = false;
}

DedupVolume::~DedupVolume() {
}

bool DedupVolume::SetOption(const string& option_name, const string& option) {
    if (option_name == "logical-size") {
        CHECK(!system_, "System already started");
        Option<int64_t> ls = ToStorageUnit(option);
        if (!ls.valid()) {
            // try with "B" prefix
            if (option.size() > 1 && (option[option.size() - 1] == 'B' || option[option.size() - 1] == 'b')) {
                ls = ToStorageUnit(option.substr(0, option.size() - 1));
                CHECK(ls.valid(), "Illegal option " << option);
            } else {
                ERROR("Illegal option " << option);
                return false;
            }
        }
        CHECK(ls.value() > 0, "Illegal logical size " << option);
        this->logical_size_ = ls.value();
        return true;
    }
    if (option_name == "id") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->id_ = To<uint32_t>(option).value();
        CHECK(this->id_ <= DedupVolume::kMaxVolumeId, "Illegal volume id: id " << this->id_ << ", max id " << DedupVolume::kMaxVolumeId);
        return true;
    }
    if (option_name == "session-count") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->session_count_ = ToStorageUnit(option).value();
        return true;
    }
    // per volume chunking config
    if (option_name == "chunking") {
        return ChangePerVolumeOption(option_name, option);
        CHECK(chunking_config_.empty(), "Chunker not set");
        chunking_config_.push_back(make_pair("type", option));
        return true;
    }
    if (StartsWith(option_name, "chunking.")) {
        return ChangePerVolumeOption(option_name, option);
    }
    // per volume filter chain
    if (option_name == "filter") {
        return ChangePerVolumeOption(option_name, option);
    }
    ERROR("Illegal option: " << option_name);
    return false;
}

bool DedupVolume::Start(DedupSystem* system, bool initial_maintaiance_mode) {
    CHECK(system, "System not set");
    CHECK(this->id_ != kUnsetVolumeId, "Volume ID not set");
    CHECK(this->logical_size_ > 0, "Volume size not set");

    this->system_ = system;
    this->maintainance_mode_ = initial_maintaiance_mode;

    DEBUG("Starting core volume: " << this->DebugString() <<
        (maintainance_mode_ ? ", maintenance mode" : "") <<
        (chunker_ ? ", chunker " : "") <<
        (!enabled_filter_names_.empty() ? ", filter" : ""));

    // Check if all filter exists
    if (!enabled_filter_names_.empty()) {
        set<string>::iterator i;
        DCHECK(system_->filter_chain(), "Filter chain not set");
        for (i = enabled_filter_names_.begin(); i != enabled_filter_names_.end(); i++) {
            DCHECK(system_->filter_chain(), "Filter chain not set");
            dedupv1::filter::Filter* filter = system_->filter_chain()->GetFilterByName(*i);
            CHECK(filter, "Filter not configured: " << *i);
        }
    }

    if (!initial_maintaiance_mode) {
        if (!chunking_config_.empty()) {

            chunker_ = Chunker::Factory().Create(chunking_config_.front().second);
            CHECK(chunker_, "Failed to create chunker");

            list<pair<string, string> >::iterator i = ++chunking_config_.begin();
            for (; i != chunking_config_.end(); ++i) {
                CHECK(chunker_->SetOption(i->first, i->second), "Failed to configure chunker");
            }

            CHECK(this->chunker_->Start(), "Failed to start per-volume chunker: " <<
                "volume " << DebugString());
        }

        CHECK(session_management_ == NULL, "Session management already set");
        this->session_management_ = new ResourceManagement<Session>();
        CHECK(this->session_management_, "Session Management failed");
        CHECK(this->session_management_->Init("session",
                this->session_count_,
                new SessionResourceType(system->content_storage(), this)), "Failed to init session management");
    }

    return true;
}

bool DedupVolume::ChangePerVolumeOption(const std::string& option_name, const std::string& option) {
    CHECK(maintainance_mode_ || !system_, "Volume not in maintenance mode nor not started");

    // per volume chunking config
    if (option_name == "chunking") {
        CHECK(chunking_config_.empty(), "Chunking already configured");
        chunking_config_.push_back(make_pair("type", option));
        return true;
    }
    if (StartsWith(option_name, "chunking.")) {
        CHECK(!chunking_config_.empty(), "Chunking type not configured");
        string chunking_option_name = option_name.substr(strlen(".chunking"));
        chunking_config_.push_back(make_pair(chunking_option_name, option));
        return true;
    }
    // per volume filter chain
    if (option_name == "filter") {
        if (system_) { // if system is already set, check directly here
            dedupv1::filter::Filter* filter = system_->filter_chain()->GetFilterByName(option);
            CHECK(filter, "Filter not configured: " << option);
        }
        enabled_filter_names_.insert(option);
        return true;
    }
    return false;
}

bool DedupVolume::ChangeOptions(const std::list<std::pair<string, string> >& options) {
    CHECK(maintainance_mode_, "Volume not in maintenance mode");

    list<pair<string, string> >::const_iterator i;
    for (i = options.begin(); i != options.end(); ++i) {
        if (StartsWith(i->first, "filter")) {
            enabled_filter_names_.clear();
        }
        if (StartsWith(i->first, "chunking")) {
            chunking_config_.clear();
        }
    }
    for (i = options.begin(); i != options.end(); i++) {
        CHECK(ChangePerVolumeOption(i->first, i->second), "Failed to change volume option");
    }
    return true;
}

bool DedupVolume::ChangeMaintenanceMode(bool maintaince_mode) {

    if (this->maintainance_mode_ == maintaince_mode) {
        return true; // nothing changed
    }

    DEBUG("Change maintenance mode: " << ToString(maintaince_mode));
    maintainance_mode_ = maintaince_mode;
    if (maintaince_mode) {
        // shut off
        if (session_management_) {
            CHECK(session_management_->GetAcquiredCount() == 0,
                "Still sessions acquired");

            session_management_->Close();
            session_management_ = NULL;
        }
        if (chunker_) {
            chunker_->Close();
            chunker_ = NULL;
        }
    } else {
        // on
        DCHECK(!chunker_, "Chunker already set");
        DCHECK(!session_management_, "Session management already set");

        if (!chunking_config_.empty()) {

            chunker_ = Chunker::Factory().Create(chunking_config_.front().second);
            CHECK(chunker_, "Failed to create chunker");

            list<pair<string, string> >::iterator i = ++chunking_config_.begin();
            for (; i != chunking_config_.end(); ++i) {
                CHECK(chunker_->SetOption(i->first, i->second), "Failed to configure chunker");
            }

            CHECK(this->chunker_->Start(), "Failed to start per-volume chunker: " <<
                "volume " << DebugString());
        }

        this->session_management_ = new ResourceManagement<Session>();
        CHECK(this->session_management_, "Session Management failed");
        CHECK(this->session_management_->Init("session",
                this->session_count_,
                new SessionResourceType(system_->content_storage(), this)), "Failed to init session management");
    }
    return true;
}

bool DedupVolume::Close() {
    DEBUG("Close " << this->DebugString());
    if (session_management_) {
        CHECK(session_management_->Close(), "Failed to close sessions");
        session_management_ = NULL;
    }
    if (chunker_) {
        CHECK(chunker_->Close(), "Failed to close chunker");
        chunker_ = NULL;
    }

    return true;
}

dedupv1::scsi::ScsiResult DedupVolume::FastCopyTo(
    DedupVolume* target_volume,
    uint64_t src_offset,
    uint64_t target_offset,
    uint64_t size,
    dedupv1::base::ErrorContext* ec) {

    CHECK_RETURN(system_,
        ScsiResult::kDefaultNotReady,
        "Volume not started");
    CHECK_RETURN(target_volume,
        ScsiResult::kIllegalMessage,
        "Target not set");
    CHECK_RETURN(maintainance_mode_, ScsiResult::kDefaultNotReady,
        "Volume is not maintenance mode");
    CHECK_RETURN(target_volume->maintainance_mode_, ScsiResult::kDefaultNotReady,
        "Volume is not maintenance mode");

    DEBUG("Fast copy: src " << this->DebugString() <<
        ", target " << target_volume->DebugString() <<
        ", src offset " << src_offset <<
        ", target offset " << target_offset <<
        ", size " << size);

    uint64_t src_end_offset = src_offset + size - 1;
    if (src_end_offset > this->GetLogicalSize()) {
        // illegal access
        WARNING("Request out of range: " <<
            "offset " << src_offset <<
            ", size " << size <<
            ", logical size " << this->GetLogicalSize() <<
            ", volume " << this->DebugString());
        return dedupv1::scsi::ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x21, 0x00);
    }

    uint64_t target_end_offset = target_offset + size - 1;
    if (target_end_offset > target_volume->GetLogicalSize()) {
        // illegal access
        WARNING("Request out of range: " <<
            "offset " << target_offset <<
            ", size " << size <<
            ", logical size " << target_volume->GetLogicalSize() <<
            ", volume " << target_volume->DebugString());
        return dedupv1::scsi::ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x21, 0x00);
    }

    uint64_t volume_mask = ~((static_cast<uint64_t>(kMaxVolumeId) - 1) << (64 - kVolumeBits));
    CHECK_RETURN((volume_mask | src_end_offset) == volume_mask,
        dedupv1::scsi::ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x21, 0x00),
        "Data range overlaps with volume bits: " <<
        ", offset " << src_end_offset <<
        ", size " << size <<
        ", volume mask " << std::hex << volume_mask);

    volume_mask = ~((static_cast<uint64_t>(kMaxVolumeId) - 1) << (64 - kVolumeBits));
    CHECK_RETURN((volume_mask | target_end_offset) == volume_mask,
        dedupv1::scsi::ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x21, 0x00),
        "Data range overlaps with volume bits: " <<
        ", offset " << target_end_offset <<
        ", size " << size <<
        ", volume mask " << std::hex << volume_mask);

    uint64_t src_request_index = 0;
    uint64_t src_request_offset = 0;
    CHECK_RETURN(this->MakeIndex(src_offset, &src_request_index, &src_request_offset), ScsiResult::kDefaultNotReady,
        "Failed to calculate block id: offset " << src_offset << ", volume " << this->DebugString());

    uint64_t target_request_index = 0;
    uint64_t target_request_offset = 0;
    CHECK_RETURN(target_volume->MakeIndex(target_offset, &target_request_index, &target_request_offset), ScsiResult::kDefaultNotReady,
        "Failed to calculate block id: offset " << target_offset << ", volume " << target_volume->DebugString());

    return system_->FastCopy(src_request_index, src_request_offset, target_request_index, target_request_offset, size, ec);
}

bool DedupVolume::MakeIndex(uint64_t offset, uint64_t* request_block_id, uint64_t* request_offset) const {
    CHECK(request_block_id, "Request block id not set");
    CHECK(request_offset, "Request offset not set");
    CHECK(system_, "System not set");
    CHECK(system_->block_size() > 0, "Block size not set");

    uint64_t block_id = offset / this->system_->block_size();
    *request_block_id = make_multi_file_address(id_, block_id, DedupVolume::kVolumeBits);
    *request_offset = offset % this->system_->block_size();
    return true;
}

dedupv1::scsi::ScsiResult DedupVolume::SyncCache() {
    return this->system_->SyncCache();
}

dedupv1::scsi::ScsiResult DedupVolume::MakeRequest(enum request_type rw,
                                                   uint64_t offset,
                                                   uint64_t size,
                                                   byte* buffer,
                                                   ErrorContext* ec) {
    CHECK_RETURN(system_,
        ScsiResult::kDefaultNotReady,
        "Volume not started");

    if (maintainance_mode_) {
        return ScsiResult::kDefaultNotReady;
    }

    uint64_t end_offset = offset + size - 1;
    if (end_offset > this->GetLogicalSize()) {
        // illegal access
        WARNING("Request out of range: " <<
            "offset " << offset <<
            ", size " << size <<
            ", logical size " << this->GetLogicalSize() <<
            ", volume " << this->DebugString());
        return dedupv1::scsi::ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x21, 0x00);
    }

    uint64_t volume_mask = ~((static_cast<uint64_t>(kMaxVolumeId) - 1) << (64 - kVolumeBits));
    CHECK_RETURN((volume_mask | end_offset) == volume_mask,
        dedupv1::scsi::ScsiResult(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x21, 0x00),
        "Data range overlaps with volume bits: " <<
        ", offset " << offset <<
        ", size " << size <<
        ", volume mask " << std::hex << volume_mask);

    uint64_t request_index = 0;
    uint64_t request_offset = 0;
    CHECK_RETURN(this->MakeIndex(offset, &request_index, &request_offset), ScsiResult::kDefaultNotReady,
        "Failed to calculate block id: offset " << offset << ", volume " << this->DebugString());

    Session* sess = this->session_management_->Acquire();
    CHECK_RETURN(sess,
        (rw == REQUEST_READ ? ScsiResult::kReadError : ScsiResult::kWriteError),
        "No dedup session available");

    DCHECK_RETURN(sess->open_request_count() == 0,
        (rw == REQUEST_READ ? ScsiResult::kReadError : ScsiResult::kWriteError),
        "Uncleared session");
    dedupv1::scsi::ScsiResult scsi_result = this->system_->MakeRequest(sess, rw, request_index, request_offset, size, buffer, ec);
    if (!this->session_management_->Release(sess)) {
        WARNING("Failed to release session");
    }

    return scsi_result;
}

dedupv1::base::Option<bool> DedupVolume::Throttle(int thread_id, int thread_count) {
    if (this->system_ == NULL) {
        return false;
    }
    return this->system_->Throttle(thread_id, thread_count);
}

string DedupVolume::DebugString() const {
    stringstream sstr;
    sstr <<  "[Volume: id " << this->GetId() <<
    ", size " << this->GetLogicalSize();
    if (enabled_filter_names_.empty()) {
        sstr << ", filter default";
    } else {
        string filter_names = dedupv1::base::strutil::Join(enabled_filter_names_.begin(),
            enabled_filter_names_.end(), ", ");
        sstr << ", filter [" << filter_names << "]";
    }
    if (chunking_config_.empty()) {
        sstr << ", chunking default";
    } else {
        sstr << ", chunking custom";
    }
    sstr << "]";
    return sstr.str();
}

bool DedupVolume::ChangeLogicalSize(uint64_t new_logical_size) {
    logical_size_ = new_logical_size;
    return true;
}

bool DedupVolume::GetBlockInterval(uint64_t* start_block_id, uint64_t* end_block_id) const {
    CHECK(start_block_id, "Start block id not set");
    CHECK(end_block_id, "End block id not set");
    CHECK(system_, "System not set");

    uint64_t block_id = 0;
    uint64_t block_offset = 0;
    CHECK(this->MakeIndex(0, &block_id, &block_offset), "Failed to calculate start block id");
    *start_block_id = block_id;

    CHECK(this->MakeIndex(this->GetLogicalSize(), &block_id, &block_offset), "Failed to calculate end block id");
    *end_block_id = block_id;
    return true;
}

bool DedupVolume::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    DCHECK(ps, "Persistent statistics not set");
    if (this->chunker_) {
        CHECK(this->chunker_->PersistStatistics(prefix + ".chunking", ps), "Failed to persist chunker stats");
    }
    return true;
}

bool DedupVolume::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    DCHECK(ps, "Persistent statistics not set");
    if (this->chunker_) {
        CHECK(this->chunker_->RestoreStatistics(prefix + ".chunking", ps), "Failed to restore chunker stats");
    }
    return true;
}

string DedupVolume::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"chunker\": " << (chunker_ ? this->chunker_->PrintLockStatistics() : "null") << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

string DedupVolume::PrintStatistics() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"chunker\": " << (chunker_ ? this->chunker_->PrintStatistics() : "null") << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

string DedupVolume::PrintTrace() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"chunker\": " << (chunker_ ? this->chunker_->PrintTrace() : "null") << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

string DedupVolume::PrintProfile() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"chunker\": " << (chunker_ ? this->chunker_->PrintProfile() : "null") << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

}
