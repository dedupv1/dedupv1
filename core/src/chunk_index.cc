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

#include <fcntl.h>
#include <unistd.h>

#include <sstream>
#include <limits>

#include "dedupv1.pb.h"

#include <core/dedup.h>
#include <core/log_consumer.h>
#include <base/memory.h>
#include <base/index.h>
#include <core/storage.h>
#include <core/chunker.h>
#include <core/filter.h>
#include <base/logging.h>
#include <base/timer.h>
#include <base/strutil.h>
#include <core/container_storage.h>
#include <core/container.h>
#include <core/dedup_system.h>
#include <core/chunk_store.h>
#include <base/fileutil.h>
#include <core/log.h>
#include <base/timer.h>
#include <base/profile.h>
#include <core/chunk_index.h>
#include <base/disk_hash_index.h>
#include <base/fault_injection.h>
#include <dedupv1_stats.pb.h>
#include <base/runnable.h>

using std::list;
using std::string;
using std::stringstream;
using std::vector;
using std::pair;
using std::make_pair;
using dedupv1::base::MemoryIndex;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::ProfileTimer;
using dedupv1::base::SlidingAverageProfileTimer;
using dedupv1::base::File;
using dedupv1::chunkstore::Container;
using dedupv1::chunkstore::ContainerStorage;
using dedupv1::chunkstore::ContainerItem;
using dedupv1::chunkstore::storage_commit_state;
using dedupv1::chunkstore::STORAGE_ADDRESS_ERROR;
using dedupv1::chunkstore::STORAGE_ADDRESS_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_NOT_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_WILL_NEVER_COMMITTED;
using dedupv1::chunkstore::Storage;
using dedupv1::base::ScopedArray;
using dedupv1::Fingerprinter;
using dedupv1::base::lookup_result;
using dedupv1::base::IndexIterator;
using dedupv1::base::Index;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::PUT_KEEP;
using dedupv1::base::DELETE_ERROR;
using dedupv1::base::DELETE_OK;
using dedupv1::base::delete_result;
using dedupv1::base::put_result;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::make_bytestring;
using dedupv1::base::ErrorContext;
using dedupv1::base::Future;
using dedupv1::base::ThreadUtil;
using dedupv1::base::Runnable;
using dedupv1::base::Threadpool;
using dedupv1::base::make_option;
using dedupv1::base::cache_lookup_method;
using dedupv1::base::cache_dirty_mode;

LOGGER("ChunkIndex");

namespace dedupv1 {
namespace chunkindex {

MetaFactory<ChunkIndex> ChunkIndex::factory_("ChunkIndex", "chunk index");

MetaFactory<ChunkIndex>& ChunkIndex::Factory() {
    return factory_;
}

ChunkIndex* ChunkIndex::CreateIndex() {
    return new ChunkIndex();
}

void ChunkIndex::RegisterChunkIndex() {
    ChunkIndex::Factory().Register("disk", &ChunkIndex::CreateIndex);
}

ChunkIndex::Statistics::Statistics() : average_lookup_latency_(256) {
    this->lock_free_ = 0;
    this->lock_busy_ = 0;
    this->imported_container_count_ = 0;
    this->throttle_count_ = 0;
    this->index_full_failure_count_ = 0;
    this->bg_container_import_wait_count_ = 0;
}

ChunkIndex::ChunkIndex() {
    this->chunk_index_ = NULL;
    this->log_ = NULL;
    this->storage_ = NULL;
    this->bg_committer_ = NULL;
    info_store_ = NULL;
    tp_ = NULL;
    this->bg_thread_count_ = 1;
    this->state_ = CREATED;
    is_replaying_ = false;
    import_if_replaying_ = true;
    dirty_import_container_exists_ = false;
    import_delay_ = 0;
    dirty_import_finished_ = false;
    dirty_chunk_count_threshold_ = 0;
    has_reported_importing_ = false;
    sampling_strategy_ = NULL;
}

ChunkIndex::~ChunkIndex() {
}

bool ChunkIndex::Init() {
    return true;
}

bool ChunkIndex::CheckIndeces() {
    CHECK(this->chunk_index_, "Persistent Chunk Index not set");
    return true;
}

bool ChunkIndex::SetOption(const string& option_name, const string& option) {
    CHECK(this->state_ == CREATED, "Illegal state: state " << this->state_);
    CHECK(option_name.size() > 0, "Option name not set");
    CHECK(option.size() > 0, "Option not set");

    if (option_name == "replaying-import") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->import_if_replaying_ = To<bool>(option).value();
        return true;
    }
    if (option_name == "import-delay") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        import_delay_ = To<uint32_t>(option).value();
        return true;
    }
    if (StartsWith(option_name, "in-combats.")) {
        CHECK(this->in_combats_.SetOption(option_name.substr(strlen("in-combats.")),
                option), "Configuration failed");
        return true;
    }
    if (option_name == "dirty-chunks-threshold") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->dirty_chunk_count_threshold_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "sampling-strategy") {
        CHECK(sampling_strategy_ == NULL, "Sampling strategy already set");
        sampling_strategy_ = ChunkIndexSamplingStrategy::Factory().Create(option);
        CHECK(sampling_strategy_, "Failed to create sampling strategy");
        return true;
    }
    if (StartsWith(option_name, "sampling-strategy.")) {
        CHECK(sampling_strategy_, "Sampling strategy not set");
        CHECK(sampling_strategy_->SetOption(
                option_name.substr(strlen("sampling-strategy.")),
                option), "Configuration failed");
        return true;
    }
    if (option_name == "persistent") {
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Persistent index creation failed");
        this->chunk_index_ = index->AsPersistentIndex();
        CHECK(this->chunk_index_, "Chunk index should persistent");
        CHECK(this->chunk_index_->HasCapability(dedupv1::base::PERSISTENT_ITEM_COUNT),
            "Index has no persistent item count");
        CHECK(this->chunk_index_->HasCapability(dedupv1::base::WRITE_BACK_CACHE),
            "Index has no write-cache support");

        // Set default options
        CHECK(this->chunk_index_->SetOption("max-key-size",
                ToString(Fingerprinter::kMaxFingerprintSize)),
            "Failed to set auto option");

        // currently the ChunkMappingData type contains three 64-bit values.
        CHECK(this->chunk_index_->SetOption("max-value-size", "32"),
            "Failed to set auto option");
        return true;
    }
    if (StartsWith(option_name, "persistent.")) {
        CHECK(this->chunk_index_, "Persistent data index not set");
        CHECK(this->chunk_index_->SetOption(option_name.substr(strlen("persistent.")),
                option), "Configuration failed");
        return true;
    }
    if (option_name == "bg-thread-count") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->bg_thread_count_ = To<uint32_t>(option).value();
        return true;
    } else if (StartsWith(option_name, "throttle.")) {
        CHECK(this->throttling_.SetOption(option_name.substr(strlen("throttle.")),
                option),
            "Failed to configure log throttling");
        return true;
    }
    ERROR("Illegal option: " << option_name);
    return false;
}

bool ChunkIndex::DumpMetaInfo() {
    DCHECK(info_store_, "Info store not set");

    DEBUG("Store container tracker: " << container_tracker_.DebugString());

    ChunkIndexLogfileData logfile_data;
    CHECK(this->container_tracker_.SerializeTo(logfile_data.mutable_container_tracker()),
        "Cannot serialize container tracker");
    CHECK(info_store_->PersistInfo("chunk-index", logfile_data),
        "Failed to store info: " << logfile_data.ShortDebugString());
    return true;
}

lookup_result ChunkIndex::ReadMetaInfo() {
    DCHECK_RETURN(info_store_, LOOKUP_ERROR, "Info store not set");

    ChunkIndexLogfileData logfile_data;
    lookup_result lr = info_store_->RestoreInfo("chunk-index", &logfile_data);
    CHECK_RETURN(lr != LOOKUP_ERROR, LOOKUP_ERROR, "Failed to restore chunk index info");
    if (lr == LOOKUP_NOT_FOUND) {
        return lr;
    }
    if (logfile_data.has_container_tracker()) {
        CHECK_RETURN(this->container_tracker_.ParseFrom(logfile_data.container_tracker()),
            LOOKUP_ERROR, "Cannot parse container tracker data: " <<
            logfile_data.ShortDebugString());

        DEBUG("Restored container tracker: " << container_tracker_.DebugString());
    }
    return LOOKUP_FOUND;
}

bool ChunkIndex::Start(const StartContext& start_context, DedupSystem* system) {
    DCHECK(system, "System not set");
    CHECK(this->state_ == CREATED, "Illegal state: state " << this->state_);
    CHECK(this->chunk_index_, "Persistent chunk index not set");

    INFO("Starting chunk index");

    this->log_ = system->log();
    DCHECK(this->log_, "Log not set");
    this->info_store_ = system->info_store();
    CHECK(this->info_store_, "Info store not set");
    this->tp_ = system->threadpool();
    CHECK(this->tp_, "Threadpool not set");

    if (sampling_strategy_ == NULL) {
        sampling_strategy_ = ChunkIndexSamplingStrategy::Factory().Create("full");
        CHECK(sampling_strategy_, "Failed to create sampling strategy");
    }
    CHECK(sampling_strategy_->Start(start_context, system),
        "Failed to start sampling strategy");

    if (!this->chunk_index_->IsWriteBackCacheEnabled()) {
        ERROR("Index has no write-back cache");
        return false;
    }
    if (dynamic_cast<dedupv1::base::DiskHashIndex*>(this->chunk_index_)) {
        // set better maximal key size if the static hash index is used
        // this is the only usage of the content storage in the chunk index
        dedupv1::ContentStorage* content_storage = system->content_storage();
#ifdef DEDUPV1_CORE_TEST
        // I need this for some tests
        if (content_storage == NULL) {
            CHECK(this->chunk_index_->SetOption("max-key-size", "20"),
                "Failed to set max key size");
        } else {
#endif
        CHECK(content_storage, "Content storage not set");
        Fingerprinter* fp = Fingerprinter::Factory().Create(content_storage->fingerprinter_name());
        CHECK(fp, "Failed to create fingerprinter");
        size_t fp_size = fp->GetFingerprintSize();
        CHECK(fp->Close(), "Failed to close fingerprinter");
        fp = NULL;
        CHECK(this->chunk_index_->SetOption("max-key-size", ToString(fp_size)),
            "Failed to set max key size");
#ifdef DEDUPV1_CORE_TEST
    }
#endif
    }

    CHECK(this->chunk_locks_.Start(start_context), "Failed to start chunk locks");
    CHECK(this->in_combats_.Start(start_context, this->log_), "Failed to start chunk in combat");
    CHECK(this->chunk_index_->Start(start_context), "Could not start index");

    if (dirty_chunk_count_threshold_ == 0) {
        // unset
        DCHECK(chunk_index_->GetEstimatedMaxCacheItemCount() > 0,
            "Illegal max cache item count");
        dirty_chunk_count_threshold_ = chunk_index_->GetEstimatedMaxCacheItemCount() * 0.7;
    }

    this->storage_ = system->storage();
    CHECK(this->storage_, "Failed to set storage");

    lookup_result info_lookup = ReadMetaInfo();
    CHECK(info_lookup != LOOKUP_ERROR, "Failed to read meta info");
    CHECK(!(info_lookup == LOOKUP_NOT_FOUND && !start_context.create()),
        "Failed to lookup meta info in non-create startup mode");
    if (info_lookup == LOOKUP_NOT_FOUND && start_context.create()) {
        CHECK(DumpMetaInfo(), "Failed to dump info");
    }

    this->bg_committer_ = new ChunkIndexBackgroundCommitter(this, this->bg_thread_count_, 5000 /* check interval */,
        import_delay_ /* wait interval */, false);
    CHECK(this->bg_committer_, "Cannot create chunk index background committer");
    CHECK(this->bg_committer_->Start(), "Failed to start bg committer");

    CHECK(this->log_->RegisterConsumer("chunk-index", this), "Cannot register chunk index");
    this->state_ = STARTED;
    return true;
}

bool ChunkIndex::Run() {
    CHECK(this->state_ == STARTED, "Illegal state: " << this->state_);
    CHECK(this->bg_committer_->Run(), "Failed to run bg committer");
    return true;
}

bool ChunkIndex::ImportAllReadyContainer() {
    DCHECK(chunk_index_, "Persistent chunk index not set");

    ChunkIndexBackgroundCommitter stop_commit(this, 4, 0, 0, true);
    CHECK(stop_commit.Start(), "Failed to start stop background committer");
    CHECK(stop_commit.Run(), "Failed to start stop background committer");
    CHECK(stop_commit.Wait(), "Failed to wait for stop background committer");

    return true;
}

bool ChunkIndex::Stop(dedupv1::StopContext stop_context) {
    if (state_ != STOPPED) {
        INFO("Stopping chunk index");
    }
    if (this->bg_committer_) {
        CHECK(this->bg_committer_->Stop(stop_context), "Failed to stop bg committer");
    }
    if (state_ == STARTED && stop_context.mode() == dedupv1::StopContext::WRITEBACK) {
        CHECK(ImportAllReadyContainer(), "Failed to import all ready container");
    }

    DEBUG("Stopped chunk index");
    this->state_ = STOPPED;
    return true;
}

bool ChunkIndex::Close() {
    DEBUG("Closing chunk index");

    CHECK(this->Stop(dedupv1::StopContext::FastStopContext()), "Failed to stop chunk index");

    if (info_store_) {
        // if the chunk index is not started, there is nothing to dump
        if (!DumpMetaInfo()) {
            WARNING("Failed to dump meta info");
        }
    }

    if (this->bg_committer_) {
        delete this->bg_committer_;
        this->bg_committer_ = NULL;
    }

    if (this->chunk_index_) {
        if (!this->chunk_index_->Close()) {
            WARNING("Cannot close main chunk index");
        }
        this->chunk_index_ = NULL;
    }

    if (sampling_strategy_) {
        if (!sampling_strategy_->Close()) {
            WARNING("Failed to close sampling strategy");
        }
        sampling_strategy_ = NULL;
    }
    if (this->log_) {
        if (this->log_->IsRegistered("chunk-index").value()) {
            if (!this->log_->UnregisterConsumer("chunk-index")) {
                WARNING("Failed to unregister from log");
            }
        }
        this->log_ = NULL;
    }
    delete this;
    return true;
}

#ifdef DEDUPV1_CORE_TEST
void ChunkIndex::ClearData() {
    if (this->bg_committer_) {
        this->bg_committer_->Stop(StopContext::WritebackStopContext());
    }
    if (this->log_) {
        log_->UnregisterConsumer("chunk-index");
        log_ = NULL;
    }
    if (this->chunk_index_) {
        this->chunk_index_->Close();
        chunk_index_ = NULL;
    }
}

#endif

lookup_result ChunkIndex::LookupNextIterator(IndexIterator* it, ChunkMapping* mapping) {
    DCHECK_RETURN(mapping, LOOKUP_ERROR, "Mapping not set");
    DCHECK_RETURN(it, LOOKUP_ERROR, "Iterator not set");

    mapping->set_fingerprint_size(Fingerprinter::kMaxFingerprintSize);

    ChunkMappingData value_data;
    enum lookup_result result = it->Next(mapping->mutable_fingerprint(), mapping->mutable_fingerprint_size(),
        &value_data);
    CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR, "Error while accessing index" <<
        " chunk mapping " << mapping->DebugString());
    if (result == LOOKUP_FOUND) {
        CHECK_RETURN(mapping->UnserializeFrom(value_data, true), LOOKUP_ERROR,
            "Cannot unserialize chunk mapping: " << value_data.ShortDebugString());
        TRACE("Found index entry: data " << mapping->DebugString());
    }
    return result;
}

dedupv1::base::lookup_result ChunkIndex::LookupPersistentIndex(ChunkMapping* mapping,
                                                               enum cache_lookup_method cache_lookup_type,
                                                               enum cache_dirty_mode dirty_mode,
                                                               dedupv1::base::ErrorContext* ec) {
    DCHECK_RETURN(chunk_index_, LOOKUP_ERROR, "Chunk index not set");
    DCHECK_RETURN(mapping, LOOKUP_ERROR, "Mapping not set");

    ChunkMappingData value_data;
    lookup_result result = chunk_index_->LookupDirty(mapping->fingerprint(),
        mapping->fingerprint_size(),
        cache_lookup_type,
        dirty_mode,
        &value_data);
    CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR, "Error while accessing index" <<
        " chunk mapping " << mapping->DebugString());
    if (result == LOOKUP_FOUND) {
        CHECK_RETURN(mapping->UnserializeFrom(value_data, true),
            LOOKUP_ERROR,
            "Cannot unserialize chunk mapping: " << value_data.ShortDebugString());

        TRACE("Found index entry: "
            "chunk " << mapping->DebugString() <<
            ", data " << value_data.ShortDebugString() <<
            ", cache lookup method " << ToString(cache_lookup_type) <<
            ", dirty mode " << ToString(dirty_mode) <<
            ", source persistent");
    }
    return result;
}

lookup_result ChunkIndex::LookupIndex(Index* index, ChunkMapping* mapping, dedupv1::base::ErrorContext* ec) {
    DCHECK_RETURN(mapping, LOOKUP_ERROR, "Mapping not set");
    DCHECK_RETURN(index, LOOKUP_ERROR, "Index not set");

    ChunkMappingData value_data;
    lookup_result result = index->Lookup(mapping->fingerprint(), mapping->fingerprint_size(), &value_data);
    CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR, "Error while accessing index" <<
        " chunk mapping " << mapping->DebugString());
    if (result == LOOKUP_FOUND) {
        CHECK_RETURN(mapping->UnserializeFrom(value_data, true),
            LOOKUP_ERROR,
            "Cannot unserialize chunk mapping: " << value_data.ShortDebugString());
        TRACE("Found index entry: "
            "chunk " << mapping->DebugString() << ", data " << value_data.ShortDebugString());
    }
    return result;
}

lookup_result ChunkIndex::Lookup(ChunkMapping* mapping,
                                 bool add_as_in_combat,
                                 ErrorContext* ec) {
    CHECK_RETURN(this->state_ == STARTED, LOOKUP_ERROR, "Illegal state: state " << this->state_);
    DCHECK_RETURN(mapping, LOOKUP_ERROR, "Mapping not set");

    SlidingAverageProfileTimer average_lookup_timer(this->stats_.average_lookup_latency_);

    ProfileTimer total_timer(this->stats_.profiling_);
    ProfileTimer lookup_timer(this->stats_.lookup_time_);

    enum lookup_result result = this->LookupPersistentIndex(mapping,
        dedupv1::base::CACHE_LOOKUP_DEFAULT,
        dedupv1::base::CACHE_ALLOW_DIRTY,
        ec);
    CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR, "Error while accessing main index: " <<
        "mapping " << mapping->DebugString());
    if (result == LOOKUP_FOUND) {
        TRACE("Lookup chunk mapping " << mapping->DebugString() << ", result found");
    } else {
        TRACE("Lookup chunk mapping " << Fingerprinter::DebugString(mapping->fingerprint(),
                mapping->fingerprint_size()) << ", result not found");
    }

    if (add_as_in_combat) {
        // here the output of the lookup is not important because even if the chunk
        // is not found, then will eventually be inserted into the chunk index
        in_combats().Touch(mapping->fingerprint(), mapping->fingerprint_size());
    }
    return result;
}

bool ChunkIndex::PutIndex(Index* index,
                          const ChunkMapping& mapping,
                          dedupv1::base::ErrorContext* ec) {
    ChunkMappingData data;
    CHECK(mapping.SerializeTo(&data),
        "Failed to serialize chunk mapping: " << mapping.DebugString());

    put_result result = index->Put(mapping.fingerprint(), mapping.fingerprint_size(), data);
    CHECK(result != PUT_ERROR,
        "Cannot put chunk mapping data: " << mapping.DebugString());
    return true;
}

bool ChunkIndex::PutPersistentIndex(const ChunkMapping& mapping,
                                    bool ensure_persistence,
                                    bool pin,
                                    dedupv1::base::ErrorContext* ec) {
    ChunkMappingData data;
    CHECK(mapping.SerializeTo(&data),
        "Failed to serialize chunk mapping: " << mapping.DebugString());

    TRACE("Put index entry: "
        "chunk " << mapping.DebugString() <<
        ", ensure persistence " << ToString(ensure_persistence) <<
        ", pin " << ToString(pin));

    put_result result;
    if (ensure_persistence) {
        result = chunk_index_->Put(mapping.fingerprint(),
            mapping.fingerprint_size(), data);
    } else {
        result = chunk_index_->PutDirty(mapping.fingerprint(),
            mapping.fingerprint_size(),
            data,
            pin);
    }
    CHECK(result != PUT_ERROR,
        "Cannot put chunk mapping data: " << mapping.DebugString());
    return true;
}

bool ChunkIndex::Delete(const ChunkMapping& mapping) {
    CHECK_RETURN(this->state_ == STARTED, DELETE_ERROR,
        "Illegal state: state " << this->state_);

    ProfileTimer timer(this->stats_.profiling_);

    TRACE("Delete from persistent chunk index: " << mapping.DebugString());
    enum delete_result result_persistent = this->chunk_index_->Delete(mapping.fingerprint(), mapping.fingerprint_size());
    CHECK(result_persistent != DELETE_ERROR, "Failed to delete mapping from persistent chunk index: " << mapping.DebugString());
    return true;
}

bool ChunkIndex::IsAcceptingNewChunks() {
    if (this->chunk_index_ == NULL) {
        return false;
    }
    return this->chunk_index_->GetTotalItemCount() < (0.9 * this->chunk_index_->GetEstimatedMaxItemCount());
}

bool ChunkIndex::Put(const ChunkMapping& mapping, ErrorContext* ec) {
    DCHECK_RETURN(this->state_ == STARTED, PUT_ERROR, "Illegal state: state " << this->state_);

    ProfileTimer total_timer(this->stats_.profiling_);
    ProfileTimer update_timer(this->stats_.update_time_);

    if (this->chunk_index_->GetItemCount() >= this->chunk_index_->GetEstimatedMaxItemCount()) {
        WARNING("Chunk index full: " <<
            "persistent index item count " << this->chunk_index_->GetItemCount() <<
            ", estimated maximal item count " << this->chunk_index_->GetEstimatedMaxItemCount());
        this->stats_.index_full_failure_count_++;
        if (ec) {
            ec->set_full();
        }
        return false;
    }

    TRACE("Updating chunk index (pinned): " << mapping.DebugString());

    // we pin the item into main memory
    return this->PutPersistentIndex(mapping, false, true, ec);
}

bool ChunkIndex::PutOverwrite(ChunkMapping& mapping, ErrorContext* ec) {
    DCHECK_RETURN(this->state_ == STARTED, PUT_ERROR,
        "Illegal state: state " << this->state_);

    ProfileTimer total_timer(this->stats_.profiling_);
    ProfileTimer update_timer(this->stats_.update_time_);

    TRACE("Updating persistent chunk index: " << mapping.DebugString());
    bool result = this->PutPersistentIndex(mapping, false, false, ec);
    CHECK(result, "Failed to put overwrite a chunk mapping: " << mapping.DebugString());

    return true;
}

bool ChunkIndex::PersistStatistics(std::string prefix,
                                   dedupv1::PersistStatistics* ps) {
    ChunkIndexStatsData data;
    data.set_imported_container_count(this->stats_.imported_container_count_);
    data.set_index_full_failure_count(stats_.index_full_failure_count_);
    CHECK(ps->Persist(prefix, data), "Failed to persist chunk index stats");
    return true;
}

bool ChunkIndex::RestoreStatistics(std::string prefix,
                                   dedupv1::PersistStatistics* ps) {
    ChunkIndexStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore chunk index stats");
    if (data.has_imported_container_count()) {
        this->stats_.imported_container_count_ = data.imported_container_count();
    }

    if (data.has_index_full_failure_count()) {
        stats_.index_full_failure_count_ = data.index_full_failure_count();
    }
    return true;
}

string ChunkIndex::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"lock free\": " << this->stats_.lock_free_ << "," << std::endl;
    sstr << "\"lock busy\": " << this->stats_.lock_busy_ << "," << std::endl;
    sstr << "\"chunk locks\": " << chunk_locks_.PrintLockStatistics() << "," << std::endl;
    sstr << "\"index\": " << (chunk_index_ ? this->chunk_index_->PrintLockStatistics() : "null") << std::endl;
    sstr << "}";
    return sstr.str();
}

string ChunkIndex::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"index\": " << (chunk_index_ ? this->chunk_index_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"throttle count\": " << this->stats_.throttle_count_ << "," << std::endl;
    sstr << "\"in combats\": " << this->in_combats_.PrintTrace() << "," << std::endl;
    sstr << "\"replaying state\": " << ToString(static_cast<bool>(is_replaying_)) << "," << std::endl;
    sstr << "\"bg container import wait count\": " << this->stats_.bg_container_import_wait_count_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string ChunkIndex::PrintStatistics() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{";
    sstr << "\"imported container count\": " <<
    this->stats_.imported_container_count_ << "," << std::endl;

    if (chunk_index_ && this->chunk_index_->GetEstimatedMaxItemCount() > 0) {
        uint64_t total_item_count = chunk_index_->GetItemCount();
        double main_fill_ratio = (1.0 * total_item_count) / this->chunk_index_->GetEstimatedMaxItemCount();
        sstr << "\"index fill ratio\": " << main_fill_ratio << "," << std::endl;
    } else {
        sstr << "\"index fill ratio\": null," << std::endl;
    }

    if (chunk_index_ && this->chunk_index_->GetEstimatedMaxCacheItemCount() > 0) {
        uint64_t total_item_count = chunk_index_->GetDirtyItemCount();
        double main_fill_ratio = (1.0 * total_item_count) / this->chunk_index_->GetEstimatedMaxCacheItemCount();
        sstr << "\"dirty cache fill ratio\": " << main_fill_ratio << "," << std::endl;
    } else {
        sstr << "\"dirty cache fill ratio\": null," << std::endl;
    }

    sstr << "\"index full failure count\": " << this->stats_.index_full_failure_count_ << "," << std::endl;
    sstr << "\"index item count\": " << (chunk_index_ ? ToString(this->chunk_index_->GetItemCount()) : "null") << ","
         << std::endl;
    sstr << "\"total index item count\": " << (chunk_index_ ? ToString(this->chunk_index_->GetTotalItemCount()) : "null") << ","
         << std::endl;
    sstr << "\"dirty index item count\": " << (chunk_index_ ? ToString(this->chunk_index_->GetDirtyItemCount()) : "null") << ","
         << std::endl;
    sstr << "\"index size\": " << (chunk_index_ ? ToString(this->chunk_index_->GetPersistentSize()) : "null")
         << std::endl;
    sstr << "}";
    return sstr.str();
}

string ChunkIndex::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"in combats\": " << this->in_combats_.PrintProfile() << "," << std::endl;
    sstr << "\"average lookup latency\": " << this->stats_.average_lookup_latency_.GetAverage() << "," << std::endl;
    sstr << "\"chunk index\": " << this->stats_.profiling_.GetSum() << "," << std::endl;
    sstr << "\"replay time\": " << this->stats_.replay_time_.GetSum() << "," << std::endl;
    sstr << "\"lookup time\": " << this->stats_.lookup_time_.GetSum() << "," << std::endl;
    sstr << "\"import time\": " << this->stats_.import_time_.GetSum() << "," << std::endl;
    sstr << "\"update time\": " << this->stats_.update_time_.GetSum() << "," << std::endl;
    sstr << "\"throttle time\": " << this->stats_.throttle_time_.GetSum() << "," << std::endl;
    sstr << "\"chunk locks\": " << chunk_locks_.PrintProfile() << "," << std::endl;

    sstr << "\"index\": " << (chunk_index_ ? this->chunk_index_->PrintProfile() : "null") << std::endl;
    sstr << "}";
    return sstr.str();
}

/**
 * Import task to import a specific container item into the chunk index.
 * The task is executed in the thread pool
 */
class ImportTask : public Runnable<bool> {
private:
    /**
     * Chunk index in that the item is imported
     */
    ChunkIndex* chunk_index_;

    /**
     * item to import
     */
    const ContainerItem item_;
    ErrorContext* ec_;
public:

    ImportTask(ChunkIndex* chunk_index, const ContainerItem& item, ErrorContext* ec) :
        chunk_index_(chunk_index), item_(item) {
        ec_ = ec;
    }

    virtual ~ImportTask() {
    }

    bool Run() {
        bool b = false;
        DCHECK(chunk_index_, "Chunk index not set");
        DCHECK(chunk_index_->CheckIndeces(), "Chunk index not initialized correctly");

        b = chunk_index_->ImportContainerItem(item_, ec_);
        delete this;
        return b;
    }

    void Close() {
        delete this;
    }
};

bool ChunkIndex::ImportContainerItem(const ContainerItem& item, ErrorContext* ec) {
    DCHECK(this->chunk_index_, "Persistent chunk index not set");
    DCHECK(item.is_indexed(), "Item should not be indexed");

    ChunkMapping mapping(item.key(), item.key_size());
    mapping.set_data_address(item.original_id());

    DEBUG("Import container item: " << item.DebugString());
    bool is_still_pinned = false;
    put_result pr = EnsurePersistent(mapping, &is_still_pinned);
    CHECK(pr != PUT_ERROR, "Failed to ensure that container item is persisted: " <<
        item.DebugString());
    if (is_still_pinned) {
        // I am sure that the container is committed
        CHECK(chunk_index_->ChangePinningState(item.key(), item.key_size(), false) != LOOKUP_ERROR,
            "Failed to changed pinning state: " << item.DebugString());
        put_result pr = EnsurePersistent(mapping, &is_still_pinned);
        CHECK(pr != PUT_ERROR, "Failed to ensure that container item is persisted: " <<
            item.DebugString());
        CHECK(!(pr == PUT_KEEP && is_still_pinned), "Item should not still be pinned");
    }
    if (pr == PUT_KEEP) {
        DEBUG("Item was not dirty in chunk index: " << item.DebugString());
    }
    TRACE("Finished importing container item: " << item.DebugString());
    return true;
}

bool ChunkIndex::ImportContainerParallel(uint64_t container_id, const Container& container,
                                         dedupv1::base::ErrorContext* ec) {
    bool failed = false;

    list<Future<bool>*> futures;
    vector<ContainerItem*>::const_iterator i;
    // TODO(fermat): We should replace the futures by a barrier.

    for (i = container.items().begin(); i != container.items().end(); i++) {
        ContainerItem* item = *i;
        if (item) {
            if (item->original_id() != container_id) {
                // This happens after a merge.
                TRACE("Skip item: " << item->DebugString() << ", import container id " << container_id);
                continue;
            }
            if (item->is_deleted()) {
                TRACE("Container item is deleted: " << item->DebugString());
                continue;
            }
            if (!item->is_indexed()) {
                TRACE("Container item should not be indexed: " << item->DebugString());
                continue;
            }
            ImportTask* task = new ImportTask(this, *item, ec); // tp takes care of deleting it
            if (!task) {
                ERROR("Failed to create task: item " << item->DebugString());
                failed = true;
                continue;
            }
            Future<bool>* future = tp_->Submit(task, Threadpool::BACKGROUND_PRIORITY, Threadpool::CALLER_RUNS);
            if (!future) {
                ERROR("Failed to submit import task: item " << item->DebugString());
                task->Close();
                task = NULL;
                failed = true;
                continue;
            }
            futures.push_back(future);
        }
    }

    TRACE("Wait for import task: " << container.DebugString());
    for (list<Future<bool>*>::iterator j = futures.begin(); j != futures.end(); ++j) {
        Future<bool>* future = *j;
        CHECK(future, "Future not set");
        bool b = future->Wait();
        if (!b) {
            WARNING("Failed to wait for import task execution: container id " << container_id);
            failed = true;
        } else if (future->is_abort()) {
            WARNING("Import task was aborted: container id " << container_id);
            failed = true;
        } else {
            bool result = false;
            future->Get(&result);
            if (unlikely(!result)) {
                failed = true;
            }
        }
        if (!future->Close()) {
            WARNING("Failed to close import task future");
        }
    }
    futures.clear();
    // waited for all
    CHECK(!failed, "Import container failed: " << container.DebugString());
    return !failed;
}

bool ChunkIndex::LoadContainerIntoCache(uint64_t container_id,
                                        dedupv1::base::ErrorContext* ec) {
    ContainerStorage* container_storage = dynamic_cast<ContainerStorage*>(this->storage_);
    DCHECK(container_storage, "Storage is no container storage");

    storage_commit_state commit_state = container_storage->IsCommittedWait(container_id);
    CHECK(commit_state != STORAGE_ADDRESS_ERROR, "Failed to check commit state: " << container_id);
    if (commit_state == STORAGE_ADDRESS_WILL_NEVER_COMMITTED) {
        DEBUG("Missing container for import: " <<
            "container id " << container_id <<
            ", commit state: will never be committed"
            ", last given container id " << (container_storage != NULL ? ToString(container_storage->GetLastGivenContainerId()) : ""));
        INFO("Missing container for import: " <<
            "container id " << container_id);
        return true;  // leave method
    } else if (commit_state == STORAGE_ADDRESS_NOT_COMMITED) {
        WARNING("Missing container for import: " <<
            "container id " << container_id <<
            ", commit state: not committed" <<
            ", dirty import container tracker " << dirty_import_container_tracker_.DebugString() <<
            ", last given container id " << (container_storage != NULL ? ToString(
                                                 container_storage->GetLastGivenContainerId()) : ""));
        return true; // leave method
    }
    // is committed

    TRACE("Load container " << container_id << " from log into cache (loading)");
    Container container;
    CHECK(container.InitInMetadataOnlyMode(container_id, container_storage->GetContainerSize()), "Container init failed");

    enum lookup_result read_result = container_storage->ReadContainerWithCache(&container);
    CHECK(read_result != LOOKUP_ERROR,
        "Could not read container for import: " <<
        "container id " << container_id <<
        ", container " << container.DebugString());
    if (read_result == LOOKUP_NOT_FOUND) {
        WARNING("Could find container for import: " << "container " << container.DebugString() <<
            ", last given container id " << (container_storage != NULL ? ToString(
                                                 container_storage->GetLastGivenContainerId()) : ""));
        return true;
    }

    // found
    DEBUG("Load container from log into cache: " << container.DebugString() <<
        ", current container id " << container_id);

    vector<ContainerItem*>::const_iterator i;
    for (i = container.items().begin(); i != container.items().end(); i++) {
        ContainerItem* item = *i;
        DCHECK(item, "Item not set");
        if (item->is_deleted()) {
            TRACE("Container item is deleted: " << item->DebugString());
            continue;
        }
        if (item->original_id() != container_id) {
            TRACE("Skip item: " << item->DebugString() << ", import container id " << container_id);
            continue;
        }
        if (!item->is_indexed()) {
            TRACE("Skip item: " << item->DebugString() << ", not indexed");
            continue;
        }
        TRACE("Load container item: " << item->DebugString());

        // the lookup is not essential, but it should not take a long time.
        ChunkMapping mapping(item->key(), item->key_size());
        lookup_result result = LookupPersistentIndex(&mapping,
            dedupv1::base::CACHE_LOOKUP_ONLY,
            dedupv1::base::CACHE_ALLOW_DIRTY, ec);
        CHECK(result != LOOKUP_ERROR, "Failed to search for chunk mapping in cache: " << mapping.DebugString())
        if (result == LOOKUP_FOUND) {
            // The usage count is absolutely wrong here.
            // It must be corrected using a background garbage collecting process. Therefore it is safe to ignore the usage count here.
            // It has to be zero.

            // When this container has not been imported before, there can be no block write event that is replayed
            // before. Therefore the id has to be zero.

            // every container is imported at least once, but if container has been merged before
            // they are imported a given item might be imported multiple times. This is avoided
            // here.
            if (container_id != mapping.data_address()) {
                TRACE("Container item was imported before: item " << item->DebugString() << ", imported container id "
                                                                  << container_id << ", mapping " << mapping.DebugString());
            }
            CHECK(chunk_index_->ChangePinningState(item->key(), item->key_size(), false),
                "Failed to change pinning state: " << item->DebugString());
        } else {
            DEBUG("We have a item from a non-imported container that is unused: " <<
                item->DebugString());

            result = LookupPersistentIndex(&mapping,
                dedupv1::base::CACHE_LOOKUP_BYPASS,
                dedupv1::base::CACHE_ALLOW_DIRTY, ec);
            CHECK(result != LOOKUP_ERROR, "Failed to search for chunk mapping in chunk index: " << mapping.DebugString())
            if (result == LOOKUP_FOUND) {
                DEBUG("Item was imported before: " << item->DebugString());
            } else {
                DEBUG("Add empty item into chunk index: " << item->DebugString());
                // When this container has not been imported before, there can be no block write event that is replayed
                // before. Therefore the id has to be zero.

                // Here we have to use the original container id as otherwise
                // already written block mapping entries and the chunk index entry
                // get ouf of sync of the container id: Container id mismatch
                mapping.set_data_address(item->original_id());

                CHECK(PutPersistentIndex(mapping, false, false, ec),
                    "Cannot put logged mapping into persistent index (dirty): " << mapping.DebugString() <<
                    ", container item " << item->DebugString());
            }
        }
    }

    DEBUG("Finished loading container " << container_id << " from log into cache: "
                                        << container.DebugString());

    return true;
}

bool ChunkIndex::ImportContainer(uint64_t container_id, dedupv1::base::ErrorContext* ec) {
    // TODO(fermat): we do no more support any other types of Storage as Container Storage. Therefore we should remove this abstraction.
    ContainerStorage* container_storage = dynamic_cast<ContainerStorage*>(this->storage_);
    DCHECK(container_storage, "Storage is no container storage");

    storage_commit_state commit_state = container_storage->IsCommittedWait(container_id);

    CHECK(commit_state != STORAGE_ADDRESS_ERROR,
        "Failed to check commit state: " << container_id);
    CHECK(commit_state != STORAGE_ADDRESS_NOT_COMMITED, "Missing container for import: " <<
        "container id " << container_id <<
        ", commit state: not committed"
        ", last given container id " << (container_storage != NULL ? ToString(container_storage->GetLastGivenContainerId()) : ""));
    if (commit_state == STORAGE_ADDRESS_WILL_NEVER_COMMITTED) {
        WARNING("Missing container for import: " << "container id " << container_id
                                                 << ", commit state: will never be committed"
            ", last given container id " << (container_storage != NULL ? ToString(
                                                 container_storage->GetLastGivenContainerId()) : ""));
        // This can happen if the system crashed before the container was committed.
        // In this case we can no more restore the chunk and throw it away. During
        // the lock replay the block mappings will also be rewinded, so that the chunk
        // is not referenced.
        return true; // leave method
    }
    // is committed

    FAULT_POINT("chunk-index.import.pre");

    if (chunk_index_->GetDirtyItemCount() > 0) {
        TRACE("Import container " << container_id << " from log (loading)");
        Container container;
        CHECK(container.InitInMetadataOnlyMode(container_id, container_storage->GetContainerSize()), "Container init failed");

        enum lookup_result read_result = container_storage->ReadContainerWithCache(&container);
        CHECK(read_result != LOOKUP_ERROR,
            "Could not read container for import: " <<
            "container id " << container_id <<
            ", container " << container.DebugString());
        if (read_result == LOOKUP_NOT_FOUND) {
            // This should not happen, as we checked before that it is committed.
            WARNING("Could find container for import: " << "container " << container.DebugString()
                                                        << ", last given container id " << (container_storage != NULL ? ToString(
                                                        container_storage->GetLastGivenContainerId()) : ""));
            return true;
        }

        // found
        INFO("Import container: " << container.DebugString());

        CHECK(ImportContainerParallel(container_id, container, ec),
            "Failed to import container: " <<
            "container id " << container_id <<
            ", container " << container.DebugString());

        DEBUG("Finished importing container " << container_id << " from log (count " << container.item_count() << ")");
    } else {
        TRACE("Import container " << container_id << " from log (skipping, all clean)");
    }
    // update chunk index meta data
    CHECK(this->lock_.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_busy_), "Failed to acquire chunk index lock");
    this->container_tracker_.ProcessedContainer(container_id);
    this->stats_.imported_container_count_.fetch_and_increment();

    // We dump there the chunk index meta info
    // This makes sure that a container is never imported twice with a long time between the imports. That would kill the usage counting.
    // If the system crashes between the import and the dumping, the import is done twice, but that is no problem.
    // An alternative would be checking if a chunk is already imported during the dirty store, but this increases the dirty restart time.
    // One IO more or less here is not critical.
    CHECK(DumpMetaInfo(), "Failed to dump meta info");

    CHECK(this->lock_.ReleaseLock(), "Failed to release chunk index lock");
    FAULT_POINT("chunk-index.import.post");
    return true;
}

bool ChunkIndex::HandleContainerCommit(const ContainerCommittedEventData &event_data) {
    DEBUG("Process committed container: " << event_data.container_id());
    uint64_t container_id = event_data.container_id();

    // the container tracker is now aware that such a container exists
    CHECK(this->lock_.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_busy_), "Failed to acquire chunk index lock");
    this->container_tracker_.ShouldProcessContainer(container_id);
    CHECK(this->lock_.ReleaseLock(), "Failed to release chunk index lock");

    return true;
}

bool ChunkIndex::HandleContainerCommitFailed(const ContainerCommitFailedEventData &event_data) {
    DEBUG("Process failed container: " << event_data.container_id());

    bool failed = false;
    for (int i = 0; i < event_data.item_key_size() && !failed; i++) {
        DEBUG("Process failed container item: " << "container id " << event_data.container_id() << ", key "
                                                << Fingerprinter::DebugString(event_data.item_key(i)));
        ChunkMapping chunk_mapping(make_bytestring(event_data.item_key(i)));
        lookup_result r = LookupPersistentIndex(&chunk_mapping,
            dedupv1::base::CACHE_LOOKUP_ONLY,
            dedupv1::base::CACHE_ALLOW_DIRTY, NO_EC);
        if (r == LOOKUP_ERROR) {
            ERROR("Failed to lookup chunk mapping: " << chunk_mapping.DebugString());
            failed = true;
        } else if (r == LOOKUP_NOT_FOUND) {
            WARNING("Cannot find item of failed container in cache: " <<
                "container id " << event_data.container_id() <<
                ", key " << Fingerprinter::DebugString(event_data.item_key(i)));
        } else if (/*r == LOOKUP_FOUND && */ chunk_mapping.data_address() == event_data.container_id()) {
            TRACE("Delete from chunk index: " << chunk_mapping.DebugString());
            delete_result dr = this->chunk_index_->Delete(chunk_mapping.fingerprint(),
                chunk_mapping.fingerprint_size());
            if (dr == DELETE_ERROR) {
                ERROR("Failed to delete item of failed container: " << "container id " << event_data.container_id()
                                                                    << ", key " << Fingerprinter::DebugString(event_data.item_key(i)));
                failed = true;
            }
        }
    }
    return !failed;
}

Option<bool> ChunkIndex::IsContainerImported(uint64_t container_id) {
    CHECK(this->lock_.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_busy_), "Failed to acquire chunk index lock");
    bool shouldImported = this->container_tracker_.ShouldProcessContainerTest(container_id);
    CHECK_RETURN(this->lock_.ReleaseLock(), IMPORT_ERROR, "Failed to release chunk index lock");
    return make_option(!shouldImported);
}

enum lookup_result ChunkIndex::ChangePinningState(const void* key, size_t key_size, bool new_pin_state) {
    DCHECK_RETURN(chunk_index_, LOOKUP_ERROR, "Chunk index not set");

    return chunk_index_->ChangePinningState(key, key_size, new_pin_state);
}

enum put_result ChunkIndex::EnsurePersistent(const ChunkMapping &mapping, bool* pinned) {
    DCHECK_RETURN(chunk_index_, PUT_ERROR, "Chunk index not set");

    DEBUG("Ensure persistence: "
        "chunk " << mapping.DebugString());

    bool is_still_pinned = false;
    put_result pr;
    pr = chunk_index_->EnsurePersistent(mapping.fingerprint(),
        mapping.fingerprint_size(), &is_still_pinned);
    CHECK_RETURN(pr != PUT_ERROR, PUT_ERROR, "Failed to persist chunk mapping: " << mapping.DebugString());

    if (pr == PUT_KEEP && is_still_pinned) {
        DEBUG("Mapping still pinned: " << mapping.DebugString());
    } else if (pr == PUT_KEEP) {
        DEBUG("Mapping wasn't dirty in cache: " << mapping.DebugString());
    } else {
        // it is persisted, we are fine here
    }
    if (pinned) {
        *pinned = is_still_pinned;
    }
    return pr;
}

bool ChunkIndex::FinishDirtyLogReplay() {
    if (dirty_import_container_exists_) {
        INFO("Load non-imported container in cache");

        // The container tracker has to contain all possible container ids that
        // are not imported into the persistent index.
        // All these containers should know (at least we should try it) imported into the cache
        dirty_import_container_tracker_.CopyFrom(container_tracker_);
        DEBUG("Load non-imported container in cache: " << dirty_import_container_tracker_.DebugString());

        // we are now loading all non-processed the containers in main memory
        uint64_t container_id = dirty_import_container_tracker_.GetNextProcessingContainer();
        while (container_id != Storage::ILLEGAL_STORAGE_ADDRESS) {
            CHECK(dirty_import_container_tracker_.ProcessingContainer(container_id),
                "Failed to mark container as in processing for cache import: " <<
                "container id " << container_id);

            DEBUG("Load container into cache: container id " << container_id);

            CHECK(this->LoadContainerIntoCache(container_id, NO_EC),
                "Failed to load container: container id " << container_id);
            CHECK(dirty_import_container_tracker_.ProcessedContainer(container_id),
                "Failed to mark container as processed: container id " << container_id);
            container_id = dirty_import_container_tracker_.GetNextProcessingContainer();
        }
        dirty_import_container_tracker_.Clear();
    }
    // Everything still pinned at this point in time has to go
    this->chunk_index_->DropAllPinned();
    dirty_import_finished_ = true;
    return true;
}

bool ChunkIndex::LogReplay(dedupv1::log::event_type event_type, const LogEventData &event_value,
                           const dedupv1::log::LogReplayContext & context) {
    ProfileTimer timer(this->stats_.replay_time_);
    FAULT_POINT("chunk-index.log.pre");
    CHECK(in_combats_.LogReplay(event_type, event_value, context), "Failed to replay event in in combats");
    if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_OPEN) {
        CHECK(state_ != STOPPED, "Failed to replay log event: chunk index already stopped");
        ContainerOpenedEventData event_data = event_value.container_opened_event();
        uint64_t container_id = event_data.container_id();

        if (context.replay_mode() == dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START) {
            TRACE("Handle opened container (dirty start): " << "" << event_data.ShortDebugString() << ", event log id "
                                                            << context.log_id());

            // in dirty replay, we have to check if the container is already imported. This also updates the tracker state
            CHECK(this->lock_.AcquireLock(), "Failed to acquire chunk index lock");
            bool shouldProcessContainer = this->container_tracker_.ShouldProcessContainer(container_id);
            CHECK(this->lock_.ReleaseLock(), "Failed to release chunk index lock");

            if (shouldProcessContainer) {
                TRACE("Mark as container to import: container id " << container_id);
                dirty_import_container_exists_ = true;
            }
        } else if (context.replay_mode() == dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG) {
            // I (dmeister) really tried to move this to the container commit event, but it is really hard to
            // do. A major issue is when block mapping written events are replayed before the importing. It
            // is much easier when the garantues an importinging before and block mapping written event
            // is replayed.

            CHECK(state_ != STOPPED, "Failed to replay log event: chunk index already stopped");
            ContainerOpenedEventData event_data = event_value.container_opened_event();
            uint64_t container_id = event_data.container_id();

            DEBUG("Replay container open (bg replay): " <<
                "container id " << container_id <<
                ", event data " << event_data.ShortDebugString() <<
                ", event log id " << context.log_id());

            // in BACKGROUND REPLAY, we have to check if the container is already imported.
            bool repeat_check = true;
            bool shouldProcessContainer = false;
            int repeat_count = 0;
            for (; repeat_check && repeat_count < 300; repeat_count++) { // try this at most 300 times
                repeat_check = false; // default case: We leave the loop
                CHECK(this->lock_.AcquireLock(), "Failed to acquire chunk index lock");
                shouldProcessContainer = this->container_tracker_.ShouldProcessContainer(container_id);
                if (shouldProcessContainer) {
                    if (this->container_tracker_.IsProcessingContainer(container_id)) {
                        // TODO(fermat): If this is not very rarely we could use a condition variable....
                        this->stats_.bg_container_import_wait_count_++;

                        // a concurrent import thread is currently importing the container
                        // we cannot simply assume that we other thread will do the right thing, because the system
                        // might crash a ms after the log replay before the other thread finished. We have to assure
                        // that the container is imported when this log replay have been replayed.
                        repeat_check = true;
                    } else {
                        this->container_tracker_.ProcessingContainer(container_id);
                    }
                }
                CHECK(this->lock_.ReleaseLock(), "Failed to release chunk index lock");
                if (repeat_check) {
                    // wait before a new iteration
                    ThreadUtil::Sleep(1);
                }
            }
            if (repeat_count == 300 && repeat_check) {
                // we waited 300 seconds someone is still importing the container.
                // a normal import takes currently around a second at most.
                // we can assume that something went wrong if we see no progress for 30 seconds.
                ERROR("Failed to import container. Concurrent import starved or failed: container id " << container_id);
                return false;
            }
            if (shouldProcessContainer) {
                CHECK(this->ImportContainer(container_id, NO_EC),
                    "Failed to import container: container id " << container_id);
            } else {
                TRACE("Not necessary to import container " << container_id << ", tracker "
                                                           << this->container_tracker_.DebugString());
            }
        } else {
            // EVENT_REPLAY_MODE_DIRECT
            TRACE("Handle opened container (direct): " << "container id " << container_id << ", event log id "
                                                       << context.log_id());
        }
    } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_COMMITED && context.replay_mode()
               == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        CHECK(state_ != STOPPED, "Failed to replay log event: chunk index already stopped");
        // Direct container is committed

        ContainerCommittedEventData event_data = event_value.container_committed_event();

        DEBUG("Replay container committed (direct): " <<
            "container id " << event_data.container_id() <<
            ", event data " << event_data.ShortDebugString() <<
            ", event log id " << context.log_id());

        CHECK(HandleContainerCommit(event_data),  "Failed to handle commit event data: " << event_data.ShortDebugString());
    } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_COMMIT_FAILED && context.replay_mode()
               == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        CHECK(state_ != STOPPED, "Failed to replay log event: chunk index already stopped");
        // Direct container failed
        ContainerCommitFailedEventData event_data = event_value.container_commit_failed_event();

        CHECK(HandleContainerCommitFailed(event_data),
            "Failed to handle commit event data: " << event_data.ShortDebugString());
    } else if (event_type == dedupv1::log::EVENT_TYPE_REPLAY_STOPPED && context.replay_mode()
               == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        ReplayStopEventData event_data = event_value.replay_stop_event();

        if (event_data.replay_type() == dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START) {
            // dirty replay finished

            if (event_data.success()) {
                CHECK(FinishDirtyLogReplay(), "Failed to finish dirty log replay");
            } else if (!event_data.success()) {
                WARNING("Skip loading container into cache: log replay failed");
            }
        }

        CHECK(this->DumpMetaInfo(), "Cannot dump chunk index meta data");
        is_replaying_ = false;
    } else if (event_type == dedupv1::log::EVENT_TYPE_REPLAY_STARTED && context.replay_mode()
               == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        is_replaying_ = true;
    } else if (event_type == dedupv1::log::EVENT_TYPE_LOG_EMPTY && context.replay_mode()
               == dedupv1::log::EVENT_REPLAY_MODE_DIRECT) {
        // we reset the container tracker when the log is empty, because we than can be sure that there will be
        // no import request for smaller containers.
        CHECK(this->lock_.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_busy_), "Failed to acquire chunk index lock");

        string container_tracker_debug_string = container_tracker_.DebugString();
        CHECK(this->container_tracker_.Reset(), "Cannot reset container tracker");

        DEBUG("Reset tracker: before " << container_tracker_debug_string << ", after "
                                       << container_tracker_.DebugString());
        CHECK(this->lock_.ReleaseLock(), "Failed to release chunk index lock");
    }
    return true;
}

Option<bool> ChunkIndex::DoImport(uint64_t container_id) {
    enum storage_commit_state commit_state = this->storage_->IsCommitted(container_id);
    CHECK(commit_state != STORAGE_ADDRESS_ERROR, "Failed to check commit state of container: " << container_id);

    if (commit_state == STORAGE_ADDRESS_NOT_COMMITED) {
        TRACE("Skip importing container " << container_id << ": not yet committed");

        // we declared that we are processing it. We don't have processed it yet
        return make_option(false);
    }
    if (commit_state == STORAGE_ADDRESS_WILL_NEVER_COMMITTED) {
        // the address is not committed, but also never will be committed as it has been opened
        // in a previous run.
        TRACE("Skip importing container " << container_id << ": not yet committed and never will be");
        return make_option(true);
    }
    if (commit_state == STORAGE_ADDRESS_COMMITED) {
        TRACE("Import container " << container_id); // a debug message is logged inside ImportContainer

        CHECK(this->ImportContainer(container_id, NO_EC),
            "Import of container id " << container_id << " failed");
        return make_option(true);
    }
    ERROR("Illegal commit state: container id " << container_id << ", state " << commit_state);
    return false;
}

ChunkIndex::import_result ChunkIndex::TryImportDirtyChunks(uint64_t * resume_handle) {
    ProfileTimer timer(this->stats_.import_time_);

    bool should_import = (import_if_replaying_ && is_replaying_ && this->chunk_index_->GetDirtyItemCount() > 0);
    if (!should_import) {
        should_import = (this->chunk_index_->GetDirtyItemCount() > dirty_chunk_count_threshold_);
    }
    if (!should_import) {
        if (unlikely(has_reported_importing_)) {
            has_reported_importing_ = false;
            INFO("Chunk index importing stopped: " <<
                "replaying state " << ToString(is_replaying_) <<
                ", dirty item count " << chunk_index_->GetDirtyItemCount() <<
                ", total item count " << chunk_index_->GetTotalItemCount() <<
                ", dirty chunk count threshold " << dirty_chunk_count_threshold_ <<
                ", persistent item count " << chunk_index_->GetItemCount());
        }
        return IMPORT_NO_MORE;
    }
    if (!has_reported_importing_.compare_and_swap(true, false)) {
        INFO("Chunk index importing started: " <<
            "replaying state " << ToString(is_replaying_) <<
            ", dirty item count " << chunk_index_->GetDirtyItemCount() <<
            ", total item count " << chunk_index_->GetTotalItemCount() <<
            ", dirty chunk count threshold " << dirty_chunk_count_threshold_ <<
            ", persistent item count " << chunk_index_->GetItemCount());
    }

    bool persisted_page = false;
    CHECK_RETURN(chunk_index_->TryPersistDirtyItem(128, resume_handle, &persisted_page),
        IMPORT_ERROR, "Failed to persist dirty items");

    if (persisted_page) {
        return IMPORT_BATCH_FINISHED;
    } else {
        return IMPORT_NO_MORE;
    }
}

ChunkIndex::import_result ChunkIndex::TryImportContainer() {
    ProfileTimer timer(this->stats_.import_time_);

    bool should_import = (import_if_replaying_ && is_replaying_);
    if (!should_import) {
        should_import = (this->chunk_index_->GetDirtyItemCount() > dirty_chunk_count_threshold_);
    }
    if (!should_import) {
        if (unlikely(has_reported_importing_)) {
            has_reported_importing_ = false;
            INFO("Chunk index importing stopped: " <<
                "replaying state " << ToString(is_replaying_) <<
                ", dirty item count " << chunk_index_->GetDirtyItemCount() <<
                ", total item count " << chunk_index_->GetTotalItemCount() <<
                ", dirty chunk count threshold " << dirty_chunk_count_threshold_ <<
                ", persistent item count " << chunk_index_->GetItemCount());
        }
        return IMPORT_NO_MORE;
    }

    dedupv1::base::ScopedLock scoped_lock(&lock_);
    CHECK_RETURN(scoped_lock.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_busy_), IMPORT_ERROR,
        "Failed to acquire chunk index lock");
    uint64_t next_processing_container_id = this->container_tracker_.GetNextProcessingContainer();
    if (next_processing_container_id == Storage::ILLEGAL_STORAGE_ADDRESS) {
        if (unlikely(has_reported_importing_)) {
            has_reported_importing_ = false;
            INFO("Chunk index importing stopped: " <<
                "replaying state " << ToString(is_replaying_) <<
                ", dirty item count " << chunk_index_->GetDirtyItemCount() <<
                ", total item count " << chunk_index_->GetTotalItemCount() <<
                ", dirty chunk count threshold " << dirty_chunk_count_threshold_ <<
                ", persistent item count " << chunk_index_->GetItemCount() <<
                ", reason no container left to import");
        }
        return IMPORT_NO_MORE;
    }
    this->container_tracker_.ProcessingContainer(next_processing_container_id);
    CHECK_RETURN(scoped_lock.ReleaseLock(), IMPORT_ERROR, "Failed to release chunk index lock");

    if (!has_reported_importing_.compare_and_swap(true, false)) {
        INFO("Chunk index importing started: " <<
            "replaying state " << ToString(is_replaying_) <<
            ", dirty item count " << chunk_index_->GetDirtyItemCount() <<
            ", total item count " << chunk_index_->GetTotalItemCount() <<
            ", dirty chunk count threshold " << dirty_chunk_count_threshold_ <<
            ", persistent item count " << chunk_index_->GetItemCount());
    }

    TRACE("Next processing container id " <<
        (next_processing_container_id == Storage::ILLEGAL_STORAGE_ADDRESS ? "<not set>" : ToString(
             next_processing_container_id)));

    // there is a container to import
    Option<bool> r = DoImport(next_processing_container_id);
    if (!r.valid()) {
        CHECK_RETURN(this->lock_.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_busy_), IMPORT_ERROR,
            "Failed to acquire chunk index lock");
        this->container_tracker_.AbortProcessingContainer(next_processing_container_id);
        CHECK_RETURN(this->lock_.ReleaseLock(), IMPORT_ERROR, "Failed to release chunk index lock");
        return IMPORT_ERROR;
    }
    if (r.value()) {
        // importing has been done
        CHECK_RETURN(this->lock_.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_busy_), IMPORT_ERROR,
            "Failed to acquire chunk index lock");
        this->container_tracker_.ProcessedContainer(next_processing_container_id);
        CHECK_RETURN(this->lock_.ReleaseLock(), IMPORT_ERROR, "Failed to release chunk index lock");
        return IMPORT_BATCH_FINISHED;
    } else {
        // there was no error, but we should abort the processing, e.g. the container is not yet committed
        CHECK_RETURN(this->lock_.AcquireLockWithStatistics(&this->stats_.lock_free_, &this->stats_.lock_busy_), IMPORT_ERROR,
            "Failed to acquire chunk index lock");
        this->container_tracker_.AbortProcessingContainer(next_processing_container_id);
        CHECK_RETURN(this->lock_.ReleaseLock(), IMPORT_ERROR, "Failed to release chunk index lock");
        return IMPORT_BATCH_FINISHED;
    }
}

void ChunkIndex::set_state(chunk_index_state new_state) {
    this->state_ = new_state;
}

Option<bool> ChunkIndex::Throttle(int thread_id, int thread_count) {
    ProfileTimer timer(this->stats_.throttle_time_);

    double item_count = this->chunk_index_->GetDirtyItemCount();
    double fill_ratio = item_count / (chunk_index_->GetEstimatedMaxCacheItemCount());
    double thread_ratio = 1.0 * thread_id / thread_count;
    Option<bool> r = throttling_.Throttle(fill_ratio, thread_ratio);
    if (r.valid() && r.value()) {
        this->stats_.throttle_count_++;
    }
    return r;
}

}
}
