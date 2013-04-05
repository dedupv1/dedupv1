/*
 * dedupv1 - iSCSI based Deduplication System for Linux
 *
 * (C) 2008-2009 Paderborn Center for Parallel Computing
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <sstream>

#include "dedupv1.pb.h"
#include "dedupv1_stats.pb.h"

#include <core/dedup.h>
#include <base/fileutil.h>
#include <base/index.h>
#include <base/logging.h>
#include <core/filter.h>
#include <base/hashing_util.h>
#include <base/locks.h>
#include <base/strutil.h>
#include <base/timer.h>
#include <core/log_consumer.h>
#include <core/log.h>
#include <core/dedup_system.h>
#include <core/chunk_store.h>
#include <core/storage.h>
#include <core/container_storage.h>
#include <core/container.h>

#include <core/bloom_filter.h>

using dedupv1::base::strutil::FormatLargeNumber;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::To;
using std::string;
using std::vector;
using std::set;
using std::stringstream;
using dedupv1::base::ProfileTimer;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::lookup_result;
using dedupv1::blockindex::BlockMapping;
using dedupv1::chunkstore::ContainerStorage;
using dedupv1::chunkstore::Container;
using dedupv1::chunkstore::ContainerItem;
using dedupv1::Session;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::BloomSet;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::base::ErrorContext;
using dedupv1::base::Option;
using dedupv1::chunkstore::Storage;

LOGGER("BloomFilter");

#define data_bytes(c) (((c)->size) / 8)
#define data_buckets(c) (((c)->size) / sizeof(uint32_t))

namespace dedupv1 {
namespace filter {

void BloomFilter::RegisterFilter() {
    Filter::Factory().Register("bloom-filter", &BloomFilter::CreateFilter);
}

Filter* BloomFilter::CreateFilter() {
    return new BloomFilter();
}

BloomFilter::BloomFilter() : Filter("bloom filter", FILTER_WEAK_MAYBE) {
    bloom_set_ = NULL;
    size_ = 0;
    filter_file_ = NULL;
}

BloomFilter::Statistics::Statistics() {
    reads_ = 0;
    writes_ = 0;
    weak_hits_ = 0;
    miss_ = 0;
}

BloomFilter::~BloomFilter() {
}

bool BloomFilter::SetOption(const string& option_name, const string& option) {
    CHECK(bloom_set_ == NULL, "Bloom filter already started");
    if (option_name == "size") {
        Option<int64_t> su = ToStorageUnit(option);
        CHECK(su.valid(), "Illegal size: " << option);
        CHECK(su.value() % 32 == 0, "Illegal size: " << option);
        CHECK(su.value() > 0, "Illegal size: " << option);
        this->size_ = su.value();
        return true;
    }
    if (option_name == "filename") {
        CHECK(option.size() <= 255, "Filter filename to long");
        this->filter_filename_ = option;
        return true;
    }
    return Filter::SetOption(option_name, option);
}

bool BloomFilter::Start(DedupSystem* system) {
    DCHECK(system, "System not set");
    INFO("Starting bloom filter");
    INFO("Usage of the bloom filter is unsafe and is only advised for research and development");

    CHECK(this->size_ > 0 && this->filter_filename_.size() > 0,
        "Bloom filter not configured");

    this->bloom_set_ = BloomSet::NewOptimizedBloomSet(size_, 0.01);
    CHECK(this->bloom_set_, "Failed to alloc bloom set");
    CHECK(this->bloom_set_->Init(), "Failed to init bloom set");
    INFO("Bloom filter configured: k " << bloom_set_->hash_count() <<
        ", size " << bloom_set_->byte_size());
    this->filter_file_ =  dedupv1::base::File::Open(
        this->filter_filename_, O_RDWR, 0);
    if (!this->filter_file_) {
        INFO("Create new bloom filter file " << this->filter_filename_);
        // file not existing;
        this->filter_file_ =  dedupv1::base::File::Open(
            this->filter_filename_, O_RDWR | O_CREAT,
            S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        CHECK(this->filter_file_, "Cannot create filter file");
        CHECK(this->DumpData(), "Cannot write filter file");
    } else { // file already existing
        INFO("Open existing bloom filter file " << this->filter_filename_);
        CHECK(this->ReadData(), "Cannot read bloom filter");
    }
    return true;
}

bool BloomFilter::ReadData() {
    CHECK(this->filter_file_, "File not set");

    CHECK(this->filter_file_->Read(0,
            this->bloom_set_->mutable_data(), this->bloom_set_->byte_size()),
        "Cannot read bloom filter data");
    return true;
}

bool BloomFilter::DumpData() {
    CHECK(this->filter_file_, "File not set");
    DEBUG("Dump bloom filter data: size " << bloom_set_->byte_size() <<
        ",k " << bloom_set_->hash_count());

    CHECK(this->filter_file_->Write(0,
            this->bloom_set_->data(), this->bloom_set_->byte_size())
        == (ssize_t) this->bloom_set_->byte_size(),
        "Cannot write filter data file");
    return true;
}

bool BloomFilter::Update(Session* session,
                         const BlockMapping* block_mapping,
                         ChunkMapping* mapping,
                         ErrorContext* ec) {
    CHECK(mapping, "Mapping not set");
    CHECK(bloom_set_, "Bloom set not set");

    ProfileTimer timer(this->stats_.time_);
    TRACE("Update bloom filter: " << mapping->DebugString());
    this->stats_.writes_++;

    CHECK(this->bloom_set_->Put(mapping->fingerprint(),
            mapping->fingerprint_size()),
        "Cannot update bloom filter: mapping " << mapping->DebugString());
    return true;
}

Filter::filter_result BloomFilter::Check( Session* session,
                                          const BlockMapping* block_mapping,
                                          ChunkMapping* mapping,
                                          ErrorContext* ec) {
    CHECK_RETURN(mapping, FILTER_ERROR, "Chunk mapping not set");
    CHECK_RETURN(bloom_set_, FILTER_ERROR, "Bloom set not set");
    ProfileTimer timer(this->stats_.time_);

    TRACE("Check bloom filter: " << mapping->DebugString());
    this->stats_.reads_++;

    // we know that the zero-chunk is stored
    if (Fingerprinter::IsEmptyDataFingerprint(mapping->fingerprint(), mapping->fingerprint_size())) {
        stats_.weak_hits_++;
        return FILTER_WEAK_MAYBE;
    }
    lookup_result lr = this->bloom_set_->Contains(mapping->fingerprint(),
        mapping->fingerprint_size());
    CHECK_RETURN(lr != LOOKUP_ERROR, FILTER_ERROR, "Failed to lookup bloom set");
    if (lr == LOOKUP_NOT_FOUND) {
        this->stats_.miss_++;
        return FILTER_NOT_EXISTING;
    } else {
        this->stats_.weak_hits_++;
        return FILTER_WEAK_MAYBE;
    }
}

bool BloomFilter::Close() {
    INFO("Closing bloom filter");
    if (this->filter_file_ && bloom_set_) {
        if (!this->DumpData()) {
            WARNING("Cannot write filter file");
        }
    }
    delete this->bloom_set_;
    this->bloom_set_ = NULL;
    if (this->filter_file_) {
        if (!this->filter_file_->Close()) {
            WARNING("Failed to close bloom filter file");
        }
        this->filter_file_ = NULL;
    }
    return Filter::Close();
}

bool BloomFilter::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    BloomFilterStatsData data;
    data.set_hit_count(stats_.weak_hits_);
    data.set_miss_count(stats_.miss_);
    data.set_read_count(stats_.reads_);
    data.set_write_count(stats_.writes_);
    CHECK(ps->Persist(prefix, data),
        "Failed to persist bloom filter stats");
    return true;
}

bool BloomFilter::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    BloomFilterStatsData data;
    CHECK(ps->Restore(prefix, &data),
        "Failed to restore bloom filter stats");
    stats_.reads_ = data.read_count();
    stats_.writes_ = data.write_count();
    stats_.miss_ = data.miss_count();
    stats_.weak_hits_ = data.hit_count();
    return true;
}

string BloomFilter::PrintStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"reads\": \"" << this->stats_.reads_ << "\"," << std::endl;
    sstr << "\"writes\": \"" << this->stats_.writes_ << "\"," << std::endl;
    sstr << "\"weak\": \"" << this->stats_.weak_hits_ << "\"," << std::endl;
    sstr << "\"miss\": \"" << this->stats_.miss_ << "\"" << std::endl;
    sstr << "}";
    return sstr.str();
}

string BloomFilter::PrintProfile() {
    stringstream sstr;
    sstr << "\"" << this->stats_.time_.GetSum() << "\"" << std::endl;
    return sstr.str();
}

}
}

