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
#include <base/fixed_index.h>
#include <base/index.h>
#include <base/base.h>
#include <base/hashing_util.h>
#include <base/locks.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/timer.h>
#include <base/fileutil.h>
#include <base/crc32.h>
#include <base/bitutil.h>
#include <base/protobuf_util.h>

#include "dedupv1_base.pb.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <tcutil.h>
#include <tcfdb.h>
#include <stdint.h>

#include <sstream>

using std::string;
using std::stringstream;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::To;
using dedupv1::base::crc_raw;
using dedupv1::base::crc;
using dedupv1::base::ProfileTimer;
using dedupv1::base::File;
using google::protobuf::Message;

LOGGER("FixedIndex");

namespace dedupv1 {
namespace base {

void FixedIndex::RegisterIndex() {
    Index::Factory().Register("disk-fixed", &FixedIndex::CreateIndex);
}

Index* FixedIndex::CreateIndex() {
    return new FixedIndex();
}

FixedIndex::FixedIndex() : IDBasedIndex(NO_CAPABILITIES) {
    this->width = kDefaultWidth;
    this->size = kDefaultSize;
    this->version_counter = 0;
    this->state = FIXED_INDEX_STATE_CREATED;
    bucket_size = 0;
}

bool FixedIndex::SetOption(const string& option_name, const string& option) {
    if (option_name == "filename") {
        CHECK(option.size() < 1024, "Illegal filename");
        this->filename.push_back(option);
        return true;
    }
    if (option_name == "width") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->width = ToStorageUnit(option).value();
        CHECK(this->width > 0, "Illegal width: " << option);
        return true;
    }
    if (option_name == "size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->size = ToStorageUnit(option).value();
        CHECK(this->size > 0, "Illegal size: " << option);
        return true;
    }
    return Index::SetOption(option_name, option);
}

bool FixedIndex::CheckFileSuperBlock(File* file) {
    CHECK(file, "File not set");

    DEBUG("Check super block: file " << file->path());

    FixedIndexMetaData super_data;
    CHECK(file->ReadSizedMessage(0, &super_data, this->bucket_size, true), "Failed to read super block");

    CHECK(super_data.width() == width, "Width changed: width " << super_data.width() << ", configured width " << width);
    CHECK(super_data.size() == size, "Size changed: size " << super_data.size() << ", configured size " << size);
    CHECK(super_data.file_count() == this->filename.size(), "File count changed: file count " << super_data.file_count() <<
        ", configured file count " << this->filename.size());

    DEBUG("Found super block: " << super_data.ShortDebugString());

    return true;
}

bool FixedIndex::Format(File* file) {
    CHECK(file, "File not set");
    DEBUG("Format file " << file->path());

    CHECK(file->Fallocate(0, this->size / this->filename.size()),
        "File allocate failed: " << file->path());

    FixedIndexMetaData super_data;
    super_data.set_width(this->width);
    super_data.set_size(this->size);
    super_data.set_file_count(this->filename.size());
    CHECK(file->WriteSizedMessage(0, super_data, this->bucket_size, true) > 0, "Failed to write file super block");

    return true;
}

bool FixedIndex::Start(const StartContext& start_context) {
    CHECK(this->state == FIXED_INDEX_STATE_CREATED, "Index in invalid state");
    CHECK(this->filename.size() > 0, "No filename specified");
    CHECK(this->size > 0, "Index size not configured");
    CHECK((this->size % this->filename.size()) == 0, "Size " << this->size << " illegal for " << this->filename.size() << " files");

    // align on full sectors

    int b = bits(this->width + 24);
    this->bucket_size = pow(2, b);

    CHECK(size % this->bucket_size == 0, "Index size not aligned with bucket size: " <<
        "width " << this->width << ", bucket size " << this->bucket_size << ", total size " << this->size);

    int io_flags = O_RDWR | O_LARGEFILE | O_SYNC;

    this->files.resize(this->filename.size());
    for (int i = 0; i < this->files.size(); i++) {
        this->files[i] = NULL;
    }

    for (int i = 0; i < this->files.size(); i++) {
        this->files[i] = File::Open(this->filename[i], io_flags, 0);
        if (!this->files[i]) {
            CHECK(start_context.create(), "Failed to open index file " << this->filename[i]);

            INFO("Creating index file " << this->filename[i]);

            CHECK(File::MakeParentDirectory(this->filename[i], start_context.dir_mode().mode()),
                "Failed to check parent directories");

            // We open the file without the O_SYNC flag to increase the speed for the formatting.
            File* format_file = File::Open(this->filename[i], O_CREAT | O_RDWR | O_LARGEFILE, S_IRUSR | S_IWUSR | S_IRGRP);
            CHECK(format_file, "Error opening storage file: " << this->filename[i] << ", message " << strerror(errno));

            CHECK(Format(format_file), "Failed to format file " << this->filename[i]);
            delete format_file;
            format_file = NULL;

            this->files[i] = File::Open(this->filename[i], io_flags, 0);
            CHECK(this->files[i], "Error opening storage file: " << this->filename[i] << ", message " << strerror(errno));

            if (start_context.create()) {
                CHECK(chmod(this->filename[i].c_str(), start_context.file_mode().mode()) == 0,
                    "Failed to change file permissions: " << this->filename[i]);
                if (start_context.file_mode().gid() != -1) {
                    CHECK(chown(this->filename[i].c_str(), -1, start_context.file_mode().gid()) == 0,
                        "Failed to change file group: " << this->filename[i]);
                }
            }
        } else {
            DEBUG("Opening file " << this->filename[i]);
            CHECK(CheckFileSuperBlock(this->files[i]), "Failed to check file super block");
        }
    }

    uint64_t size_per_file = this->size / this->files.size();
    for (int i = 0; i < this->files.size(); i++) {
        Option<off_t> file_size = this->files[i]->GetSize();
        CHECK(file_size.valid(), "Failed to check file size: " << this->files[i]->path());

        CHECK(file_size.value() == size_per_file, "File has wrong size: " <<
            files[i]->path() <<
            ", expected size " << size_per_file <<
            ", actual size " << file_size.value());
    }

    DEBUG("Starting index: width " << this->width <<
        ", bucket size " << this->bucket_size <<
        ", total size " << this->size);

    this->state = FIXED_INDEX_STATE_STARTED;
    return true;
}

bool FixedIndex::GetFile(int64_t id, File** file, int64_t* file_id) {
    DCHECK(file_id, "file id not set");
    DCHECK(file, "file not set");

    int file_index = id % this->files.size();
    *file = this->files[file_index];
    *file_id = (id / this->files.size());
    return true;
}

size_t FixedIndex::GetOffset(File* file, int64_t file_id) {
    // the first bucket is for the index super block
    int64_t offset = (file_id + 1) * this->bucket_size;
    return offset;
}

lookup_result FixedIndex::ReadBucket(File* file, int64_t file_id, int64_t global_id, Message* message) {
    CHECK_RETURN(file, LOOKUP_ERROR, "File not set");

    size_t offset = GetOffset(file, file_id);

    ssize_t reads = 0;
    FixedIndexBucketData data;
    {
        ProfileTimer disk_timer(this->disk_time);
        if (!file->ReadSizedMessage(offset, &data, this->bucket_size, true)) {
            return LOOKUP_ERROR;
        }
    }
    TRACE("Read bucket " << file_id << ": offset " << offset << ", data " << data.ShortDebugString());

    if (!data.has_state()) {
        // there is a single case where would be ok. an unwritten block.
        TRACE("Bucket has no state: Check if this is ok here: " << data.ShortDebugString());
        CHECK_RETURN(data.has_data() == false && data.has_crc() == false && data.ByteSize() == 0,
            LOOKUP_ERROR,
            "Bucket data has not state field: offset " << offset <<
            ", file size " << file->GetSize().value() <<
            ", bucket size " << this->bucket_size <<
            ", read bytes " << reads <<
            ", file id " << file_id <<
            ", file " << file->path() <<
            ", data " << data.ShortDebugString());
        data.set_state(FIXED_INDEX_STATE_INVALID);
    }

    if (data.state() == FIXED_INDEX_STATE_INVALID) {
        return LOOKUP_NOT_FOUND;
    }

    CHECK_RETURN(data.has_data(), LOOKUP_ERROR, "Bucket data has no data field: " << data.ShortDebugString());
    CHECK_RETURN(data.has_crc() || data.has_crc_bytes(), LOOKUP_ERROR,
        "Bucket data has no crc field: " << data.ShortDebugString());

    if (data.has_key()) {
        CHECK_RETURN(data.key() == global_id, LOOKUP_ERROR, "Illegal bucket: " << data.ShortDebugString() <<
            ", search key " << global_id);
    }

    if (data.has_crc()) {
        uint32_t crc_data = crc_raw(data.data().data(), data.data().size());
        CHECK_RETURN(crc_data == data.crc(), LOOKUP_ERROR, "CRC mismatch: " << data.ShortDebugString());
    } else {
        // old version
        string crc_data = crc(data.data().data(), data.data().size());
        CHECK_RETURN(crc_data == data.crc_bytes(), LOOKUP_ERROR, "CRC mismatch: " << data.ShortDebugString());
    }
    if (message) {
        CHECK_RETURN(message->ParseFromString(data.data()), LOOKUP_ERROR,
            "Failed to parse message");
    }
    return LOOKUP_FOUND;

}

lookup_result FixedIndex::Lookup(const void* key, size_t key_size,
                                 Message* message) {
    ProfileTimer timer(this->profiling);

    CHECK_RETURN(key_size <= sizeof(int64_t), LOOKUP_ERROR, "Illegal key size: " << key_size);
    CHECK_RETURN(this->state == FIXED_INDEX_STATE_STARTED, LOOKUP_ERROR, "Index not started");
    CHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    int64_t id = 0;
    memcpy(&id, key, key_size);

    File* current_file = NULL;
    int64_t current_id = 0;
    CHECK_RETURN(GetFile(id, &current_file, &current_id), LOOKUP_ERROR, "Failed to get id");

    lookup_result r = this->ReadBucket(current_file, current_id, id, message);
    TRACE("Lookup bucket " << id << ": result " << r);
    return r;
}

put_result FixedIndex::WriteBucket(File* file, int64_t file_id, int64_t global_id, const Message& message) {
    size_t offset = GetOffset(file, file_id);

    FixedIndexBucketData data;
    message.SerializePartialToString(data.mutable_data());
    data.set_key(global_id);
    data.set_state(FIXED_INDEX_STATE_VALID);
    data.set_crc(crc_raw(data.data().data(), data.data().size()));

    TRACE("Write bucket " << file_id << ": offset " << offset << ", data " << data.ShortDebugString());
    {
        ProfileTimer disk_timer(this->disk_time);
        ssize_t writes = file->WriteSizedMessage(offset, data, this->bucket_size, true);
        if (writes == -1) {
            ERROR("Error writing bucket into data file");
            return PUT_ERROR;
        }
    }
    return PUT_OK;
}

delete_result FixedIndex::DeleteBucket(File* file, int64_t file_id, int64_t global_id) {
    size_t offset = GetOffset(file, file_id);

    FixedIndexBucketData data;
    data.set_state(FIXED_INDEX_STATE_INVALID);

    TRACE("Mark bucket " << file_id << " as invalid: offset " << offset << ", data " << data.ShortDebugString());
    {
        ProfileTimer disk_timer(this->disk_time);
        ssize_t writes = file->WriteSizedMessage(offset, data, this->bucket_size, true);
        if (writes == -1) {
            ERROR("Error deleting bucket from data file");
            return DELETE_ERROR;
        }
    }
    return DELETE_OK;
}

put_result FixedIndex::Put(const void* key, size_t key_size,
                           const Message& message) {
    ProfileTimer timer(this->profiling);

    CHECK_RETURN(this->state == FIXED_INDEX_STATE_STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= sizeof(int64_t), PUT_ERROR, "Illegal key size: " << key_size);
    CHECK_RETURN(message.ByteSize() <= this->width, PUT_ERROR, "Value size too large: " << message.ByteSize() << ", width " << this->width);

    int64_t id = 0;
    memcpy(&id, key, key_size);
    CHECK_RETURN(id <= GetLimitId(), PUT_ERROR, "id exceeds database: id " << id << ", limit id " << GetLimitId());

    TRACE("Write id " << id << ", message " << message.ShortDebugString());

    File* current_file = NULL;
    int64_t current_id = 0;
    CHECK_RETURN(GetFile(id, &current_file, &current_id), PUT_ERROR, "Failed to get id");

    enum put_result r = this->WriteBucket(current_file, current_id, id, message);
    this->version_counter.fetch_and_increment();
    return r;
}

put_result FixedIndex::PutIfAbsent(const void* key, size_t key_size,
                                   const Message& message) {
    ERROR("PutIfAbsent not supported");
    return PUT_ERROR;
}

delete_result FixedIndex::Delete(const void* key, size_t key_size) {
    ProfileTimer timer(this->profiling);
    CHECK_RETURN(this->state == FIXED_INDEX_STATE_STARTED, DELETE_ERROR, "Index not started");
    CHECK_RETURN(key, DELETE_ERROR, "Key not set");
    CHECK_RETURN(key_size <= sizeof(int64_t), DELETE_ERROR, "Illegal key size: " << key_size);

    int64_t id = 0;
    memcpy(&id, key, key_size);
    CHECK_RETURN(id <= GetLimitId(), DELETE_ERROR, "id exceeds database: id " << id << ", limit id " << GetLimitId());

    TRACE("Delete id " << id);

    File* current_file = NULL;
    int64_t current_id = 0;
    CHECK_RETURN(GetFile(id, &current_file, &current_id), DELETE_ERROR, "Failed to get id");

    enum delete_result r = this->DeleteBucket(current_file, current_id, id);
    this->version_counter.fetch_and_increment();
    return r;
}

FixedIndex::~FixedIndex() {
    DEBUG("Closing index");
    for (size_t i = 0; i < this->files.size(); i++) {
        if (this->files[i]) {
            if (!this->files[i]->Sync()) {
                WARNING("Failed to sync transaction file: " << this->filename[i]);
            }
            delete files[i];
            this->files[i] = NULL;
        }
    }
    this->files.clear();
    DEBUG("Closed index");
}

string FixedIndex::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"total\": " << this->profiling.GetSum() << "," << std::endl;
    sstr << "\"disk time\": " << this->disk_time.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

string FixedIndex::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"limit id\": " << GetLimitId() << "," << std::endl;
    sstr << "\"items\": " << GetItemCount() << "," << std::endl;
    sstr << "\"size\": " << GetPersistentSize() << std::endl;
    sstr << "}";
    return sstr.str();
}

uint64_t FixedIndex::GetPersistentSize() {
    uint64_t size_sum = 0;

    for (size_t i = 0; i < this->files.size(); i++) {
        Option<off_t> s = this->files[i]->GetSize();
        if (s.valid()) {
            size_sum += s.value();
        }
    }

    return size_sum;
}

int64_t FixedIndex::GetLimitId() {
    if (this->bucket_size == 0) {
        return 0;
    }
    uint64_t limit_id = this->size / this->bucket_size;
    limit_id -= this->filename.size();
    return limit_id - 1;
}

bool FixedIndex::SupportsIterator() {
    return true;
}

IndexIterator* FixedIndex::CreateIterator() {
    if (this->state != FIXED_INDEX_STATE_STARTED) {
        return NULL;
    }
    return new FixedIndexIterator(this);
}

FixedIndexIterator::FixedIndexIterator(FixedIndex* index) {
    this->index_ = index;
    this->id = 0;
    this->version_counter = index->version_counter;
}

FixedIndexIterator::~FixedIndexIterator() {
}

lookup_result FixedIndexIterator::Next(void* key, size_t* key_size,
                                       Message* message) {
    CHECK_RETURN(this->index_, LOOKUP_ERROR, "Index not set");
    CHECK_RETURN(this->version_counter == this->index_->version_counter, LOOKUP_ERROR, "Concurrent modification error");

    DEBUG("Get next entry: " << this->id);
    File* current_file = NULL;
    int64_t current_id = 0;
    CHECK_RETURN(this->index_->GetFile(id, &current_file, &current_id), LOOKUP_ERROR, "Failed to get id");

    enum lookup_result r = this->index_->ReadBucket(current_file, current_id, id, message);
    if (r == LOOKUP_FOUND) {
        if (key) {
            CHECK_RETURN(*key_size >= sizeof(uint64_t), LOOKUP_ERROR, "Illegal key size " << (*key_size));
            memcpy(key, &this->id, sizeof(uint64_t));
            *key_size = sizeof(uint64_t);
        }
    }
    this->id++;

    return r;
}

uint64_t FixedIndex::GetItemCount() {
    return 0;
}

}
}
