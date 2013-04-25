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

#include <base/leveldb_index.h>
#include <leveldb/filter_policy.h>
#include <leveldb/env.h>
#include <leveldb/iterator.h>
#include <leveldb/write_batch.h>
#include <leveldb/cache.h>

#include <base/strutil.h>
#include <base/protobuf_util.h>

#include <unistd.h>
#include <sys/stat.h>

using std::string;
using std::stringstream;
using std::pair;
using std::make_pair;
using std::vector;
using std::tr1::tuple;
using leveldb::DB;
using leveldb::Options;
using leveldb::WriteOptions;
using leveldb::ReadOptions;
using leveldb::Slice;
using leveldb::Status;
using google::protobuf::Message;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::ParseSizedMessage;

LOGGER("LeveldbIndex");

namespace dedupv1 {
namespace base {

const string LeveldbIndex::kItemCountKeyString("s");
const int LeveldbIndex::kDefaultBloomFilterBitsPerKey = 2;

class LeveldbIndexIterator : public IndexIterator {
    DISALLOW_COPY_AND_ASSIGN(LeveldbIndexIterator);
private:
    LeveldbIndex* index_;
    uint64_t version_counter_;
    leveldb::Iterator* iterator_;
    const leveldb::Snapshot* snapshot_;
public:

    LeveldbIndexIterator(LeveldbIndex* index) : index_(index) {
        version_counter_ = index->version_counter_;

        snapshot_ = index_->db_->GetSnapshot();

        ReadOptions options;
        options.snapshot = snapshot_;
        options.verify_checksums = index_->checksum_;
        iterator_ = index_->db_->NewIterator(options);

        iterator_->SeekToFirst();
    }

    ~LeveldbIndexIterator() {
        if (iterator_ != NULL) {
            delete iterator_;
            iterator_ = NULL;
        }
        if (snapshot_) {
            index_->db_->ReleaseSnapshot(snapshot_);
            snapshot_ = NULL;
        }
    }

    lookup_result Next(void* key, size_t* key_size,
                       Message* message) {
        CHECK_RETURN(iterator_ != NULL, LOOKUP_ERROR,
            "Iterator not set");
        CHECK_RETURN(version_counter_ == index_->version_counter_, LOOKUP_ERROR,
            "Concurrent modification error");

        if (!iterator_->Valid()) {
            return LOOKUP_NOT_FOUND;
        }

        // filter out item count entry
        Slice item_count_slice(LeveldbIndex::kItemCountKeyString);
        if (iterator_->key().compare(item_count_slice) == 0) {
            iterator_->Next();
            if (!iterator_->Valid()) {
                return LOOKUP_NOT_FOUND;
            }
        }

        DEBUG("Iterate: key " << ToHexString(iterator_->key().data(),
                iterator_->key().size()) <<
            ", value " << ToHexString(iterator_->value().data(),
                iterator_->value().size()));
        if (key != NULL) {
            CHECK_RETURN(key_size, LOOKUP_ERROR,
                "Key size not given");
            if (*key_size < iterator_->key().size()) {
                ERROR("Key too small");
                return LOOKUP_ERROR;
            }
            memcpy(key, iterator_->key().data(),
                iterator_->key().size());
            *key_size = iterator_->key().size();
        }

        if (message != NULL) {
            bool b = ParseSizedMessage(message,
                iterator_->value().data(),
                iterator_->value().size(), false).valid();
            if (!b) {
                ERROR("Failed to parse message: " <<
                    ToHexString(iterator_->value().data(),
                        iterator_->value().size()));
                return LOOKUP_ERROR;
            }
        }

        iterator_->Next();
        return LOOKUP_FOUND;
    }
};

/**
 * Custom environment for dedupv1.
 * This is needed to customize the permissions
 * of the leveldb index data structures
 */
class Dedupv1Env : public leveldb::EnvWrapper {
private:
    FileMode file_mode_;

    FileMode dir_mode_;

    Status ChangeMode(const std::string& fname, const FileMode& mode) {
        Status s; // ok status
        struct stat st;
        if (stat(fname.c_str(), &st) != 0) {
            return leveldb::Status::IOError(fname + ": Failed to stat file",
                strerror(errno));
        }
        if (st.st_gid != mode.gid()) {
            if (chown(fname.c_str(), -1, mode.gid()) != 0) {
                return leveldb::Status::IOError(fname + ": Failed to change file group",
                    strerror(errno));
            }
        }
        if ((st.st_mode & 0777) != mode.mode()) {
            if (chmod(fname.c_str(), mode.mode()) != 0) {
                return leveldb::Status::IOError(fname + ": Failed to change file mode",
                    strerror(errno));
            }
        }
        return Status();
    }
public:
    Dedupv1Env(const FileMode& file_mode, const FileMode& dir_mode) :
        EnvWrapper(leveldb::Env::Default()), file_mode_(file_mode), dir_mode_(dir_mode) {
    }

// Create a brand new sequentially-readable file with the specified name.
// On success, stores a pointer to the new file in *result and returns OK.
// On failure stores NULL in *result and returns non-OK.  If the file does
// not exist, returns a non-OK status.
//
// The returned file will only be accessed by one thread at a time.
    virtual leveldb::Status NewSequentialFile(const std::string& fname,
                                              leveldb::SequentialFile** result) {
        Status s = EnvWrapper::NewSequentialFile(fname, result);
        if (s.ok()) {
            s = ChangeMode(fname, file_mode_);
        }
        return s;
    }

    // Create a brand new random access read-only file with the
    // specified name.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores NULL in *result and
    // returns non-OK.  If the file does not exist, returns a non-OK
    // status.
    //
    // The returned file may be concurrently accessed by multiple threads.
    virtual leveldb::Status NewRandomAccessFile(const std::string& fname,
                                                leveldb::RandomAccessFile** result) {
        Status s = EnvWrapper::NewRandomAccessFile(fname, result);
        if (s.ok()) {
            s = ChangeMode(fname, file_mode_);
        }
        return s;
    }

    // Create an object that writes to a new file with the specified
    // name.  Deletes any existing file with the same name and creates a
    // new file.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores NULL in *result and
    // returns non-OK.
    //
    // The returned file will only be accessed by one thread at a time.
    virtual leveldb::Status NewWritableFile(const std::string& fname,
                                            leveldb::WritableFile** result) {
        Status s = EnvWrapper::NewWritableFile(fname, result);
        if (s.ok()) {
            s = ChangeMode(fname, file_mode_);
        }
        return s;
    }

    // Create the specified directory.
    virtual Status CreateDir(const std::string& dirname) {
        Status s = EnvWrapper::CreateDir(dirname);
        if (s.ok()) {
            s = ChangeMode(dirname, dir_mode_);
        }
        return s;
    }

    virtual Status LockFile(const std::string& fname, leveldb::FileLock** lock) {
        Status s = EnvWrapper::LockFile(fname, lock);
        if (s.ok()) {
            s = ChangeMode(fname, file_mode_);
        }
        return s;
    }
};

void LeveldbIndex::RegisterIndex() {
    Index::Factory().Register("leveldb-disk-lsm",
        &LeveldbIndex::CreateIndex);
}

Index* LeveldbIndex::CreateIndex() {
    return new LeveldbIndex();
}

LeveldbIndex::LeveldbIndex() : PersistentIndex(PERSISTENT_ITEM_COUNT | NATIVE_BATCH_OPS) {
    this->db_ = NULL;
    this->bloom_filter_bits_per_key_ = kDefaultBloomFilterBitsPerKey;
    this->use_compression_ = true;
    this->sync_ = true;
    this->checksum_ = true;
    version_counter_ = 0;
    item_count_ = 0;
    cache_size_ = 1024;
    block_size_ = 0; // use default value
    lazy_item_count_persistent_interval_ = 1024;
}

LeveldbIndex::~LeveldbIndex() {
    if (db_ != NULL) {
        delete db_;
        db_ = NULL;
    }
}

LeveldbIndex::Statistics::Statistics() {
    this->lookup_count_ = 0;
    this->delete_count_ = 0;
    this->update_count_ = 0;
}

bool LeveldbIndex::SetOption(const string& option_name, const string& option) {
    CHECK(db_ == NULL, "Illegal state");

    if (option_name == "bloom-filter") {
        Option<bool> b = To<bool>(option);
        if (b.valid()) {
            if (!b.value()) {
                bloom_filter_bits_per_key_ = 0; // disable bloom filter
            } else {
                bloom_filter_bits_per_key_ = kDefaultBloomFilterBitsPerKey;
            }
        } else {
            // no problem
            Option<int> i = To<int>(option);
            CHECK(i.valid(), "Failed to parse bloom filter value: " << option);
            CHECK(i.value() > 0, "Illegal bloom filter value: " << option);
            bloom_filter_bits_per_key_ = i.value();
        }
        return true;
    }
    if (option_name == "compression") {
        Option<bool> b = To<bool>(option);
        CHECK(b.valid(), "Illegal compression value: " << option);
        use_compression_ = b.value();
        return true;
    }
    if (option_name == "sync") {
        Option<bool> b = To<bool>(option);
        CHECK(b.valid(), "Illegal sync value: " << option);
        sync_ = b.value();
        return true;
    }
    if (option_name == "checksum") {
        Option<bool> b = To<bool>(option);
        CHECK(b.valid(), "Illegal checksum value: " << option);
        checksum_ = b.value();
        return true;
    }
    if (option_name == "max-item-count") {
        Option<int64_t> b = ToStorageUnit(option);
        CHECK(b.valid(), "Illegal estimated max item count value: " << option);
        estimated_max_item_count_ = b.value();
        return true;
    }
    if (option_name == "block-size") {
        Option<int64_t> b = ToStorageUnit(option);
        CHECK(b.valid(), "Illegal block size value " << option);
        CHECK(b.value() >= 512, "Illegal block size value " << option);
        block_size_ = b.value();
        return true;
    }
    if (option_name == "cache-size") {
        Option<int64_t> b = ToStorageUnit(option);
        CHECK(b.valid(), "Illegal cache size value " << option);
        CHECK(b.value() >= 0, "Illegal cache size value " << option);
        cache_size_ = b.value();
        return true;
    }
    if (option_name == "filename") {
        CHECK(index_dir_.empty(), "Filename already set");
        CHECK(option.size() > 0, "Illegal filename");
        index_dir_ = option;
        return true;
    }
    return PersistentIndex::SetOption(option_name, option);
}

bool LeveldbIndex::Start(const StartContext& start_context) {
    CHECK(db_ == NULL, "Illegal state");
    CHECK(!index_dir_.empty(), "Index directory not set")

    Options options;
    options.env = new Dedupv1Env(start_context.file_mode(), start_context.dir_mode());

    if (start_context.create()) {
        options.create_if_missing = true;
    }

    if (bloom_filter_bits_per_key_ > 0) {
        options.filter_policy = leveldb::NewBloomFilterPolicy(bloom_filter_bits_per_key_);
    }
    if (!use_compression_) {
        options.compression = leveldb::kNoCompression;
    } else {
        options.compression = leveldb::kSnappyCompression;
    }
    if (block_size_ > 0) {
        options.block_size = block_size_;
    }
    if (cache_size_ > 0) {
        options.block_cache = leveldb::NewLRUCache(cache_size_);
    }

    leveldb::Status s = DB::Open(options, index_dir_, &db_);
    // now open the database
    if (!s.ok()) {
        ERROR("Error creating leveldb database: " << s.ToString());
        db_ = NULL; // just to make sure
        return false;
    }

    if (!RestoreItemCount()) {
        ERROR("Failed to restore the persistent item count");
        delete db_;
        db_ = NULL;
        return false;
    }
    return true;
}

bool LeveldbIndex::LazyStoreItemCount(uint64_t version_count, bool force) {
    CHECK(db_ != NULL, "Illegal state");

    if (force || (version_count % lazy_item_count_persistent_interval_) == 0) {
        // persist the item count
        Slice key_slice(kItemCountKeyString);
        Slice value_slice(dedupv1::base::strutil::ToString(item_count_));
        WriteOptions options;
        options.sync = true;

        Status s = db_->Put(options, key_slice, value_slice);
        if (!s.ok()) {
            ERROR("Failed to persist the lazy item count: " << s.ToString());
            return false;
        }
    }
    return true;
}

bool LeveldbIndex::RestoreItemCount() {
    CHECK(db_ != NULL, "Illegal state");
    CHECK(item_count_ == 0, "Illegal item count state");

    Slice key_slice(kItemCountKeyString);
    ReadOptions options;
    options.verify_checksums = true;
    string target;
    Status s = db_->Get(options, key_slice, &target);
    if (s.ok()) {
        Option<uint64_t> item_count = dedupv1::base::strutil::To<uint64_t>(target);
        CHECK(item_count.valid(), "Illegal stored item count");
        DEBUG("Restoring the item count: " << item_count.value());
        item_count_ = item_count.value();
    } else if (s.IsNotFound()) {
        // no existing entry
        item_count_ = 0;
    } else {
        // some error code
        ERROR("Failed to read stored item count value: " <<
            ", message " << s.ToString());
        return false;
    }
    return true;
}

lookup_result LeveldbIndex::Lookup(const void* key, size_t key_size,
                                   Message* message) {
    ProfileTimer timer(stats_.total_time_);
    ProfileTimer lookup_timer(stats_.update_time_);

    CHECK_RETURN(db_ != NULL, LOOKUP_ERROR, "Index not started");
    CHECK_RETURN(key, LOOKUP_ERROR, "Key not set");
    CHECK_RETURN(key_size <= INT_MAX, LOOKUP_ERROR, "Key size too large");
    CHECK_RETURN(key_size >= 2, LOOKUP_ERROR, "Key too small");

    ReadOptions options;
    options.verify_checksums = checksum_;
    Slice key_slice(reinterpret_cast<const char*>(key), key_size);
    string target;
    Status s = db_->Get(options, key_slice, &target);

    if (s.ok()) {
        bool b = true;
        if (message != NULL) {
            b = ParseSizedMessage(message,
                target.data(), target.size(),
                false).valid();
            if (!b) {
                ERROR("Failed to parse message: " <<
                    ToHexString(target.data(), target.size()));
            }
        }
        stats_.lookup_count_++;
        return b ? LOOKUP_FOUND : LOOKUP_ERROR;
    } else if (s.IsNotFound()) {
        return LOOKUP_NOT_FOUND;
    } else {
        // some error status
        ERROR("Failed to lookup value: " <<
            "key " << ToHexString(key, key_size) <<
            ", message " << s.ToString());
        return LOOKUP_ERROR;
    }
}

put_result LeveldbIndex::Put(const void* key, size_t key_size,
                             const Message& message) {
    ProfileTimer timer(stats_.total_time_);
    ProfileTimer update_timer(stats_.update_time_);

    CHECK_RETURN(db_ != NULL, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= INT_MAX, PUT_ERROR, "Key size too large");
    CHECK_RETURN(key_size >= 2, PUT_ERROR, "Key too small");

    WriteOptions options;
    options.sync = sync_;

    Slice key_slice(reinterpret_cast<const char*>(key), key_size);
    string target;
    CHECK_RETURN(SerializeSizedMessageToString(message, &target, false),
        PUT_ERROR, "Failed to serialize message: " <<
        message.ShortDebugString());

    DEBUG("Put: key " << ToHexString(key, key_size) <<
        ", value " << ToHexString(target.data(), target.size()));

    Slice value_slice(target);
    Status s = db_->Put(options, key_slice, value_slice);

    if (!s.ok()) {
        ERROR("Failed to put value: " <<
            "key " << ToHexString(key, key_size) <<
            ", value " << message.ShortDebugString() <<
            ", message " << s.ToString());
        return PUT_ERROR;
    } else {
        stats_.update_count_++;
        item_count_++;
        version_counter_++;

        if (!LazyStoreItemCount(version_counter_, false)) {
            ERROR("Failed to store the item count");
            return PUT_ERROR;
        }
        return PUT_OK;
    }
}

put_result LeveldbIndex::PutBatch(const vector<tuple<bytestring, const Message*> >& data) {
    ProfileTimer timer(stats_.total_time_);
    ProfileTimer update_timer(stats_.update_time_);

    CHECK_RETURN(db_ != NULL, PUT_ERROR, "Index not started")

    WriteOptions options;
    options.sync = sync_;

    leveldb::WriteBatch batch;
    vector<tuple<bytestring, const Message*> >::const_iterator i;
    for (i = data.begin(); i != data.end(); i++) {
        bytestring key(std::tr1::get<0>(*i));
        const Message* message = std::tr1::get<1>(*i);
        CHECK_RETURN(message, PUT_ERROR, "Message not set");

        Slice key_slice(reinterpret_cast<const char*>(key.data()), key.size());
        string target;
        CHECK_RETURN(SerializeSizedMessageToString(*message, &target, false),
            PUT_ERROR, "Failed to serialize message: " <<
            message->ShortDebugString());

        DEBUG("Put: key " << ToHexString(key.data(), key.size()) <<
            ", value " << ToHexString(target.data(), target.size()));

        Slice value_slice(target);
        batch.Put(key_slice, value_slice);
    }

    Status s = db_->Write(options, &batch);
    if (!s.ok()) {
        ERROR("Failed to write patch: " <<
            "message " << s.ToString());
        return PUT_ERROR;
    } else {
        stats_.update_count_++;
        item_count_ += data.size();
        version_counter_++;

        if (!LazyStoreItemCount(version_counter_, false)) {
            ERROR("Failed to store the item count");
            return PUT_ERROR;
        }
        return PUT_OK;
    }
}

delete_result LeveldbIndex::Delete(const void* key, size_t key_size) {
    ProfileTimer timer(stats_.total_time_);
    ProfileTimer delete_timer(stats_.delete_time_);

    CHECK_RETURN(db_ != NULL, DELETE_ERROR, "Index not started");
    CHECK_RETURN(key != NULL, DELETE_ERROR, "Key not set");
    CHECK_RETURN(key_size <= INT_MAX, DELETE_ERROR, "Key size too large");
    CHECK_RETURN(key_size >= 2, DELETE_ERROR, "Key too small");

    DEBUG("Delete key: " << ToHexString(key, key_size));

    Slice key_slice(reinterpret_cast<const char*>(key), key_size);
    WriteOptions options;
    options.sync = sync_;
    Status s = db_->Delete(options, key_slice);
    if (s.ok()) {
        stats_.delete_count_++;
        version_counter_++;
        item_count_--;

        if (!LazyStoreItemCount(version_counter_, false)) {
            ERROR("Failed to store the item count");
            return DELETE_ERROR;
        }

        return DELETE_OK;
    } else {
        // some error status
        ERROR("Failed to delete value: " <<
            "key " << ToHexString(key, key_size) <<
            ", message " << s.ToString());
        return DELETE_ERROR;
    }
}

string LeveldbIndex::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"total time\": " << this->stats_.total_time_.GetSum() << "," << std::endl;
    sstr << "\"lookup time\": " << this->stats_.lookup_time_.GetSum() << "," << std::endl;
    sstr << "\"update time\": " << this->stats_.update_time_.GetSum() << "," << std::endl;
    sstr << "\"delete time\": " << this->stats_.delete_time_.GetSum() << "" << std::endl;
    sstr << "}";
    return sstr.str();
}

string LeveldbIndex::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"lookup count\": " << this->stats_.lookup_count_ << "," << std::endl;
    sstr << "\"update count\": " << this->stats_.update_count_ << "," << std::endl;
    sstr << "\"delete count\": " << this->stats_.delete_count_ << "" << std::endl;
    sstr << "}";
    return sstr.str();
}

uint64_t LeveldbIndex::GetItemCount() {
    return item_count_;
}

uint64_t LeveldbIndex::GetEstimatedMaxItemCount() {
    return estimated_max_item_count_;
}

uint64_t LeveldbIndex::GetPersistentSize() {
    // There is no really good way to get this information
    return 0;
}

IndexIterator* LeveldbIndex::CreateIterator() {
    CHECK_RETURN(db_ != NULL, NULL, "Illegal state to create iterator");
    return new LeveldbIndexIterator(this);
}

}
}
