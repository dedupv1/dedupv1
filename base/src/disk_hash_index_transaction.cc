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

#include <base/disk_hash_index.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sstream>
#include <unistd.h>

#include "dedupv1_base.pb.h"

#include <base/index.h>
#include <base/hashing_util.h>
#include <base/bitutil.h>
#include <base/locks.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/timer.h>
#include <base/protobuf_util.h>
#include <base/disk_hash_index_transaction.h>
#include <base/memory.h>
#include <base/crc32.h>

using std::string;
using std::stringstream;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::ProfileTimer;
using dedupv1::base::bits;
using dedupv1::base::File;
using dedupv1::base::ScopedLock;
using dedupv1::base::ScopedArray;
using dedupv1::base::CRC;

LOGGER("DiskHashIndexTrans");

namespace dedupv1 {
namespace base {
namespace internal {

DiskHashIndexTransactionSystem::DiskHashIndexTransactionSystem(DiskHashIndex* index) {
    this->index_ = index;
    this->transaction_area_size_ = kDefaultTransactionAreaSize;
    this->page_size_ = 0;
}

DiskHashIndexTransactionSystem::Statistics::Statistics() {
    lock_free_ = 0;
    lock_busy_ = 0;
    transaction_count_ = 0;
}

bool DiskHashIndexTransactionSystem::SetOption(const std::string& option_name, const std::string& option) {
    if (option_name == "filename") {
        CHECK(option.size() > 0, "Illegal file name");
        this->transaction_filename_.push_back(option);
        return true;
    }
    if (option_name == "area-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->transaction_area_size_ = ToStorageUnit(option).value();
        CHECK(this->transaction_area_size_ <= 1024 * 1024, "Transaction area size too large: " << option);
        return true;
    }
    if (option_name == "page-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->page_size_ = ToStorageUnit(option).value();
        CHECK(this->page_size_ <= 1024 * 1024, "Page size too large: " << option);
        return true;
    }
    ERROR("Illegal option: " << option_name);
    return false;
}

bool DiskHashIndexTransactionSystem::Start(const StartContext& start_context, bool allow_restore) {
    CHECK(this->index_, "Index not set");
    CHECK(this->page_size() != 0, "Page size not set");
    CHECK(this->transaction_area_size() != 0, "Transaction area size not set");

    DEBUG("Start transaction system: " <<
        "page size " << page_size() <<
        ", transaction area count " << transaction_area_size());

    CHECK(this->transaction_filename_.size() > 0,
        "Illegal filename count: " << this->transaction_filename_.size())
    CHECK(this->transaction_filename_.size() == 1 || this->transaction_filename_.size() % 2 == 0,
        "Illegal filename count: " << this->transaction_filename_.size())
    CHECK(this->transaction_area_size() % this->transaction_filename_.size() == 0,
        "Transaction area count is not dividable to transaction filenames: " <<
        "transaction area count " << this->transaction_area_size() <<
        ", file count " << this->transaction_filename_.size());

    // Acquire the locks
    CHECK(this->lock_.Init(this->transaction_area_size()), "Failed to init locks");
    last_file_index_.resize(this->transaction_area_size());
    for (int i = 0; i < last_file_index_.size(); i++) {
        last_file_index_[i] = -1;
    }
    areas_per_file_ = this->transaction_area_size() / this->transaction_filename_.size();

    this->transaction_file_.resize(this->transaction_filename_.size());

    int io_flags = O_RDWR | O_LARGEFILE | O_SYNC;

    for (int i = 0; i < transaction_filename_.size(); i++) {
        this->transaction_file_[i] = File::Open(this->transaction_filename_[i], io_flags, 0);
        if (!allow_restore && this->transaction_file_[i]) {
            // the disk hash index as created a new set of file, then we should do the same
            INFO("Overwriting transaction file: " << this->transaction_filename_[i]);
            delete this->transaction_file_[i];
            this->transaction_file_[i] = NULL;
            CHECK(File::Remove(this->transaction_filename_[i]), "Failed to remove " << this->transaction_filename_[i]);
        }
        if (this->transaction_file_[i] == NULL) {
            CHECK(start_context.create(), "Failed to open transaction file: " << this->transaction_filename_[i]);

            INFO("Creating transaction file " << this->transaction_filename_[i]);

            CHECK(File::MakeParentDirectory(this->transaction_filename_[i], start_context.dir_mode().mode()),
                "Failed to check parent directories");

            // retry with create mode
            this->transaction_file_[i] = File::Open(this->transaction_filename_[i], io_flags | O_CREAT | O_EXCL,
                start_context.file_mode().mode());
            CHECK(this->transaction_file_[i] != NULL, "Failed to open transaction file: " << this->transaction_filename_[i]);

            if (start_context.create()) {
                CHECK(chmod(this->transaction_filename_[i].c_str(), start_context.file_mode().mode()) == 0,
                    "Failed to change file permissions: " << this->transaction_filename_[i]);
                if (start_context.file_mode().gid() != -1) {
                    CHECK(chown(this->transaction_filename_[i].c_str(), -1, start_context.file_mode().gid()) == 0,
                        "Failed to change file group: " << this->transaction_filename_[i]);
                }
            }

            // format the transaction files
            ScopedArray<byte> buffer(new byte[this->page_size()]);
            CHECK(buffer.Get(), "Failed to alloc buffer");
            memset(buffer.Get(), 0, page_size());

            DiskHashTransactionPageData transaction_page_data;
            // nothing set

            for (int j = 0; j < areas_per_file_; j++) {
                uint64_t file_pos = j * page_size();

                // we write zeros first and than an empty transaction entry so that we can sure that there are no
                // holes in the file
                CHECK(this->transaction_file_[i]->Write(file_pos, buffer.Get(), page_size()) == page_size(),
                    "Failed to write zero page");
                CHECK(this->transaction_file_[i]->WriteSizedMessage(file_pos, transaction_page_data, page_size(), true) >= 0,
                    "Failed to write zero transaction page");
            }
        }
    }
    if (allow_restore) {
        // we have to restore transactions
        CHECK(Restore(), "Failed to restore data");
    }
    return true;
}

bool DiskHashIndexTransactionSystem::CorrectItemCount(const DiskHashTransactionPageData& page_data) {
    if ((page_data.has_version() && page_data.version() > index_->version_counter_) ||
        (page_data.has_version() && page_data.version() == index_->version_counter_ && page_data.item_count() > index_->item_count_)) {
        // There is always a small race condition between the version update and the item count update and I
        // don't see a reason to close it because the item count is approximate only always. However, there
        // are a new unit test that see this differently. To make them calm, this fix for it.
        DEBUG("Recover item count: " <<
            "index item item " << this->index_->item_count_ <<
            ", index version " << this->index_->version_counter_ <<
            ", page item count " << page_data.item_count() <<
            ", page item version " << page_data.version());

        if (this->index_->version_counter_ > 0 && this->index_->version_counter_ + 1024 <= page_data.version()) {
            DEBUG("Strange version: data " << page_data.ShortDebugString() <<
                ", index item count " << this->index_->item_count_ <<
                ", index version " << this->index_->version_counter_);
        }

        this->index_->version_counter_ = page_data.version();
        this->index_->item_count_ = page_data.item_count();
        this->index_->total_item_count_ = page_data.item_count();
    }
    return true;
}

bool DiskHashIndexTransactionSystem::RestoreAreaIndex(File* file, int i) {
    CHECK(file, "File not set");
    DEBUG("Check transaction area: " <<
        "file " << file->path() <<
        ", file transaction area " << i <<
        ", file position " << (i * this->page_size()));

    off_t file_pos = i * this->page_size();
    DiskHashTransactionPageData page_data;
    if (!file->ReadSizedMessage(file_pos, &page_data, this->page_size(), true)) {
        WARNING("Failed to read transaction page data: index " <<
            "index " << i <<
            " transaction area page size " << page_size() <<
            ", file " << file->path() <<
            ", offset " << file_pos);
        // we assume that the system crashed during that the transaction data is written
        // that also means that the original page is ok
        return true;
    }
    TRACE("Transaction page data: " << page_data.ShortDebugString());
    if (page_data.has_bucket_id() == false) {
        // no open transaction here, the area was not used before
        DEBUG("Check transaction area: index " << i << ", empty");
        return true;
    }

    CRC crc_gen;
    crc_gen.Update(page_data.data().data(), page_data.data().size());
    if (crc_gen.GetRawValue() != page_data.transaction_crc()) {
        // if the data doesn't match the crc, the transaction data hasn't been written correctly
        // that also means that the original page is ok
        INFO("Check transaction area: index " << i << ", transaction write failed, original data ok: " <<
            "data crc " << crc_gen.GetRawValue() <<
            ", transaction crc " << page_data.transaction_crc());
        return true;
    }
    CHECK(page_data.data().size() <= this->index_->page_size(),
        "Illegal page size in transaction data: " <<
        " page data size " << page_data.data().size() <<
        " index page size " << this->index_->page_size());
    // now we have a correctly written transaction thing

    unsigned int file_index = 0;
    unsigned cache_index = 0;
    index_->GetFileIndex(page_data.bucket_id(), &file_index, &cache_index);
    File* index_file = index_->GetFile(file_index);
    CHECK(index_file, "File not set");

    ScopedArray<byte> buffer(new byte[this->index_->page_size()]);
    CHECK(buffer.Get(), "Failed to alloc buffer");
    memset(buffer.Get(), 0, this->index_->page_size());

    DiskHashPage page(this->index_, page_data.bucket_id(), buffer.Get(), this->index_->page_size());
    bool read_failed = false;
    if (!page.Read(index_file)) {
        WARNING("Failed to read hash page: " <<
            "index file " << index_file->path());
        read_failed = true;
    }

    CRC crc_gen2;
    crc_gen2.Update(page.raw_buffer(), page.used_size());
    if (crc_gen2.GetRawValue() == page_data.transaction_crc()) {
        if (read_failed) {
            WARNING("Failed page is equal to the transaction data: Not correctable by transaction");
            return false;
        } else {
            CorrectItemCount(page_data);
            // everything is ok
            return true;
        }
    } else if (crc_gen2.GetRawValue() == page_data.original_crc()) {
        if (read_failed) {
            WARNING("Failed page is equal to the origin data: Not correctable by transaction");
            return false;
        }
    }
    if (read_failed) {
        // original data is in place and there is now way to use the forward log.
        return true;
    }
    // page data is corrupt => Restore from forward-log
    INFO("Restore chunk index bucket: " <<
        "transaction file " << file->path() <<
        ", file transaction area " << i <<
        ", recovery from forward log: " <<
        "original crc " << page_data.original_crc() <<
        ", modified crc " << page_data.transaction_crc() <<
        ", version " << page_data.version() <<
        ", item count " << page_data.item_count() <<
        ", index file " << index_file->path());

    DCHECK(index_->page_size_ >= page_data.data().size(), "Illegal page size");
    memcpy(buffer.Get(), page_data.data().data(), page_data.data().size());
    // page has now updated buffer
    CHECK(page.ParseBuffer(),
        "Failed to reparse data from transaction data: " <<
        "page data " << page_data.ShortDebugString() <<
        ", data size " << page_data.data().size());

    // page is correctly updated to transaction data
    CHECK(page.Write(index_file), "Failed to write restored data from transaction: " <<
        "page data " << page_data.ShortDebugString() <<
        ", page " << page.DebugString());

    CHECK(CorrectItemCount(page_data),
        "Failed to correct item count: " <<
        "page data " << page_data.ShortDebugString());

    return true;
}

bool DiskHashIndexTransactionSystem::Restore() {
    CHECK(this->index_, "Index not set");
    DEBUG("Restore transactions");

    bool failed = false;
    for (std::vector<File*>::iterator i = transaction_file_.begin(); i != transaction_file_.end(); i++) {
        File* file = *i;
        CHECK(file, "File not set");
        for (int i = 0; i < areas_per_file_; i++) {
            if (!RestoreAreaIndex(file, i)) {
                WARNING("Failed to restore from transaction area " << i);
                failed = true;
            }
        }
    }
    return !failed;
}

DiskHashIndexTransactionSystem::~DiskHashIndexTransactionSystem() {
    DEBUG("Close transaction system");
    for (std::vector<File*>::iterator i = transaction_file_.begin(); i != transaction_file_.end(); i++) {
        File* file = *i;
        if (file) {
            if (!file->Sync()) {
                WARNING("Failed to sync transaction file: " << file->path());
            }
            delete file;
            *i = NULL;
        }
    }
    transaction_file_.clear();
}

bool DiskHashIndexTransaction::Init(DiskHashPage& original_page) {
    this->page_bucket_id_ = original_page.bucket_id();

    CHECK(original_page.SerializeToBuffer(), "Failed to serialize buffer: " << original_page.DebugString());
    CRC crc_gen;
    crc_gen.Update(original_page.raw_buffer(), original_page.used_size());
    uint32_t crc_value = crc_gen.GetRawValue();

    page_data_.set_bucket_id(this->page_bucket_id_);
    page_data_.set_original_crc(crc_value);

    TRACE("Init transaction: bucket " << this->page_bucket_id_ <<
        ", original page " << original_page.DebugString() <<
        ", original crc " << crc_value);

    return true;
}

bool DiskHashIndexTransaction::Start(int new_file_index, DiskHashPage& modified_page)  {
    // no transaction
    if (this->trans_system_ == NULL) {
        return true;
    }
    ProfileTimer total_timer(this->trans_system_->stats_.total_time_);
    DCHECK(this->state_ == CREATED, "Illegal state: " << this->state_);
    DCHECK(this->page_bucket_id_ == modified_page.bucket_id(), "Illegal bucket id");
    DCHECK(modified_page.used_size() <= modified_page.raw_buffer_size(),
        "Illegal used size: " << modified_page.DebugString() <<
        ", used size " << modified_page.used_size() <<
        ", buffer size " << modified_page.raw_buffer_size());

    ProfileTimer prepare_timer(this->trans_system_->stats_.prepare_time_);
    uint64_t trans_area = trans_system_->transaction_area(page_bucket_id_);

    TRACE("Starting transaction: bucket " << this->page_bucket_id_ <<
        ", modified page " << modified_page.DebugString() <<
        ", transaction area " << trans_area);

    CHECK(modified_page.SerializeToBuffer(), "Failed to serialize buffer: " << modified_page.DebugString());
    CRC crc_gen;
    crc_gen.Update(modified_page.raw_buffer(), modified_page.used_size());
    uint32_t crc_value = crc_gen.GetRawValue();

    // we here only use the used size, so that most of the time a 4KB page is enough
    // but we have to be careful with crc values
    page_data_.set_data(modified_page.raw_buffer(), modified_page.used_size());
    page_data_.set_transaction_crc(crc_value);
    page_data_.set_version(this->trans_system_->index_->version_counter_.fetch_and_increment());
    page_data_.set_item_count(this->trans_system_->index_->item_count_);

    // round up to full blocks to avoid read/modify write cycles
    size_t value_size = RoundUpFullBlocks(page_data_.ByteSize() + 32, modified_page.raw_buffer_size());
    byte* message_data = new byte[value_size];
    // Make valgrind happy
    // We only use it as a buffer for a sized protobuf message
    memset(message_data, 0, value_size);
    ScopedArray<byte> scoped_message_data(message_data);

    Option<size_t> vs = SerializeSizedMessageCached(page_data_, message_data, value_size, true);
    CHECK(vs.valid(), "Cannot serialize sized message: " << page_data_.ShortDebugString());
    CHECK(vs.value() <= trans_system_->page_size(), "Serialized message is the large: "
        "size " << value_size <<
        ", transaction page size " << trans_system_->page_size());
    DCHECK(value_size <= trans_system_->page_size(), "Illegal value size: " <<
        "value size " << value_size <<
        ", page data size " << page_data_.ByteSize() <<
        ", modified page raw buffer size " << modified_page.raw_buffer_size() <<
        ", transaction page size " << trans_system_->page_size());
    prepare_timer.stop();

    MutexLock* bucket_lock = trans_system_->lock_.Get(trans_area);
    DEBUG("Get lock: page bucket id " << page_bucket_id_ <<
        ", trans area " << trans_area <<
        ", lock " << bucket_lock->DebugString());

    {
        ProfileTimer lock_timer(this->trans_system_->stats_.lock_time_);
        CHECK(bucket_lock->AcquireLockWithStatistics(&this->trans_system_->stats_.lock_free_,
                &this->trans_system_->stats_.lock_busy_), "Failed to acquire lock: " <<
            "bucket " << page_bucket_id_ <<
            ", transaction area " << trans_area);
    }

    // if we acquired the lock, we can be sure that there is no other transaction open at that transaction
    // area index

    TRACE("Get last file index: area " << trans_area << ", last file index size " << this->trans_system_->last_file_index_.size());
    int old_file_index = this->trans_system_->last_file_index_[trans_area];
    if (old_file_index != -1) {
        ProfileTimer total_timer(this->trans_system_->stats_.sync_file_time_);
        CHECK(trans_system_->index_->SyncFile(old_file_index),
            "Failed to sync file: file index " << old_file_index);
        // when we proceed further, we should be sure that the data of this file is flushed to disk
    }

    File* file = this->trans_system_->transaction_file(trans_area);
    CHECK(file, "File not set");

    TRACE("Start transaction: bucket " << this->page_bucket_id_ <<
        ", transaction area " << trans_area <<
        ", file " << file->path() <<
        ", file transaction area " << this->trans_system_->file_transaction_area(trans_area) <<
        ", crc " << crc_value <<
        ", version " << page_data_.version() <<
        ", item count " << page_data_.item_count());

    {
        off_t transaction_offset = trans_system_->transaction_area_offset(trans_area);
        TRACE("Write transaction entry: transaction area " << trans_area <<
            ", file position " << transaction_offset <<
            ", size " << value_size <<
            ", original crc " << page_data_.original_crc() <<
            ", modified crc " << page_data_.transaction_crc() <<
            ", version " << page_data_.version() <<
            ", item count " << page_data_.item_count());
        ProfileTimer disk_timer(this->trans_system_->stats_.disk_time_);
        CHECK(file->Write(transaction_offset, message_data, value_size) == (ssize_t) value_size,
            "Failed to write transaction data: offset " << transaction_offset <<
            ", page data " << page_data_.ShortDebugString());
    }
    this->trans_system_->last_file_index_[trans_area] = new_file_index;
    // file is now dirty
    this->trans_system_->index_->MarkAsDirty(new_file_index);
    this->state_ = COMMITTED;

    // we do not release the bucket lock here. the lock is
    // released at the end of the transaction
    return true;
}

DiskHashIndexTransaction::DiskHashIndexTransaction(DiskHashIndexTransactionSystem* trans_system,
                                                   DiskHashPage& original_page) {
    // if trans system is not set, no transaction should be done
    this->trans_system_ = trans_system;

    if (trans_system_) {
        trans_system_->stats_.transaction_count_++;
        ProfileTimer total_timer(this->trans_system_->stats_.total_time_);
        // if we abort this the state should be failed
        this->state_ = FAILED;

        if (trans_system->transaction_file_.size() == 0) {
            WARNING("Transaction system not started");
            return;
        }
        if (!this->Init(original_page)) {
            WARNING("Start of transaction failed");
            return;
        }
        this->state_ = CREATED;
    } else {
        this->page_bucket_id_ = 0;
        this->state_ = COMMITTED;
    }
}

DiskHashIndexTransaction::~DiskHashIndexTransaction() {
    if (this->trans_system_) {
        ProfileTimer total_timer(this->trans_system_->stats_.total_time_);
        uint64_t trans_area = trans_system_->transaction_area(page_bucket_id_);
        if (this->state_ == CREATED || state_ == FAILED) {
            // abort
            TRACE("Abort transaction: bucket " << this->page_bucket_id_ <<
                ", transaction area " << trans_area);
        } else if (this->state_ == COMMITTED) {
            MutexLock* bucket_lock = trans_system_->lock_.Get(trans_area);
            if (!bucket_lock->ReleaseLock()) {
                WARNING("Failed to release lock");
            }
        }
    }
    // else: do nothing
}

bool DiskHashIndexTransaction::Commit() {
    if (this->trans_system_) {
        ProfileTimer total_timer(this->trans_system_->stats_.total_time_);
        uint64_t trans_area = trans_system_->transaction_area(page_bucket_id_);
        TRACE("Finish transaction: " <<
            "bucket " << this->page_bucket_id_ <<
            ", transaction area " << trans_area);
        this->state_ = FINISHED;

        // release transaction area lock
        MutexLock* bucket_lock = trans_system_->lock_.Get(trans_area);
        if (!bucket_lock->ReleaseLock()) {
            WARNING("Failed to release lock");
        }
    }
    // else to nothing
    return true;
}

string DiskHashIndexTransactionSystem::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"transaction count\": " << this->stats_.transaction_count_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string DiskHashIndexTransactionSystem::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"lock free\": " << this->stats_.lock_free_ << "," << std::endl;
    sstr << "\"lock busy\": " << this->stats_.lock_busy_ << std::endl;
    sstr << "}";
    return sstr.str();
}

string DiskHashIndexTransactionSystem::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"total time\": " << this->stats_.total_time_.GetSum() << "," << std::endl;
    sstr << "\"sync file time\": " << this->stats_.sync_file_time_.GetSum() << "," << std::endl;
    sstr << "\"serialization time\": " << this->stats_.serialisation_time_.GetSum() << "," << std::endl;
    sstr << "\"lock time\": " << this->stats_.lock_time_.GetSum() << "," << std::endl;
    sstr << "\"prepare time\": " << this->stats_.prepare_time_.GetSum() << "," << std::endl;
    sstr << "\"disk time\": " << this->stats_.disk_time_.GetSum() << std::endl;
    sstr << "}";
    return sstr.str();
}

}

}
}
