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

#ifndef DISK_HASH_INDEX_TRANSACTION_H__
#define DISK_HASH_INDEX_TRANSACTION_H__

#include <base/base.h>
#include <base/fileutil.h>
#include <base/locks.h>
#include <base/profile.h>
#include <base/hashing_util.h>

#include "dedupv1_base.pb.h"

namespace dedupv1 {
namespace base {

class DiskHashIndex;

namespace internal {

class DiskHashEntryPage;

/**
 * A simple transaction system for the disk-based hash index.
 *
 * The disk-based hash index is a standard disk-based hash index with pages.
 *
 * A transaction system is necessary because sector writes seem not atomic in general. See the
 * discussion in "Crash-only software: More than meets the eye" on LWN for details (http://lwn.net/Articles/191059/).
 * Because a pages is shared by multiple keys a write by a single key might destroy the data integrity.
 * A deduplication system cannot rely on a backup system, well, because we are the backup system. We are
 * often the last line of defense.
 *
 * The main idea of the transaction system is a forward transaction log. The goal is to
 * minimize write operations. The transaction system used only a single IO operation per transaction.
 *
 * Before a modifying index operations writes to disk (Put, PutIfAbsent, Delete) the following data
 * is written to an transaction area:
 * - The CRC of the original page buffer data
 * - The CRC of the modified page buffer data
 * - The modified page buffer data (Step 1)
 * Only when this data is written to the transaction file, the base write operation is done (Step 2). Nothing is
 * done during a commit or abort except that a lock is released. The transaction is not marked as done on disk to safe
 * IO operations.
 *
 * The transaction file has a limited number of places to which transaction data is written. The transaction
 * data is assigned to the places by modulo hashing on the bucket id. This means that no to transactions on two
 * buckets that map to the same transaction area index are possible, but this allows to use a single static-size
 * file.
 *
 * When the transaction system restarts, the transaction areas are checked:
 * - If the transaction area is empty, nothing is done
 * - If the transaction area cannot be read, we assume that the Step 1 failed. This means by that the
 * original index data is clean and in its "before" state. If the data cannot be read because of a data
 * parsing failure and the data is equal to a known good state (transaction crc or original data crc) there
 * is a bug in dedupv1 that has nothing to do with the transaction or storage system.
 *
 * - If the index page is equal to the transaction crc, this means that the update operations was fine.
 * - If the index page is equal to the original crc, this means that the system crashes before the write
 * - could take place. We are able to apply the modified page buffer data from the transaction data. However,
 * this is not strictly necessary for atomicity.
 * - If the index page data doesn't match any crc, the write failed and corrupted the data. We are now able
 * to restore the page from the forward-logged data in Step 1.
 *
 * The transaction system doesn't assume that the disk sector writes are atomic. The transaction system assumes
 * that it is reasonable unlikely that a garbaged (because of a partial write) page passes a CRC check.
 *
 * A limitation of this approach is that it is not possible to group multiple update operations into a single
 * transaction. The approach is therefore limited to atomic commit situations.
 *
 */
class DiskHashIndexTransactionSystem {
    private:
        friend class DiskHashIndexTransaction;
        DISALLOW_COPY_AND_ASSIGN(DiskHashIndexTransactionSystem);

        /**
         * Statistics about transaction system
         */
        class Statistics {
            public:
                /**
                 * Constructor
                 * All values are set to zero.
                 */
                Statistics();
                tbb::atomic<uint32_t> transaction_count_;

                tbb::atomic<uint32_t> lock_free_;
                tbb::atomic<uint32_t> lock_busy_;

                dedupv1::base::Profile total_time_;
                dedupv1::base::Profile sync_file_time_;
                dedupv1::base::Profile write_time_;
                dedupv1::base::Profile serialisation_time_;
                dedupv1::base::Profile lock_time_;
                dedupv1::base::Profile disk_time_;
                dedupv1::base::Profile prepare_time_;
        };

        /**
         * Default number of transaction area places. This also means that at
         * most kDefaultTransactionAreaSize transactions can be open concurrently
         */
        static const uint64_t kDefaultTransactionAreaSize = 1024;

        /**
         * Reference to the base index
         */
        DiskHashIndex* index_;

        /**
         * Pointer to the file for the transaction data
         */
        std::vector<File*> transaction_file_;

        /**
         * Filename of the transaction data file
         */
        std::vector<std::string> transaction_filename_;

        /**
         * Number of transaction area places in the transaction file
         */
        uint64_t transaction_area_size_;

        /**
         * Transaction areas per transaction file
         */
        uint64_t areas_per_file_;

        /**
         * the page size must be at least as large to hold the complete data of an index
         * page + transaction metadata as 2 crc values.
         */
        uint64_t page_size_;

        /**
         * Lock to protect the transaction area places against concurrent access
         */
        dedupv1::base::MutexLockVector lock_;

        std::vector<int> last_file_index_;

        /**
         * Statistics
         */
        Statistics stats_;

        /**
         * restore the transaction from the transaction area with the given index.
         *
         * @param file file in which an area should be restored
         * @param i index of the restore area
         * @return
         */
        bool RestoreAreaIndex(File* file, int i);

        /**
         * Restore transactions from the transaction log
         * @return
         */
        bool Restore();

        /**
         * Corrects the index item count based on the count data in the
         * page data
         */
        bool CorrectItemCount(const DiskHashTransactionPageData& page_data);
    public:
        /**
         * Constructor
         * @param index
         * @return
         */
        explicit DiskHashIndexTransactionSystem(DiskHashIndex* index);

        virtual ~DiskHashIndexTransactionSystem();

        /**
         * Configures the transaction system
         *
         * Available options:
         * - filename: String with file where the transaction data is stored (multi)
         * - area-size: StorageUnit
         * - page-size: StorageUnit
         *
         * @param option_name
         * @param option
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * The transaction system has to be started as the latest element of the
         * of the index. The system restores transactions if necessary during the startup.
         *
         * @param start_context
         * @param allow_restore
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context, bool allow_restore);

        /**
         * Returns the pointer the the base index
         * @return
         */
        inline DiskHashIndex* index();

        /**
         * returns a pointer to the transaction file
         * @return
         */
        inline File* transaction_file(uint64_t bucket_id);

        /**
         * returns the configured transaction filename
         * @return
         */
        inline std::string transaction_filename(uint64_t bucket_id) const;

        /**
         * returns the configured number of transaction area places
         * @return
         */
        inline uint64_t transaction_area_size() const;

        /**
         * returns the configured page size of a single transaction area
         * @return
         */
        inline uint64_t page_size() const;

        /**
         * returns the index of the transaction area that should be used by a bucket id
         * @param bucket_id
         * @return
         */
        inline uint64_t transaction_area(uint64_t bucket_id) const;

        inline uint64_t file_transaction_area(uint64_t bucket_id) const;

        /**
         * returns the offset of a file of the given transaction area offset
         * @param transaction_area_index
         * @return
         */
        inline off_t transaction_area_offset(uint64_t transaction_area_index) const;

        /**
         * returns the lock that should be used for the given bucket id
         * @param bucket_id
         * @return
         */
        inline MutexLock* lock(uint64_t bucket_id);

        std::string PrintTrace();
        std::string PrintLockStatistics();
        std::string PrintProfile();
};

/**
 * Class for a single atomic commit transaction for the disk-based hash index.
 */
class DiskHashIndexTransaction {
    private:
        /**
         * Reference to the transaction system.
         * If set to NULL, no transactions should be used
         */
        DiskHashIndexTransactionSystem* trans_system_;

        /**
         * Enumeration of the different states of the transaction
         */
        enum transaction_state {
            CREATED,  //!< CREATED
            COMMITTED,//!< COMMITTED
            FINISHED,
            FAILED    //!< FAILED
        };

        /**
         * State of the transaction
         */
        transaction_state state_;

        /**
         * Bucket id of the transaction
         */
        uint64_t page_bucket_id_;

        /**
         * transaction page data that holds the transaction metadata
         * Depending on the state of the transaction, the page data might not be fully
         * filled.
         */
        DiskHashTransactionPageData page_data_;

        /**
         * Inits the transaction
         * @param original_page
         * @return
         */
        bool Init(DiskHashPage& original_page);
    public:
        /**
         *
         * @param trans_system May be null if no transactions should be used
         * @param original_page
         * @return
         */
        DiskHashIndexTransaction(DiskHashIndexTransactionSystem* trans_system, DiskHashPage& original_page);

        /**
         * Destructor.
         *
         * If the transaction is destroyed before the start call, the assume that the transaction
         * aborted and that the data should not be changed. If the transaction is started, the data
         * of the transaction will be recovered.
         * @return
         */
        ~DiskHashIndexTransaction();

        /**
         * We use a forward logging approach.
         * If the system crashes after a successful start call, the data will be recovered.
         *
         * @param new_file_index file index to which the modified page is stored.
         * @param modified_page
         * @return
         */
        bool Start(int new_file_index, DiskHashPage& modified_page);

        /**
         * Commits the transaction. This method should be called if the primary data is
         * written safely.
         *
         * As we use a forward logging, the commit does nothing more than the release the lock that
         * as protected the transaction area. Other clients are not free to overwrite the transaction
         * area as the data is safe.
         *
         * @return
         */
        bool Commit();


};

DiskHashIndex* DiskHashIndexTransactionSystem::index() {
    return index_;
}

File* DiskHashIndexTransactionSystem::transaction_file(uint64_t transaction_area) {
    return transaction_file_[transaction_area % transaction_file_.size()];
}

std::string DiskHashIndexTransactionSystem::transaction_filename(uint64_t transaction_area) const {
    return transaction_filename_[transaction_area % transaction_filename_.size()];
}

uint64_t DiskHashIndexTransactionSystem::transaction_area_size() const {
    return transaction_area_size_;
}

uint64_t DiskHashIndexTransactionSystem::page_size() const {
    return page_size_;
}

uint64_t DiskHashIndexTransactionSystem::transaction_area(uint64_t bucket_id) const {
  uint32_t hash_value = 0;
  murmur_hash3_x86_32(&bucket_id, sizeof(bucket_id), 1, &hash_value);
  return hash_value % transaction_area_size_;
}

uint64_t DiskHashIndexTransactionSystem::file_transaction_area(uint64_t transaction_area) const {
    return transaction_area % this->areas_per_file_;
}

off_t DiskHashIndexTransactionSystem::transaction_area_offset(uint64_t transaction_area) const {
    return file_transaction_area(transaction_area) * this->page_size();
}

MutexLock* DiskHashIndexTransactionSystem::lock(uint64_t transaction_area) {
    return lock_.Get(transaction_area);
}

}
}
}

#endif /* DISK_HASH_INDEX_TRANSACTION_H_ */
