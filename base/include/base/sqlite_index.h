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

#ifndef SQLITE_INDEX_H__
#define SQLITE_INDEX_H__

#include <base/index.h>

#include <sqlite3.h>
#include <tbb/atomic.h>
#include <tbb/spin_rw_mutex.h>
#include <vector>
#include <string>
#include <tr1/tuple>

#include <base/locks.h>
#include <base/profile.h>
#include <base/startup.h>

namespace dedupv1 {
namespace base {

/**
 * Disk-based B+-Tree in the sqlite implementation.
 * The type name is "sqlite-disk-btree".
 *
 * The index has two subtypes depending on the key size.
 * If a key size <= 8 is used, the key is of the type integer. Otherwise
 * a blob type is used. The integer version is much faster (e.g. a factor of 2) than
 * the blob version.
 */
class SqliteIndex : public PersistentIndex {
    DISALLOW_COPY_AND_ASSIGN(SqliteIndex);

    class StatementGroup {
        public:
            std::string countStatement;
            std::string createStatement;
            std::string lookupStatement;
            std::string putUpdateStatement;
            std::string putStatement;
            std::string putIfAbsentStatement;
            std::string deleteStatement;
            std::string beginStatement;
            std::string commitStatement;
            std::string abortStatement;
            std::string cursorStatement;
            std::string cursorLastStatement;
    };

	/**
	 * current statement groups
	 * There are two statement groups depending on the key size.
	 */
    StatementGroup statements;
    static StatementGroup GetIntegerStatementGroup();
    static StatementGroup GetBlobStatementGroup();

    /**
     * States of the index
     */
    enum State {
        CREATED,
        STARTED
    };

    /**
     * Vector of sqlite database connections
     */
    std::vector<sqlite3*> db;

    ReadWriteLockVector locks_;

    /**
     * Vector of the filenames
     */
    std::vector<std::string> filename;

	/**
	 * State of the index
	 */
    State state;

    dedupv1::base::Profile write_profiling_;

    dedupv1::base::Profile lookup_profiling_;

    /**
     * Current version counter
     */
    tbb::atomic<uint64_t> version_counter;

    /**
     * sets the max key size.
     * If a key size <= 8 bytes is used, we use a faster integer mode.
     * The default is the blob key mode for larger keys.
     */
    size_t max_key_size;

	/**
	 * Configures size of the cache
	 */
    uint32_t cache_size;

    /**
     * Lock to protect the base members. Should
     */
    tbb::spin_rw_mutex lock;

    /**
     * If set to a non-zero value, the db files are allocates to this size via
     * fallocate
     */
    uint64_t preallocate_size_;

    /**
     * Database chunk size
     * From sqlite documentation: "Allocating database file space in large chunks
     * (say 1MB at a time), may reduce file-system fragmentation and improve
     * performance on some systems."
     *
     */
    int chunk_size_;

    uint64_t estimated_max_item_count;

    tbb::atomic<uint64_t> item_count_;

    bool sync;

    bool GetDBIndex(const void* key, size_t key_size, int* index);

    sqlite3_stmt* GetStatement(sqlite3* db, const std::string& statement);

    bool BeginTransaction(sqlite3* db);
    bool CommitTransaction(sqlite3* db);
    bool AbortTransaction(sqlite3* db);

    inline bool IsIntegerMode() const;

    uint64_t GetInitialItemCount();

    enum put_result InternalPut(
                    const void* key, size_t key_size,
                    const void* value, size_t value_size);

    enum put_result InternalPutBatch(
            const std::vector<std::tr1::tuple<bytestring, bytestring> >& data);

    enum put_result InternalPutIfAbsent(
                    const void* key, size_t key_size,
                    const void* value, size_t value_size);

    public:
    /**
     * Constructor.
     * @return
     */
    SqliteIndex();

    /**
     * Destructor
     * @return
     */
    virtual ~SqliteIndex();

    static Index* CreateIndex();

    static void RegisterIndex();

    /**
     *
     * Available options:
     * - filename: String with file where the transaction data is stored (multi)
     * - cache-size: StorageUnit
     * - sync: Boolean
     * - max-item-count: uint64_t
     * - lazy-sync (ignored)
     * - max-key-size: size_t
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    bool Start(const dedupv1::StartContext& start_context);

    /**
     *
     * @param key
     * @param key_size
     * @param message
     * @return
     */
    virtual enum lookup_result Lookup(const void* key, size_t key_size,
            google::protobuf::Message* message);

    /**
     *
     * @param key
     * @param key_size
     * @param message
     * @return
     */
    virtual enum put_result Put(const void* key, size_t key_size,
            const google::protobuf::Message& message);

    /**
     *
     * @param key
     * @param key_size
     * @param message
     * @return
     */
    virtual enum put_result PutIfAbsent(
            const void* key, size_t key_size,
            const google::protobuf::Message& message);

    /**
     * Used for raw updates
     */
    virtual enum put_result RawPutIfAbsent(
            const void* key, size_t key_size,
            const void* value, size_t value_size);

    /**
     * Used for raw updates
     */
    virtual enum put_result RawPut(
            const void* key, size_t key_size,
            const void* value, size_t value_size);

    /**
     * Used for raw lookups
     */
    virtual enum lookup_result RawLookup(const void* key, size_t key_size,
            void* value, size_t* value_size);

    virtual enum put_result RawPutBatch(
            const std::vector<std::tr1::tuple<bytestring, bytestring> >& data);

    virtual enum put_result PutBatch(
            const std::vector<std::tr1::tuple<bytestring, const google::protobuf::Message*> >& data);

    virtual enum delete_result Delete(const void* key, size_t key_size);

    virtual std::string PrintProfile();

    virtual uint64_t GetPersistentSize();

    virtual uint64_t GetEstimatedMaxItemCount();

    virtual uint64_t GetItemCount();

    virtual bool SupportsCursor();

    virtual IndexCursor* CreateCursor();

    virtual IndexIterator* CreateIterator();

    friend class SingleFileSqliteCursor;
    friend class SqliteIterator;
};

class SqliteIterator : public IndexIterator {
        SqliteIndex* index_;
        sqlite3_stmt* stmt_;
        sqlite3* db_;
        int db_index_;
        bool end_;
        uint64_t version_counter_;
        SqliteIterator(SqliteIndex* index);
    public:

        virtual ~SqliteIterator();

        virtual enum lookup_result Next(void* key, size_t* key_size,
                google::protobuf::Message* message);

        friend class SqliteIndex;
};

/**
 * Cursor class for a sqlite-disk-btree.
 * This variant is used if the btree has only a single file
 *
 * Instances are created via the CreateCursor method.
 */
class SingleFileSqliteCursor : public IndexCursor {
        DISALLOW_COPY_AND_ASSIGN(SingleFileSqliteCursor);
        sqlite3_stmt* cursor_stmt;
        SqliteIndex* index;
        sqlite3* db;

        SingleFileSqliteCursor(SqliteIndex* index, sqlite3* db);
    public:
        virtual enum lookup_result First();
        virtual enum lookup_result Next();
        virtual enum lookup_result Last();
        virtual enum lookup_result Jump(const void* key, size_t key_size);
        virtual bool Remove();
        virtual bool Get(void* key, size_t* key_size,
                google::protobuf::Message* message);
        virtual bool Put(const google::protobuf::Message& message);

        virtual bool IsValidPosition();

        virtual ~SingleFileSqliteCursor();

        friend class SqliteIndex;
};

bool SqliteIndex::IsIntegerMode() const {
    return (this->max_key_size <= 8);
}

}
}

#endif /* SQLITE_INDEX_H_ */
