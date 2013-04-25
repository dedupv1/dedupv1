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

#include <base/sqlite_index.h>

#include <base/logging.h>
#include <base/hashing_util.h>
#include <base/strutil.h>
#include <base/fileutil.h>
#include <base/protobuf_util.h>
#include <base/memory.h>
#include <vector>
#include <set>
#include <tr1/tuple>
#include <sstream>

using std::stringstream;
using std::string;
using dedupv1::base::File;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::ToStorageUnit;
using google::protobuf::Message;
using dedupv1::base::ScopedArray;
using std::set;
using std::multimap;
using std::vector;
using std::tr1::tuple;
using std::make_pair;
using std::tr1::make_tuple;
using dedupv1::base::SerializeMessageToString;

LOGGER("SqliteIndex");

namespace dedupv1 {
namespace base {

void SqliteIndex::RegisterIndex() {
    Index::Factory().Register("sqlite-disk-btree", &SqliteIndex::CreateIndex);
}

Index* SqliteIndex::CreateIndex() {
    return new SqliteIndex();
}

SqliteIndex::SqliteIndex() : PersistentIndex(PERSISTENT_ITEM_COUNT | RETURNS_DELETE_NOT_FOUND | RAW_ACCESS | PUT_IF_ABSENT) {
    this->version_counter = 0;
    this->state = CREATED;
    this->cache_size = 1024;
    this->max_key_size = 512;
    this->estimated_max_item_count = 0;
    this->sync = true;
    item_count_ = 0;
    preallocate_size_ = 0;
    chunk_size_ = 1024 * 1024;
}

bool SqliteIndex::SetOption(const string& option_name, const string& option) {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, true);
    CHECK(this->state == CREATED, "Illegal state " << this->state);

    if (option_name == "filename") {
        CHECK(option.size() < 1024, "Illegal filename");
        this->filename.push_back(option);
        return true;
    }
    if (option_name == "cache-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->cache_size = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "preallocated-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->preallocate_size_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "chunk-size") {
        CHECK(ToStorageUnit(option).valid(), "Illegal option " << option);
        this->chunk_size_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "sync") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->sync = To<bool>(option).value();
        return true;
    }
    if (option_name == "max-item-count") {
        Option<int64_t> i = ToStorageUnit(option);
        CHECK(i.valid(), "Illegal option " << option);
        CHECK(i.value() > 0, "Illegal option " << option);
        this->estimated_max_item_count = i.value();
        return true;
    }
    if (option_name == "lazy-sync") {
        return true; // ignore
    }
    if (option_name == "max-key-size") {
        CHECK(To<size_t>(option).valid(), "Illegal option " << option);
        size_t v = To<size_t>(option).value();
        CHECK(v > 0, "Illegal max key size");
        CHECK(v <= 512, "Illegal max key size");
        this->max_key_size = v;
        return true;
    }
    return PersistentIndex::SetOption(option_name, option);
}

SqliteIndex::StatementGroup SqliteIndex::GetBlobStatementGroup() {
    StatementGroup sg;
    sg.createStatement = "CREATE TABLE blobkey(k BLOB PRIMARY KEY, v BLOB)";
    sg.countStatement = "SELECT COUNT(*) FROM blobkey";
    sg.lookupStatement = "SELECT v FROM blobkey WHERE k = :k";
    sg.putStatement = "INSERT OR REPLACE INTO blobkey VALUES (:k, :v)";
    sg.putIfAbsentStatement = "INSERT INTO blobkey VALUES (:k, :v)";
    sg.deleteStatement = "DELETE FROM blobkey WHERE k = :k";
    sg.beginStatement = "BEGIN";
    sg.commitStatement = "COMMIT";
    sg.abortStatement = "ROLLBACK";
    sg.cursorStatement = "SELECT k, v FROM blobkey ORDER BY k ASC";
    sg.cursorLastStatement = "SELECT k, v FROM blobkey ORDER BY k DESC LIMIT 1";
    return sg;
}

SqliteIndex::StatementGroup SqliteIndex::GetIntegerStatementGroup() {
    StatementGroup sg;
    sg.createStatement = "CREATE TABLE intkey(k INTEGER PRIMARY KEY, v BLOB)";
    sg.countStatement = "SELECT COUNT(*) FROM intkey";
    sg.lookupStatement = "SELECT v FROM intkey WHERE k = :k";
    sg.putStatement = "INSERT OR REPLACE INTO intkey VALUES (:k, :v)";
    sg.putIfAbsentStatement = "INSERT INTO intkey VALUES (:k, :v)";
    sg.deleteStatement = "DELETE FROM intkey WHERE k = :k";
    sg.beginStatement = "BEGIN";
    sg.commitStatement = "COMMIT";
    sg.abortStatement = "ROLLBACK";
    sg.cursorStatement = "SELECT k, v FROM intkey ORDER BY k ASC";
    sg.cursorLastStatement = "SELECT k, v FROM intkey ORDER BY k DESC LIMIT 1";
    return sg;
}

bool SqliteIndex::Start(const dedupv1::StartContext& start_context) {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, true);
    CHECK(this->state == CREATED, "Index in invalid state");
    CHECK(this->filename.size() > 0, "No filename specified");
    CHECK(this->estimated_max_item_count > 0, "No max item count specified");

    if (IsIntegerMode()) {
        this->statements = GetIntegerStatementGroup();
    } else {
        this->statements = GetBlobStatementGroup();
    }
    this->db.resize(this->filename.size());
    bool failed = false;
    for (size_t i = 0; i < this->db.size(); i++) {
        sqlite3* new_db = NULL;
        int error_code = sqlite3_open_v2(
            this->filename[i].c_str(),
            &new_db,
            SQLITE_OPEN_READWRITE,
            NULL);
        if (error_code != SQLITE_OK) {
            if (new_db) {
                sqlite3_close(new_db);
                new_db = NULL;
            }
            if (start_context.create()) {
                INFO("Creating index " << this->filename[i]);

                CHECK(File::MakeParentDirectory(this->filename[i], start_context.dir_mode().mode()),
                    "Failed to check parent directories");

                error_code = sqlite3_open_v2(
                    this->filename[i].c_str(),
                    &new_db,
                    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                    NULL);
                if (error_code == SQLITE_OK) {
                    char* errmsg = NULL;
                    if (sqlite3_exec(new_db, statements.createStatement.c_str(), NULL, NULL, &errmsg) != SQLITE_OK) {
                        ERROR("Failed to create table: message " << errmsg);
                        sqlite3_close(new_db);
                        failed = true;
                        break;
                    }
                }

                int localChunkSize = this->chunk_size_; // copy to local int to ensure the correct type, it has to be an int
                if (sqlite3_file_control(new_db, NULL, SQLITE_FCNTL_CHUNK_SIZE, &localChunkSize) != SQLITE_OK) {
                    ERROR("Failed to set chunk size");
                    failed = true;
                    break;
                }

                if (this->preallocate_size_ > 0) {
                    sqlite3_int64 szFile = preallocate_size_ / filename.size();
                    if (sqlite3_file_control(new_db, NULL, SQLITE_FCNTL_SIZE_HINT, &szFile) != SQLITE_OK) {
                        ERROR("Failed to preallocate database file");
                        failed = true;
                        break;
                    }
                }

                CHECK(chmod(this->filename[i].c_str(), start_context.file_mode().mode()) == 0,
                    "Failed to change file permissions: " << this->filename[i]);
                if (start_context.file_mode().gid() != -1) {
                    CHECK(chown(this->filename[i].c_str(), -1, start_context.file_mode().gid()) == 0,
                        "Failed to change file group: " << this->filename[i]);
                }

                // WAL file
                string wal_filename = this->filename[i] + ".wal";

                // Touch/Create WAL file
                File* wal_file = File::Open(wal_filename, O_RDWR | O_CREAT | O_EXCL | O_LARGEFILE, start_context.file_mode().mode());
                CHECK(wal_file, "Error opening storage file: " << wal_filename << ", message " << strerror(errno));
                delete wal_file;
                wal_file = NULL;
                if (start_context.file_mode().gid() != -1) {
                    CHECK(chown(wal_filename.c_str(), -1, start_context.file_mode().gid()) == 0,
                        "Failed to change file group: " << wal_filename);
                }

            }
        }
        if (error_code != SQLITE_OK) {
            // new_db is set even with errors
            ERROR("Failed to open sqlite db: " << this->filename[i] << ", message " << sqlite3_errmsg(new_db));
            sqlite3_close(new_db);
            failed = true;
            break;
        }

        // set pragmas
        if (!failed) {
            uint32_t db_cache_size = this->cache_size / this->filename.size();

            char* errmsg = NULL;
            // for some reason, the prepared statement approach isn't working with pragmas
            string stmt = "PRAGMA cache_size = " + ToString(db_cache_size) + ";";
            if (SQLITE_VERSION_NUMBER >= 3007000) {
                stmt += "PRAGMA journal_mode = WAL;";
            } else {
                WARNING("Fallback to non-wal transaction mode");
                stmt += "PRAGMA journal_mode = TRUNCATE;";
            }
            if (this->sync) {
                stmt += "PRAGMA synchronous = FULL;";
            } else {
                stmt += "PRAGMA synchronous = NORMAL;";
            }

            if (sqlite3_exec(new_db, stmt.c_str(), NULL, NULL, &errmsg) != SQLITE_OK) {
                ERROR("Failed to set pragmas: message " << errmsg);
                sqlite3_close(new_db);
                failed = true;
                break;
            }
        }

        this->db[i] = new_db;
    }
    CHECK(locks_.Init(db.size()), "Failed to init read/write locks");

    item_count_ = GetInitialItemCount();
    this->state = STARTED;
    return !failed;
}

bool SqliteIndex::BeginTransaction(sqlite3* db) {
    CHECK(db, "Database not set");
    char* errmsg = NULL;
    CHECK(sqlite3_exec(db, statements.beginStatement.c_str(), NULL, NULL, &errmsg) == SQLITE_OK,
        "Failed to start transaction: message " << errmsg);
    return true;
}

bool SqliteIndex::AbortTransaction(sqlite3* db) {
    CHECK(db, "Database not set");
    char* errmsg = NULL;
    CHECK(sqlite3_exec(db, statements.abortStatement.c_str(), NULL, NULL, &errmsg) == SQLITE_OK,
        "Failed to abort transaction: message " << errmsg);
    return true;
}

bool SqliteIndex::CommitTransaction(sqlite3* db) {
    CHECK(db, "Database not set");
    char* errmsg = NULL;
    CHECK(sqlite3_exec(db, statements.commitStatement.c_str(), NULL, NULL, &errmsg) == SQLITE_OK,
        "Failed to commit transaction: message " << errmsg);
    return true;
}

sqlite3_stmt* SqliteIndex::GetStatement(sqlite3* db, const string& statement) {
    sqlite3_stmt* stmt = NULL;
    if (sqlite3_prepare_v2(
            db,
            statement.c_str(),
            statement.size(),
            &stmt,
            NULL) != SQLITE_OK) {
        ERROR("Failed to create statement: statement " << statement << ", message " << sqlite3_errmsg(db));
        return NULL;
    }
    return stmt;
}

enum lookup_result SqliteIndex::RawLookup(const void* key, size_t key_size,
                                          void* value, size_t* value_size) {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    ProfileTimer timer(this->lookup_profiling_);

    CHECK_RETURN(this->state == STARTED, LOOKUP_ERROR, "Index not started");
    CHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    int current_db_index = 0;
    CHECK_RETURN(GetDBIndex(key, key_size, &current_db_index), LOOKUP_ERROR, "Cannot get db index");
    sqlite3* current_db = this->db[current_db_index];
    CHECK_RETURN(current_db, LOOKUP_ERROR, "Cannot get db");

    ScopedReadWriteLock scoped_rw_lock(this->locks_.Get(current_db_index));
    CHECK_RETURN(scoped_rw_lock.AcquireReadLock(), LOOKUP_ERROR, "Failed to acquire read lock");

    sqlite3_stmt* select_stmt = GetStatement(current_db, statements.lookupStatement);
    CHECK_RETURN(select_stmt, LOOKUP_ERROR, "Cannot get statement");

    lookup_result r = LOOKUP_ERROR;
    int ec = 0;
    if (IsIntegerMode()) {
        int64_t int_key = 0;
        memcpy(&int_key, key, key_size);
        ec = sqlite3_bind_int64(select_stmt, 1, int_key);
    } else {
        ec = sqlite3_bind_blob(select_stmt, 1, key, key_size, NULL);
    }
    if (ec != SQLITE_OK) {
        ERROR("Failed to bind parameter: " << sqlite3_errmsg(current_db));
    } else {
        // OK
        ec = sqlite3_step(select_stmt);
        if (ec == SQLITE_ROW) {
            int result_size = sqlite3_column_bytes(select_stmt, 0);
            const void* result = sqlite3_column_blob(select_stmt, 0);

            if (result) {
                if (value) {
                    if (result_size > (*value_size)) {
                        ERROR("Illegal value size: " << result_size);
                        r = LOOKUP_ERROR;
                    } else {
                        r = LOOKUP_FOUND;
                        memcpy(value, result, result_size);
                    }
                    if (value_size) {
                        *value_size = 0;
                    }
                } else {
                    r = LOOKUP_FOUND;
                }
                if (value_size) {
                    *value_size = 0;
                }
            } else if (result_size == 0) {
                // result is NULL in this case
                if (value_size) {
                    *value_size = 0;
                }
                r = LOOKUP_FOUND;
            } else {
                ERROR("Result not set: " <<
                    "key " << ToHexString(key, key_size) <<
                    ", result size " << result_size);
            }
        } else if (ec == SQLITE_DONE) {
            r = LOOKUP_NOT_FOUND;
        } else {
            ERROR("Failed to stop query: " << sqlite3_errmsg(current_db) <<
                ", db filename " << filename[current_db_index] <<
                ", error code " << ec);

        }
    }
    if (sqlite3_finalize(select_stmt) != SQLITE_OK) {
        ERROR("Failed to free statement");
        r = LOOKUP_ERROR;
    }
    CHECK_RETURN(scoped_rw_lock.ReleaseLock(), LOOKUP_ERROR, "Failed to release read lock");
    return r;
}

enum lookup_result SqliteIndex::Lookup(const void* key, size_t key_size,
                                       Message* message) {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    ProfileTimer timer(this->lookup_profiling_);

    CHECK_RETURN(this->state == STARTED, LOOKUP_ERROR, "Index not started");
    CHECK_RETURN(key, LOOKUP_ERROR, "Key not set");

    int current_db_index = 0;
    CHECK_RETURN(GetDBIndex(key, key_size, &current_db_index), LOOKUP_ERROR, "Cannot get db index");
    sqlite3* current_db = this->db[current_db_index];
    CHECK_RETURN(current_db, LOOKUP_ERROR, "Cannot get db");

    ScopedReadWriteLock scoped_rw_lock(this->locks_.Get(current_db_index));
    CHECK_RETURN(scoped_rw_lock.AcquireReadLock(), LOOKUP_ERROR, "Failed to acquire read lock");

    sqlite3_stmt* select_stmt = GetStatement(current_db, statements.lookupStatement);
    CHECK_RETURN(select_stmt, LOOKUP_ERROR, "Cannot get statement");

    lookup_result r = LOOKUP_ERROR;
    int ec = 0;
    if (IsIntegerMode()) {
        int64_t int_key = 0;
        memcpy(&int_key, key, key_size);
        ec = sqlite3_bind_int64(select_stmt, 1, int_key);
    } else {
        ec = sqlite3_bind_blob(select_stmt, 1, key, key_size, NULL);
    }
    if (ec != SQLITE_OK) {
        ERROR("Failed to bind parameter: " << sqlite3_errmsg(current_db));
    } else {
        // OK
        ec = sqlite3_step(select_stmt);
        if (ec == SQLITE_ROW) {
            int result_size = sqlite3_column_bytes(select_stmt, 0);
            const void* result = sqlite3_column_blob(select_stmt, 0);

            if (result) {
                if (message) {
                    if (!message->ParseFromArray(result, result_size)) {
                        ERROR("Failed to parse message");
                    } else {
                        r = LOOKUP_FOUND;
                    }
                } else {
                    r = LOOKUP_FOUND;
                }
            } else if (result_size == 0) {
                // result is NULL in this case
                if (message) {
                    message->Clear();
                }
                r = LOOKUP_FOUND;
            } else {
                ERROR("Result not set: " <<
                    "key " << ToHexString(key, key_size) <<
                    ", result size " << result_size);
            }
        } else if (ec == SQLITE_DONE) {
            r = LOOKUP_NOT_FOUND;
        } else {
            ERROR("Failed to stop query: " << sqlite3_errmsg(current_db) <<
                ", db filename " << filename[current_db_index] <<
                ", error code " << ec);

        }
    }
    if (sqlite3_finalize(select_stmt) != SQLITE_OK) {
        ERROR("Failed to free statement");
        r = LOOKUP_ERROR;
    }
    CHECK_RETURN(scoped_rw_lock.ReleaseLock(), LOOKUP_ERROR, "Failed to release read lock");
    return r;
}

enum put_result SqliteIndex::RawPut(
    const void* key, size_t key_size,
    const void* value, size_t value_size) {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    ProfileTimer timer(this->write_profiling_);

    TRACE("Put: key " << ToHexString(key, key_size));

    CHECK_RETURN(this->state == STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= this->max_key_size, PUT_ERROR, "Key size too large");

    return InternalPut(key, key_size, value, value_size);
}

enum put_result SqliteIndex::RawPutBatch(
    const vector<tuple<bytestring, bytestring> >& data) {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    ProfileTimer timer(this->write_profiling_);

    CHECK_RETURN(this->state == STARTED, PUT_ERROR, "Index not started");

    return InternalPutBatch(data);
}

enum put_result SqliteIndex::InternalPutBatch(
    const vector<tuple<bytestring, bytestring> >& data) {
    bool overwrite = false;
    put_result r = PUT_OK;

    DEBUG("Batched put: " << data.size());

    multimap<int, tuple<bytestring, bytestring> > db_assignment;

    /**
     * This loop does two things:
     * - Orders the k/v pairs by the database to execute the inserts in pairs
     * - Checks if the key is valid
     */
    vector<tuple<bytestring, bytestring> >::const_iterator di;
    for (di = data.begin(); di != data.end(); di++) {
        CHECK_RETURN(std::tr1::get<0>(*di).size() <= this->max_key_size, PUT_ERROR, "Key size too large");

        int current_db_index = 0;
        CHECK_RETURN(GetDBIndex(std::tr1::get<0>(*di).data(), std::tr1::get<0>(*di).size(), &current_db_index), PUT_ERROR, "Cannot get db index");
        sqlite3* current_db = this->db[current_db_index];
        CHECK_RETURN(current_db, PUT_ERROR, "Cannot get db");

        db_assignment.insert(make_pair(current_db_index, *di));
    }

    sqlite3* last_db = NULL;
    ReadWriteLock* used_lock = NULL;
    multimap<int, tuple<bytestring, bytestring> >::iterator dai;
    for (dai = db_assignment.begin(); dai != db_assignment.end() && r == PUT_OK; dai++) {
        int current_db_index = dai->first;
        sqlite3* current_db = this->db[current_db_index];
        bytestring& key(std::tr1::get<0>(dai->second));
        bytestring& value(std::tr1::get<1>(dai->second));

        DEBUG("Perform batched put: " << static_cast<void*>(current_db) << ", key " << ToHexString(key.data(), key.size()));

        if (last_db != current_db) {
            if (last_db != NULL) {
                if (!this->CommitTransaction(last_db)) {
                    ERROR("Failed to commit transaction");
                    last_db = NULL;
                    r = PUT_ERROR;
                    break;
                }
            }
            last_db = NULL;

            if (used_lock) {
                if (!used_lock->ReleaseLock()) {
                    ERROR("Failed to release lock");
                    r = PUT_ERROR;
                    break;
                }
            }
            used_lock = NULL;

            ReadWriteLock* next_used_lock = locks_.Get(current_db_index);
            if (!next_used_lock->AcquireWriteLock()) {
                ERROR("Failed to get write lock");
                r = PUT_ERROR;
                break;
            }
            used_lock = next_used_lock;

            if (!this->BeginTransaction(current_db)) {
                ERROR("Failed to begin transaction");
                r = PUT_ERROR;
                break;
            }
            last_db = current_db;
        }

        sqlite3_stmt* stmt = GetStatement(current_db, statements.putIfAbsentStatement);
        if (!stmt) {
            ERROR("Cannot get statement");
            r = PUT_ERROR;
            break;
        }

        int ec1 = 0;
        if (IsIntegerMode()) {
            int64_t int_key = 0;
            memcpy(&int_key, key.data(), key.size());
            ec1 = sqlite3_bind_int64(stmt, 1, int_key);
        } else {
            ec1 = sqlite3_bind_blob(stmt, 1, key.data(), key.size(), NULL);
        }

        int ec2 = sqlite3_bind_blob(stmt, 2, value.data(), value.size(), NULL);
        if (ec1 != SQLITE_OK || ec2 != SQLITE_OK) {
            ERROR("Failed to bind parameter: " << sqlite3_errmsg(current_db));
            r = PUT_ERROR;
        } else {
            // OK
            int ec = sqlite3_step(stmt);
            if (ec == SQLITE_DONE || ec == SQLITE_ROW) {
                r = PUT_OK;
                item_count_++;
            } else if (ec == SQLITE_CONSTRAINT) {
                overwrite = true;
            } else {
                ERROR("Failed to step query: error code " << ec << ", message " << sqlite3_errmsg(current_db));
                r = PUT_ERROR;
            }
        }
        sqlite3_reset(stmt); // do not check the return statement here
        if (sqlite3_finalize(stmt) != SQLITE_OK) {
            ERROR("Failed to free statement");
            r = PUT_ERROR;
            break;
        }
        if (overwrite && r == PUT_OK) {
            sqlite3_stmt* overwrite_stmt = GetStatement(current_db, statements.putStatement);
            if (!overwrite_stmt) {
                ERROR("Cannot get statement");
                r = PUT_ERROR;
            }
            if (IsIntegerMode()) {
                int64_t int_key = 0;
                memcpy(&int_key, key.data(), key.size());
                ec1 = sqlite3_bind_int64(overwrite_stmt, 1, int_key);
            } else {
                ec1 = sqlite3_bind_blob(overwrite_stmt, 1, key.data(), key.size(), NULL);
            }

            ec2 = sqlite3_bind_blob(overwrite_stmt, 2, value.data(), value.size(), NULL);
            if (ec1 != SQLITE_OK || ec2 != SQLITE_OK) {
                ERROR("Failed to bind parameter: " << sqlite3_errmsg(current_db));
                r = PUT_ERROR;
            } else {
                // OK
                int ec = sqlite3_step(overwrite_stmt);
                if (ec == SQLITE_DONE || ec == SQLITE_ROW) {
                    r = PUT_OK;
                } else {
                    ERROR("Failed to step query: error code " << ec << ", message " << sqlite3_errmsg(current_db));
                    r = PUT_ERROR;
                }
            }
            sqlite3_reset(overwrite_stmt); // do not check the return statement here
            if (sqlite3_finalize(overwrite_stmt) != SQLITE_OK) {
                ERROR("Failed to free statement");
                r = PUT_ERROR;
            }
        }
    }

    if (last_db != NULL) {
        // Commit or abort
        if (r == PUT_ERROR) {
            if (!this->AbortTransaction(last_db)) {
                ERROR("Failed to abort transaction");
                r = PUT_ERROR;
            }
        } else {
            if (!this->CommitTransaction(last_db)) {
                ERROR("Failed to commit transaction");
                r = PUT_ERROR;
            }
        }
        last_db = NULL;
    }
    if (used_lock) {
        if (!used_lock->ReleaseLock()) {
            ERROR("Failed to release lock");
            r = PUT_ERROR;
        }
    }
    used_lock = NULL;

    this->version_counter++;
    return r;
}

enum put_result SqliteIndex::InternalPut(
    const void* key, size_t key_size,
    const void* value, size_t value_size) {
    bool overwrite = false;
    int current_db_index = 0;
    CHECK_RETURN(GetDBIndex(key, key_size, &current_db_index), PUT_ERROR, "Cannot get db index");
    sqlite3* current_db = this->db[current_db_index];
    CHECK_RETURN(current_db, PUT_ERROR, "Cannot get db");

    ScopedReadWriteLock scoped_rw_lock(this->locks_.Get(current_db_index));
    CHECK_RETURN(scoped_rw_lock.AcquireWriteLock(), PUT_ERROR, "Failed to acquire write lock");

    sqlite3_stmt* stmt = GetStatement(current_db, statements.putIfAbsentStatement);
    CHECK_RETURN(stmt, PUT_ERROR, "Cannot get statement");

    put_result r = PUT_OK;
    int ec1 = 0;
    if (IsIntegerMode()) {
        int64_t int_key = 0;
        memcpy(&int_key, key, key_size);
        ec1 = sqlite3_bind_int64(stmt, 1, int_key);
    } else {
        ec1 = sqlite3_bind_blob(stmt, 1, key, key_size, NULL);
    }

    int ec2 = sqlite3_bind_blob(stmt, 2, value, value_size, NULL);
    if (ec1 != SQLITE_OK || ec2 != SQLITE_OK) {
        ERROR("Failed to bind parameter: " << sqlite3_errmsg(current_db));
        r = PUT_ERROR;
    } else {
        // OK
        int ec = sqlite3_step(stmt);
        if (ec == SQLITE_DONE || ec == SQLITE_ROW) {
            r = PUT_OK;
            item_count_++;
        } else if (ec == SQLITE_CONSTRAINT) {
            overwrite = true;
        } else {
            ERROR("Failed to step query: error code " << ec << ", message " << sqlite3_errmsg(current_db));
            r = PUT_ERROR;
        }
    }
    sqlite3_reset(stmt); // do not check the return statement here
    if (sqlite3_finalize(stmt) != SQLITE_OK) {
        ERROR("Failed to free statement");
        r = PUT_ERROR;
    }
    if (overwrite && r == PUT_OK) {
        sqlite3_stmt* overwrite_stmt = GetStatement(current_db, statements.putStatement);
        CHECK_RETURN(overwrite_stmt, PUT_ERROR, "Cannot get statement");
        if (IsIntegerMode()) {
            int64_t int_key = 0;
            memcpy(&int_key, key, key_size);
            ec1 = sqlite3_bind_int64(overwrite_stmt, 1, int_key);
        } else {
            ec1 = sqlite3_bind_blob(overwrite_stmt, 1, key, key_size, NULL);
        }

        ec2 = sqlite3_bind_blob(overwrite_stmt, 2, value, value_size, NULL);
        if (ec1 != SQLITE_OK || ec2 != SQLITE_OK) {
            ERROR("Failed to bind parameter: " << sqlite3_errmsg(current_db));
            r = PUT_ERROR;
        } else {
            // OK
            int ec = sqlite3_step(overwrite_stmt);
            if (ec == SQLITE_DONE || ec == SQLITE_ROW) {
                r = PUT_OK;
            } else {
                ERROR("Failed to step query: error code " << ec << ", message " << sqlite3_errmsg(current_db));
                r = PUT_ERROR;
            }
        }
        sqlite3_reset(overwrite_stmt); // do not check the return statement here
        if (sqlite3_finalize(overwrite_stmt) != SQLITE_OK) {
            ERROR("Failed to free statement");
            r = PUT_ERROR;
        }
    }
    this->version_counter++;
    return r;
}

enum put_result SqliteIndex::PutBatch(
    const vector<tuple<bytestring, const Message*> >& data) {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    ProfileTimer timer(this->write_profiling_);

    CHECK_RETURN(this->state == STARTED, PUT_ERROR, "Index not started");

    std::vector<std::tr1::tuple<bytestring, bytestring> > raw_data;
    raw_data.reserve(data.size());

    vector<tuple<bytestring, const Message*> >::const_iterator i;

    for (i = data.begin(); i != data.end(); i++) {
        CHECK_RETURN(std::tr1::get<0>(*i).size() <= this->max_key_size, PUT_ERROR, "Key size too large");
        CHECK_RETURN(std::tr1::get<1>(*i) != NULL, PUT_ERROR, "Message not set");

        const Message* message = std::tr1::get<1>(*i);
        bytestring value;
        CHECK_RETURN(SerializeMessageToString(*message, &value), PUT_ERROR, "Failed to serialize message");
        raw_data.push_back(make_tuple(std::tr1::get<0>(*i), value));
    }

    return InternalPutBatch(raw_data);
}

enum put_result SqliteIndex::Put(const void* key, size_t key_size,
                                 const Message& message) {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    ProfileTimer timer(this->write_profiling_);

    TRACE("Put: key " << ToHexString(key, key_size) << ", value " << message.ShortDebugString());

    CHECK_RETURN(this->state == STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= this->max_key_size, PUT_ERROR, "Key size too large");

    ScopedArray<byte> scoped_array(new byte[message.ByteSize()]);
    CHECK_RETURN(message.SerializeWithCachedSizesToArray(scoped_array.Get()),
        PUT_ERROR,
        "Failed to serialize message: " << message.DebugString());

    return InternalPut(
        key, key_size,
        scoped_array.Get(), message.GetCachedSize());
}

enum put_result SqliteIndex::InternalPutIfAbsent(
    const void* key, size_t key_size,
    const void* value, size_t value_size) {
    int current_db_index = 0;
    CHECK_RETURN(GetDBIndex(key, key_size, &current_db_index), PUT_ERROR, "Cannot get db index");
    sqlite3* current_db = this->db[current_db_index];
    CHECK_RETURN(current_db, PUT_ERROR, "Cannot get db");

    ScopedReadWriteLock scoped_rw_lock(this->locks_.Get(current_db_index));
    CHECK_RETURN(scoped_rw_lock.AcquireWriteLock(), PUT_ERROR, "Failed to acquire write lock");

    sqlite3_stmt* stmt = GetStatement(current_db, statements.putIfAbsentStatement);
    CHECK_RETURN(stmt, PUT_ERROR, "Cannot get statement");

    put_result r = PUT_ERROR;
    int ec1 = 0;
    if (IsIntegerMode()) {
        int64_t int_key = 0;
        memcpy(&int_key, key, key_size);
        ec1 = sqlite3_bind_int64(stmt, 1, int_key);
    } else {
        ec1 = sqlite3_bind_blob(stmt, 1, key, key_size, NULL);
    }

    int ec2 = sqlite3_bind_blob(stmt, 2, value, value_size, NULL);
    if (ec1 != SQLITE_OK || ec2 != SQLITE_OK) {
        ERROR("Failed to bind parameter: " << sqlite3_errmsg(current_db));
    } else {
        // OK
        int ec = sqlite3_step(stmt);
        if (ec == SQLITE_DONE || ec == SQLITE_ROW) {
            item_count_++;
            r = PUT_OK;
        } else if (ec == SQLITE_CONSTRAINT) {
            r = PUT_KEEP;
        } else {
            ERROR("Failed to stop query: " << sqlite3_errmsg(current_db));

        }
    }
    sqlite3_reset(stmt); // do not check the error code
    if (sqlite3_finalize(stmt) != SQLITE_OK) {
        ERROR("Failed to free statement: " << sqlite3_errmsg(current_db));
        r = PUT_ERROR;
    }

    this->version_counter.fetch_and_increment();
    return r;
}

enum put_result SqliteIndex::RawPutIfAbsent(
    const void* key, size_t key_size,
    const void* value, size_t value_size) {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    ProfileTimer timer(this->write_profiling_);

    CHECK_RETURN(this->state == STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= this->max_key_size, PUT_ERROR, "Key size too large");

    return InternalPutIfAbsent(key, key_size, value, value_size);

}

enum put_result SqliteIndex::PutIfAbsent(const void* key, size_t key_size,
                                         const Message& message) {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    ProfileTimer timer(this->write_profiling_);

    CHECK_RETURN(this->state == STARTED, PUT_ERROR, "Index not started");
    CHECK_RETURN(key, PUT_ERROR, "Key not set");
    CHECK_RETURN(key_size <= this->max_key_size, PUT_ERROR, "Key size too large");

    ScopedArray<byte> scoped_array(new byte[message.ByteSize()]);
    CHECK_RETURN(message.SerializeWithCachedSizesToArray(scoped_array.Get()),
        PUT_ERROR,
        "Failed to serialize message: " << message.DebugString());

    return InternalPutIfAbsent(key, key_size, scoped_array.Get(), message.GetCachedSize());
}

enum delete_result SqliteIndex::Delete(const void* key, size_t key_size) {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    ProfileTimer timer(this->write_profiling_);

    CHECK_RETURN(this->state == STARTED, DELETE_ERROR, "Index not started");
    CHECK_RETURN(key, DELETE_ERROR, "Key not set");
    CHECK_RETURN(key_size <= this->max_key_size, DELETE_ERROR, "Key size too large");

    int current_db_index = 0;
    CHECK_RETURN(GetDBIndex(key, key_size, &current_db_index), DELETE_ERROR, "Cannot get db index");
    sqlite3* current_db = this->db[current_db_index];
    CHECK_RETURN(current_db, DELETE_ERROR, "Cannot get db");
    sqlite3_stmt* stmt = GetStatement(current_db, statements.deleteStatement);
    CHECK_RETURN(stmt, DELETE_ERROR, "Cannot get statement");

    delete_result r = DELETE_ERROR;
    int ec = 0;
    if (IsIntegerMode()) {
        int64_t int_key = 0;
        memcpy(&int_key, key, key_size);
        ec = sqlite3_bind_int64(stmt, 1, int_key);
    } else {
        ec = sqlite3_bind_blob(stmt, 1, key, key_size, NULL);
    }
    if (ec != SQLITE_OK) {
        ERROR("Failed to bind parameter: " << sqlite3_errmsg(current_db));
    } else {
        // OK
        ec = sqlite3_step(stmt);
        if (ec == SQLITE_DONE) {
            int c = sqlite3_changes(current_db);
            if (c == 0) {
                r = DELETE_NOT_FOUND;
            } else {
                item_count_--;
                r = DELETE_OK;
            }
        } else {
            ERROR("Failed to stop query: " << sqlite3_errmsg(current_db));

        }
    }
    if (sqlite3_finalize(stmt) != SQLITE_OK) {
        ERROR("Failed to free statement");
        r = DELETE_ERROR;
    }
    this->version_counter.fetch_and_increment();
    return r;
}

SqliteIndex::~SqliteIndex() {
    for (size_t i = 0; i < this->db.size(); i++) {
        if (this->db[i]) {
            if (sqlite3_close(this->db[i]) != SQLITE_OK) {
                WARNING("Failed to close database");
            }
            this->db[i] = NULL;
        }
    }
    this->db.clear();
}

bool SqliteIndex::SupportsCursor() {
    tbb::spin_rw_mutex::scoped_lock scoepd_lock(lock, false);
    return this->filename.size() == 1;
}

IndexCursor* SqliteIndex::CreateCursor() {
    if (this->state != STARTED) {
        return NULL;
    }
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    if (this->db.size() == 1) {
        return new SingleFileSqliteCursor(this, this->db[0]);
    }
    ERROR("Cursor not supported for multi-file index");
    return NULL;
}

IndexIterator* SqliteIndex::CreateIterator() {
    if (this->state != STARTED) {
        return NULL;
    }
    return new SqliteIterator(this);
}

SqliteIterator::SqliteIterator(SqliteIndex* index) {
    index_ = index;
    stmt_ = NULL;
    db_ = NULL;
    db_index_ = -1;
    end_ = true;
    version_counter_ = index->version_counter;
}

SqliteIterator::~SqliteIterator() {
    if (this->stmt_) {
        sqlite3_reset(this->stmt_);
        sqlite3_finalize(this->stmt_);
        this->stmt_ = NULL;
    }
}

enum lookup_result SqliteIterator::Next(void* key, size_t* key_size,
                                        google::protobuf::Message* message) {
    CHECK_RETURN(this->version_counter_ == this->index_->version_counter,
        LOOKUP_ERROR, "Concurrent modification error");
    TRACE("Get next entry: end " << end_ <<
        ", index " << db_index_ <<
        ", db count " << this->index_->db.size());

    while (true) {
        if (end_) {
            // get next or exit
            if (db_index_ + 1 >= index_->db.size()) {
                return LOOKUP_NOT_FOUND;
            }

            if (this->stmt_) {
                sqlite3_reset(this->stmt_);
                sqlite3_finalize(this->stmt_);
                this->stmt_ = NULL;
            }

            // new stmt
            TRACE("Get new statement");
            this->db_index_++;
            this->db_ = index_->db[this->db_index_];
            this->stmt_ = this->index_->GetStatement(this->db_, this->index_->statements.cursorStatement);
            CHECK_RETURN(this->stmt_, LOOKUP_ERROR, "Cannot get statement");
            end_ = false;
        }
        TRACE("Step");
        int ec = sqlite3_step(this->stmt_);
        if (ec == SQLITE_ROW) {
            TRACE("Step: Row");
            if (key) {
                CHECK_RETURN(key_size, LOOKUP_ERROR, "key size not given");
                if (this->index_->IsIntegerMode()) {
                    int64_t result = sqlite3_column_int64(this->stmt_, 0);
                    CHECK_RETURN(*key_size >= sizeof(result), LOOKUP_ERROR, "Too small key size");
                    memcpy(key, &result, sizeof(result));
                    *key_size = sizeof(result);
                } else {
                    int result_size = sqlite3_column_bytes(this->stmt_, 0);
                    const void* result = sqlite3_column_blob(this->stmt_, 0);
                    if (!result) {
                        *key_size = 0;
                        ERROR("Cannot read key under cursor");
                        return LOOKUP_ERROR;
                    }
                    CHECK_RETURN(*key_size >= (size_t) result_size, LOOKUP_ERROR, "Too small key size");
                    memcpy(key, result, result_size);
                    *key_size = result_size;
                }
            }
            if (message) {
                // value
                int result_size = sqlite3_column_bytes(this->stmt_, 1);
                const void* result = sqlite3_column_blob(this->stmt_, 1);
                CHECK_RETURN(result, LOOKUP_ERROR, "Result not set");

                CHECK_RETURN(message->ParseFromArray(result, result_size),
                    LOOKUP_ERROR, "Failed to parse message");
            }
            return LOOKUP_FOUND;
        } else if (ec == SQLITE_DONE) {
            TRACE("Step: Done");
            if (db_index_ + 1 >= index_->db.size()) {
                return LOOKUP_NOT_FOUND;
            } else {
                end_ = true;
                // and next round
            }
        } else {
            ERROR("Failed to get first element");
            return LOOKUP_ERROR;
        }
    }
}

string SqliteIndex::PrintProfile() {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    stringstream sstr;
    sstr << "{";
    sstr << "\"write time\": " << this->write_profiling_.GetSum() << "," << std::endl;
    sstr << "\"lookup time\": " << this->lookup_profiling_.GetSum() << "" << std::endl;
    sstr << "}";
    return sstr.str();
}

uint64_t SqliteIndex::GetPersistentSize() {
    tbb::spin_rw_mutex::scoped_lock scoped_lock(lock, false);
    uint64_t size_sum = 0;
    for (size_t i = 0; i < this->filename.size(); i++) {
        Option<bool> e = File::Exists(this->filename[i]);
        if (e.valid() && e.value()) {
            Option<off_t> s = File::GetFileSize(this->filename[i]);
            if (s.valid()) {
                size_sum += s.value();
            } else {
                // probably the file is not existing
                size_sum += 0;
            }
        }
    }
    return size_sum;
}

uint64_t SqliteIndex::GetEstimatedMaxItemCount() {
    return estimated_max_item_count;
}

uint64_t SqliteIndex::GetInitialItemCount() {
    uint64_t items_sum = 0;
    for (size_t i = 0; i < this->db.size(); i++) {
        if (this->db[i]) {
            sqlite3_stmt* stmt = GetStatement(this->db[i], statements.countStatement);
            CHECK_RETURN(stmt, 0, "Cannot get statement");

            int ec = sqlite3_step(stmt);
            if (ec == SQLITE_ROW) {
                uint32_t count = sqlite3_column_int64(stmt, 0);
                items_sum += count;
            } else {
                WARNING("Failed to execute count statement");
            }
            sqlite3_reset(stmt); // do not check the return statement here
            sqlite3_finalize(stmt);
        }
    }
    return items_sum;
}

uint64_t SqliteIndex::GetItemCount() {
    return item_count_;
}

bool SqliteIndex::GetDBIndex(const void* key, size_t key_size, int* index) {
    CHECK(this->db.size() > 0, "Index not started");
    CHECK(index, "index not started");

    uint32_t hash_value = 0;
    murmur_hash3_x86_32(key, key_size, 0, &hash_value);
    *index = hash_value % this->db.size();
    return true;
}

SingleFileSqliteCursor::SingleFileSqliteCursor(SqliteIndex* index, sqlite3* db) {
    this->index = index;
    this->db = db;
    this->cursor_stmt = NULL;
}

enum lookup_result SingleFileSqliteCursor::First() {
    if (this->cursor_stmt) {
        sqlite3_reset(this->cursor_stmt);
        sqlite3_finalize(this->cursor_stmt);
        this->cursor_stmt = NULL;
    }

    // new stmt
    this->cursor_stmt = this->index->GetStatement(this->db, this->index->statements.cursorStatement);
    CHECK_RETURN(this->cursor_stmt, LOOKUP_ERROR, "Cannot get statement");
    return Next();
}

enum lookup_result SingleFileSqliteCursor::Next() {
    CHECK_RETURN(this->cursor_stmt, LOOKUP_ERROR, "Invalid position");

    lookup_result r = LOOKUP_ERROR;
    int ec = sqlite3_step(this->cursor_stmt);
    if (ec == SQLITE_ROW) {
        r = LOOKUP_FOUND;
    } else if (ec == SQLITE_DONE) {
        r = LOOKUP_NOT_FOUND;
    } else {
        ERROR("Failed to get first element");
        r = LOOKUP_ERROR;
    }
    if (r != LOOKUP_FOUND) {
        if (this->cursor_stmt) {
            sqlite3_reset(this->cursor_stmt);
            sqlite3_finalize(this->cursor_stmt);
            this->cursor_stmt = NULL;
        }
    }
    return r;
}

enum lookup_result SingleFileSqliteCursor::Last() {
    if (this->cursor_stmt) {
        sqlite3_reset(this->cursor_stmt);
        sqlite3_finalize(this->cursor_stmt);
        this->cursor_stmt = NULL;
    }

    // new stmt
    this->cursor_stmt = this->index->GetStatement(this->db, this->index->statements.cursorLastStatement);
    CHECK_RETURN(this->cursor_stmt, LOOKUP_ERROR, "Cannot get statement");
    return Next();
}

enum lookup_result SingleFileSqliteCursor::Jump(const void* key, size_t key_size) {
    // not supported
    return LOOKUP_ERROR;
}

bool SingleFileSqliteCursor::Remove() {
    CHECK_RETURN(this->cursor_stmt, LOOKUP_ERROR, "Invalid position");

    int key_size = sqlite3_column_bytes(this->cursor_stmt, 0);
    const void* key = sqlite3_column_blob(this->cursor_stmt, 0);

    CHECK(this->index->Delete(key, key_size) != DELETE_ERROR, "Failed to delete key");
    CHECK(this->Next() != LOOKUP_ERROR, "Failed to move to next position");
    return true;
}

bool SingleFileSqliteCursor::Get(void* key, size_t* key_size,
                                 Message* message) {
    CHECK(this->cursor_stmt, "Invalid position");

    if (key) {
        CHECK(key_size, "key size not given");
        CHECK(*key_size >= 1, "Illegal key size");

        if (this->index->IsIntegerMode()) {
            int64_t result = sqlite3_column_int64(this->cursor_stmt, 0);
            // we need to know here if the result will fit into the keysize
            // therefore we need to know the real size of the result value
            int32_t result_size = sizeof(result);
            if (result <= UINT8_MAX) {
                result_size = sizeof(UINT8_MAX);
            }else if (result <= UINT16_MAX)  {
                result_size = sizeof(UINT16_MAX);
            }else if (result <= UINT32_MAX)  {
                result_size = sizeof(UINT32_MAX);
            }else if (result <= UINT64_MAX)  {
                result_size = sizeof(UINT64_MAX);
            }
            CHECK(*key_size >= result_size, "Result size to big for key (key_size: " << *key_size << "; key: " << key << "; result_size: " << result_size << "; result: " << result << ")");
            memset(key, 0, *key_size);
            memcpy(key, &result, result_size);
            *key_size = result_size;
        } else {
            int result_size = sqlite3_column_bytes(this->cursor_stmt, 0);
            const void* result = sqlite3_column_blob(this->cursor_stmt, 0);
            if (!result) {
                *key_size = 0;
                ERROR("Cannot read key under cursor");
                return false;
            }
            CHECK(*key_size >= (size_t) result_size, "Too small key size");
            memcpy(key, result, result_size);
            *key_size = result_size;
        }
    }

    if (message) {
        // value
        int result_size = sqlite3_column_bytes(this->cursor_stmt, 1);
        const void* result = sqlite3_column_blob(this->cursor_stmt, 1);
        CHECK(result, "Result not set");

        CHECK(message->ParseFromArray(result, result_size), "Failed to parse message");
    }
    return true;
}

bool SingleFileSqliteCursor::Put(const Message& message) {
    CHECK_RETURN(this->cursor_stmt, LOOKUP_ERROR, "Invalid position");

    if (this->index->IsIntegerMode()) {
        int64_t key = sqlite3_column_int64(this->cursor_stmt, 0);
        CHECK(this->index->Put(&key, sizeof(key), message) != PUT_ERROR, "Failed to update key");
    } else {
        int key_size = sqlite3_column_bytes(this->cursor_stmt, 0);
        const void* key = sqlite3_column_blob(this->cursor_stmt, 0);

        CHECK(this->index->Put(key, key_size, message) != PUT_ERROR, "Failed to update key");
    }
    return true;
}

bool SingleFileSqliteCursor::IsValidPosition() {
    return this->cursor_stmt != NULL;
}

SingleFileSqliteCursor::~SingleFileSqliteCursor() {
    if (this->cursor_stmt) {
        sqlite3_reset(this->cursor_stmt);
        sqlite3_finalize(this->cursor_stmt);
        this->cursor_stmt = NULL;
    }
}

}
}

