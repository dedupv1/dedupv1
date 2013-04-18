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

#ifndef STORAGE_H__
#define STORAGE_H__

#include <core/dedup.h>
#include <base/factory.h>
#include <base/error.h>

#include <stdint.h>
#include <map>
#include <string>

#include <core/idle_detector.h>
#include <core/log.h>
#include <core/statistics.h>

namespace dedupv1 {

class DedupSystem;

/**
 * \namespace dedupv1::chunkstore
 * Namespace for classes related to the chunk store
 */
namespace chunkstore {

/**
 * A session can be accessed concurrently, but only by a single request
 */
class StorageSession {
    public:

        virtual ~StorageSession() {
        }

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        virtual bool WriteNew(const void* key, size_t key_size, const void* data,
                size_t data_size,
                bool is_indexed,
                uint64_t* address,
                dedupv1::base::ErrorContext* ec) = 0;

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Read(uint64_t address, const void* key, size_t key_size,
                void* data, size_t* data_size, dedupv1::base::ErrorContext* ec);

        /**
         * Deletes the record from the storage system.
         *
         * The default implementation simply returns true.
         *
         * @param address
         * @param key_list
         * @param ec
         * @return
         */
        virtual bool Delete(uint64_t address, const std::list<bytestring>& key_list, dedupv1::base::ErrorContext* ec);

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        bool Delete(uint64_t address, const byte* key, size_t key_size, dedupv1::base::ErrorContext* ec);

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Close();
};

/**
 * Type for the commit state of a address
 */
enum storage_commit_state {
    STORAGE_ADDRESS_ERROR = 0,              //!< STORAGE_ADDRESS_ERROR
    STORAGE_ADDRESS_COMMITED = 1,           //!< STORAGE_ADDRESS_COMMITED
    STORAGE_ADDRESS_NOT_COMMITED = 2,       //!< STORAGE_ADDRESS_NOT_COMMITED
    STORAGE_ADDRESS_WILL_NEVER_COMMITTED = 3//!< STORAGE_ADDRESS_WILL_NEVER_COMMITTED
};

/**
 * The Storage system is used to store and read chunks of data.
 *
 * While it was a nice idea to have polymorphism for the storage system,
 * it was impossible to develop a crash-safe fast system with it.
 * Currently, there is only one implementation (container-storage) and
 * nearly all other components depend on the fact that the container storage
 * is used.
 */
class Storage : public dedupv1::StatisticProvider {
    private:
    DISALLOW_COPY_AND_ASSIGN(Storage);

    static MetaFactory<Storage> factory_;
    public:

    static MetaFactory<Storage>& Factory();

    /**
     * storage address used only for the empty chunk (-2).
     * This storage address is not valid to be ever saved persistently.
     */
    static const uint64_t EMPTY_DATA_STORAGE_ADDRESS;

    /**
     * storage address used when no legal storage address is known (-1).
     * This storage address is not valid to be ever saved persistently.
     */
    static const uint64_t ILLEGAL_STORAGE_ADDRESS;

    /**
     * Constructor
     * @return
     */
    Storage();

    /**
     * Destructor.
     * @return
     */
    virtual ~Storage();

    /**
     * Inits a storage implementation
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Init();

    /**
     * Sets an option of an storage implementation. set_option should only be called before calling start
     *
     * No available options
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Starts a storage system. After a successful start the write, and read calls should work.
     *
     * @param start_context
     * @param system
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(const dedupv1::StartContext& start_context, DedupSystem* system);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Run();

    /**
     * Stops the storage
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Stop(const dedupv1::StopContext& stop_context);

    /**
     * Closes the storage
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Close();

    /**
     * Abstract method that creates a new storage session
     * @return
     */
    virtual StorageSession* CreateSession() = 0;

    /**
     * Waits if the container is currently in the write cache or in the bg committer
     */
    virtual enum storage_commit_state IsCommittedWait(uint64_t address) = 0;

    /**
     * Checks if a given address is committed or not
     * @param address
     * @return
     */
    virtual enum storage_commit_state IsCommitted(uint64_t address) = 0;

    /**
     * Flushes all open data to disk.
     *
     * Might block for a longer time (seconds) and should
     * therefore not used in the critical data path.
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Flush(dedupv1::base::ErrorContext* ec);

    /**
     * Checks if the given address might represent a valid (but
     * not necessary committed) address or if it contains
     * a special magic number
     *
     * @param address
     * @param allow_empty
     * @return
     */
    static bool IsValidAddress(uint64_t address, bool allow_empty = false);

    virtual uint64_t GetActiveStorageDataSize() = 0;

    virtual bool CheckIfFull();
};

}
}

#endif  // STORAGE_H__
