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

#ifndef CHUNK_STORE_H__
#define CHUNK_STORE_H__

#include <tbb/atomic.h>

#include <string>
#include <vector>

#include <core/idle_detector.h>
#include <base/profile.h>
#include <core/block_mapping.h>
#include <core/storage.h>
#include <core/log.h>
#include <core/chunk_mapping.h>
#include <base/startup.h>
#include <core/statistics.h>

namespace dedupv1 {

class DedupSystem;

namespace chunkstore {

/**
 * The chunk store is a small frontend before the configuration
 * storage system.
 *
 * It adds some statistics gathering to the storage system.
 */
class ChunkStore : public dedupv1::StatisticProvider {
private:

    /**
     * Statistics about the chunk store
     */
    class Statistics {
public:
        Statistics();

        tbb::atomic<uint64_t> storage_reads_;
        tbb::atomic<uint64_t> storage_real_writes_;
        tbb::atomic<uint64_t> storage_total_writes_;

        tbb::atomic<uint64_t> storage_reads_bytes_;
        tbb::atomic<uint64_t> storage_real_writes_bytes_;
        tbb::atomic<uint64_t> storage_total_writes_bytes_;

        dedupv1::base::Profile time_;
    };

    /**
     * Reference to the storage subsystem
     */
    Storage* chunk_storage_;

    /**
     * Statistics about the chunk store
     */
    Statistics stats_;
public:
    /**
     * Constructor
     * @return
     */
    ChunkStore();

    virtual ~ChunkStore();

    /**
     * Inits the chunk store to use the given storage type.
     * @param storage_type
     * @return true iff ok, otherwise an error has occurred
     */
    bool Init(const std::string& storage_type);

    /**
     * Configures the chunk store.
     * @param option_name
     * @param option
     * @return true if ok, otherwise an error has occurred
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Starts the chunk store.
     *
     * @param system
     * @param start_context context of the startup
     * @return true iff ok, otherwise an error has occurred
     */
    bool Start(
        const dedupv1::StartContext& start_context,
        dedupv1::DedupSystem* system);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool Run();

    /**
     * Stops background threads of the chunk store.
     *
     * The chunk store should not emit log entries during or after this call as the log is usually
     * already stopped.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool Stop(const dedupv1::StopContext& stop_context);

    /**
     * After a successful call, the data address for all chunks that have an SENTINAL
     * value before are set.
     *
     * @param chunk_mappings
     * @param ec Error context that can be filled if case of special errors
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool WriteBlock(
        dedupv1::chunkindex::ChunkMapping* chunk_mappings,
        dedupv1::base::ErrorContext* ec);

    /**
     * On Success, it sets the data address member of the block mapping item.
     *
     * @param item
     * @param buffer
     * @param buffer_size
     * @param ec Error context that can be filled if case of special errors
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool ReadBlock(
        dedupv1::blockindex::BlockMappingItem* item,
        byte* buffer,
        uint32_t chunk_offset,
        uint32_t size,
        dedupv1::base::ErrorContext* ec);

    /**
     * Flushes all open data to disk
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Flush(dedupv1::base::ErrorContext* ec);

    /**
     * returns the underlying storage implementation.
     * @return
     */
    inline Storage* storage();

    /**
     * Persists the statistics of the chunk store and its subcomponents
     * @param prefix
     * @param ps
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * Restores the statistics of the chunk store and its subcomponents.
     *
     * @param prefix
     * @param ps
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * returns lock information about the chunk store and
     * the underlying storage.
     *
     * @return
     */
    virtual std::string PrintLockStatistics();

    /**
     * returns statistical information about the chunk store and
     * the underlying storage.
     *
     * @return
     */
    virtual std::string PrintStatistics();

    /**
     * returns profile information about the chunk store and
     * the underlying storage.
     *
     * @return
     */
    virtual std::string PrintProfile();

    /**
     * returns trace information about the chunk store and the
     * underlying storage
     */
    virtual std::string PrintTrace();

    bool CheckIfFull();

    DISALLOW_COPY_AND_ASSIGN(ChunkStore);
};

Storage* ChunkStore::storage() {
    return this->chunk_storage_;
}

}
}

#endif  // CHUNK_STORE_H__
