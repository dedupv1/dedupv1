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

#ifndef CONTAINER_STORAGE_ALLOC_H__
#define CONTAINER_STORAGE_ALLOC_H__

#include "dedupv1.pb.h"

#include <core/dedup.h>
#include <base/locks.h>
#include <base/index.h>
#include <base/startup.h>
#include <base/bitmap.h>
#include <core/statistics.h>
#include <base/option.h>
#include <base/fileutil.h>
#include <base/profile.h>
#include <core/log_consumer.h>
#include <core/log.h>

#include <tbb/spin_mutex.h>
#include <tbb/concurrent_vector.h>

namespace dedupv1 {
namespace chunkstore {

class Container;
class ContainerStorage;

enum alloc_result {
    /**
     * The allocation failed.
     */
    ALLOC_ERROR,

    /**
     * The place for an container could not be allocated because the container storage
     * is full
     */
    ALLOC_FULL,

    /**
     * The allocation was successful
     */
    ALLOC_OK,
};

/**
 * The container storage allocator strategy
 * controls where to store new container data on disk.
 *
 * The allocator is a common problem in storage system. The both usual
 * methods are bitmap and extend based allocation.
 */
class ContainerStorageAllocator : public dedupv1::StatisticProvider {
    DISALLOW_COPY_AND_ASSIGN(ContainerStorageAllocator);
    public:

    /**
     * Constructor
     * @return
     */
    ContainerStorageAllocator();

    /**
     * Destructor
     * @return
     */
    virtual ~ContainerStorageAllocator();

    /**
     * returns true iff all storage places are given and no more
     * containers can be stored.
     * @return
     */
    virtual bool CheckIfFull();

    /**
     * Called when a container should is created and a new address should be assigned to it.
     *
     * Should only be called after the start of the allocator.
     *
     * @param container reference to the container that should be stored.
     * @param is_new_container true if the container has never been written before, false if
     * we want to get a new address for a merge or delete item operation. The reason for this
     * parameter is that an allocator might not give the last free container place to a newly
     * written container.
     * @param new_address out parameter that should be set to the address of
     * the container. If the call returns true, the address has to be set.
     * The address is ensured to be free for the container. If the container processing
     * fails after the call of OnNewContainer, the client of this
     * method should call OnAbortContainer to that the allocator is able
     * to free the place.
     *
     * @return
     */
    virtual enum alloc_result OnNewContainer(const Container& container,
            bool is_new_container,
            ContainerStorageAddressData* new_address) = 0;

    /**
     * Called when a new container is committed.
     *
     * @param container
     * @param address
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnCommitContainer(const Container& container,
            const ContainerStorageAddressData& address);

    /**
     * Called when the processing with an assigned address fails to that
     * the allocator can free the container place.
     *
     * @param container
     * @param address
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnAbortContainer(const Container& container, const ContainerStorageAddressData& address) = 0;

    /**
     * Configures the allocator. The default implementation logs and
     * error (unknown option) and returns false.
     *
     * No available options
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Stars the allocator. If the call is successful, the
     * allocator should be able to handle requests.
     *
     * @param start_context
     * @param storage
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(const dedupv1::StartContext& start_context, ContainerStorage* storage);

    /**
     * Runs the storage allocator.
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Run();

    /**
     * Called when a container is merged.
     *
     * Should only be called after the start of the allocator.
     *
     * @param data
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnMerge(const ContainerMergedEventData& data);

    /**
     * The new address is already known to the allocator since OnCommit is called.
     * However, the allocator is now free the mark the old address as free.
     *
     * Should only be called after the start of the allocator.
     *
     * @param data
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnMove(const ContainerMoveEventData& data);

    /**
     * Called when a container is deleted.
     * Note: This method is not called, when only a single container item is
     * deleted. The container has to be completely empty.
     * @param data
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnDeleteContainer(const ContainerDeletedEventData& data);

    /**
     * Note: Container may not be committed at this point.
     *
     * Should only be called after the start of the allocator.
     *
     * @param container
     * @param key
     * @param key_size
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool OnRead(const Container& container, const void* key, size_t key_size);

    /**
     *
     * @param event_type type of the event
     * @param event_value value of the event.
     * @param context context information about the event, e.g. the event log id or the replay mode
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool LogReplay(dedupv1::log::event_type event_type,
            const LogEventData& event_value,
            const dedupv1::log::LogReplayContext& context);

    /**
     * Stops the container allocator.
     *
     * @param stop_context
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Stop(const dedupv1::StopContext& stop_context);

    /**
     * Closes the allocator and frees all its resources.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Close();

    /**
     * Checks if the given address is free.
     *
     * @param address
     * @return
     */
    virtual dedupv1::base::Option<bool> IsAddressFree(const ContainerStorageAddressData& address) = 0;

    virtual uint64_t GetActiveStorageDataSize() = 0;

#ifdef DEDUPV1_CORE_TEST
    virtual void ClearData();
#endif
};

#ifdef DEDUPV1_CORE_TEST
        class MemoryBitmapContainerStorageAllocatorTestFriend;
#endif

/**
 * Fully fletched storage allocator strategy that used
 * bitmaps to maintain which container positions
 * are used and free.
 *
 * By default, a container is appended to a randomly chosen file.
 * If container have been merged, newly written container are stored
 * in the than free container positions.
 *
 * A file address has the following structure
 * [file index], [offset inside the file in bytes]
 *
 * The allocator used a write back system. The write back system causes
 * that the bitmap data is not stored
 * on disk directly in the data path, but at some point later, e.g.
 * during the log replay. Usually, the write back system tries
 * to collapse multiple updates of the bitmap into single IOs.
 *.
 */
class MemoryBitmapContainerStorageAllocator : public ContainerStorageAllocator {
#ifdef DEDUPV1_CORE_TEST
        friend class MemoryBitmapContainerStorageAllocatorTestFriend;
#endif
    private:
        DISALLOW_COPY_AND_ASSIGN(MemoryBitmapContainerStorageAllocator);

        /**
         * Statistics about the bitmap storage allocator.
         */
        class Statistics {
            public:
                /**
                 * Constructor
                 */
                Statistics();

                /**
                 * number of allocation operations in the current session
                 */
                tbb::atomic<uint64_t> alloc_count_;

                /**
                 * number of free operations in the current session
                 */
                tbb::atomic<uint64_t> free_count_;

                tbb::atomic<uint64_t> persist_count_;

                /**
                 * time spend in the allocator
                 */
                dedupv1::base::Profile alloc_time_;

                /**
                 * Time spend on disk.
                 */
                dedupv1::base::Profile disk_time_;

                /**
                 * Time spent with log replay
                 */
                dedupv1::base::Profile replay_time_;

        };

        /**
         * State of the bitmap allocator
         */
        enum State {
            CREATED,//!< CREATED
            STARTED,//!< STARTED
            STOPPED //!< STOPPED
        };

        /**
         * A container file object collects all
         * data about a given container file
         */
        class ContainerFile {
            public:
                /**
                 * Constructor
                 */
                ContainerFile() {
                    bitmap_ = NULL;
                    last_free_pos_ = 0;
                }

                /**
                 * free/used bitmap
                 */
                dedupv1::base::Bitmap* bitmap_;

                /**
                 * Current marker for the bitmap. The next file area is searched after
                 * the pointer. This also means that the complete container file is allocated before
                 * a freed area is reused, again.
                 */
                size_t last_free_pos_;

        };

        /**
         * Current state of the bitmap allocator.
         */
        State state_;

        /**
         * Statistics about the allocator.
         */
        Statistics stats_;

        /**
         * Reference to the storage system
         */
        ContainerStorage* storage_;

        /**
         * Index used to store the bitmap on disk.
         */
        dedupv1::base::PersistentIndex* persistent_bitmap_;

        /**
         * Vector of container file data
         */
        tbb::concurrent_vector<ContainerFile> file_;

        /**
         * Locks protecting the single entries of file_
         */
        dedupv1::base::MutexLockVector file_locks_;

        /**
         * approximate number of free container places
         * Note that this variable is (while atomic) updated without a central lock and
         * might be out-dated.
         */
        tbb::atomic<uint32_t> free_count_;

        /**
         * number of total available container places
         */
        uint32_t total_count_;

        /**
         * size of a page
         */
        uint32_t page_size_;

        /**
         * Lock the protected the next_file_ variable
         */
        tbb::spin_mutex next_file_lock_;

        /**
         * Next file to allocate a container on
         */
        int next_file_;

        dedupv1::log::Log* log_;

        /**
         * Some operations should acquire a read lock. This is not import for the operations that are
         * called in the operational state, but very important for the monitors that can be called at very time.
         *
         * The operations that modify the file_ vector should acquire a write lock
         */
        dedupv1::base::ReadWriteLock lock_;

        bool Store(int file_index);

        /**
         * Persist the page containing the alloc info about the given Container
         *
         * @param address The container whose page shall be persisted
         * @return true iff o.k.
         */
        bool EnsurePagePersisted(const ContainerStorageAddressData& address);

        /**
         * Persists a given page in the allocator index
         * Assumes that the file lock is held.
         *
         * @param file_index The file to persist the page for
         * @param item_index The entry, whose page shall be persisted
         * @return true iff o.k.
         */
        bool PersistPage(int file_index, int item_index);

        /**
         * Searches a free address in that file
         * Assumes that the file lock is already held.
         *
         * @param file_index The index of the file to search for
         * @param new_address out-value to give back the address
         * @return true iff o.k.
         */
        bool SearchFreeAddress(int file_index, ContainerStorageAddressData* new_address);

        /**
         * Get the Index of the next File to search for the next Fraa Container on.
         *
         * @return The id if the file.
         */
        int GetNextFile();

        /**
         * Mark the Adress of the given Container as used.
         *
         * This is used during dirty Replay.
         *
         * @param address The address to mark as used
         * @param is_crash_replay if true the system is recovering from a crash
         * @return true iff o.k.
         */
        bool MarkAddressUsed(const ContainerStorageAddressData& address, bool is_crash_replay);

    public:
        /**
         * Creates a new allocator of the current type.
         * @return
         */
        static ContainerStorageAllocator* CreateAllocator();

        /**
         * Registers the allocator type.
         */
        static void RegisterAllocator();

        /**
         * Constructor.
         * @return
         */
        MemoryBitmapContainerStorageAllocator();

        /**
         * Destructor.
         * @return
         */
        virtual ~MemoryBitmapContainerStorageAllocator();

        /**
         * Configures the allocator.
         * @param option_name
         * @param option
         * @return
         */
        virtual bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Starts the bitmap allocator.
         * @param start_context
         * @param storage
         * @return
         */
        virtual bool Start(const dedupv1::StartContext& start_context, ContainerStorage* storage);

        virtual bool Run();

        virtual bool Stop(const dedupv1::StopContext& stop_context);

        virtual bool Close();

        virtual bool CheckIfFull();

        virtual enum alloc_result OnNewContainer(const Container& container, bool is_new_container,
                ContainerStorageAddressData* new_address);

        virtual bool OnAbortContainer(const Container& container, const ContainerStorageAddressData& new_address);

        /**
         * The new address is already known to the allocator since OnCommit is called.
         * However, the allocator is now free the mark the old addresses as free.
         *
         * @param data merge event data of the merge to process. The merge
         * data contains the old addresses and the new addresses.
         * @return
         */
        virtual bool OnMerge(const ContainerMergedEventData& data);

        /**
         * The new address is already known to the allocator since OnCommit is called.
         * However, the allocator is now free the mark the old address as free.
         *
         * @param data move event of the container move to processed by the allocator. The
         * move data contains the old address and the new address
         * @return
         */
        virtual bool OnMove(const ContainerMoveEventData& data);

        virtual bool OnDeleteContainer(const ContainerDeletedEventData& data);

        inline uint64_t free_count() const;

        /**
         * Marks a certain address as free.
         *
         * Is acquiring the file lock
         * TODO (dmeister) Why is it public?
         */
        bool FreeAddress(const ContainerStorageAddressData& address, bool is_crash_replay);

        dedupv1::base::Option<bool> IsAddressFree(const ContainerStorageAddressData& address);

        /**
         *
         * @param event_type type of the event
         * @param event_value value of the event.
         * @param context context information about the event, e.g. the event log id or the replay mode
         * @return
         */
        bool LogReplay(dedupv1::log::event_type event_type, const LogEventData& event_value,
                const dedupv1::log::LogReplayContext& context);

        std::string PrintTrace();

        std::string PrintProfile();

        std::string PrintStatistics();

        virtual uint64_t GetActiveStorageDataSize();

#ifdef DEDUPV1_CORE_TEST
        virtual void ClearData();
#endif
};

uint64_t MemoryBitmapContainerStorageAllocator::free_count() const {
    return free_count_;
}

/**
 * Factory for storage allocators.
 */
class ContainerStorageAllocatorFactory {
        DISALLOW_COPY_AND_ASSIGN(ContainerStorageAllocatorFactory);
    public:
        ContainerStorageAllocatorFactory();
        bool Register(const std::string& name, ContainerStorageAllocator*(*factory)(void));

        /**
         * Creates a new allocator instance with the given type.
         *
         * @param name
         * @return
         */
        static ContainerStorageAllocator* Create(const std::string& name);

        static ContainerStorageAllocatorFactory* GetFactory() {
            return &factory;
        }

    private:
        /**
         * holds a map from a allocator type name to a
         * constructor method.
         */
        std::map<std::string, ContainerStorageAllocator*(*)(void)> factory_map;

        /**
         * Singleton instance
         */
        static ContainerStorageAllocatorFactory factory;
};

}
}

#endif  // CONTAINER_STORAGE_ALLOC_H__
