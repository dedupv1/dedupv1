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

#ifndef INDEX_H__
#define INDEX_H__

#include <base/base.h>
#include <base/startup.h>
#include <base/factory.h>

#include <google/protobuf/message.h>

#include <map>
#include <string>
#include <tr1/tuple>
#include <vector>

namespace dedupv1 {
namespace base {

class IndexCursor;
class IndexIterator;
class PersistentIndex;
class MemoryIndex;

/**
 * Enumeration as the result type for lookup operations
 */
enum lookup_result {
    /**
     * An error occured during the lookup.
     */
    LOOKUP_ERROR,

    /**
     * a valid entry for the given read/search/lookup operation was found
     */
    LOOKUP_FOUND,

    /**
     * no valid entry for a given key could be found.
     */
    LOOKUP_NOT_FOUND
};

/**
 * Enumeration as the result type for put operations
 */
enum put_result {
    /**
     * An error occured during the put operation
     */
    PUT_ERROR,

    /**
     * The put operation was successful
     */
    PUT_OK,

    /**
     * The data was not written, because the exactly same
     * data has been written before. Usually this is equal to PUT_OK, but
     * depending on the situation a different behavior is better.
     *
     * Not all index implementation provide this result.
     */
    PUT_KEEP
};

/**
 * Enumeration as the result type for delete options
 */
enum delete_result {
    /**
     * An error occured during the delete operation
     */
    DELETE_ERROR,

    /**
     * The delete operation was successful. A key was deleted.
     */
    DELETE_OK,

    /**
     * The key to delete was not found in the index
     */
    DELETE_NOT_FOUND
};

/**
 * Enumeration of the capabilities of indexes.
 * As the capabilities are used as bit flag, only powers of 2 should be used as
 * enumeration constants.
 */
enum index_capability {
    NO_CAPABILITIES = 0,

    /**
     * Capability to support a persistent item count.
     */
    PERSISTENT_ITEM_COUNT = 1,

    /**
     * Capability to support iterators
     */
    HAS_ITERATOR = 2,

    /**
     * Capability to support a write back cache.
     * However, an index with this Capability might support this feature only
     * with a certain configuration.
     */
    WRITE_BACK_CACHE = 4,

    /**
     * Capability that DELETE_NOT_FOUND is returned when
     * a key is deleted that do not exists.
     */
    RETURNS_DELETE_NOT_FOUND = 8,

    RAW_ACCESS = 16,

    /**
     * If an index has this capability it has an optimized implementation
     * of batched operations
     */
    NATIVE_BATCH_OPS = 32,

    /**
     * Supports an atomic compare and swap operation
     */
    COMPARE_AND_SWAP = 64,

    /**
     * Supports the PutIfAbsent operation
     */
    PUT_IF_ABSENT = 128,
};

enum cache_lookup_method {
    /**
     * Normal lookup method,
     * Checks cache and persistent. Allows dirty
     */
    CACHE_LOOKUP_DEFAULT,

    CACHE_LOOKUP_ONLY,

    /**
     * Always goes to disk
     */
    CACHE_LOOKUP_BYPASS
};

enum cache_dirty_mode {
    CACHE_ONLY_CLEAN,

    CACHE_ALLOW_DIRTY
};

/**
 * An Index is a data structure with basically three operations:
 * Put, Lookup, Delete
 *
 * It is therefore similar to a classical dictionary if used
 * in-memory ore a key-value store if persistent.
 *
 * In most cases, subclasses should not inherit from Index directly, but the
 * specializations MemoryIndex and PersistentIndex
 */
class Index {
        DISALLOW_COPY_AND_ASSIGN(Index);
    private:
        /**
         * Flag if the index is persistent or not
         */
        bool persistent_;

        /**
         * capabilities of the index.
         * The capability is an bit-union of all capabilities of the index
         */
        int capabilities_;

        /**
         * Factory for all indexes
         */
        static MetaFactory<Index> factory_;
    public:

        /**
         * Factory to create new index objects
         */
        static MetaFactory<Index>& Factory();

        /**
         * Constructor
         * @param persistent (Usually a constant) that indicates if the index is persistent or volatile (in-memory). The persistence
         * should only depend on the index type and is forbidden to change during runtime.
         * @param capability other capability of the index. Capability are also not allowed to change depending on the configuration or
         * on the runtime state. The capability is an bit-union of all capabilities of the index
         */
        explicit Index(bool persistent, int capabilities);

        /**
         * Destructor
         */
        virtual ~Index();

        /**
         * Configures the index.
         * The valid options and option values are implementation dependent.
         * However, there are some conventions:
         * - Every filename that is configured should have the string "filename" in the option name
         *
         * If an subclass does not handle the option name, this base implementation should be called.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Starts the index.
         * The index operations should work after a successful start. The configuration should not be
         * changed after that. New files should only be created when start_context.create() is true.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Start(const dedupv1::StartContext& start_context) = 0;

        /**
         * Looks up a given key in the index and can return the value.
         *
         * @param key key of the item to lookup (Should not be NULL)
         * @param key_size size of the item key
         * @param message pointer to a message to fill. Can be NULL. If it is NULL, the Lookup
         * operation checks if the key exists or not.
         *
         * @return LOOKUP_ERROR if an error occurred during the lookup. LOOKUP_FOUND if a item
         * with the key was found. If message was set, the message should be filled. LOOKUP_NOT_FOUND is returned
         * if no item with the given key could be found.
         */
        virtual enum lookup_result Lookup(
                const void* key, size_t key_size, google::protobuf::Message* message) = 0;

        /**
         * Puts a new key/value pair in the index.
         *
         * Existing entries with the same key should be overwritten.
         *
         * Note: Returning a PUT_OOK result is optional. An implementation
         * is free to return PUT_OK instead.
         *
         * @param key
         * @param key_size
         * @param message message to write to the index
         *
         * @return
         */
        virtual enum put_result Put(
                const void* key, size_t key_size,
                const google::protobuf::Message& message) = 0;

        /**
         * Puts the key/value-pair in the index if there doesn't
         * exists an entry with the same key.
         * The lookup/put is guarantedd to be atomic.
         *
         * This operation is optional. It is only supported
         * by an index if it has the capability PUT_IF_ABSENT.
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
         * Deletes the entry with the given key.
         *
         * The index implementation should DELETE_NOT_FOUND iff there is no entry for
         * the given key. DELETE_OK should be returned iff an entry has been removed
         *
         * @param key
         * @param key_size
         * @return
         */
        virtual enum delete_result Delete(const void* key, size_t key_size) = 0;

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
         * batched raw updates.
         *
         * By default implementation delegating all updates to RawPut
         */
        virtual enum put_result RawPutBatch(
                const std::vector<std::tr1::tuple<bytestring, bytestring> >& data);

        /**
         * batched updates.
         *
         * By default implementation delegating all updates to Put
         */
        virtual enum put_result PutBatch(
                const std::vector<std::tr1::tuple<bytestring, const google::protobuf::Message*> >& data);

        /**
         * Used for raw lookups
         */
        virtual enum lookup_result RawLookup(const void* key, size_t key_size,
                void* value, size_t* value_size);

        virtual enum put_result CompareAndSwap(const void* key, size_t key_size,
                const google::protobuf::Message& message,
                const google::protobuf::Message& compare_message,
                google::protobuf::Message* result_message);

        /**
         * Closes the index and frees it resources
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Close();

        /**
         * returns the number of items in the index.
         *
         * As such an value is hard to keep correct in the presence of crashes, the value
         * is not guaranteed to be correct for persistent indexes. If the index has the
         * capability PERSISTENT_ITEM_COUNT, the index guarantees that the number is correct even
         * after restarts and crashes.
         *
         * @return
         */
        virtual uint64_t GetItemCount() = 0;

        /**
         * Prints lock information about the index
         * @return
         */
        virtual std::string PrintLockStatistics();

        /**
         * Prints profile information about the index
         * @return
         */
        virtual std::string PrintProfile();

        /**
         * Prints trace statistics about the index
         * @return
         */
        virtual std::string PrintTrace();

        /**
         * returns true if the index is a persistent index.
         * That means that in general the data is still available
         * after the index as been closed and reopend with the same
         * configuration.
         *
         * Iff IsPersistent() returns true, AsPersisentIndex should not return NULL
         * Iff IsPersistent() returns false, As MemoryIndex should not return NULL
         *
         * @return true iff the index is persistent
         */
        bool IsPersistent();

        /**
         * Checks if the index has the given capability
         * @return true iff the index has the given capability
         */
        virtual bool HasCapability(enum index_capability cap);

        /**
         * if the index is a persistent index, the index is returned as persistent
         * index by this method.
         *
         * @return
         */
        PersistentIndex* AsPersistentIndex();

        /**
         * if the index is a persistent index, the index is returned as persistent
         * index by this method.
         *
         * @return
         */
        MemoryIndex* AsMemoryIndex();

        /**
        * Create a new iterator. All persistent index should support an
        * iterator. Memory indexes can support the iterator. The support can be checked via
        * the HAS_ITERATOR capability.
        *
        * @return a new iterator pointer or NULL if an error occurred.
        */
        virtual IndexIterator* CreateIterator();
};

/**
 * Base class for all indexes that store the data in volatile memory
 */
class MemoryIndex : public Index {
    public:
        /**
         * Constructor.
         */
        MemoryIndex(int capabilities);

        /**
         * Removes all elements from the memory index
         */
        virtual bool Clear() = 0;

        /**
         * returns the number of items in the index.
         * For memory indexes, this value should be correct.
         *
         * @return the number of items currently stored in the index
         */
        virtual uint64_t GetItemCount() = 0;

        /**
         * returns the size of the index in memory
         * @return the size of the index in memory in bytes. The value can be approximate.
         */
        virtual uint64_t GetMemorySize() = 0;
};

/**
 * Base class for all index that store the data in a persistent memory.
 */
class PersistentIndex : public Index {
    public:
        /**
         * Constructor
         */
        PersistentIndex(int capabilities);

        /**
         * Configures the persistent index
         */
        virtual bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Persistent space is always limited. Each index can be full.
         *
         * @return an estimated maximal number of items the index can store. This is useful to
         * calculate the fill ratio of the index. An index can, but must not execute the insertion
         * of new items
         */
        virtual uint64_t GetEstimatedMaxItemCount() = 0;

        /**
         * returns true if the started index supports to creation
         * of cursors.
         * @return
         */
        virtual bool SupportsCursor();

        /**
         * Returns the size on disk in bytes
         */
        virtual uint64_t GetPersistentSize() = 0;

        /**
         * Creates a new index cursor.
         * @return a new cursor pointer or NULL if cursors are not
         * supported or an error happened
         */
        virtual IndexCursor* CreateCursor();

        /**
         * May lookup dirty data if the index has the write-back cache capability
         * Otherwise the normal lookup method is used
         */
        virtual enum lookup_result LookupDirty(const void* key, size_t key_size,
                enum cache_lookup_method cache_lookup_type,
                enum cache_dirty_mode dirty_mode,
                google::protobuf::Message* message);

        /**
         * May Put data into the write back index if the index has the
         * write back cache capability.
         * Otherwise the normal put method is used
         */
        virtual enum put_result PutDirty(const void* key, size_t key_size,
                    const google::protobuf::Message& message, bool pin);

        /**
         * Ensures that the last write of the given key and key size is persistent.
         * May push data to disk if the index has the write back cache capability.
         *
         * May return true if the key is never written.
         * Always should return true if the index has not the write back
         * cache capability.
         */
        virtual enum put_result EnsurePersistent(const void* key, size_t key_size, bool* pinned);

        /**
         * returns true if the index has the write back capability and
         * the configuration allows the usage of the write-back cache
         */
        virtual bool IsWriteBackCacheEnabled();

        virtual enum lookup_result ChangePinningState(const void* key, size_t key_size, bool new_pin_state);

        virtual uint64_t GetDirtyItemCount();

        virtual uint64_t GetTotalItemCount();

        virtual uint64_t GetEstimatedMaxCacheItemCount();

        virtual bool TryPersistDirtyItem(
                uint32_t max_batch_size,
                uint64_t* resume_handle,
                bool* persisted);

        /**
         */
        virtual bool DropAllPinned();

        /**
         */
        virtual bool PersistAllDirty();
};

/**
 * An ID based index supports only positive 64-bit values as keys. They
 * therefore present a constraint on the normal index implementations.
 *
 * The key is here used as an id. An id based index usually a kind of
 * persistent array as a fast, but very simple data structure.
 *
 */
class IDBasedIndex : public PersistentIndex {
    public:
        /**
         * Constructor
         */
        explicit IDBasedIndex(int capabilities);

        /**
         * Destructor
         */
        virtual ~IDBasedIndex();

        /**
         * Persistent space is always limited. Each index can be full.
         *
         * @return an estimated maximal number of items the index can store. This is useful to
         * calculate the fill ratio of the index. An index can, but must not execute the insertion
         * of new items
         */
        virtual uint64_t GetEstimatedMaxItemCount();

        /**
         * Maximal supported id (aka key) of by the index.
         * Should only be called after Start.
         *
         * @return
         */
        virtual int64_t GetLimitId() = 0;
};

/**
 * An iterator is used to iterator over all keys.
 *
 * An iterator is much more limited than a cursor. In particular, there is no specification
 * of an ordering, there is no need to support multiple iterators at once. The iterator might
 * fail if the index is modified between calls.
 *
 * Every disk based index should support iteration. A memory-based index can support it. The support
 * can be checked via the HAS_ITERATOR capability
 */
class IndexIterator {
        DISALLOW_COPY_AND_ASSIGN(IndexIterator);
    public:
        /**
         * Constructor
         * @return
         */
        IndexIterator();

        /**
         * Destructor
         * @return
         */
        virtual ~IndexIterator();

        /**
         * Get the next key/value pair if possible.
         * If LOOKUP_NOT_FOUND is returned, the last key has been iteratored.
         *
         * @param key
         * @param key_size
         * @param message
         * @return
         */
        virtual enum lookup_result Next(void* key, size_t* key_size,
                google::protobuf::Message* message) = 0;

};

/**
 * A cursor is a much more complex access method than an iterator.
 * It allows jump, and allows modifying operations
 * like Put at the current cursor position and Remove the current
 * cursor position.
 *
 * The cursor position is not valid before a first call of
 * First(), Last() or Jump()
 */
class IndexCursor {
        DISALLOW_COPY_AND_ASSIGN(IndexCursor);
    public:
        /**
         * Constructor
         * @return
         */
        IndexCursor();

        /**
         * Destructor
         * @return
         */
        virtual ~IndexCursor();

        /**
         * Sets the cursor to the first position
         * @return
         */
        virtual enum lookup_result First() = 0;

        /**
         * Moves the cursor to the next position
         * @return
         */
        virtual enum lookup_result Next() = 0;

        /**
         * Moves the cursor to the last position
         * @return
         */
        virtual enum lookup_result Last() = 0;

        /**
         * Moves the cursor to the first position after the key
         *
         * @param key
         * @param key_size
         * @return
         */
        virtual enum lookup_result Jump(const void* key, size_t key_size) = 0;

        /**
         * If a cursor position is removed, the cursor is set to a new (next) position.
         * However, if there are no other positions, the position is undefined.
         *
         * To check if the position is valid, a check using IsValidPosition() might be necessary.
         * @return
         */
        virtual bool Remove() = 0;

        /**
         * Gets the key and the value at the current cursor position
         *
         * @param key
         * @param key_size
         * @param message
         * @return
         */
        virtual bool Get(void* key, size_t* key_size,
                google::protobuf::Message* message) = 0;

        /**
         * Updates the value at the current cursor position.
         * @param message
         * @return
         */
        virtual bool Put(const google::protobuf::Message& message) = 0;

        /**
         * Checks if the current cursor position is valid.
         *
         * @return
         */
        virtual bool IsValidPosition() = 0;
};

}
}

#endif  // INDEX_H__
