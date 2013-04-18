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

#ifndef CONTAINER_H__
#define CONTAINER_H__

#include <core/dedup.h>
#include <core/fingerprinter.h>
#include <base/compress.h>
#include <base/fileutil.h>

#include <gtest/gtest_prod.h>

#include <ctime>
#include <string>
#include <list>
#include <set>
#include <vector>

namespace dedupv1 {
namespace chunkstore {


/**
 * A container item stored the chunk data of exactly one chunk.
 * The key of the container items is the fingerprint of the chunk data.
 */
class ContainerItem {
    private:

        /**
         * Fingerprint key of the container item.
         * This is equal to the fingerprint/key of the
         * chunk
         */
        byte key_[dedupv1::Fingerprinter::kMaxFingerprintSize];

        /**
         * Size of the key.
         * key_size <= Fingerprinter::kMaxFingerprintSize
         */
        size_t key_size_;

        /**
         * Offset of the data of the container item in the
         * container data. The offset is calculated from
         * the beginning of the container, not from the
         * beginning of the container data area.
         */
        size_t offset_;

        /**
         * on-disk size of the item.
         * It is the possibly compressed size of the item data plus the
         * size of the ContainerItemValueData message.
         */
        size_t item_size_;

        /**
         * Uncompressed data size of the container item
         */
        size_t raw_size_;

        /**
         * Flag if the container item is deleted and is allowed
         * to be deleted from the container eventually.
         */
        bool deleted_;

        /**
         * container if of the container the item has been added in the first place, e.g.
         * before any merging.
         */
        uint64_t original_id_;

        /**
         * Indicates if the container item should have a corresponding
         * entry in the chunk index.
         */
        bool is_indexed_;

    public:
        /**
         * Constructor
         */
        ContainerItem(const byte* key,
            size_t key_size,
            size_t offset,
            size_t raw_size,
            size_t item_size,
            uint64_t original_id,
            bool is_indexed);

        /**
         * returns the key of the container item.
         * The key of the container item is the fingerprint of the data.
         *
         * @return
         */
        inline const byte* key() const;


        inline size_t key_size() const;

        /**
         * Returns a developer-readable (hex) representation of the key string.
         * @return
         */
        std::string key_string() const;

        inline size_t offset() const;

        /**
         * The item size denotes the size of the complete item data inside the container data area.
         * This value must be separated from the raw size (aka the chunk size) and the "on disk size"
         * after an eventual compression.
         *
         * @return
         */
        inline size_t item_size() const;
        inline size_t raw_size() const;

        inline bool is_deleted() const;

        /**
         * The original container id of an item is the id of the container to which the item
         * was added at the first time. The primary id of the container might change a lot, but at
         * all times the original id of all times have to be the primary or one of the secondary ids
         * of the container.
         *
         * We maintain the original id because this is necessary for the correct importing into the chunk index.
         * @return
         */
        inline uint64_t original_id() const;

        /**
         * true iff the item should have a corresponding entry in the chunk index.
         */
        inline bool is_indexed() const;

        /**
         * Tests for equality.
         * @param item container item to compare with
         * @return
         */
        bool Equals(const ContainerItem& item) const;

        std::string DebugString() const;

        static bool Compare(const ContainerItem& i1, const ContainerItem& i2);

        friend class Container;
};

/**
 * A container is a persistent data item that stores the data of multiple chunks (100 - 1000).
 * The on disk data-layout is
 * -------------------------
 * - MD - Data             -
 * -------------------------
 * The complete size is the container size (usually around 4-10 MB).
 * The MD size is always the meta data size (a constant).
 *
 * In the data area multiple container items are stored. Each item
 * consists of the following structure:
 * ---------------------------------------------------------------------
 * - Int - ContainerValueDataItem - Data                               -
 * ---------------------------------------------------------------------
 *
 * Each entry in the meta data section points to an specific (non-overlapping) such
 * region (offset / on_disk_size). The meta data item and the region together form a container
 * item which presents a stored chunk data.
 */
class Container {
    private:
        DISALLOW_COPY_AND_ASSIGN(Container);
        friend class ContainerTest;
        FRIEND_TEST(ContainerTest, SerializeContainer);

        /**
         * Current data position.
         * pos - META_DATA_SIZE gives you the current on-disk size of all data in the container
         */
        size_t pos_;

        /**
         * After deleting items, the space is not directly freed, but we hold a record how much
         * space is actually used by active items.
         */
        size_t active_data_size_;

        /**
         * Size of the container (usually 4 MB).
         */
        size_t container_size_;

        /**
         * Primary id of the container.
         * During merge operations, the primary id of the container can change. The new primary id
         * is the least used container id of both containers. Used here means that there exists a non-deleted item with that
         * container id. All other used
         * ids are collected into the secondary id list.
         */
        uint64_t primary_id_;

        /**
         * Secondary ids of the container.
         * If two containers are merged, the least used container id is the new primary id and all other used
         * ids become the new secondary ids. All id that are not used anymore are not used anymore and should be deleted
         * from the container storage meta data index during merge.
         */
        std::set<uint64_t> secondary_ids_;

        /**
         * List of container items of the container.
         * The items are maintained in a sorted fashion by fingerprint to allow a fast binary search.
         */
        std::vector<ContainerItem*> items_;

        /**
         * Note: We do not use items.size() because IsFull is called very often and items.size() takes some time.
         * The item count currently contains the deleted and the undeleted items.
         */
        int32_t item_count_;

        /**
         * Flag signaling if the container is already stored or not.
         * If a container is stored, the operations allowed are limited, e.g. adding a new
         * item is forbidden. The only mutable allowed operation is the merging of two containers into a
         * new container.
         */
        bool stored_;

        /**
         * container data.
         * This includes the meta data part that is not updated and written during the in-memory
         * operations). The meta data part is only serialized and unserializes during load and read operations.
         */
        byte* data_;

        /**
         * flag that indicates if the container has loaded only its meta data.
         * Certain data operations are forbidden in this state.
         */
        bool metaDataOnly_;

        /**
         * Time the container has been committed or merged.
         * Set to "0" if the container has not been committed before.
         *
         * As the commit time uses the system clock, it should only be used for documentation purposes.
         */
        time_t commit_time_;

        /**
         * Returns a mutable pointer to the data
         * @return
         */
        inline byte* mutable_data();

        static void InsertItemSorted(std::vector<ContainerItem*>* items, ContainerItem* item);
    public:
        static const uint64_t kLeastValidContainerId = 1;

        /**
         * The default container size
         */
        static uint32_t const kDefaultContainerSize = 4 * 1024 * 1024;

        /**
         * The minimal size of chunks are can be compressed.
         */
        static const size_t kMinCompressedChunkSize = 128;

        /**
         * Size of the meta data region
         */
        static uint32_t const kMetaDataSize = 124 * 1024;

        /**
         * Maximal (serialized) size of an item metadata. Used to detect
         * if there is enough space for a new item to be added.
         */
        static const size_t kMaxSerializedItemMetadataSize = 84;

        /**
         * Constructor.
         */
        Container();

        /**
         * Destructor.
         * @return
         */
        ~Container();

        /**
         * Inits the container to use the given primary id and the given container size.
         *
         * @param id
         * @param container_size
     * @return true iff ok, otherwise an error has occurred
         */
        bool Init(uint64_t id, size_t container_size);

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        bool InitInMetadataOnlyMode(uint64_t id, size_t container_size);

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        void Reuse(uint64_t id);

        bool CopyItem(const Container& parent_container, const ContainerItem& item);

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        bool AddItem(const byte* key, size_t key_size,
                const byte* data, size_t data_size,
                bool is_indexed,
                dedupv1::base::Compression* comp);

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        bool DeleteItem(const byte* key, size_t key_size);

        inline std::vector<ContainerItem*>& items();
        inline const std::vector<ContainerItem*>& items() const;

        /**
         * Returns the primary id
         * @return
         */
        inline uint64_t primary_id() const;

        inline const std::set<uint64_t>& secondary_ids() const;

        inline uint32_t data_position() const;

        inline uint32_t total_data_size() const;
        inline uint32_t active_data_size() const;

        inline uint32_t item_count() const;
        inline size_t container_size() const;

        inline std::time_t commit_time() const;

        /**
         * Unserialized metadata from the beginning section of the container
         * @return 0 means error, otherwise the length of the meta data
         */
        bool UnserializeMetadata(bool verify_checksum);

        /**
         * Serializes the metadata to the beginning section of the container
         * @return 0 means error, otherwise the length of the meta data
         */
        size_t SerializeMetadata(bool calculate_checksum);

        /**
         * Stores the container to the given file at the given offset. Every
         * contents at the given position is overwritten without any further notice.
         *
         * The container must be valid (id set, etc) for this method.
         *
         * @param file file to store the container to
         * @param offset offset inside the file to store the container
         * @param calculate_checksum calculates the checksum of the given container and
         * store the checksum besides the data.
     * @return true iff ok, otherwise an error has occurred
         */
        bool StoreToFile(dedupv1::base::File* file, off_t offset, bool calculate_checksum);

        /**
         * Loads the container from the given file at the given offset.
         * The primary id of the container must be set to the primary or any secondary id of the container
         * at that data position.
         * The primary id is changed to the stored primary id if a secondary id has been set.
         *
         * @param file
         * @param offset
         * @param verify_checksum iff true, the checksum of the container on disk
         * should be verified.
     * @return true iff ok, otherwise an error has occurred
         */
        bool LoadFromFile(dedupv1::base::File* file, off_t offset, bool verify_checksum);

        bool CopyFrom(const Container& container, bool copyId = false);

        /**
         * Tests for equality.
         * The next pointer (used for C style linked lists) is not part of the equality
         * definition.
         * @param container
         * @return
         */
        bool Equals(const Container& container) const;

        /**
         * Checks (using a simple heuristic) if the container is too full to add a new
         * item with the given fp_size and the given data size.
         *
         * @param fp_size
         * @param data_size
         * @return
         */
        inline bool IsFull(size_t fp_size, size_t data_size);

        /**
         * Copies the raw (uncompressed) data of the given container item.
         *
         * @param item
         * @param dest
         * @param dest_size
     * @return true iff ok, otherwise an error has occurred
         */
        bool CopyRawData(const ContainerItem* item, void* dest, size_t dest_size) const;

        /**
         * Searches for an item with the given fingerprint in the container.
         *
         * @param fp
         * @param fp_size
         * @param find_deleted
         * @return
         */
        const ContainerItem* FindItem(const void* fp, size_t fp_size, bool find_deleted = false) const;

        /**
         * Seareches for an item with the given fingerprint in the container.
         * @param fp
         * @param fp_size
         * @param find_deleted
         * @return
         */
        ContainerItem* FindItem(const void* fp, size_t fp_size, bool find_deleted = false);

        /**
         * Merges the both containers into a new container
         *
         * @param container1
         * @param container2
     * @return true iff ok, otherwise an error has occurred
         */
        bool MergeContainer(const Container& container1, const Container& container2);

        /**
         * Checks if the container has the given id as primary or secondary id.
         *
         * @param id
         * @return
         */
        bool HasId(uint64_t id) const;

        std::string DebugString() const;

        inline bool is_stored() const;

        inline bool is_metadata_only() const;
};

uint32_t Container::item_count() const  {
    return this->item_count_;
}

bool Container::IsFull(size_t fp_size, size_t data_size) {
    size_t free_space = container_size() - data_position();
    size_t max_needed_space = (data_size + kMaxSerializedItemMetadataSize);
    bool space_is_free = max_needed_space < free_space;
    if (!space_is_free) {
        return true;
    }
    size_t max_needed_metadata_space = (this->item_count() + 1) * kMaxSerializedItemMetadataSize;
    size_t available_metadata_space = kMetaDataSize;
    bool meta_data_is_free = max_needed_metadata_space < available_metadata_space;
    if (!meta_data_is_free) {
        return true;
    }
    return false;
}

bool ContainerItem::is_indexed() const {
  return is_indexed_;
}

const byte* ContainerItem::key() const {
    return this->key_;
}

size_t ContainerItem::key_size() const {
    return this->key_size_;
}

size_t ContainerItem::offset() const {
    return this->offset_;
}

size_t ContainerItem::item_size() const {
    return this->item_size_;
}

size_t ContainerItem::raw_size() const {
    return this->raw_size_;
}

bool ContainerItem::is_deleted() const {
    return this->deleted_;
}

inline uint64_t ContainerItem::original_id() const {
    return this->original_id_;
}

std::vector<ContainerItem*>& Container::items() {
    return this->items_;
}

const std::vector<ContainerItem*>& Container::items() const {
    return this->items_;
}

uint64_t Container::primary_id() const {
    return this->primary_id_;
}

const std::set<uint64_t>& Container::secondary_ids() const {
    return this->secondary_ids_;
}

uint32_t Container::data_position() const {
    return this->pos_;
}

uint32_t Container::total_data_size() const {
    return this->pos_ - kMetaDataSize;
}

uint32_t Container::active_data_size() const {
    return this->active_data_size_ - kMetaDataSize;
}

size_t Container::container_size() const {
    return container_size_;
}

std::time_t Container::commit_time() const {
    return this->commit_time_;
}

bool Container::is_stored() const {
    return this->stored_;
}

bool Container::is_metadata_only() const {
    return this->metaDataOnly_;
}

byte* Container::mutable_data() {
    return this->data_;
}

}
}

#endif  // CONTAINER_H__

