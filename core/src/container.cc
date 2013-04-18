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
#include <core/container.h>

#include <ctime>
#include <sstream>

#include <dedupv1.pb.h>

#include <base/logging.h>
#include <base/index.h>
#include <base/hashing_util.h>
#include <base/strutil.h>
#include <base/crc32.h>
#include <base/sha1.h>
#include <core/fingerprinter.h>
#include <base/fileutil.h>
#include <base/protobuf_util.h>
#include <base/compress.h>
#include <core/storage.h>
#include <base/adler32.h>

using std::set;
using std::vector;
using std::string;
using std::stringstream;
using dedupv1::base::Compression;
using dedupv1::base::File;
using dedupv1::Fingerprinter;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::ParseSizedMessage;
using dedupv1::base::SerializeSizedMessage;
using dedupv1::base::raw_compare;
using dedupv1::base::Option;
using dedupv1::base::crc;
using dedupv1::base::AdlerChecksum;
using dedupv1::base::sha1;

LOGGER("Container");

namespace dedupv1 {
namespace chunkstore {

bool Container::UnserializeMetadata(bool verify_checksum) {
    DCHECK(this->data_, "Container not inited");

    this->items_.clear();
    this->item_count_ = 0;
    this->active_data_size_ = kMetaDataSize;

    ContainerData container_data;
    CHECK(ParseSizedMessage(&container_data, this->data_, kMetaDataSize, verify_checksum).valid(),
        "Cannot parse data: " << container_data.InitializationErrorString());

    if (verify_checksum && container_data.has_checksum() && !this->metaDataOnly_) {
        // we can only check the checksum if we have all data
        AdlerChecksum adler;
        adler.Update(this->data_ + kMetaDataSize, this->container_size_ - kMetaDataSize);
        CHECK(container_data.checksum() == adler.checksum(),
            "Container checksum mismatch: " << container_data.ShortDebugString() <<
            ", calculated checksum " << adler.checksum());
    }

    // with this block and the block after the loop of the container items
    // we check if the load id (aka the primary id before the real primary id has been set
    // from the container data) is the primary or a secondary id. We then call the
    // load id a valid id.
    bool valid_id = false;
    uint64_t check_container_id = this->primary_id();
    if (container_data.has_primary_id()) {
        this->primary_id_ = container_data.primary_id();
        if (container_data.primary_id() == check_container_id) {
            valid_id = true;
        }
    }

    this->items_.reserve(container_data.items_size());
    for (int i = 0; i < container_data.items_size(); i++) {
        const ContainerItemData& item_data = container_data.items(i);
        CHECK(item_data.has_fp() &&
            item_data.has_item_size() &&
            item_data.has_position_offset() &&
            item_data.has_raw_size(), "Illegal container item " << item_data.DebugString());
        uint64_t original_id = 0;
        if (item_data.has_original_id()) {
            original_id = item_data.original_id();
            if (original_id != this->primary_id_) {
                // we found a new secondary id
                this->secondary_ids_.insert(original_id);
            }
        } else {
            // backward mode
            original_id = container_data.primary_id();
        }
        ContainerItem* item = new ContainerItem((byte *) item_data.fp().data(),
            item_data.fp().size(),
            item_data.position_offset(),
            item_data.raw_size(),
            item_data.item_size(),
            original_id,
            item_data.indexed());

        CHECK(item, "Alloc item failed");
        if (item_data.has_deleted() && item_data.deleted()) {
            item->deleted_ = true;
            // do not increment item count for deleted items
        } else {
            this->active_data_size_ += item_data.item_size();
            this->item_count_++;
        }

        TRACE("Read container item " << item->DebugString());
        CHECK_RETURN(item->offset() + item->item_size() <= this->container_size_, 0,
            "container corruption: " <<
            "item offset " << item->offset() <<
            ", item size " << item->item_size() <<
            ", container size " << this->container_size());

        this->items_.push_back(item);
    }

    for (set<uint64_t>::iterator j = this->secondary_ids_.begin(); j != this->secondary_ids_.end(); j++) {
        if (*j == check_container_id) {
            valid_id = true;
        }
    }
    CHECK(valid_id, "Container id verification failed: container id " << check_container_id <<
        ", container " << this->DebugString());

    if (container_data.has_commit_time()) {
        this->commit_time_ = container_data.commit_time();
    }
    this->pos_ = container_data.container_size();
    return true;
}

size_t Container::SerializeMetadata(bool calculate_checksum) {
    DCHECK(this->data_, "Container not inited");
    DCHECK(!metaDataOnly_, "Cannot serialize metadata in metadata-only mode");

    ContainerData container_data;
    container_data.set_primary_id(this->primary_id_);
    container_data.set_container_size(this->pos_);

    for (vector<ContainerItem*>::iterator i = this->items_.begin(); i != this->items_.end(); i++) {
        ContainerItem* item = *i;
        ContainerItemData* item_data = container_data.add_items();
        item_data->set_fp(item->key_, item->key_size_);
        item_data->set_position_offset(item->offset_);
        item_data->set_item_size(item->item_size_);
        item_data->set_raw_size(item->raw_size_);
        item_data->set_original_id(item->original_id_);
        if (item->is_deleted()) {
            item_data->set_deleted(true);
        }
        if (item->is_indexed()) {
          item_data->set_indexed(true);
        }
        TRACE("" << item->DebugString() << ": " << item_data->ShortDebugString() << ", size " << item_data->ByteSize());
    }

    if (this->commit_time_ > 0) {
        container_data.set_commit_time(this->commit_time_);
    }
    if (calculate_checksum) {
        AdlerChecksum adler;
        adler.Update(this->data_ + kMetaDataSize, this->container_size_ - kMetaDataSize);
        container_data.set_checksum(adler.checksum());
        TRACE("Container id " << this->primary_id_ << " has now checksum " << container_data.checksum());
    }

    Option<size_t> meta_data_size = SerializeSizedMessage(container_data, this->data_, kMetaDataSize, calculate_checksum);
    CHECK_RETURN(meta_data_size.valid(), 0, "Cannot serialize data: " << this->DebugString());

    TRACE("Container metadata: size " << meta_data_size.value() << ", items " << container_data.items_size());

    return meta_data_size.value();

}
Container::Container() {
    this->data_ = NULL;
    this->pos_ = 0;
    this->primary_id_ = Storage::ILLEGAL_STORAGE_ADDRESS;
    this->container_size_ = 0;
    this->active_data_size_ = 0;
    this->stored_ = false;
    this->metaDataOnly_ = false;
    this->item_count_ = 0;
    this->commit_time_ = 0;
}

bool Container::Init(uint64_t id, size_t container_size) {
    CHECK(container_size >= kMetaDataSize, "Container size must be larger then meta data area: container size " << container_size);
    this->data_ = new byte[container_size];
    CHECK(this->data_, "Container init failed");
    memset(this->data_, 0, container_size);

    this->pos_ = kMetaDataSize;
    this->active_data_size_ = kMetaDataSize;
    this->primary_id_ = id;
    this->items_.clear();
    this->item_count_ = 0;
    this->container_size_ = container_size;
    this->stored_ = false;
    this->metaDataOnly_ = false;
    this->commit_time_ = 0;
    return true;
}

bool Container::InitInMetadataOnlyMode(uint64_t id, size_t container_size) {
    CHECK(container_size >= kMetaDataSize, "Container size must be larger then meta data area: container size " << container_size);
    this->data_ = new byte[kMetaDataSize];
    CHECK(this->data_, "Container init failed");
    memset(this->data_, 0, kMetaDataSize);

    this->pos_ = kMetaDataSize;
    this->active_data_size_ = kMetaDataSize;
    this->primary_id_ = id;
    this->items_.clear();
    this->item_count_ = 0;
    this->container_size_ = container_size;
    this->stored_ = false;
    this->metaDataOnly_ = true;
    this->commit_time_ = 0;
    return true;
}

void Container::Reuse(uint64_t id) {
    this->pos_ = kMetaDataSize;
    this->active_data_size_ = kMetaDataSize;
    this->primary_id_ = id;
    for (vector<ContainerItem*>::iterator i = this->items_.begin(); i != this->items_.end(); i++) {
        ContainerItem* item = *i;
        if (item) {
            delete item;
        }
    }
    this->items_.clear();
    this->item_count_ = 0;
    this->secondary_ids_.clear();
    this->stored_ = false;
    this->commit_time_ = 0;
    // meta data mode stays the same
}
namespace {

Compression* GetCompression(CompressionMode mode) {
    if (mode == COMPRESSION_BZ2) {
        return Compression::NewCompression(Compression::COMPRESSION_BZ2);
    } else if (mode == COMPRESSION_DEFLATE) {
        return Compression::NewCompression(Compression::COMPRESSION_ZLIB_1);
    } else if (mode == COMPRESSION_LZ4) {
        return Compression::NewCompression(Compression::COMPRESSION_LZ4);
    } else if (mode == COMPRESSION_SNAPPY) {
        return Compression::NewCompression(Compression::COMPRESSION_SNAPPY);
    }
    ERROR("Compression not supported yet");
    return NULL;
}

bool DecompressItem(const ContainerItemValueData& item_data, const byte* data, void* dest, size_t dest_size) {
    Compression* comp = GetCompression(item_data.compression());
    DCHECK(comp, "Cannot create compression");

    bool failed = false;
    if (comp->Decompress(dest, dest_size, data, item_data.on_disk_size()) < 0) {
        ERROR("Failed to decompress container data");
        failed = true;
    }
    delete comp;
    return !failed;
}
}
bool Container::CopyRawData(const ContainerItem* item, void* dest, size_t dest_size) const {
    DCHECK(item, "Item not set");
    DCHECK(this->data_, "Container not inited");
    CHECK(metaDataOnly_ == false, "Container has only loaded meta data");

    TRACE("Read item " << item->key_string() << ":" <<
        "offset " << item->offset_ <<
        ", item size " << item->item_size_ <<
        ", raw size " << item->raw_size_ <<
        ", dest size " << dest_size);

    DCHECK(item->offset() + item->item_size() <= container_size_, "Illegal item: " << item->DebugString());
    ContainerItemValueData item_data;
    Option<size_t> message_size = ParseSizedMessage(&item_data, this->data_ + item->offset(), item->item_size(), false);
    CHECK(message_size.valid(), "cannot parse sized message");

    TRACE("Item data: " << item_data.ShortDebugString() <<
        ", message size " << message_size.value() <<
        ", data offset " << item->offset_ + message_size.value() <<
        ", stored sha1 " << sha1(data_ + item->offset_ + message_size.value(), item_data.on_disk_size()));

    size_t data_offset = item->offset_ + message_size.value();

    TRACE("Data crc " << crc(this->data_ + data_offset, item_data.on_disk_size()));

    if (!item_data.has_compression() || item_data.compression() == COMPRESSION_NO) {
        DCHECK(item_data.on_disk_size() == item->raw_size(),
            "Illegal item size: " << item->DebugString() <<
            ", item data " << item_data.ShortDebugString());
        DCHECK(dest_size >= item_data.on_disk_size(), "Illegal destination size");
        DCHECK(data_offset + item_data.on_disk_size() <= container_size_, "Illegal source offset");
        memcpy(dest, this->data_ + data_offset, item_data.on_disk_size());
    } else {
        CHECK(DecompressItem(item_data, this->data_ + data_offset, dest, dest_size),
            "Failed to decompress item: " << item->DebugString() <<
            ", item data " << item_data.ShortDebugString());
    }
    TRACE("Data offset " << data_offset <<
        ", data crc " << crc(this->data_ + data_offset, item_data.on_disk_size()) <<
        ", raw crc " << crc(dest, dest_size));
    return true;
}

bool Container::DeleteItem(const byte* key, size_t key_size) {
    CHECK(this->metaDataOnly_ == false, "Container has only loaded meta data");

    ContainerItem* item = FindItem(key, key_size, true);
    CHECK(item, "Item not found: " << Fingerprinter::DebugString(key, key_size));

    TRACE("Delete item from container " << this->primary_id() <<
        ", fp " << Fingerprinter::DebugString((byte *) key, key_size));

    if (item->deleted_ == false) {
        item->deleted_ = true;
        this->active_data_size_ -= item->item_size();
        this->item_count_--;
    }
    return true;
}

void Container::InsertItemSorted(vector<ContainerItem*>* items, ContainerItem* new_item) {
    int j = 0;
    for (vector<ContainerItem*>::iterator i = items->begin(); i != items->end(); i++, j++) {
        ContainerItem* item = *i;
        int comp_result = raw_compare(new_item->key(), new_item->key_size(), item->key(), item->key_size());
        if (comp_result >= 0) {
            continue;
        }
        // insert here
        items->insert(i, new_item);
        return;
    }
    // not inserted before
    items->push_back(new_item);
}

bool Container::CopyItem(const Container& parent_container, const ContainerItem& item) {
    CHECK(this->stored_ == false, "Cannot add items to a stored container: container " << this->primary_id());
    CHECK(this->metaDataOnly_ == false, "Container has only loaded meta data");
    DCHECK(this->data_, "Container not inited");
    CHECK(!IsFull(item.key_size(), item.raw_size()), "Data size too large: " <<
        "pos " << this->pos_ <<
        ", data size " << item.raw_size() <<
        ", items " << this->item_count() <<
        ", container size " << this->container_size());
    DCHECK(pos_ + item.item_size() <= container_size_, "Illegal copy item: " << item.DebugString());

    memcpy(this->data_ + this->pos_, parent_container.data_ + item.offset(), item.item_size());

    ContainerItem* new_item = new ContainerItem(item.key(),
        item.key_size(),
        this->pos_,
        item.raw_size(),
        item.item_size(),
        item.original_id(),
        item.is_indexed());
    CHECK(new_item, "Alloc container item failed");

    if (item.is_deleted()) {
        new_item->deleted_ = true;
        // do not increase item count and active data size of deleted items
    } else {
        this->item_count_++;
        this->active_data_size_ += item.item_size();
    }

    this->pos_ += item.item_size();

    TRACE("Copy item " << new_item->key_string() << " to offset " << new_item->offset_ << " (item size " << new_item->item_size_ << ", raw size " << new_item->raw_size_ << ")");
    Container::InsertItemSorted(&this->items_, new_item);
    return true;
}

bool Container::AddItem(const byte* key,
    size_t key_size,
    const byte* data,
    size_t data_size,
    bool is_indexed,
    Compression* comp) {
    CHECK(this->stored_ == false, "Cannot add items to a stored container: container " << this->primary_id());
    CHECK(key, "Key not set");
    CHECK(data, "Data not set");
    CHECK(this->metaDataOnly_ == false, "Container has only loaded meta data");
    CHECK(this->data_, "Container not inited");
    CHECK(!IsFull(key_size, data_size), "Data size too large: pos " << this->pos_ <<
        ", data size " << data_size <<
        ", items " << this->item_count() <<
        ", container size " << this->container_size());

    size_t offset = this->pos_;

    ContainerItemValueData value_data;
    value_data.set_on_disk_size(data_size);

    byte data_buffer[data_size * 2];
    if (data_size >= kMinCompressedChunkSize && comp) {
        ssize_t compressed_size = comp->Compress(data_buffer, data_size * 2, data, data_size);
        CHECK(compressed_size >= 0, "Cannot compress data: size " << data_size);
        if (compressed_size < (ssize_t) data_size) {
            value_data.set_on_disk_size(compressed_size);

            if (comp->GetCompressionType() == Compression::COMPRESSION_ZLIB_1 ||
                comp->GetCompressionType() == Compression::COMPRESSION_ZLIB_3 ||
                comp->GetCompressionType() == Compression::COMPRESSION_ZLIB_9) {
                value_data.set_compression(COMPRESSION_DEFLATE);
            } else if (comp->GetCompressionType() == Compression::COMPRESSION_BZ2) {
                value_data.set_compression(COMPRESSION_BZ2);
            } else if (comp->GetCompressionType() == Compression::COMPRESSION_LZ4) {
                value_data.set_compression(COMPRESSION_LZ4);
            } else if (comp->GetCompressionType() == Compression::COMPRESSION_SNAPPY) {
                value_data.set_compression(COMPRESSION_SNAPPY);
            } else {
                ERROR("Unsupported compression type: " << comp->GetCompressionType());
                return false;
            }
        } else {
            // Compression not successful
            comp = NULL;
        }
    } else {
        comp = NULL;
    }

    CHECK(this->container_size_ - offset >= (ssize_t) kMaxSerializedItemMetadataSize, "Illegal available message size: " <<
        this->container_size_ - offset);
    Option<size_t> message_size = SerializeSizedMessage(value_data, this->data_ + offset, this->container_size_ - offset, false);
    CHECK(message_size.valid(), "Cannot serialize sized message: max size " << (this->container_size_ - offset));

    // add the size of the item metadata size
    this->pos_ += message_size.value();
    this->active_data_size_ += message_size.value();

    CHECK(pos_ + value_data.on_disk_size() <= container_size_,
        "Illegal item data: pos " << pos_ <<
        ", data size " << value_data.on_disk_size() <<
        ", container size " << container_size_);

    if (comp == NULL) {
        // Compression not active or not successful
        memcpy(this->data_ + pos_, data, value_data.on_disk_size());
    } else {
        memcpy(this->data_ + pos_, data_buffer, value_data.on_disk_size());
    }

    // adds the size of the (on disk) data
    pos_ += value_data.on_disk_size();
    this->active_data_size_ += value_data.on_disk_size();

    size_t item_size = message_size.value() + value_data.on_disk_size();

    ContainerItem* item = new ContainerItem(key,
        key_size,
        offset,
        data_size,
        item_size,
        this->primary_id(),
        is_indexed);
    CHECK(item, "Alloc container item failed");

    TRACE("Add item " << item->key_string() << ": container " << this->primary_id() <<
        ", offset " << item->offset() <<
        ", item size " << item->item_size() <<
        ", raw size " << item->raw_size() <<
        ", item data: " << value_data.ShortDebugString() <<
        ", message size " << message_size.value() <<
        ", data offset " << pos_ - value_data.on_disk_size() <<
        ", sha1 " << sha1(data, data_size) <<
        ", stored sha1 " << sha1(data_ + pos_ - value_data.on_disk_size(), value_data.on_disk_size()));
    Container::InsertItemSorted(&this->items_, item);
    this->item_count_++;

    return true;
}

ContainerItem::ContainerItem(const byte* key, size_t key_size,
                             size_t offset,
                             size_t raw_size,
                             size_t item_size,
                             uint64_t original_id,
                             bool is_indexed) {
    memcpy(this->key_, key, key_size);
    this->key_size_ = key_size;
    this->offset_ = offset;
    this->item_size_ = item_size;
    this->raw_size_ = raw_size;
    this->deleted_ = false;
    this->is_indexed_ = is_indexed;
    this->original_id_ = original_id;
}

bool ContainerItem::Equals(const ContainerItem& item) const {
    if (!(this->key_size_ == item.key_size_)) {
        return false;
    }
    if (!(this->offset_ == item.offset_)) {
        return false;
    }
    if (!(this->raw_size_ == item.raw_size_)) {
        return false;
    }
    if (!(this->item_size_ == item.item_size_)) {
        return false;
    }
    if (!(this->deleted_ == item.deleted_)) {
        return false;
    }
    if (memcmp(this->key_, item.key_, this->key_size_) != 0) {
        return false;
    }
    return true;
}

string ContainerItem::key_string() const {
    return Fingerprinter::DebugString((byte *) key_, key_size_);
}

Container::~Container() {
    delete[] this->data_;
    this->data_ = NULL;
    this->pos_ = 0;
    for (vector<ContainerItem*>::iterator i = this->items_.begin(); i != this->items_.end(); i++) {
        ContainerItem* item = *i;
        delete item;
    }
    this->items_.clear();
    this->item_count_ = 0;
}

bool Container::CopyFrom(const Container& container, bool copyId) {
    DCHECK(this->container_size_ == container.container_size(), "Container not compatible");
    DCHECK(this->items_.size() == 0, "Destination container not ready for reuse: " << this->DebugString());
    DCHECK(this->data_ != NULL, "Destination container not inited");
    DCHECK(!(this->is_metadata_only() == false && container.is_metadata_only() == true), "Source container should not be meta data only");

    if (copyId) {
        this->primary_id_ = container.primary_id();
        this->secondary_ids_ = container.secondary_ids_;
    }
    this->pos_ = container.pos_;

    if (this->is_metadata_only()) {
        memcpy(this->data_, container.data_, kMetaDataSize);
    } else {
        memcpy(this->data_, container.data_, this->container_size());
    }

    this->items_.reserve(container.items_.size());
    for (vector<ContainerItem*>::const_iterator i = container.items_.begin(); i != container.items_.end(); i++) {
        const ContainerItem* item = *i;
        ContainerItem* copy_item = new ContainerItem(item->key(), item->key_size(),
            item->offset(), item->raw_size(), item->item_size(), item->original_id(),
            item->is_indexed());
        CHECK(copy_item, "Alloc container item failed");
        if (item->is_deleted()) {
            copy_item->deleted_ = true;
        }

        this->items_.push_back(copy_item);
    }
    this->active_data_size_ = container.active_data_size_;
    this->item_count_ = container.item_count_;
    return true;
}

bool Container::Equals(const Container& container) const {
    DCHECK(this->data_, "Container not inited");
    DCHECK(container.data_, "Container not inited");

    if (!(this->container_size_ == container.container_size_)) {
        return false;
    }
    if (!(this->primary_id_ == container.primary_id_)) {
        return false;
    }
    if (!(this->secondary_ids_ == container.secondary_ids_)) {
        return false;
    }

    if (!(this->pos_ == container.pos_)) {
        return false;
    }

    if (!(this->item_count() == container.item_count())) {
        return false;
    }

    vector<ContainerItem*>::const_iterator i;
    vector<ContainerItem*>::const_iterator j;
    for (i = this->items_.begin(), j = container.items_.begin(); i != this->items_.end(), j != container.items_.end(); i++, j++) {
        const ContainerItem* item1 = *i;
        const ContainerItem* item2 = *j;
        if (!item1->Equals(*item2)) {
            return false;
        }
        if (memcmp(this->data_ + item1->offset_, container.data_ + item2->offset_, item1->item_size_) != 0) {
            return false;
        }
    }
    return true;
}

bool Container::LoadFromFile(File* file, off_t offset, bool verify_checksum) {
    CHECK(file, "File not set");
    DCHECK(this->data_, "Container not inited");

    if (this->metaDataOnly_ == false) {
        ssize_t read_data_size = file->Read(offset, this->data_, this->container_size_);
        CHECK(read_data_size == (ssize_t) this->container_size_, "Cannot read container data: " <<
            "read data size "  << read_data_size <<
            ", container size " << this->container_size_);

        DEBUG("Load container from file: " <<
            "container id " << this->primary_id() <<
            ", path " << file->path() <<
            ", offset " << offset <<
            ", container crc " << crc(this->data_, this->container_size_));
    } else {
        ssize_t read_data_size = file->Read(offset, this->data_, Container::kMetaDataSize);
        CHECK(read_data_size == (ssize_t) Container::kMetaDataSize, "Cannot read container meta data: " <<
            "read data size "  << read_data_size <<
            ", container meta data size " << Container::kMetaDataSize);
    }

    uint64_t container_id = this->primary_id(); // unserialize metadata might change primary id
    CHECK(this->UnserializeMetadata(verify_checksum), "Failed to unserialize container: " <<
        "container id " << container_id <<
        ", (partially loaded) container " << this->DebugString() <<
        ", offset " << offset);
    CHECK(this->pos_, "Position not set correctly");
    CHECK(this->pos_ <= this->container_size_, "Illegal container size: " << this->container_size_);

    this->stored_ = true;
    TRACE("Load container from file: " <<
        "container " << this->DebugString() <<
        ", container position " << this->pos_);
    return true;
}

bool Container::StoreToFile(File* file, off_t offset, bool calculate_checksum) {
    DCHECK(file, "File not set");
    DCHECK(this->primary_id() != Storage::ILLEGAL_STORAGE_ADDRESS && this->primary_id() != Storage::EMPTY_DATA_STORAGE_ADDRESS && this->primary_id() != 0,
        "Illegal container id " << this->primary_id());
    DCHECK(!metaDataOnly_, "Cannot serialize metadata in metadata-only mode");
    DCHECK(this->data_, "Container not inited");

    if (this->commit_time_ == 0) {
        this->commit_time_ = std::time(&this->commit_time_);
    }

    CHECK(this->SerializeMetadata(calculate_checksum), "Cannot serialize container: " << this->DebugString());
    CHECK(file->Write(offset, this->data_, this->container_size_)
        == (ssize_t) this->container_size_, "Container write failed: " << this->DebugString());
    this->stored_ = true;

    DEBUG("Store container to file: " <<
        "container " << this->DebugString() <<
        ", path " << file->path() <<
        ", offset " << offset <<
        ", container crc " << crc(this->data_, this->container_size_));
    return true;
}

const ContainerItem* Container::FindItem(const void* fp, size_t fp_size, bool find_deleted) const {
    // Note: http://googleresearch.blogspot.com/2006/06/extra-extra-read-all-about-it-nearly.html NOLINT
    // Note also that we cannot used std::binary_search here as it does not return the found value

    int low = 0;
    int high = this->items_.size() - 1;

    while (low <= high) {
        int mid = low + ((high - low) / 2);
        const ContainerItem* item = this->items_[mid];
        CHECK_RETURN(item != NULL, NULL, "Item not set");

        int comp_result = raw_compare(item->key(), item->key_size(), fp, fp_size);
        if (comp_result < 0) {
            low = mid + 1;
            TRACE("Check index " << mid <<
                ", item " << item->key_string() <<
                ", search fp " << Fingerprinter::DebugString(fp, fp_size) <<
                ", result: lower");
        } else if (comp_result > 0) {
            high = mid - 1;
            TRACE("Check index " << mid <<
                ", item " << item->key_string() <<
                ", search fp " << Fingerprinter::DebugString(fp, fp_size) <<
                ", result: upper");
        } else {
            TRACE("Check index " << mid <<
                ", item " << item->key_string() <<
                ", search fp " << Fingerprinter::DebugString(fp, fp_size) <<
                ", result: found");
            // key found
            if (item->is_deleted() && !find_deleted) {
                return NULL;
            }
            return item;
        }
    }
    return NULL; // Key not found
}

ContainerItem* Container::FindItem(const void* fp, size_t fp_size, bool find_deleted) {
    // Note: http://googleresearch.blogspot.com/2006/06/extra-extra-read-all-about-it-nearly.html NOLINT
    // Note also that we cannot used std::binary_search here as it does not return the found value

    int low = 0;
    int high = this->items_.size() - 1;

    while (low <= high) {
        int mid = low + ((high - low) / 2);
        ContainerItem* item = this->items_[mid];
        CHECK_RETURN(item != NULL, NULL, "Item not set");

        int comp_result = raw_compare(item->key(), item->key_size(), fp, fp_size);
        if (comp_result < 0) {
            low = mid + 1;
            TRACE("Check index " << mid <<
                ", item " << item->key_string() <<
                ", search fp " << Fingerprinter::DebugString(fp, fp_size) <<
                ", result: lower");
        } else if (comp_result > 0) {
            high = mid - 1;
            TRACE("Check index " << mid <<
                ", item " << item->key_string() <<
                ", search fp " << Fingerprinter::DebugString(fp, fp_size) <<
                ", result: upper");
        } else {
            TRACE("Check index " << mid <<
                ", item " << item->key_string() <<
                ", search fp " << Fingerprinter::DebugString(fp, fp_size) <<
                ", result: found");
            // key found
            if (item->is_deleted() && !find_deleted) {
                return NULL;
            }
            return item;
        }
    }
    return NULL; // Key not found
}

bool Container::MergeContainer(const Container& container1, const Container& container2) {
    CHECK(container1.is_metadata_only() == false, "container has only meta data: " << container1.DebugString());
    CHECK(container2.is_metadata_only() == false, "container has only meta data: " << container2.DebugString());

    // collect ids
    set<uint64_t> ids;
    // Note that copy items takes care of the sorting
    vector<ContainerItem*>::const_iterator i;
    for (i = container1.items().begin(); i != container1.items().end(); i++) {
        const ContainerItem* item = *i;
        if (item->is_deleted()) {
            continue;
        }
        ids.insert(item->original_id());
        CHECK(this->CopyItem(container1, *item), "Failed to copy item " << item->DebugString());
    }
    for (i = container2.items().begin(); i != container2.items().end(); i++) {
        const ContainerItem* item = *i;
        if (item->is_deleted()) {
            continue;
        }
        ids.insert(item->original_id());
        CHECK(this->CopyItem(container2, *item), "Failed to copy item " << item->DebugString());
    }

    // smallest id as new primary
    // this is not necessarily one of the old primary ids, e.g. if the old primary id has not more
    // valid items in the new container
    this->primary_id_ = *ids.begin();
    this->secondary_ids_ = ids;
    this->secondary_ids_.erase(this->primary_id_);
    return true;
}

string ContainerItem::DebugString() const {
    stringstream sstr;
    sstr << "[" << Fingerprinter::DebugString(this->key_, this->key_size_) <<
    ", offset " << this->offset_ <<
    ", item size " << this->item_size_ <<
    ", raw size " << this->raw_size_ <<
    ", original id " << this->original_id_;
    if (is_deleted()) {
        sstr << ", deleted";
    }
    sstr << "]";
    return sstr.str();
}

std::string Container::DebugString() const {
    string s = "[id " + ToString(primary_id());

    if (this->secondary_ids_.size() > 0) {
        s += ", secondary id ";
        set<uint64_t>::const_iterator j;
        for (j = this->secondary_ids_.begin(); j != this->secondary_ids_.end(); j++) {
            if (j != this->secondary_ids_.begin()) {
                s += ", ";
            }
            s += ToString(*j);
        }
    }
    s += ", item count " + ToString(item_count());
    s += ", total data size " + ToString(total_data_size());
    s += ", active data size " + ToString(active_data_size()) + ", position " + ToString(data_position());
    if (this->commit_time_ > 0) {
        char buf[64]; // 26 bytes should be enough
        string time_s(ctime_r(&this->commit_time_, buf));
        time_s = time_s.substr(0, time_s.size() - 1); // remove the line feed
        s += ", commit time " + time_s;
    }
    s += "]";
    return s;
}

bool Container::HasId(uint64_t id) const {
    if (this->primary_id_ == id) {
        return true;
    }
    set<uint64_t>::const_iterator j = this->secondary_ids_.find(id);
    return j != this->secondary_ids_.end();
}

}
}
