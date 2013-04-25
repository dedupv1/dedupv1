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

#include <base/bitmap.h>
#include <base/crc32.h>
#include "dedupv1_base.pb.h"

LOGGER("Bitmap");

using dedupv1::base::CRC;

namespace dedupv1 {
namespace base {

const uint64_t Bitmap::kItemFullMask = ~static_cast<uint64_t> (0);

Bitmap::Bitmap(size_t size) {
    size_ = size;
    bitfield_size_ = size / 64;
    if ((size % 64) > 0) {
        bitfield_size_++;
    }
    clean_bits_ = size;
    persistent_index_ = NULL;
    key_ = 0;
    key_size_ = 0;
    dirty_ = true;
    page_size_ = 0;
    dirtyBitmap_ = NULL;
    bitfield_ = new uint64_t[bitfield_size_];
    ClearAll();
    dirty_ = true;
}

Bitmap::~Bitmap() {
    if (bitfield_)
        delete[] bitfield_;
    if (key_)
        delete[] key_;
    if (dirtyBitmap_) {
        delete dirtyBitmap_;
    }
}

bool Bitmap::Load(bool crashed) {
    // TODO(fermat): But size_ and clean_bits_ into the data structure and build the CRC over all
    CHECK(persistent_index_, "No persistence set");

    DEBUG("Loading Bitmap. crash: " << crashed);

    (*key_postfix_) = 0;
    BitmapData data;
    CHECK(persistent_index_->Lookup(key_, key_size_, &data) == LOOKUP_FOUND,
            "Failed to load bitmap");

    CHECK(data.has_page_size() && data.has_crc() && data.has_clean_bits() && data.has_size(), "Illegal data: " << data.ShortDebugString());

    CRC crc;
    uint64_t clean_bits = data.clean_bits();
    crc.Update(&clean_bits, sizeof(uint64_t));

    size_t size = data.size();
    CHECK(size == size_, "Read size and size from Constructor do not fit. Read size is " << size << " while this Bitmap shall take " << size_);
    crc.Update(&size, sizeof(size_t));

    size_t page_size = data.page_size();
    CHECK(page_size == page_size_, "Read page_size and set page size do not fit. Read " << page_size << " but should be " << page_size_);
    crc.Update(&page_size, sizeof(size_t));

    CHECK(crc.GetRawValue() == data.crc(), "CRC mismatch: " << data.ShortDebugString() << ", calculated crc " << crc.GetRawValue());

    TRACE("Will load " << pages() << " pages.");
    for (uint32_t i = 0; i < pages(); i++) {
        (*key_postfix_) = i + 1;
        BitmapPageData pageData;
        CHECK(persistent_index_->Lookup(key_, key_size_, &pageData) == LOOKUP_FOUND,
                "Failed to load bitmap");

        CRC innerCRC;
        CHECK(pageData.has_data() && pageData.has_crc(), "Illegal data: " << pageData.ShortDebugString());
        if ((i == pages() - 1) && (((bitfield_size_ * sizeof(uint64_t)) % page_size_) > 0)) {
            CHECK(pageData.data().size() == bitfield_size_ * sizeof(uint64_t) % page_size_, "Illigal page size in (last) page " << i << ": " << pageData.data().size());
        } else {
            CHECK(pageData.data().size() == page_size_, "Illigal page size in page " << i << ": " << pageData.data().size());
        }
        innerCRC.Update(pageData.data().data(), pageData.data().size());
        CHECK(innerCRC.GetRawValue() == pageData.crc(), "CRC mismatch: " << pageData.ShortDebugString() << ", calculated crc " << innerCRC.GetRawValue());

        uint64_t* bitdata = bitfield_ + (i * page_size_ / sizeof(uint64_t));
        memcpy(bitdata, pageData.data().data(), pageData.data().size());
        TRACE("Loaded page " << i << " with size " << pageData.data().size());
        for (size_t j = 0; j < page_size_ / sizeof(uint64_t); j++) {
            TRACE("Page " << i << " pos " << j << ": " << *bitdata);
            bitdata++;
        }
    }

    if (crashed) {
        clean_bits_ = bitfield_size_ * 64;
        for (size_t i = 0; i < bitfield_size_; i++) {
            clean_bits_ -= __builtin_popcountll(bitfield_[i]);
        }
    } else {
        clean_bits_ = clean_bits;
    }

    dirty_ = false;
    return true;
}

bool Bitmap::Store(bool is_new) {
    // TODO(fermat): But size_ and clean_bits_ into the data structure and build the CRC over all
    CHECK(persistent_index_, "No persistence set");

    CHECK(StoreMetadata(is_new), "Could not Store Metadata");

    for (uint32_t i = 0; i < pages(); i++) {
        CHECK(StorePageWithoutMetadata(i, is_new), "Could not store page " << i);
    }
    dirtyBitmap_->ClearAll();
    dirty_ = false;

    return true;
}

bool Bitmap::StoreMetadata(bool is_new) {
    BitmapData data;
    data.set_clean_bits(clean_bits_);
    data.set_size(size_);
    data.set_page_size(page_size_);
    CRC crc;
    crc.Update(&clean_bits_, sizeof(uint64_t));
    crc.Update(&size_, sizeof(size_t));
    crc.Update(&page_size_, sizeof(size_t));
    data.set_crc(crc.GetRawValue());

    DEBUG("Storing meta data: " << data.ShortDebugString());
    (*key_postfix_) = 0;
    if (is_new) {
        CHECK(persistent_index_->Lookup(key_, key_size_, NULL) == LOOKUP_NOT_FOUND,
                "Metadata can not be stored as new, because there exists an entry with the same key");
    }
    CHECK(persistent_index_->Put(key_, key_size_, data) != PUT_ERROR,
            "Failed to store bitmap: " << data.ShortDebugString());
    return true;
}

bool Bitmap::StorePageWithoutMetadata(uint32_t page, bool is_new) {
    Option<bool> b = dirtyBitmap_->is_set(page);
    DCHECK(b.valid(), "Failed to check page dirty state: page " << page);
    if (!b.value()) {
        return true;
    }
    DEBUG("Storing page " << page);

    BitmapPageData data;
    uint64_t* bitdata = bitfield_ + (page * page_size_ / sizeof(uint64_t));
    if ((page == pages() - 1) && (((bitfield_size_ * sizeof(uint64_t)) % page_size_) > 0)) {
        size_t rest_size = (bitfield_size_ * sizeof(uint64_t)) % page_size_;
        data.set_data(bitdata, rest_size);
        TRACE("Bitdata " << bitdata << " is " << (bitfield_size_ * sizeof(uint64_t)) % page_size_ << " from start, will store " << rest_size << " elements (last page)");
    } else {
        data.set_data(bitdata, page_size_);
        TRACE("Bitdata " << bitdata << " is " << (bitfield_size_ * sizeof(uint64_t)) % page_size_ << " from start, will store " << page_size_ << " elements");
    }
    CRC crc;
    crc.Update(data.data().data(), data.data().size());
    data.set_crc(crc.GetRawValue());
    DEBUG("Storing page data: data size " << data.data().size() << ", crc " << data.crc());

    (*key_postfix_) = page + 1;
    // TODO (dmeister) Use PutIfAbsent here
    if (is_new) {
        CHECK(persistent_index_->Lookup(key_, key_size_, NULL) == LOOKUP_NOT_FOUND,
                "Page " << page << " can not be stored as new, because there exists an entry with the same key");
    }
    CHECK(persistent_index_->Put(key_, key_size_, data) != PUT_ERROR,
            "Failed to store bitmap: " << data.ShortDebugString());
    CHECK(dirtyBitmap_->clear(page), "Could not clear bit " << page << " of dirty bitmap");
    return true;
}

Option<bool> Bitmap::StorePage(uint32_t page) {
    CHECK(persistent_index_, "No persistence set");
    CHECK(page < pages(), "page " << page << " out of range, only " << pages() << " available");

    bool stored = false;
    Option<bool> s = dirtyBitmap_->is_set(page);
    DCHECK(s.valid(), "Failed to check page dirty state: page " << page);
    if (s.value()) {
        CHECK(StorePageWithoutMetadata(page, false), "Could not Store page " << page);
        CHECK(StoreMetadata(false), "Could not store Metadata");
        dirty_ = (dirtyBitmap_->clean_bits() != dirtyBitmap_->size());
        stored = true;
    }

    return make_option(stored);
}

void Bitmap::ClearAll() {
    memset(bitfield_, 0, bitfield_size_ * sizeof(uint64_t));
    // I set all unreachabe bit, so I have not to tread this special during search
    if ((size_ % 64) > 0) {
        for (size_t i = size_ % 64; i < 64; i++) {
            bit_set(bitfield_ + bitfield_size_ - 1, i);
        }
    }
    clean_bits_ = size_;
    dirty_ = true;
    if (persistent_index_) {
        // I do not want to check for each page if it had set bits before
        dirtyBitmap_->SetAll();
    }
}

bool Bitmap::SetAll() {
    memset(bitfield_, 0xFF, bitfield_size_ * sizeof(uint64_t));
    // I set all unreachabe bit, so I have not to tread this special during search
    clean_bits_ = 0;
    dirty_ = true;
    if (persistent_index_) {
        // I do not want to check for each page if it had clean bits before
        dirtyBitmap_->SetAll();
    }
    return true;
}

bool Bitmap::Negate() {
    for (size_t i = 0; i < bitfield_size_; i++) {
        bitfield_[i] = ~(bitfield_[i]);
    }
    if ((size_ % 64) > 0) {
        for (size_t i = size_ % 64; i < 64; i++) {
            bit_set(bitfield_ + bitfield_size_ - 1, i);
        }
    }
    if (persistent_index_) {
        dirtyBitmap_->SetAll();
    }
    clean_bits_ = size_ - clean_bits_;
    return true;
}

bool Bitmap::setPersistence(PersistentIndex* persistent_index, const void* key_prefix, const size_t key_prefix_size,
        size_t page_size) {
    CHECK(persistent_index, "Persistence Index is NULL");
    CHECK(key_prefix, "Key has to be set");
    CHECK(key_prefix_size > 0, "Key size has to be greater then 0");
    CHECK(page_size > 0, "page size of 0 not supported for persistent index")
    CHECK(page_size % sizeof(uint64_t) == 0, "page size has to be a multiple of " << sizeof(uint64_t));

    size_t bytes = bitfield_size_ * sizeof(uint64_t);
    size_t pages = bytes / page_size;
    if ((bytes % page_size) != 0) {
        pages++;
    }
    CHECK(pages < UINT32_MAX, "Too much pages"); // It has to be smaller, as we need one place for the Metadata

    persistent_index_ = persistent_index;
    page_size_ = page_size;
    key_size_ = key_prefix_size + 4;
    key_ = new byte[key_prefix_size + 4];
    CHECK(key_, "Could not get memory for key. Key size is " << key_prefix_size);
    memcpy(key_, key_prefix, key_prefix_size);
    key_postfix_ = reinterpret_cast<uint32_t*> (key_ + key_prefix_size);
    (*key_postfix_) = 0;

    dirtyBitmap_ = new Bitmap(pages);
    CHECK(dirtyBitmap_, "Could not create dirty Bitmap with size " << pages);
    CHECK(dirtyBitmap_->SetAll(), "Could not set all bits of dirty bitmap");
    dirty_ = true;

    return true;
}

Option<size_t> Bitmap::find_next_unset(size_t start_position, size_t end_position) {
    CHECK(start_position < size_, "Startposition has to be smaller then size");
    CHECK(end_position <= size_, "Endposition has to be smaller or equal to size");

    // At many cases here we do not have to deal the last entry of bitfield in a special way, as invalid positions are set to one
    // We could improve the speed by changing the layout:
    // If we allow only to search for 1 values we could invert the whole Bitmap, so we would compare to 0 and would not have to negate the entries.

    // We could also change the value directly here, so it is less expensible then a following set

    // This method is implemented to be fast, therefore the code is quite long.

    size_t end = end_position;
    if (end_position <= start_position)
        end = size_;
    size_t page_pos = start_position / 64;
    size_t pos_in_page = start_position % 64;
    size_t iterator = page_pos * 64;

    // Search in first 64-Bit entry
    if (pos_in_page != 0) {
        uint64_t or_mask = (uint64_t(1) << pos_in_page) - 1;
        uint64_t test_line = ~(bitfield_[page_pos] | or_mask);
        if (test_line != 0) {
            int pos = __builtin_ffsll(test_line) - 1;
            iterator += pos;
            if (likely(iterator < end)) {
                return make_option(iterator);
            }
            return false;
        }
        iterator += 64;
        pos_in_page = 0;
        page_pos++;
    }

    // Now search between end of first page to end
    while (iterator < end) {
        if (bitfield_[page_pos] == kItemFullMask) {
            page_pos++;
            iterator += 64;
        } else {
            int pos = __builtin_ffsll(~(bitfield_[page_pos])) - 1;
            iterator += pos;
            if (likely(iterator < end)) {
                return make_option(iterator);
            }
            return false;
        }
    }

    // Now search from the beginning to end_position
    if ((end > end_position) && (end_position > 0)) {
        // now we have to rap the search
        iterator = 0;
        page_pos = 0;
        end = end_position;

        while (iterator < end) {
            if (bitfield_[page_pos] == kItemFullMask) {
                page_pos++;
                iterator += 64;
            } else {
                int pos = __builtin_ffsll(~(bitfield_[page_pos])) - 1;
                iterator += pos;
                if (likely(iterator < end)) {
                    return make_option(iterator);
                }
                return false;
            }
        }
    }

    // No unset element found
    return false;
}

}
}
