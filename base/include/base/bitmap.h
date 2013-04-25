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

#ifndef BITMAP_H__
#define BITMAP_H__

#include <base/base.h>
#include <base/bitutil.h>
#include <base/option.h>
#include <base/logging.h>
#include <base/index.h>

namespace dedupv1 {
namespace base {

/**
 * This class represents a set of bit. It is possible to check and set the
 * state of each single bit. The bitmap can also be persisted.
 *
 * This class is not threadsafe, so do not use it concurrently.
 */
class Bitmap {
    private:
        DISALLOW_COPY_AND_ASSIGN(Bitmap);

        /**
         * Mask with all values set to one
         */
        static const uint64_t kItemFullMask;

        /**
         * Bitfield holding all data
         */
        uint64_t* bitfield_;

        /**
         * number of bits as passed in constructor
         */
        size_t size_;

        /**
         * number of 64-Bit-Words the bitfield consists of
         */
        size_t bitfield_size_;

        /**
         * number of unset bits in the bitmap
         */
        size_t clean_bits_;

        /**
         * Index to store the bitmap to and to load it from
         */
        PersistentIndex* persistent_index_;

        /**
         * Key where to keep persistent data
         */
        byte* key_;

        /**
         * Key size where to keep the persistent data
         */
        size_t key_size_;

        /**
         * part of the key changing with each page
         */
        uint32_t* key_postfix_;

        /**
         * is the in memory mapping dirty or in sync with the persistence
         */
        bool dirty_;

        /**
         * size of a page to write back
         */
        size_t page_size_;

        /**
         * A non persistent Bitmap storing for each Bitmap if it is dirty or not.
         *
         * Attention: persisting it could lead to a endless recusion.
         */
        Bitmap* dirtyBitmap_;

        /**
         * true if the Metadata was ever persisted
         */
        bool is_persisted_;

    public:
        /**
         * Create a new Bitmap, all bits are unset.
         *
         * @param size number of bits in the bitmap
         */
        Bitmap(size_t size);

        /**
         * Destructor
         */
        ~Bitmap();

        /**
         * set the persistence of the index
         *
         * The key is copied in this method, so the caller keeps to be responsible to delete it.
         *
         * @param persistent_index The index to persistet the Bitmap
         * @param key_prefix The key used to access the index
         * @param key_prefix_size The size of the key
         * @param page_size size of each page written to index
         * @return true iff everything is o.k.
         */
        bool setPersistence(PersistentIndex* persistent_index, const void* key_prefix, const size_t key_prefix_size,
                size_t page_size);

        /**
         * Load the Bitmap.
         *
         * The persistence has to be set before using setPersistence. The method fails without persistence.
         *
         * @param crashed if true the bitmap assumes it was not Stored consistent, so it counts the free bits
         *
         * @return true iff everything is o.k.
         */
        bool Load(bool crashed);

        /**
         * Store the whole Bitmap.
         *
         * The persistence has to be set before using setPersistence. The method fails without persistence.
         *
         * If this call fails it is possible that there are already data written to persistency,
         * so the state of the persistent version is undefined.
         *
         * @param is_new If true Store checks that it does not overwrite anything.
         *               If false the check will not be done, so a new Bitmap could override data.
         * @return true iff everything is o.k.
         */
        bool Store(bool is_new);

        /**
         * Stores a single page if it is dirty.
         *
         * The persistence has to be set before using setPersistence. The method fails without persistence.
         *
         * @param page The page to store
         * @return A unset option if the storing failed with an error.
         * If the option contains true iff everything is o.k.
         */
        dedupv1::base::Option<bool> StorePage(uint32_t page);

        /**
         * is the inMemory data dirty, or is it in sync with persistence?
         *
         * A Bitmap without persistence is every time dirty.
         */
        inline bool isDirty();

        /**
         * check if the persistence is set
         */
        inline bool hasPersistence();

        /**
         * check if the Bit at the given position is set
         *
         * @param position bit to check
         * @return The option is false, if the given position position is not
         *         valid, else the value gives the state of the bit.
         */
        inline Option<bool> is_set(size_t position);

        /**
         * check if the Bit at the given position is clean
         *
         * This option is just the opposite to is_set.
         *
         * @param position bit to check
         * @return The option is false, if the given position position is not
         *         valid, else the value gives the negated state of the bit.
         */
        inline Option<bool> is_clean(size_t position);

        /**
         * set the bit at the given position
         *
         * @param position bit to set
         * @return false on error, else true
         */
        inline bool set(size_t position);

        /**
         * unset the bit at the given position
         *
         * @param position bit to clear
         * @return false on error, else true
         */
        inline bool clear(size_t position);

        /**
         * Clears the hole Bitmap, so it is in the same state as after construction
         */
        void ClearAll();

        /**
         * Negates the whole bitmap, so each Bit that was set gets clean and each clean bit will be set.
         */
        bool Negate();

        /**
         * Sets all Bits of the Bitmap
         */
        bool SetAll();

        /**
         * find the first unset bit in the given intervall
         *
         * Some examples:
         *   * find(0, size()) Finds the first unset bit
         *   * find(7, 8) checks if bit 7 is unset (is_set is more efficient for this task)
         *   * find(pos, size()) finds the next unset bit between position and end
         *   * find(pos, pos) Finds the first unset bit beginning at pos, wrapping the search
         *
         * @param start_position where to start the search (search start at this position)
         * @param end_position where to stop position (the element itself will not be checked)
         * @return Option is false if position is invalid or no bit is unset, else value has the wanted position
         */
        Option<size_t> find_next_unset(size_t start_position, size_t end_position);

        /**
         * Get the number of bits in this bitmap
         *
         * @return the number of bits in this bitmap
         */
        inline size_t size();

        /**
         * Get the configured page size.
         *
         * @return the configured page size.
         */
        inline size_t page_size();

        /**
         * Get the number of pages
         */
        inline uint32_t pages();

        /**
         * Get the number of clean bits
         */
        inline size_t clean_bits();

        /**
         * Get the page number containing the given pos.
         *
         * @param pos position to get the page for
         * @return page number
         */
        inline size_t page(size_t pos);

    private:
        /**
         * Write the metadata only.
         *
         * @param page the page to update
         * @param is_new iff true Store checks that it does not overwrite anything
         * @return true iff everything is o.k.
         */
        bool StoreMetadata(bool is_new);

        /**
         * Write a single page, but do not update the metadata.
         *
         * @param page the page to update
         * @param is_new iff true Store checks that it does not overwrite anything
         * @return true iff everything is o.k.
         */
        bool StorePageWithoutMetadata(uint32_t page, bool is_new);
};

bool Bitmap::isDirty() {
    if (!persistent_index_) {
        return true;
    }
    return dirty_;
}

bool Bitmap::hasPersistence() {
    return persistent_index_;
}

Option<bool> Bitmap::is_set(size_t position) {
    if (position >= size_)
        return false;
    if (!bitfield_)
        return false;
    return make_option(bit_test(bitfield_[position / 64], position % 64));
}

Option<bool> Bitmap::is_clean(size_t position) {
    if (position >= size_)
        return false;
    if (!bitfield_)
        return false;
    return make_option(!bit_test(bitfield_[position / 64], position % 64));
}

bool Bitmap::set(size_t position) {
    if (position >= size_)
        return false;
    if (!bitfield_)
        return false;
    if (!bit_test(bitfield_[position / 64], position % 64)) {
        clean_bits_--;
        dirty_ = true;
        if (persistent_index_) {
            dirtyBitmap_->set(position / (page_size_ * 8));
        }
        bit_set(bitfield_ + (position / 64), position % 64);
    }
    return true;
}

bool Bitmap::clear(size_t position) {
    if (position >= size_)
        return false;
    if (!bitfield_)
        return false;
    if (bit_test(bitfield_[position / 64], position % 64)) {
        clean_bits_++;
        dirty_ = true;
        if (persistent_index_) {
            dirtyBitmap_->set(position / (page_size_ * 8));
        }
        bit_clear(bitfield_ + (position / 64), position % 64);
    }
    return true;
}

size_t Bitmap::size() {
    if (!bitfield_)
        return 0;
    return size_;
}

size_t Bitmap::page_size() {
    if (!persistent_index_)
        return 0;
    return page_size_;
}

uint32_t Bitmap::pages() {
    if (!persistent_index_)
        return 0;
    return static_cast<uint32_t> (dirtyBitmap_->size());
}

size_t Bitmap::clean_bits() {
    return clean_bits_;
}

size_t Bitmap::page(size_t pos) {
    if (!persistent_index_) {
        return 0;
    }
    return pos / (page_size_ * 8);
}

}
}

#endif /* BITMAP_H_ */
