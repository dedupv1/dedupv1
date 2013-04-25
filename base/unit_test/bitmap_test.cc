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

#include <gtest/gtest.h>
#include <base/bitmap.h>
#include <base/startup.h>
#include <base/fileutil.h>
#include <test_util/log_assert.h>
#include "index_test_util.h"

#include <vector>
LOGGER("BitmapTest");

using dedupv1::base::Bitmap;
using dedupv1::base::Option;

class BitmapTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Bitmap* bitmap_;

    dedupv1::base::Index* index;

    virtual void SetUp() {
        bitmap_ = NULL;
        index = NULL;
    }

    virtual void TearDown() {
        if (bitmap_) {
            delete bitmap_;
            bitmap_ = 0;
        }
        if (index) {
            delete index;
            index = NULL;
        }
        ASSERT_TRUE(ClearWork());
    }

    bool ClearWork() {
        std::vector<std::string> files;
        CHECK(dedupv1::base::File::ListDirectory("work/", &files), "Could not read work directory");
        for (size_t i = 0; i < files.size(); i++) {
            if ((files[i] != ".") && (files[i] != "..")) {
                CHECK(dedupv1::base::File::Remove(dedupv1::base::File::Join("work/", files[i])), "Could not remove from work dir: " << files[i]);
            }
        }
        return true;
    }
};

TEST_F(BitmapTest, Create)
{
    std::vector<uint64_t> sizes;
    sizes.push_back(0);
    sizes.push_back(1);
    sizes.push_back(5);
    sizes.push_back(63);
    sizes.push_back(64);
    sizes.push_back(65);
    sizes.push_back(127);
    sizes.push_back(128);
    sizes.push_back(129);

    for (size_t s = 0; s < sizes.size(); s++) {
        size_t size = sizes[s];
        DEBUG("Size " << size);

        bitmap_ = new Bitmap(size);

        ASSERT_EQ(size, bitmap_->size());
        Option<bool> value;
        for (size_t i = 0; i < size; i++) {
            value = bitmap_->is_set(i);
            ASSERT_TRUE(value.valid()) << "Error reading bit " << i << " of " << size;
            ASSERT_FALSE(value.value()) << "Bit " << i << " of " << size << " was set, but should not";
        }

        value = bitmap_->is_set(size);
        ASSERT_FALSE(value.valid());

        delete bitmap_;
        bitmap_ = NULL;
    }
}

TEST_F(BitmapTest, SetPersistenceFailing)
{
    bitmap_ = new Bitmap(62);

    index = dedupv1::testing::CreateIndex("sqlite-disk-btree;filename=work/tc_test_data;max-key-size=8;max-item-count=16K");
    dedupv1::base::PersistentIndex* pi = index->AsPersistentIndex();
    ASSERT_TRUE(pi);
    uint32_t key = 1;
    size_t key_size = sizeof(uint32_t);

    EXPECT_LOGGING(dedupv1::test::ERROR).Times(5);

    ASSERT_FALSE(bitmap_->setPersistence(NULL, &key, key_size, 4096)); // No Index
    ASSERT_FALSE(bitmap_->setPersistence(pi, NULL, key_size, 4096)); // No key
    ASSERT_FALSE(bitmap_->setPersistence(pi, &key, 0, 4096)); // key of size 0
    ASSERT_FALSE(bitmap_->setPersistence(pi, &key, key_size, 0)); // page_size 0
    ASSERT_FALSE(bitmap_->setPersistence(pi, &key, key_size, 73)); // page_size not dividable by 8
    ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, 4096));
}

TEST_F(BitmapTest, NoPersistence)
{
    bitmap_ = new Bitmap(62);

    EXPECT_LOGGING(dedupv1::test::ERROR).Times(4); // both Store and both Load

    ASSERT_FALSE(bitmap_->hasPersistence());
    ASSERT_TRUE(bitmap_->isDirty());
    ASSERT_FALSE(bitmap_->Store(false));
    ASSERT_FALSE(bitmap_->Store(true));
    ASSERT_FALSE(bitmap_->Load(false));
    ASSERT_FALSE(bitmap_->Load(true));
}

TEST_F(BitmapTest, Persistent)
{
    std::vector<uint64_t> sizes;
    sizes.push_back(0);
    sizes.push_back(1);
    sizes.push_back(5);
    sizes.push_back(63);
    sizes.push_back(64);
    sizes.push_back(65);
    sizes.push_back(127);
    sizes.push_back(128);
    sizes.push_back(129);
    sizes.push_back(4096 * 8 - 1);
    sizes.push_back(4096 * 8);
    sizes.push_back(4096 * 8 + 1);
    sizes.push_back(4096 * 16 - 1);
    sizes.push_back(4096 * 16);
    sizes.push_back(4096 * 16 + 1);
    sizes.push_back(4096 * 265 - 1);
    sizes.push_back(4096 * 256);
    sizes.push_back(4096 * 256 + 1);

    std::vector<uint64_t> page_sizes;
    page_sizes.push_back(64);
    page_sizes.push_back(4096);

    for (size_t t = 0; t < page_sizes.size(); t++) {
        size_t page_size = page_sizes[t];
        DEBUG("Page Size " << page_size);

        for (size_t s = 0; s < sizes.size(); s++) {
            size_t size = sizes[s];
            DEBUG("Size " << size);

            bitmap_ = new Bitmap(size);

            DEBUG("Size is " << size);
            index = dedupv1::testing::CreateIndex(
                "sqlite-disk-btree;filename=work/tc_test_data;max-key-size=8;max-item-count=16K");
            dedupv1::base::PersistentIndex* pi = index->AsPersistentIndex();
            ASSERT_TRUE(pi);
            ASSERT_TRUE(pi->Start(dedupv1::StartContext()));

            uint32_t key = 1;
            size_t key_size = sizeof(uint32_t);

            ASSERT_FALSE(bitmap_->hasPersistence());
            ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, page_size));
            ASSERT_TRUE(bitmap_->hasPersistence());
            ASSERT_EQ(page_size, bitmap_->page_size());
            if ((size % (page_size * 8)) == 0) {
                ASSERT_EQ(size / (8 * page_size), bitmap_->pages());
            } else {
                ASSERT_EQ((size / (8 * page_size)) + 1, bitmap_->pages());
            }

            ASSERT_TRUE(bitmap_->isDirty());
            for (size_t i = 0; i < size; i += 2) {
                ASSERT_TRUE(bitmap_->set(i));
            }
            ASSERT_EQ((bitmap_->size()) / 2, bitmap_->clean_bits());

            ASSERT_TRUE(bitmap_->Store(true));
            delete bitmap_;

            bitmap_ = new Bitmap(size);
            ASSERT_FALSE(bitmap_->hasPersistence());
            ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, page_size));
            ASSERT_TRUE(bitmap_->hasPersistence());
            ASSERT_TRUE(bitmap_->Load(false));
            ASSERT_EQ((bitmap_->size()) / 2, bitmap_->clean_bits());

            Option<bool> value;
            for (size_t i = 0; i < size; i++) {
                if ((i % 2) == 0) {
                    value = bitmap_->is_set(i);
                    ASSERT_TRUE(value.valid()) << "Error reading bit " << i;
                    ASSERT_TRUE(value.value()) << "Bit " << i << " should be set, but it is not";
                } else {
                    value = bitmap_->is_set(i);
                    ASSERT_TRUE(value.valid()) << "Error reading bit " << i;
                    ASSERT_FALSE(value.value()) << "Bit " << i << " should be set, but it is not";
                }
            }
            delete bitmap_;

            bitmap_ = new Bitmap(size);
            ASSERT_FALSE(bitmap_->hasPersistence());
            ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, page_size));
            ASSERT_TRUE(bitmap_->hasPersistence());
            ASSERT_TRUE(bitmap_->Load(true));
            ASSERT_EQ((bitmap_->size()) / 2, bitmap_->clean_bits());
            delete bitmap_;
            bitmap_ = NULL;

            delete index;
            index = NULL;
            ClearWork();
        }
    }
}

TEST_F(BitmapTest, StorePage)
{
    size_t page_size = 8 * 4;
    size_t bits_per_page = page_size * 8;
    bitmap_ = new Bitmap(bits_per_page * 32); // 32 Pages
    ASSERT_TRUE(bitmap_);

    index = dedupv1::testing::CreateIndex("sqlite-disk-btree;filename=work/tc_test_data;max-key-size=8;max-item-count=16K");
    dedupv1::base::PersistentIndex* pi = index->AsPersistentIndex();
    ASSERT_TRUE(pi);
    ASSERT_TRUE(pi->Start(dedupv1::StartContext()));
    uint32_t key = 1;
    size_t key_size = sizeof(uint32_t);

    ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, page_size));
    ASSERT_TRUE(bitmap_->Store(true));

    ASSERT_TRUE(bitmap_->set(bits_per_page + 5));
    ASSERT_TRUE(bitmap_->set(bits_per_page * 2 + 10));

    ASSERT_TRUE(bitmap_->isDirty());
    ASSERT_TRUE(bitmap_->StorePage(1).valid());
    ASSERT_TRUE(bitmap_->isDirty());
    ASSERT_TRUE(bitmap_->StorePage(2).valid());
    ASSERT_FALSE(bitmap_->isDirty());

    ASSERT_TRUE(bitmap_->set(bits_per_page * 3 + 7));
    ASSERT_TRUE(bitmap_->set(bits_per_page * 4 + 2));

    ASSERT_TRUE(bitmap_->isDirty());
    Option<bool> b = bitmap_->StorePage(4);
    ASSERT_TRUE(b.valid());
    ASSERT_TRUE(b.value());

    ASSERT_TRUE(bitmap_->isDirty());
    ASSERT_EQ(bits_per_page * 32 - 4, bitmap_->clean_bits());
    delete bitmap_;

    Option<bool> value;
    bitmap_ = new Bitmap(bits_per_page * 32);
    ASSERT_TRUE(bitmap_);
    ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, page_size));
    ASSERT_TRUE(bitmap_->Load(false));
    value = bitmap_->is_set(bits_per_page + 5);
    ASSERT_TRUE(value.valid());
    ASSERT_TRUE(value.value());
    value = bitmap_->is_set(bits_per_page * 2 + 10);
    ASSERT_TRUE(value.valid());
    ASSERT_TRUE(value.value());
    value = bitmap_->is_set(bits_per_page * 4 + 2);
    ASSERT_TRUE(value.valid());
    ASSERT_TRUE(value.value());
    value = bitmap_->is_set(bits_per_page * 3 + 7);
    ASSERT_TRUE(value.valid());
    ASSERT_FALSE(value.value());
    ASSERT_EQ(bits_per_page * 32 - 4, bitmap_->clean_bits()); // We are in a crash situation, so the read clean_bits is wrong
    delete bitmap_;

    bitmap_ = new Bitmap(bits_per_page * 32);
    ASSERT_TRUE(bitmap_);
    ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, page_size));
    ASSERT_TRUE(bitmap_->Load(true)); // Now we count the zeros
    value = bitmap_->is_set(bits_per_page + 5);
    ASSERT_TRUE(value.valid());
    ASSERT_TRUE(value.value());
    value = bitmap_->is_set(bits_per_page * 2 + 10);
    ASSERT_TRUE(value.valid());
    ASSERT_TRUE(value.value());
    value = bitmap_->is_set(bits_per_page * 4 + 2);
    ASSERT_TRUE(value.valid());
    ASSERT_TRUE(value.value());
    value = bitmap_->is_set(bits_per_page * 3 + 7);
    ASSERT_TRUE(value.valid());
    ASSERT_FALSE(value.value());
    ASSERT_EQ(bits_per_page * 32 - 3, bitmap_->clean_bits());
}

TEST_F(BitmapTest, Override)
{
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(2); // Failed Store override with check if new gives two errors

    bitmap_ = new Bitmap(4096); // 32 Pages
    ASSERT_TRUE(bitmap_);

    index = dedupv1::testing::CreateIndex("sqlite-disk-btree;filename=work/tc_test_data;max-key-size=8;max-item-count=16K");
    dedupv1::base::PersistentIndex* pi = index->AsPersistentIndex();
    ASSERT_TRUE(pi);
    ASSERT_TRUE(pi->Start(dedupv1::StartContext()));
    uint32_t key = 1;
    size_t key_size = sizeof(uint32_t);

    ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, 64));
    ASSERT_TRUE(bitmap_->Store(false)); // We also store a new bitmap without checking
    ASSERT_TRUE(bitmap_->SetAll());
    ASSERT_FALSE(bitmap_->Store(true)); // But if checking is on, we do not override, even if it is the same bitmap
    delete bitmap_;

    Option<bool> value;
    bitmap_ = new Bitmap(4096);
    ASSERT_TRUE(bitmap_);
    ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, 64));
    ASSERT_TRUE(bitmap_->Load(false));
    for (size_t i = 0; i < bitmap_->size(); i++) {
        value = bitmap_->is_set(i);
        ASSERT_TRUE(value.valid());
        ASSERT_FALSE(value.value());
    }
    ASSERT_TRUE(bitmap_->SetAll());
    ASSERT_TRUE(bitmap_->Store(false));
    delete bitmap_;

    bitmap_ = new Bitmap(4096);
    ASSERT_TRUE(bitmap_);
    ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, 64));
    ASSERT_TRUE(bitmap_->Load(false));
    for (size_t i = 0; i < bitmap_->size(); i++) {
        value = bitmap_->is_set(i);
        ASSERT_TRUE(value.valid());
        ASSERT_TRUE(value.value());
    }
}

TEST_F(BitmapTest, PersistentWrongSize)
{
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(2); // both Load calls

    bitmap_ = new Bitmap(63);
    ASSERT_TRUE(bitmap_);

    index = dedupv1::testing::CreateIndex(
        "sqlite-disk-btree;filename=work/tc_test_data;max-key-size=8;max-item-count=16K");
    dedupv1::base::PersistentIndex* pi = index->AsPersistentIndex();
    ASSERT_TRUE(pi);
    pi->Start(dedupv1::StartContext());

    uint32_t key = 1;
    size_t key_size = sizeof(uint32_t);

    ASSERT_FALSE(bitmap_->hasPersistence());
    ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, 4096));
    ASSERT_TRUE(bitmap_->hasPersistence());

    ASSERT_TRUE(bitmap_->isDirty());
    ASSERT_TRUE(bitmap_->Store(true));
    delete bitmap_;

    bitmap_ = new Bitmap(64);
    ASSERT_FALSE(bitmap_->hasPersistence());
    ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, 4096));
    ASSERT_TRUE(bitmap_->hasPersistence());
    ASSERT_FALSE(bitmap_->Load(false)); // ERROR

    delete bitmap_;
    bitmap_ = new Bitmap(62);
    ASSERT_FALSE(bitmap_->hasPersistence());
    ASSERT_TRUE(bitmap_->setPersistence(pi, &key, key_size, 4096));
    ASSERT_TRUE(bitmap_->hasPersistence());
    ASSERT_FALSE(bitmap_->Load(false)); // ERROR
}

TEST_F(BitmapTest, Negate)
{
    bitmap_ = new Bitmap(295);
    ASSERT_TRUE(bitmap_);
    ASSERT_EQ(295, bitmap_->clean_bits());

    ASSERT_TRUE(bitmap_->Negate());
    ASSERT_EQ(0, bitmap_->clean_bits());
    Option<bool> value;
    for (size_t i = 0; i < bitmap_->size(); i++) {
        value = bitmap_->is_set(i);
        ASSERT_TRUE(value.valid());
        ASSERT_TRUE(value.value());
    }

    ASSERT_TRUE(bitmap_->Negate());
    ASSERT_EQ(295, bitmap_->clean_bits());
    for (size_t i = 0; i < bitmap_->size(); i++) {
        value = bitmap_->is_set(i);
        ASSERT_TRUE(value.valid());
        ASSERT_FALSE(value.value());
    }

    for (size_t i = 0; i < bitmap_->size(); i += 2) {
        ASSERT_TRUE(bitmap_->set(i));
    }
    ASSERT_EQ(147, bitmap_->clean_bits());
    for (size_t i = 0; i < bitmap_->size(); i++) {
        value = bitmap_->is_set(i);
        ASSERT_TRUE(value.valid());
        if ((i % 2) == 0) {
            ASSERT_TRUE(value.value());
        } else {
            ASSERT_FALSE(value.value());
        }
    }
    ASSERT_TRUE(bitmap_->Negate());
    ASSERT_EQ(148, bitmap_->clean_bits());
    for (size_t i = 0; i < bitmap_->size(); i++) {
        value = bitmap_->is_set(i);
        ASSERT_TRUE(value.valid());
        if ((i % 2) == 1) {
            ASSERT_TRUE(value.value());
        } else {
            ASSERT_FALSE(value.value());
        }
    }
    ASSERT_TRUE(bitmap_->Negate());
    ASSERT_EQ(147, bitmap_->clean_bits());
    for (size_t i = 0; i < bitmap_->size(); i++) {
        value = bitmap_->is_set(i);
        ASSERT_TRUE(value.valid());
        if ((i % 2) == 0) {
            ASSERT_TRUE(value.value());
        } else {
            ASSERT_FALSE(value.value());
        }
    }
}

TEST_F(BitmapTest, SetAndClear)
{
    std::vector<uint64_t> sizes;
    sizes.push_back(0);
    sizes.push_back(1);
    sizes.push_back(5);
    sizes.push_back(63);
    sizes.push_back(64);
    sizes.push_back(65);
    sizes.push_back(127);
    sizes.push_back(128);
    sizes.push_back(129);

    for (size_t s = 0; s < sizes.size(); s++) {
        size_t size = sizes[s];
        DEBUG("Size " << size);

        bitmap_ = new Bitmap(size);
        ASSERT_TRUE(bitmap_);

        // Set
        Option<bool> value;
        for (size_t i = 0; i < size; i++) {
            ASSERT_EQ(size - i, bitmap_->clean_bits()) << "Round " << i;
            value = bitmap_->is_set(i);
            ASSERT_TRUE(value.valid()) << "Error reading bit " << i << " of " << size;
            ASSERT_FALSE(value.value()) << "Bit " << i << " of " << size << " was set, but should not";
            ASSERT_TRUE(bitmap_->set(i));
            ASSERT_EQ(size - i - 1, bitmap_->clean_bits()) << " Round " << i;
            value = bitmap_->is_set(i);
            ASSERT_TRUE(value.valid()) << "Error reading bit " << i << " of " << size;
            ASSERT_TRUE(value.value()) << "Bit " << i << " of " << size << " was not set, but should";

            ASSERT_TRUE(bitmap_->set(i)); // second set should not change anything
            ASSERT_EQ(size - i - 1, bitmap_->clean_bits());
            value = bitmap_->is_set(i);
            ASSERT_TRUE(value.valid()) << "Error reading bit " << i << " of " << size;
            ASSERT_TRUE(value.value()) << "Bit " << i << " of " << size << " was not set, but should";
        }

        value = bitmap_->set(size);
        ASSERT_FALSE(value.valid());

        // Clear
        for (size_t i = 0; i < size; i++) {
            ASSERT_EQ(i, bitmap_->clean_bits());
            value = bitmap_->is_set(i);
            ASSERT_TRUE(value.valid()) << "Error reading bit " << i << " of " << size;
            ASSERT_TRUE(value.value()) << "Bit " << i << " of " << size << " was not set, but should";
            ASSERT_TRUE(bitmap_->clear(i));
            ASSERT_EQ(i + 1, bitmap_->clean_bits());
            value = bitmap_->is_set(i);
            ASSERT_TRUE(value.valid()) << "Error reading bit " << i << " of " << size;
            ASSERT_FALSE(value.value()) << "Bit " << i << " of " << size << " was set, but should not";

            ASSERT_TRUE(bitmap_->clear(i));
            ASSERT_EQ(i + 1, bitmap_->clean_bits());
            value = bitmap_->is_set(i);
            ASSERT_TRUE(value.valid()) << "Error reading bit " << i << " of " << size;
            ASSERT_FALSE(value.value()) << "Bit " << i << " of " << size << " was set, but should not";
        }

        value = bitmap_->clear(size);
        ASSERT_FALSE(value.valid());

        delete bitmap_;
        bitmap_ = 0;
    }
}

TEST_F(BitmapTest, SetAndClearAll)
{
    std::vector<uint64_t> sizes;
    sizes.push_back(0);
    sizes.push_back(1);
    sizes.push_back(5);
    sizes.push_back(63);
    sizes.push_back(64);
    sizes.push_back(65);
    sizes.push_back(127);
    sizes.push_back(128);
    sizes.push_back(129);

    for (size_t s = 0; s < sizes.size(); s++) {
        size_t size = sizes[s];
        DEBUG("Size " << size);

        bitmap_ = new Bitmap(size);
        ASSERT_TRUE(bitmap_);

        Option<bool> value;
        for (size_t i = 0; i < size; i++) {
            value = bitmap_->is_set(i);
            ASSERT_TRUE(value.valid()) << "Error reading bit " << i << " of " << size;
            ASSERT_FALSE(value.value()) << "Bit " << i << " of " << size << " was set, but should not";
            ASSERT_TRUE(bitmap_->set(i));
        }

        bitmap_->ClearAll();

        for (size_t i = 0; i < size; i++) {
            value = bitmap_->is_set(i);
            ASSERT_TRUE(value.valid()) << "Error reading bit " << i << " of " << size;
            ASSERT_FALSE(value.value()) << "Bit " << i << " of " << size << " was set, but should not";
        }

        ASSERT_TRUE(bitmap_->SetAll());

        for (size_t i = 0; i < size; i++) {
            value = bitmap_->is_set(i);
            ASSERT_TRUE(value.valid()) << "Error reading bit " << i << " of " << size;
            ASSERT_TRUE(value.value()) << "Bit " << i << " of " << size << " was set, but should not";
        }

        delete bitmap_;
        bitmap_ = 0;
    }
}

TEST_F(BitmapTest, FindNextClean)
{
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(36); // 18 sizes, each two Errors

    Option<bool> value;
    Option<size_t> pos_value;

    std::vector<uint64_t> sizes;
    sizes.push_back(0);
    sizes.push_back(1);
    sizes.push_back(5);
    sizes.push_back(63);
    sizes.push_back(64);
    sizes.push_back(65);
    sizes.push_back(64 * 2 - 1);
    sizes.push_back(64 * 2);
    sizes.push_back(64 * 2 + 1);
    sizes.push_back(64 * 3 - 1);
    sizes.push_back(64 * 3);
    sizes.push_back(64 * 3 + 1);
    sizes.push_back(64 * 4 - 1);
    sizes.push_back(64 * 4);
    sizes.push_back(64 * 4 + 1);
    sizes.push_back(64 * 16 - 1);
    sizes.push_back(64 * 16);
    sizes.push_back(64 * 16 + 1);

    for (size_t s = 0; s < sizes.size(); s++) {
        size_t size = sizes[s];
        DEBUG("Size " << size);

        bitmap_ = new Bitmap(size);
        ASSERT_TRUE(bitmap_);

        size_t start[5];
        start[0] = 0;
        if (size > 0) {
            start[1] = size - 1; // This will result in Overflow tests
            start[2] = size / 2; // This will result in Overflow Tests for small sizes (size_ <= 20)
        } else {
            start[1] = 0;
            start[2] = 0;
        }
        if (size >= 3) {
            start[3] = size - 3; // This will result in Overflow tests
        } else {
            start[3] = 0;
        }
        if (size > 64) {
            start[4] = 64;
            start[5] = 60; // This will result in Overflowtests for size <= 70

        } else {
            start[4] = 0;
            start[5] = 0;
        }

        pos_value = bitmap_->find_next_unset(size, 0); // ERROR
        ASSERT_FALSE(pos_value.valid());

        pos_value = bitmap_->find_next_unset(0, size + 1); // ERROR
        ASSERT_FALSE(pos_value.valid());

        if (size > 0) {
            for (byte k = 0; k < 5; k++) {
                DEBUG("Starting position " << (uint16_t) k << " is " << start[k]);
                size_t startpos = start[k];
                for (size_t i = 0; i < size; i++) {
                    pos_value = bitmap_->find_next_unset(startpos, startpos);
                    ASSERT_TRUE(pos_value.valid()) << "Find of bit " << i << " failed";
                    ASSERT_EQ((startpos + i) % size, pos_value.value()) << "In round " << i;
                    ASSERT_TRUE(bitmap_->set(pos_value.value()));
                }

                TRACE("All set");

                pos_value = bitmap_->find_next_unset(startpos, startpos); // ERROR if size == 0
                ASSERT_FALSE(pos_value.valid());

                pos_value = bitmap_->find_next_unset(0, 0); // ERROR if size == 0
                ASSERT_FALSE(pos_value.valid());

                if (size > 10) {
                    for (byte i = 0; i < 5; i++) {
                        ASSERT_TRUE(bitmap_->clear((startpos + 5 + i) % size));
                    }

                    for (byte i = 0; i < 5; i++) {
                        pos_value = bitmap_->find_next_unset(startpos, startpos);
                        ASSERT_TRUE(pos_value.valid());
                        ASSERT_EQ((startpos + 5 + i) % size, pos_value.value());
                        ASSERT_TRUE(bitmap_->set(pos_value.value()));
                    }

                    pos_value = bitmap_->find_next_unset(startpos, startpos);
                    ASSERT_FALSE(pos_value.valid());

                    pos_value = bitmap_->find_next_unset(0, 0);
                    ASSERT_FALSE(pos_value.valid());

                    for (byte i = 0; i < 5; i++) {
                        ASSERT_TRUE(bitmap_->clear((startpos + 5 + i) % size));
                    }

                    for (byte i = 0; i < 3; i++) {
                        pos_value = bitmap_->find_next_unset(startpos, (startpos + 8) % size);
                        ASSERT_TRUE(pos_value.valid());
                        ASSERT_EQ((startpos + 5 + i) % size, pos_value.value());
                        ASSERT_TRUE(bitmap_->set(pos_value.value()));
                    }

                    pos_value = bitmap_->find_next_unset(startpos, (startpos + 8) % size);
                    ASSERT_FALSE(pos_value.valid());

                    pos_value = bitmap_->find_next_unset(startpos, (startpos + 9) % size);
                    ASSERT_TRUE(pos_value.valid());
                    ASSERT_EQ((startpos + 8) % size, pos_value.value());

                    pos_value = bitmap_->find_next_unset(startpos, (startpos + 10) % size);
                    ASSERT_TRUE(pos_value.valid());
                    ASSERT_EQ((startpos + 8) % size, pos_value.value());

                    pos_value = bitmap_->find_next_unset(startpos, startpos);
                    ASSERT_TRUE(pos_value.valid());
                    ASSERT_EQ((startpos + 8) % size, pos_value.value());
                }
                bitmap_->ClearAll();
            }
        }

        delete bitmap_;
        bitmap_ = 0;
    }
}
