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

#include "container_test_helper.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

#include <base/logging.h>
#include <core/storage.h>
#include <core/chunk_mapping.h>
#include <core/dedup_system.h>

#include <set>

using std::string;
using std::set;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::chunkindex::ChunkIndex;
using dedupv1::chunkstore::Storage;
using dedupv1::chunkstore::StorageSession;
using dedupv1::DedupSystem;

LOGGER("ContainerTestHelper");

ContainerTestHelper::ContainerTestHelper(size_t test_data_size, size_t test_data_count) {
    this->test_data_count_ = test_data_count;
    this->test_data_size_ = test_data_size;

    this->test_data_ = new byte[test_data_count * test_data_size];
    this->addresses_ = new uint64_t[test_data_count];
    this->fp_ = new uint64_t[test_data_count];
    this->state_ = false;
}

ContainerTestHelper::~ContainerTestHelper() {
    delete[] this->test_data_;
    delete[] this->addresses_;
    delete[] this->fp_;
}

byte* ContainerTestHelper::data(int i) {
    CHECK_RETURN(this->state_, NULL, "SetUp not called");
    CHECK_RETURN(i >= 0 && i < static_cast<int>(this->test_data_count_), NULL, "Illegal index: " << i << ", count " << this->test_data_count_);
    return &this->test_data_[i * test_data_size_];
}

uint64_t ContainerTestHelper::data_address(int i) {
    CHECK_RETURN(this->state_, -2, "SetUp not called");
    CHECK_RETURN(i >= 0 && i < static_cast<int>(this->test_data_count_), Storage::ILLEGAL_STORAGE_ADDRESS, "Illegal index: " << i << ", count " << this->test_data_count_);
    return this->addresses_[i];
}

uint64_t* ContainerTestHelper::mutable_data_address(int i) {
    CHECK_RETURN(this->state_, NULL, "SetUp not called");
    CHECK_RETURN(i >= 0 && i < static_cast<int>(this->test_data_count_), NULL, "Illegal index: " << i << ", count " << this->test_data_count_);
    return &this->addresses_[i];
}

bytestring ContainerTestHelper::fingerprint(int i) {
    CHECK_RETURN(this->state_, bytestring(), "SetUp not called");
    CHECK_RETURN(i >= 0 && i < static_cast<int>(this->test_data_count_), bytestring(), "Illegal index: " << i << ", count " << this->test_data_count_);
    bytestring s;
    byte* fp_pointer = (byte *) &(this->fp_[i]);
    size_t fp_size = sizeof(this->fp_[i]);
    s.assign(fp_pointer, fp_size);
    return s;
}

bool ContainerTestHelper::SetUp() {
    CHECK(this->state_ == false, "SetUp already called");
    this->state_ = true;

    int fd = open("/dev/urandom",O_RDONLY);
    for (size_t i = 0; i < this->test_data_count_; i++) {
        size_t j = 0;
        while (j < test_data_count_) {
            size_t r = read(fd, data(i) + j, this->test_data_size_ - j);
            j += r;
        }
        this->fp_[i] = i + 1;
        this->addresses_[i] = Storage::ILLEGAL_STORAGE_ADDRESS;
    }
    close(fd);
    return true;
}

bool ContainerTestHelper::WriteDefaultData(DedupSystem* system, int offset, int count) {
    CHECK(this->state_, "SetUp not called");
    CHECK(system, "System not set");
    Storage* storage = system->storage();
    CHECK(storage, "Storage not set");
    StorageSession* s = storage->CreateSession();
    CHECK(s, "Storage session not set");

    bool r = WriteDefaultData(s, system->chunk_index(), offset, count);
    CHECK(s->Close(), "Cannot close storage session");
    return r;
}

bool ContainerTestHelper::WriteDefaultData(StorageSession* s, ChunkIndex* chunk_index, int offset, int count) {
    for (int i = offset; i < (offset + count); i++) {
        byte* d = this->data(i);
        CHECK(d, "Date not set");
        CHECK(s->WriteNew(&this->fp_[i], sizeof(this->fp_[i]), d, this->test_data_size_, true, &this->addresses_[i], NO_EC),
            "Write " << i << " failed");

        if (chunk_index != NULL) {
            ChunkMapping mapping(
                reinterpret_cast<const byte*>(&this->fp_[i]), sizeof(this->fp_[i]));
            mapping.set_data_address(this->addresses_[i]);
            CHECK(chunk_index->Put(mapping, NO_EC), "Failed to add chunk mapping");
        }
        DEBUG("Wrote index " << i << ", container id " << this->addresses_[i]);
    }

    return true;
}

void ContainerTestHelper::LoadContainerDataIntoChunkIndex(DedupSystem* system) {
    set<uint64_t> container_set;

    for (int i = 0; i < test_data_count(); i++) {
        container_set.insert(data_address(i));
    }

    set<uint64_t>::iterator i;
    for (i = container_set.begin(); i != container_set.end(); i++) {
        system->chunk_index()->LoadContainerIntoCache(*i, NO_EC);
    }
}

size_t ContainerTestHelper::Append(BlockMapping* m, size_t offset, int i, size_t size) {
    BlockMappingItem item(0, size);
    item.set_fingerprint_size(this->fingerprint(i).size());
    item.set_data_address(this->data_address(i));
    memcpy(item.mutable_fingerprint(), this->fingerprint(i).data(), this->fingerprint(i).size());

    if (!m->Append(offset, item)) {
        return 0;
    }
    return offset + item.size();
}

bool ContainerTestHelper::FillSameBlockMapping(BlockMapping* m, int i) {
    CHECK(m, "Mapping not set");
    size_t offset = 0;

    offset = Append(m, offset, i, 6179);
    CHECK(offset != 0U, "Append failed");

    offset = Append(m, offset, i, 7821);
    CHECK(offset != 0U, "Append failed");

    offset = Append(m, offset, i, 4723);
    CHECK(offset != 0U, "Append failed");

    offset = Append(m, offset, i, 2799);
    CHECK(offset != 0U, "Append failed");

    offset = Append(m, offset, i, 4822);
    CHECK(offset != 0U, "Append failed");

    offset = Append(m, offset, i, 13060);
    CHECK(offset != 0U, "Append failed");

    offset = Append(m, offset, i, 5194);
    CHECK(offset != 0U, "Append failed");

    offset = Append(m, offset, i, 7200);
    CHECK(offset != 0U, "Append failed");

    offset = Append(m, offset, i, 4540);
    CHECK(offset != 0U, "Append failed");

    offset = Append(m, offset, i, 4083);
    CHECK(offset != 0U, "Append failed");

    offset = Append(m, offset, i, 5115);
    CHECK(offset != 0U, "Append failed");

    return true;
}

bool ContainerTestHelper::FillBlockMapping(BlockMapping* m) {
    CHECK(m, "Mapping not set");

    size_t offset = 0;
    int i = 0;

    offset = Append(m, offset, i, 6179);
    CHECK(offset != 0U, "Append failed");
    i++;

    offset = Append(m, offset, i, 7821);
    CHECK(offset != 0U, "Append failed");
    i++;

    offset = Append(m, offset, i, 4723);
    CHECK(offset != 0U, "Append failed");
    i++;

    offset = Append(m, offset, i, 2799);
    CHECK(offset != 0U, "Append failed");
    i++;

    offset = Append(m, offset, i, 4822);
    CHECK(offset != 0U, "Append failed");
    i++;

    offset = Append(m, offset, i, 13060);
    CHECK(offset != 0U, "Append failed");
    i++;

    offset = Append(m, offset, i, 5194);
    CHECK(offset != 0U, "Append failed");
    i++;

    offset = Append(m, offset, i, 7200);
    CHECK(offset != 0U, "Append failed");
    i++;

    offset = Append(m, offset, i, 4540);
    CHECK(offset != 0U, "Append failed");
    i++;

    offset = Append(m, offset, i, 4083);
    CHECK(offset != 0U, "Append failed");
    i++;

    offset = Append(m, offset, i, 5115);
    CHECK(offset != 0U, "Append failed");
    i++;

    return true;
}
