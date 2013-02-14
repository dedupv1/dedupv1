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

#include <base/memchunk.h>
#include <base/crc32.h>
#include <base/logging.h>

#include <stdlib.h>

using std::string;
using dedupv1::base::crc;

LOGGER("Memchunk");

namespace dedupv1 {
namespace base {

Memchunk::Memchunk(size_t size) {
    this->size_ = size;
    if (size > 0) {
        this->data_ = new byte[size];
        memset(this->data_, 0, size);
    } else {
        this->data_ = NULL;
    }
    this->owner_ = true;
}

Memchunk::Memchunk(void* data, size_t size, bool owner) {
    this->size_ = size;
    if (size > 0 && data == NULL) {
        this->data_ = new byte[size];
        memset(this->data_, 0, size);
    } else {
        this->data_ = reinterpret_cast<byte*>(data);
    }
    this->owner_ = owner;
}

Memchunk::~Memchunk() {
    if (data_ && owner_) {
        delete[] data_;
    }
}

bool Memchunk::Realloc(size_t new_size) {
    DEBUG("Reallocating memory: " << new_size);
    if (new_size == size_) {
        return true;
    } else if (new_size > size_) {
        byte* new_data = new byte[new_size];
        CHECK(new_data, "Alloc failed");
        memcpy(new_data, data_, size_);
        memset(new_data + size_, 0, new_size - size_);
        delete[] this->data_;
        this->data_ = new_data;
        this->size_ = new_size;
    } else {
        byte* new_data = new byte[new_size];
        CHECK(new_data, "Alloc failed");
        memcpy(new_data, data_, new_size);
        delete[] this->data_;
        this->data_ = new_data;
        this->size_ = new_size;
    }
    return true;
}

string Memchunk::checksum() const {
    return crc(this->data_, this->size_);
}

Memchunk* Memchunk::NewAsCopy(void* data, size_t size) {
    void* buffer = new byte[size];
    CHECK_RETURN(buffer, NULL, "Allocation failed: size " << size);
    memcpy(buffer, data, size);
    return new Memchunk(buffer, size, true);
}

}
}
