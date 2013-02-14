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

#include <core/chunk.h>
#include <base/logging.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

LOGGER("Chunk");

namespace dedupv1 {

Chunk::Chunk() {
    data_ = NULL;
    size_ = 0;
    max_size_ = 0;
}

bool Chunk::Init(size_t size) {
    this->data_ = new byte[size];
    CHECK(this->data_, "Alloc Chunk Data failed");
    memset(this->data_, 0, size);
    this->size_ = size;
    this->max_size_ = size;
    return true;
}

void Chunk::Detach() {
    this->size_ = 0;
}

Chunk::~Chunk() {
    if (this->data_) {
        delete[] this->data_;
        this->data_ = NULL;
    }
    this->size_ = 0;
    this->max_size_ = 0;
}

Chunk* ChunkResourceType::Create() {
    Chunk* c = new Chunk();
    CHECK_RETURN(c, NULL, "Memalloc chunk failed");
    if (!c->Init(Chunk::kMaxChunkSize)) {
        ERROR("Init chunk failed");
        delete c;
        return NULL;
    }
    return c;
}

void ChunkResourceType::Close(Chunk* c) {
    if (!c) {
        WARNING("Chunk not set");
        return;
    }
    delete c;
}

void ChunkResourceType::Reinit(Chunk* c) {
    if (!c) {
        WARNING("Chunk not set");
        return;
    }
    c->Detach();
}

}
