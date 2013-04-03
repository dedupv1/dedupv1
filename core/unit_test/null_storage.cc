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
#include "null_storage.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>

#include <core/storage.h>
#include <core/dedup.h>
#include <base/logging.h>

namespace dedupv1 {
namespace chunkstore {

uint64_t NullStorage::GetActiveStorageDataSize() {
    return 0;
}

void NullStorage::RegisterStorage() {
    Storage::Factory().Register("null-storage", &NullStorage::CreateStorage);
}

Storage* NullStorage::CreateStorage() {
    Storage * storage = new NullStorage();
    return storage;
}

StorageSession* NullStorage::CreateSession() {
    return new NullStorageSession();
}

}
}

