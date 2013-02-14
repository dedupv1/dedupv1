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

#include <base/base.h>

#include <pthread.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <stdio.h>

#include <base/bitutil.h>
#include <base/logging.h>
#include <base/disk_hash_index.h>
#include <base/hash_index.h>
#include <base/tc_hash_index.h>
#include <base/tc_hash_mem_index.h>
#include <base/tc_btree_index.h>
#include <base/tc_fixed_index.h>
#include <base/fixed_index.h>
#include <base/sqlite_index.h>

uint64_t make_multi_file_address(uint64_t file_index,
                                 uint64_t file_offset, unsigned int file_count_bits) {
    uint64_t file_bits = ((uint64_t) file_index) << (63ULL
                                                     - file_count_bits);
    uint64_t address = file_bits | file_offset;
    return address;
}

unsigned int multi_file_get_file_index(uint64_t multi_file_adress,
                                       unsigned int file_count_bits) {
    return multi_file_adress >> (63ULL - file_count_bits);
}

unsigned int multi_file_get_file_offset(uint64_t multi_file_adress,
                                        unsigned int file_count_bits) {
    multi_file_adress = multi_file_adress << (file_count_bits + 1);
    multi_file_adress = multi_file_adress >> (file_count_bits + 1); /* Remove the file index bits out of the adress */
    return multi_file_adress;
}

namespace dedupv1 {
namespace base {

bytestring make_bytestring(const byte* v, size_t s) {
    bytestring bs;
    bs.assign(reinterpret_cast<const byte*>(v), s);
    return bs;
}

bytestring make_bytestring(const std::string& s) {
    bytestring bs;
    bs.assign(reinterpret_cast<const byte*>(s.data()), s.size());
    return bs;
}

void RegisterDefaults() {
    dedupv1::base::DiskHashIndex::RegisterIndex();
    dedupv1::base::HashIndex::RegisterIndex();
    dedupv1::base::TCHashIndex::RegisterIndex();
    dedupv1::base::TCBTreeIndex::RegisterIndex();
    dedupv1::base::TCMemHashIndex::RegisterIndex();
    dedupv1::base::TCFixedIndex::RegisterIndex();
    dedupv1::base::FixedIndex::RegisterIndex();
    dedupv1::base::SqliteIndex::RegisterIndex();
}

}
}
