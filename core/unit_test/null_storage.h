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
#ifndef NULL_STORAGE_H
#define NULL_STORAGE_H

#include <core/dedup.h>
#include <core/storage.h>

namespace dedupv1 {
namespace chunkstore {

/**
 * A null implementation of the storage interface.
 * The null storage doesn't store or reads data, but always
 * simply returns true.
 *
 * Usually, it is better to use the MockStorage class as it
 * is much more configurable than the NullStorage class.
 */
class NullStorage : public Storage {
    public:
        static Storage* CreateStorage();
        static void RegisterStorage();

        NullStorage() {
        }
        virtual ~NullStorage() {
        }

        virtual uint64_t GetActiveStorageDataSize();

        virtual StorageSession* CreateSession();

        virtual enum storage_commit_state IsCommittedWait(uint64_t address) {
          return STORAGE_ADDRESS_NOT_COMMITED;
        }

        virtual enum storage_commit_state IsCommitted(uint64_t address) {
          return STORAGE_ADDRESS_NOT_COMMITED;
        }


};

class NullStorageSession : public StorageSession {
    public:
        NullStorageSession() {}

        virtual bool WriteNew(const void* key, size_t key_size, const void* data,
                size_t data_size, bool is_indexed,
                uint64_t* address, dedupv1::base::ErrorContext* ec) {
            *address = 0;
            return true;
        }

        virtual bool Read(uint64_t address,
            const void* key, size_t key_size,
            void* data, size_t* data_size,
            dedupv1::base::ErrorContext* ece) {
            return true;
        }
};

}
}

#endif

