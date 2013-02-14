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

#ifndef MEMCHUNK_H__
#define MEMCHUNK_H__

#include <base/base.h>

#include <string>

namespace dedupv1 {
namespace base {

/**
 * A managed chunk of memory.
 *
 * Used mainly by the http client
 */
class Memchunk {
        DISALLOW_COPY_AND_ASSIGN(Memchunk);

        /**
         * data buffer
         */
        byte* data_;

        /**
         * size of the data buffer
         */
        size_t size_;

        /**
         * flag if the memchunk is the owner of the data.
         * If the memchunk is the owner, the object is responsible
         * to free the memory.
         */
        bool owner_;
    public:
        /**
         * Constructor
         * @param data data buffer to use
         * @param size size of the data buffer
         * @param owner flag if the new memchunk should be the owner. If the object
         * is created, the client of the constructor should not be free the object.
         *
         * @return
         */
        Memchunk(void* data, size_t size, bool owner = true);

        /**
         * Alternative constructor that allocates a new buffer
         * with the given size
         */
        explicit Memchunk(size_t size);

        /**
         * Destructor.
         * If the memchunk is the owner of the buffer, the
         * buffer is released
         * @return
         */
        ~Memchunk();

        /**
         * current size of the buffer
         * @return
         */
        inline size_t size() const;

        /**
         * mutable pointer to the buffer
         * @return
         */
        inline byte* value();

        /**
         * constant pointer to the buffer
         * @return
         */
        inline const byte* value() const;

        /**
         * returns true iff the object is the owner of the
         * buffer
         * @return
         */
        inline bool is_owned() const;

        /**
         * Reallocates the buffer.
         * The semantics are the same as using the POSIX
         * realloc function.
         *
         * @param new_size
         * @return true iff ok, otherwise an error has occurred
         */
        bool Realloc(size_t new_size);

        /**
         * Returns a checksum of the memchunk data
         * @return
         */
        std::string checksum() const;

        /**
         * Creates a new memchunk as a copy of existing data
         * @param data
         * @param size
         * @return
         */
        static Memchunk* NewAsCopy(void* data, size_t size);
};

byte* Memchunk::value() {
    return data_;
}

const byte* Memchunk::value() const {
    return data_;
}

size_t Memchunk::size() const {
    return size_;
}

bool Memchunk::is_owned() const {
    return owner_;
}

}
}

#endif  // MEMCHUNK_H__
