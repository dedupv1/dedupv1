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

#ifndef NULL_CHUNKER_H_
#define NULL_CHUNKER_H_

#include <list>

#include <core/dedup.h>
#include <core/chunker.h>
#include <base/profile.h>


namespace dedupv1 {

class NullChunker : public Chunker {
        DISALLOW_COPY_AND_ASSIGN(NullChunker);

    public:
        static void RegisterChunker();
        static Chunker* CreateChunker();

        /**
         * Constructor
         */
        NullChunker();

        /**
         * Destructor
         */
        virtual ~NullChunker();

        /**
         * Starts the static chunker
         */
        virtual bool Start(dedupv1::base::ResourceManagement<Chunk>* cmc);

        /**
         * Create a new chunker session.
         */
        virtual ChunkerSession* CreateSession();

        /**
         * Returns the minimal (normal) chunk size.
         * Note however, that the chunker might generate smaller chunk in cases that the
         * session end is reached and the chunk is forced to finish.
         * @return
         */
        virtual size_t GetMinChunkSize();

        /**
         * returns the maximal chunk size.
         * There should never be a chunk that is larger.
         * @return
         */
        virtual size_t GetMaxChunkSize();

        /**
         * returns the average chunk size to system should
         * generate.
         *
         * @return
         */
        virtual size_t GetAvgChunkSize();
};

class NullChunkerSession : public ChunkerSession {
    public:
        explicit NullChunkerSession();

        virtual ~NullChunkerSession() {

        }

        virtual bool ChunkData(const byte* data,
                unsigned int offset, unsigned int size, bool last_chunk_call, std::list<Chunk*>* chunks);

        virtual unsigned int open_chunk_position();

        virtual bool GetOpenChunkData(byte* data, unsigned int offset, unsigned int size);

        virtual bool Clear();
};

}

#endif /* NULL_CHUNKER_H_ */

