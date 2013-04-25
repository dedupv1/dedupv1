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

#ifndef STATIC_CHUNKER_H__
#define STATIC_CHUNKER_H__

#include <list>

#include <core/dedup.h>
#include <core/chunker.h>
#include <base/profile.h>


namespace dedupv1 {

/**
 * A static chunker is a chunker that tries
 * to split up the stream into equal sized blocks.
 *
 * The result is usually not as good as using the RabinChunker,
 * but the static chunker is fast and in some situations
 * e.g. VMs not much worser.
 *
 * For more information: See
 * "Meister, Brinkmann: Multi-Level Comparision of
 * Deduplication ..., SYSTOR, Haifa, 2009"
 */
class StaticChunker : public Chunker {
    DISALLOW_COPY_AND_ASSIGN(StaticChunker);

    /**
     * Average chunk size.
     *
     */
    unsigned int avg_chunk_size_;

    /**
     * Holds profile information about the static chunker
     */
    dedupv1::base::Profile profile_;

    public:
    static void RegisterChunker();
    static Chunker* CreateChunker();

    /**
     * Constructor
     */
    StaticChunker();

    /**
     * Destructor
     */
    virtual ~StaticChunker();

    /**
     * Starts the static chunker
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start();

    /**
     * Create a new chunker session.
     */
    virtual ChunkerSession* CreateSession();

    /**
     *
     * Available options:
     * - avg-chunk-size: StorageUnit
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool SetOption(const std::string& option_name, const std::string& option);

    virtual std::string PrintProfile();

    /**
     * Min. Chunk Size = Avg. Chunk Size = Max. Chunk Size in a static-sized chunk
     * strategy
     * @return
     */
    virtual size_t GetMinChunkSize();

    /**
     * Min. Chunk Size = Avg. Chunk Size = Max. Chunk Size in a static-sized chunk
     * strategy
     * @return
     */
    virtual size_t GetMaxChunkSize();

    /**
     * Min. Chunk Size = Avg. Chunk Size = Max. Chunk Size in a static-sized chunk
     * strategy
     * @return
     */
    virtual size_t GetAvgChunkSize();


    friend class StaticChunkerSession;
};

class StaticChunkerSession : public ChunkerSession {
        StaticChunker* chunker;
        byte* current_chunk;
        unsigned int current_chunk_pos;

        bool AcceptChunk(std::list<Chunk*>* chunks);
    public:
        explicit StaticChunkerSession(StaticChunker* chunker);

        virtual ~StaticChunkerSession();

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool ChunkData(const byte* data,
                unsigned int offset, unsigned int size, bool last_chunk_call, std::list<Chunk*>* chunks);

        virtual unsigned int open_chunk_position();

        virtual bool GetOpenChunkData(byte* data, unsigned int offset, unsigned int size);

        virtual bool Clear();
};

}

#endif  // STATIC_CHUNKER_H__
