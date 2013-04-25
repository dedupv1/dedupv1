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

#ifndef CHUNKER_H__
#define CHUNKER_H__

#include <core/dedup.h>
#include <base/factory.h>
#include <core/statistics.h>
#include <core/chunk.h>
#include <base/resource_management.h>

#include <map>
#include <list>
#include <string>

namespace dedupv1 {

/**
 * \defgroup chunker Chunking
 *
 * A chunker is a strategy to divide a stream of blocks into smaller elements ("chunks") that
 * should be stored separately.
 *
 * Often a chunk has a size larger than a single block, so the data of block may not result in a chunk.
 *
 */

/**
 * \ingroup chunker
 * A ChunkerSession contains the thread-bound parts of a chunker used in a single session
 */
class ChunkerSession {
    private:
        DISALLOW_COPY_AND_ASSIGN(ChunkerSession);
    public:
        /**
         * Empty constructor
         * @return
         */
        ChunkerSession();

        /**
         * Empty destructor
         * @return
         */
        virtual ~ChunkerSession();

        /**
         * Chunk the given data and add the created chunks in
         * the given list.
         * Not all data that is given must be enclosed in the
         * chunks returns. There may be data that is not
         * ready to be enclosed in a chunk (open data). All
         * of this data should be enclosed in a chunk in later
         * requests of ChunkData of the same session or at
         * latest in Close or CloseRequest calls.
         *
         * @param data data to chunk
         * @param offset offset within the block. Used by the static chunker for the alignment.
         * @param size size of the data to chunk
         * @param last_chunk_call true iff this is the last chunk call in the request. At the end all data should be assigned to a chunk.
         * @param chunks linked list to hold all created chunks
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool ChunkData(const byte* data,
                unsigned int offset,
                unsigned int size,
                bool last_chunk_call,
                std::list<Chunk*>* chunks) = 0;

        /**
         * Close the chunker session.
          * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Close();

        /**
         * returns the number of bytes that are currently chunked, but
         * are not encloses in a closed chunk (open data).
         * @return
         */
        virtual uint32_t open_chunk_position() = 0;

        /**
         * returns the chunk data that is not enclosed in a closed chunk
         * (open data).
         *
         * @param data
         * @param offset
         * @param size
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool GetOpenChunkData(byte* data, unsigned int offset, unsigned int size) = 0;

        /**
         * Clears the chunker session.
         * Used to reset a chunker session after an error.
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Clear();
};

/**
 * \ingroup chunker
 * Abstract base class for chunker implementations.
 */
class Chunker : public dedupv1::StatisticProvider {
    private:
    DISALLOW_COPY_AND_ASSIGN(Chunker);

    static MetaFactory<Chunker> factory_;
    public:

    static MetaFactory<Chunker>& Factory();
    /**
     * Constructor
     * @return
     */
    Chunker();

    /**
     * Destructor
     * @return
     */
    virtual ~Chunker();

    /**
     * Inits the chunker. The method is designed to be overwritten by subclasses.
     * The default implementation returns true.
     * @return
     */
    virtual bool Init();

    /**
     * Starts the chunker. The method must be overwritten by subclasses. The
     * chunker object must be ready for chunking after a successful call of this method.
     *
     * @param cmc
     * @return
     */
    virtual bool Start() = 0;

    /**
     * Creates a new thread-bound chunker session.
     * @return
     */
    virtual ChunkerSession* CreateSession() = 0;

    /**
     * Configures the chunker. The method is designed to be overwritten by subclasses.
     * The valid configurations depend from the actual subclass.
     *
     * No available options
     *
     * @param option_name
     * @param option
     * @return
     */
    virtual bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Closes the chunker. The method is designed to be overwritten by subclasses.
     * The default implementation frees its resources and returns true.
     * @return
     */
    virtual bool Close();

    /**
     * Returns the minimal (normal) chunk size.
     * Note however, that the chunker might generate smaller chunk in cases that the
     * session end is reached and the chunk is forced to finish.
     * @return
     */
    virtual size_t GetMinChunkSize() = 0;

    /**
     * returns the maximal chunk size.
     * There should never be a chunk that is larger.
     * @return
     */
    virtual size_t GetMaxChunkSize() = 0;

    /**
     * returns the average chunk size to system should
     * generate.
     *
     * @return
     */
    virtual size_t GetAvgChunkSize() = 0;
};

}

#endif  // CHUNKER_H__
