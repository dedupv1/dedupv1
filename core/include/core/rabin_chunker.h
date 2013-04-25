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

#ifndef RABIN_CHUNKER_H__
#define RABIN_CHUNKER_H__

#include <tbb/atomic.h>

#include <core/dedup.h>
#include <core/chunker.h>
#include <base/profile.h>
#include <cryptopp/gf2n.h>
#include <gtest/gtest_prod.h>

namespace dedupv1 {

/**
 * Rabin Chunking (also called Variable-Sized Chunking or Content-Defined Chunking)
 * is a method to chunk a data stream into parts that is sonely based on the contents
 * of the data and not on the position of the data in the data stream.
 *
 * It calculates for all substrings with a fixed size k a hash value. Often a window size of 48 bytes is used.
 * Using these hash values, all data between to positions, whose fingerprints f fullfill
 * f mod n = c for a constant 0 < c <= n, is saved in a single chunk.
 *
 * Thread safety: the RabinChunker can be used from multiple threads in parallel, the RabinChunkerSession
 * should only be used by a single thread.
 *
 * This chunking method was first proposed in "A. Muthitacharoen, B. Chen, and D. Mazieres.
 * A low-bandwidth network file system. In Symposium on Operating Systems Principles,
 * pages 174–187, 2001.". Since then is become a standard technique in deduplication systems.
 *
 * For a comparision of different chunking methods, see "D. Meister and A. Brinkmann,
 * Multi-level comparision of data deduplication in a backup scenario, SYSTOR 2009, Haifa."
 *
 * The chunking method has its name because Rabin's fingerprint method is often used, because it is
 * fast, has known and reliable collission properties, and it can be modified to be a rolling hash function.
 *
 * Rabin's fingerprinting method is based on "M. O. Rabin. Fingerprinting by random polynomials.
 * Technical report, Center for Research in Computing Technology, 1981.". Broder provided in
 * "A. Broder. Some applications of Rabin’s fingerprinting method, pages 143–152. Springer Verlag, 1993."
 * an first implementation strategy based on modulo tables.
 *
 * This implementation is based on "C. Chan and H. Lu. Fingerprinting using poloynomial
 * (rabin’s method). CM- PUT690 Term Projekt, December 2001.", but optimized to use
 * 64-bit registers which provided a huge speedup. The polynom operations are described (in a more
 * general way) in "D. Knuth, The Art of Computer Programming, Volume 3.".
 *
 * For more informations and a proof of correctness, see: "Dirk Meister, Data Deduplication
 * using Flash-based indexes, Master thesis, University of Paderborn, 2009" or the
 * summary in "docs/rabin-notes.pdf" (German).
 *
 */
class RabinChunker : public Chunker {
    DISALLOW_COPY_AND_ASSIGN(RabinChunker);
    friend class RabinChunkerTest;
    FRIEND_TEST(RabinChunkerTest, ModTable);
    FRIEND_TEST(RabinChunkerTest, InvertTable);
    FRIEND_TEST(RabinChunkerTest, Rolling);
    FRIEND_TEST(FingerprinterTest, EmptyFingerprint);

    /**
     * Type for statistics about the rabin chunker
     */
    class Statistics {
        public:
            Statistics();
            /**
             * number of created chunks
             */
            tbb::atomic<uint64_t> chunks_;

            /**
             * number of chunks that have been forced to close because of the size
             */
            tbb::atomic<uint64_t> size_forced_chunks_;

            /**
             * number of chunks that have been forced to close because of an end of
             * the chunker session closed.
             */
            tbb::atomic<uint64_t> close_forced_chunks_;

            /**
             * time spend chunking
             */
            dedupv1::base::Profile time_;
    };

    /**
     * most significant bti as polynomial
     */
    static const CryptoPP::PolynomialMod2 kMostSignificantBit;
    static const uint64_t kDefaultPolynom;
    static const uint32_t kDefaultWindowSize = 48;
    static const uint32_t kShift = 55;

    /**
     * default minimal chunk size
     */
    static const uint32_t kDefaultMinChunkSize = 2028;

    /**
     * default maximal chunk size
     */
    static const uint32_t kDefaultMaxChunkSize = 32768;

    protected:
    /**
     * The irreducible polynom used by Rabin's fingerprinting method
     */
    CryptoPP::PolynomialMod2 poly_;

    /**
     * The breakmark pattern to detect chunk boundaries
     */
    uint64_t breakmark_;

    /**
     * the lookup table for
     */
    uint64_t t_[256];

    /**
     * inversion table used to make Rabin's fingerprints a
     * rolling hash function.
     */
    uint64_t u_[256];

    /**
     * Average chunk size
     */
    unsigned int avg_chunk_;

    /**
     * Minimal allowed chunk size.
     * If the chunk generates a smaller chunk, the chunk is not
     * accepted and the next chunk boundary defines the chunk.
     */
    unsigned int min_chunk_;

    /**
     * Maximal allowed chunk size.
     * If no chunk boundary is generated before the chunk has this
     * size, the chunk is forced to be accepted.
     */
    unsigned int max_chunk_;

    /**
     * Size of the hash window.
     */
    unsigned short window_size_;

    /**
     * precalculated min_chunk_ - window_size_
     * This value is accessed very often in the critical path
     */
    unsigned int position_window_before_min_size_;

    /**
     * Statistics about the rabin chunker
     */
    Statistics stats_;

    /**
     * precalculates the modulo table used to calculate in module "p".
     */
    void CalculateModTable();

    /**
     * precalculates the invert table used to remove a character from the fingerprint of a window.
     *
     * It used a "invert table" to speed up the processing.
     *
     */
    void CalculateInvertTable();

    /**
     * appends a new byte to an existing fingerprint using the data stored in chunker.
     */
    uint64_t FingerprintAppendByte(uint64_t p, byte m);
    public:
    static void RegisterChunker();
    static Chunker* CreateChunker();

    /**
     * Constructor
     * @return
     */
    RabinChunker();

    /**
     * Destructor
     */
    virtual ~RabinChunker();

    /**
     * Starts the rabin chunker.
     * Here the module and the inverse table are calculated.
     *
     * @param cmc
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start();

    /**
     * Creates a new chunker session that should only be used
     * by a single thread.
     * @return
     */
    virtual ChunkerSession* CreateSession();

    /**
     * Configures the chunker.
     *
     * Available options:
     * - avg-chunk-size: StorageUnit, Sets the average chunk size.
     * - min-chunk-size: StorageUnit, Minimal chunk size
     * - max-chunk-size: StorageUnit, Maximal chunk size
     * - window-size: uint32_t
     * - polynom: String
     *
     * @param name
     * @param data
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool SetOption(const std::string& name, const std::string& data);

    /**
     * prints the pre-calculated tables T and U for testing purposes.
     */
    void PrintTables();

    /**
     * Persist the statistics.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * Restore the statistics at startup
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
     * prints statistics about the chunker
     */
    virtual std::string PrintStatistics();

    /**
     * print profile information about the chunker
     */
    virtual std::string PrintProfile();

    /**
     * returns the configured minimal chunk size
     */
    virtual size_t GetMinChunkSize();

    /**
     * returns the configured maximal chunk size
     */
    virtual size_t GetMaxChunkSize();

    /**
     * return the configured average chunk size
     */
    virtual size_t GetAvgChunkSize();

    friend class RabinChunkerSession;
};

/**
 * The rabin chunker session is the part of the rabin chunker that
 * performs the actual chunking.
 *
 * The RabinChunker is kind of a factory for the session.
 * The session should only be used within a single thread.
 */
class RabinChunkerSession : public ChunkerSession {
    protected:
        friend class RabinChunkerTest;
        FRIEND_TEST(RabinChunkerTest, Window);
        FRIEND_TEST(RabinChunkerTest, Simple);
        FRIEND_TEST(RabinChunkerTest, Rolling);

        /**
         * Reference to the global chunker
         */
        RabinChunker* chunker_;

        /**
         * Currently calcuated fingerprint
         */
        unsigned long long fingerprint_;

        /**
         * Cyclic buffer for the current window
         */
        byte* window_buffer_;

        /**
         * Current start position of the cycle
         * in the window buffer
         */
        uint16_t window_buffer_pos_;

        /**
         * Data of the current chunk.
         * This stores all the data of the current chunk that has been inserted into the session before the current ChunkData call
         */
        byte* overflow_chunk_data_;

        /**
         * size of the chunk that is currently in-creation.
         */
        uint32_t overflow_chunk_data_pos_;

        /**
         * Note: Implementation is inlined. Definition: see below.
         * @param c
         */
        inline void UpdateWindowFingerprint(byte c);
        void UpdateFingerprint(byte c);

        /**
         * Creates a new chunk from the current chunk data and appends it to the chunk list.
         * Called when a chunk is finished and should be accepted for further processing
         */
        bool AcceptChunk(std::list<Chunk*>* chunks, const byte* data, uint32_t size);
    public:
        /**
         * Constructor
         */
        explicit RabinChunkerSession(RabinChunker* chunker);

        /**
         * Destructor
         */
        virtual ~RabinChunkerSession();

        /**
         * Chunks a data stream using the sliding window rabin fingerprint.
         *
         * @param data            pointer to a byte buffer of size "size"
         * @param offset
         * @param size            size of the data buffer
         * @param chunks list that holds all created chunks
         */
        virtual bool ChunkData(const byte* data,
                unsigned int offset, unsigned int size, bool last_chunk_call, std::list<Chunk*>* chunks);

        /**
         * return the number of bytes that are processed, but not assigned to a chunk
         */
        virtual uint32_t open_chunk_position();

        /**
         * copies data that is processed, but not assigned to a chunk
         *
         * @param data data buffer to copy to
         * @param offset offset within the open chunk data
         * @param size number of bytes to copy.
         */
        virtual bool GetOpenChunkData(byte* data, unsigned int offset, unsigned int size);

    	/**
         * return the current rabin fingerprint value (64bit)
         */
        inline unsigned long long fingerprint();

        /**
         * Clears the session
         */
        virtual bool Clear();
};

unsigned long long RabinChunkerSession::fingerprint() {
    return fingerprint_;
}

inline void RabinChunkerSession::UpdateWindowFingerprint(byte c) {
    // update cyclic window buffer
    if (unlikely((++this->window_buffer_pos_) == chunker_->window_size_)) {
        this->window_buffer_pos_ = 0;
    }
    byte old_char = this->window_buffer_[this->window_buffer_pos_];

    // update fingerprint by removing the oldest char in the buffer and adding the new byte
    unsigned long long old_fingerprint = this->fingerprint_ ^ chunker_->u_[static_cast<int>(old_char)];
    this->fingerprint_ = ((old_fingerprint << 8) | c) ^ chunker_->t_[old_fingerprint >> RabinChunker::kShift];
    this->window_buffer_[this->window_buffer_pos_] = c;
}

}

#endif  // RABIN_CHUNKER_H__
