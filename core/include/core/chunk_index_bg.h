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

#ifndef CHUNK_INDEX_BG_H__
#define CHUNK_INDEX_BG_H__

#include <vector>

#include <core/dedup.h>
#include <base/thread.h>
#include <base/locks.h>
#include <base/startup.h>

namespace dedupv1 {
namespace chunkindex {

class ChunkIndex;

/**
 * \ingroup chunkindex
 *
 * The chunk index background committer is used to
 * migrate chunk entries from the
 * auxiliary index to the main index if the chunk entry
 * is ready for that (that is if the container is committed).
 */
class ChunkIndexBackgroundCommitter {
        DISALLOW_COPY_AND_ASSIGN(ChunkIndexBackgroundCommitter);
    private:
        /**
         * States of the chunk index background committer
         */
        enum chunk_index_bg_state {
            CREATED,//!< CREATED
            STARTED,//!< STARTED
            RUNNING,//!< RUNNING
            STOPPED //!< STOPPED
        };

		/**
		 * chunk index
		 */
        ChunkIndex* chunk_index_;

        /**
         * check interval in seconds
         */
        uint32_t check_interval_;

        /**
         * interval between to container imports.
         */
        uint32_t wait_interval_;

        /**
         * Vector of threads in which the operations are done.
         * All threads execute the Loop method using a unique
         * thread id.
         */
        std::vector<dedupv1::base::Thread<bool>* > threads_;

        /**
         * Lock to protect the member of this class, especially
         * the state
         */
        dedupv1::base::MutexLock lock_;

        /**
         * Set to true if the committer is shutting down.
         */
        bool stoppingMode_;

        /**
         * State.
         * Protected by the lock
         */
        enum chunk_index_bg_state state_;

        /**
         * Loop method that is executed in background threads.
         *
         * @param thread_id
         * @return true iff ok, otherwise an error has occurred
         */
        bool Loop(uint32_t thread_id);
    public:
        /**
         * Constructor
         * @param block_index
         * @param check_interval
         * @param thread_count
         * @param stoppingMode if true, the committer is executed in the stopping phase
         * of the chunk index
         * @return
         */
        ChunkIndexBackgroundCommitter(ChunkIndex* block_index,
                uint32_t thread_count,
                uint32_t check_interval,
                uint32_t wait_interval,
                bool stoppingMode = false);

        /**
         * Desctrutor
         * @return
         */
        ~ChunkIndexBackgroundCommitter();

        /**
         * Starts the bg committer
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start();

        /**
         * Runs the bg committer
         * @return true iff ok, otherwise an error has occurred
         */
        bool Run();

        /**
         * Stops the background thread of the bg committer
         * @return true iff ok, otherwise an error has occurred
         */
        bool Stop(const dedupv1::StopContext& stop_context);

        /**
         * May deadlock if the bg committer has not been started before
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool Wait();
};

}
}

#endif  // CHUNK_INDEX_BG_H__
