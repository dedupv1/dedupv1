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

#ifndef BLOCK_INDEX_BG_H__
#define BLOCK_INDEX_BG_H__

#include <vector>

#include <core/dedup.h>
#include <base/thread.h>
#include <base/locks.h>
#include <base/startup.h>

namespace dedupv1 {
namespace blockindex {

class BlockIndex;

/**
 * \ingroup blockindex
 *
 * The block index background committer is used
 * to move ready items from the auxiliary index to the
 * main index if a) the system is currently shutting down or b)
 * if the auxiliary index gets too large.
 */
class BlockIndexBackgroundCommitter {
    private:
        /**
         * Enumeration for the block index bg committer states
         */
        enum block_index_bg_state {
            CREATED, // !< CREATED
            STARTED, // !< STARTED
            RUNNING, // !< RUNNING
            STOPPED  // !< STOPPED
        };

        /**
         * Reference to the block index
         */
        dedupv1::blockindex::BlockIndex* block_index_;

        /**
         * check interval in seconds
         */
        uint32_t check_interval_;

        /**
         * thread that executes the importing.
         */
        std::vector<dedupv1::base::Thread<bool>* > threads_;

        /**
         * Number of threads to use for importing block mappings into
         * the background.
         */
        uint32_t thread_count_;

        /**
         * Lock to protected the internal structures, e.g.
         * the state member
         */
        dedupv1::base::MutexLock lock_;

        /**
         * State.
         * Protected by the lock
         */
        enum block_index_bg_state state_;

        bool stop_mode_;

        /**
         * Thread function that runs in a loop until the
         * background committed is stopped. All the
         * background commit processed is issued by this method.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Loop(int thread_id);
    public:
        /**
         * Creates a new block index committer
         * @param block_index
         * @return
         */
        BlockIndexBackgroundCommitter(BlockIndex* block_index,
                bool stop_mode);

        /**
         * Starts the operations of the block index background committer
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(int thread_count);

        /**
         * Starts the block index committer thread.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Run();

        /**
         * Stops the block index committer thread.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Stop(const dedupv1::StopContext& stop_context);

        DISALLOW_COPY_AND_ASSIGN(BlockIndexBackgroundCommitter);
};
}
}
#endif  // BLOCK_INDEX_BG_H__
