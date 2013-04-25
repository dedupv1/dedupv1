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

#ifndef CONTAINER_STORAGE_BG_H__
#define CONTAINER_STORAGE_BG_H__

#include <vector>
#include <string>
#include <set>

#include <tbb/spin_mutex.h>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/storage.h>
#include <core/log_consumer.h>
#include <base/index.h>
#include <base/thread.h>
#include <base/handover_store.h>
#include <base/profile.h>
#include <base/barrier.h>
#include <base/sliding_average.h>
#include <base/strutil.h>
#include <base/option.h>

namespace dedupv1 {
namespace chunkstore {

class ContainerStorage;
class ContainerStorageSession;
class Container;
class ContainerItem;
class ContainerGCStrategy;

/**
 * The container storage background committer is a
 * helper class to avoid committing a container in the
 * critical path. Using this class the container is
 * committed in the background.
 *
 * Note:
 * - At each point in time there is at most one container in hand-over state. This
 *   state should not take longer than a few milliseconds if there is a few commit
 *   thread.
 */
class ContainerStorageBackgroundCommitter {
    public:
        /**
         * Maximal number of bg commit threads
         */
        static const size_t kMaxThreads = 32;

        /**
         * Default number of bg commit threads
         */
        static const size_t kDefaultThreadCount = 8;
    private:

        /**
         * state of the committer
         */
        enum run_state {
            CREATED, //!< CREATED
            STARTING,//!< STARTING
            RUNNING, //!< RUNNING
            STOPPING,//!< STOPPING
            STOPPED  //!< STOPPED
        };

        /**
         * Reference to the storage system.
         */
        ContainerStorage* storage_;

        /**
         * Reference to the container that is currently committed.
         * Each thread has its own container
         */
        std::vector<Container* > current_container_;

        /**
         * Handover facility to handover single containers
         * between container storage and the committer in a concurrent-safe
         * way.
         */
        dedupv1::base::HandoverStore<std::pair<Container*, ContainerStorageAddressData> > handover_store_;

        /**
         * lock to ensure that only a single container is handed over
         * at a single point in time.
         */
        dedupv1::base::MutexLock handover_lock_;

        /**
         * Barrier for  that is fired if the handover is finished
         */
        dedupv1::base::Barrier handover_finished_barrier_;

        /**
         * Condition that is fired if a commit is finished.
         */
        dedupv1::base::Condition commit_finished_condition_;

        dedupv1::base::MutexLock commit_finished_condition_lock_;

        /**
         * Condition that is fired after the thread started running.
         * This ensures a consistent state and prevents a Start/Stop deadlock.
         *
         * We have to use a pointer here as the number of thread that should wait in the barrier is dynamic.
         *
         */
        dedupv1::base::Barrier* start_barrier_;

        /**
         * Statistics about the storage background committer
         */
        class Statistics {
            public:
                /**
                 * Constructor
                 */
                Statistics();

                /**
                 * Number of commit threads that are currently busy
                 */
                tbb::atomic<size_t> threads_busy_count_;

                /**
                 * TODO (dmeister): ???
                 */
                tbb::atomic<size_t> waiting_thread_count_;

                /**
                 * Average handover time over the last 8 containers that have been handed over.
                 */
                dedupv1::base::SimpleSlidingAverage average_waiting_time_;
        };

        /**
         * statistics
         */
        Statistics stats_;

        volatile enum run_state run_state_;

        /**
         * the background thread in that the container are committed
         */
        std::vector<dedupv1::base::Thread<bool>*> threads_;

        /**
         * Number of threads
         */
        size_t thread_count_;

        /**
         * tracks the container ids that are handed over but are not yet committed
         *
         * Note: Should only be modified with handover_container_set_lock acquired.
         */
        std::set<uint64_t> current_handover_container_set_;

        /**
         * Lock to protected the current_handover_container_set.
         * Used to avoid deadlocks that might be possible when using the normal handover_lock.
         * We had problems with a spinlock because the thread that held the lock
         * didn't made any progress. Therefore we are going back to a normal pthread lock
         */
        dedupv1::base::MutexLock handover_container_set_lock_;

        dedupv1::base::timed_bool ProcessContainer(Container* worker_container);
    public:
        /**
         * Constructor
         * @return
         */
        ContainerStorageBackgroundCommitter();

        virtual ~ContainerStorageBackgroundCommitter();

        /**
         * Configures the container storage bg committer.
         *
         * Available options:
         * - thread-count: size_t
         *
         * @param option_name
         * @param option
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(ContainerStorage* storage);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool Run();

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        // TODO(fermat): Why is thread_id a size_t and not an uint32_t or something like this...
        bool Loop(size_t thread_id);

        /**
         * The container is handed over to a container
         * background thread. There the container is
         * copied. When the handover method returns, the client
         * is allowed to reuse the container. The container background
         * thread committed a copied version of the container in
         * the background.
         *
         * @param c Container to handover to the background committers
         * @param address Address where to store the container
         * @return
         */
        dedupv1::base::timed_bool Handover(Container* c, const ContainerStorageAddressData& address);

        /**
         * Stops the background committer
         * @return
         */
        bool Stop(const dedupv1::StopContext& stop_context);

        /**
         * We assume that we work on an container if it is
         * a) not committed yet and b) <= then the last container that
         * has been handed over.
         *
         * @param address
         * @return
         */
        dedupv1::base::Option<bool> IsCurrentlyProcessedContainerId(uint64_t address);

        /**
         * returns the condition that is signaled when a commit of a container has finished
         * @return
         */
        dedupv1::base::timed_bool CommitFinishedConditionWaitTimeout(uint32_t s);

        bool WaitUntilProcessedContainerFinished();

        /**
         * returns the thread count of the background committer
         * @return
         */
        inline size_t thread_count() const;

        /**
         * Print trace data about the container storage that should be embedded into the trace
         * data of the container storage
         * @return
         */
        std::string PrintEmbeddedTrace();

        /**
         * Print profile data about the container storage that should be embedded into the trace
         * data of the container storage
         * @return
         */
        std::string PrintEmbeddedProfile();

        DISALLOW_COPY_AND_ASSIGN(ContainerStorageBackgroundCommitter);
};

size_t ContainerStorageBackgroundCommitter::thread_count() const {
    return this->thread_count_;
}

}
}

#endif  // CONTAINER_STORAGE_BG_H__

