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
#ifndef CONTAINER_TRACKER_H__
#define CONTAINER_TRACKER_H__

#include <set>
#include "dedupv1.pb.h"

#include <core/dedup.h>

namespace dedupv1 {

/**
 * The container tracker is used to keep track of the
 * containers that have been processed by a client.
 *
 * A container tracker can also be serialized to a message
 * and stored, so that the state of a container tracker
 * can be restored after an restart.
 *
 * Note: Not thread-safe.
 */
class ContainerTracker {
        DISALLOW_COPY_AND_ASSIGN(ContainerTracker);
    private:
        uint64_t least_non_processed_container_id_;
        std::set<uint64_t> processed_containers_;

        uint64_t highest_seen_container_id_;

        std::set<uint64_t> currently_processed_containers_;

        /**
         * The tracker doesn't care if an container id not committed. It can return
         * that id anyway
         * @return
         */
        inline uint64_t GetLeastNonProcessedContainerId() const;
    public:
        ContainerTracker();
		
		/**
		 * Copies the state of the tracker
		 */
		void CopyFrom(const ContainerTracker& tracker);

        /**
         * A side effect is that we learn about new container ids as we assume that
         * each id that is checked with this function is already committed.
         * @param id
         * @return
         */
        bool ShouldProcessContainer(uint64_t id);

        /**
         * Same without side effect
         * @param id
         * @return
         */
        bool ShouldProcessContainerTest(uint64_t id) const;

        bool ProcessingContainer(uint64_t id);

        bool AbortProcessingContainer(uint64_t id);

        /**
         * marks a container as fully processed.
         */
        bool ProcessedContainer(uint64_t id);

        /**
         * returns true iff the container is currently be processed.
         */
        inline bool IsProcessingContainer(uint64_t id) const;

        /**
         * The tracker should reset if the system is started or restarted.
         * The system ensured that there will be never a commit of a container with an id
         * that is less or equal than the highest container id up to now.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Reset();

        /**
         * Clears the container tracker
         */
        void Clear();

        bool ParseFrom(const ContainerTrackerData& data);
        bool SerializeTo(ContainerTrackerData* data) const;

        /**
         * The tracker doesn't care if an container id not committed. It can return
         * that id anyway
         * @return
         */
        uint64_t GetNextProcessingContainer() const;

        inline uint64_t GetSize() const;

        std::string DebugString() const;
};

uint64_t ContainerTracker::GetLeastNonProcessedContainerId() const {
    return this->least_non_processed_container_id_;
}

bool ContainerTracker::IsProcessingContainer(uint64_t id) const {
    return this->currently_processed_containers_.find(id) != this->currently_processed_containers_.end();
}

uint64_t ContainerTracker::GetSize() const {
    return this->processed_containers_.size();
}

}

#endif  // CONTAINER_TRACKER_H__
