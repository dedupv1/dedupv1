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
#ifndef __CHUNK_INDEX_RESTORER_H
#define __CHUNK_INDEX_RESTORER_H

#include <gtest/gtest_prod.h>

#include <string>

#include <core/chunk_index.h>
#include <core/container_storage.h>
#include "dedupv1d.h"

namespace dedupv1 {
namespace contrib {
namespace restorer {

/**
* This class bundles functions for the chunk index restorer in order to make them
* testable.
*
* It is not necessary to replay the log before restoring the chunk index.
* This is important because it might not be possible to replay the log without the
* chunk index.
*/
class ChunkIndexRestorer {
        FRIEND_TEST(DedupSystemTest, ChunkIndexRestorerRestore);
    public:
        ChunkIndexRestorer();
        ~ChunkIndexRestorer();


        /**
        * Initializes the storage and chunk index from the config file.
        */
        bool InitializeStorageAndChunkIndex(const std::string& filename);

        /**
        * Restores the chunk index by reading through the entire container storage.
        */
        bool RestoreChunkIndexFromContainerStorage();

        /**
        * Closes the system.
        */
        bool Stop();

    private:
        dedupv1d::Dedupv1d* system_;
        dedupv1::DedupSystem* dedup_system_;

        bool started_;

        /**
        * Reads data from the containers.
        */
        bool ReadContainerData(dedupv1::chunkindex::ChunkIndex* chunk_index);

        /**
        * Restores the usage count of all chunk mappings.
        */
        bool RestoreUsageCount(dedupv1::chunkindex::ChunkIndex* chunk_index);
};

}
}
}


#endif  // __CHUNK_INDEX_RESTORER_H
