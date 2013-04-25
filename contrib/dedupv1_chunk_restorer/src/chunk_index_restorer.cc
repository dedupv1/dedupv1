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

#include "chunk_index_restorer.h"

#include <iostream>

#include <core/block_index.h>
#include <core/block_mapping.h>
#include <core/chunk_index.h>
#include <core/container.h>
#include <core/container_storage.h>
#include <core/dedup_system.h>
#include <base/hashing_util.h>
#include <base/index.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <base/startup.h>
#include <base/memory.h>

#include <dedupv1.pb.h>
#include <tbb/tick_count.h>

using std::vector;
using dedupv1::DedupSystem;
using dedupv1::StartContext;
using dedupv1::chunkindex::ChunkIndex;
using dedupv1::chunkstore::Storage;
using dedupv1::chunkstore::ContainerStorage;
using dedupv1::chunkstore::Container;
using dedupv1::chunkstore::ContainerItem;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::blockindex::BlockIndex;
using dedupv1::base::PersistentIndex;
using dedupv1::base::IndexIterator;
using dedupv1::base::ScopedPtr;
using dedupv1d::Dedupv1d;
using dedupv1::log::Log;
using dedupv1::base::Option;
using dedupv1::FileMode;
using tbb::tick_count;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;

LOGGER("ChunkIndexRestorer");

namespace dedupv1 {
namespace contrib {
namespace restorer {

ChunkIndexRestorer::ChunkIndexRestorer() : dedup_system_(NULL), started_(false) {
    system_ = NULL;
}

bool ChunkIndexRestorer::InitializeStorageAndChunkIndex(const std::string& filename) {
    CHECK(!started_, "Chunk index restorer already started");
    system_ = new Dedupv1d();
    CHECK(system_->LoadOptions(filename), "Error loading options");

    CHECK(system_->OpenLockfile(), "Failed to acquire lock on lockfile");

    dedup_system_ = system_->dedup_system();
    dedup_system_->set_info_store(system_->info_store());

    dedup_system_->set_threadpool(system_->threadpool());

    // Start the log.
    // the log should not be created, but we have assume that the
    // system is dirty.
    StartContext start_context(StartContext::NON_CREATE, StartContext::DIRTY, StartContext::FORCE, false);
    start_context.set_crashed(true);

    CHECK(system_->info_store()->Start(start_context), "Failed to start info store");
    CHECK(dedup_system_->block_locks()->Start(), "Failed to start block locks");

    CHECK(dedup_system_->log()->Start(start_context, dedup_system_), "Failed to start log");
    CHECK(dedup_system_->log()->Run(), "Failed to run log");

    // Start the storage with create = false => old container storage will be read.
    CHECK(dedup_system_->storage()->Start(start_context, dedup_system_), "Could not start storage.");
    // Start the block index with create = false => old block index will be read.
    CHECK(dedup_system_->block_index()->Start(start_context, dedup_system_), "Failed to start block index");

    // here we replay the complete log. Parts are in memory, parts are not
    // Note: The chunk index is not a registered user
    CHECK(dedup_system_->log()->PerformDirtyReplay(),
        "Failed to perform dirty replay");

    // now we persist the block mapping data
    CHECK(dedup_system_->block_index()->ImportAllReadyBlocks(),
        "Failed to import all ready block mappings");

    // Get and start the chunk index with create = restore => new chunk index will be created,
    // if we want to restore.
    StartContext restore_start_context(StartContext::CREATE, StartContext::DIRTY, StartContext::FORCE);
    if (system_->daemon_group().size() > 0) {
        // use custom group
        Option<FileMode> file_mode = dedupv1::FileMode::Create(system_->daemon_group(), false, 0);
        CHECK(file_mode.valid(), "Failed to get file mode for group: " << system_->daemon_group());
        restore_start_context.set_file_mode(file_mode.value());

        file_mode = dedupv1::FileMode::Create(system_->daemon_group(), true, 0);
        CHECK(file_mode.valid(), "Failed to get file mode for group: " << system_->daemon_group());
        restore_start_context.set_dir_mode(file_mode.value());
    }
    CHECK(dedup_system_->chunk_index()->Start(restore_start_context, dedup_system_), "Unable to create chunk index");
    started_ = true;
    return true;
}

bool ChunkIndexRestorer::RestoreChunkIndexFromContainerStorage() {
    CHECK(started_, "Chunk index restorer not started");
    // 0. Get the storage and chunk index from the system
    CHECK(dedup_system_ != NULL, "Dedup System is null");
    ChunkIndex* chunk_index = dedup_system_->chunk_index();
    CHECK(chunk_index, "Chunk Index was not set while restoring");

    INFO("Step 1");
    CHECK(ReadContainerData(chunk_index), "Could not read container data");

    INFO("Step 2");
    CHECK(RestoreUsageCount(chunk_index), "Could not restore usage count");
    return true;
}

bool ChunkIndexRestorer::ReadContainerData(ChunkIndex* chunk_index) {
    CHECK(chunk_index, "Chunk index not set");

    Storage* tmp_storage = dedup_system_->storage();
    CHECK(tmp_storage, "Dedup System storage NULL");
    ContainerStorage* storage = dynamic_cast<ContainerStorage*>(tmp_storage);
    CHECK(storage, "Storage was not a container storage while restoring");

    IndexIterator* i = storage->meta_data_index()->CreateIterator();
    CHECK(i, "Failed to get iterator");
    ScopedPtr<IndexIterator> scoped_iterator(i);

    INFO("Restoring chunk index data");

    // Have a vector<bool> for all duplicate (i.e. secondary) ids so that we don't read
    // those containers also. Note that a vector<bool> only consumes 1 bit per field.
    std::vector<bool> duplicate_ids(storage->meta_data_index()->GetItemCount(), false);

    uint64_t container_entry_count = storage->meta_data_index()->GetItemCount();
    uint64_t processed_container = 0;
    double progress = 0.0;
    tick_count start_time = tick_count::now();

    uint64_t container_id;
    size_t key_size = sizeof(container_id);
    lookup_result lr = i->Next(&container_id, &key_size, NULL);
    while (lr == LOOKUP_FOUND) {
        DEBUG("Process container id " << container_id);

        if ((processed_container * 100.0 / container_entry_count) >= progress + 1.0) {
            progress = (processed_container * 100.0 / container_entry_count);

            tick_count::interval_t run_time = tick_count::now() - start_time;
            INFO("Restoring chunk index data: " << static_cast<int>(progress) << "%, running time " << run_time.seconds() << "s");
        }

        if (duplicate_ids.size() <= container_id) {
            duplicate_ids.resize(container_id * 2, false);
        }
        if (!duplicate_ids.at(container_id)) {
            // Read the container.
            Container container(container_id, storage->GetContainerSize(), true);
            lookup_result read_result = storage->ReadContainer(&container);
            CHECK(read_result != LOOKUP_ERROR, "Failed to read container " << container_id);
            if (read_result == LOOKUP_NOT_FOUND) {
                WARNING("Inconsistent container meta data: container " << container_id << " not found");
            } else {
                DEBUG("Restore container " << container.DebugString());

                // Get the container items.
                vector<ContainerItem*>& items = container.items();
                vector<ContainerItem*>::const_iterator item_iterator;
                for (item_iterator = items.begin(); item_iterator != items.end(); ++item_iterator) {
                    ContainerItem* item = *item_iterator;
                    // Create a chunk mapping.
                    ChunkMapping mapping(item->key(), item->key_size());

                    // Set the correct data address
                    mapping.set_data_address(item->original_id());
                    // Usage-Count is adjusted later.
                    mapping.set_usage_count(0);

                    DEBUG("Restore container item " << mapping.DebugString());
                    // Insert the chunk mapping into the chunk index.
                    CHECK(chunk_index->PutPersistentIndex(mapping, true, false, NO_EC),
                        "Failed to store chunk mapping: " << mapping.DebugString());

                }

                if (duplicate_ids.size() <= container.primary_id()) {
                    duplicate_ids.resize(container.primary_id() * 2, false);
                }
                duplicate_ids.at(container.primary_id()) = true;

                // Inform the chunk index that it needs to save the containers.
                chunk_index->container_tracker()->ProcessedContainer(container.primary_id());

                // Update the duplicate ids.
                std::set<uint64_t>::const_iterator id_iterator;
                for (id_iterator = container.secondary_ids().begin(); id_iterator != container.secondary_ids().end(); ++id_iterator) {
                    if (duplicate_ids.size() <= *id_iterator) {
                        duplicate_ids.resize(*id_iterator * 2, false);
                    }
                    duplicate_ids.at(*id_iterator) = true;

                    // Inform the chunk index that it needs to save the containers.
                    chunk_index->container_tracker()->ProcessedContainer(*id_iterator);
                }
            }
        }
        processed_container++;
        lr = i->Next(&container_id, &key_size, NULL);
    }
    CHECK(lr != LOOKUP_ERROR, "Failed to get container id");

    return true;
}

bool ChunkIndexRestorer::RestoreUsageCount(ChunkIndex* chunk_index) {
    CHECK(started_, "Chunk index restorer not started");
    CHECK(chunk_index, "Chunk index not set");

    // Iterate over the block index to get the usage count.
    BlockIndex* block_index = dedup_system_->block_index();
    CHECK(block_index, "Dedup System block index NULL");
    PersistentIndex* persistent_block_index = block_index->persistent_block_index();
    CHECK(persistent_block_index, "Persistent Block Index NULL");

    INFO("Restoring chunk usage count data");

    IndexIterator* iter = persistent_block_index->CreateIterator();
    CHECK(iter, "Index iterator was NULL");
    ScopedPtr<IndexIterator> scoped_iter(iter);

    uint64_t block_entry_count = persistent_block_index->GetItemCount();
    uint64_t processed_blocks = 0;
    double progress = 0.0;
    tick_count start_time = tick_count::now();

    BlockMappingData block_mapping_data;
    uint64_t key;
    size_t key_size = sizeof(key);
    while (iter->Next(&key, &key_size, &block_mapping_data) == LOOKUP_FOUND) {

        if ((processed_blocks * 100.0 / block_entry_count) >= progress + 1.0) {
            progress = (processed_blocks * 100.0 / block_entry_count);

            tick_count::interval_t run_time = tick_count::now() - start_time;
            INFO("Restoring chunk usage count data: " << static_cast<int>(progress) << "%, running time " << run_time.seconds() << "s");
        }
        BlockMapping block_mapping(key, dedup_system_->block_size());
        CHECK(block_mapping.UnserializeFrom(block_mapping_data, false),
            "Failed to unserialize data: " << block_mapping_data.ShortDebugString());
        DEBUG("Process block: " << block_mapping.DebugString());

        // Iterate over the block mapping items.
        std::list<BlockMappingItem>::iterator j;
        for (j = block_mapping.items().begin(); j != block_mapping.items().end(); ++j) {
            BlockMappingItem& item(*j);

            if (Fingerprinter::IsEmptyDataFingerprint(item.fingerprint(), item.fingerprint_size())) {
                // we do not maintain the usage count for the "virtual" empty data fingerprint
                continue;
            }
            // Get the corresponding mapping from the chunk index.
            ChunkMapping mapping(item.fingerprint(), item.fingerprint_size());
            mapping.set_data_address(item.data_address());

            lookup_result result = chunk_index->Lookup(&mapping, false, NO_EC);
            CHECK(result == LOOKUP_FOUND, "Block mapping not found in chunk index: " <<
                "block mapping " << block_mapping.DebugString() <<
                ", chunk mapping " << mapping.DebugString() <<
                " result: " << result);

            // Increase usage count of the mapping by one.
            mapping.set_usage_count(mapping.usage_count() + 1);
            if (mapping.usage_count_change_log_id() < block_mapping_data.event_log_id()) {
                mapping.set_usage_count_change_log_id(block_mapping_data.event_log_id());
            }
            DEBUG("Update usage count " << mapping.DebugString());

            // Put it back to the chunk index.
            CHECK(chunk_index->PutPersistentIndex(mapping, true, false, NO_EC),
                "Failed to put usage change to index: " << mapping.DebugString());
        }
        processed_blocks++;
    }
    return true;
}

bool ChunkIndexRestorer::Close() {
    // TODO (dmeister): Here we bypass the normal shutdown system. It should work, but is certainly not optimal
    DEBUG("Closing chunk index restorer");
    if (system_) {
        // we cannot declare the system as clean because the block indexes might not be cleaned
        dedupv1::StopContext stop_context(dedupv1::StopContext::FastStopContext());

        if (dedup_system_) {
            if (dedup_system_->chunk_index()) {
                CHECK(dedup_system_->chunk_index()->Stop(stop_context), "Cannot stop chunk index");
            }
            if (dedup_system_->storage()) {
                CHECK(dedup_system_->storage()->Stop(stop_context), "Cannot stop storage");
            }
            if (dedup_system_->block_index()) {
                CHECK(dedup_system_->block_index()->Stop(stop_context), "Cannot stop block index");
            }
        }

        CHECK(system_->Close(), "Failed to close system");
        system_ = NULL;
    }
    return true;
}

}
}
}
