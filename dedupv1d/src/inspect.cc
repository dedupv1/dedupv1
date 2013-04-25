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

#include "inspect.h"
#include <base/logging.h>
#include <sstream>

#include <core/dedup_system.h>
#include <base/logging.h>
#include <core/container.h>
#include <core/storage.h>
#include <core/container_storage.h>
#include <base/strutil.h>
#include <core/log.h>
#include <core/log_consumer.h>
#include <core/block_index.h>
#include <core/block_mapping.h>
#include <core/chunk_index.h>
#include <core/chunk_mapping.h>
#include <core/dedupv1_scsi.h>
#include <base/memory.h>
#include <base/hashing_util.h>
#include "monitor.h"

using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::raw_compare;
using std::string;
using std::pair;
using std::stringstream;
using std::endl;
using std::list;
using std::vector;
using std::set;
using dedupv1::base::make_bytestring;
using dedupv1::scsi::ScsiResult;
using dedupv1::base::CRC;
using dedupv1::chunkstore::ContainerStorage;
using dedupv1::chunkstore::Container;
using dedupv1::chunkstore::ContainerItem;
using dedupv1::chunkstore::Storage;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::base::ScopedArray;
using dedupv1::Fingerprinter;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::lookup_result;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::chunkindex::ChunkIndex;
using dedupv1::DedupSystem;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_DELETED;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN;
using dedupv1::log::EVENT_TYPE_CONTAINER_COMMITED;
using dedupv1::log::EVENT_TYPE_CONTAINER_MERGED;
using dedupv1::log::Log;
using dedupv1::REQUEST_READ;
using dedupv1::chunkstore::storage_commit_state;
using dedupv1::chunkstore::STORAGE_ADDRESS_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_ERROR;
using dedupv1::chunkstore::STORAGE_ADDRESS_NOT_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_WILL_NEVER_COMMITTED;
using dedupv1::log::event_type;

LOGGER("Inspect");

namespace dedupv1d {

Inspect::Inspect(Dedupv1d* ds) {
    ds_ = ds;
}

namespace {
string FormatContainerAddress(const pair<lookup_result, ContainerStorageAddressData>& address) {
    stringstream sstr;
    sstr << "\"address\": ";
    if (address.first == LOOKUP_ERROR) {
        sstr << "\"<error>\"";
    } else if (address.first == LOOKUP_NOT_FOUND) {
        sstr << "\"<not found>\"";
    } else {
        sstr << "{" <<
        "\"file\": \"" << address.second.file_index() << "\"," <<
        "\"offset\": \"" << address.second.file_offset() << "\"";
        if (address.second.has_primary_id()) {
            sstr << ", " <<
            "\"primary id\": \"" << address.second.primary_id() << "\"";
        }
        if (address.second.has_log_id()) {
            sstr << ", " <<
            "\"log id\": \"" << address.second.log_id() << "\"";
        }
        sstr << "}";
    }
    return sstr.str();
}
}

string Inspect::ShowContainerHeader(uint64_t container_id) {
    DEBUG("Inspect container header " << container_id);
    stringstream sstr;
    sstr << "{";
    DedupSystem* system = ds_->dedup_system();
    if (system == NULL) {
        sstr << "\"ERROR\": \"System not found\"" << std::endl;
    } else {
        ContainerStorage* storage = dynamic_cast<ContainerStorage*>(system->storage());
        if (storage == NULL) {
            sstr << "\"ERROR\": \"Storage not found\"" << std::endl;
        } else if (storage->state() != ContainerStorage::RUNNING && storage->state() != ContainerStorage::STARTED) {
            sstr << "\"ERROR\": \"Storage not started\"" << std::endl;
        } else {
            Container container(container_id, storage->GetContainerSize(), false);

            enum lookup_result r = storage->ReadContainer(&container);
            if (r == LOOKUP_ERROR) {
                sstr << "\"ERROR\": \"Container " << container_id << " read failed\"" << std::endl;
            } else if (r == LOOKUP_NOT_FOUND) {
                enum storage_commit_state commit_state = storage->IsCommitted(container_id);
                if (commit_state == STORAGE_ADDRESS_ERROR) {
                    sstr << "\"ERROR\": \"Container " << container_id << " not found: failed to check commit state\"" << std::endl;
                } else if (commit_state == STORAGE_ADDRESS_COMMITED) {
                    sstr << "\"ERROR\": \"Container " << container_id << " not found: container is committed\"," << std::endl;
                    sstr << "\"commit state\": \"committed\"," << std::endl;
                } else if (commit_state == STORAGE_ADDRESS_NOT_COMMITED) {
                    sstr << "\"commit state\": \"not committed\"" << std::endl;
                } else if (commit_state == STORAGE_ADDRESS_WILL_NEVER_COMMITTED) {
                    sstr << "\"commit state\": \"never\"" << std::endl;
                } else {
                    sstr << "\"ERROR\": \"Container " << container_id << " not found: illegal commit state\"" << std::endl;
                }
            } else {
                // found
                sstr << "\"primary id\": \"" << container.primary_id() << "\"," << std::endl;
                sstr << "\"secondary ids\": [";
                set<uint64_t>::const_iterator j;
                for (j = container.secondary_ids().begin(); j != container.secondary_ids().end(); j++) {
                    if (j != container.secondary_ids().begin()) {
                        sstr << ",";
                    }
                    sstr << "\"" << *j << "\"";
                }
                sstr << "]," << std::endl;
                sstr << "\"commit state\": \"committed\"," << std::endl;
                if (container.commit_time() == 0) {
                    sstr << "\"commit time\": null," << std::endl;
                } else {
                    char time_buf[26]; // a buffer of 26 bytes is enough http://www.mkssoftware.com/docs/man3/ctime_r.3.asp
                    memset(time_buf, 0, 26);
                    time_t t = container.commit_time();
                    ctime_r(&t, time_buf);
                    time_buf[strlen(time_buf) - 1] = 0;
                    sstr << "\"commit time\": \"" << time_buf << "\"," << std::endl;
                }

                // address information
                pair<lookup_result, ContainerStorageAddressData> r = storage->LookupContainerAddress(container_id, NULL, false);
                sstr << FormatContainerAddress(r);
            }
        }
    }
    sstr << "}";
    return sstr.str();
}

string Inspect::ShowContainer(uint64_t container_id, bytestring* fp_filter) {
    DEBUG("Inspect container " << container_id);
    stringstream sstr;
    sstr << "{";
    DedupSystem* system = ds_->dedup_system();
    if (system == NULL) {
        sstr << "\"ERROR\": \"System not found\"" << std::endl;
    } else {
        ContainerStorage* storage = dynamic_cast<ContainerStorage*>(system->storage());
        if (storage == NULL) {
            sstr << "\"ERROR\": \"Storage not found\"" << std::endl;
        } else if (storage->state() != ContainerStorage::RUNNING && storage->state() != ContainerStorage::STARTED) {
            sstr << "\"ERROR\": \"Storage not started\"" << std::endl;
        } else {
            Container container(container_id, storage->GetContainerSize(), false);

            enum lookup_result r = storage->ReadContainer(&container);
            if (r == LOOKUP_ERROR) {
                sstr << "\"ERROR\": \"Container " << container_id << " read failed\"" << std::endl;
            } else if (r == LOOKUP_NOT_FOUND) {
                enum storage_commit_state commit_state = storage->IsCommitted(container_id);
                if (commit_state == STORAGE_ADDRESS_ERROR) {
                    sstr << "\"ERROR\": \"Container " << container_id << " not found: failed to check commit state\"" << std::endl;
                } else if (commit_state == STORAGE_ADDRESS_COMMITED) {
                    sstr << "\"ERROR\": \"Container " << container_id << " not found: container is committed\"," << std::endl;
                    sstr << "\"commit state\": \"committed\"," << std::endl;
                } else if (commit_state == STORAGE_ADDRESS_NOT_COMMITED) {
                    sstr << "\"commit state\": \"not committed\"" << std::endl;
                } else if (commit_state == STORAGE_ADDRESS_WILL_NEVER_COMMITTED) {
                    sstr << "\"commit state\": \"never\"" << std::endl;
                } else {
                    sstr << "\"ERROR\": \"Container " << container_id << " not found: illegal commit state\"" << std::endl;
                }
            } else {
                // found
                sstr << "\"primary id\": \" " << container.primary_id() << "\"," << std::endl;
                sstr << "\"secondary ids\": [";
                set<uint64_t>::const_iterator j;
                for (j = container.secondary_ids().begin(); j != container.secondary_ids().end(); j++) {
                    if (j != container.secondary_ids().begin()) {
                        sstr << ",";
                    }
                    sstr << "\"" << *j << "\"";
                }
                sstr << "]," << std::endl;
                sstr << "\"commit state\": \"committed\"," << std::endl;
                if (container.commit_time() == 0) {
                    sstr << "\"commit time\": null," << std::endl;
                } else {
                    char time_buf[26]; // a buffer of 26 bytes is enough http://www.mkssoftware.com/docs/man3/ctime_r.3.asp
                    memset(time_buf, 0, 26);
                    time_t t = container.commit_time();
                    ctime_r(&t, time_buf);
                    time_buf[strlen(time_buf) - 1] = 0;
                    sstr << "\"commit time\": \"" << time_buf << "\"," << std::endl;
                }
                sstr << "\"items\": [";
                vector<ContainerItem*>::const_iterator i;
                bool first = true;
                for (i = container.items().begin(); i != container.items().end(); i++) {
                    ContainerItem* item = *i;

                    if (fp_filter == NULL || raw_compare(fp_filter->data(), fp_filter->size(), item->key(), item->key_size()) == 0) {
                        if (!first) {
                            sstr << ",";
                        }
                        sstr << "{";
                        sstr << "\"fp\": \"" << Fingerprinter::DebugString(item->key(), item->key_size()) << "\",";
                        sstr << "\"offset\": \"" << item->offset() << "\",";
                        sstr << "\"on-disk size\": \"" << item->item_size() << "\",";
                        sstr << "\"raw size\": \"" << item->raw_size() << "\",";
                        sstr << "\"original id\": \"" << item->original_id() << "\"";
                        if (item->is_deleted()) {
                            sstr << ", \"state\": \"deleted\"";
                        }
                        sstr << "}";
                        first = false;
                    }
                }
                sstr << "]," << std::endl;

                // address information
                pair<lookup_result, ContainerStorageAddressData> r = storage->LookupContainerAddress(container_id, NULL, false);
                sstr << FormatContainerAddress(r);
            }
        }
    }
    sstr << "}";
    return sstr.str();
}

string PrintBlockMappingPairData(const BlockMappingPairData& data) {
    stringstream sstr;
    sstr << "{";
    sstr << "\"block id\": " << data.block_id() << ",";
    sstr << "\"version\": " << data.version_counter() << ",";
    sstr << "\"blocks\": [";
    for (int i = 0; i < data.items_size(); i++) {
        if (i != 0) {
            sstr << ",";
        }
        sstr << "{";
        bytestring bs = make_bytestring(data.items(i).fp());
        if (Fingerprinter::IsEmptyDataFingerprint(bs)) {
            sstr << "\"chunk\": \"<empty>\",";
        } else {
            sstr << "\"chunk\": \"" << Fingerprinter::DebugString(data.items(i).fp()) << "\",";
        }
        if (data.items(i).data_address() == Storage::EMPTY_DATA_STORAGE_ADDRESS) {
            sstr << "\"address\": \"<empty>\",";
        } else {
            sstr << "\"address\": \"" << data.items(i).data_address() << "\",";
        }
        sstr << "\"offset\": " << data.items(i).chunk_offset() << ",";
        sstr << "\"size\": " << data.items(i).size() << ",";
        sstr << "\"usage count modifier\": " << data.items(i).usage_count_modifier();
        sstr << "}";
    }
    sstr << "]";
    sstr << "}";
    return sstr.str();
}

string PrintBlockMappingData(const BlockMappingData& data) {
    stringstream sstr;
    sstr << "{";
    sstr << "\"block id\": " << data.block_id() << ",";
    sstr << "\"version\": " << data.version_counter() << ",";
    sstr << "\"blocks\": [";
    for (int i = 0; i < data.items_size(); i++) {
        if (i != 0) {
            sstr << ",";
        }
        sstr << "{";
        bytestring bs = make_bytestring(data.items(i).fp());
        if (Fingerprinter::IsEmptyDataFingerprint(bs)) {
            sstr << "\"chunk\": \"<empty>\",";
        } else {
            sstr << "\"chunk\": \"" << Fingerprinter::DebugString(data.items(i).fp()) << "\",";
        }
        if (data.items(i).data_address() == Storage::EMPTY_DATA_STORAGE_ADDRESS) {
            sstr << "\"address\": \"<empty>\",";
        } else {
            sstr << "\"address\": \"" << data.items(i).data_address() << "\",";
        }
        sstr << "\"offset\": " << data.items(i).chunk_offset() << ",";
        sstr << "\"size\": " << data.items(i).size() << "";
        sstr << "}";
    }
    sstr << "]";
    sstr << "}";
    return sstr.str();
}

string Inspect::ShowBlock(uint64_t block_id) {
    DedupSystem* system = ds_->dedup_system();
    CHECK_RETURN_JSON(system, "System not set");
    dedupv1::blockindex::BlockIndex* block_index = system->block_index();
    CHECK_RETURN_JSON(block_index, "Block index not set");

    stringstream sstr;

    if (block_index->state() != dedupv1::blockindex::BlockIndex::STARTED) {
        sstr << "null";
    } else {
        BlockMapping mapping(block_id, system->block_size());
        CHECK_RETURN_JSON(block_index->ReadBlockInfo(NULL, &mapping, NO_EC), "Failed to read block mapping");

        sstr << "{";
        sstr << "\"block id\": " << block_id << ",";
        sstr << "\"version\": " << mapping.version() << "," << std::endl;

        if (mapping.has_checksum()) {
            sstr << "\"checksum\": \"" << ToHexString(mapping.checksum().data(), mapping.checksum().size()) << "\"," << std::endl;
        }
        sstr << "\"blocks\": [";
        list<BlockMappingItem>::iterator i;
        for (i = mapping.items().begin(); i != mapping.items().end(); i++) {
            if (i != mapping.items().begin()) {
                sstr << ",";
            }
            sstr << "{";
            if (Fingerprinter::IsEmptyDataFingerprint(i->fingerprint(), i->fingerprint_size())) {
                sstr << "\"chunk\": \"<empty>\",";
            } else {
                sstr << "\"chunk\": \"" << Fingerprinter::DebugString(i->fingerprint(), i->fingerprint_size()) << "\",";
            }
            if (i->data_address() == Storage::EMPTY_DATA_STORAGE_ADDRESS) {
                sstr << "\"address\": \"<empty>\",";
            } else {
                sstr << "\"address\": \"" << i->data_address() << "\",";
            }
            sstr << "\"offset\": " << i->chunk_offset() << ",";
            sstr << "\"size\": " << i->size();
            sstr << "}";
        }
        sstr << "]";
        sstr << "}";
    }
    return sstr.str();
}

string Inspect::ShowLogInfo() {
    DedupSystem* system = ds_->dedup_system();
    CHECK_RETURN_JSON(system, "System not set");
    Log* log = system->log();
    CHECK_RETURN_JSON(log, "Log not set");

    stringstream sstr;
    sstr << "{";
    sstr << "\"log id\": " << log->log_id() << "," << std::endl;
    sstr << "\"replay id\": " << log->replay_id() << std::endl;
    sstr << "}";
    return sstr.str();
}

string Inspect::ShowLog(uint64_t log_id) {
    DedupSystem* system = ds_->dedup_system();
    CHECK_RETURN_JSON(system, "System not set");
    Log* log = system->log();
    CHECK_RETURN_JSON(system, "Log not set");

    LogEntryData log_entry;
    bytestring log_value;

    Log::log_read r = log->ReadEntry(log_id, &log_entry, &log_value, NULL);
    CHECK_RETURN_JSON(r != Log::LOG_READ_ERROR, "Failed to read log id");
    CHECK_RETURN_JSON(r != Log::LOG_READ_PARTIAL,
        "Log id is not the first part of a partial log entry")
    CHECK_RETURN_JSON(r != Log::LOG_READ_NOENT, "Log id is empty");
//    CHECK_RETURN_JSON(r != Log::LOG_READ_REPLAYED, "Log id is already replayed");

    LogEventData event_data;
    CHECK_RETURN_JSON(event_data.ParseFromArray(log_value.data(), log_value.size()),
        "Failed to parse log value");

    enum event_type event_type = static_cast<enum event_type>(event_data.event_type());
    stringstream sstr;
    sstr << "{";
    sstr << "\"log id\": " << log_id << "," << std::endl;
    sstr << "\"type\": \"" << Log::GetEventTypeName(event_type) << "\"," << std::endl;
    sstr << "\"size\": " << event_data.ByteSize() << "," << std::endl;
    if (event_type == EVENT_TYPE_BLOCK_MAPPING_WRITTEN) {
        BlockMappingWrittenEventData data = event_data.block_mapping_written_event();
        sstr << "\"data\": " << PrintBlockMappingPairData(data.mapping_pair());
    } else if (event_type == EVENT_TYPE_CONTAINER_COMMITED) {
        ContainerCommittedEventData data = event_data.container_committed_event();
        sstr << "\"data\": {";
        sstr << "\"container id\": " << data.container_id();
        if (data.has_address()) {
            sstr << "," << endl;
            sstr << "\"address\": \"" << ContainerStorage::DebugString(data.address()) << "\"";
        }
        sstr << "}";
    } else if (event_type == dedupv1::log::EVENT_TYPE_OPHRAN_CHUNKS) {
        OphranChunksEventData data = event_data.ophran_chunks_event();
        sstr << "\"data\": {";
        sstr << "\"ophran chunks\": [";

        for (int i = 0; i < data.chunk_fp_size(); i++) {
            if (i != 0) {
                sstr << ",";
            }
            sstr << Fingerprinter::DebugString(data.chunk_fp(i).data(), data.chunk_fp(i).size()) << std::endl;
        }
        sstr << "]" << std::endl;
        sstr << "}";

    } else if (event_type == EVENT_TYPE_CONTAINER_MERGED) {
        ContainerMergedEventData data = event_data.container_merged_event();
        sstr << "\"data\": {";
        sstr << "\"container\": [";

        // first container
        sstr << "{";
        sstr << "\"primary id\": " << data.first_id() << "," << std::endl;
        sstr << "\"secondary ids\": [";
        for (int j = 0; j < data.first_secondary_id_size(); j++) {
            if (j != 0) {
                sstr << ",";
            }
            sstr << data.first_secondary_id(j) << std::endl;
        }
        sstr << "]," << std::endl;
        sstr << "\"address\": \"" << ContainerStorage::DebugString(data.first_address()) << "\"" << std::endl;
        sstr << "},";

        // second container
        sstr << "{";
        sstr << "\"primary id\": " << data.second_id() << "," << std::endl;
        sstr << "\"secondary ids\": [";
        for (int j = 0; j < data.second_secondary_id_size(); j++) {
            if (j != 0) {
                sstr << ",";
            }
            sstr << data.second_secondary_id(j) << std::endl;
        }
        sstr << "]," << std::endl;
        sstr << "\"address\": \"" << ContainerStorage::DebugString(data.second_address()) << "\"" << std::endl;
        sstr << "}";

        sstr << "]" << std::endl;
        sstr << "}";
    } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_MOVED) {
        ContainerMoveEventData data = event_data.container_moved_event();
        sstr << "\"data\": {";
        sstr << "\"primary id\": " << data.container_id() << "," << std::endl;
        sstr << "\"old address\": \"" << ContainerStorage::DebugString(data.old_address()) << "\"," << std::endl;
        sstr << "\"new address\": \"" << ContainerStorage::DebugString(data.new_address()) << "\"" << std::endl;
        sstr << "}";
    } else {
        sstr << "\"data\": \"" << event_data.DebugString() << "\"" << std::endl;
    }
    sstr << "}";
    return sstr.str();
}

string Inspect::ShowChunk(const bytestring& fp) {
    DedupSystem* system = ds_->dedup_system();
    CHECK_RETURN_JSON(system, "System not set");
    ChunkIndex* chunk_index = system->chunk_index();
    CHECK_RETURN_JSON(chunk_index, "Chunk index not set");

    ChunkMapping mapping(fp);
    enum lookup_result r = chunk_index->Lookup(&mapping, false, NO_EC);
    CHECK_RETURN_JSON(r != LOOKUP_ERROR, "Failed to lookup chunk");
    CHECK_RETURN_JSON(r != LOOKUP_NOT_FOUND, "Chunk not found: " << Fingerprinter::DebugString(fp));
    stringstream sstr;
    sstr << "{";
    sstr << "\"data address\": " << mapping.data_address() << "," << std::endl;
    sstr << "\"usage count\": " << mapping.usage_count() << "," << std::endl;
    sstr << "\"usage count change log id\": " << mapping.usage_count_change_log_id() << std::endl;
    sstr << "}";
    return sstr.str();
}

}

