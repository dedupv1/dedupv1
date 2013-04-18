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

#include <string>
#include <list>
#include <sstream>

#include "dedupv1.pb.h"
#include "dedupv1_stats.pb.h"

#include <core/dedup.h>

#include <base/locks.h>
#include <base/logging.h>
#include <base/index.h>

#include <core/log_consumer.h>
#include <core/log.h>
#include <core/block_mapping.h>
#include <core/block_mapping_pair.h>
#include <core/chunk_index.h>
#include <base/runnable.h>
#include <base/thread.h>
#include <core/dedup_system.h>
#include <base/strutil.h>
#include <base/memory.h>
#include <core/storage.h>
#include <base/fault_injection.h>
#include <core/garbage_collector.h>
#include <core/chunk_locks.h>
#include <core/block_index.h>

#include <tbb/tick_count.h>
#include <tbb/task.h>

using std::string;
using std::stringstream;
using std::vector;
using std::list;
using std::set;
using std::map;
using std::multimap;
using std::make_pair;
using std::pair;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::MutexLock;
using dedupv1::base::ScopedLock;
using dedupv1::base::TIMED_FALSE;
using dedupv1::base::NewRunnable;
using dedupv1::base::Thread;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::DELETE_ERROR;
using dedupv1::base::DELETE_OK;
using dedupv1::base::Index;
using dedupv1::base::IndexIterator;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingPair;
using dedupv1::chunkstore::StorageSession;
using dedupv1::Fingerprinter;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::put_result;
using dedupv1::log::event_type;
using dedupv1::log::LogReplayContext;
using dedupv1::log::EVENT_TYPE_OPHRAN_CHUNKS;
using dedupv1::log::EVENT_REPLAY_MODE_DIRECT;
using dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG;
using dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_DELETED;
using dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITE_FAILED;
using dedupv1::log::EVENT_TYPE_LOG_EMPTY;
using dedupv1::base::ProfileTimer;
using dedupv1::base::delete_result;
using dedupv1::chunkstore::Container;
using dedupv1::chunkstore::ContainerStorage;
using dedupv1::chunkstore::ContainerItem;
using dedupv1::chunkstore::storage_commit_state;
using dedupv1::chunkstore::STORAGE_ADDRESS_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_ERROR;
using dedupv1::chunkstore::STORAGE_ADDRESS_NOT_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_WILL_NEVER_COMMITTED;
using tbb::tick_count;
using dedupv1::chunkstore::Storage;
using dedupv1::base::ScopedPtr;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::PUT_KEEP;
using dedupv1::base::timed_bool;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::chunkindex::ChunkLocks;
using dedupv1::base::Future;
using dedupv1::base::Runnable;
using dedupv1::blockindex::BlockIndex;
using dedupv1::base::strutil::FriendlySubstr;
using dedupv1::base::ThreadUtil;

LOGGER("GarbageCollector");

namespace dedupv1 {
namespace gc {

MetaFactory<GarbageCollector> GarbageCollector::factory_("GarbageCollector", "gc");

MetaFactory<GarbageCollector>& GarbageCollector::Factory() {
  return factory_;
}

GarbageCollector::GarbageCollector(enum gc_concept concept) : gc_concept_(concept) {
}

GarbageCollector::~GarbageCollector() {
}

bool GarbageCollector::Init() {
    return true;
}

bool GarbageCollector::SetOption(const std::string& option_name, const std::string& option) {
    ERROR("Invalid option: " << option_name << "=" << option);
    return false;
}

bool GarbageCollector::Start(const dedupv1::StartContext& start_context,
                             DedupSystem* system) {
    return true;
}

bool GarbageCollector::Stop(const dedupv1::StopContext& stop_context) {
    return true;
}

bool GarbageCollector::Close() {
    delete this;
    return true;
}

bool GarbageCollector::Run() {
    return true;
}

bool GarbageCollector::StartProcessing() {
    return true;
}

bool GarbageCollector::StopProcessing() {
    return true;
}

bool GarbageCollector::PauseProcessing() {
    return true;
}

bool GarbageCollector::ResumeProcessing() {
    return true;
}

bool GarbageCollector::IsProcessing() {
    return false;
}

dedupv1::base::Option<bool> GarbageCollector::IsGCCandidate(
    uint64_t address,
    const void* fp,
    size_t fp_size) {
    return false;
}

bool GarbageCollector::PutGCCandidates(
    const std::multimap<uint64_t, dedupv1::chunkindex::ChunkMapping>& gc_chunks,
    bool failed_mode) {
    return false;
}

dedupv1::base::PersistentIndex* GarbageCollector::candidate_info() {
    return NULL;
}

bool GarbageCollector::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    return true;
}

bool GarbageCollector::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    return true;
}

string GarbageCollector::PrintStatistics() {
    return "null";
}

string GarbageCollector::PrintProfile() {
    return "null";
}

string GarbageCollector::PrintTrace() {
    return "null";
}

string GarbageCollector::PrintLockStatistics() {
    return "null";
}

#ifdef DEDUPV1_CORE_TEST
void GarbageCollector::ClearData() {
}
#endif

}
}
