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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>

#include <core/block_index.h>
#include <base/strutil.h>
#include <base/index.h>
#include <core/storage.h>
#include <core/chunk_index.h>
#include <core/dedup_system.h>
#include <core/filter.h>
#include <core/chunker.h>
#include <core/fingerprinter.h>
#include <base/aprutil.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <core/container.h>
#include <core/container_storage.h>
#include <base/config.h>
#include "inspect.h"

#include "dedupv1.pb.h"
#include <gflags/gflags.h>
#include <json/json.h>

#ifdef LOGGING_LOG4CXX
#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/propertyconfigurator.h"
#include "log4cxx/helpers/exception.h"
#include "log4cxx/xml/domconfigurator.h"
#endif

using std::cout;
using std::cin;
using std::getline;
using std::string;
using std::vector;
using dedupv1::base::strutil::Trim;
using dedupv1::base::strutil::Split;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::DedupSystem;
using dedupv1::chunkstore::ContainerStorage;
using dedupv1::chunkstore::Container;
using dedupv1::chunkstore::ContainerItem;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::chunkindex::ChunkIndex;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::log::Log;
using dedupv1::Fingerprinter;
using dedupv1d::Inspect;
using dedupv1d::Dedupv1d;
using std::stringstream;

DEFINE_string(config, DEDUPV1_DEFAULT_CONFIG, "dedupv1 configuration file");
DEFINE_string(logging, DEDUPV1_ROOT "/etc/dedupv1/console_logging.xml", "Logging configuration file");
DEFINE_bool(only_active, false, "Dumps only non-replayed log entries");
DEFINE_uint64(only_last, 0, "Show only the last X log entries");

LOGGER("Dedupv1Dump");

bool DumpLogEntry(Log* log, uint64_t log_id) {
    CHECK(log, "Log not set");

    LogEntryData log_entry;
    bytestring log_value;

    Log::log_read r = log->ReadEntry(log_id, &log_entry, &log_value, NULL);
    CHECK(r != Log::LOG_READ_ERROR, "Failed to read log id");
    CHECK(r != Log::LOG_READ_NOENT, "Log id is empty");
    if (r == Log::LOG_READ_PARTIAL) {
        return true;
    }
    LogEventData event_data;
    CHECK(event_data.ParseFromArray(log_value.data(), log_value.size()),
            "Failed to parse log value");

    char state = ' ';
    if (log_id <= log->replay_id()) {
        state = '#';
    }

    dedupv1::log::event_type event_type = static_cast<dedupv1::log::event_type>(event_data.event_type());
    stringstream sstr;
    sstr << state << std::setw(6) << log_id << "\t" << std::setw(20) << std::setiosflags(std::ios::left) << Log::GetEventTypeName(event_type);
    if (event_type == dedupv1::log::EVENT_TYPE_BLOCK_MAPPING_WRITTEN) {
        BlockMappingWrittenEventData data = event_data.block_mapping_written_event();
        sstr << "\t block id " << data.mapping_pair().block_id() << ", version " << data.mapping_pair().version_counter();
    } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_COMMITED) {
        ContainerCommittedEventData data = event_data.container_committed_event();
        sstr << "\t container id " << data.container_id() << ", address " << ContainerStorage::DebugString(data.address());
    } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_MERGED) {
        ContainerMergedEventData data = event_data.container_merged_event();
        sstr << "\t container id " << data.first_id() << ", hew address " << ContainerStorage::DebugString(data.new_address()) <<
                ": " << data.first_id() << " (address " << ContainerStorage::DebugString(data.first_address()) <<
                "), " << data.second_id() << " (address " << ContainerStorage::DebugString(data.second_address()) << ")";
    } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_MOVED) {
        ContainerMoveEventData data = event_data.container_moved_event();
        sstr << "\t container id " << data.container_id() << ", new address " << ContainerStorage::DebugString(data.new_address()) <<
                ", old address " << ContainerStorage::DebugString(data.old_address());
    } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_DELETED) {
        ContainerDeletedEventData data = event_data.container_deleted_event();
        sstr << "\t container id " << data.container_id() << ", address " << ContainerStorage::DebugString(data.address());
    } else if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_OPEN) {
        ContainerOpenedEventData data = event_data.container_opened_event();
        sstr << "\t container id " << data.container_id() << ", address " << ContainerStorage::DebugString(data.address());
    } else if (event_type == dedupv1::log::EVENT_TYPE_REPLAY_STARTED) {
        ReplayStartEventData data = event_data.replay_start_event();
        sstr << "\t replay type " << data.replay_type() << " replay id " << data.replay_id() << ", log id " << data.log_id();
    } else if (event_type == dedupv1::log::EVENT_TYPE_REPLAY_STOPPED) {
        ReplayStopEventData data = event_data.replay_stop_event();
        sstr << "\t replay type " << data.replay_type() << " replay id " << data.replay_id() << ", log id " << data.log_id();
    } else if (event_type == dedupv1::log::EVENT_TYPE_SYSTEM_START) {
        if (event_data.has_system_start_event()) {
            // TODO (dmeister): This is downwards compatibility check. May be removed in the future
            SystemStartEventData data = event_data.system_start_event();
            sstr << "\t create " << ToString(data.create()) <<  ", dirty " << ToString(data.dirty()) <<
                    ", forced " << ToString(data.forced()) <<
                    ", crashed " << ToString(data.crashed());
        }
    }
    sstr << std::endl;
    printf("%s", sstr.str().c_str());
    return true;
}

bool DumpLog(DedupSystem* dedup_system) {
    Log* log = dedup_system->log();
    CHECK(log, "Log not set");

    int64_t log_id = log->log_id() - log->log_data()->GetLimitId();
    if (FLAGS_only_last != 0) {
        if (log_id < log->log_id() - FLAGS_only_last) {
            log_id = log->log_id() - FLAGS_only_last;
        }
    }
    if (log_id < 0) {
        log_id = 0;
    }
    if (FLAGS_only_active) {
        log_id = log->replay_id();
    }
    for(;log_id < log->log_id(); log_id++) {
        DumpLogEntry(log, log_id);
    }
    return true;
}

int main(int argc, char * argv[]) {
    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    google::SetUsageMessage("[options]");
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc != 1) {
        fprintf(stderr, "Illegal arguments\n");
        return 1;
    }

    umask(0007); // umask always succeeds
#ifdef LOGGING_LOG4CXX
    log4cxx::xml::DOMConfigurator::configureAndWatch(FLAGS_logging);
#endif
#ifdef LOGGING_SYSLOG
    dedupv1::base::logging::Syslog::instance().Open("dedupv1_debug");
#endif

    DedupSystem::RegisterDefaults();
    Dedupv1d* system = new Dedupv1d();
    INFO("Loading options: " << FLAGS_config);
    if (!system->LoadOptions(FLAGS_config)) {
        exit(1);
    }

    if(!system->OpenLockfile()) {
        exit(1);
    }

    INFO("Starting");

    // Start the system, but do not replay the log
    // Present the system as valid or invalid as it is at present
    dedupv1::StartContext start_context(dedupv1::StartContext::NON_CREATE);
    if (!system->Start(start_context, true)) {  // start, but do not perform a log replay
        WARNING("System start failed: Data inconsistencies possible");
    }
    if(!DumpLog(system->dedup_system())) {
        exit(1);
    }
    INFO("Closing");
    if(!system->Close()) {
        WARNING("Failed to close system");
    }
    system = NULL;

    google::protobuf::ShutdownProtobufLibrary();
}


