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
#include <linenoise.h>

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

DEFINE_string(config, DEDUPV1_DEFAULT_CONFIG, "dedupv1 configuration file");
DEFINE_string(logging, DEDUPV1_ROOT "/etc/dedupv1/console_logging.xml", "Logging configuration file");

LOGGER("Dedupv1Debug");

void Print(const std::string& monitor_data) {
    Json::Value root;   // will contains the root value after parsing.
    Json::Reader reader;
    bool parsingSuccessful = reader.parse( monitor_data, root );
    if ( !parsingSuccessful ) {
        // report to the user the failure and their locations in the document.
        std::cout << monitor_data;
    } else {
        std::cout << root;
    }
}

bool CommandLoop(Inspect* inspect) {
    char* line = NULL;
    uint64_t last_log_id = -1;

    while((line = linenoise("> ")) != NULL) {
        if (line[0] != '\0') {
            linenoiseHistoryAdd(line);

            string command(line);
            command = Trim(command);
            if (command == "quit" || command == "q") {
                break;
            }
            if (!command.empty()) {
                vector<string> command_parts;
                Split(command, " ", &command_parts);

                if(command_parts[0] == "container" || command_parts[0] == "c") {
                    CHECK(command_parts.size() == 2 || command_parts.size() == 3,
                            "Illegal command");

                    CHECK(To<uint64_t>(command_parts[1]).valid(), "Illegal command");
                    uint64_t container_id = To<uint64_t>(command_parts[1]).value();

                    if (command_parts.size() == 2) {
                        Print(inspect->ShowContainer(container_id, NULL));
                    } else {
                        string hex_fp = command_parts[2];
                        bytestring fp;
                        if(!Fingerprinter::FromDebugString(hex_fp, &fp)) {
                            WARNING("Failed to parse fingerprint");
                        } else {
                            Print(inspect->ShowContainer(container_id, &fp));
                        }
                    }
                } else if(command_parts[0] == "container-head") {
                    CHECK(command_parts.size() == 2, "Illegal command");
                    CHECK(To<uint64_t>(command_parts[1]).valid(), "Illegal command");
                    uint64_t container_id = To<uint64_t>(command_parts[1]).value();
                    Print(inspect->ShowContainerHeader(container_id));
                } else if (command_parts[0] == "fingerprint" || command_parts[0] == "chunk" || command_parts[0] == "fp") {
                    CHECK(command_parts.size() == 2, "Illegal command");
                    string hex_fp = command_parts[1];
                    bytestring fp;
                    if(!Fingerprinter::FromDebugString(hex_fp, &fp)) {
                        WARNING("Failed to parse fingerprint");
                    } else {
                        Print(inspect->ShowChunk(fp));
                    }
                } else if (command_parts[0] == "block" || command_parts[0] == "b") {
                    CHECK(command_parts.size() == 2, "Illegal command");
                    CHECK(To<uint64_t>(command_parts[1]).valid(), "Illegal command");
                    uint64_t block_id = To<uint64_t>(command_parts[1]).value();
                    Print(inspect->ShowBlock(block_id));
                } else if (command_parts[0] == "log" || command_parts[0] == "l") {
                    CHECK(command_parts.size() == 2, "Illegal command");

                    if (command_parts[1] == "next") {
                        last_log_id++;
                        Print(inspect->ShowLog(last_log_id));
                    } else if (command_parts[1] == "back") {
                        if (last_log_id == -1 || last_log_id == 0) {
                            cout << "Illegal command log back" << std::endl;
                        } else {
                            last_log_id--;
                            Print(inspect->ShowLog(last_log_id));
                        }
                    } else {
                        CHECK(To<uint64_t>(command_parts[1]).valid(), "Illegal command");
                        uint64_t log_id = To<uint64_t>(command_parts[1]).value();
                        Print(inspect->ShowLog(log_id));

                        last_log_id = log_id;
                    }
                } else if (command_parts[0] == "log-info") {
                    CHECK(command_parts.size() == 1, "Illegal command");
                    Print(inspect->ShowLogInfo());
                } else {
                    if (command_parts[0] != "help") {
                        cout << "Illegal command " << command << std::endl;
                    }
                    cout << "Available commands: " << std::endl <<
                            "container <cid>" << std::endl <<
                            "container-head <cid>" << std::endl <<
                            "fingerprint <fp>" << std::endl <<
                            "block <bid>" << std::endl <<
                            "log <logid>" << std::endl <<
                            "log-info" << std::endl <<
                            "help" << std::endl <<
                            "quit" << std::endl;
                }
            }
        }
        free(line);
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
    Inspect inspect(system);
    if(!CommandLoop(&inspect)) {
        exit(1);
    }
    INFO("Closing");
    if(!system->Close()) {
        WARNING("Failed to close system");
    }
    system = NULL;

    google::protobuf::ShutdownProtobufLibrary();
}


