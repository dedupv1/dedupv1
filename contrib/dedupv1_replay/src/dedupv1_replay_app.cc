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

#include <tbb/tick_count.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>

#include <core/block_index.h>
#include <base/strutil.h>
#include <base/index.h>
#include <core/storage.h>
#include <core/chunk_index.h>
#include <core/container_storage.h>
#include <core/dedup_system.h>
#include <core/filter.h>
#include <core/chunker.h>
#include <core/fingerprinter.h>
#include <base/aprutil.h>
#include <base/logging.h>
#include <base/config.h>

#include "dedupv1d.h"

#include "dedupv1.pb.h"
#include "dedupv1_replayer.h"

#ifdef LOGGING_LOG4CXX
#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/propertyconfigurator.h"
#include "log4cxx/helpers/exception.h"
#include "log4cxx/xml/domconfigurator.h"
#include <log4cxx/ndc.h>
#endif

#include <gflags/gflags.h>

using tbb::tick_count;
using std::string;
using dedupv1::log::LogConsumer;
using dedupv1::log::Log;
using dedupv1::DedupSystem;

LOGGER("Dedupv1Replay");

DEFINE_string(config, DEDUPV1_DEFAULT_CONFIG, "dedupv1 configuration file");
DEFINE_string(logging, DEDUPV1_ROOT "/etc/dedupv1/console_logging.xml", "Logging configuration file");

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
    log4cxx::NDC ndc("dedupv1_replay");
#endif
#ifdef LOGGING_SYSLOG
    dedupv1::base::logging::Syslog::instance().Open("dedupv1_replay");
#endif

    DedupSystem::RegisterDefaults();

    dedupv1::contrib::replay::Dedupv1Replayer replayer;
    int rc = 0;
    if (!replayer.Initialize(FLAGS_config)) {
        ERROR("Unable to initalize replayer");
        rc = 1;
    }
    if (rc == 0 && !replayer.Replay()) {
        ERROR("Unable to replay dedupv1 log");
        rc = 1;
    }
    replayer.Stop();
    google::protobuf::ShutdownProtobufLibrary();
    return rc;
}
