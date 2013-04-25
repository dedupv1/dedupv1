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
#include <iostream>
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
#include <core/container.h>
#include <core/container_storage.h>
#include <core/dedup_system.h>
#include <core/filter.h>
#include <core/chunker.h>
#include <core/fingerprinter.h>
#include <base/aprutil.h>
#include <base/logging.h>
#include <base/config.h>
#include "dedupv1.pb.h"
#include "dedupv1d.h"
#include "dedupv1_checker.h"

#ifdef LOGGING_LOG4CXX
#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/propertyconfigurator.h"
#include "log4cxx/helpers/exception.h"
#include "log4cxx/xml/domconfigurator.h"
#include <log4cxx/ndc.h>
#endif

#include <gflags/gflags.h>

using std::string;
LOGGER("Dedupv1Check");

DEFINE_string(config, DEDUPV1_DEFAULT_CONFIG, "dedupv1 configuration file");
DEFINE_string(logging, DEDUPV1_ROOT "/etc/dedupv1/console_logging.xml", "Logging configuration file");
DEFINE_bool(log, false, "If set, a check of an un-replayed log is performed");
DEFINE_bool(repair, false, "If set, correctable errors are found, these are repaired");
DEFINE_int32(passes, 0, "Divide the chunks in this number of passes, to reduce needed main memory. 0 to compute by main memory. Will be set to the next power of 2.");

int main(int argc, char * argv[]) {
    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    google::SetUsageMessage("[options]\n"
        "\n"
        "Exit codes\n"
        "0 - No errors\n"
        "1 - Storage system errors corrected\n"
        "4 - Storage system errors left uncorrected\n"
        "8 - Fatal error\n");
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc != 1) {
        fprintf(stderr, "Illegal arguments\n");
        return 1;
    }

    if (FLAGS_passes < 0) {
        fprintf(stderr, "Passes must be between 0 and %d\n", 2^15);
        return 1;
    }

    umask(0007); // umask always succeeds

#ifdef LOGGING_LOG4CXX
    log4cxx::xml::DOMConfigurator::configureAndWatch(FLAGS_logging);
    log4cxx::NDC ndc("dedupv1_check");
#endif
#ifdef LOGGING_SYSLOG
    dedupv1::base::logging::Syslog::instance().Open("dedupv1_check");
#endif

    dedupv1::DedupSystem::RegisterDefaults();

    dedupv1::contrib::check::Dedupv1Checker checker(FLAGS_log, FLAGS_repair);
    int rc = 0;
    if (!checker.set_passes(FLAGS_passes)) {
        ERROR("Could not set passes, will exit.\n");
        return 1;
    }
    if (!checker.Initialize(FLAGS_config)) {
        ERROR("Unable to initialize checker");
        rc = 8;
    }
    if (rc == 0) {
        if (!checker.Check()) {
            ERROR("Failed to check dedupv1");
            rc = 8;
        }
    }
    if (rc == 0) {
        if (checker.fixed_errors() > 0) {
            INFO("" << checker.fixed_errors() << " errors repaired");
            rc |= 1;
        }
        uint32_t uncorrected_error_count = checker.reported_errors() - checker.fixed_errors();
        if (uncorrected_error_count > 0) {
            INFO("" << uncorrected_error_count << " uncorrected errors found");
            rc |= 4;
        } else {
            INFO("Data integrity checked");
        }
    }
    // Close down everything.
    checker.Stop();
    google::protobuf::ShutdownProtobufLibrary();
    return rc;
}
