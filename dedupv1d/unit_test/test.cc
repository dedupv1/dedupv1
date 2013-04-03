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

#include <gtest/gtest.h>
#include <curl/curl.h>

#include <core/dedup_system.h>
#include <test_util/log_assert.h>
#include <test_util/test_listener.h>

#include "dedupv1.pb.h"

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/propertyconfigurator.h"
#include "log4cxx/helpers/exception.h"

using ::testing::EmptyTestEventListener;
using ::testing::InitGoogleTest;
using ::testing::Test;
using ::testing::TestCase;
using ::testing::TestEventListeners;
using ::testing::TestInfo;
using ::testing::TestPartResult;
using ::testing::UnitTest;

LOGGER("Test");

int main(int argc, char** argv) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    curl_global_init(CURL_GLOBAL_ALL);

#ifdef LOGGING_LOG4CXX
    log4cxx::PropertyConfigurator::configureAndWatch("logging.conf");
#endif
#ifdef LOGGING_SYSLOG
    dedupv1::base::logging::Syslog::instance().Open("dedupv1d_test", LOG_LOCAL1);
#endif

    umask(0007); // umask always succeeds
    int r = 0;
#ifndef NO_SCST
    r = access("/dev/scst_user", F_OK);
    if (r != 0) {
        ERROR("/dev/scst_user not ready.");
        exit(1);
    }
#endif
    r = access("work", R_OK | W_OK);
    if (r != 0) {
        mkdir("work", 0755);
    }

    dedupv1::DedupSystem::RegisterDefaults();

    testing::InitGoogleTest(&argc, argv);

    ::testing::TestEventListeners& listeners =
        ::testing::UnitTest::GetInstance()->listeners();
    // Adds a listener to the end.  Google Test takes the ownership.
    listeners.Append(new dedupv1::test::CleanWorkDirListener);

    int result = RUN_ALL_TESTS();

    curl_global_cleanup();

    google::protobuf::ShutdownProtobufLibrary();

    return result;
}
