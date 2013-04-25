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

#include <base/base.h>
#include <base/aprutil.h>
#include <base/protobuf_util.h>

#include "dedupv1.pb.h"

#include <core/dedup_system.h>
#include <test_util/test_listener.h>
#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/propertyconfigurator.h"
#include "log4cxx/helpers/exception.h"

using ::testing::InitGoogleTest;
using ::testing::Test;
using ::testing::TestCase;
using ::testing::TestEventListeners;
using ::testing::TestInfo;
using ::testing::TestPartResult;
using ::testing::UnitTest;
using log4cxx::PropertyConfigurator;
using log4cxx::Logger;

LOGGER("TEST");

int main(int argc, char** argv) {
    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.
    GOOGLE_PROTOBUF_VERIFY_VERSION;

#ifdef LOGGING_LOG4CXX
    PropertyConfigurator::configureAndWatch("logging.conf");
#endif
#ifdef LOGGING_SYSLOG
    dedupv1::base::logging::Syslog::instance().Open("dedupv1_core_test", LOG_LOCAL1);
#endif
    dedupv1::base::ProtobufLogHandler::SetLog4CxxHandler();

    umask(0007); // umask always succeeds
    int r = access("work", R_OK | W_OK);
    if (r != 0) {
        if (mkdir("work", 0755) != 0) {
            WARNING("Failed to create core/unit_test/work directory");
        }
    }

    r = access("work/real", R_OK | W_OK);
    if (r != 0) {
        if (mkdir("work/real", 0755) != 0) {
            WARNING("Failed to create core/unit_test/work_real directory");
        }
    }

#ifdef LOGGING_LOG4CXX
    dedupv1::base::ProtobufLogHandler::SetLog4CxxHandler();
#endif

    dedupv1::DedupSystem::RegisterDefaults();

    InitGoogleTest(&argc, argv);

    TestEventListeners& listeners =
        UnitTest::GetInstance()->listeners();
    // Adds a listener to the end.  Google Test takes the ownership.
    listeners.Append(new dedupv1::test::CleanWorkDirListener);
    listeners.Append(new dedupv1::test::CopyRealWorkDirListener);

    int result = RUN_ALL_TESTS();

    google::protobuf::ShutdownProtobufLibrary();

    return result;
}
