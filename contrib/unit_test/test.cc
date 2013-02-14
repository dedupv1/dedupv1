/*
 * dedupv1 - iSCSI based Deduplication System for Linux
 *
 * (C) 2008 Dirk Meister
 * (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
 */

#include <gtest/gtest.h>

#include <iostream>

#include <base/aprutil.h>
#include <base/protobuf_util.h>

#include "dedupv1.pb.h"

#include <test/test_listener.h>
#include <core/dedup_system.h>

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

int main(int argc, char** argv) {
    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    umask(0007); // umask always succeeds
#ifdef LOGGING_LOG4CXX
    log4cxx::PropertyConfigurator::configureAndWatch("logging.conf");
#endif
#ifdef LOGGING_SYSLOG
    dedupv1::base::logging::Syslog::instance().Open("dedupv1_core_test", LOG_LOCAL1);
#endif
    int r = access("work", R_OK | W_OK);
    if (r != 0) {
        mkdir("work", 0755);
    }

    r = access("work/real", R_OK | W_OK);
    if (r != 0) {
        mkdir("work/real", 0755);
    }

    dedupv1::base::ProtobufLogHandler::SetLog4CxxHandler();
    dedupv1::DedupSystem::RegisterDefaults();

    testing::InitGoogleTest(&argc, argv);

    ::testing::TestEventListeners& listeners =
        ::testing::UnitTest::GetInstance()->listeners();
    // Adds a listener to the end.  Google Test takes the ownership.
    listeners.Append(new dedupv1::test::CleanWorkDirListener);

    int result = RUN_ALL_TESTS();

    google::protobuf::ShutdownProtobufLibrary();

    return result;
}
