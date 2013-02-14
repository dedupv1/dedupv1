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
#include <base/fault_injection.h>
#include <base/logging.h>
#include <test/log_assert.h>

#include <stdio.h>
#include <unistd.h>
#include <signal.h>

LOGGER("FaultInjectionTest");

void NormalExit() {
    FAULT_POINT("fault-inject-test.normal");
    exit(0);
}

class FaultInjectionDeathTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(FaultInjectionDeathTest, NormalExit) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(NormalExit(), ::testing::ExitedWithCode(0), "");
}

#ifdef FAULT_INJECTION

void CrashExit() {
    dedupv1::base::FaultInjection::ActivateFaultPoint("fault-inject-test.crash");
    FAULT_POINT("fault-inject-test.crash"); // it should crash now
    exit(0);
}

TEST_F(FaultInjectionDeathTest, FaultPointCrash) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(CrashExit(), ::testing::KilledBySignal(SIGABRT), ".*");
}

#endif
