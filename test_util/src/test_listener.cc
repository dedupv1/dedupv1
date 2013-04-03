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

#include <test_util/test_listener.h>

#include <gtest/gtest.h>
#include <sys/types.h>
#include <dirent.h>

using std::string;
using std::vector;
using ::testing::EmptyTestEventListener;
using ::testing::InitGoogleTest;
using ::testing::Test;
using ::testing::TestCase;
using ::testing::TestEventListeners;
using ::testing::TestInfo;
using ::testing::TestPartResult;
using ::testing::UnitTest;

namespace dedupv1 {
namespace test {

static int GetWorkDirFileCount() {
  DIR* dp = NULL;
  dp = opendir("work");
  if (dp == NULL) {
    return -1;
  }
  int count = 0;
  struct dirent* dirp;
  while((dirp = readdir(dp)) != NULL) {
      count += 1;
  }
  closedir(dp);
  return count;
}

// Called before a test starts.
void CleanWorkDirListener::OnTestStart(const TestInfo& test_info) {
    if (GetWorkDirFileCount() > 2) {
        system("rm -rf work/* 2>&1");
    }
}

// Called after a failed assertion or a SUCCESS().
void CleanWorkDirListener::OnTestPartResult(const TestPartResult& test_part_result) {
}

// Called after a test ends.
void CleanWorkDirListener::OnTestEnd(const TestInfo& test_info) {
}

// Called before a test starts.
void CopyRealWorkDirListener::OnTestStart(const TestInfo& test_info) {
    system("rsync data/real/* work/real/ 2>&1");
}

// Called after a failed assertion or a SUCCESS().
void CopyRealWorkDirListener::OnTestPartResult(const TestPartResult& test_part_result) {
}

// Called after a test ends.
void CopyRealWorkDirListener::OnTestEnd(const TestInfo& test_info) {
}

}
}
