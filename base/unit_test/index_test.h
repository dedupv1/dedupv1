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

#ifndef INDEX_TEST_H_
#define INDEX_TEST_H_

#include <gtest/gtest.h>

#include <base/index.h>
#include <test_util/log_assert.h>

#include <string>
#include <vector>

namespace dedupv1 {
namespace base {

/**
 * Parameterized tests for all index implementations
 */
class IndexTest : public testing::TestWithParam<std::string> {
    protected:
    USE_LOGGING_EXPECTATION();

    Index* index;
    std::string config;

    byte buffer[8192];
    int buffer_size;

    /**
     * Sets up the test
     */
    virtual void SetUp();

    /**
     * Tears down the text
     */
    virtual void TearDown();

    /**
     * Restarts the index
     */
    void Restart();
    public:

    /**
     * Creates a new index with the given options. The options are specified as a ";" separated string
     * with the first element denoting the index type.
     */
    static Index* CreateIndex(std::string options);

    static bool Write(Index* index,int start, int end);
    static bool BatchWrite(Index* index,int start, int end);

    static bool Read(Index* index, int start, int end);


};

}
}

#endif /* INDEX_TEST_H_ */
