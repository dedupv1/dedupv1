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

#ifndef DEDUP_SYSTEM_TEST_H_
#define DEDUP_SYSTEM_TEST_H_

#include <gtest/gtest.h>
#include <string>

#include <core/dedup_system.h>
#include <test/log_assert.h>

namespace dedupv1 {

class DedupSystem;

class DedupSystemTest : public testing::TestWithParam<const char*>  {
    protected:
    USE_LOGGING_EXPECTATION();

    DedupSystem* system;
    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;

    virtual void SetUp();
    virtual void TearDown();
    public:
    static DedupSystem* CreateDefaultSystem(std::string config_options,
            dedupv1::InfoStore* info_store,
            dedupv1::base::Threadpool* tp,
            bool start = true,
            bool restart = false,
	        bool crashed = false,
            bool dirty = true,
            bool full_replay = false);
};

};

#endif /* DEDUP_SYSTEM_TEST_H_ */
