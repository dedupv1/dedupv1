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
#include "dedupv1d.h"
#include "dedupv1d_volume.h"
#include "scst_handle.h"
#include <core/dedup_volume.h>
#include <core/dedup_system.h>
#include <base/strutil.h>
#include "port_util.h"
#include "lock_monitor.h"
#include "monitor.h"
#include <base/http_client.h>
#include <base/logging.h>
#include <test_util/log_assert.h>
#include <json/json.h>
#include "monitor_test.h"

using std::string;
using dedupv1::base::HttpResult;

namespace dedupv1d {
namespace monitor {

INSTANTIATE_TEST_CASE_P(LockMonitor,
    MonitorAdapterTest,
    ::testing::Values("lock"));

}
}
