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
#include "dedupv1d_session.h"
#include "scst_handle.h"
#include "command_handler.h"

#include <core/dedup_volume.h>
#include <base/strutil.h>
#include <core/dedup_system.h>
#include <test/log_assert.h>

namespace dedupv1d {

class Dedupv1dSessionTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Dedupv1dSession* sess;

    virtual void SetUp() {
        sess = NULL;
    }

    virtual void TearDown() {
        if (sess) {
            delete sess;
            sess = NULL;
        }
    }
};

TEST_F(Dedupv1dSessionTest, Create) {
    sess = new Dedupv1dSession(123, "dedupv1", "iqn.bla", 0);
    ASSERT_TRUE(sess);

    ASSERT_EQ(sess->session_id(), 123);
    ASSERT_EQ(sess->lun(), 0);
    ASSERT_EQ(sess->initiator_name(), "iqn.bla");
    ASSERT_EQ(sess->target_name(), "dedupv1");
}

}
