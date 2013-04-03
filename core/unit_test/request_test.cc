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
#include <core/dedup.h>
#include <test_util/log_assert.h>
#include <core/request.h>

using dedupv1::Request;
using std::string;

namespace dedupv1 {

class RequestTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
    
    Request* request_;
    byte* buffer_;
    
    virtual void SetUp() {
        request_ = NULL;
        buffer_ = new byte[64 * 1024];
        ASSERT_TRUE(buffer_);
    }

    virtual void TearDown() {
        if (request_ != NULL) {
            delete request_;
        }
        if (buffer_) {
            delete[] buffer_;
        }
    }
};


TEST_F(RequestTest, Init) {
    request_ = new Request(REQUEST_READ, 0, 0, 64 * 1024, buffer_, 64 * 1024);
    ASSERT_TRUE(request_);
}

TEST_F(RequestTest, DebugString) {
    request_ = new Request(REQUEST_READ, 0, 0, 64 * 1024, buffer_, 64 * 1024);
    ASSERT_TRUE(request_);
    string ds = request_->DebugString();
    ASSERT_GT(ds.size(), 0);
}

TEST_F(RequestTest, IsValid) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(2);
        
    request_ = new Request(REQUEST_READ, 0, 0, 64 * 1024, buffer_, 64 * 1024);
    ASSERT_TRUE(request_);
    ASSERT_TRUE(request_->IsValid());
    delete request_;
    request_ = NULL;
    
    // Illegal offset
    request_ = new Request(REQUEST_READ, 0, 17, 1024, buffer_, 64 * 1024);
    ASSERT_TRUE(request_);
    ASSERT_FALSE(request_->IsValid());
    delete request_;
    request_ = NULL;
    
    // Illegal size
    request_ = new Request(REQUEST_READ, 0, 0, 4711, buffer_, 64 * 1024);
    ASSERT_TRUE(request_);
    ASSERT_FALSE(request_->IsValid());
    delete request_;
    request_ = NULL;
}

}

