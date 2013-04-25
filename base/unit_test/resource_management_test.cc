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
#include <base/resource_management.h>
#include <test_util/log_assert.h>

namespace dedupv1 {
namespace base {

class BufferResourceType : public ResourceType<byte> {
    size_t size;
public:
    BufferResourceType(size_t s) {
        this->size = s;
    }

    virtual byte* Create();
    virtual void Reinit(byte* resource);
    virtual void Close(byte* resource);
};

class ResourceManagementTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    ResourceManagement<byte>* rmc;

    virtual void SetUp() {
        rmc = new ResourceManagement<byte>();
        rmc->Init("test", 4, new BufferResourceType(32));
        ASSERT_TRUE(rmc);
    }

    virtual void TearDown() {
        if (rmc) {
            delete rmc;
            rmc = NULL;
        }
    }
};

TEST_F(ResourceManagementTest, Start) {

}

TEST_F(ResourceManagementTest, Cycle) {
    byte* s = rmc->Acquire();
    ASSERT_TRUE(s);

    ASSERT_TRUE(rmc->Release(s));
}

TEST_F(ResourceManagementTest, DoubleCycle) {
    byte* s1 = rmc->Acquire();
    ASSERT_TRUE(s1);
    ASSERT_TRUE(rmc->Release(s1));

    byte* s2 = rmc->Acquire();
    ASSERT_TRUE(s2);
    ASSERT_TRUE(rmc->Release(s2));
}

TEST_F(ResourceManagementTest, DoubleInterleaved) {
    byte* s1 = rmc->Acquire();
    ASSERT_TRUE(s1);
    byte* s2 = rmc->Acquire();
    ASSERT_TRUE(s2);

    ASSERT_TRUE(rmc->Release(s2));
    ASSERT_TRUE(rmc->Release(s1));
}

TEST_F(ResourceManagementTest, DoubleReveresed) {
    byte* s1 = rmc->Acquire();
    ASSERT_TRUE(s1);
    byte* s2 = rmc->Acquire();
    ASSERT_TRUE(s2);

    ASSERT_TRUE(rmc->Release(s1));
    ASSERT_TRUE(rmc->Release(s2));
}

TEST_F(ResourceManagementTest, Full) {
    EXPECT_LOGGING(dedupv1::test::WARN).Once();

    byte* s1 = rmc->Acquire();
    ASSERT_TRUE(s1);
    byte* s2 = rmc->Acquire();
    ASSERT_TRUE(s2);
    byte* s3 = rmc->Acquire();
    ASSERT_TRUE(s3);
    byte* s4 = rmc->Acquire();
    ASSERT_TRUE(s4);

    EXPECT_FALSE(rmc->Acquire()) << "Acquire should fail because all resources are used";

    EXPECT_TRUE(rmc->Release(s1));
    EXPECT_TRUE(rmc->Release(s2));
    EXPECT_TRUE(rmc->Release(s3));
    EXPECT_TRUE(rmc->Release(s4));
}

byte* BufferResourceType::Create() {
    byte* buffer = new byte[size];
    return buffer;
}

void BufferResourceType::Close(byte* buffer) {
    delete[] buffer;
}

void BufferResourceType::Reinit(byte* resource) {
    memset(resource, 0, this->size);
}

}
}
