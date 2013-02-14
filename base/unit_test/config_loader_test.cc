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

#include <string>

#include <base/config_loader.h>
#include <test/log_assert.h>
#include <base/callback.h>
#include <base/config.h>

using dedupv1::base::NewCallback;
using dedupv1::base::ConfigLoader;
using std::string;

class ConfigLoaderTest : public testing::Test {
    bool SetOption(const string& option_name, const string& option) {
        last_option = option;
        last_option_name = option_name;
        return true;
    }
protected:
    USE_LOGGING_EXPECTATION();

    ConfigLoader config_loader;
    string last_option_name;
    string last_option;
public:
    ConfigLoaderTest() : config_loader(NewCallback(this, &ConfigLoaderTest::SetOption)) {
    }
};

TEST_F(ConfigLoaderTest, Normal) {
    ASSERT_TRUE(config_loader.ProcessLine("hello=world", 0));
    ASSERT_EQ("hello", last_option_name);
    ASSERT_EQ("world", last_option);
}

TEST_F(ConfigLoaderTest, NormalComment) {
    ASSERT_TRUE(config_loader.ProcessLine("hello=world", 0));
    ASSERT_TRUE(config_loader.ProcessLine("#hello=world2", 1));
    ASSERT_EQ("hello", last_option_name);
    ASSERT_EQ("world", last_option);
}

TEST_F(ConfigLoaderTest, Strip) {
    ASSERT_TRUE(config_loader.ProcessLine("   hello  =  world  ", 0));
    ASSERT_EQ("hello", last_option_name);
    ASSERT_EQ("world", last_option);
}

TEST_F(ConfigLoaderTest, InlineCommand) {
    ASSERT_TRUE(config_loader.ProcessLine("hello=world   #World 2", 0));
    ASSERT_EQ("hello", last_option_name);
    ASSERT_EQ("world", last_option);

    ASSERT_TRUE(config_loader.ProcessLine("   #hello2=world2", 1));
    ASSERT_EQ("hello", last_option_name);
    ASSERT_EQ("world", last_option);

}
