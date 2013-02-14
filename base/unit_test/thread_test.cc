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

#include <base/thread.h>
#include <base/strutil.h>
#include <test/log_assert.h>

#include <string>

using std::string;
using dedupv1::base::strutil::ToString;

namespace dedupv1 {
namespace base {

string thread_func();
int* thread_func2();

string thread_func() {
    sleep(2);
    return "Hello World";
}

int* thread_func2() {
    sleep(2);
    int* i = new int;
    *i = 1;
    return i;
}

class ThreadTestRunnable {
    int i;
public:
    ThreadTestRunnable(int i) {
        this->i = i;
    }
    string Print() {
        return ToString(this->i);
    }
};

class ThreadTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();
};

TEST_F(ThreadTest, FunctionRunnable) {
    Runnable<string>* r = NewRunnable(thread_func);
    Thread<string> t(r, "FunctionRunnable thread");

    ASSERT_TRUE(t.Start());
    string s;
    ASSERT_TRUE(t.Join(&s));
    ASSERT_EQ(s, "Hello World");
}

TEST_F(ThreadTest, ClassRunnableNoStart) {
    ThreadTestRunnable ttr(10);
    Runnable<string>* r = NewRunnable(&ttr, &ThreadTestRunnable::Print);
    Thread<string> t(r, "ClassRunnable thread");
}

TEST_F(ThreadTest, ClassRunnable) {
    ThreadTestRunnable ttr(10);
    Runnable<string>* r = NewRunnable(&ttr, &ThreadTestRunnable::Print);
    Thread<string> t(r, "ClassRunnable thread");

    ASSERT_TRUE(t.Start());
    string s;
    ASSERT_TRUE(t.Join(&s));
    ASSERT_EQ(s, "10");
}

TEST_F(ThreadTest, DoubleJoin) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ThreadTestRunnable ttr(10);
    Runnable<string>* r = NewRunnable(&ttr, &ThreadTestRunnable::Print);
    Thread<string> t(r, "ClassRunnable thread");

    ASSERT_TRUE(t.Start());
    string s;
    ASSERT_TRUE(t.Join(&s));
    ASSERT_EQ(s, "10");

    ASSERT_FALSE(t.Join(NULL));
}

TEST_F(ThreadTest, HighPriority) {
    ThreadTestRunnable ttr(10);
    Runnable<string>* r = NewRunnable(&ttr, &ThreadTestRunnable::Print);
    Thread<string> t(r, "ClassRunnable thread");
    ASSERT_TRUE(t.SetPriority(5));

    ASSERT_TRUE(t.Start());
    string s;
    ASSERT_TRUE(t.Join(&s));
    ASSERT_EQ(s, "10");
}

TEST_F(ThreadTest, WithoutStart) {
    Thread<string> t(NewRunnable(thread_func), "WithoutStart thread");
}

TEST_F(ThreadTest, IsStarted) {
    Thread<string> t(NewRunnable(thread_func), "IsStarted thread");
    ASSERT_FALSE(t.IsStarted());
    ASSERT_TRUE(t.Start());
    ASSERT_TRUE(t.IsStarted());
}

TEST_F(ThreadTest, DoubleStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    Thread<string> t(NewRunnable(thread_func), "DoubleStart thread");
    ASSERT_TRUE(t.Start());
    ASSERT_FALSE(t.Start()) << "The second Start call should fail";
}

TEST_F(ThreadTest, JoinBeforeStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    Thread<string> t(NewRunnable(thread_func), "JoinBeforeStart thread");
    ASSERT_FALSE(t.Join(NULL)) << "A join should fail if the thread is not started yet";
}

TEST_F(ThreadTest, Yield) {
    ASSERT_TRUE(ThreadUtil::Yield());
}

TEST_F(ThreadTest, PointerFunctionRunnable) {
    Thread<int*> t(NewRunnable(thread_func2), "PointerFunctionRunnable thread");

    ASSERT_TRUE(t.Start());
    int* i = NULL;
    ASSERT_TRUE(t.Join(&i));
    ASSERT_TRUE(i);
    ASSERT_EQ(*i, 1);
    delete i;
}

}
}
