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
#include <re2/re2.h>

#include <test_util/log_assert.h>

using std::vector;

namespace dedupv1 {
namespace test {

LoggingExpectationSet::LoggingExpectationSet() {
    this->appender_ = new LoggingExpectationAppender();
    this->appender_->addRef();
    this->appender_->Attach(this);
    this->report_ = true;

    // default expectation
    LevelModifier m(WARN, FATAL);
    this->CreateLoggingExpectation(m).Never();
}

void LoggingExpectationSet::Reset() {
    this->report_ = true;

    vector<LoggingExpectation*>::iterator i = this->expectations_.begin();
    i++; // skip default expectation

    // erase everything else
    while (i != this->expectations_.end()) {
        delete *i;
        this->expectations_.erase(i);
    }

}

LoggingExpectationSet::~LoggingExpectationSet() {
    this->appender_->Release();
    this->appender_->releaseRef();
    this->appender_ = NULL;

    if (this->report_) {
        CheckAndReport();
    }

    vector<LoggingExpectation*>::iterator i = this->expectations_.begin();
    // erase everything
    for (; i != this->expectations_.end(); i++) {
        delete *i;
    }
}

void LoggingExpectationSet::SkipReporting() {
    this->report_ = false;
}

bool LoggingExpectationSet::CheckAndReport() {
    bool expection_failed = false;
    vector<LoggingExpectation*>::iterator i;
    for (i = this->expectations_.begin(); i != this->expectations_.end(); i++) {
        LoggingExpectation* le = *i;
        if (le) {
            if (!le->Check()) {
                std::string msg = le->Report();

                // copied out of gmock-internals-utils.cc
                testing::internal::AssertHelper(
                    testing::TestPartResult::kNonFatalFailure,
                    /*file*/ NULL,
                    /*line*/ -1,
                    msg.c_str()) = testing::Message();
                expection_failed = true;
            }
        }
    }
    return !expection_failed;
}

bool LoggingExpectationSet::Check() {
    bool expection_failed = false;
    vector<LoggingExpectation*>::iterator i;
    for (i = this->expectations_.begin(); i != this->expectations_.end(); i++) {
        LoggingExpectation* le = *i;
        if (le) {
            if (!le->Check()) {
                expection_failed = true;
            }
        }
    }
    return !expection_failed;
}

LoggingExpectation& LoggingExpectationSet::CreateLoggingExpectation(std::string regex) {
    LoggingExpectation* le = new LoggingExpectation(regex);
    this->expectations_.push_back(le);
    return *le;
}

LoggingExpectation& LoggingExpectationSet::CreateLoggingExpectation(const LevelModifier& lm) {
    LoggingExpectation* le = new LoggingExpectation(lm);
    this->expectations_.push_back(le);
    return *le;
}

bool LoggingExpectationSet::Process(const log4cxx::spi::LoggingEventPtr &event) {
    bool failed = false;
    bool matched = false;
    vector<LoggingExpectation*>::reverse_iterator i;
    for (i = this->expectations_.rbegin(); i != this->expectations_.rend(); i++) {

        if (i + 1 == this->expectations_.rend() && matched) {
            // aka it is the default one and if the event has matched before
            continue;
        }

        LoggingExpectation* le = *i;
        if (le) {
            bool local_matched = false;
            if (!le->Process(event, &local_matched)) {
                failed = true;
            }
            if (local_matched) {
                matched = true;
            }
        }
    }
    return !failed;
}

LoggingExpectation::LoggingExpectation(std::string regex) {
    this->regex_ = regex;
    this->logger_name_ = "";
    this->min_times_ = 0;
    this->max_times_ = 0;
    this->repeatetly_set_ = false;
    this->never_set_ = false;
    this->event_count_ = 0;
}

LoggingExpectation::LoggingExpectation(const LevelModifier& lm) {
    this->regex_ = "";
    this->level_ = lm;
    this->logger_name_ = "";
    this->min_times_ = 0;
    this->max_times_ = 0;
    this->repeatetly_set_ = false;
    this->never_set_ = false;
    this->event_count_ = 0;
}

LoggingExpectation::~LoggingExpectation() {
}

bool LoggingExpectation::Process(const log4cxx::spi::LoggingEventPtr &event, bool* matched) {
    if (matched) {
        *matched = false; // might be set to true later
    }

    // logger name set
    if (logger_name_.size() > 0) {
        if (event->getLoggerName() != logger_name_) {
            return true;
        }
    }
    // level set
    if (level_.is_level_set()) {
        if (!level_.Matches(event->getLevel())) {
            return true;
        }
    }

    if (regex_.size() > 0) {
        // regex is set
        re2::RE2 pattern(regex_);
        if (!re2::RE2::PartialMatch(event->getMessage(), pattern)) {
            return true;
        }
    }

    if (matched) {
        *matched = true;
    }
    this->event_count_++;
    return true;
}

std::string LoggingExpectation::DebugString(const log4cxx::spi::LoggingEventPtr &event) {
    return "[logger " + event->getLoggerName() +
           ", level " + event->getLevel()->toString() +
           ", message " + event->getMessage() + "]";
}

LoggingExpectation& LoggingExpectation::Times(int n) {
    EXPECT_FALSE(n == 0) << "Times(0) should not be called. Use .Never()";
    EXPECT_TRUE(n > 0) << "n should be positive in Times(n)";
    EXPECT_TRUE(max_times_ == 0) << ".Times(n) should be only called once per EXPECT_LOG";
    EXPECT_FALSE(repeatetly_set_) << ".Times(n) shouldn't be called after .Repeatedly()";
    EXPECT_FALSE(never_set_) << ".Times(n) shouldn't be called after .Never()";
    min_times_ = n;
    max_times_ = n;
    return *this;
}

LoggingExpectation& LoggingExpectation::Times(int min_n, int max_n) {
    EXPECT_FALSE(max_n == 0) << "Times(*,0) should not be called. Use .Never()";
    EXPECT_TRUE(min_n >= 0) << "n should be positive in Times(n,*)";
    EXPECT_TRUE(max_n > 0) << "n should be positive in Times(*,n)";
    EXPECT_FALSE(min_n == max_n) << "n should differ from m in Times(n,m). Use .Times(n)";
    EXPECT_TRUE(min_n < max_n) << "n should be less than m in Times(n,m)";
    EXPECT_TRUE(min_times_ == 0) << ".Times(n,m) should be only called once per EXPECT_LOG";
    EXPECT_FALSE(repeatetly_set_) << ".Times(n,m) shouldn't be called after .Repeatedly()";
    EXPECT_FALSE(never_set_) << ".Times(n) shouldn't be called after .Never()";
    min_times_ = min_n;
    max_times_ = max_n;
    return *this;
}

LoggingExpectation& LoggingExpectation::Repeatedly() {
    EXPECT_TRUE(max_times_ == 0) << ".Repeatedly() should be only called after .Times(n) or .Once()";
    EXPECT_FALSE(repeatetly_set_) << ".Repeatedly() shouldn't be called twice";
    EXPECT_FALSE(never_set_) << ".Repeatedly() shouldn't be called after .Never()";
    repeatetly_set_ = true;
    return *this;
}

LoggingExpectation& LoggingExpectation::Once() {
    EXPECT_TRUE(max_times_ == 0) << ".Once() should be only called after .Times(n) or .Once()";
    EXPECT_FALSE(repeatetly_set_) << ".Once() shouldn't be called after .Repeatedly()";
    EXPECT_FALSE(never_set_) << ".Once() shouldn't be called after .Never()";
    min_times_ = 1;
    max_times_ = 1;
    return *this;
}

LoggingExpectation& LoggingExpectation::Never() {
    EXPECT_TRUE(max_times_ == 0) << ".Never() should be only called after .Times(n) or .Once()";
    EXPECT_FALSE(repeatetly_set_) << ".Never() shouldn't be called after .Repeatedly()";
    EXPECT_FALSE(never_set_) << ".Never() shouldn't be called twice";
    never_set_ = true;
    return *this;
}

LoggingExpectation& LoggingExpectation::Logger(std::string log_name) {
    EXPECT_TRUE(this->logger_name_.size() == 0) << "Logger(name) shouldn't be called twice";
    this->logger_name_ = log_name;
    return *this;
}

LoggingExpectation& LoggingExpectation::Level(const LevelModifier& lm) {
    EXPECT_TRUE(lm.is_level_set()) << "Level must be set in .Level() call";
    EXPECT_FALSE(this->level_.is_level_set()) << "No two calls of .Level() in EXPECT_LOG";
    this->level_ = lm;
    return *this;
}

LoggingExpectation& LoggingExpectation::Matches(std::string regex) {
    EXPECT_TRUE(regex_.size() == 0) << ".Matches() should only be called once";
    regex_ = regex;
    return *this;
}

std::string LoggingExpectation::Report() {
    std::stringstream sstr;
    sstr << "Expectation " << DebugString() << " failed: occurred " <<
           this->event_count_ << " times";
    return sstr.str();
}

bool LoggingExpectation::Check() {
    EXPECT_TRUE(max_times_ > 0 || repeatetly_set_ || never_set_ ) << "Times(n), Once(), Never(), or Repeatedly() should be called for each EXPECT_LOG";
    if (repeatetly_set_) {
        return true;
    }
    if (never_set_) {
        return this->event_count_ == 0;
    }
    return min_times_ <= event_count_ && max_times_ >= event_count_;
}

std::string LoggingExpectation::DebugString() const {
    bool first = true;
    std::stringstream s;
    if (level_.is_level_set()) {
        s << "level " << level_.DebugString();
        first = false;
    }
    if (regex_.size() > 0) {
        if (!first) {
            s << ", ";
        }
        s << "message " << regex_;
    }
    // as regex or level must be set, we do not check for ", " anymore
    if (logger_name_.size() > 0) {
        s << ", logger " << logger_name_;
    }

    if (repeatetly_set_) {
        s << ", cardinality repeatedly";
    } else if (never_set_) {
        s << ", cardinality never";
    } else if (max_times_ > 0) {
        if (min_times_ == max_times_) {
            s << ", cardinality " << min_times_;
        } else {
            s << ", cardinality " << min_times_ <<
                 "-" << max_times_;
        }
    } else {
        // not set => append nothing
    }
    return s.str();
}

LevelModifier::LevelModifier() {
    this->level_set_ = false;
}

LevelModifier::LevelModifier(dedupv1::test::Level l) {
    this->min_level_ = l;
    this->max_level_ = l;
    this->level_set_ = true;
}

LevelModifier::LevelModifier(dedupv1::test::Level min, dedupv1::test::Level max) {
    this->min_level_ = min;
    this->max_level_ = max;
    this->level_set_ = true;
}

dedupv1::test::Level LevelModifier::min_level() const {
    return min_level_;
}

dedupv1::test::Level LevelModifier::max_level() const {
    return max_level_;
}

bool LevelModifier::is_level_set() const {
    return level_set_;
}

std::string LevelModifier::DebugString() const {
    if (!is_level_set()) {
        return "<not set>";
    }
    if (min_level_ == max_level_) {
        return DebugString(min_level_);
    }
    return DebugString(min_level_) + "-" + DebugString(max_level_);
}

std::string LevelModifier::DebugString(dedupv1::test::Level l) {
    if (l == TRACE) {
        return "TRACE";
    } else if (l == DEBUG) {
        return "DEBUG";
    } else if (l == INFO) {
        return "INFO";
    } else if (l == WARN) {
        return "WARN";
    } else if (l == ERROR) {
        return "ERROR";
    } else if (l == FATAL) {
        return "FATAL";
    }
    return "<Unknown level>";
}

bool LevelModifier::Matches(log4cxx::LevelPtr level_ptr) const {

    // get the level as our enum
    int level_int = level_ptr->toInt();
    Level level;
    if (level_int == log4cxx::Level::TRACE_INT) {
        level = TRACE;
    } else if (level_int == log4cxx::Level::DEBUG_INT) {
        level = DEBUG;
    } else if (level_int == log4cxx::Level::INFO_INT) {
        level = INFO;
    } else if (level_int == log4cxx::Level::WARN_INT) {
        level = WARN;
    } else if (level_int == log4cxx::Level::ERROR_INT) {
        level = ERROR;
    } else if (level_int == log4cxx::Level::FATAL_INT) {
        level = FATAL;
    } else {
        return false;
    }

    // check
    if (level < this->min_level_) {
        return false;
    }
    if (level > this->max_level_) {
        return false;
    }
    return true;
}

void LoggingExpectationAppender::append (const log4cxx::spi::LoggingEventPtr &event, log4cxx::helpers::Pool &p) {
    if (event->getLoggerName() == "LogAssert") {
        return;
    }
    if (!this->les_) {
        return;
    }
    this->les_->Process(event);
}

LoggingExpectationAppender::LoggingExpectationAppender() {
    this->les_ = NULL;
}

void LoggingExpectationAppender::Attach(LoggingExpectationSet* les) {
    this->les_ = les;
    log4cxx::Logger::getRootLogger()->addAppender(this);
}

void LoggingExpectationAppender::Release() {
    log4cxx::Logger::getRootLogger()->removeAppender(this);
}

LoggingExpectationAppender::~LoggingExpectationAppender() {
}

bool LoggingExpectationAppender::requiresLayout() const {
    return false;
}

void LoggingExpectationAppender::close() {
}

}
}
