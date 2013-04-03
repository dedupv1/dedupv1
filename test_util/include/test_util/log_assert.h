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
#ifndef LOG_ASSERT_H_
#define LOG_ASSERT_H_

#include <string>
#include <vector>
#include <log4cxx/logger.h>
#include <log4cxx/appenderskeleton.h>

#define USE_LOGGING_EXPECTATION() dedupv1::test::LoggingExpectationSet __log_expect__set__

#define EXPECT_LOGGING(init) __log_expect__set__.CreateLoggingExpectation(init)

#define EXPECT_LOGGING_RESET() __log_expect__set__.Reset()

namespace dedupv1 {
namespace test {

class LoggingExpectationSet;

/**
* Logging levels
*/
enum Level {
        TRACE,//!< TRACE
        DEBUG,//!< DEBUG
        INFO, //!< INFO
        WARN, //!< WARN
        ERROR, //!< ERROR
        FATAL //!< FATAL
};

class LevelModifier {
    private:
        dedupv1::test::Level min_level_;
        dedupv1::test::Level max_level_;
        bool level_set_;
    public:
        LevelModifier();
        LevelModifier(dedupv1::test::Level l); // implicit
        LevelModifier(dedupv1::test::Level min, dedupv1::test::Level max);

        dedupv1::test::Level min_level() const;
        dedupv1::test::Level max_level() const;
        bool is_level_set() const;

        bool Matches(log4cxx::LevelPtr level_ptr) const;

        std::string DebugString() const;

        static std::string DebugString(dedupv1::test::Level l);
};

/**
* Internal log4cxx appender class that observes
* all events and deletes them for processing to
* all registers expectations.
*/
class LoggingExpectationAppender : public log4cxx::AppenderSkeleton {
    private:
    /**
    * reference to the current log expectation set
    */
    LoggingExpectationSet* les_;
    protected:

    /**
    * implements the append method of the AppenderSkeleton.
    * Deletes all events to the expectation set for processing.
    * @param event
    * @param p
    */
    void append (const log4cxx::spi::LoggingEventPtr &event, log4cxx::helpers::Pool &p);

    public:
    /**
    * return false as the appender does use a layout
    * @return
    */
    bool requiresLayout() const;

    /**
    * empty implementation as this method is not used, but must be implemented
    * to satisfy the Appender base class.
    */
    void close();

    /**
    * Attaches the appender to a parent expectation set
    * @param les
    */
    void Attach(LoggingExpectationSet* les);

    /**
    * Releases the appender
    */
    void Release();

    /**
    * Constructor
    * @return
    */
    LoggingExpectationAppender();

    /**
    * Destructor
    * @return
    */
    ~LoggingExpectationAppender();
};

/**
* A single expectation about logging message during a
* unit test.
*
* Users should not create instances of the class on their own,
* but use the EXPECT_LOGGING() macro.
*/
class LoggingExpectation {
        friend class LoggingExpectationSet;

        /**
        * regular expression as filter to logging message
        */
        std::string regex_;

        /**
        *
        */
        LevelModifier level_;

        /**
        * name of a logger as filter of logging messages
        */
        std::string logger_name_;

        /**
        * number of times a matching logging event should occur at least.
        * Greater than 0 if configured
        */
        int min_times_;

        /**
        * number of times a matching logging event should occur at most.
        * Greater than 0 if configured
        */
        int max_times_;

        /**
        * true iff a unspecified number of events are allowed to occur
        * TODO (dmeister): Is there any value in this?
        */
        int repeatetly_set_;

        /**
        * true iff there should be no matching event
        */
        int never_set_;

        /**
        * current number a message that fulfills all
        * filter conditions is observed
        */
        int event_count_;

        /**
        * processes the logging event.
        *
        * @param event
        * @return
        */
        bool Process(const log4cxx::spi::LoggingEventPtr &event, bool* matched);
    public:
        /**
        * Constructor using a regular expression for the logging message
        * @param regex
        * @return
        */
        LoggingExpectation(std::string regex);

        /**
        * Constructor
        * @param lm
        * @return
        */
        LoggingExpectation(const LevelModifier& lm);

        /**
        * Destructor.
        * @return
        */
        ~LoggingExpectation();

        /**
        * Sets the number of times such a logging event should occur
        * @param n
        * @return
        */
        LoggingExpectation& Times(int n);

        LoggingExpectation& Times(int min_n, int max_n);

        /**
        * Specifies that there can be a any number of matching logging events.
        * @return
        */
        LoggingExpectation& Repeatedly();

        /**
        * Specifies that there should be one and only one matching logging event
        * @return
        */
        LoggingExpectation& Once();

        /**
        * Specifies that there should be no matching logging event
        * @return
        */
        LoggingExpectation& Never();

        /**
        * Specifies the name of the logger of matching events
        * @param log_name
        * @return
        */
        LoggingExpectation& Logger(std::string log_name);

        /**
        * Specifies the level of matching events.
        * @param lm
        * @return
        */
        LoggingExpectation& Level(const LevelModifier& lm);

        /**
        * Specifies a regular expression that the message of logging events
        * should match.
        *
        * @param regex
        * @return
        */
        LoggingExpectation& Matches(std::string regex);

        /**
        * returns true iff the expectation is fulfilled.
        * @return
        */
        bool Check();

        /**
        * returns a string that describes the expectation and its state.
        * This method is used to report the (failed) expectation to gtest.
        * @return
        */
        std::string Report();

        /**
        * returns a developer-readable representation of the expectation
        * @return
        */
        std::string DebugString() const;

        static std::string DebugString(const log4cxx::spi::LoggingEventPtr &event);
};

/**
* Set of expectations in a unit test.
* Even the expectation set leaves the scope, all
* unsatisfied expectations are reported to gtest.
*
* Users should not create instances of the class
* directly. They should use the
* USE_LOGGING_EXPECTATION macro in their testing::Test
* subclass.
*
* A expectation set has a default expectation that
* there should be no logging events with an ERROR level.
*/
class LoggingExpectationSet {
        friend class LoggingExpectationAppender;

        /**
        * vector of all logging expectations
        */
        std::vector<LoggingExpectation*> expectations_;

        /**
        * pointer to the related appender
        */
        LoggingExpectationAppender* appender_;

        /**
        * flag if failed expectations should be reported to gtest.
        */
        bool report_;

        /**
        * checks the expectations and reports the results
        * @return
        */
        bool CheckAndReport();

        /**
        * processes and logging event
        * @param event
        * @return
        */
        bool Process(const log4cxx::spi::LoggingEventPtr &event);
    public:
        /**
        * Constructor
        * @return
        */
        LoggingExpectationSet();

        /**
        * Destructor.
        * The expectations are checked during a destructor call
        * @return
        */
        ~LoggingExpectationSet();

        /**
        * creates and registers a new logging expectations and returns it
        * for further configuration.
        *
        * @param regex
        * @return
        */
        LoggingExpectation& CreateLoggingExpectation(std::string regex);

        /**
        * creates and registers a new logging expectations and returns it
        * for further configuration.
        *
        * @param lm
        * @return
        */
        LoggingExpectation& CreateLoggingExpectation(const LevelModifier& lm);

        /**
        * returns true iff all expectations are fulfilled.
        * @return
        */
        bool Check();

        void Reset();

        /**
        * Deactivates the reporting to gtst.
        */
        void SkipReporting();
};

}
}

#endif /* LOG_ASSERT_H_ */
