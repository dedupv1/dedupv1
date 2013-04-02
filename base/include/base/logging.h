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
/**
 * @file logging.h
 * Functions and defines for error and debug logging.
 */
#ifndef LOGGING_H__
#define LOGGING_H__

#include <string>
#include <tbb/atomic.h>
#include <base/base.h>
#include <base/config.h>

/**
 * \defgroup logging Logging
 *
 * The logging framework is used to collect
 * information about the errors and other events in the system. The goal is
 * to make the development and usage of the system easier by enabling a good
 * way to find errors and especially the causes.
 *
 * The logging system has three implementations:
 * - log4cxx system.
 * - syslog
 * - console
 *
 * The log4cxx is the most sophisticated system, but we had problems with
 * segfaults. Therefore log4cxx is only used for testing and debugging, and syslog
 * is used in production.
 *
 * The main macros ERROR, DEBUG, INFO, and WARNING are custom
 * wrapper around the standard log4cxx macros to avoid long file names.
 */

const char* file_basename(const char* file);

namespace dedupv1 {
namespace base {

/**
 * Class to collect statistics about the logging system.
 */
class LoggingStatistics {
        /**
         * Singleton instance
         */
        static LoggingStatistics instance_;

        /**
         * Number of error in the current process
         */
        tbb::atomic<uint64_t> error_count_;

        /**
         * Number of warnings in the current process
         */
        tbb::atomic<uint64_t> warn_count_;
    public:
        /**
         * Constructor
         * @return
         */
        LoggingStatistics();

        inline tbb::atomic<uint64_t>& error_count();

        inline tbb::atomic<uint64_t>& warn_count();

        std::string PrintStatistics();

        static LoggingStatistics& GetInstance();
};

tbb::atomic<uint64_t>& LoggingStatistics::error_count() {
    return error_count_;
}

tbb::atomic<uint64_t>& LoggingStatistics::warn_count() {
    return warn_count_;
}

}
}

#ifdef LOGGING_CONSOLE_FOR_NDAEMON
#define LOGGING_CONSOLE
#undef LOGGING_LOG4CXX
#undef LOGGING_SYSLOG
#endif

#ifdef LOGGING_LOG4CXX
#include <base/logging_log4cxx.h>
#endif
#ifdef LOGGING_SYSLOG
#include <base/logging_syslog.h>
#endif
#ifdef LOGGING_CONSOLE
#include <base/logging_console.h>
#endif

#ifndef ERROR
#error "No logging approach used"
#endif

/**
 * \ingroup logging
 * Checks is the 1. parameter is true. If this is not the case, an error message is logged
 * if the message in the 2. parameter and returns the method with false.
 */
#define CHECK(x, msg) {if (unlikely(!(x))) { ERROR(msg);return false;}}

#ifndef WITH_DCHECK
#define DCHECK(x, msg)
#else
#define DCHECK(x, msg) CHECK(x, msg)
#endif

/**
 * \ingroup logging
 * Checks is the 1. parameter is true. If this is not the case, an error message is logged
 * if the message in the 2. parameter and gotos a label error
 */
#define CHECK_GOTO(x, msg) { if (unlikely(!(x))) {ERROR(msg);goto error;}}

#ifndef WITH_DCHECK
#define DCHECK_GOTO(x, msg)
#else
#define DCHECK_GOTO(x, msg) CHECK_GOTO(x, msg)
#endif

/**
 * \ingroup logging
 * Checks is the 1. parameter is true. If this is not the case, an error message is logged
 * if the message in the 3. parameter and returns the method with 2. parameter
 */
#define CHECK_RETURN(x, e, msg) { if (unlikely(!(x))) { ERROR(msg);return e;}}

#ifndef WITH_DCHECK
#define DCHECK_RETURN(x, e, msg)
#else
#define DCHECK_RETURN(x, e, msg) CHECK_RETURN(x, e, msg)
#endif

/**
 * \ingroup logging
 */
#define CHECK_ERRNO(x, msg) CHECK(x != -1, msg << strerror(errno));

#ifndef WITH_DCHECK
#define DCHECK_ERRNO(x, msg)
#else
#define DCHECK_ERRNO(x, msg) CHECK_ERRNO(x, msg)
#endif

#endif  // LOGGING_H__
