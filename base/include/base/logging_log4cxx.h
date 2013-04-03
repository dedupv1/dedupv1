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

#ifndef LOGGING_LOG4CXX_H_
#define LOGGING_LOG4CXX_H_

#ifdef LOGGING_LOG4CXX

#include <log4cxx/logger.h>
#include <log4cxx/ndc.h>

/**
 * \ingroup logging
 * Logs an error message. The default logger of the file is used.
 */
#define ERROR(msg) { \
        if (logger && logger->isErrorEnabled()) {\
            ::log4cxx::helpers::MessageBuffer oss_; \
            ::log4cxx::spi::LocationInfo loc(file_basename(__FILE__),__func__,__LINE__); \
            logger->forcedLog(::log4cxx::Level::getError(), oss_.str(oss_ << msg), loc); \
            dedupv1::base::LoggingStatistics::GetInstance().error_count()++;}}

#define IF_ERROR() if (logger && logger->IsErrorEnabled())

#define ERROR_LOGGER(logger_instance, msg) { \
        if (logger_instance && logger_instance->isErrorEnabled()) {\
            ::log4cxx::helpers::MessageBuffer oss_; \
            ::log4cxx::spi::LocationInfo loc(file_basename(__FILE__),__func__,__LINE__); \
            logger_instance->forcedLog(::log4cxx::Level::getError(), oss_.str(oss_ << msg), loc); \
            dedupv1::base::LoggingStatistics::GetInstance().error_count()++;}}

#ifdef NDEBUG
/**
 * \ingroup logging
 * Logs a trace message if tracing is active for the file logger.
 * All trace message are no-ops if the system is compiled in the release mode
 */
#define TRACE(msg)
#define IF_TRACE() if (false)
#define TRACE_LOGGER(logger_instance, msg)
#elif defined COVERAGE_MODE
#define TRACE(msg)
#define IF_TRACE() if (false)
#define TRACE_LOGGER(logger_instance, msg)
#else
#define TRACE(msg) { \
        if (unlikely(logger && logger->isTraceEnabled())) {\
            ::log4cxx::helpers::MessageBuffer oss_; \
            ::log4cxx::spi::LocationInfo loc(file_basename(__FILE__),__func__,__LINE__); \
            logger->forcedLog(::log4cxx::Level::getTrace(), oss_.str(oss_ << msg), loc); }}

#define IF_TRACE() if (unlikely(logger && logger->isTraceEnabled()))

#define TRACE_LOGGER(logger_instance, msg) { \
        if (unlikely(logger_instance && logger_instance->isTraceEnabled())) {\
            ::log4cxx::helpers::MessageBuffer oss_; \
            ::log4cxx::spi::LocationInfo loc(file_basename(__FILE__),__func__,__LINE__); \
            logger_instance->forcedLog(::log4cxx::Level::getTrace(), oss_.str(oss_ << msg), loc); }}
#endif

/**
 * \ingroup logging
 * Logs a debug message if the debug level is active for the file logger.
 */
#define DEBUG(msg) { \
        if (unlikely(logger && logger->isDebugEnabled())) {\
            ::log4cxx::helpers::MessageBuffer oss_; \
            ::log4cxx::spi::LocationInfo loc(file_basename(__FILE__),__func__,__LINE__); \
            logger->forcedLog(::log4cxx::Level::getDebug(), oss_.str(oss_ << msg), loc); }}

#define IF_DEBUG() if (unlikely(logger && logger->isDebugEnabled()))

#define DEBUG_LOGGER(logger_instance, msg) { \
        if (unlikely(logger_instance && logger_instance->isDebugEnabled())) {\
            ::log4cxx::helpers::MessageBuffer oss_; \
            ::log4cxx::spi::LocationInfo loc(file_basename(__FILE__),__func__,__LINE__); \
            logger_instance->forcedLog(::log4cxx::Level::getDebug(), oss_.str(oss_ << msg), loc); }}

/**
 * \ingroup logging
 * Logs a information message if the info level is active for the file logger.
 */
#define INFO(msg) { \
        if (logger && logger->isInfoEnabled()) {\
            ::log4cxx::helpers::MessageBuffer oss_; \
            ::log4cxx::spi::LocationInfo loc(file_basename(__FILE__),__func__,__LINE__); \
            logger->forcedLog(::log4cxx::Level::getInfo(), oss_.str(oss_ << msg), loc); }}

#define IF_INFO() if (logger && logger->isInfoEnabled())

#define INFO_LOGGER(logger_instance, msg) { \
        if (logger_instance && logger_instance->isInfoEnabled()) {\
            ::log4cxx::helpers::MessageBuffer oss_; \
            ::log4cxx::spi::LocationInfo loc(file_basename(__FILE__),__func__,__LINE__); \
            logger_instance->forcedLog(::log4cxx::Level::getInfo(), oss_.str(oss_ << msg), loc); }}
/**
 * \ingroup logging
 * Logs a warning message if the warn level is active for the file logger.
 */
#define WARNING(msg) { \
        if (logger && logger->isWarnEnabled()) {\
            ::log4cxx::helpers::MessageBuffer oss_; \
            ::log4cxx::spi::LocationInfo loc(file_basename(__FILE__),__func__,__LINE__); \
            logger->forcedLog(::log4cxx::Level::getWarn(), oss_.str(oss_ << msg), loc); \
            dedupv1::base::LoggingStatistics::GetInstance().warn_count()++; }}

#define IF_WARNING() if (logger && logger->isWarnEnabled())

#define WARNING_LOGGER(logger_instance, msg) { \
        if (logger_instance && logger_instance->isWarnEnabled()) {\
            ::log4cxx::helpers::MessageBuffer oss_; \
            ::log4cxx::spi::LocationInfo loc(file_basename(__FILE__),__func__,__LINE__); \
            logger_instance->forcedLog(::log4cxx::Level::getWarn(), oss_.str(oss_ << msg), loc); \
            dedupv1::base::LoggingStatistics::GetInstance().warn_count()++; }}

/**
 * \ingroup logging
 * Creates a static file-level logger. Usually the name of the
 * main class in the file is used as logger name.
 */
#define LOGGER(cls) static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger(cls))
#define MAKE_LOGGER(name, logger_name) static log4cxx::LoggerPtr name(log4cxx::Logger::getLogger(logger_name))
#define GET_LOGGER(logger_name) log4cxx::Logger::getLogger(logger_name)
#define LOGGER_CLASS log4cxx::LoggerPtr

/**
 * \ingroup logging
 * Returns a logger with the give name
 */
#define GET_LOGGER(logger_name) log4cxx::Logger::getLogger(logger_name)

#define NESTED_LOG_CONTEXT(ndc_name) log4cxx::NDC ndc(ndc_name);

#endif

#endif /* LOGGING_LOG4CXX_H_ */
