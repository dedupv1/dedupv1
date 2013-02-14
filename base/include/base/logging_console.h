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

#ifndef LOGGING_CONSOLE_H_
#define LOGGING_CONSOLE_H_

#ifdef LOGGING_CONSOLE

#include <base/base.h>
#include <string>
#include <sstream>
#include <iostream>

#define ERROR(msg) { std::cerr << "ERROR " << msg << std::endl;}
#define ERROR_LOGGER(logger, msg) ERROR(msg)
#define IF_ERROR() if (true)

#define WARNING(msg) { std::cout << "WARNING " << msg << std::endl;}
#define WARNING_LOGGER(logger, msg) WARNING(msg)
#define IF_WARNING() if (true)

#define INFO(msg) { std::cout << msg << std::endl;}
#define INFO_LOGGER(logger, msg) INFO(msg)
#define IF_INFO() if (true)

#define DEBUG(msg)
#define DEBUG_LOGGER(logger, msg)
#define IF_DEBUG() if (false)

#define TRACE(msg)
#define TRACE_LOGGER(logger, msg)
#define IF_TRACE() if (false)

#define NESTED_LOG_CONTEXT(ndc_name)

#define LOGGER(cls)
#define MAKE_LOGGER(name, logger_name)
#define GET_LOGGER(logger_name) NULL
#define LOGGER_CLASS void*

#endif

#endif /* LOGGING_CONSOLE_H_ */
