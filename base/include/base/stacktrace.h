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

#ifndef STACKTRACE_H__
#define STACKTRACE_H__

#include <base/base.h>

// only on Linux
#ifndef __APPLE__

namespace dedupv1 {
namespace base {

/**
 * Registers a single handler for SIGSEGV that prints a stack trace and register informations into the log.
 *
 * We have not found the stacktrace to be really helpful as it does only contains the trace for a single thread
 * of execution. However, it makes pretty clear in the log that the system crashed due to a segfault and that is never
 * a good thing.
 *
 * @return true iff ok, otherwise an error has occurred
 */
bool setup_sigsegv();

}
}

#endif

#endif  // STACKTRACE_H__
