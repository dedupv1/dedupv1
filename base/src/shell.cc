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

#include <base/shell.h>

#include <base/memory.h>
#include <base/logging.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/select.h>

using std::string;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::ScopedArray;

LOGGER("Shell");

namespace dedupv1 {
namespace base {

Option<bytestring> RunUntilCompletion(const string& cmd) {
    // here we use the high-level system because in-process buffering helps here
    FILE *stream = popen(cmd.c_str(), "r");
    CHECK(stream, "Failed to run " << cmd);

    bytestring data;
    byte* buffer = new byte[1024];
    ScopedArray<byte> scoped_buffer(buffer);

    while (int b = (fread(buffer, 1, 1024, stream)) > 0) {
        data.append(buffer, b);
    }
    CHECK(pclose(stream) == 0, "Failed to close process stream: cmd " << cmd);
    return make_option(data);
}

}
}
