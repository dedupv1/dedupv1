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

#include <base/startup.h>
#include <unistd.h>
#include <sys/stat.h>
#include <grp.h>
#include <base/logging.h>

using dedupv1::base::Option;
using dedupv1::base::make_option;
using std::string;

LOGGER("Startup");

namespace dedupv1 {

FileMode::FileMode(int gid, bool dir, int mode) {
    gid_ = gid;
    if (mode == 0) {
        if (!dir) {
            mode_ = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;
        } else {
            mode_ = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IXUSR | S_IXGRP;
        }
    } else {
        mode_ = mode;
    }
}

FileMode::FileMode(bool dir) {
    gid_ = -1;

    if (!dir) {
        mode_ = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;
    } else {
        mode_ = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IXUSR | S_IXGRP;
    }
}

FileMode FileMode::Create(int gid, bool is_dir, int mode) {
    return FileMode(gid, is_dir, mode);
}

Option<FileMode> FileMode::Create(const std::string& group_name, bool is_dir, int mode) {
    struct group* gr = NULL;
    struct group grx;
    size_t grpbuflen = sysconf(_SC_GETGR_R_SIZE_MAX);

    char* grpbuf = new char[grpbuflen];
    int err = getgrnam_r(group_name.c_str(), &grx, grpbuf,
            grpbuflen, &gr);
    if (!(err == 0 && gr != NULL)) {
        ERROR("Failed to lookup group: " << group_name);
        delete[] grpbuf;
        return false;
    }
    int gid = gr->gr_gid;
    delete[] grpbuf;
    return make_option(FileMode(gid, is_dir, mode));
}

StopContext::StopContext() {
    mode_ = FAST;
}

StopContext::StopContext(enum shutdown_mode mode) {
    mode_ = mode;
}

StopContext StopContext::FastStopContext() {
    return StopContext(FAST);
}

StopContext StopContext::WritebackStopContext() {
    return StopContext(WRITEBACK);
}

string StartContext::DebugString() const {
    string suffix = "";
    if (create()) {
        suffix = "system create";
    }
    if (dirty()) {
        if (suffix.empty()) {
            suffix = "system dirty";
        } else {
            suffix += ", system dirty";
        }
    }
    if (force()) {
        if (suffix.empty()) {
            suffix = "force";
        } else {
            suffix += ", force";
        }
    }
    if (has_crashed()) {
        if (suffix.empty()) {
            suffix = "crashed";
        } else {
            suffix += ", crashed";
        }
    }
    return suffix;
}

StartContext::StartContext(enum create_mode create,
        enum dirty_mode dirty,
        enum force_mode force,
        bool readonly) : file_mode_(false), dir_mode_(true) {
    create_ = create;
    dirty_ = dirty;
    readonly_ = readonly;
    force_ = force;
    crashed_ = false;
}

}

