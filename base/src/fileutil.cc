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

#include <base/base.h>
#include <base/fileutil.h>
#include <base/bitutil.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/protobuf_util.h>
#include <base/memory.h>

#include <stdio.h>
#include <stdlib.h>
#include "sys/stat.h"
#include "sys/time.h"
#include <sys/types.h>
#include <sys/file.h>
#include <dirent.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <libgen.h>

using std::vector;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::strutil::EndsWith;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::ScopedArray;
using std::string;
LOGGER("File");

namespace dedupv1 {
namespace base {

File::File(int fd, const std::string& path) {
    this->fd_ = fd;
    this->path_ = path;
}

Option<off_t> File::GetFileSize(const string& file) {
    struct stat stbuf;

    if (stat(file.c_str(), &stbuf)) {
        ERROR("Failed to stat file " << file << ", message " << strerror(errno));
        return false;
    }
    return make_option(stbuf.st_size);
}

bool File::Fallocate(off_t offset, off_t len) {
    Option<off_t> size = GetSize();
    CHECK(size.valid(), "Unable to get size of " << path());
    CHECK(offset <= size.value(), "File " << path() << " has size " << size.value() << ", so offset " << offset << " is not allowed");
    off_t skip = size.value() - offset;
    if (skip >= len) {
        return true;
    }
    len -= skip;
    offset = size.value();

#ifdef HAS_FALLOCATE
    bool write_without_falloc = false;
    if (!write_without_falloc) {
        while (true) {
            if (fallocate(this->fd_, 0, offset, len) == 0) {
                return true;
            } else {
                if (errno != EINTR) {
                    CHECK(errno == ENOTSUP, path() << ", message during falloc: " << strerror(errno) << " (" << errno << ")");
                    write_without_falloc = true;
                    break;
                }
                // On EINTR try again...
            }
        }
    }
#else
    bool write_without_falloc = true;
#endif
    if (write_without_falloc) {
        // we will do this in 4M pages, to be quite efficient
        size_t write_page_size = 4 * 1024 * 1024;
        byte* buffer = new byte[write_page_size];
        ScopedArray<byte> sBuffer(buffer);
        memset(buffer, 0, write_page_size);
        if (offset % write_page_size != 0) {
            off_t write_size = offset % write_page_size;
            if (write_size > len) {
                write_size = len;
            }
            int result = Write(offset, buffer, write_size);
            CHECK(result == write_size && result != File::kIOError,
                "Write to file " << path() << " failed at position " << offset);
            len -= write_size;
            offset += write_size;
        }
        while (len >= write_page_size) {
            int result = Write(offset, buffer, write_page_size);
            CHECK(result == write_page_size && result != File::kIOError,
                "Write to file " << path() << " failed at position " << offset);
            len -= write_page_size;
            offset += write_page_size;
        }
        if (len > 0) {
            int result = Write(offset, buffer, len);
            CHECK(result == len && result != File::kIOError,
                "Write to file " << path() << " failed at position " << offset);
        }
    }
    return true;
}

bool File::GetLine(int* offset, string* line, unsigned int max) {
    line->clear();
    unsigned int i = 0;
    while (i < max) {
        char c;
        if (this->Read(*offset, (void *) &c, 1) != 1) {
            return i > 0;
        }
        if (c == '\n') {
            (*offset)++;
            return true;
        }
        (*line) += c;
        (*offset)++;
        i++;
    }
    return true;
}

File* File::Open(const string& path, int flags, int rights) {
    int fd = open(path.c_str(), flags, rights);
    if (fd == -1) {
        if (errno != ENOENT) {
            ERROR("Failed to open " << path << ": " << strerror(errno));
        }
        return NULL;
    }
    TRACE("Open file " << path);
    return new File(fd, path);
}

File::~File() {
    if (fd_ > 0) {
        if (close(this->fd_) == -1) {
            ERROR(path() << ", message " << strerror(errno));
        }
    }
}

ssize_t File::Read(off_t offset, void* data, size_t size) {
    ssize_t bytes = 0;
    while (true) {
        bytes = pread(this->fd_, data, size, offset);
        if (bytes >= 0) {
            break;
        }
        // bytes < 0
        if (errno == EINTR) {
            continue; // operation has been interrupted, try again
        }
        ERROR(path() << ", message " << strerror(errno));
        return bytes;

    }
    return bytes;
}

ssize_t File::Read(void* data, size_t size) {
    ssize_t bytes = 0;
    while (true) {
        bytes = read(this->fd_, data, size);
        if (bytes >= 0) {
            break;
        }
        // bytes < 0
        if (errno == EINTR) {
            continue; // operation has been interrupted, try again
        }
        ERROR(path() << ", message " << strerror(errno));
        return bytes;

    }
    return bytes;
}

ssize_t File::Write(const void* data, size_t size) {
    ssize_t bytes = 0;
    while (true) {
        bytes = write(this->fd_, data, size);
        if (bytes >= 0) {
            break;
        }
        // bytes < 0
        if (errno == EINTR) {
            continue; // operation has been interrupted, try again
        }
        ERROR("Write failed: " << path() << ", size " << size << ": " << strerror(errno));
        return bytes;
    }
    return bytes;
}

ssize_t File::Write(off_t offset, const void* data, size_t size) {
    ssize_t bytes = 0;
    while (true) {
        bytes = pwrite(this->fd_, data, size, offset);
        if (bytes >= 0) {
            break;
        }
        // bytes < 0
        if (errno == EINTR) {
            continue; // operation has been interrupted, try again
        }
        ERROR("Write failed: " << path() << ", offset " << offset << ", size " << size << ": " << strerror(errno));
        return bytes;
    }
    return bytes;
}

Option<off_t> File::Seek(off_t offset, int origin) {
    off_t seek_offset = lseek(this->fd_, offset, origin);
    CHECK(seek_offset != -1, "Failed to seek:" <<
        "fd " << fd_ <<
        ", offset " << offset <<
        ", origin " << origin <<
        ", message " << strerror(errno));
    return make_option(seek_offset);
}

Option<off_t> File::GetSize() {
    struct stat s;
    int err = fstat(this->fd_, &s);
    CHECK(err != -1, path() << ", message " << strerror(errno));
    return make_option(s.st_size);
}

bool File::Truncate(const std::string& path, size_t new_size) {
    int err = truncate(path.c_str(), new_size);
    CHECK(err != -1, strerror(errno));
    return true;
}

bool File::Truncate(size_t new_size) {
    int err = ftruncate(this->fd_, new_size);
    CHECK(err != -1, strerror(errno));
    return true;
}

bool File::Sync() {
    CHECK(fsync(this->fd_) != -1, path() << ", message " << strerror(errno));
    return true;
}

ssize_t File::WriteSizedMessage(off_t offset, const ::google::protobuf::Message& message, size_t max_size,
                                bool checksum) {
    size_t value_size = message.ByteSize() + 32;
    byte value[value_size];

    Option<size_t> vs = SerializeSizedMessageCached(message, value, value_size, checksum);
    CHECK_RETURN(vs.valid(), -1, "Cannot serialize sized message");
    CHECK_RETURN(vs.value() <= max_size, -1, "Serialized message is the large: size " << value_size << ", max size " << max_size);

    if (Write(offset, value, vs.value()) != (ssize_t) vs.value()) {
        return -1;
    }
    return vs.value();
}

bool File::ReadSizedMessage(off_t offset, ::google::protobuf::Message* message, size_t max_size, bool checksum) {
    byte value[max_size];

    ssize_t r = Read(offset, value, max_size);
    CHECK(r >= 0, strerror(errno));

    // EOF
    if (unlikely(r == 0)) {
        return false;
    }

    CHECK(ParseSizedMessage(message, value, r, checksum).valid(), "Failed to parse sized message: " <<
        "path " << this->path_ <<
        ", offset " << offset <<
        ", max value size " << max_size <<
        ", value size " << r);
    return true;
}

File* File::FromFileDescriptor(int fd) {
    CHECK_RETURN(fd >= 0, NULL, "File descriptor not set");
    return new File(fd, "");
}

bool File::ListDirectory(const string& dir, vector<string>* files) {
    CHECK(files, "Files not set");

    files->clear();
    DIR *dp;
    struct dirent *dirp;
    CHECK((dp = opendir(dir.c_str())) != NULL, "Error(" << errno << ") opening " << dir);

    while ((dirp = readdir(dp)) != NULL) {
        files->push_back(string(dirp->d_name));
    }
    closedir(dp);
    return true;
}

bool File::Remove(const string& path) {
    int r = unlink(path.c_str());
    CHECK(r == 0, "Remove of " << path << " failed: " << strerror(errno));
    return true;
}

bool File::Stat(const string& path, struct stat* s) {
    CHECK(s, "Stat not set");
    int r = stat(path.c_str(), s);
    CHECK(r == 0, "Stat failed: " << strerror(errno));
    return true;
}

Option<bool> File::Exists(const string& path) {
    struct stat s;
    memset(&s, 0, sizeof(s));
    int r = stat(path.c_str(), &s);
    if (r == 0) {
        return make_option(true);
    } else if (errno == ENOENT) {
        return make_option(false);
    }
    ERROR("Failed to check for file existence: " << path << ", message " << strerror(errno));
    return false;
}

Option<string> File::Basename(const string& path) {
    char* s = strdup(path.c_str());
    char* p = basename(s);
    if (!p) {
        ERROR("Failed to get basename: " << path << ", message " << strerror(errno));
        free(s);
        return false;
    }
    string r(p);
    free(s);
    return make_option(r);
}

Option<string> File::Dirname(const string& path) {
    char* s = strdup(path.c_str());
    char* p = dirname(s);
    if (!p) {
        ERROR("Failed to get dirname: " << path << ", message " << strerror(errno));
        free(s);
        return false;
    }
    string r(p);
    free(s);
    return make_option(r);
}

bool File::Mkdir(const string& path, int mode) {
    if (mkdir(path.c_str(), mode) != 0) {
        ERROR("Failed to make directory: " << path << ", message " << strerror(errno));
        return false;
    }
    return true;
}

bool File::MakeParentDirectory(const string& path, int mode) {
    Option<string> parent_dir = Dirname(path);
    if (!parent_dir.valid()) {
        return false;
    }
    Option<bool> exists = Exists(parent_dir.value());
    if (!exists.valid()) {
        return false;
    }
    if (exists.value()) {
        Option<bool> is_dir = IsDirectory(parent_dir.value());
        if (!is_dir.valid()) {
            return false;
        }
        CHECK(is_dir.value(), "Parent path element is not a directory: " << path);
        return true;
    }
    // not exists

    if (!MakeParentDirectory(parent_dir.value(), mode)) {
        return false;
    }
    return Mkdir(parent_dir.value(), mode);
}

const string& File::path() const {
    return path_;
}

Option<bool> File::IsDirectory(const string& path) {
    struct stat s;
    memset(&s, 0, sizeof(s));
    int r = stat(path.c_str(), &s);
    if (r == 0) {
        return make_option(S_ISDIR(s.st_mode));

    }
    ERROR("Failed to check for file existence: " << path << "message " << strerror(errno));
    return false;
}

string File::Join(const string& a, const string& b) {
    if (StartsWith(b, "/")) {
        // is absolute path
        return b;
    }
    string path = a;
    if (!EndsWith(a, "/")) {
        path += "/";
    }
    return path + b;
}

bool File::Lock(bool exclusive) {
    if (flock(fd_, (exclusive ? LOCK_EX : LOCK_SH)) != 0) {
        ERROR("Failed to lock file: " << path_ << ", message " << strerror(errno));
        return false;
    }
    return true;
}

Option<bool> File::TryLock(bool exclusive) {
    int err = flock(fd_, (exclusive ? LOCK_EX : LOCK_SH) | LOCK_NB);
    if (err == 0) {
        return make_option(true);
    }
    if (errno == EWOULDBLOCK) {
        return make_option(false);
    }
    ERROR("Failed to lock file: " << path_ << ", message " << strerror(errno));
    return false;
}

bool File::Unlock() {
    if (flock(fd_, LOCK_UN) != 0) {
        ERROR("Failed to unlock file: " << path_ << ", message " << strerror(errno));
        return false;
    }
    return true;
}

Option<bytestring> File::ReadContents(const string& filename) {
    File* f = File::Open(filename, O_RDONLY, 0);
    CHECK(f, "Failed open file " << filename);

    bytestring bs;
    int b = 4096;
    byte* buffer = new byte[b];
    memset(buffer, 0, b);
    size_t offset = 0;
    ssize_t r = f->Read(offset, buffer, b);
    while (r > 0) {
        bs.append(buffer, r);
        offset += r;
        r = f->Read(offset, buffer, b);
    }
    delete[] buffer;
    buffer = NULL;
    delete f;
    f = NULL;
    CHECK(r >= 0, "Failed to read: filename " << filename << ", offset " << offset << ", message " << strerror(errno));
    return make_option(bs);
}

bool File::CopyFile(const std::string& src_name, const std::string& dest_name, int dest_mode, bool overwrite) {
    CHECK(src_name != dest_name, "Copy source equals destination");

    File* src = File::Open(src_name, O_RDONLY, 0);
    if (!src) {
        // no warning or error as open already printed a good error message
        return false;
    }

    int dest_flags = O_WRONLY | O_CREAT;
    if (!overwrite) {
        dest_flags |= O_EXCL;
    }
    File* dest = File::Open(dest_name, dest_flags, dest_mode);
    if (!dest) {
        delete src;
        return false;
    }

    bool failed = false;
    int b = 4096;
    byte* buffer = new byte[b];
    ssize_t r = src->Read(buffer, b);
    while (r > 0 && !failed) {
        // usually we only need a single write call, but there can be short writes. This inner loop handles them
        while (r > 0 && !failed) {
            ssize_t w = dest->Write(buffer, r);
            if (w <= 0) {
                ERROR("write error during copy: " <<
                    "source " << src_name <<
                    ", destination " << dest_name);
                failed = true;
                continue; // some write error, r is > 0 => leave the loops
            }

            r -= w;
        }
        r = src->Read(buffer, b);
    }
    if (r < 0) {
        ERROR("Read error during copy: " <<
            "source " << src_name <<
            ", destination " << dest_name);
        failed = true;
    }

    // clean up
    delete[] buffer;
    buffer = NULL;

    if (!dest->Sync()) {
        ERROR("Failed to sync dest");
        failed = true;
    }
    delete dest;
    delete src;
    return !failed;
}

}
}
