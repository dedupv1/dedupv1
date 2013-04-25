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
 * @file fileutil.h
 * Utility classes and methods for file handling
 */

#ifndef FILEUTIL_H__
#define FILEUTIL_H__

#include <base/base.h>
#include <base/option.h>

#include <stdio.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <google/protobuf/message.h>

#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif

namespace dedupv1 {
namespace base {

/**
 * A file.  What should I say more?
 *
 * I avoid using the stream library, because I lose to much controls about
 * the semantics. The file implementation of read and write are more or less directly
 * mapped to the syscalls.
 */
class File {
    public:
        static const ssize_t kIOError = -1;
        static const int kDefaultFileMode = S_IWUSR | S_IRUSR | S_IRGRP;
    private:
        DISALLOW_COPY_AND_ASSIGN(File);

        /**
         * file descriptor. The fd of a File instance should always be valid.
         */
        int fd_;

        /**
         * path with that the file has been opened.
         * The path is empty if the File object is created from a fd directly
         */
        std::string path_;

        /**
         * Internal constructor.
         * Create instances using the static Open call.
         *
         * @param fd file descriptor
         * @param path path of the file that is created or opened
         * @return
         */
        explicit File(int fd, const std::string& path);

    public:
        /**
         * Usually the Close method is the preferred way to close a file
         * as the destructor has no way to communicate errors, but it with the exception
         * of the missing error handling also a valid way. At least, the file is closed if
         * it is still open.
         */
        ~File();

        /**
         * Opens (or creates) a new file.
         *
         * @param path
         * @param flags
         * @param rights
         * @return returns a new pointer to a File object if the open was successful, otherwise a NULL pointer
         * is returned
         */
        static File* Open(const std::string& path, int flags, int rights);

        /**
         * Returns the size of the file at the given path.
         *
         * @param path
         * @return file size or an empty option in case of an error.
         */
        static dedupv1::base::Option<off_t> GetFileSize(const std::string& path);

        /**
         * Removes the file at the given path.
         * @param path
         * @return true iff ok, otherwise an error has occurred
         */
        static bool Remove(const std::string& path);

        /**
         * Gathers the stat structure for the file at the given path.
         * @param path
         * @param stat
         * @return true iff ok, otherwise an error has occurred
         */
        static bool Stat(const std::string& path, struct stat* stat);

        /**
         * Checks if a file with the given path exists.
         * @param path
         * @return
         */
        static dedupv1::base::Option<bool> Exists(const std::string& path);

        /**
         * Allocate Space on disk for the file.
         *
         * Using this command it is possible to allocate space on disk.
         * It uses fallocate to get the space, if this fails, it appends
         * zeros to file to ensure the length. fallocate is supported on
         * ext4, but not on ext3. On filesystems not supporting fallocate
         * this call takes the time a normal write write would take to
         * append the data.
         *
         * It is possible to use an offset and to give a length for the
         * file from the offset. So the file size will be (offset + len).
         * After successful call of Fallocate calls to GetSize will
         * deliver this size, call to "ls" or "du" on the filesystem will
         * also.
         *
         * The offset must be in the range of the file (so offset must be
         * smaller or equal GetSize()). Therefore it is not possible to
         * allocate memory at any place in the file. If offset is smaller
         * then the size, then Fallocate will NOT overwrite
         * any data, it will just skip the part to end of file and allocate
         * from there. The most common use case for this method will be to
         * use as offset 0 (zero) and as len the wished file length.
         *
         * If offset + leng is smaller then the file size, Fallocate does
         * nothing, it just returns true. It is not possible tu shrink a file
         * using fallocate. If you want to do so, use Truncate().
         *
         * @param offset the offset from where to allocate the storage (most
         *               probably 0)
         * @param len The length to allocate (if offset is 0 the wished file
         *            size)
         * @return true if space is reserved, false on error.
         */
        bool Fallocate(off_t offset, off_t len);

        /**
         * Gets a new line from a file beginning at the given offset.
         *
         * @param offset as input parameter, it is a pointer to the
         * position in the file at which the search for a line starts.
         * When the search is successful, the pointer contains the position
         * where the search ended.
         *
         * @param offset
         * @param line
         * @param max
         * @return true iff ok, otherwise an error has occurred
         */
        bool GetLine(int* offset, std::string* line, unsigned int max);

        /**
         * Reads data at the given offset.
         *
         * The call is interrupted safe in the sense that it is retried, when EINTR is returned.
         *
         * @param offset
         * @param data
         * @param size
         * @return
         */
        ssize_t Read(off_t offset, void* data, size_t size);

        /**
         * Reads data at the current file pointer.
         * This method should not be used in concurrent situations.
         *
         * The call is interrupted safe in the sense that it is retried, when EINTR is returned.
         *
         * @param data
         * @param size
         * @return
         */
        ssize_t Read(void* data, size_t size);

        /**
         *
         * The call is interrupted safe in the sense that it is retried, when EINTR is returned.
         *
         * @param offset
         * @param message
         * @param max_size
         * @param checksum iff true, the checksum of the read message is checked.
         *
         * @return Returns false in case of an error, returns true if the offset has been at or after the end
         * of the file, returns the number
         * of bytes read.
         */
        bool ReadSizedMessage(off_t offset, ::google::protobuf::Message* message, size_t max_size, bool checksum);

        /**
         * Writes data at the given offset.
         *
         * The call is interrupted safe in the sense that it is retried, when EINTR is returned.
         *
         * @param offset
         * @param data
         * @param size
         * @return
         */
        ssize_t Write(off_t offset, const void* data, size_t size);

        /**
         * Writes data at the current file pointer.
         * This method should not be used in concurrent situations.
         * @param data
         * @param size
         * @return
         */
        ssize_t Write(const void* data, size_t size);

        /**
         *
         * The call is interrupted safe in the sense that it is retried, when EINTR is returned.
         *
         * @param offset Offset the write a sized message at
         * @param message message to write
         * @param max_size maximal allowed message size
         * @param checksum iff true, a message checksum should be calculated
         * and stored besides the message.
         *
         * @return -1 = ERROR
         */
        ssize_t WriteSizedMessage(off_t offset, const ::google::protobuf::Message& message, size_t max_size,
                bool checksum);

        /**
         * Gets the file size of the open file.
         *
         * @return
         */
        dedupv1::base::Option<off_t> GetSize();

        /**
         * Seeks the file pointer inside a file
         * @param offset
         * @param origin
         * @return
         */
        dedupv1::base::Option<off_t> Seek(off_t offset, int origin);

        /**
         * Truncates a file.
         *
         * @param new_size
         * @return true iff ok, otherwise an error has occurred
         */
        bool Truncate(size_t new_size);

        /**
         * Truncate the file given by the path
         */
        static bool Truncate(const std::string& path, size_t new_size);

        /**
         * Syncs the data and the metadata of a file
         * @return true iff ok, otherwise an error has occurred
         */
        bool Sync();

        /**
         * returns the path.
         * @return
         */
        const std::string& path() const;

        /**
         * Returns the file descriptor of the file
         * @return
         */
        inline int fd() const;

        /**
         * Create a file from a pre-existing file descriptor. The ownership of the
         * fd is transfered to the file. The client should not use the fd directly after
         * creating a File instance with the fd.
         *
         * @param fd
         * @return
         */
        static File* FromFileDescriptor(int fd);

        /**
         * Lists the files in a given directory.
         *
         * @param dir
         * @param files
         * @return true iff ok, otherwise an error has occurred
         */
        static bool ListDirectory(const std::string& dir, std::vector<std::string>* files);

        /**
         * Checks if the given path is a directory
         * @param path pathname to check
         * @return
         */
        static dedupv1::base::Option<bool> IsDirectory(const std::string& path);

        /**
         * Joins the two path together.
         * The method is inspired by os.path.join in python.
         *
         * @param a
         * @param b
         * @return
         */
        static std::string Join(const std::string& a, const std::string& b);

        /**
         * Returns the basename of the given path.
         *
         * The call is not thread-safe, thanks to the implementation of glibc under
         * Linux.
         *
         * @param path
         * @return
         */
        static dedupv1::base::Option<std::string> Basename(const std::string& path);

        /**
         * Returns the dirname of the given path.
         *
         * The call is not thread-safe, thanks to the implementation of glibc under
         * Linux.
         *
         * @param path
         * @return
         */
        static dedupv1::base::Option<std::string> Dirname(const std::string& path);

        /**
         * Creates a new directory with the given path and the given mode.
         * @param path
         * @param mode
         * @return true iff ok, otherwise an error has occurred
         */
        static bool Mkdir(const std::string& path, int mode);

        /**
         * Creates all parent directories of the given path if necessary.
         * If new directories have to be created, the given mode is used.
         *
         * The call is not thread-safe, thanks to the implementation of glibc under
         * Linux.
         *
         * @param path path for which all parent directories should be created
         * @param mode mode to use for new directories.
         * @return
         */
        static bool MakeParentDirectory(const std::string& path, int mode);

        /**
         * Locks the file using the flock syscall.
         *
         * @param exclusive iff true, an exclusive lock is acquired. Otherwise
         * a shared lock is used
         *
         * The method might block if the file is a locked
         * @return true iff ok, otherwise an error has occurred
         */
        bool Lock(bool exclusive);

        /**
         * Tries to lock the file using the flock syscall.
         * If the lock is not free, this method is not blocking
         *
         * @param exclusive iff true, an exclusive lock is acquired. Otherwise
         * a shared lock is used
         */
        dedupv1::base::Option<bool> TryLock(bool exclusive);

        /**
         * Unlocks a file
         * @return true iff ok, otherwise an error has occurred
         */
        bool Unlock();

        /**
         * Reads the complete contents of the file and returns it as bytestring
         *
         * @param filename filename of the file to read
         */
        static dedupv1::base::Option<bytestring> ReadContents(const std::string& filename);

        /**
         * Copies the contents of a file from src to dest. If dests already exists, it is only overwritten, when
         * overwrite is true.
         *
         * @param src filename of the source
         * @param dest filename of the destination
         * @param dest_mode file mode of the destination file. If not set the filemode kDefaultFileMode is used. The
         * file mode is only used if the destination file doesn't existed before.
         *
         * @return true iff the copy operation was successful, in case on an error, false is returned. If the copy operations
         * fails the state of the destination file is not specified: It might be that the file is missing, or that the file contains parts
         * of the src file.
         */
        static bool CopyFile(const std::string& src, const std::string& dest, int dest_mode = kDefaultFileMode,
                bool overwrite = true);

};

int File::fd() const {
    return fd_;
}

}
}

#endif  // FILEUTIL_H__
