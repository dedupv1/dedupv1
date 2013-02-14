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
#include <base/fileutil.h>
#include <base/logging.h>
#include <test/log_assert.h>

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <stdlib.h>

using std::vector;
using std::string;
using dedupv1::base::File;
using dedupv1::base::Option;
LOGGER("FileUtilTest");

class FileUtilTest: public testing::Test {
    protected:
        USE_LOGGING_EXPECTATION();

        File* file;

        virtual void SetUp() {
            this->file = NULL;
        }

        virtual void TearDown() {
            if (file) {
                ASSERT_TRUE(file->Close());
            }
        }
    };

TEST_F(FileUtilTest, Open)
{
    file = File::Open("data/line-file", O_RDWR, S_IRUSR);
    ASSERT_TRUE(file);
}

TEST_F(FileUtilTest, OpenFail)
{
    file = File::Open("data/file-not-existing", O_RDWR, S_IRUSR);
    ASSERT_FALSE(file);
}

TEST_F(FileUtilTest, Fallocate_allocate)
{
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    std::string path = "work/falloc-file";
    Option<off_t> size;
    Option<bool> exists = File::Exists(path);
    ASSERT_TRUE(exists.valid());
    if (exists.value()) {
        ASSERT_TRUE(File::Remove(path));
    }

    file = File::Open(path, O_RDWR | O_CREAT | O_EXCL | O_LARGEFILE, 0777);
    ASSERT_TRUE(file);

    ASSERT_TRUE(file->Fallocate(0, 1024));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(1024, size.value());
    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(1024, size.value());

    // And also after close and open
    ASSERT_TRUE(file->Close());
    file = File::Open(path, O_RDWR | O_EXCL | O_LARGEFILE, S_IRUSR);
    ASSERT_TRUE(file);
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(1024, size.value());

    ASSERT_TRUE(file->Fallocate(1024, 1024));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(2048, size.value());

    ASSERT_TRUE(file->Fallocate(0, 3586));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3586, size.value());

    ASSERT_TRUE(file->Fallocate(273, 3586));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3859, size.value());

    ASSERT_TRUE(file->Fallocate(15, 86));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3859, size.value());
    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3859, size.value());

    // We simply fail if offset is to big
    ASSERT_FALSE(file->Fallocate(5136, 17));
    ASSERT_TRUE(file->Close());
    file = File::Open(path, O_RDWR | O_EXCL | O_LARGEFILE, S_IRUSR);
    ASSERT_TRUE(file);
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3859, size.value());
    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3859, size.value());

    ASSERT_TRUE(file->Close());
    ASSERT_TRUE(File::Remove(path));
    file = NULL;
}

TEST_F(FileUtilTest, Fallocate_allocate_write)
{
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    std::string path = "work/falloc-file";
    Option<off_t> size;
    Option<bool> exists = File::Exists(path);
    ASSERT_TRUE(exists.valid());
    if (exists.value()) {
        ASSERT_TRUE(File::Remove(path));
    }

    file = File::Open(path, O_RDWR | O_CREAT | O_EXCL | O_LARGEFILE, 0777);
    ASSERT_TRUE(file);

    ASSERT_TRUE(file->Fallocate(0, 1024));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(1024, size.value());
    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(1024, size.value());

    // And also after close and open
    ASSERT_TRUE(file->Close());
    file = File::Open(path, O_RDWR | O_EXCL | O_LARGEFILE, S_IRUSR);
    ASSERT_TRUE(file);
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(1024, size.value());

    ASSERT_TRUE(file->Fallocate(1024, 1024));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(2048, size.value());

    ASSERT_TRUE(file->Fallocate(0, 3586));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3586, size.value());

    ASSERT_TRUE(file->Fallocate(273, 3586));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3859, size.value());

    ASSERT_TRUE(file->Fallocate(15, 86));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3859, size.value());
    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3859, size.value());

    // We simply fail if offset is to big
    ASSERT_FALSE(file->Fallocate(5136, 17));
    ASSERT_TRUE(file->Close());
    file = File::Open(path, O_RDWR | O_EXCL | O_LARGEFILE, S_IRUSR);
    ASSERT_TRUE(file);
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3859, size.value());
    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(3859, size.value());

    // And now with a few pages
    ASSERT_TRUE(file->Fallocate(0, 786336));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(786336, size.value());
    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(786336, size.value());

    ASSERT_TRUE(file->Fallocate(0, 819200));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(819200, size.value());
    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(819200, size.value());

    ASSERT_TRUE(file->Fallocate(0, 823296));
    size = file->GetSize();
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(823296, size.value());
    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(823296, size.value());

    ASSERT_TRUE(file->Close());
    ASSERT_TRUE(File::Remove(path));
    file = NULL;
}

TEST_F(FileUtilTest, Fallocate_no_override)
{
    std::string path = "work/falloc-file";
    std::string string_test = "dedupv1 is a fantastic cool thing and nobody would do his backup on something else.";
    const char* test = string_test.c_str();
    char buffer[1024];
    memset(buffer, 1, 1024);
    Option<off_t> size;

    Option<bool> exists = File::Exists(path);
    ASSERT_TRUE(exists.valid());
    if (exists.value()) {
        ASSERT_TRUE(File::Remove(path));
    }

    file = File::Open(path, O_RDWR | O_CREAT | O_EXCL | O_LARGEFILE, 0777);
    ASSERT_TRUE(file);

    ASSERT_EQ(strlen(test) + 1, file->Write(test, strlen(test) + 1));
    ASSERT_TRUE(file->Close());

    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(strlen(test) + 1, size.value());

    file = File::Open(path, O_RDWR | O_EXCL | O_LARGEFILE, S_IRUSR);
    ASSERT_TRUE(file);

    memset(buffer, 1, 1024);
    ASSERT_EQ(strlen(test) + 1, file->Read(0, buffer, 1024));
    ASSERT_FALSE(strcmp(buffer, test)) << "Read and Write Data differ: [" << test << "] to [" << buffer << "]";
    for (int i = strlen(test) + 1; i < 1024; i++) {
        ASSERT_EQ(1, buffer[i]) << "Byte " << i << " has to be 1 but is " << (uint16_t) buffer[i] << " (strlen is " << strlen(test) << ")";
    }

    ASSERT_TRUE(file->Fallocate(0, 10));
    ASSERT_TRUE(file->Close());

    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(strlen(test) + 1, size.value());

    file = File::Open(path, O_RDWR | O_EXCL | O_LARGEFILE, S_IRUSR);
    ASSERT_TRUE(file);
    memset(buffer, 1, 1024);
    ASSERT_EQ(strlen(test) + 1, file->Read(0, buffer, 1024));
    ASSERT_FALSE(strcmp(buffer, test)) << "Read and Write Data differ: [" << test << "] to [" << buffer << "]";
    for (int i = strlen(test) + 1; i < 1024; i++) {
        ASSERT_EQ(1, buffer[i]) << "Byte " << i << " has to be 1 but is " << (uint16_t) buffer[i] << " (strlen is " << strlen(test) << ")";
    }

    ASSERT_TRUE(file->Fallocate(strlen(test) - 5, 20));
    ASSERT_TRUE(file->Close());

    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(strlen(test) + 15, size.value());

    file = File::Open(path, O_RDWR | O_EXCL | O_LARGEFILE, S_IRUSR);
    ASSERT_TRUE(file);
    memset(buffer, 1, 1024);
    ASSERT_EQ(strlen(test) + 15, file->Read(0, buffer, 1024));
    ASSERT_FALSE(strcmp(buffer, test)) << "Read and Write Data differ: [" << test << "] to [" << buffer << "]";
    for (int i = strlen(test); i < strlen(test) + 15; i++) {
        ASSERT_EQ(0, buffer[i]) << "Byte " << i << " has to be 0 but is " << (uint16_t) buffer[i] << " (strlen is " << strlen(test) << ")";
    }
    for (int i = strlen(test) + 15; i < 1024; i++) {
        ASSERT_EQ(1, buffer[i]) << "Byte " << i << " has to be 1 but is " << (uint16_t) buffer[i] << " (strlen is " << strlen(test) << ")";
    }

    ASSERT_TRUE(file->Close());
    ASSERT_TRUE(File::Remove(path));
    file = NULL;
}

TEST_F(FileUtilTest, Fallocate_no_override_write)
{
    std::string path = "work/falloc-file";
    std::string string_test = "dedupv1 is a fantastic cool thing and nobody would do his backup on something else.";
    const char* test = string_test.c_str();
    char buffer[1024];
    memset(buffer, 1, 1024);
    Option<off_t> size;

    Option<bool> exists = File::Exists(path);
    ASSERT_TRUE(exists.valid());
    if (exists.value()) {
        ASSERT_TRUE(File::Remove(path));
    }

    file = File::Open(path, O_RDWR | O_CREAT | O_EXCL | O_LARGEFILE, 0777);
    ASSERT_TRUE(file);

    ASSERT_EQ(strlen(test) + 1, file->Write(test, strlen(test) + 1));
    ASSERT_TRUE(file->Close());

    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(strlen(test) + 1, size.value());

    file = File::Open(path, O_RDWR | O_EXCL | O_LARGEFILE, S_IRUSR);
    ASSERT_TRUE(file);

    memset(buffer, 1, 1024);
    ASSERT_EQ(strlen(test) + 1, file->Read(0, buffer, 1024));
    ASSERT_FALSE(strcmp(buffer, test)) << "Read and Write Data differ: [" << test << "] to [" << buffer << "]";
    for (int i = strlen(test) + 1; i < 1024; i++) {
        ASSERT_EQ(1, buffer[i]) << "Byte " << i << " has to be 1 but is " << (uint16_t) buffer[i] << " (strlen is " << strlen(test) << ")";
    }

    ASSERT_TRUE(file->Fallocate(0, 10));
    ASSERT_TRUE(file->Close());

    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(strlen(test) + 1, size.value());

    file = File::Open(path, O_RDWR | O_EXCL | O_LARGEFILE, S_IRUSR);
    ASSERT_TRUE(file);
    memset(buffer, 1, 1024);
    ASSERT_EQ(strlen(test) + 1, file->Read(0, buffer, 1024));
    ASSERT_FALSE(strcmp(buffer, test)) << "Read and Write Data differ: [" << test << "] to [" << buffer << "]";
    for (int i = strlen(test) + 1; i < 1024; i++) {
        ASSERT_EQ(1, buffer[i]) << "Byte " << i << " has to be 1 but is " << (uint16_t) buffer[i] << " (strlen is " << strlen(test) << ")";
    }

    ASSERT_TRUE(file->Fallocate(strlen(test) - 5, 20));
    ASSERT_TRUE(file->Close());

    size = File::GetFileSize(path);
    ASSERT_TRUE(size.valid());
    ASSERT_EQ(strlen(test) + 15, size.value());

    file = File::Open(path, O_RDWR | O_EXCL | O_LARGEFILE, S_IRUSR);
    ASSERT_TRUE(file);
    memset(buffer, 1, 1024);
    ASSERT_EQ(strlen(test) + 15, file->Read(0, buffer, 1024));
    ASSERT_FALSE(strcmp(buffer, test)) << "Read and Write Data differ: [" << test << "] to [" << buffer << "]";
    for (int i = strlen(test); i < strlen(test) + 15; i++) {
        ASSERT_EQ(0, buffer[i]) << "Byte " << i << " has to be 0 but is " << (uint16_t) buffer[i] << " (strlen is " << strlen(test) << ")";
    }
    for (int i = strlen(test) + 15; i < 1024; i++) {
        ASSERT_EQ(1, buffer[i]) << "Byte " << i << " has to be 1 but is " << (uint16_t) buffer[i] << " (strlen is " << strlen(test) << ")";
    }

    ASSERT_TRUE(file->Close());
    ASSERT_TRUE(File::Remove(path));
    file = NULL;
}

TEST_F(FileUtilTest, GetLine)
{
    file = File::Open("data/line-file", O_RDWR, S_IRUSR);
    ASSERT_TRUE(file);
    string buffer;

    int offset = 0;
    ASSERT_TRUE(file->GetLine( &offset, &buffer, 1024)) << "Cannot read 1st line";
    ASSERT_EQ(buffer, "block-index=static-disk-hash");

    ASSERT_TRUE(file->GetLine( &offset, &buffer, 1024));
    ASSERT_EQ(buffer, "block-index.page-size=2K");

    ASSERT_TRUE(file->GetLine( &offset, &buffer, 1024));
    ASSERT_EQ(buffer, "block-index.size=4M");

    ASSERT_TRUE(file->GetLine( &offset, &buffer, 1024));
    ASSERT_EQ(buffer, "block-index.filename=work/block-index1");

    ASSERT_TRUE(file->GetLine( &offset, &buffer, 1024));
    ASSERT_EQ(buffer, "block-index.filename=work/block-index2");

    ASSERT_FALSE(file->GetLine( &offset, &buffer, 1024));
    ASSERT_EQ(buffer, "") << "Line is not reset if EOF";
}

TEST_F(FileUtilTest, GetLine2)
{
    file = File::Open("data/line-file2", O_RDWR, S_IRUSR);
    ASSERT_TRUE(file);

    string buffer;

    int offset = 0;
    ASSERT_TRUE(file->GetLine( &offset, &buffer, 1024)) << "Cannot read 1st line";
    ASSERT_EQ(buffer, "block-index=static-disk-hash");

    ASSERT_TRUE(file->GetLine( &offset, &buffer, 1024));
    ASSERT_EQ(buffer, "block-index.page-size=2K");

    ASSERT_TRUE(file->GetLine( &offset, &buffer, 1024));
    ASSERT_EQ(buffer, "block-index.size=4M");

    ASSERT_TRUE(file->GetLine( &offset, &buffer, 1024));
    ASSERT_EQ(buffer, "block-index.filename=work/block-index1");

    ASSERT_TRUE(file->GetLine( &offset, &buffer, 1024));
    ASSERT_EQ(buffer, "block-index.filename=work/block-index2");

    ASSERT_FALSE(file->GetLine( &offset, &buffer, 1024));
    ASSERT_EQ(buffer, "") << "Line is not reset if EOF";
}

TEST_F(FileUtilTest, ListDirectory)
{
    vector<string> files;

    ASSERT_TRUE(File::ListDirectory("work", &files));
    vector<string>::iterator i;
    for (i = files.begin(); i != files.end(); i++) {
        DEBUG(*i);
    }
    int old_size = files.size();

    file = File::Open("work/line-file2", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    byte buffer[64 * 1024];
    memset(buffer, 0, 64 * 1024);
    ASSERT_EQ(file->Write(0, buffer, 64 * 1024), 64 * 1024);
    file->Close();
    file = NULL;

    ASSERT_TRUE(File::ListDirectory("work", &files));
    for (i = files.begin(); i != files.end(); i++) {
        DEBUG(*i);
    }
    ASSERT_EQ(files.size(), old_size + 1);
    bool found = false;
    for (i = files.begin(); i != files.end(); i++) {
        if (*i == "line-file2") {
            found = true;
        }
    }
    ASSERT_TRUE(found) << "Found line-file2 in listing";
}

TEST_F(FileUtilTest, Mkdir)
{
    ASSERT_TRUE(File::Mkdir("work/a", S_IRUSR | S_IWUSR | S_IXUSR));

    Option<bool> a = File::Exists("work/a");
    ASSERT_TRUE(a.valid());
    ASSERT_TRUE(a.value());

    a = File::IsDirectory("work/a");
    ASSERT_TRUE(a.valid());
    ASSERT_TRUE(a.value());
}

TEST_F(FileUtilTest, Basename)
{
    Option<string> a = File::Basename("work/a");
    ASSERT_TRUE(a.valid());
    ASSERT_EQ("a", a.value());

    a = File::Dirname("work/a");
    ASSERT_TRUE(a.valid());
    ASSERT_EQ("work", a.value());
}

TEST_F(FileUtilTest, MakeParentDirectory)
{
    ASSERT_TRUE(File::MakeParentDirectory("work/a/b/d/a", S_IRUSR | S_IWUSR | S_IXUSR));

    Option<bool> a = File::Exists("work/a/b/d");
    ASSERT_TRUE(a.valid());
    ASSERT_TRUE(a.value());
}

TEST_F(FileUtilTest, ReadContents)
{
    Option<bytestring> o = File::ReadContents("data/dedupv1_test.conf");
    ASSERT_TRUE(o.valid());
    ASSERT_GT(o.value().size(), 800);
}

TEST_F(FileUtilTest, ReadWrite)
{
    byte buffer[1024];
    memset(buffer, 1, 1024);

    // write
    File* f1 = File::Open("work/tmp", O_RDWR | O_CREAT, S_IRUSR);
    ASSERT_TRUE(f1);

    // Here we have a sparse file write
    ASSERT_EQ(1024, f1->Write(1024, buffer, 1024));

    ASSERT_TRUE(f1->Sync());
    ASSERT_TRUE(f1->Close());

    // read
    File* f2 = File::Open("work/tmp", O_RDONLY, S_IRUSR);
    ASSERT_TRUE(f2);

    byte read_buffer[1024];
    memset(read_buffer, 0, 1024);

    ASSERT_EQ(1024, f2->Read(0, read_buffer, 1024));

    ASSERT_EQ(1024, f2->Read(1024, read_buffer, 1024));
    ASSERT_TRUE(memcmp(buffer, read_buffer, 1024) == 0);

    ASSERT_TRUE(f2->Close());

    unlink("work/tmp");
}

/**
 * Tests the file locking methods.
 */
TEST_F(FileUtilTest, Locking)
{
    File* f1 = File::Open("data/line-file", O_RDWR, S_IRUSR);
    ASSERT_TRUE(f1);

    ASSERT_TRUE(f1->Lock(true));

    File* f2 = File::Open("data/line-file", O_RDWR, S_IRUSR);
    ASSERT_TRUE(f2);

    Option<bool> l = f2->TryLock(true);
    ASSERT_TRUE(l.valid());
    ASSERT_FALSE(l.value());

    ASSERT_TRUE(f1->Unlock());
    ASSERT_TRUE(f1->Close());

    l = f2->TryLock(true);
    ASSERT_TRUE(l.valid());
    ASSERT_TRUE(l.value());

    ASSERT_TRUE(f2->Unlock());

    ASSERT_TRUE(f2->Close());
}

TEST_F(FileUtilTest, Copy)
{
    ASSERT_TRUE(File::CopyFile("data/line-file", "work/c"));

    Option<bytestring> o1 = File::ReadContents("data/line-file");
    Option<bytestring> o2 = File::ReadContents("work/c");
    ASSERT_TRUE(o1.valid());
    ASSERT_TRUE(o2.valid());
    ASSERT_TRUE(o1.value() == o2.value());

    // overwrite
    ASSERT_TRUE(File::CopyFile("data/line-file", "work/c"));

    o1 = File::ReadContents("data/line-file");
    o2 = File::ReadContents("work/c");
    ASSERT_TRUE(o1.valid());
    ASSERT_TRUE(o2.valid());
    ASSERT_TRUE(o1.value() == o2.value());
}

/**
 * Tests the File::Truncate method.
 */
TEST_F(FileUtilTest, Truncate)
{
    ASSERT_TRUE(File::CopyFile("data/line-file", "work/c"));

    ASSERT_TRUE(File::Truncate("work/c", 5));

    Option<off_t> s = File::GetFileSize("work/c");
    ASSERT_TRUE(s.valid());
    ASSERT_EQ(5, s.value());
}

/**
 * Tests at File::CopyFile fails when the destination file
 * exists in the non-overwrite mode.
 */
TEST_F(FileUtilTest, CopyNoOverwrite)
{
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(File::CopyFile("data/line-file", "work/c"));
    ASSERT_FALSE(File::CopyFile("data/line-file", "work/c", File::kDefaultFileMode, false));
}
