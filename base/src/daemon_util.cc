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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <syslog.h>
#include <errno.h>
#include <pwd.h>
#include <grp.h>
#include <signal.h>

#include <base/daemon_util.h>
#include <base/logging.h>
#include <base/strutil.h>

LOGGER("DaemonUtil");

using dedupv1::base::File;
using dedupv1::base::strutil::ToString;
using std::string;

namespace dedupv1 {
namespace base {

#ifndef __APPLE__

#define EXIT_SUCCESS 0
#define EXIT_FAILURE 1

static void child_handler(int signum) {
    switch (signum) {
    case SIGALRM:
        ERROR("Daemon child start timeout");
        exit(EXIT_FAILURE);
        break;
    case SIGUSR1:
        exit(EXIT_SUCCESS);
        break;
    case SIGCHLD:
        ERROR("Daemon child died");
        exit(EXIT_FAILURE);
        break;
    }
}

bool daemonize(const string& daemon_name, const string& daemon_user, const string& daemon_group, const string& lockfile, File** lock_fd) {
    DEBUG("Daemonize: " <<
        "daemon name " << daemon_name <<
        ", daemon user " << daemon_user <<
        ", daemon group " << daemon_group <<
        ", lock file " << lockfile);

    CHECK(getppid() != 1, "Process already a daemon");

    if (daemon_group.size() > 0) {
        // Drop user if there is one, and we were run as daemon user
        struct group* grp = getgrnam(daemon_group.c_str());
        CHECK(grp != NULL, "Group " << daemon_group << " does not exists");

        INFO("Setting group to " << daemon_group);
        CHECK(setegid( grp->gr_gid ) == 0, "Unable to switch group: " <<
            "gid " << grp->gr_gid <<
            ", group name " << daemon_group <<
            ", message " << strerror(errno));

    }

    if (daemon_user.size() > 0) {
        // Drop user if there is one, and we were run as daemon user
        struct passwd *pw = getpwnam(daemon_user.c_str());
        CHECK(pw != NULL, "User doesn't exists: " << daemon_user);

        INFO("Setting user to " << daemon_user);
        CHECK(setuid( pw->pw_uid ) == 0, "Unable to switch user: " <<
            "uid " << pw->pw_uid <<
            ", user name " << daemon_name <<
            ", message " << strerror(errno));

    }

    // Trap signals that we expect to receive
    signal(SIGCHLD, child_handler);
    signal(SIGUSR1, child_handler);
    signal(SIGALRM, child_handler);

    // Fork off the parent process
    pid_t pid = fork();
    CHECK(pid >= 0, "Unable to fork daemon: " << strerror(errno));
    // If we got a good PID, then we can exit the parent process.
    if (pid > 0) {

        /* Wait for confirmation from the child via SIGTERM or SIGCHLD, or
           for 60 seconds to elapse (SIGALRM).  pause() should not return. */
        alarm(60);
        pause();

        // TODO (dmeister): The parent doesn't clean up its memory
        exit(EXIT_FAILURE);
    }
    INFO("Starting with pid " << getpid());
    // At this point we are executing as the child process
    pid_t parent = getppid();

    // Cancel certain signals
    signal(SIGCHLD, SIG_IGN); /* A child process dies */
    signal(SIGTSTP, SIG_IGN); /* Various TTY signals */
    signal(SIGTTOU, SIG_IGN);
    signal(SIGTTIN, SIG_IGN);
    signal(SIGHUP,  SIG_IGN); /* Ignore hangup signal */
    signal(SIGALRM, SIG_DFL); // it is important the reset SIGALRM to the default handler

    // Change the file mode mask
    umask(0007); // umask always succeeds

    // Create a new SID for the child process
    pid_t sid = setsid();
    CHECK(sid >= 0, "Unable to create a new session: " << strerror(errno));

    /* Change the current working directory.  This prevents the current
       directory from being locked; hence not being able to remove it. */
    CHECK(chdir("/") >= 0, "Unable to change directory: " << strerror(errno));

    /* Redirect standard files to /dev/null */
    CHECK(freopen("/dev/null", "r", stdin) != NULL, "Failed to redirect stdin");
    CHECK(freopen("/dev/null", "w", stdout) != NULL, "Failed to redirect stdout");
    CHECK(freopen("/dev/null", "w", stderr) != NULL, "Failed to redirect stderr");

    // Tell the parent process that we are ok
    kill(parent, SIGUSR1);

    File* lf = NULL;
    if (lockfile.size() > 0) {
        // in earlier version, we used O_EXCL, but we are now using file locks.
        lf = File::Open(lockfile, O_RDWR | O_CREAT, 0660);
        CHECK(lf, "Unable to create lock file: " << lockfile << ", error " << strerror(errno));

        Option<bool> flock = lf->TryLock(true);
        CHECK(flock.valid(), "Failed to lock file " << lf->path());
        if (!flock.value()) {
            ERROR("Failed to get exclusive file lock: " << lf->path());
            delete lf;
            return false;
        }
        if (!lf->Truncate(0)) {
            ERROR("Failed to truncate lock file: " << lf->path());
            delete lf;
            return false;
        }
        string buffer = ToString(getpid());
        if (lf->Write(0, buffer.c_str(), buffer.size()) != buffer.size()) {
            ERROR("Failed to write pid: " << lf->path());
            delete lf;
            return false;
        }
        if (!lf->Sync()) {
            ERROR("Failed to fsync lock file: " << lf->path());
            delete lf;
            return false;
        }
    }

    if (lock_fd) {
        *lock_fd = lf;
    }
    return true;
}

#endif

}
}
