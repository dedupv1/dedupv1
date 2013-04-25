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

#include "dedupv1d.h"
#include <signal.h>
#include <syslog.h>
#include <unistd.h>
#include <base/daemon_util.h>
#include <core/dedup_system.h>
#include <base/aprutil.h>
#include <base/logging.h>
#include <base/stacktrace.h>
#include <base/fileutil.h>
#include <base/startup.h>
#include <sys/prctl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "dedupv1.pb.h"

#ifdef LOGGING_LOG4CXX
#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/propertyconfigurator.h"
#include "log4cxx/helpers/exception.h"
#include "log4cxx/xml/domconfigurator.h"
#include <log4cxx/ndc.h>
#endif

using std::string;
using dedupv1::base::File;
using dedupv1::StartContext;

static dedupv1d::Dedupv1d* ds = NULL;

LOGGER("Dedupv1d");

void dedupv1d_sigint(int sig, siginfo_t *info, void *uap) {

    if (info) {
        INFO("Received fast stop signal: " <<
                "signal " << sig <<
                ", sending uid " << info->si_uid <<
                ", sending pid " << info->si_pid);
    }

    INFO("Stopping dedupv1d (fast mode)");
    if (ds) {
        // if stopped via SIGINT: use fast shutdown
        if (!ds->Shutdown(dedupv1::StopContext::FastStopContext())) {
            WARNING("Failed to shutdown application");
        }
    }
}

void dedupv1d_sigquit(int sig, siginfo_t *info, void *uap) {
    if (info) {
        INFO("Received stop signal: " <<
                "signal " << sig <<
                ", sending uid " << info->si_uid <<
                ", sending pid " << info->si_pid);
    }
    if (ds) {
        // if stopped via SIGQUIT: use normal shutdown
        if (!ds->Shutdown(dedupv1::StopContext())) {
            WARNING("Failed to shutdown application");
        }
    }
}

void ChangeDumpingState(int dump_state) {
    int ret = 0;
    if (dump_state == 1) {
        ret = prctl( PR_GET_DUMPABLE, 0, 0, 0, 0 );
        if (ret == -1) {
            WARNING("Failed to check dumpstate: " << strerror(errno));
            return;
        } else if (ret == 0) {
            DEBUG("Make dedupv1d dumpable");
            ret = prctl( PR_SET_DUMPABLE, 1, 0, 0, 0 );
            if (ret != 0) {
                WARNING("Failed to set dumpstate: " << strerror(errno));
                return;
            }

            // check change
            ret = prctl( PR_GET_DUMPABLE, 0, 0, 0, 0 );
            if (ret == -1) {
                WARNING("Failed to check dumpstate: " << strerror(errno));
                return;
            } else  if (ret == 0) {
                WARNING("Failed to set dumpable state");
            }

            {
                struct rlimit rlim;
                /* Get the core dump limitation */
                ret = getrlimit(RLIMIT_CORE, &rlim);
                if (ret != 0) {
                    WARNING("Failed to get core limit: " << strerror(errno));
                    return;
                }

                /* Set the core dump limitation to be unlimited */
                rlim.rlim_cur = RLIM_INFINITY;
                rlim.rlim_max = RLIM_INFINITY;
                ret = setrlimit(RLIMIT_CORE, &rlim);
                if (ret != 0) {
                    WARNING("Failed to set core limit: " << strerror(errno));
                    return;
                }

                /* Get the core dump limitation again */
                ret = getrlimit(RLIMIT_CORE, &rlim);
                if (ret != 0) {
                    WARNING("Failed to get core limit: " << strerror(errno));
                    return;
                } else {
                    if (rlim.rlim_cur != RLIM_INFINITY || rlim.rlim_max != RLIM_INFINITY) {
                        WARNING("Failed to set core limit");
                    }
                }
            }

        } else {
            DEBUG("dedupv1d is already dumpable");
        }
    } else if (dump_state == 2) {
        ret = prctl( PR_GET_DUMPABLE, 0, 0, 0, 0 );
        if (ret == -1) {
            WARNING("Failed to check dumpstate: " << strerror(errno));
            return;
        } else if (ret == 1) {
            DEBUG("Make dedupv1d non-dumpable");
            ret = prctl( PR_SET_DUMPABLE, 0, 0, 0, 0 );
            if (ret != 0) {
                WARNING("Failed to set dumpstate: " << strerror(errno));
                return;
            }
            ret = prctl( PR_GET_DUMPABLE, 0, 0, 0, 0 );
            if (ret == -1) {
                WARNING("Failed to check dumpstate: " << strerror(errno));
                return;
            }
            if (ret == 10) {
                WARNING("Failed to set non-dumpable state");
            }
        } else {
            DEBUG("dedupv1d is already non-dumpable");
        }
    }
}

namespace {
/**
 * Register standard signal handlers
 */
bool RegisterSignals() {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_flags = SA_SIGINFO;

    sa.sa_sigaction = dedupv1d_sigint;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    sa.sa_sigaction = dedupv1d_sigquit;
    sigaction(SIGQUIT, &sa, NULL);

    // ignore these
    signal(SIGPIPE, SIG_IGN);
    signal(SIGALRM, SIG_IGN);

    // print a kind of stacktrace if segfaulted
    dedupv1::base::setup_sigsegv();

    return true;
}
}

int main(int argc, char** argv) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    bool no_daemon = false;
    StartContext::create_mode create = StartContext::NON_CREATE;
    StartContext::force_mode force = StartContext::NO_FORCE;
    string default_logging_config_file = File::Join(DEDUPV1_ROOT, "etc/dedupv1/logging.xml");
    string logging_config_file = default_logging_config_file;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--no-daemon") == 0) {
            no_daemon = true;
        }
        if (strcmp(argv[i], "--create") == 0) {
            create = StartContext::CREATE;
        }
        if (strcmp(argv[i], "--force") == 0) {
            force = StartContext::FORCE;
        }
        if (strcmp(argv[i], "--logging") == 0) {
            if (i + 1 < argc) {
                logging_config_file = argv[i + 1];
            }
        }
    }

#ifdef LOGGING_LOG4CXX
    log4cxx::xml::DOMConfigurator::configureAndWatch(logging_config_file);
    log4cxx::NDC ndc("dedupv1d");
#endif
#ifdef LOGGING_SYSLOG
    dedupv1::base::logging::Syslog::instance().Open("dedupv1d");
#endif

    if (logging_config_file != default_logging_config_file) {
        DEBUG("Using custom logging config: " << logging_config_file);
    }

    INFO("Starting dedupv1d");
    dedupv1::DedupSystem::RegisterDefaults();

    CHECK_RETURN(argc >= 2, 1, "Cannot start dedupv1d without configuration file argument");

    ds = new dedupv1d::Dedupv1d();
    CHECK_RETURN(ds->LoadOptions(argv[1]), 1, "Cannot load options: file " << argv[1]);

    StartContext start_context(create);
    start_context.set_force(force);

#ifdef DAEMON
    if (!no_daemon) {
        File* lock_fd = NULL;
        CHECK_RETURN(daemonize("dedupv1d",
                ds->daemon_user(),
                ds->daemon_group(),
                ds->daemon_lockfile(),
                &lock_fd), 1, "Failed to start dedupv1d daemon process");

        CHECK_RETURN(RegisterSignals(), 1, "Failed to register signals");

        CHECK_RETURN(ds->AttachLockfile(lock_fd), 1, "Failed to attach lock file");
    } else {
        INFO("Running as dedupv1 application");
#endif
        CHECK_RETURN(ds->OpenLockfile(), 1, "Failed to open lock file");
#ifdef DAEMON
    }
#endif
    ChangeDumpingState(ds->dump_state());

    CHECK_RETURN(ds->Start(start_context), 1, "Starting dedupv1d failed");

    CHECK_RETURN(ds->Run(), 1, "Running dedupv1d failed");

    CHECK_RETURN(ds->Wait(), 1, "Waiting for shutdown failed");

    int rc = 0;
    if (!ds->Stop()) {
        ERROR("Stopping dedupv1d failed");
        rc = 1;
    }

    delete ds;
    ds = NULL;

    google::protobuf::ShutdownProtobufLibrary();
    return rc;
}

