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
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <pwd.h>
#include <grp.h>
#include <signal.h>
#include <sys/resource.h>
#include <errno.h>

#include <sstream>
#include <string>
#include <list>

#include <core/dedup.h>
#include <core/log_consumer.h>
#include <base/locks.h>
#include <core/block_index.h>
#include <base/strutil.h>
#include <base/index.h>
#include <core/storage.h>
#include <core/chunk_index.h>
#include <base/disk_hash_index.h>
#include <base/hash_index.h>
#include <base/tc_hash_index.h>
#include <base/tc_hash_mem_index.h>
#include <base/tc_btree_index.h>
#include <core/container_storage.h>
#include <core/dedup_system.h>
#include <core/dedup_volume.h>
#include <core/filter.h>
#include <core/chunk_index_filter.h>
#include <core/block_index_filter.h>
#include <core/bytecompare_filter.h>
#include <core/chunker.h>
#include <core/static_chunker.h>
#include <core/rabin_chunker.h>
#include <core/fingerprinter.h>
#include <base/threadpool.h>
#include <base/logging.h>
#include <base/fileutil.h>
#include <base/callback.h>
#include <base/config_loader.h>
#include <base/startup.h>
#include <base/crc32.h>

#include "dedupv1d_volume.h"
#include "dedupv1d_volume_info.h"
#include "scst_handle.h"
#include "command_handler.h"
#include "dedupv1d.h"
#include "monitor.h"
#include "profile_monitor.h"
#include "lock_monitor.h"
#include "stats_monitor.h"
#include "trace_monitor.h"
#include "status_monitor.h"
#include "config_monitor.h"
#include "log_monitor.h"
#include "log_replayer.h"
#include "volume_monitor.h"
#include "target_monitor.h"
#include "group_monitor.h"
#include "user_monitor.h"
#include "version_monitor.h"
#include "perftools_monitor.h"
#include "inspect_monitor.h"
#include "flush_monitor.h"
#include "idle_monitor.h"
#include "gc_monitor.h"
#include "fault_inject_monitor.h"
#include "logging_monitor.h"
#include "error_monitor.h"
#include "session_monitor.h"
#include "monitor_monitor.h"
#include "container_gc_monitor.h"
#include "version.h"
#include "dedupv1d_target_info.h"
#include "dedupv1d_group_info.h"
#include "dedupv1d_user_info.h"
#include "dedupv1d_stats.pb.h"

using std::list;
using std::string;
using std::stringstream;
using std::endl;
using std::vector;
using dedupv1::base::File;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::Threadpool;
using dedupv1d::monitor::MonitorSystem;
using dedupv1::DedupSystem;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::NewCallback;
using dedupv1::base::ConfigLoader;
using dedupv1::StartContext;
using dedupv1::base::ScheduleContext;
using dedupv1::base::ScheduleOptions;
using dedupv1::base::crc;
LOGGER("Dedupv1d");
MAKE_LOGGER(g_stats_logger, "Dedupv1dStats");

namespace dedupv1d {

const uint32_t Dedupv1d::kDefaultThreadpoolSize = 32;

Dedupv1d::Dedupv1d() :
    dedupv1::base::memory::NewHandlerListener() {
    this->configured_ = false;
    this->dedup_system_ = NULL;
    this->monitor_ = NULL;
    this->state_ = CREATED;
    this->current_config_volume_ = NULL;

    this->monitor_config_["profile"] = MONITOR_ENABLED;
    this->monitor_config_["lock"] = MONITOR_ENABLED;
    this->monitor_config_["stats"] = MONITOR_ENABLED;
    this->monitor_config_["trace"] = MONITOR_ENABLED;
    this->monitor_config_["status"] = MONITOR_ENABLED;
    this->monitor_config_["config"] = MONITOR_ENABLED;
    this->monitor_config_["log"] = MONITOR_ENABLED;
    this->monitor_config_["volume"] = MONITOR_ENABLED;
    this->monitor_config_["target"] = MONITOR_ENABLED;
    this->monitor_config_["group"] = MONITOR_ENABLED;
    this->monitor_config_["user"] = MONITOR_ENABLED;
    this->monitor_config_["version"] = MONITOR_ENABLED;
    this->monitor_config_["inspect"] = MONITOR_ENABLED;
    this->monitor_config_["flush"] = MONITOR_ENABLED;
    this->monitor_config_["idle"] = MONITOR_ENABLED;
    this->monitor_config_["gc"] = MONITOR_ENABLED;
    this->monitor_config_["container-gc"] = MONITOR_ENABLED;
    this->monitor_config_["logging"] = MONITOR_ENABLED;
    this->monitor_config_["error"] = MONITOR_ENABLED;
    this->monitor_config_["session"] = MONITOR_ENABLED;
    this->monitor_config_["monitor"] = MONITOR_ENABLED;
#ifdef FAULT_INJECTION
    this->monitor_config_["fault-inject"] = MONITOR_ENABLED;
#else
    this->monitor_config_["fault-inject"] = MONITOR_FORBIDDEN;
#endif
    this->daemon_user_ = "";
    this->daemon_group_ = "";

#ifdef DEDUPV1D_TEST
    // we ignore the locking during the tests
    this->daemon_lockfile_ = "";
#else
    this->daemon_lockfile_ = File::Join(DEDUPV1_ROOT, "var/lock/dedupv1d");
#endif
    this->daemon_dirtyfile_ = File::Join(DEDUPV1_ROOT, "var/lib/dedupv1/dirty");
    this->log_replayer_ = NULL;
    this->stats_persist_interval_ = 60; // each minute
    this->uptime_log_interval_ = 10 * 60; // every 10 minutes
    this->last_service_time_ = -1.0;
    dump_state_ = 0;
    lockfile_handle_ = NULL;

#ifndef DEBUG_TEST
    memory_parachute_size_ = 128 * 1024 * 1024; // 128 MB
#else
    memory_parachute_size_ = 0;
#endif

    this->max_memory = 0;
    startup_tick_count_ = tbb::tick_count::now();

    // TODO (dmeister): This should not be an set options call
    this->threads_.SetOption("size", ToString(kDefaultThreadpoolSize));

    this->dedup_system_ = new DedupSystem();
    this->monitor_ = new MonitorSystem();
    this->log_replayer_ = new LogReplayer();
    this->volume_info_ = new Dedupv1dVolumeInfo();
    this->target_info_ = new Dedupv1dTargetInfo();
    this->group_info_ = new Dedupv1dGroupInfo();
    this->user_info_ = new Dedupv1dUserInfo();
}

bool Dedupv1d::WriteDirtyState(const dedupv1::FileMode& file_mode, bool dirty, bool stopped) {
    string dirtyfile_backup_name = this->daemon_dirtyfile() + ".tmp";

    Option<bool> dirty_file_exists = File::Exists(this->daemon_dirtyfile());
    Option<bool> dirty_tmp_file_exists = File::Exists(dirtyfile_backup_name);

    CHECK(dirty_file_exists.valid(), "Cannot check if dirty file exists");
    CHECK(dirty_tmp_file_exists.valid(), "Cannot check if dirty file exists");

    bool tmp_file_created = false;
    if (dirty_file_exists.value()) {
        // create copy
        CHECK(File::CopyFile(this->daemon_dirtyfile(), dirtyfile_backup_name, file_mode.mode()),
            "Failed to copy dirty file");
        tmp_file_created = true;

        if (!dirty_tmp_file_exists.value() && file_mode.gid() != -1) {
            // change backup file owner only if the file is new
            CHECK(chown(dirtyfile_backup_name.c_str(), -1, file_mode.gid()) == 0,
                "Failed to change file group: " << dirtyfile_backup_name);
        }
    }

    File* dirty_file = File::Open(this->daemon_dirtyfile(), O_RDWR | O_CREAT, file_mode.mode());
    CHECK(dirty_file, "Unable to open dirty file: " << this->daemon_dirtyfile() << ", error " << strerror(errno));

    DirtyFileData dirty_data;
    dirty_data.set_config(this->config_data_);
    dirty_data.set_clean(!dirty);
    dirty_data.set_stopped(stopped);
    dirty_data.set_revision(DEDUPV1_REVISION_STR);

    DEBUG("Write dirty state: " << dirty_data.ShortDebugString());

    CHECK(dirty_file->WriteSizedMessage(0, dirty_data, 8 * 4096, true) > 0,
        "Failed to write dirty file with data " << dirty_data.ShortDebugString());
    CHECK(dirty_file->Sync(), "Failed to sync dirty file");
    delete dirty_file;
    dirty_file = NULL;

    if (!dirty_file_exists.value() && file_mode.gid() != -1) {
        // Only try to change file owner if the file is created at first
        CHECK(chown(this->daemon_dirtyfile().c_str(), -1, file_mode.gid()) == 0,
            "Failed to change file group: " << this->daemon_dirtyfile());
    }

    // no everything is ok, we can delete the .tmp file

    if (tmp_file_created) {
        if (!File::Remove(dirtyfile_backup_name)) {
            WARNING("Failed to remove tmp dirty file");
        }
    }

    return true;
}

namespace {
Option<DirtyFileData> ReadDirtyFile(const std::string& filename) {
    DirtyFileData dirty_data;
    File* dirty_file = File::Open(filename, O_RDWR, 0);
    CHECK(dirty_file, "Unable to open dirty file: " << filename << ", error " << strerror(errno));

    if (!dirty_file->ReadSizedMessage(0, &dirty_data, 8 * 4096, true)) {
        ERROR("Failed to read dirty file: " << filename);
        delete dirty_file;
        return false;
    }
    delete dirty_file;
    return make_option(dirty_data);
}

}

Option<DirtyFileData> Dedupv1d::CheckDirtyState() {
    Option<bool> dirty_file_exists = File::Exists(this->daemon_dirtyfile());
    Option<bool> dirty_tmp_file_exists = File::Exists(this->daemon_dirtyfile() + ".tmp");

    CHECK(dirty_file_exists.valid(), "Cannot check if dirty file exists");
    CHECK(dirty_tmp_file_exists.valid(), "Cannot check if dirty file exists");

    if (dirty_file_exists.value()) {
        Option<DirtyFileData> r = ReadDirtyFile(this->daemon_dirtyfile());
        if (r.valid()) {
            // check r
            if (r.value().has_clean() && r.value().has_config() && r.value().has_revision()) {
                return r;
            }
            WARNING("Illegal dirty state: " << r.value().ShortDebugString());
        }
    }
    if (dirty_tmp_file_exists.value()) {
        // try the replacement file
        Option<DirtyFileData> r = ReadDirtyFile(this->daemon_dirtyfile() + ".tmp");
        if (r.valid()) {
            // check r
            if (r.value().has_clean() && r.value().has_config() && r.value().has_revision()) {
                DirtyFileData dfd = r.value();
                dfd.set_clean(false); // if we have to use the backup file we are dirty (!)
                return make_option(dfd);
            }
            WARNING("Illegal dirty state: " << r.value().ShortDebugString());
        }
    }
    ERROR("Failed to read dirty file");
    return false;
}

bool Dedupv1d::AttachLockfile(dedupv1::base::File* lock_file) {
    DCHECK(lock_file, "Lockfile not set");
    CHECK(!lockfile_handle_, "Lockfile already set");
    lockfile_handle_ = lock_file;
    return true;
}

bool Dedupv1d::OpenLockfile() {
#ifndef DEDUPV1D_TEST
    // we ignore the locking during the tests
    if (daemon_lockfile_.empty()) {
        return true;
    }
#endif
    CHECK(!lockfile_handle_, "Lockfile already set");
    CHECK(daemon_lockfile().size() > 0, "Lockfile name not set");

    DEBUG("Open lock file " << daemon_lockfile());

    // in earlier version, we used O_EXCL, but we are now using file locks.
    File* lf = File::Open(daemon_lockfile(), O_RDWR | O_CREAT, 0660);
    CHECK(lf, "Unable to create lock file: " << daemon_lockfile() << ", error " << strerror(errno));

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

    // everything is fine
    lockfile_handle_ = lf;

    return true;
}

namespace {
bool GetStartContextFileModes(StartContext* start_context, const std::string& group_name) {
    Option<dedupv1::FileMode> file_mode = dedupv1::FileMode::Create(group_name, false, 0);
    CHECK(file_mode.valid(), "Failed to get file mode for group: " << group_name);
    start_context->set_file_mode(file_mode.value());

    file_mode = dedupv1::FileMode::Create(group_name, true, 0);
    CHECK(file_mode.valid(), "Failed to get file mode for group: " << group_name);
    start_context->set_dir_mode(file_mode.value());
    return true;
}
}

bool Dedupv1d::Start(const StartContext& preliminary_start_context, bool no_log_replay) {
    CHECK(this->state_ == CREATED, "Dedupv1 system already started");
    CHECK(this->configured_, "Dedupv1 system not configured");
    this->start_context_ = preliminary_start_context;

    if (this->max_memory > 0) {
        struct rlimit limit;
        memset(&limit, 0, sizeof(limit));
        CHECK(getrlimit(RLIMIT_AS, &limit) == 0, "Failed to get memory limit of process: " << strerror(errno));
        CHECK(limit.rlim_max >= this->max_memory, "Cannot limit memory to " << this->max_memory << " because max hard limit is " << limit.rlim_max);
        limit.rlim_cur = this->max_memory;
        CHECK(setrlimit(RLIMIT_AS, &limit) == 0, "Failed to set memory limit of process" << strerror(errno));
    }

    if (memory_parachute_size_ > 0) {
        CHECK(dedupv1::base::memory::RegisterMemoryParachute(
                memory_parachute_size_),
            "Registering new handler failed");
        CHECK(dedupv1::base::memory::AddMemoryParachuteListener(this), "Failed to add the listener");
    }

    if (daemon_user_.size() > 0) {
        CHECK(getpwnam(daemon_user_.c_str()) != NULL, "User " << daemon_user_ << " does not exists");
    }
    if (daemon_group_.size() > 0) {
        CHECK(getgrnam(daemon_group_.c_str()) != NULL, "Group " << daemon_group_ << " does not exists");

        // use custom group
        CHECK(GetStartContextFileModes(&start_context_, this->daemon_group_),
            "Failed to get start context file modes for group " << this->daemon_group_);
    }

    Option<bool> dirty_file_exists = dedupv1::base::File::Exists(this->daemon_dirtyfile());
    CHECK(dirty_file_exists.valid(), "Cannot check if dirty file exists");

    if (start_context_.create()) {
        if (dirty_file_exists.value()) {
            ERROR("System already initialized: " << "dirty file name " << this->daemon_dirtyfile_
                                                 << ", reason dirty file exists in create mode");

            if (!start_context_.force()) {
                return false;
            }
        }
        // dirty file does not exists (or we force it)
        CHECK(this->WriteDirtyState(start_context_.file_mode(), true, false),
            "Failed to write new dirty state file");
    } else if (!start_context_.create()) {
        if (unlikely(!dirty_file_exists.value())) {
            ERROR("System not initialized: " << "dirty file name " << this->daemon_dirtyfile_);

            if (!start_context_.force()) {
                return false;
            } else {
                // rescue the state
                CHECK(this->WriteDirtyState(start_context_.file_mode(), true, false),
                    "Failed to write new dirty state file");
            }
        } else {
            // dirty file exists (normal case)

            Option<DirtyFileData> dirty_state = this->CheckDirtyState();
            if (dirty_state.valid()) {
                DEBUG("Check config: " << this->config_data_ << ", dirty state " << dirty_state.value().DebugString());
                // check config
                if (dirty_state.value().has_config() && dirty_state.value().config() != this->config_data_) {
                    if (start_context_.force()) {
                        WARNING("Configuration changed");
                    } else {
                        ERROR("Configuration changed: " << "stored config " << crc(dirty_state.value().config().data(),
                                dirty_state.value().config().size()) << ", current config " << crc(this->config_data_.data(),
                                this->config_data_.size()));
                        return false;
                    }
                }
                if (dirty_state.value().has_revision() && dirty_state.value().revision() != DEDUPV1_REVISION_STR) {
                    INFO("Daemon version change: old revision: " << dirty_state.value().revision() << ", current revision "
                                                                 << DEDUPV1_REVISION_STR);
                }
                if (!this->start_context_.dirty() && !this->start_context_.create()) {
                    // here we may set the state back to clean, but we prevent this if the state has explicitly set to dirty before
                    start_context_.set_dirty(!dirty_state.value().clean() ? StartContext::DIRTY : StartContext::CLEAN);
                }

                if (!dirty_state.value().stopped()) {
                    start_context_.set_crashed(true);
                }
            } else {
                // dirty_state == false => An error occurred during the reading
                // when force is active, we overwrite the wrong state, otherwise this is an error
                if (!start_context_.force()) {
                    return false;
                }
            }

            // write that the system is dirty
            CHECK(this->WriteDirtyState(start_context_.file_mode(), true, false),
                "Failed to write new dirty state file");
        }
    }

    INFO("Start dedupv1d system: " << start_context_.DebugString());
    state_ = STARTING;

    INFO(dedupv1d::ReportVersion());

    if (this->monitor_config_["profile"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("profile", new dedupv1d::monitor::ProfileMonitorAdapter(this)),
            "Cannot add profile monitor");
    }
    if (this->monitor_config_["lock"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("lock", new dedupv1d::monitor::LockMonitorAdapter(this)),
            "Cannot add lock monitor");
    }
    if (this->monitor_config_["stats"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("stats", new dedupv1d::monitor::StatsMonitorAdapter(this)),
            "Cannot add stats monitor");
    }
    if (this->monitor_config_["trace"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("trace", new dedupv1d::monitor::TraceMonitorAdapter(this)),
            "Cannot add trace monitor");
    }
    if (this->monitor_config_["status"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("status", new dedupv1d::monitor::StatusMonitorAdapter(this)),
            "Cannot add status monitor");
    }
    if (this->monitor_config_["config"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("config", new dedupv1d::monitor::ConfigMonitorAdapter(this)),
            "Cannot add config monitor");
    }
    if (this->monitor_config_["log"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("log", new dedupv1d::monitor::LogMonitorAdapter(this)),
            "Cannot add log monitor");
    }
    if (this->monitor_config_["volume"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("volume", new dedupv1d::monitor::VolumeMonitorAdapter(this)),
            "Cannot add volume monitor");
    }
    if (this->monitor_config_["target"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("target", new dedupv1d::monitor::TargetMonitorAdapter(this)),
            "Cannot add target monitor");
    }
    if (this->monitor_config_["group"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("group", new dedupv1d::monitor::GroupMonitorAdapter(this)),
            "Cannot add group monitor");
    }
    if (this->monitor_config_["user"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("user", new dedupv1d::monitor::UserMonitorAdapter(this)),
            "Cannot add user monitor");
    }
    if (this->monitor_config_["version"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("version", new dedupv1d::monitor::VersionMonitorAdapter()),
            "Cannot add version monitor");
    }
    if (this->monitor_config_["inspect"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("inspect", new dedupv1d::monitor::InspectMonitorAdapter(this)), "Cannot add inspect monitor");
    }
    if (this->monitor_config_["flush"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("flush", new dedupv1d::monitor::FlushMonitorAdapter(this)), "Cannot add flush monitor");
    }
    if (this->monitor_config_["idle"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("idle", new dedupv1d::monitor::IdleMonitorAdapter(this)), "Cannot add idle monitor");
    }
    if (this->monitor_config_["gc"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("gc", new dedupv1d::monitor::GCMonitorAdapter(this)), "Cannot add gc monitor");
    }
    if (this->monitor_config_["container-gc"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("container-gc", new dedupv1d::monitor::ContainerGCMonitorAdapter(this)),
            "Cannot add container gc monitor");
    }
    if (this->monitor_config_["logging"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("logging", new dedupv1d::monitor::LoggingMonitorAdapter()), "Cannot add logging monitor");
    }
    if (this->monitor_config_["error"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("error", new dedupv1d::monitor::ErrorMonitorAdapter(this)), "Cannot add error monitor");
    }
    if (this->monitor_config_["session"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("session", new dedupv1d::monitor::SessionMonitorAdapter(this)), "Cannot add session monitor");
    }
    if (this->monitor_config_["fault-inject"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("fault-inject", new dedupv1d::monitor::FaultInjectMonitorAdapter()), "Cannot add fault-inject monitor");
    }
    if (this->monitor_config_["monitor"] == MONITOR_ENABLED) {
        CHECK(this->monitor_->Add("monitor", new dedupv1d::monitor::MonitorMonitorAdapter(this)), "Cannot add monitor monitor");
    }
    CHECK(this->monitor_->Start(start_context_), "Cannot start monitor");

    CHECK(this->persistent_stats_.Start(start_context_), "Failed to start persistent stats");

    CHECK(this->info_store_.Start(start_context_), "Failed to start info store");

    CHECK(this->dedup_system_->Start(start_context_, &info_store_, &threads_), "Cannot start dedup subsystem");

    if (this->group_info_) {
        CHECK(this->group_info_->Start(start_context_), "Cannot start group info");
    }
    if (this->target_info_) {
        CHECK(this->target_info_->Start(start_context_, volume_info_, user_info_),
            "Cannot start target info");
    }
    if (this->user_info_) {
        CHECK(this->user_info_->Start(start_context_),
            "Cannot start user info");
    }
    if (this->volume_info_) {
        CHECK(this->volume_info_->Start(start_context_, this->group_info_, this->target_info_, this->dedup_system_), "Cannot start volume info");
    }

    CHECK(RestoreStatistics(), "Failed to restore statistics");

    CHECK(this->threads_.Start(), "Cannot start threadpool");

    this->state_ = DIRTY_REPLAY;

    // if we have created a new log file, we ignore the dirty mode
    // we set the state before the replay so that in terms of the replay to log is started
    if (start_context_.dirty()) {
        if (!no_log_replay) {
            dedupv1::log::Log* log = this->dedup_system_->log();
            CHECK(log, "Log not set");

            INFO("System is dirty: Full log replay");
            CHECK(log->PerformDirtyReplay(), "Crash replay failed");
        }
    }

    // start the log replayer after the log replay
    CHECK(this->log_replayer_->Start(this->dedup_system_->log(),
            this->dedup_system_->idle_detector()),
        "Cannot start log replayer");

    // essentially here we are accepting a changed configuration
    CHECK(this->WriteDirtyState(start_context_.file_mode(), true, false), "Failed to set dirty state");
    this->state_ = STARTED;

    INFO("Started dedupv1d system");
    return true;
}

bool Dedupv1d::Run() {
    CHECK( this->state_ == STARTED, "Illegal state: " << this->state_);

    DEBUG("Running dedupv1d system");

    CHECK(this->dedup_system_->Run(), "Cannot run dedup system");

    CHECK(this->scheduler_.Start(&threads_), "Cannot start scheduler");
    CHECK(this->scheduler_.Run(), "Cannot run scheduler");

    ScheduleOptions options(this->stats_persist_interval_);
    CHECK(scheduler_.Submit("stats", options, NewCallback(this, &Dedupv1d::ScheduledPersistStatistics)),
        "Failed to submit scheduled task");

    if (this->uptime_log_interval_ > 0) {
        ScheduleOptions log_options(this->uptime_log_interval_);
        CHECK(scheduler_.Submit("stats-log", log_options, NewCallback(this, &Dedupv1d::ScheduledLogUptime)),
            "Failed to submit scheduled task");
    }

    CHECK(this->log_replayer_->Run(), "Cannot run log replayer");

    this->state_ = RUNNING;
    CHECK(this->volume_info_->Run(), "Cannot run volumes");
    return true;
}

bool Dedupv1d::Wait() {
    CHECK(this->state_ == RUNNING, "Illegal state: " << this->state_);
    CHECK(this->stop_condition_lock_.AcquireLock(), "Failed to acquire stop condition lock");

    bool failed = false;
    if (!stop_condition_.ConditionWait(&stop_condition_lock_)) {
        ERROR("Cannot wait for system stop");
        failed = true;
    }
    CHECK(this->stop_condition_lock_.ReleaseLock(), "Failed to release stop condition lock");
    return !failed;
}

Dedupv1d::~Dedupv1d() {
    list<Dedupv1dVolume*>::iterator i;
    if (state_ == RUNNING || state_ == STARTED || state_ == STARTING) {
        if (!this->Stop()) {
            WARNING("Failed to stop dedupv1d");
        }
    }
    if (this->monitor_) {
        if (this->monitor_->state() == MonitorSystem::MONITOR_STATE_STARTED) {
            if (!this->monitor_->RemoveAll()) {
                WARNING("Cannot remove all monitor adapter");
            }
        }
        delete monitor_;
        this->monitor_ = NULL;
    }
    if (this->volume_info_) {
        delete volume_info_;
        this->volume_info_ = NULL;
    }
    if (this->user_info_) {
        delete user_info_;
        this->user_info_ = NULL;
    }
    if (this->target_info_) {
        delete target_info_;
        this->target_info_ = NULL;
    }
    if (this->group_info_) {
        delete group_info_;
        this->group_info_ = NULL;
    }
    if (this->log_replayer_) {
        delete log_replayer_;
        this->log_replayer_ = NULL;
    }
    if (this->dedup_system_) {
        delete dedup_system_;
        this->dedup_system_ = NULL;
    }
    if (!this->threads_.Stop()) {
        ERROR("cannot close threadpool");
    }
    if (this->lockfile_handle_) {
        string lockfile_path = lockfile_handle_->path();
        Option<bool> exists = File::Exists(lockfile_path);
        if (exists.valid() && exists.value()) {
            if (!File::Remove(lockfile_path)) {
                WARNING("Failed to remove lockfile");
            }
        }
        delete lockfile_handle_;
        this->lockfile_handle_ = NULL;
    }
    monitor_config_.clear();
}

bool Dedupv1d::LoadOptions(const string& filename) {
    INFO("Load configuration from " << filename);

    ConfigLoader config_load(NewCallback(this, &Dedupv1d::SetOption));
    CHECK(config_load.ProcessFile(filename), "Cannot process configuration file: " << filename);
    this->config_data_ = config_load.config_data();
    return true;
}

bool Dedupv1d::SetOption(const string& option_name, const string& option) {
    CHECK(this->state_ == CREATED, "Illegal state: " << this->state_);

    if (!this->configured_) {
        this->configured_ = true;
    }
    if (option_name == "daemon.user") {
        struct passwd* pwd = getpwnam(option.c_str());
        CHECK(pwd != NULL, "User " << option << " does not exists");
        this->daemon_user_ = option;
        return true;
    }
    if (option_name == "daemon.group") {
        struct group* grp = getgrnam(option.c_str());
        CHECK(grp != NULL, "Group " << option << " does not exists");
        this->daemon_group_ = option;
        return true;
    }
    if (option_name == "daemon.lockfile") {
        CHECK(option.size() < 256, "Illegal filename (too long)");
        this->daemon_lockfile_ = option;
        return true;
    }
    if (option_name == "daemon.dirtyfile") {
        CHECK(option.size() < 256, "Illegal filename (too long)");
        this->daemon_dirtyfile_ = option;
        return true;
    }
    if (option_name == "daemon.core-dump") {
        Option<bool> d = To<bool>(option);
        CHECK(d.valid(), "Illegal core dump: " << option);
        this->dump_state_ = d.value();
        return true;
    }
    if (option_name == "daemon.memory-parachute") {
        Option<bool> b = To<bool>(option);
        if (b.valid()) {
            if (!b.value()) {
                this->memory_parachute_size_ = 0;
            }
            return true;
        }
        CHECK(ToStorageUnit(option).valid(), "Illegal value for option " << option_name << ": " << option);
        CHECK(ToStorageUnit(option).value() <= INT_MAX, "Illegal value for option " << option_name << ": " << option << " (value must be lower-equal then " << INT_MAX << ")");
        CHECK(ToStorageUnit(option).value() >= 0, "Illegal value for option " << option_name << ": " << option << " (value must be greater-equal then 0)");
        memory_parachute_size_ = ToStorageUnit(option).value();
        return true;
    }
    if (option_name == "daemon.max-memory") {
        CHECK(ToStorageUnit(option).valid(), "Illegal value for option " << option_name << ": " << option);
        CHECK(ToStorageUnit(option).value() <= LONG_MAX, "Illegal value for option " << option_name << ": " << option << " (value must be lower-equal then " << LONG_MAX << ")");
        CHECK(ToStorageUnit(option).value() >= 0, "Illegal value for option " << option_name << ": " << option << " (value must be greater-equal then 0)");
        this->max_memory = ToStorageUnit(option).value();
        return true;
    }

    // set the monitor configuration
    if (StartsWith(option_name, "monitor.")) {
        string monitor_name = option_name.substr(strlen("monitor."));
        std::map<std::string, enum monitor_config_state>::iterator i = this->monitor_config_.find(monitor_name);
        if (i != this->monitor_config_.end()) {
            CHECK(To<bool>(option).valid(), "Illegal option: " << option);
            bool monitor_state = To<bool>(option).value();
            this->monitor_config_[monitor_name] = (monitor_state ? MONITOR_ENABLED : MONITOR_DISABLED);
        } else {
            // pass to monitor system
            return this->monitor_->SetOption(monitor_name, option);
        }
        return true;
    }
    if (StartsWith(option_name, "threadpool.")) {
        return this->threads_.SetOption(option_name.substr(strlen("threadpool.")), option);
    }
    if (StartsWith(option_name, "volume-info.")) {
        return this->volume_info_->SetOption(option_name.substr(strlen("volume-info.")), option);
    }
    if (StartsWith(option_name, "target-info.")) {
        return this->target_info_->SetOption(option_name.substr(strlen("target-info.")), option);
    }
    if (StartsWith(option_name, "group-info.")) {
        return this->group_info_->SetOption(option_name.substr(strlen("group-info.")), option);
    }
    if (StartsWith(option_name, "user-info.")) {
        return this->user_info_->SetOption(option_name.substr(strlen("user-info.")), option);
    }
    if (StartsWith(option_name, "log-replay.")) {
        return this->log_replayer_->SetOption(option_name.substr(strlen("log-replay.")), option);
    }
    if (option_name == "target") {
        // for preconfigured targets, here we use the complete option name
        return this->target_info_->SetOption(option_name, option);
    }
    if (StartsWith(option_name, "target.")) {
        // for preconfigured targets, here we use the complete option name
        return this->target_info_->SetOption(option_name, option);
    }
    if (option_name == "user") {
        // for preconfigured users, here we use the complete option name
        return this->user_info_->SetOption(option_name, option);
    }
    if (StartsWith(option_name, "user.")) {
        // for preconfigured users, here we use the complete option name
        return this->user_info_->SetOption(option_name, option);
    }
    if (option_name == "group") {
        // for preconfigured groups, here we use the complete option name
        return this->group_info_->SetOption(option_name, option);
    }
    if (StartsWith(option_name, "group.")) {
        // for preconfigured groups, here we use the complete option name
        return this->group_info_->SetOption(option_name, option);
    }
    if (StartsWith(option_name, "volume.")) {
        // for preconfigured volumes, here we use the complete option name
        return this->volume_info_->SetOption(option_name, option);
    }
    if (option_name == "stats.persist-interval") {
        Option<double> d = To<double>(option);
        CHECK(d.valid(), "Illegal persist interval: " << option);
        CHECK(d.value() >= 1.0, "Illegal persist interval: " << option);
        this->stats_persist_interval_ = d.value();
        return true;
    }
    if (option_name == "update.log-interval") {
        Option<bool> b = To<bool>(option);
        if (b.valid()) {
            if (!b.value()) {
                this->uptime_log_interval_ = 0;
            }
            return true;
        }
        Option<double> d = To<double>(option);
        CHECK(d.valid(), "Illegal log interval: " << option);
        CHECK(d.value() >= 1.0, "Illegal log interval: " << option);
        this->uptime_log_interval_ = d.value();
        return true;
    }
    if (StartsWith(option_name, "stats.")) {
        return this->persistent_stats_.SetOption(option_name.substr(strlen("stats.")), option);
    }
    if (StartsWith(option_name, "info.")) {
        return this->info_store_.SetOption(option_name.substr(strlen("info.")), option);
    }
    if (option_name == "logging") {
        return true;
    }
    return this->dedup_system_->SetOption(option_name, option);
}

bool Dedupv1d::Shutdown(const dedupv1::StopContext& stop_context) {
    bool state_was_running = (this->state_ == RUNNING);
    if (state_was_running) {

        this->stop_context_ = stop_context;
        CHECK(stop_condition_.Broadcast(), "Cannot broadcast system stop");
    }
    return true;
}

bool Dedupv1d::Stop() {

    enum dedupv1d_state old_state = state_;
    this->state_ = STOPPED;
    bool failed = false;

    if (stop_context_.Get().mode() == dedupv1::StopContext::FAST) {
        INFO("Stopping dedupv1d");
    } else {
        INFO("Stopping dedupv1d (writeback mode)");
    }
    Option<bool> is_scheduled = this->scheduler_.IsScheduled("stats");
    if (is_scheduled.valid() && is_scheduled.value()) {
        if (!this->scheduler_.Remove("stats")) {
            ERROR("Error to remove scheduled stats task");
            failed = true;
        }
    }
    is_scheduled = this->scheduler_.IsScheduled("stats-log");
    if (is_scheduled.valid() && is_scheduled.value()) {
        if (!this->scheduler_.Remove("stats-log")) {
            ERROR("Error to remove scheduled stats task");
            failed = true;
        }
    }
    if (!this->scheduler_.Stop()) {
        ERROR("Cannot stop scheduler");
        failed = true;
    }
    if (this->volume_info_) {
        if (!this->volume_info_->Stop(stop_context_.Get())) {
            ERROR("Cannot stop volumes");
            failed = true;
        }
    }
    if (this->log_replayer_) {
        if (!this->log_replayer_->Stop(stop_context_.Get())) {
            ERROR("Cannot stop log replayer");
            failed = true;
        }
    }
    if (this->dedup_system_) {
        if (!this->dedup_system_->Stop(stop_context_.Get())) {
            ERROR("Cannot stop dedup subsystem");
            failed = true;
        }
    }
    if (this->monitor_) {
        if (!this->monitor_->Stop(stop_context_.Get())) {
            ERROR("Cannot stop monitor");
            failed = true;
        }
    }
    if (old_state != CREATED) {
        if (!PersistStatistics()) {
            ERROR("Failed to persist statistics");
            failed = true;
        }
    }

    if (!this->threads_.Stop()) {
        ERROR("Cannot stop threadpool");
        failed = true;
    }
    if (old_state != CREATED && old_state != STARTING) {
        // we only clean the dirty state if everything has been fine up to now
        // if we use the fast shutdown, we mark the system as dirty
        bool dirty = (stop_context_.Get().mode() == dedupv1::StopContext::FAST) || failed;
        bool stopped = !failed;
        if (!this->WriteDirtyState(start_context_.file_mode(), dirty, stopped)) {
            ERROR("Cannot set dirty state");
            failed = true;
        }
    }

    if (lockfile_handle_) {
        if (!lockfile_handle_->Unlock()) {
            WARNING("Failed to unlock lock file");
        }
        delete lockfile_handle_;
        lockfile_handle_ = NULL;
    }

    if (!dedupv1::base::memory::ClearMemoryParachute()) {
        ERROR("Failed to clear memory parachute");
        failed = true;
    }

    return !failed;
}

bool Dedupv1d::ScheduledPersistStatistics(const ScheduleContext& context) {
    if (!context.abort()) {
        return PersistStatistics();
    }
    return true;
}

bool Dedupv1d::ScheduledLogUptime(const ScheduleContext& context) {
    if (!context.abort()) {
        double uptime = (tbb::tick_count::now() - startup_tick_count_).seconds();
        INFO_LOGGER(g_stats_logger, "Uptime: " << uptime << "s");
    }
    return true;
}

bool Dedupv1d::PersistStatistics() {
    // Create Message with service time for the statistics and save it
    Dedupv1dStatsData status_data;
    status_data.set_service_time(last_service_time_ + (tbb::tick_count::now() - startup_tick_count_).seconds());
    CHECK(this->persistent_stats_.Persist("dedupv1d", status_data), "Failed to persist dedupv1d stats: " << status_data.ShortDebugString());

    if (dedup_system_) {
        CHECK(dedup_system_->PersistStatistics("dedupv1", &this->persistent_stats_),
            "Failed to persist dedup system stats");
    }
    if (volume_info_) {
        CHECK(volume_info_->PersistStatistics("volume-info", &this->persistent_stats_),
            "Failed to persist volume info stats");
    }
    return true;
}

bool Dedupv1d::RestoreStatistics() {
    Dedupv1dStatsData status_data;
    CHECK(this->persistent_stats_.Restore("dedupv1d", &status_data), "Failed to restore dedupv1d stats: " << status_data.ShortDebugString());
    if (status_data.has_service_time()) {
        this->last_service_time_ = status_data.service_time();
    } else {
        this->last_service_time_ = 0;
    }

    if (dedup_system_) {
        CHECK(dedup_system_->RestoreStatistics("dedupv1", &this->persistent_stats_),
            "Failed to restore dedup system stats");
    }
    if (volume_info_) {
        CHECK(volume_info_->RestoreStatistics("volume-info", &this->persistent_stats_),
            "Failed to restore volume info stats");
    }
    return true;
}

string Dedupv1d::PrintTrace() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"core\": " << (dedup_system_ ? this->dedup_system_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"monitor\": " << (monitor_ ? this->monitor_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"logging\": " << dedupv1::base::LoggingStatistics::GetInstance().PrintStatistics() << "," << std::endl;
    sstr << "\"volumes\": " << (volume_info_ ? this->volume_info_->PrintTrace() : "null") << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

string Dedupv1d::PrintStatistics() {
    double uptime = (tbb::tick_count::now() - startup_tick_count_).seconds();

    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);

    sstr << "{" << std::endl;
    sstr << "\"uptime\": " << uptime << "," << std::endl;
    double st = servicetime();
    if (st >= 0) {
        sstr << "\"servicetime\": " << st << "," << std::endl;
    } else {
        sstr << "\"servicetime\": null," << std::endl;
    }
    sstr << "\"core\": " << (dedup_system_ ? this->dedup_system_->PrintStatistics() : "null") << "," << std::endl;
    sstr << "\"volumes\": " << (volume_info_ ? this->volume_info_->PrintStatistics() : "null") << "," << std::endl;
    sstr << "\"volumesummary\": " << (volume_info_ ? this->volume_info_->PrintStatisticSummary() : "null") << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

string Dedupv1d::PrintProfile() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"core\": " << (dedup_system_ ? this->dedup_system_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"monitor\": " << (monitor_ ? this->monitor_->PrintProfile() : "null") << "," << std::endl;
    sstr << "\"volumes\": " << (volume_info_ ? this->volume_info_->PrintProfile() : "null") << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

string Dedupv1d::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{" << std::endl;
    sstr << "\"core\": " << (dedup_system_ ? this->dedup_system_->PrintLockStatistics() : "null") << "," << std::endl;
    sstr << "\"volumes\": " << (volume_info_ ? this->volume_info_->PrintLockStatistics() : "null") << std::endl;
    sstr << "}" << std::endl;
    return sstr.str();
}

bool Dedupv1d::ReceiveOutOfMemoryEvent() {
    // Shut the daemon down.
    ERROR("Machine is running out of memory, will shutdown now.");
    CHECK(this->Shutdown(dedupv1::StopContext::FastStopContext()), "Failed to shutdown dedupv1d (memory parachute event)");
    return true;
}

#ifdef DEDUPV1D_TEST2
void Dedupv1d::ClearData() {
    persistent_stats_.ClearData();
    log_replayer()->ClearData();
    volume_info()->ClearData();
    group_info()->ClearData();
    user_info()->ClearData();
    target_info()->ClearData();
    dedup_system()->ClearData();
    info_store()->ClearData();
}
#endif
}
