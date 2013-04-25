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

#ifndef DEDUPV1D_H__
#define DEDUPV1D_H__

#include <stdint.h>
#include <core/dedup.h>
#include <base/locks.h>
#include <base/barrier.h>
#include <base/memory_new_handler.h>
#include <base/threadpool.h>
#include <base/scheduler.h>
#include <base/protected.h>
#include <core/dedup_system.h>

#include <gtest/gtest_prod.h>

#include <tbb/tick_count.h>
#include <list>
#include <string>
#include <map>

#include "dedupv1d.pb.h"

#include "monitor.h"

namespace dedupv1d {

class LogReplayer;
class Dedupv1dVolume;
class Dedupv1dVolumeInfo;
class Dedupv1dTarget;
class Dedupv1dTargetInfo;
class Dedupv1dGroup;
class Dedupv1dGroupInfo;
class Dedupv1dUser;
class Dedupv1dUserInfo;

/**
 * \defgroup dedupv1d SCST/Daemon-based interface to the deduplication system.
 */

/**
 * \ingroup dedupv1d
 *
 * Main class for the dedupv1 daemon and all related problems.
 */
class Dedupv1d : public dedupv1::base::memory::NewHandlerListener, public dedupv1::StatisticProvider {
    private:
    DISALLOW_COPY_AND_ASSIGN(Dedupv1d);
    FRIEND_TEST(Dedupv1dTest, DirtyFlagAfterCrash);

    /**
     * Default size of the thread pool
     */
    static const uint32_t kDefaultThreadpoolSize;

    public:

    /**
     * enumeration of the state of the dedupv1d
     */
    enum dedupv1d_state {
        CREATED,//!< DEDUPV1_SCSI_STATE_CREATED
        STARTING,
        DIRTY_REPLAY,
        STARTED,//!< DEDUPV1_SCSI_STATE_STARTED
        RUNNING,//!< DEDUPV1_SCSI_STATE_RUNNING
        STOPPED //!< DEDUPV1_SCSI_STATE_STOPPED
    };

    /**
     * configuration states of different monitors.
     */
    enum monitor_config_state {
        MONITOR_ENABLED, //!< MONITOR_ENABLED
        MONITOR_DISABLED,//!< MONITOR_DISABLED
        MONITOR_FORBIDDEN//!< MONITOR_FORBIDDEN
    };
    private:
    /**
     * Threadpool to execute tasks, e.g. command handling threads
     */
    dedupv1::base::Threadpool threads_;

    /**
     * Scheduler for the scheduled execution of tasks.
     */
    dedupv1::base::Scheduler scheduler_;

    /**
     * Pointer to the deduplication subsystem
     **/
    dedupv1::DedupSystem* dedup_system_;

    /**
     * Pointer to the monitor system.
     */
    dedupv1d::monitor::MonitorSystem* monitor_;

    /**
     * Pointer to the volume info
     */
    Dedupv1dVolumeInfo* volume_info_;

    /**
     * Pointer to the target info
     */
    Dedupv1dTargetInfo* target_info_;

    /**
     * Pointer to the group info
     */
    Dedupv1dGroupInfo* group_info_;

    /**
     * Pointer to the user info
     */
    Dedupv1dUserInfo* user_info_;

    /**
     * Application state
     */
    volatile enum dedupv1d_state state_;

    /**
     * Condition used during the stop of the daemon
     */
    dedupv1::base::Condition stop_condition_;

    /**
     * Lock that should be held when waiting for the stop condition
     */
    dedupv1::base::MutexLock stop_condition_lock_;

    /**
     * Currently configured volume.
     * Should not be set after the start.
     */
    Dedupv1dVolume* current_config_volume_;

    /**
     * flag denoting if the profile monitor is enabled
     */
    std::map<std::string, enum monitor_config_state> monitor_config_;

    /**
     * name of the user the dedupv1d daemon should run in.
     * If not set, the parent user (calling user) is used.
     */
    std::string daemon_user_;

    /**
     * name of the group the dedupv1d daemon should run in.
     * If not set, the parent group is used.
     */
    std::string daemon_group_;

    /**
     * Name of the lockfile of the daemon.
     * Usually /opt/dedupv1/var/lock/dedupv1d.
     * scripts.
     */
    std::string daemon_lockfile_;

    /**
     * Handle to the lock file.
     */
    dedupv1::base::File* lockfile_handle_;

    /**
     * Name of the dirty file of the daemon.
     * Usually /opt/dedupv1/var/lib/dedupv1/dirty
     *
     * We cannot use the lockfile for the dirty determination as
     * the lock file is automatically deleted by the OS during a restart.
     */
    std::string daemon_dirtyfile_;

    /**
     * Set to NULL, but set in load_options function is instance is configured
     * using a file;
     * */
    std::string config_data_;

    /**
     * Reference to the log replayer.
     */
    LogReplayer* log_replayer_;

    /**
     * true iff at least a single option is configured
     */
    bool configured_;

    dedupv1::StartContext start_context_;

    dedupv1::base::Protected<dedupv1::StopContext> stop_context_;

    dedupv1::IndexPersistentStatistics persistent_stats_;

    dedupv1::IndexInfoStore info_store_;

    /**
     * Interval in seconds after that the statistics of the system
     * should be persisted.
     */
    double stats_persist_interval_;

    /**
     * Interval in seconds after that the uptime of the system
     * are logged to the log file.
     */
    double uptime_log_interval_;

    /**
     * State if the possibility for core dumps of the dedupv1 should
     * be prohibited or enforced.
     *
     * 0 - unset: no change
     * 1 - true: dedupv1d should be core dumpable
     * 2 - false: dedupv1 should'nt be core dumpable
     */
    int dump_state_;

    /**
     * Size of the memory parachute in bytes
     */
    int memory_parachute_size_;

    /**
     * Maximum size this process may get. 0 means do not change.
     *
     * The type long is choosed, because it is also the type used in setrlimit.
     */
    long max_memory;

    /**
     * Time the now running system was started.
     */
    tbb::tick_count startup_tick_count_;

    /**
     * Whole runtime of the system without the actual run.
     */
    double last_service_time_;


    /**
     * Reads the dirty state from the dirty file.
     * If the first read files, the system tries the backup file, but marks it dirty anyway.
     */
    dedupv1::base::Option<DirtyFileData> CheckDirtyState();

    /**
     * Writes the dirty state to the dirty file.
     * Copies a temporary backup to overcome crashes during a call of this method.
     *
     * The file ownership is important in this method. All created files
     * should be assigned to the daemon group (dedupv1) and be read- and writeable
     * by all members of the group.
     */
    bool WriteDirtyState(const dedupv1::FileMode& file_mode, bool dirty, bool stopped);

    bool ScheduledPersistStatistics(const dedupv1::base::ScheduleContext& context);
    bool ScheduledLogUptime(const dedupv1::base::ScheduleContext& context);

    public:
    /**
     * Constructor
     * @return
     */
    Dedupv1d();

    ~Dedupv1d();

    /**
     *
     * One check that is performed by this method, it that the configuration has not changed. The check is
     * not performed when the start is forced (forced set to true in the start context). However, in general
     * changes are not supported and might or might not be valid and might or might not be detected.
     *
     * @param preliminary_start_context the start context is only preliminary has the system doesn't
     * know all values yet, e.g. the dirty state. If the dirty state is set to true, we force a dirty state. If the
     * dirty state is set to clean, the dedupv1d checks it state and might set it to dirty.
     *
     * @param no_log_replay The log is not replayed even if the system is dirty. So the system might be in an
     * inconsistent state. This option should not be used for the normal processing, but it is necessary in some
     * situations, e.g. in dedupv1_debug where there should be no log replay.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool Start(const dedupv1::StartContext& preliminary_start_context, bool no_log_replay = false);

    /**
     * Loads the options from the given file
     * @param filename
     * @return true iff ok, otherwise an error has occurred
     */
    bool LoadOptions(const std::string& filename);

    /**
     *
     * Available options:
     * - daemon.user: String
     * - daemon.group: String
     * - daemon.lockfile: String
     * - daemon.dirtyfile: String
     * - daemon.core-dump: Boolean
     * - daemon.memory-parachute: false or StorageUnit
     * - daemon.max-memory: StorageUnit
     * - monitor.*: String
     * - threadpool.*
     * - volume-info.*
     * - target-info.*
     * - group-info.*
     * - user-info.*
     * - log-replay.*
     * - target: String
     * - target.*: String
     * - user: String
     * - user.*: String
     * - group: String
     * - group.*: String
     * - volume.*: String
     * - stats.persist-interval: Double
     * - stats.log-interval (is depreciated)
     * - update.log-interval: Double
     * - stats.*
     * - info.*
     * - core-dump (is depreciated. Use daemon.core-dump instead)
     * - logging
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool Run();

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool Stop();

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool Wait();

    /**
     * Set lock_file for this process. Needed to build deamon.
     *
     * The object takes the responsibilty for the File object. It will be closed (and destroyed).
     *
     * @param lock_file The lock file
     */
    bool AttachLockfile(dedupv1::base::File* lock_file);

    bool OpenLockfile();

    /**
     * Start the shutdown of the system. The method is returning after the shutdown
     * completed.
     *
     * TODO (dmeister): This is usually called the Stop method. The naming convention got here
     * confused.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool Shutdown(const dedupv1::StopContext& stop_context);

    /**
     * returns the content of the configuration file
     * @return
     */
    inline std::string config_data() const;

    /**
     * returns the state of dedupv1d
     * @return
     */
    inline enum dedupv1d_state state() const;

    /**
     * returns the log replayer.
     * @return
     */
    inline LogReplayer* log_replayer();

    /**
     * returns the dedup engine.
     * @return
     */
    inline dedupv1::DedupSystem* dedup_system();

    /**
     * returns the volume info
     * @return
     */
    inline Dedupv1dVolumeInfo* volume_info();

    /**
     * returns the target info
     * @return
     */
    inline Dedupv1dTargetInfo* target_info();

    /**
     * returns the group info
     * @return
     */
    inline Dedupv1dGroupInfo* group_info();

    /**
     * returns the user info
     * @return
     */
    inline Dedupv1dUserInfo* user_info();

    /**
     * returns the monitor system.
     * @return
     */
    inline dedupv1d::monitor::MonitorSystem* monitor();

    inline dedupv1::InfoStore* info_store();

    /**
     * returns the username in which the
     * daemon should run in.
     * @return
     */
    inline const std::string& daemon_user() const;

    inline const std::string& daemon_group() const;

    /**
     * returns the lockfile of the daemon
     * @return
     */
    inline const std::string& daemon_lockfile() const;

    /**
     * returns the filename of the dirty state file of the daemon
     */
    inline const std::string& daemon_dirtyfile() const;

    /**
     * returns a reference to the start context.
     */
    inline const dedupv1::StartContext& start_context() const;

    /**
     * returns the threadpool to used by the daemon
     */
    inline dedupv1::base::Threadpool* threadpool();

    virtual bool PersistStatistics();

    virtual bool RestoreStatistics();

    std::string PrintTrace();

    /**
     * prints statistics about dedupv1d
     * @return
     */
    std::string PrintStatistics();

    /**
     * prints profile informations
     * @return
     */
    std::string PrintProfile();

    /**
     * prints lock usage and contention informations.
     * @return
     */
    std::string PrintLockStatistics();

    /**
     * prints version informations about dedupv1d.
     * @return
     */
    std::string ReportVersion();

    /**
     * Called if the new handler is called meaning that the system is
     * out of memory. Inherited from NewHandlerListener.
     */
    bool ReceiveOutOfMemoryEvent();

    inline int dump_state() const;

    /**
     * Get the uptime of the system (seconds since last restart)
     */
   inline double uptime() const;

   /**
    * Get the servicetime of the system (sum of all uptimes of dedupv1d on this machine)
    */
   inline double servicetime() const;

   /**
    * Returns a copy of the stop context.
    */
   inline dedupv1::StopContext GetStopContext();

#ifdef DEDUPV1D_TEST
    void ClearData();
#endif
};

dedupv1::StopContext Dedupv1d::GetStopContext() {
    return stop_context_.Get();
}

inline double Dedupv1d::uptime() const {
    return (tbb::tick_count::now() - startup_tick_count_).seconds();
}

inline double Dedupv1d::servicetime() const {
    if (last_service_time_ >= 0)
        return last_service_time_ + uptime();
    return -1.0;
}

inline int Dedupv1d::dump_state() const {
    return dump_state_;
}

std::string Dedupv1d::config_data() const {
    return this->config_data_;
}

Dedupv1d::dedupv1d_state Dedupv1d::state() const {
    return this->state_;
}

LogReplayer* Dedupv1d::log_replayer() {
    return this->log_replayer_;
}

dedupv1::DedupSystem* Dedupv1d::dedup_system() {
    return this->dedup_system_;
}

Dedupv1dVolumeInfo* Dedupv1d::volume_info() {
    return this->volume_info_;
}

Dedupv1dTargetInfo* Dedupv1d::target_info() {
    return this->target_info_;
}

Dedupv1dGroupInfo* Dedupv1d::group_info() {
    return this->group_info_;
}

Dedupv1dUserInfo* Dedupv1d::user_info() {
    return this->user_info_;
}

dedupv1d::monitor::MonitorSystem* Dedupv1d::monitor() {
    return this->monitor_;
}

const std::string& Dedupv1d::daemon_user() const {
    return this->daemon_user_;
}

const std::string& Dedupv1d::daemon_group() const {
    return this->daemon_group_;
}

const std::string& Dedupv1d::daemon_lockfile() const {
    return this->daemon_lockfile_;
}

const std::string& Dedupv1d::daemon_dirtyfile() const {
    return this->daemon_dirtyfile_;
}

const dedupv1::StartContext& Dedupv1d::start_context() const {
    return this->start_context_;
}

dedupv1::InfoStore* Dedupv1d::info_store() {
    return &this->info_store_;
}

dedupv1::base::Threadpool* Dedupv1d::threadpool() {
    return &this->threads_;
}

}

#endif  // DEDUPV1D_H__
