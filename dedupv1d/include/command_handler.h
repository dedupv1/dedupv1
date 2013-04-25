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
#ifndef COMMAND_HANDLER_H__
#define COMMAND_HANDLER_H__

#include <stdint.h>

#include <tbb/spin_rw_mutex.h>
#include <tbb/atomic.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/tick_count.h>

#include <gtest/gtest_prod.h>

#include <core/dedup.h>
#include <base/sliding_average.h>
#include <core/dedupv1_scsi.h>
#include <core/statistics.h>
#include <core/info_store.h>
#include "dedupv1d.pb.h"

#include "scst_handle.h"

namespace dedupv1d {

class Dedupv1dVolume;
class CommandHandlerSession;

/**
 * Holds the current state of a command handler thread
 */
class CommandHandlerThreadState {
    private:
        /**
         * Currently executed command.
         * 0 means idle.
         */
        int command_;

        /**
         * session id of the session using a command handler thread if the thread is not idle
         */
        uint64_t session_;

        /**
         * active command id if the thread is not idle
         */
        uint64_t cmd_id_;
    public:
        /**
         * Constructor
         * @return
         */
        CommandHandlerThreadState() {
            command_ = 0;
        }

        /**
         * marks a thread as idle
         */
        void clear() {
            command_ = 0;
            session_ = 0;
            cmd_id_ = 0;
        }

        /**
         * sets the command id
         */
        void set_cmd_id(uint64_t cmd_id) {
            cmd_id_ = cmd_id;
        }

        /**
         * sets the session id
         */
        void set_session(uint64_t session) {
            session_ = session;
        }

        /**
         * sets the command id
         */
        void set_command(int command) {
            command_ = command;
        }

        uint64_t cmd_id() const {
            return cmd_id_;
        }

        uint64_t session() const {
            return session_;
        }

        int command() const {
            return command_;
        }
};

/**
 * class to hold an error report
 */
class CommandErrorReport {
    public:
        /**
         * Default constructor to allow usage in containers
         */
        inline CommandErrorReport();

        /**
         * Explicit constructor
         */
        CommandErrorReport(std::time_t time, int opcode, uint64_t sector, const dedupv1::scsi::ScsiResult& result,
                const std::string& details);

        /**
         * returns the time of the error
         * Note: It contains the absolute time which may be problematic if the system clock is changed. We
         * here store the time only for documentation purposes
         */
        inline std::time_t time() const;

        /**
         * opcode of the command causing the error
         */
        inline int opcode() const;
        inline uint64_t sector() const;
        inline const dedupv1::scsi::ScsiResult& result() const;
        inline const std::string& details() const;

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool ParseFrom(const CommandErrorReportData& data);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool SerializeTo(CommandErrorReportData* data) const;
    private:
        std::time_t time_;

        int opcode_;
        uint64_t sector_;
        dedupv1::scsi::ScsiResult result_;
        std::string details_;
};

CommandErrorReport::CommandErrorReport() {
    time_ = 0;
    sector_ = 0;
    opcode_ = 0;
}

std::time_t CommandErrorReport::time() const {
    return time_;
}

int CommandErrorReport::opcode() const {
    return opcode_;
}

uint64_t CommandErrorReport::sector() const {
    return sector_;
}

const dedupv1::scsi::ScsiResult& CommandErrorReport::result() const {
    return result_;
}

const std::string& CommandErrorReport::details() const {
    return details_;
}

/**
 * The command handler is the per-volume object that handles the SCSI
 * requests and an application level (in
 * contrast to the abstract, SCST oriented, SCSTHandle).
 *
 * A command handler belongs to a volume. Each command handler thread
 * of a volume has a session belonging to the command handler.
 *
 * For a complete understanding, the lecture of the SCST User Level Specification is necessary. See
 * http://scst.sourceforge.net/scst_user_spec.txt for more information.
 *
 * \ingroup dedupv1d
 */
class CommandHandler: public dedupv1::StatisticProvider {
    public:
        static const std::string kVendorName;
        static const std::string kProductName;

        /**
         * Command handler statistics
         */
        class Statistics {
            private:
                tbb::tick_count start_tick_;

                dedupv1::base::SlidingAverage write_througput_average_;

                dedupv1::base::SlidingAverage read_througput_average_;

                tbb::spin_mutex throughput_average_lock_;
            public:
                Statistics();

                /**
                 * Number of executed SCSI commands
                 */
                tbb::atomic<uint64_t> scsi_command_count_;

                /**
                 * We use the unordered map because there updates/inserts are possible
                 * while in iterator is open.
                 */
                tbb::concurrent_unordered_map<byte, tbb::atomic<uint64_t> > scsi_command_map_;

                tbb::concurrent_unordered_map<byte, tbb::atomic<uint64_t> > scsi_task_mgmt_map_;

                tbb::concurrent_unordered_map<byte, tbb::atomic<uint64_t> > error_count_map_;

                /**
                 * Number of memory allocations
                 */
                tbb::atomic<uint64_t> memory_allocation_count_;

                tbb::atomic<uint64_t> sector_read_count_;

                tbb::atomic<uint64_t> sector_write_count_;

                tbb::atomic<uint64_t> retry_count_;

                /**
                 * Number of memory releases
                 */
                tbb::atomic<uint64_t> memory_release_count_;

                bool UpdateWrite(uint64_t size);

                bool UpdateRead(uint64_t size);

                /**
                 * Returns the average write throughput
                 * @return
                 */
                double average_write_throughput();

                /**
                 * Returns the average read throughput.
                 * @return
                 */
                double average_read_throughput();
        };

        /**
         * Constructor
         * @return
         */
        CommandHandler();

        /**
         * Destructor.
         *
         * @return
         */
        virtual ~CommandHandler();

        /**
         * Starts the command handler.
         * The volume should connect to the SCST and be able to process
         * SCSI requests.
         *
         * The started flag is set to true.
         *
         * @param volume the parent volume.
         * @param info_store info storage to use. The command handler can
         * store its state of config there.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(Dedupv1dVolume* volume, dedupv1::InfoStore* info_store);

        /**
         * Creates a command handler session for a SCSI handling thread.
         * Each thread should have its own session.
         *
         * @return
         */
        CommandHandlerSession* CreateSession(int thread_id);

        /**
         * Returns the parent volume.
         * @return
         */
        inline Dedupv1dVolume* GetVolume() const;

        /**
         * Returns if the command handler is started.
         * @return
         */
        inline bool IsStarted() const;

        /**
         * Returns the number of command handler session.s
         *
         * @return
         */
        inline int GetSessionCount() const;

        /**
         * Persists the statistics data
         * @param prefix
         * @param ps
         * @return true iff ok, otherwise an error has occurred
         */
        bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        /**
         * Restore the statistics state.
         *
         * @param prefix
         * @param ps
         * @return true iff ok, otherwise an error has occurred
         */
        bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        /**
         * Print statistics
         * @return
         */
        std::string PrintStatistics();

        /**
         * Print trace statistics
         * @return
         */
        std::string PrintTrace();

        /**
         * Returns the error reports.
         *
         * Acquires the error report lock. Therefore the method is not const.
         *
         * We cannot use a lower-case method name as the method has to acquire a lock
         * @return
         */
        inline std::list<CommandErrorReport> GetErrorReports();

        /**
         * Adds a new error report
         */
        bool AddErrorReport(int opcode, uint64_t sector, const dedupv1::scsi::ScsiResult& result);

        /**
         * Returns a human-readable name for a SCSI task management function.
         * See http://www.t10.org/ftp/t10/document.06/06-179r0.pdf for more information.
         * @param fn
         * @return
         */
        static std::string GetTaskMgmtFunctionName(uint32_t fn);

        /**
         * Returns a human printable name of a SCSI opcode.
         * @param obcode
         * @return
         */
        static std::string GetOpcodeName(byte obcode);

        /**
         * returns a mutable reference to the state of the given command handler thread
         */
        inline CommandHandlerThreadState& runner_state(int thread_id);

        /**
         * returns a mutable pointer to the statistics
         */
        inline Statistics* stats();

        /**
         * returns a mutable pointer to the command handler statistics
         */
        inline Statistics* GetMutableStatistics();

        /**
         * returns the average response time.
         */
        inline double average_response_time();

    private:

        /**
         * restores the error reports from the info store.
         * Usually called once at the start of the command handler.
         */
        bool RestoreErrorReports();

        /**
         * Each volume as exactly on command_handler and each command_handler served exactly one volume
         */
        Dedupv1dVolume* volume_;

        /**
         * Number of active sessions of the handler.
         * A session in this context is not a user session. A command
         * handler session is used for per-worker-thread data.
         */
        tbb::atomic<uint32_t> session_count_;

        /**
         * Statistics about the average response time
         */
        dedupv1::base::SimpleSlidingAverage response_time_average_;

        /**
         * Statistics about the average response time for write requests
         */
        dedupv1::base::TemplateSimpleSlidingAverage<256> response_time_write_average_;

        /**
         * Statistics
         */
        Statistics stats_;

        /**
         * Flag that denotes if the command handler is already started.
         */
        bool started;

        /**
         * map mapping from a thread id to the thread state object containing information about the states of command
         * handler threads
         */
        tbb::concurrent_unordered_map<int, CommandHandlerThreadState> runner_states_;

        /**
         * lock to protect the error reports list
         */
        tbb::spin_mutex error_report_lock_;

        /**
         * List of error reports
         * protected by error_report_lock_
         */
        std::list<CommandErrorReport> error_reports_;

        /**
         * Tick count when the error reports have been stored the last time.
         */
        tbb::tick_count error_reports_last_store_;

        int max_error_count_;

        /**
         * Info store
         */
        dedupv1::InfoStore* info_store_;

        friend class CommandHandlerSession;
        DISALLOW_COPY_AND_ASSIGN(CommandHandler);
};

CommandHandler::Statistics* CommandHandler::stats() {
    return &stats_;
}

double CommandHandler::average_response_time() {
    return response_time_average_.GetAverage();
}

CommandHandler::Statistics* CommandHandler::GetMutableStatistics() {
    return &stats_;
}

std::list<CommandErrorReport> CommandHandler::GetErrorReports() {
    tbb::spin_mutex::scoped_lock scoped_lock(error_report_lock_);
    return error_reports_;
}

CommandHandlerThreadState& CommandHandler::runner_state(int thread_id) {
    return runner_states_[thread_id];
}

Dedupv1dVolume* CommandHandler::GetVolume() const {
    return this->volume_;
}

bool CommandHandler::IsStarted() const {
    return this->started;
}

int CommandHandler::GetSessionCount() const {
    return this->session_count_;
}

/**
 * Object creates for each command handler thread.
 * Each object is only accessed by a single thread
 */
class CommandHandlerSession: public ScstCommandHandler {
    private:

        friend class CommandHandlerTest;
        FRIEND_TEST(CommandHandlerTest, ReadCapacity);
        FRIEND_TEST(CommandHandlerTest, ReadCapacityOverflow);
        FRIEND_TEST(CommandHandlerTest, ReadCapacity16);
        FRIEND_TEST(CommandHandlerTest, ReadCapacity16Large);

        /**
         * pointer to the command handler
         */
        CommandHandler* ch;

        /**
         * thread id of the command handler session (and its thread)
         */
        int thread_id_;

#ifndef NO_SCST
        /**
         * Buffer for a SCST error message.
         * This is very hacky, but a way for exception handling for the SCSI request
         */
        byte error_sense_buffer[SCST_SENSE_BUFFERSIZE];

        /**
         * Pointer to the operations structure
         */
        ScstCommandHandler* handler;

        /**
         * Sets an SCSI error to report back to the client.
         *
         * It looks like error data is transfered to a client via the REQUEST SENSE command.
         * For more information see http://en.wikipedia.org/wiki/SCSI_Request_Sense_Command
         *
         * The method provides a mapping from the internal scsi_result enumeration to the
         * SCSI sense error handling.
         *
         * @param reply
         * @param result
         * @return
         */
        bool SetScsiError(struct scst_user_scsi_cmd_reply_exec* reply, const dedupv1::scsi::ScsiResult& result);
        bool SetScsiError(struct scst_user_scsi_cmd_reply_parse* reply, const dedupv1::scsi::ScsiResult& result);

        /**
         * Extracts the offset (in bytes) from the execute-command.
         *
         * @param cmd
         * @param offset
         * @return
         */
        dedupv1::scsi::ScsiResult ExtractOffset(struct scst_user_scsi_cmd_exec* cmd, uint64_t* offset);

        /**
         * Executes a READ command by passing the read to the dedup subsystem.
         *
         * Called when a read command for the data beginning at the offset (in bytes) of the size (in bytes) should
         * be executed. The result data should be copied to the pbuf member of the command structure.
         *
         * @param cmd
         * @param offset
         * @param size
         * @return
         */
        dedupv1::scsi::ScsiResult ExecuteRead(struct scst_user_scsi_cmd_exec* cmd, uint64_t offset, uint64_t size);

        /**
         * Executes the WRITE command by passing the write to the dedup subsystem.
         *
         * Called when a write command for the data beginning at the offset (in bytes) of the size (in bytes) should
         * be executed. The result data should be copied from the pbuf member of the command structure.
         *
         * @param cmd
         * @param offset
         * @param size
         * @return
         */
        dedupv1::scsi::ScsiResult ExecuteWrite(struct scst_user_scsi_cmd_exec* cmd, uint64_t offset, uint64_t size);

        /**
         * Executes the MODESENDE command
         */
        dedupv1::scsi::ScsiResult ExecuteModeSense(struct scst_user_scsi_cmd_exec* cmd,
                struct scst_user_scsi_cmd_reply_exec* reply);

        /**
         * Reports the capacity of the volume back to the client.
         *
         * For more information see: http://en.wikipedia.org/wiki/SCSI_Read_Capacity_Command.
         *
         * @param cmd
         * @param reply
         * @return
         */
        dedupv1::scsi::ScsiResult ExecuteReadCapacity(struct scst_user_scsi_cmd_exec* cmd,
                struct scst_user_scsi_cmd_reply_exec* reply);

        dedupv1::scsi::ScsiResult ExecuteReadCapacity16(struct scst_user_scsi_cmd_exec* cmd,
                struct scst_user_scsi_cmd_reply_exec* reply);

        /**
         * Executes the SYNCRONIZE CACHE command
         */
        dedupv1::scsi::ScsiResult ExecuteSynchronizeCache(struct scst_user_scsi_cmd_exec* cmd,
                struct scst_user_scsi_cmd_reply_exec* reply);

        /**
         * Executes the VERIFY command
         */
        dedupv1::scsi::ScsiResult ExecuteVerify(struct scst_user_scsi_cmd_exec* cmd, uint64_t offset, uint64_t size);

        /**
         * Executes and inquiry command.
         *
         * For more information: see http://en.wikipedia.org/wiki/SCSI_Inquiry_Command
         *
         * @param cmd
         * @param reply
         * @return
         */
        dedupv1::scsi::ScsiResult ExecuteInquiry(struct scst_user_scsi_cmd_exec* cmd,
                struct scst_user_scsi_cmd_reply_exec* reply);
#endif
    public:

        /**
         * Constructor of a command handler session for a
         * given command handler.
         *
         * @param ch parent command handler.
         * @param thread_id id of the thread in which the session is active
         * @return
         */
        CommandHandlerSession(CommandHandler* ch, int thread_id);

        virtual ~CommandHandlerSession();

        /**
         * returns the parent command handler of the session
         * @return
         */
        inline CommandHandler* GetCommandHandler();

#ifndef NO_SCST
        /**
         * returns the error sense buffer where the
         * SCSI sense data should be stored in if an command
         * handling error occus.
         *
         * @return
         */
        inline byte* GetErrorSenseBuffer();

        /**
         * Called if a new user sessions is connected with the volume.
         * The user sessions are managed at the dedupv1d_volume level. However, the separation of concerns
         * could be a bit better here.
         *
         * The session is added to the session_list member of the volume.
         */
        virtual bool AttachSession(uint32_t cmd_h, struct scst_user_sess* sess);

        /**
         * Called if a user session is detached from the dedup system.
         * The matching dedupv1d_session object is removed from the volumes session list.
         */
        virtual void DetachSession(uint32_t cmd_h, uint64_t sess_h);

        /**
         * Called when a task management function is called.
         * Note. At the moment, I don't know that to do with it. Should be reviewed by someone with knowledge in these things.
         */
        virtual int TaskMgmt(uint32_t cmd_h, uint64_t sess_h, struct scst_user_tm* tm);

        /**
         * Parses a SCSI execution command and
         * handles the command.
         *
         * @param cmd
         * @param reply
         * @return
         */
        virtual void ExecuteSCSICommand(uint32_t cmd_h, uint64_t sess_h, struct scst_user_scsi_cmd_exec* cmd,
                struct scst_user_scsi_cmd_reply_exec* reply);

        /**
         * Called when a new memory region should be allocated and
         * given to the kernel.
         *
         * @param cmd
         * @param reply
         * @return
         */
        virtual bool AllocMem(uint32_t cmd_h, uint64_t sess_h, struct scst_user_scsi_cmd_alloc_mem* cmd,
                struct scst_user_scsi_cmd_reply_alloc_mem* reply);

        /**
         * Called when a allocated memory region is returned from the
         * kernel to the user and should be freed.
         *
         * @param cmd
         * @return
         */
        virtual bool OnFreeMemory(uint32_t cmd_h, uint64_t sess_h, struct scst_user_scsi_on_free_cmd* cmd);

        /**
         * Called when an allocated and cached memory region is returned
         * from the kernel to the user and should be freed.
         *
         * @param cmd
         * @return
         */
        virtual bool OnFreeCachedMemory(uint32_t cmd_h, uint64_t sess_h, struct scst_user_on_cached_mem_free* cmd);

        virtual void OnParse(uint32_t cmd_h, uint64_t sess_h, struct scst_user_scsi_cmd_parse* cmd,
                struct scst_user_scsi_cmd_reply_parse* reply);

#endif
};

#ifndef NO_SCST
byte* CommandHandlerSession::GetErrorSenseBuffer() {
    return this->error_sense_buffer;
}
#endif

CommandHandler* CommandHandlerSession::GetCommandHandler() {
    return this->ch;
}

}

#endif  // COMMAND_HANDLER_H__
