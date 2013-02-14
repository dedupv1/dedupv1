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

#ifndef SCST_HANDLE_H__
#define SCST_HANDLE_H__

#include <core/dedup.h>
#include <base/config.h>

#ifndef NO_SCST
#include <scst_user.h>
#endif

#include <string>

#include <core/dedupv1_scsi.h>

namespace dedupv1d {

#ifdef NO_SCST
/**
 * Version of the scst command handler to allow
 * the compilation of the tests on Mac.
 *
 * \ingroup dedupv1d
 */
class ScstCommandHandler {
};
#else

/**
 * Handler for commands received from SCST.
 *
 * \ingroup dedupv1d
 */
class ScstCommandHandler {
    public:
        /**
         * Called when a new session is attached
         * @param sess
         * @return
         */
        virtual bool AttachSession(uint32_t cmd_h, struct scst_user_sess* sess);

        /**
         * Called when a session is detached
         * @param sess_h
         */
        virtual void DetachSession(uint32_t cmd_h, uint64_t sess_h);

        /**
         * Called for a SCSI task management function
         * @param tm
         * @return
         */
        virtual int TaskMgmt(uint32_t cmd_h, uint64_t sess_h, struct scst_user_tm* tm);

        /**
         * Called when a SCSI command should be executed
         * @param cmd
         * @param reply
         */
        virtual void ExecuteSCSICommand(uint32_t cmd_h, uint64_t sess_h,
                struct scst_user_scsi_cmd_exec* cmd, struct scst_user_scsi_cmd_reply_exec* reply) = 0;

        /**
         * Called when a new memory block should be allocated
         * @param cmd
         * @param reply
         * @return
         */
        virtual bool AllocMem(uint32_t cmd_h, uint64_t sess_h, struct scst_user_scsi_cmd_alloc_mem* cmd, struct scst_user_scsi_cmd_reply_alloc_mem* reply)  = 0;

        /**
         * Called when a memory block should be freed.
         *
         * @param cmd
         * @return
         */
        virtual bool OnFreeMemory(uint32_t cmd_h, uint64_t sess_h, struct scst_user_scsi_on_free_cmd* cmd) = 0;

        /**
         * Called when a cached memory block should be freed.
         *
         * @param cmd
         * @return
         */
        virtual bool OnFreeCachedMemory(uint32_t cmd_h, uint64_t sess_h, struct scst_user_on_cached_mem_free* cmd) = 0;

        /**
         * Called when a SCSI command data block should be parsed by the command handler.
         *
         * @param cmd
         * @param reply
         */
        virtual void OnParse(uint32_t cmd_h, uint64_t sess_h, struct scst_user_scsi_cmd_parse* cmd, struct scst_user_scsi_cmd_reply_parse* reply) = 0;
};
#endif

/**
 * \ingroup dedupv1d
 * The SCST handle encapsulates most of the communication with the SCST through the /dev/scst_user interface.
 */
class ScstHandle {
    private:
        DISALLOW_COPY_AND_ASSIGN(ScstHandle);
    public:
        /**
         * Type for the state of the SCST handle
         */
        enum scst_handle_state {
            SCST_HANDLE_STATE_CREATED,//!< SCST_HANDLE_STATE_CREATED
            SCST_HANDLE_STATE_STARTED,//!< SCST_HANDLE_STATE_STARTED
            SCST_HANDLE_STATE_STOPPED //!< SCST_HANDLE_STATE_STOPPED
        };
    private:

        /**
         * State of the SCST handle
         */
        enum scst_handle_state state_;

        /**
         * Flag if the handle is registered at SCST
         */
        bool registered_;

#ifndef NO_SCST
        /**
         * file identifier of /dev/scst_user (or whatever scst_user_filename is set to)
         */
        int file;
#endif
        /**
         * Filename of the scst_user file. Normally always set to /dev/scst_user
         */
        std::string scst_user_filename_;

        /**
         * Name of the device
         */
        std::string device_name_;
    private:
        /**
         * Registers the SCSI device of this handle at the SCSI.
         * The block size if the SCSI block size to use, often 4K.
         *
         * @param block_size
         * @return
         */
        bool Register(uint32_t block_size);

        /**
         * Unregisters a SCST handle
         * @return
         */
        bool Unregister();

        /**
         * Returns a human-readable name of the SCST subcode given.
         *
         * @param subcode
         * @return
         */
        static std::string GetSubcommandName(uint32_t subcode);

        /**
         * Handles and SCST command.
         *
         * @param handler
         * @param cmd
         * @param response
         * @return
         */
        bool HandleCommand(ScstCommandHandler* handler, struct scst_user_get_cmd* cmd, struct scst_user_reply_cmd* response);
    public:
        /**
         * Constructor
         * @return
         */
        ScstHandle();

        /**
         * Destructor
         * @return
         */
        ~ScstHandle();

        /**
         * Configures the SCST handle
         *
         * Available options:
         * - device-name: String
         *
         * @param option_name
         * @param option
         * @return
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Starst the SCST handle
         * @param block_size
         * @return
         */
        bool Start(uint32_t block_size);

        /**
         * Restart is the same as start, but the /dev/scst_user file is not opened.
         *
         * @param block_size
         * @return
         */
        bool Restart(uint32_t block_size);

        /**
         * Stops the SCST handle
         * @return
         */
        bool Stop();

        bool HandleProcessCommand(ScstCommandHandler* handler);

        /**
         * returns the (possibly autogenerated) device name
         * @return
         */
        std::string device_name() const;

        /**
         * returns the state of the SCST handle
         * @return
         */
        enum scst_handle_state state() const;

        bool NotifyDeviceCapacityChanged();

#ifdef DEDUPV1_TEST
        /**
         * Method for use only in test cases.
         */
        bool ClearData();
#endif

        /**
         * Returns a developer-readable representation of the SCST handle
         * @return
         */
        std::string DebugString() const;

        inline bool is_registered() const {
            return registered_;
        }
};

}

#endif  // SCST_HANDLE_H__
