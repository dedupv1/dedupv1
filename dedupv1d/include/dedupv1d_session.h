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

#ifndef DEDUPV1D_SESSION_H__
#define DEDUPV1D_SESSION_H__

#include <stdint.h>

#include <string>

#include <core/dedup.h>
#include <core/dedupv1_scsi.h>
#include <tbb/concurrent_queue.h>

namespace dedupv1d {

/**
 * A dedupv1d session is a connection with a user of a LUN. This can either be a local
 * user using the scst_local module or iSCSI and FC connections.
 *
 * A session belongs to a volume.
 *
 * A session object should not be changed after creation.
 */
class Dedupv1dSession {
    private:
        /**
         * (SCST) session id of the user
         */
        uint64_t session_id_;

        /**
         * Target name of the session
         */
        std::string target_name_;

        /**
         * Name of the initiator
         */
        std::string initiator_name_;

        /**
         * Logical unit number
         */
        uint64_t lun_;

    public:
        /**
         * Constructor for a session.
         *
         * @param session_id session id of the session
         * @param target_name target name to which the session is connected
         * @param lun logical number in the target of the volume
         * @param initiator_name name of the initiator from which the session
         * is connected.
         * @return
         */
        Dedupv1dSession(uint64_t session_id,
                const std::string& target_name,
                const std::string& initiator_name,
                uint64_t lun);

        /**
         * Default constructor for a session
         */
        Dedupv1dSession();

        /**
         * returns the id of the session.
         * @return
         */
        inline uint64_t session_id() const;

        /**
         * returns the name of the target. The target
         * name is transfered from SCST
         * @return
         */
        inline const std::string& target_name() const;

        /**
         * Initiator name of the sesssion
         */
        inline const std::string& initiator_name() const;

        /**
         * returns the lun id
         * @return
         */
        inline uint64_t lun() const;

        /**
         * returns a developer-readable representation
         * of the session.
         *
         * @return
         */
        std::string DebugString() const;
};

uint64_t Dedupv1dSession::session_id() const {
    return this->session_id_;
}

const std::string& Dedupv1dSession::target_name() const {
    return this->target_name_;
}

const std::string& Dedupv1dSession::initiator_name() const {
    return this->initiator_name_;
}

uint64_t Dedupv1dSession::lun() const {
    return this->lun_;
}

}

#endif  // DEDUPV1D_SESSION_H__
