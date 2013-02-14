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

#ifndef DEDUPV1_SCSI_H__
#define DEDUPV1_SCSI_H__

#include <core/dedup.h>

#include <base/option.h>
#include <string>

namespace dedupv1 {
namespace scsi {

/**
 * SCSI result
 */
enum scsi_result {
    SCSI_OK = 0x00,              //!< SCSI_OK

    SCSI_CHECK_CONDITION  = 0x02,//!< SCSI_CHECK_CONDITION

    SCSI_BUSY = 0x08             //!< SCSI_BUSY
};

/**
 * SCSI sense key
 */
enum scsi_sense_key {
    SCSI_KEY_OK = 0x00,             //!< SCSI_KEY_OK

    SCSI_KEY_RECOVERD = 0x01,       //!< SCSI_KEY_RECOVERD

    SCSI_KEY_NOT_READY = 0x02,      //!< SCSI_KEY_NOT_READY

    SCSI_KEY_MEDIUM_ERROR = 0x03,   //!< SCSI_KEY_MEDIUM_ERROR

    SCSI_KEY_HARDWARE_ERROR = 0x04, //!< SCSI_KEY_HARDWARE_ERROR

    SCSI_KEY_ILLEGAL_REQUEST = 0x05,//!< SCSI_KEY_ILLEGAL_REQUEST

    SCSI_KEY_UNIT_ATTENTION = 0x06, //!< SCSI_KEY_UNIT_ATTENTION

    SCSI_KEY_DATA_PROTECTED = 0x07, //!< SCSI_KEY_DATA_PROTECTED

    SCSI_KEY_BLANK_CHECK = 0x08,    //!< SCSI_KEY_BLANK_CHECK

    SCSI_KEY_VENDOR_SPECIFIC = 0x09,//!< SCSI_KEY_VENDOR_SPECIFIC

    SCSI_KEY_COPY_ABORTED = 0x0A,   //!< SCSI_KEY_COPY_ABORTED

    SCSI_KEY_ABORTED_COMMAND = 0x0B,//!< SCSI_KEY_ABORTED_COMMAND

    // 0x0C is obsolete

    SCSI_KEY_VOLUME_OVERFLOW = 0x0D,//!< SCSI_KEY_VOLUME_OVERFLOW

    SCSI_KEY_MISCOMPARE = 0x0E,     //!< SCSI_KEY_MISCOMPARE

    // 0x0F is reserved
};



/**
 * Result codes for SCSI or SCSI-like requests.
 *
 * Note: If a new code is added, the methods GetSCSIResultName and bool CommandHandlerSession::SetScsiError
 * have to be modified.
 */
class ScsiResult {
    private:
        enum scsi_result result_;
        enum scsi_sense_key sense_key_;
        byte asc_;
        byte ascq_;
    public:
        ScsiResult();
        ScsiResult(enum scsi_result result, enum scsi_sense_key, byte asc, byte ascq);

        inline operator bool () const; //conversion operator

        inline enum scsi_result result() const;
        inline enum scsi_sense_key sense_key() const;
        inline byte asc() const;
        inline byte ascq() const;

        std::string DebugString() const;

        static dedupv1::base::Option<ScsiResult> ParseFrom(uint8_t status, const byte* sense_buffer, size_t sense_len);

        dedupv1::base::Option<size_t> SerializeTo(byte* sense_buffer, size_t max_sense_len) const;

        static const ScsiResult kOk;
        static const ScsiResult kDefaultNotReady;
        static const ScsiResult kNotReadyStandby;
        static const ScsiResult kNotReadyManualIntervention;
        static const ScsiResult kIllegalMessage;

        static const ScsiResult kReadError;
        static const ScsiResult kWriteError;
};

ScsiResult::operator bool() const {
    return (result_ == SCSI_OK ||
            (result_ == SCSI_CHECK_CONDITION && sense_key_ == SCSI_KEY_UNIT_ATTENTION) ||
            (result_ == SCSI_CHECK_CONDITION && sense_key_ == SCSI_KEY_NOT_READY));
}

enum scsi_result ScsiResult::result() const {
    return result_;
}

enum scsi_sense_key ScsiResult::sense_key() const {
    return sense_key_;
}

byte ScsiResult::asc() const {
    return asc_;
}

byte ScsiResult::ascq() const {
    return ascq_;
}

// specify IDS for some SCSI command that have not been declared before

#ifndef VERIFY_16
#define VERIFY_16             0x8f
#endif
#ifndef VERIFY_10
#define VERIFY_10	      0x2f
#endif
#ifndef READ_CAPACITY_16
#define READ_CAPACITY_16 0x9e
#endif
#ifndef SYNCHRONIZE_CACHE
#define SYNCHRONIZE_CACHE 0x35
#endif
#ifndef SYNCHRONIZE_CACHE_16
#define SYNCHRONIZE_CACHE_16 0x91
#endif

}
}

#endif  // DEDUPV1_SCSI_H__
