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
#include <core/dedupv1_scsi.h>
#include <base/logging.h>

using std::string;
using dedupv1::base::Option;
using dedupv1::base::make_option;

namespace dedupv1 {
namespace scsi {

LOGGER("SCSI");

const ScsiResult ScsiResult::kOk;
const ScsiResult ScsiResult::kDefaultNotReady(SCSI_CHECK_CONDITION, SCSI_KEY_NOT_READY, 0x04, 0x00);
const ScsiResult ScsiResult::kNotReadyStandby(SCSI_CHECK_CONDITION, SCSI_KEY_NOT_READY, 0x04, 0x0B);
const ScsiResult ScsiResult::kNotReadyManualIntervention(SCSI_CHECK_CONDITION, SCSI_KEY_NOT_READY, 0x04, 0x03);
const ScsiResult ScsiResult::kIllegalMessage(SCSI_CHECK_CONDITION, SCSI_KEY_ILLEGAL_REQUEST, 0x24, 0x00);
const ScsiResult ScsiResult::kReadError(SCSI_CHECK_CONDITION, SCSI_KEY_MEDIUM_ERROR, 0x11, 0x00);
const ScsiResult ScsiResult::kWriteError(SCSI_CHECK_CONDITION, SCSI_KEY_MEDIUM_ERROR, 0x03, 0x00);

ScsiResult::ScsiResult() {
    result_ = SCSI_OK;
    sense_key_ = SCSI_KEY_OK;
    asc_ = 0;
    ascq_ = 0;
}


ScsiResult::ScsiResult(enum scsi_result result, enum scsi_sense_key sense_key, byte asc, byte ascq) {
    result_ = result;
    sense_key_ = sense_key;
    asc_ = asc;
    ascq_ = ascq;
}

std::string ScsiResult::DebugString() const {
    std::stringstream sstr;
    sstr << "[status " << result_ <<
    ", sense key 0x" << std::hex << sense_key_ <<
    ", asc 0x" << std::hex << static_cast<int>(asc_) <<
    ", ascq 0x" << std::hex << static_cast<int>(ascq_) << "]";
    return sstr.str();
}

Option<ScsiResult> ScsiResult::ParseFrom(uint8_t status, const byte* sense_buffer, size_t sense_len) {
    DCHECK(sense_buffer, "Sense buffer not set");
    DCHECK(sense_len >= 14, "Illegal max sense len");
    CHECK(sense_buffer[0] == 0x70, "Unsupported sense format");
    CHECK(sense_buffer[7] == 0x0a, "Unsupported sense format");

    ScsiResult r = ScsiResult(static_cast<scsi_result>(status), static_cast<scsi_sense_key>(sense_buffer[2]), sense_buffer[12], sense_buffer[13]);
    return make_option(r);
}

Option<size_t> ScsiResult::SerializeTo(byte* sense_buffer, size_t max_sense_len) const {
    DCHECK(sense_buffer, "Sense buffer not set");
    DCHECK(max_sense_len >= 18, "Illegal max sense len: " << max_sense_len);

    memset(sense_buffer, 0, 18);
    sense_buffer[0] = 0x70; // Current errors
    sense_buffer[2] = sense_key();
    sense_buffer[7] = 0x0a; // 10 bytes
    sense_buffer[12] = asc();
    sense_buffer[13] = ascq();

    return make_option(18LU);
}

}
}
