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

#include "command_handler.h"
#include <tbb/spin_rw_mutex.h>
#include <core/dedup_system.h>
#include <core/dedup_volume.h>
#include <test_util/log_assert.h>
#include <base/strutil.h>
#include "dedupv1d.h"
#include "dedupv1d_volume.h"
#include "dedupv1d_session.h"

using std::string;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::Option;
using dedupv1::scsi::ScsiResult;

namespace dedupv1d {

class CommandHandlerTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    CommandHandler* ch;
    Dedupv1dVolume* volume;
    CommandHandlerSession* chs;
    dedupv1::MemoryInfoStore info_store;

    virtual void SetUp() {
        volume = new Dedupv1dVolume(true);
        ASSERT_TRUE(volume);

        ch = new CommandHandler();
        ASSERT_TRUE(ch);

        chs = NULL;
    }

    virtual void TearDown() {
        if (chs) {
            delete chs;
            chs = NULL;
        }
        if (volume) {
            delete volume;
            volume = NULL;
        }
        if (ch) {
            delete ch;
            ch = NULL;
        }
    }
};

TEST_F(CommandHandlerTest, Create) {
}

TEST_F(CommandHandlerTest, OpcodeName) {
    ASSERT_EQ("REQUEST SENSE", CommandHandler::GetOpcodeName(0x03));
    ASSERT_EQ("WRITE (6)", CommandHandler::GetOpcodeName(0x0A));
    ASSERT_EQ("SPACE (6)", CommandHandler::GetOpcodeName(0x11));

    ASSERT_EQ("ERASE", CommandHandler::GetOpcodeName(0x19));
    ASSERT_EQ("WRITE (10)", CommandHandler::GetOpcodeName(0x2A));

    ASSERT_EQ("VERIFY (10)", CommandHandler::GetOpcodeName(0x2F));
    ASSERT_EQ("WRITE BUFFER", CommandHandler::GetOpcodeName(0x3B));
    ASSERT_EQ("LOG SELECT", CommandHandler::GetOpcodeName(0x4C));
    ASSERT_EQ("XDWRITE", CommandHandler::GetOpcodeName(0x50));
    ASSERT_EQ("PERSISTENT RESERVE OUT", CommandHandler::GetOpcodeName(0x5F));
    ASSERT_EQ("ATA COMMAND PASS THROUGH(16)", CommandHandler::GetOpcodeName(0x85));
    ASSERT_EQ("REPORT LUNS", CommandHandler::GetOpcodeName(0xA0));
    ASSERT_EQ("WRITE (12)", CommandHandler::GetOpcodeName(0xAA));
    ASSERT_EQ("READ ELEMENT STATUS", CommandHandler::GetOpcodeName(0xB4));
}

TEST_F(CommandHandlerTest, Start) {
    ASSERT_TRUE(ch->Start(volume, &info_store));

}

TEST_F(CommandHandlerTest, CreateSessionBeforeStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    chs = ch->CreateSession(0);
    ASSERT_FALSE(chs);
}

TEST_F(CommandHandlerTest, CreateSession) {
    ASSERT_TRUE(ch->Start(volume, &info_store));

    chs = ch->CreateSession(0);
    ASSERT_TRUE(chs);
    ASSERT_EQ(chs->GetCommandHandler(), ch);
    ASSERT_EQ(ch->GetSessionCount(), 1);

    delete chs;
    chs = NULL;
    ASSERT_EQ(ch->GetSessionCount(), 0);

}

#ifndef NO_SCST
TEST_F(CommandHandlerTest, UserSession) {
    ASSERT_TRUE(ch->Start(volume, &info_store));

    chs = ch->CreateSession(0);
    ASSERT_TRUE(chs);

    struct scst_user_sess scst_sess;
    memset(&scst_sess, 0, sizeof(scst_sess));
    scst_sess.sess_h = 123U;
    strcpy(scst_sess.target_name, "dedupv1");
    scst_sess.lun = 0;

    ASSERT_TRUE(chs->AttachSession(0, &scst_sess));

    ASSERT_EQ(ch->GetVolume()->session_count(), 1U);

    Option<Dedupv1dSession> dedupv1d_session = ch->GetVolume()->FindSession(123);
    ASSERT_TRUE(dedupv1d_session.valid());
    ASSERT_EQ(dedupv1d_session.value().session_id(), 123U);
    ASSERT_EQ(dedupv1d_session.value().target_name(), "dedupv1");
    ASSERT_EQ(dedupv1d_session.value().lun(), 0U);
    dedupv1d_session = NULL;

    chs->DetachSession(1, 123U);
    ASSERT_EQ(ch->GetVolume()->session_count(), 0U);
}

TEST_F(CommandHandlerTest, ReadCapacityOverflow) {
    ASSERT_TRUE(volume->SetOption("logical-size", "4T"));

    ASSERT_TRUE(ch->Start(volume, &info_store));

    chs = ch->CreateSession(0);
    ASSERT_TRUE(chs);

    byte buffer[64];
    struct scst_user_scsi_cmd_exec cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.pbuf = reinterpret_cast<uint64_t>(buffer);
    cmd.cdb_len = 10;
    cmd.bufflen = 8;
    struct scst_user_scsi_cmd_reply_exec reply;
    memset(&reply, 0, sizeof(reply));

    ASSERT_TRUE(chs->ExecuteReadCapacity(&cmd, &reply));
    ASSERT_EQ(buffer[0], 0xFF);
    ASSERT_EQ(buffer[1], 0xFF);
    ASSERT_EQ(buffer[2], 0xFF);
    ASSERT_EQ(buffer[3], 0xFF);
    ASSERT_EQ(reply.resp_data_len, 8);
}

TEST_F(CommandHandlerTest, ReadCapacity) {
    ASSERT_TRUE(volume->SetOption("logical-size", "512M"));

    ASSERT_TRUE(ch->Start(volume, &info_store));

    chs = ch->CreateSession(0);
    ASSERT_TRUE(chs);

    byte buffer[64];
    memset(buffer, 0, 64);
    struct scst_user_scsi_cmd_exec cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.pbuf = reinterpret_cast<uint64_t>(buffer);
    cmd.cdb_len = 10;
    cmd.bufflen = 8;
    struct scst_user_scsi_cmd_reply_exec reply;
    memset(&reply, 0, sizeof(reply));

    ASSERT_TRUE(chs->ExecuteReadCapacity(&cmd, &reply));
    uint32_t block_count = 0;
    block_count += (buffer[0] << 24);
    block_count += (buffer[1] << 16);
    block_count += (buffer[2] << 8);
    block_count += (buffer[3] << 0);
    block_count++;
    ASSERT_EQ(block_count, 1024 * 1024);
    ASSERT_EQ(reply.resp_data_len, 8);
}

TEST_F(CommandHandlerTest, ReadCapacity16Large) {
    ASSERT_TRUE(volume->SetOption("logical-size", "4T"));

    ASSERT_TRUE(ch->Start(volume, &info_store));

    chs = ch->CreateSession(0);
    ASSERT_TRUE(chs);

    byte buffer[64];
    struct scst_user_scsi_cmd_exec cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.pbuf = reinterpret_cast<uint64_t>(buffer);
    cmd.bufflen = 8;
    struct scst_user_scsi_cmd_reply_exec reply;
    memset(&reply, 0, sizeof(reply));

    ASSERT_TRUE(chs->ExecuteReadCapacity16(&cmd, &reply));
    uint64_t block_count = 0;
    block_count += (static_cast<uint64_t>(buffer[0]) << 56);
    block_count += (static_cast<uint64_t>(buffer[1]) << 48);
    block_count += (static_cast<uint64_t>(buffer[2]) << 40);
    block_count += (static_cast<uint64_t>(buffer[3]) << 32);
    block_count += (static_cast<uint64_t>(buffer[4]) << 24);
    block_count += (static_cast<uint64_t>(buffer[5]) << 16);
    block_count += (static_cast<uint64_t>(buffer[6]) << 8);
    block_count += (static_cast<uint64_t>(buffer[7]) << 0);
    block_count++;
    ASSERT_EQ(block_count, ToStorageUnit("4T").value() / 512);
}

TEST_F(CommandHandlerTest, UnitAttentionAfterChangeToMaintainanceMode) {
    ASSERT_TRUE(volume->SetOption("logical-size", "512M"));

    uint64_t cmd_h = 1;
    ASSERT_TRUE(ch->Start(volume, &info_store));

    chs = ch->CreateSession(0);
    ASSERT_TRUE(chs);
    struct scst_user_sess sess;
    memset(&sess, 0, sizeof(sess));
    sess.sess_h = 1;
    chs->AttachSession(++cmd_h, &sess);

    ASSERT_TRUE(volume->ChangeMaintenanceMode(true));

    byte buffer[64];
    memset(buffer, 0, 64);
    struct scst_user_scsi_cmd_exec cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.pbuf = reinterpret_cast<uint64_t>(buffer);
    cmd.bufflen = 8;
    struct scst_user_scsi_cmd_reply_exec reply;
    memset(&reply, 0, sizeof(reply));

    chs->ExecuteSCSICommand(++cmd_h, 1, &cmd, &reply);

    Option<ScsiResult> scsi_result = ScsiResult::ParseFrom(reply.status, reinterpret_cast<const byte*>(reply.psense_buffer), reply.sense_len);
    ASSERT_TRUE(scsi_result.valid());
    ASSERT_EQ(dedupv1::scsi::SCSI_CHECK_CONDITION, scsi_result.value().result());
    ASSERT_EQ(dedupv1::scsi::SCSI_KEY_UNIT_ATTENTION, scsi_result.value().sense_key());
    ASSERT_EQ(0x3F, static_cast<int>(scsi_result.value().asc()));
    ASSERT_EQ(0x0E, static_cast<int>(scsi_result.value().ascq()));

    // second command
    memset(&reply, 0, sizeof(reply));
    chs->ExecuteSCSICommand(++cmd_h, 1, &cmd, &reply);
    scsi_result = ScsiResult::ParseFrom(reply.status, reinterpret_cast<const byte*>(reply.psense_buffer), reply.sense_len);
    ASSERT_TRUE(scsi_result.valid());
    ASSERT_NE(dedupv1::scsi::SCSI_KEY_UNIT_ATTENTION, scsi_result.value().sense_key());

    chs->DetachSession(++cmd_h, 1);

    delete chs;
    chs = NULL;
}

TEST_F(CommandHandlerTest, ReadCapacity16) {
    ASSERT_TRUE(volume->SetOption("logical-size", "512M"));

    ASSERT_TRUE(ch->Start(volume, &info_store));

    chs = ch->CreateSession(0);
    ASSERT_TRUE(chs);

    byte buffer[64];
    memset(buffer, 0, 64);
    struct scst_user_scsi_cmd_exec cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.pbuf = reinterpret_cast<uint64_t>(buffer);
    cmd.bufflen = 8;
    struct scst_user_scsi_cmd_reply_exec reply;
    memset(&reply, 0, sizeof(reply));

    ASSERT_TRUE(chs->ExecuteReadCapacity16(&cmd, &reply));
    uint64_t block_count = 0;
    block_count += (static_cast<uint64_t>(buffer[0]) << 56);
    block_count += (static_cast<uint64_t>(buffer[1]) << 48);
    block_count += (static_cast<uint64_t>(buffer[2]) << 40);
    block_count += (static_cast<uint64_t>(buffer[3]) << 32);
    block_count += (static_cast<uint64_t>(buffer[4]) << 24);
    block_count += (static_cast<uint64_t>(buffer[5]) << 16);
    block_count += (static_cast<uint64_t>(buffer[6]) << 8);
    block_count += (static_cast<uint64_t>(buffer[7]) << 0);
    block_count++;
    ASSERT_EQ(block_count, 1024 * 1024);
}
#endif

}
