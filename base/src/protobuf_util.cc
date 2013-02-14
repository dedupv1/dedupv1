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
#include <base/base.h>
#include <base/protobuf_util.h>
#include <base/logging.h>
#include <base/crc32.h>
#include <base/strutil.h>

#include <string>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

using std::string;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::strutil::ToString;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::CodedInputStream;

LOGGER("Protobuf");

namespace dedupv1 {
namespace base {

#ifdef LOGGING_LOG4CXX

void ProtobufLogHandler::Log(::google::protobuf::LogLevel level, const char* filename, int line, const std::string& message) {
    if (level == google::protobuf::LOGLEVEL_INFO && logger->isInfoEnabled()) {
        log4cxx::spi::LocationInfo location(filename, "", line);
        logger->info(message, location);
    } else if ((level == google::protobuf::LOGLEVEL_FATAL || level == google::protobuf::LOGLEVEL_DFATAL) && logger->isFatalEnabled()) {
        log4cxx::spi::LocationInfo location(filename, "", line);
        logger->fatal(message, location);
    } else if (level == google::protobuf::LOGLEVEL_ERROR && logger->isErrorEnabled()) {
        log4cxx::spi::LocationInfo location(filename, "", line);
        logger->error(message, location);
    } else if (level == google::protobuf::LOGLEVEL_WARNING && logger->isWarnEnabled()) {
        log4cxx::spi::LocationInfo location(filename, "", line);
        logger->warn(message, location);
    }
}

void ProtobufLogHandler::SetLog4CxxHandler() {
    google::protobuf::SetLogHandler(&ProtobufLogHandler::Log);
}

#endif

bool SerializeSizedMessageCachedToString(const ::google::protobuf::Message& message, bytestring* target, bool checksum) {
    // Note: Any games do use ParseFromArray directly to avoid the construction of the CodedInputStream are
    // useless, because ParseFromArray internally constructs an CodedInputStream
    // Another note: The size of the target is wrong in this method as protobuf acquires a larger buffer
    // when the stream leaves the scope, the length is corrected.

    DCHECK(target, "Target not set");
    DCHECK(message.IsInitialized(), "Message not initialized: " << message.InitializationErrorString());

    string target2;
    if (!SerializeSizedMessageCachedToString(message, &target2, checksum)) {
        return false;
    }
    target->assign(reinterpret_cast<const byte*>(target2.data()), target2.size());
    return true;
}

bool SerializeSizedMessageCachedToString(const ::google::protobuf::Message& message, std::string* target, bool checksum) {
    // Note: Any games do use ParseFromArray directly to avoid the construction of the CodedInputStream are
    // useless, because ParseFromArray internally constructs an CodedInputStream
    // Another note: The size of the target is wrong in this method as protobuf acquires a larger buffer
    // when the stream leaves the scope, the length is corrected.

    DCHECK(target, "Target not set");
    DCHECK(message.IsInitialized(), "Message not initialized: " << message.InitializationErrorString());

    ::google::protobuf::io::StringOutputStream arrayout(target);
    ::google::protobuf::io::CodedOutputStream output(&arrayout);

    output.WriteVarint32(message.GetCachedSize());
    message.SerializeWithCachedSizes(&output);

    uint32_t size_without_crc = output.ByteCount();
    uint32_t checksum_value = 0;
    if (checksum) {
        CRC crc;
        crc.Update(target->data(), size_without_crc);
        checksum_value = crc.GetRawValue();
    }
    output.WriteVarint32(checksum_value);
    CHECK(!output.HadError(), "Output has error: " << message.ShortDebugString());

    TRACE("Serialized message: " << (message.ByteSize() < 256 ? message.ShortDebugString() : message.ShortDebugString().substr(0, 256)) <<
        ", message size " << message.ByteSize() <<
        ", crc " << checksum_value <<
        ", size without crc " << size_without_crc);
    return true;
}

Option<size_t> SerializeSizedMessageCached(const ::google::protobuf::Message& message, void* value, size_t max_value_size, bool checksum) {
    // Note: Any games do use ParseFromArray directly to avoid the construction of the CodedInputStream are
    // useless, because ParseFromArray internally constructs an CodedInputStream

    DCHECK(message.IsInitialized(), "Message not initialized: " << message.InitializationErrorString());
    ::google::protobuf::io::ArrayOutputStream arrayout(value, max_value_size);
    ::google::protobuf::io::CodedOutputStream output(&arrayout);

    output.WriteVarint32(message.GetCachedSize());

    message.SerializeWithCachedSizes(&output);

    uint32_t size_without_crc = output.ByteCount();
    uint32_t checksum_value = 0;
    if (checksum) {
        CRC crc;
        crc.Update(value, size_without_crc);
        checksum_value = crc.GetRawValue();
    }
    output.WriteVarint32(checksum_value);

    TRACE("Serialized message: " << (message.ByteSize() < 256 ? message.ShortDebugString() : message.ShortDebugString().substr(0, 256)) <<
        ", message size " << message.ByteSize() <<
        ", crc " << checksum_value <<
        ", size without crc: " << size_without_crc);
    CHECK(output.ByteCount() <= max_value_size, "Illegal write: " <<
        message.ShortDebugString() <<
        ", total size " << output.ByteCount() <<
        ", available size " << max_value_size);
    CHECK(!output.HadError(), "Output has error: "
        "message " << (message.ByteSize() < 256 ? message.ShortDebugString() : message.ShortDebugString().substr(0, 256)) <<
        ", message byte count " << message.ByteSize() <<
        ", total byte count " << output.ByteCount() <<
        ", max value size " << max_value_size);
    return make_option(static_cast<size_t>(output.ByteCount()));
}

bool MessageEquals(const ::google::protobuf::Message& m1, const ::google::protobuf::Message& m2) {
    if (m1.GetTypeName() != m2.GetTypeName()) {
        return false;
    }
    if (m1.ByteSize() != m2.ByteSize()) {
        return false;
    }
    if (m1.SerializeAsString() != m2.SerializeAsString()) {
        return false;
    }
    return true;
}

bool SerializeMessageToString(const ::google::protobuf::Message& message, bytestring* target) {
    DCHECK(target, "Target not set");

    target->clear();
    target->resize(message.ByteSize());
    CHECK(message.SerializeToArray(const_cast<byte*>(&(*target->begin())), target->size()),
        "Failed to serialize message. " << message.ShortDebugString());
    return true;
}

bool SerializeSizedMessageToString(const ::google::protobuf::Message& message, std::string* target, bool checksum) {
    // Note: Any games to use ParseFromArray directly to avoid the construction of the CodedInputStream are
    // useless, because ParseFromArray internally constructs an CodedInputStream
    message.ByteSize();
    return SerializeSizedMessageCachedToString(message, target, checksum);
}

bool SerializeSizedMessageToString(const ::google::protobuf::Message& message, bytestring* target, bool checksum) {
    // Note: Any games to use ParseFromArray directly to avoid the construction of the CodedInputStream are
    // useless, because ParseFromArray internally constructs an CodedInputStream
    message.ByteSize();
    return SerializeSizedMessageCachedToString(message, target, checksum);
}

Option<size_t> SerializeSizedMessage(const ::google::protobuf::Message& message, void* value, size_t max_value_size, bool checksum) {
    // Note: Any games to use ParseFromArray directly to avoid the construction of the CodedInputStream are
    // useless, because ParseFromArray internally constructs an CodedInputStream
    message.ByteSize();
    return SerializeSizedMessageCached(message, value, max_value_size, checksum);
}

Option<size_t> ParseSizedMessage(::google::protobuf::Message* message, const void* value, size_t max_value_size, bool checksum) {
    DCHECK(message, "Message not set");
    // Note: Any games to use ParseFromArray directly to avoid the construction of the CodedInputStream are
    // useless, because ParseFromArray internally constructs an CodedInputStream

    ::google::protobuf::io::CodedInputStream input((byte*) value, max_value_size);
    input.PushLimit(max_value_size);

    uint32_t data_size = 0;
    CHECK(input.ReadVarint32(&data_size), "Cannot read size data: max value size: " << max_value_size);
    
    ::google::protobuf::io::CodedInputStream::Limit data_limit = input.PushLimit(data_size);
    CHECK(message->ParseFromCodedStream(&input), "Failed to parse data: " <<
        "data " << (max_value_size < 256 ? ToHexString(value, max_value_size) :  ToHexString(value, 256) + "...") <<
        ", data size " << max_value_size <<
        ", message " << message->InitializationErrorString());
    input.PopLimit(data_limit);

    uint32_t current_read = max_value_size - input.BytesUntilLimit();
    uint32_t stored_checksum = 0;
    CHECK(input.ReadVarint32(&stored_checksum),
        "Cannot read stored checksum: " <<
        "data " << (max_value_size < 256 ? ToHexString(value, max_value_size) :  ToHexString(value, 256) + "...") <<
        ", data size " << max_value_size <<
        ",  message " << (message->ByteSize() < 256 ? message->ShortDebugString() : message->ShortDebugString().substr(0, 256)));

    TRACE("Found message: " << (message->ByteSize() < 256 ? message->ShortDebugString() : message->ShortDebugString().substr(0, 256)) <<
        ", message size " << data_size <<
        ", stored crc " << stored_checksum <<
        ", current read bytes " << current_read);

    if (checksum && stored_checksum != 0) {
        CRC crc;
        crc.Update(value, current_read);
        uint32_t calc_checksum = crc.GetRawValue();
        CHECK(stored_checksum == calc_checksum, "Message checksum mismatch: " <<
            "message " << (message->ByteSize() < 256 ? message->ShortDebugString() : message->ShortDebugString().substr(0, 256)) <<
            ", data " << (max_value_size < 256 ? ToHexString(value, max_value_size) :  ToHexString(value, 256) + "...") <<
            ", data size " << max_value_size <<
            ", message size " << data_size <<
            ", processed bytes " << (max_value_size - input.BytesUntilLimit()) <<
            ", stored checksum " << stored_checksum <<
            ", calculated checksum " << calc_checksum);
    }
    return make_option(max_value_size - input.BytesUntilLimit());
}

}
}
