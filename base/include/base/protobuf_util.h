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
#ifndef PROTOBUF_UTIL_H__
#define PROTOBUF_UTIL_H__

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/message.h>

#include <base/base.h>
#include <base/option.h>

#include <string>

namespace dedupv1 {
namespace base {

#ifdef LOGGING_LOG4CXX

class ProtobufLogHandler {
    public:
        static void Log(::google::protobuf::LogLevel level, const char* filename, int line, const std::string& message);
        static void SetLog4CxxHandler();
};

#endif

/**
 *
 * @param message
 * @param target
 * @param checksum
 * @return true iff ok, otherwise an error has occurred
 */
bool SerializeSizedMessageCachedToString(const ::google::protobuf::Message& message, std::string* target, bool checksum);

/**
 * @return true iff ok, otherwise an error has occurred
 */
bool SerializeSizedMessageCachedToString(const ::google::protobuf::Message& message, bytestring* target, bool checksum);

/**
 *
 * @param message
 * @param target
 * @param checksum
 * @return true iff ok, otherwise an error has occurred
 */
bool SerializeSizedMessageToString(const ::google::protobuf::Message& message, std::string* target, bool checksum);

/**
 * @return true iff ok, otherwise an error has occurred
 */
bool SerializeSizedMessageToString(const ::google::protobuf::Message& message, bytestring* target, bool checksum);

/**
 * Tests if two messages are equal.
 *
 * Two messages are equal iff the are from the same type and are serialized to the same data.
 *
 * @return true iff the two messages are considered equal.
 */
bool MessageEquals(const ::google::protobuf::Message& m1, const ::google::protobuf::Message& m2);

/**
 * Serializes a message to a bytestring.
 * The method is similar to Message::SerializeToString, but was a bytestring as target.
 *
 * @return true iff ok, otherwise an error has occurred
 */
bool SerializeMessageToString(const ::google::protobuf::Message& message, bytestring* target);

/**
 * Serialize a protobuf message with an int prefix that gives the size of the message.
 *
 * @param message
 * @param value
 * @param max_value_size
 * @return -1 in case of error
 */
Option<size_t> SerializeSizedMessageCached(const ::google::protobuf::Message& message, void* value, size_t max_value_size, bool checksum);

/**
 * Serialize a protobuf message with an int prefix that gives the size of the message.
 *
 * @param message
 * @param value
 * @param max_value_size
 * @return -1 in case of error
 */
Option<size_t> SerializeSizedMessage(const ::google::protobuf::Message& message, void* value, size_t max_value_size, bool checksum);

/**
 * parses a protobuf message with an int prefix that gives the size of the message.
 *
 * @param message
 * @param value
 * @param max_value_size
 * @return -1 in case of error
 */
Option<size_t> ParseSizedMessage(::google::protobuf::Message* message, const void* value, size_t max_value_size, bool checksum);

}
}

#endif  // PROTOBUF_UTIL_H__
