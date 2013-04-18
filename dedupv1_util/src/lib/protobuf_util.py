#
# dedupv1 - iSCSI based Deduplication System for Linux
#
# (C) 2008 Dirk Meister
# (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
# (C) 2012 Dirk Meister, Johannes Gutenberg University Mainz
#
# This file is part of dedupv1.
#
# dedupv1 is free software: you can redistribute it and/or modify it under the terms of the
# GNU General Public License as published by the Free Software Foundation, either version 3
# of the License, or (at your option) any later version.
#
# dedupv1 is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
# even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with dedupv1. If not, see http://www.gnu.org/licenses/.
#

from google.protobuf.internal import decoder
from google.protobuf.internal import encoder
import binascii

def read_sized_message(message, raw_data):
    """ Python port of dedupv1::base::ReadSizedMessage

        A current limitation is that the crc value is not checked
    """
    message.ParseFromString(get_message_contents(raw_data))

def write_sized_message(message, crc = True):
    """ serializes a protobuf message and prefixes it with the length of the message and
        suffixes it with an optional crc checksum over the length and the messager

        It is a python port of dedupv1::base::WriteSizedMessage
    """

    output = []
    variant_encoder = encoder._VarintEncoder()

    # there we need a method that when it is called writes the bytes. using the append method of a list results in appending
    # the bytes to the list
    variant_encoder(output.append, message.ByteSize())
    output.append(message.SerializeToString())

    if (crc):
        crc_value = binascii.crc32(output[0]) # crc the size
        crc_value = binascii.crc32(output[1], crc_value) # crc the contents based on the size crc. This is equal to crc both at one step
        crc_value = crc_value & 0xffffffff # make it unsigned
        variant_encoder(output.append, crc_value)
    else:
        variant_encoder(output.append, 0)

    return "".join(output)

def get_message_contents(raw_data):
    """ returns the contents of a sized message as byte string.

        The function is similar to read_sized_message except, but we do not parse the message data

        A current limitation is that the crc value is not checked
    """
    (data_size, data_size_len) = decoder._DecodeVarint32(raw_data, 0)
    return raw_data[data_size_len:data_size_len+data_size]
