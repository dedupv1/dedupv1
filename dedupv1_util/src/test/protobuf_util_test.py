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

import sys
import unittest
import os

sys.path.append("../lib")

if "DEDUPV1_ROOT" not in os.environ:
    os.environ["DEDUPV1_ROOT"] = "/opt/dedupv1"
DEDUPV1_ROOT = os.environ["DEDUPV1_ROOT"]

sys.path.append(os.path.normpath(os.path.join(DEDUPV1_ROOT, "lib/python")))
for file in [f for f in os.listdir(os.path.join(DEDUPV1_ROOT, "lib/python2.6/site-packages/")) if f.endswith(".egg")]:
    sys.path.append(os.path.normpath(os.path.join(DEDUPV1_ROOT, "lib/python2.6/site-packages/", file)))
import dedupv1d_pb2
import dedupv1_pb2
import dedupv1_base_pb2
import protobuf_util

class ProtobufTest(unittest.TestCase):
    """ Unit tests for the protobuf functions from protobuf_util.py
    """ 
       
    def test_write(self):
        """ test_write
        
            This methods tests that a simple message written with write_sized_message and read with read_sized_message
            result in the same output message as the original message
        """
        data = dedupv1_base_pb2.DiskHashTransactionPageData()
        data.bucket_id = 10
        data.version = 10
        
        output = protobuf_util.write_sized_message(data)
        self.assertTrue(len(output))
        
        data2 = dedupv1_base_pb2.DiskHashTransactionPageData()
        protobuf_util.read_sized_message(data2, output)
        
        self.assertEqual(data, data2)
    
if __name__ == "__main__":
    unittest.main() 