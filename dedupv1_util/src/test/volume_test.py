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
sys.path.append("../lib")

from volume import *
import unittest

class VolumeTest(unittest.TestCase):
    
    def test_check_volume_name(self):
        self.assertRaises(Exception, check_volume_name, None)
        self.assertRaises(Exception, check_volume_name, "")
        self.assertRaises(Exception, check_volume_name, "1234567890123456789012345678901234567890123456789") # 49
        self.assertRaises(Exception, check_volume_name, u"Vol\u00f6ume")
        self.assertRaises(Exception, check_volume_name, u"Vol+ume")
        self.assertRaises(Exception, check_volume_name, u"Vol$ume")
        self.assertRaises(Exception, check_volume_name, u"Vol@ume")
        self.assertRaises(Exception, check_volume_name, "Vol ume")
        self.assertRaises(Exception, check_volume_name, "Vol:ume")
        self.assertTrue(check_volume_name("a"))
        self.assertTrue(check_volume_name("This.is_my-2nd-Volume"))
        self.assertTrue(check_volume_name("123456789012345678901234567890123456789012345678")) # 48

if __name__ == "__main__":
    unittest.main() 