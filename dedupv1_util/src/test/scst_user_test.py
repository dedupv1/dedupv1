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

from scst_user import *
import unittest

class UserTest(unittest.TestCase):
    
    def test_check_user_name(self):
        self.assertRaises(Exception, check_user_name, None)
        self.assertRaises(Exception, check_user_name, "")
        self.assertRaises(Exception, check_user_name, "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123") # 513
        self.assertRaises(Exception, check_user_name, "Default_User")
        self.assertRaises(Exception, check_user_name, u"U\u00f6ser")
        self.assertRaises(Exception, check_user_name, u"U+ser")
        self.assertRaises(Exception, check_user_name, u"U$ser")
        self.assertRaises(Exception, check_user_name, u"U@ser")
        self.assertRaises(Exception, check_user_name, "U ser")
        self.assertTrue(check_user_name("a"))
        self.assertTrue(check_user_name("This.is_my-2nd:User"))
        self.assertTrue(check_user_name("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012")) # 512

if __name__ == "__main__":
    unittest.main() 