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

from dedupv1_util import *
import unittest

class Dedupv1UtilTest(unittest.TestCase):
    
    def test_to_storage_unit(self):
        """ test_to_storage_unit
        
            Tests if string storage numbers are correctly converted into integer values
        """
        self.assertEqual(1024, to_storage_unit("1024"))
        self.assertEqual(1024, to_storage_unit("1K"))
        self.assertEqual(1024 * 1024, to_storage_unit("1048576"))
        self.assertEqual(1024 * 1024, to_storage_unit("1M"))
        self.assertEqual(1024 * 1024 * 1024, to_storage_unit("1073741824"))
        self.assertEqual(1024 * 1024 * 1024, to_storage_unit("1G"))
        self.assertEqual(1024 * 1024 * 1024 * 1024, to_storage_unit("1099511627776"))
        self.assertEqual(1024 * 1024 * 1024 * 1024, to_storage_unit("1T"))
        
    def test_format_storage_unit(self):
        """ test_format_storage_unit
        
            Tests if integer values are correctly converted (formatted) to human readable storage values
        """
        self.assertEqual("1.0K", format_storage_unit(1024))
        self.assertEqual("1.0M", format_storage_unit(1024 * 1024))
        self.assertEqual("1.0G", format_storage_unit(1024 * 1024 * 1024))
        self.assertEqual("1.0T", format_storage_unit(1024 * 1024 * 1024 * 1024))
    
if __name__ == "__main__":
    unittest.main() 