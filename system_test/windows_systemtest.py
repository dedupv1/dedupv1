#!/usr/bin/python
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

import unittest
import os
import sys
import glob
from Dedupv1Test import Dedupv1Test
from ScstTest import ScstTest
from SysTest import SysTest
from DataTest import DataTest
from time import sleep
import optparse
from md5sum import md5
from run import Run#
from remote_run import WindowsRemoteRun
import shutil
from systemtest import perform_systemtest
from systemtest import Dedupv1dBaseSystemTest
from systemtest import print_json
import re
import tempfile
from OpeniSCSITest import OpeniSCSITest
from WindowsSysTest import WindowsSysTest
import re

class Dedupv1WindowsSystemTest(Dedupv1dBaseSystemTest):
    """ Systems tests that checks the interaction of dedupv1d/SCST
		an Microsoft Windows and its iSCSI initiator.
	"""
    def setUp(self):        
        super(Dedupv1WindowsSystemTest, self).setUp()
        
        windows_host_ip = self.configuration_value("windows host ip")
        windows_random_data_dir = self.configuration_value("windows random data dir", "C:\\testdata\\")
        local_random_data_dir = os.path.join("test data dir")
        self.remote_run = WindowsRemoteRun(windows_host_ip)
        self.win_sys = WindowsSysTest(self.remote_run)
        
    def tearDown(self):        
        self.win_sys.iscsi_disconnect()
        
        super(Dedupv1WindowsSystemTest, self).tearDown()

    def test_connect(self):
        """ test_connect
        """
        self.start_default_system()
        
        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special") 
        self.assertExitcode(0)
        
        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0") 
        self.assertExitcode(0)
        
        self.win_sys.iscsi_connect("iqn.2005-03.info.christmann:backup:special")

    def test_format(self):
        """ test_format
        """
        self.start_default_system()
        
        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special") 
        self.assertExitcode(0)
        
        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0") 
        self.assertExitcode(0)
        
        self.win_sys.iscsi_connect("iqn.2005-03.info.christmann:backup:special")

        self.assertFalse(self.remote_run.py.os.path.exists("%s:\\" % self.win_sys.drive_letter))
        
        self.win_sys.format()
        
        self.assertTrue(self.remote_run.py.os.path.exists("%s:\\" % self.win_sys.drive_letter))
        
    def test_copy_random_data(self, size=1024):
        """ test_copy_random_data
        """
        self.start_default_system()
        
        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special") 
        self.assertExitcode(0)
        
        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0") 
        self.assertExitcode(0)
        
        self.win_sys.iscsi_connect("iqn.2005-03.info.christmann:backup:special")
        
        self.win_sys.format()    
        
        # Generate random data
        remote_file = self.win_sys.get_random_data_file(size)
        
        self.remote_run("xcopy %s %s" % (remote_file, "%s:\\" % self.win_sys.drive_letter))
        
if __name__ == "__main__":
    perform_systemtest([Dedupv1WindowsSystemTest])
