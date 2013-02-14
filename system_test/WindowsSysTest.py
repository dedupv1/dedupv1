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

import os
import sys
import subprocess
import simplejson
from time import sleep
import uuid
import re

class WindowsSysTest:
    """ Interface to system functions for Windows
    """
    def __init__(self, remote_run, windows_random_data_dir, local_random_data_dir):
        def get_ip_list(self):        
            import socket
            return ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")])
        
        self.remote_run = remote_run
        self.iscsi_session_id = None

        ip_list = self.get_ip_list()
        self.ip = ip_list[0] # local IP address
        
        self.disk_index = 1
        self.drive_letter = "M"
        self.random_data_dir = windows_random_data_dir
        self.local_random_data_dir = dedupv1_random_data_dir
    
    def iscsi_connect(self, target_name):
        """ connects Windows to the given iSCSI target
        """
        ip_list = self.get_ip_list()
        ip = ip_list[0]
        
        self.remote_run("iscsicli.exe AddTargetPortal %s 3260" % self.ip)
        sleep(5)
        
        self.remote_run("iscsicli.exe ListTargets")
        sleep(5)
        
        self.remote_run("iscsicli.exe listinitiators")
        sleep(5)
        
        o = self.remote_run("iscsicli.exe QLoginTarget %s" % (target_name))
        self.iscsi_session_id = re.search(r"Sitzungs-ID: (\S*)", o).group(1)
        
    def get_random_data_file(self, size):
        local_file = os.path.join(self.local_random_data_dir, "random-%s" % (size))
        
        # we cannot use os.path.join here
        remote_file = self.random_data_dir + "random-%s" % (size)
        
        if not self.remote_run.py.os.path.exists(remote_file):
            self.remote_run.upload(local_file, remote_file)
        return remote_file
    
    def iscsi_disconnect(self):
        """ disconnects the Windows iSCSI initator from the session
        """
        if not self.iscsi_session_id:
            return 
        
        self.remote_run("iscsicli.exe LogoutTarget %s" % (self.iscsi_session_id))
        
        self.remote_run("iscsicli.exe RemoveTargetPortal %s 3260" % self.ip)   
        
        sleep(5)     
                
    def format(self):
        """ partitions and formats a drive
            Note: Currently the device to format is not found dynamically.
            The drive to used, is stored in self.disk_index
        """
        part_commands = self.remote_run.write_tmp_file("""SELECT DISK %s
CLEAN
CREATE PARTITION PRIMARY
FORMAT FS=NTFS LABEL="DEDUPE" QUICK
ASSIGN LETTER=%s
""" % (self.disk_index, self.drive_letter))
        self.remote_run("diskpart /S \"%s\"" % part_commands)
        
