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
import os

sys.path.append("/opt/dedupv1/lib/python")
import scst
from monitor import Monitor
from monitor import MonitorException
import scst_user
import iscsi_scst
import target
import scst

class ScstTest:
    """ Number of helper function to interact with SCST
    
        Often there are two ways to get the data.
        - via a monitor
        - directly via SCST
    """
    
    def __init__(self, hostname="localhost", port=9001):
        self.monitor = Monitor(hostname, port);

    def get_user_in_target(self, target_name):
        """ gets all user in a target.
            The data is gathered over the target monitor
        """
        t = target.read_target_by_name(self.monitor, target_name)
        if not t:
            return None
        return iscsi_scst.get_users_in_target(t)
    
    def get_targets(self):
        """ get a list of all targets.
            The data is gathered directly from SCST
        """
        return iscsi_scst.get_targets()
    
    def get_groups(self):
        """ gets all groups that are registered at SCST
		"""
        return scst.get_scst_groups()
    
    def stop_iscsi(self):
        """ stops the iSCSI daemon
        """
        return iscsi_scst.stop_iscsi()
    
    def get_pid(self):
        """ returns the pid of the daemon
            The data is collected via the status monitor.
            If the daemon is not running, None is returned
        """
        try:
            return int(self.monitor.read("status")["pid"])
        except MonitorException:
            return None
    
    def get_devices_in_group(self, group_name):
        """ returns all devices that are registered at SCST
		for a given group
		"""
        return scst.get_devices_in_group(group_name)
        