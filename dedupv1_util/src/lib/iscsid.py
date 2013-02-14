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


from command import execute_cmd
import time

def discover(server):
    """ frontend function to open-iscsi and its discovery function """
    return execute_cmd("iscsiadm -m discovery -t sendtargets -p %s" % server)

def get_device_list():
    """ returns a list of all block devices.
        The list does not only contain the iSCSI devices, but really all
    """
    return [l.strip() for l in execute_cmd("ls /dev/disk/by-path")]

def connect(server, name):
    """ connects to a given iSCSI target
    """
    execute_cmd("iscsiadm -m node -p %s -T %s -l"
            % (server, name))
    
    time.sleep(3)

    devs = filter(lambda d: d.find(server) >= 0 and d.find(name) >= 0, get_device_list())
    devs_msg = ["New Device: " + d + "\n" for d in devs]
    print "".join(devs_msg)

def disconnect(server, name):
    """ disconnects from a given iSCSI target
    """
    execute_cmd("iscsiadm -m node -p %s -T %s -u"
            % (server, name))