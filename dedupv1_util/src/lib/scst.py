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
import re
from command import execute_cmd
from time import sleep
import pwd
import grp
import sys
import traceback

class ScstException(Exception):
    """ base class for all SCST related exceptions
    """
    def __init__(self, message, base = None):
        Exception.__init__(self)
        self.msg = message
        self.base = base
        
    def __str__(self):
        if self.base == None:
            return self.msg
        else:
            return self.msg + "\n\tCause: " + self.base.__str__()
    
def check_file_access(path, username):
    """ checks if a given user can access the given path
    
        the check relies on the user id and the group memberships
        of the user of the stat data of the file. Extended ACL
        are not checked
    """
    path_stat = os.stat(path)
    uid = path_stat.st_uid
    gid = path_stat.st_gid
    
    pw = pwd.getpwna(username)
    if not pw:
        raise Exception("username doesn't exists")
    if uid == pw.pw_uid: # uid correct
        return True
    
    gr = grp.getgrgid(gid)
    if username in gr.gr_mem:
        return True
    return False

def check_file_group_access(path, check_gid):
    """ checks if a given group (by group id) can access the 
        path
    """
    st = os.stat(path)
    uid = st.st_uid
    gid = st.st_gid
    
    if gid == check_gid:
        return True
    return False
    
def kernel_module_loaded(module_name):
    """ checks if a given module is loaded into the kernel """
    return len(filter(lambda line: line.find(module_name) >= 0, 
                      file("/proc/modules").readlines())) > 0

def validate_scst(group_name):
    """ validates the current state of the SCST and throws
        an exception if the state is not valid
    """
    group = None
    if group_name:
        group = grp.getgrnam(group_name)
        if not group:
            raise ScstException("Group missing")
    if not kernel_module_loaded("scst"):
        raise ScstException("scst kernel module not loaded")
    if not kernel_module_loaded("scst_user"):
        raise ScstException("scst_user kernel module not loaded")
    if not os.path.exists("/dev/scst_user"):
        raise ScstException("SCST User target module (scst_user) is not started")
    else:
        if group and not check_file_group_access("/dev/scst_user", group.gr_gid):
            raise ScstException("Device /dev/scst_user has wrong permissions")
    if not os.path.exists("/proc/scsi_tgt/scsi_tgt"):
        raise ScstException("SCST is not loaded")
    if group and not check_file_group_access("/proc/scsi_tgt/scsi_tgt", group.gr_gid):
        raise ScstException("File /proc/scsi_tgt/scsi_tgt has wrong permissions")

def check_scst(group_name):
    """ checks the current state of SCST and tries to correct it
        This function is usually called with root permissions
    """
    if not kernel_module_loaded("scst"):
        if group_name:
            group_gid = grp.getgrnam(group_name).gr_gid
            execute_cmd("modprobe scst proc_gid=%s" % str(group_gid))
            sleep(1)
        else:
            execute_cmd("modprobe scst")
            sleep(1)
    if not kernel_module_loaded("scst_user"):
        execute_cmd("modprobe scst_user")
        sleep(1)
    if not os.path.exists("/dev/scst_user"):
        raise ScstException("SCST User target module (scst_user) is not started")
    else:
        if group_name:
            group = grp.getgrnam(group_name)
            if not check_file_group_access("/dev/scst_user", group.gr_gid):
                execute_cmd("chgrp %s /dev/scst_user" % group_name)
                execute_cmd("chmod g+rw /dev/scst_user")

def get_scst_groups():
    """ returns a list of all scst groups (local)
    """
    data = os.listdir("/proc/scsi_tgt/groups")
    return data

def exists_device(volume_name):
    device_path = "/proc/scsi_tgt/scsi_tgt"
    data = open(device_path, "r").read()
    data = [line for line in data.split("\n")][1:]
    
    pattern = re.compile(r"(\S*)\s*(\S*)")
    for line in data:
        p = pattern.match(line)
        if p and p.groups()[0] == p.groups()[1]:
            device_name = p.groups()[0]
            if device_name == volume_name:
                return True
    return False;

def get_devices():
    """ returns a list of all SCST devices (local)
    """
    device_path = "/proc/scsi_tgt/scsi_tgt"
    data = open(device_path, "r").read()
    data = [line for line in data.split("\n")][1:]
    
    pattern = re.compile(r"(\S*)\s*(\S*)")
    devices = []
    for line in data:
        p = pattern.match(line)
        if p and p.groups()[0] == p.groups()[1]:
            device_name = p.groups()[0]
            if len(device_name):
                devices.append(device_name)
    return devices

def get_luns_in_group(group_name):
    try:
        group_path = os.path.join("/proc/scsi_tgt/groups", group_name)
        if not os.path.exists(group_path):
            raise ScstException("Group doesn't exists: " + group_name)
        
        devices_path = os.path.join(group_path, "devices")
        data = open(devices_path, "r").read()
        data = [line for line in data.split("\n")][1:]
        
        luns = []
        for line in data:
            p = re.match(r"(\S*)\s*(\S*)\s*(\S*)", line)
            if p:
                lun = p.groups(2)[0]
                if len(lun):
                    luns.append(lun)
        return luns
    except Exception as e:
        raise ScstException("Failed to get luns in group %s" % (group_name), e)    

def get_devices_in_group(group_name):
    """ returns a list of all SCST devices in a given group
    """
    try:
        group_path = os.path.join("/proc/scsi_tgt/groups", group_name)
        if not os.path.exists(group_path):
            raise ScstException("Group doesn't exists: " + group_name)
        
        devices_path = os.path.join(group_path, "devices")
        data = open(devices_path, "r").read()
        data = [line for line in data.split("\n")][1:]
        
        devices = []
        for line in data:
            p = re.match(r"(\S*)\s*(\S*)\s*(\S*)", line)
            if p:
                device_name = p.groups(1)[0]
                if len(device_name):
                    devices.append(device_name)
        return devices
    except Exception as e:
        raise ScstException("Failed to get devices in group %s" % (group_name), e)
    
def add_initiator_pattern_to_group(initiator_name, group_name):
    """ adds a initiator pattern to a group """
    try:
        tgt_file = open(os.path.join("/proc/scsi_tgt/groups", group_name, "names"), "w")
        tgt_file.write("add %s" % (initiator_name))    
        tgt_file.close()
    except Exception as e:
        raise ScstException("Failed to add initiator %s to group %s" % (initiator_name, group_name), e)

def rm_initiator_pattern_from_group(initiator_name, group_name):
    """ removes a initiator pattern from a group """
    try:
        tgt_file = open(os.path.join("/proc/scsi_tgt/groups", group_name, "names"), "w")
        tgt_file.write("del %s" % (initiator_name))    
        tgt_file.close()
    except Exception as e:
        raise ScstException("Failed to remove initiator %s from group %s" % (initiator_name, group_name), e)

def get_initiator_pattern_in_group(group_name):
    """ gets all initiator pattern from a group """
    try:
        tgt_file = open(os.path.join("/proc/scsi_tgt/groups", group_name, "names"), "r")
        group_data = tgt_file.read()
        groups = filter(lambda g: len(g) > 0, group_data.split("\n"))    
        tgt_file.close()
        return groups
    except Exception as e:
        raise ScstException("Failed to get initiator pattern from group %s" % (group_name), e)    

def exists_group(group_name):
    """ checks if a group with the given name is registered at SCST
    """
    try:
        return os.path.exists(os.path.join("/proc/scsi_tgt/groups", group_name))
    except Exception as e:
        raise ScstException("Failed to check if group %s exists" % (group_name), e)    

def get_group_session(group_name):
    try:
        sess_file = open("/proc/scsi_tgt/sessions", "r")
        lines = [line for line in sess_file][1:]
        sess = []
        for line in lines:
            p = re.match(r"(\S*)\s*(\S*)\s*(\S*)\s*(\S*)", line)
            if p:
                if group_name == p.groups()[2]:
                    sess.append({"target": p.groups()[0], "initiator": p.groups()[1]})
        return sess
    except Exception as e:
        raise ScstException("Failed to get session of group %s" % group_name, e)

def rm_group(group_name):    
    """ removes a group """
    try:
        if not exists_group(group_name):
            raise Exception("Group %s missing" % group_name)
        
        if len(get_group_session(group_name)) > 0:
            raise Exception("Cannot remove group with active session")
        
        tgt_file = open("/proc/scsi_tgt/scsi_tgt", "w")
        tgt_file.write("del_group %s" % (group_name))
        tgt_file.close()
    except Exception as e:
        raise ScstException("Failed to remove group %s" % group_name, e)
    
def add_group(group_name):
    """ adds a group to SCST """
    try:
        tgt_file = open("/proc/scsi_tgt/scsi_tgt", "w")
        tgt_file.write("add_group %s" % (group_name))    
        tgt_file.close()
    except Exception as e:
        raise ScstException("Failed to add group %s" % group_name, e)
    
def register_volume(vol):
    """ registers a given volume.
    
        The volume should already be registered by dedupv1d, we
        here assign it to all groups and targets it should be assigned to.
        
        The vol is a Volume object
    """
    try:        
        for i in xrange(5):
            if exists_device(vol.name()):
                break
            sleep(1) # volume is not yet available.
            # We have seen this as a timing issue
        else:
            # volume is not available after 5 seconds, something is wrong
            raise Exception("Volume %s missing" % vol.name())
                
        for (group_name, lun) in vol.groups():
            add_to_group(vol, group_name + ":" + lun)
            
        for (target_name, lun) in vol.targets():
            group_name = "Default_" + target_name  
            if not group_name in get_scst_groups():
                add_group(group_name)
                
            add_to_group(vol, group_name + ":" + lun)
    except Exception as e:
        raise ScstException("Failed to register volume %s" % (vol.name()), e)      
      
def add_to_group(vol, group):
    """ adds the given volume to the group
    
        vol is a Volume object
        group is a string containing a group/lun pair
    """
    try:
        p = group.rpartition(":")
        (group_name, lun) = (p[0], p[2])
        group_devices_filename = os.path.join("/proc/scsi_tgt/groups", group_name, "devices")
        if not os.path.exists(group_devices_filename):
            raise Exception("Group file " + group_devices_filename + " missing")
        
        if not exists_device(vol.name()):
            raise Exception("Volume %s missing" % vol.name())
        
        if vol.name() in get_devices_in_group(group_name):
            raise Exception("Volume %s is already in group %s" % (vol.name(), group_name))
        
        if lun in get_luns_in_group(group_name):
            raise Exception("LUN %s already assigned in group %s" % (lun, group_name))
        
        c = "add %s %s" % (vol.name(), lun)
        devices_group_file = open(group_devices_filename, "w")
        devices_group_file.write(c)
        devices_group_file.close()
    except Exception as e:
        raise ScstException("Failed to add volume %s to group %s, lun %s" % (vol.name(), group_name, lun), e)

def rm_from_group(volume_name, group_name):
    """ removes the volume from the group

		volume_name: string that contains the name of the volume to remove from the group
		group_name: string that contains the name of the group the volume should be removed from
    """
    try:        
        group_devices_filename = os.path.join("/proc/scsi_tgt/groups", group_name, "devices")
        if not os.path.exists(group_devices_filename):
            raise Exception("Group file " + group_devices_filename + " missing")

        devices_group_file = open(group_devices_filename, "w")
        devices_group_file.write("del %s" % (volume_name))
    except Exception as e:
        raise ScstException("Failed to remove volume %s from group %s" % (volume_name, group_name), e) 
    
if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1:
        print get_scst_groups()
    elif sys.argv[1] == "groups":
        print get_scst_groups()
    elif sys.argv[1] == "devices":
        print get_devices()