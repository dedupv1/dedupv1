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


from copy import deepcopy
import re

def read_target(monitor, tid):
    """ reads the target with the given target id (tid)
    """
    target_data = monitor.read("target")
    if not tid in target_data:
        raise Exception("Target %s not existing" % (tid))
    
    return Target(tid, target_data[tid])

def read_target_by_name(monitor, target_name):
    """ reads the target data of the target with the given name
    """
    target_data = monitor.read("target")
    
    for (tid, target) in target_data.items():
        if target["name"] == target_name:
            return Target(tid, target)
    else:
        raise Exception("Target %s not existing" % (target_name))


def read_all_targets(monitor):
    """ reads the target data of all targets
    """
    target_data = monitor.read("target")
    targets = {}
    for tid in sorted(target_data.keys()):
        data = target_data[tid]
        targets[tid] = (Target(tid, data))
    return targets

def check_target_name(target_name):
    """ Check target name according to rfc 3222
    """
    if target_name == None:
        raise Exception("Target name not given")
    if len(target_name) == 0:
        raise Exception("Target name is empty")
    if len(target_name) > 223: # (tested max: 256) see http://tools.ietf.org/html/rfc3720#section-3.2.6.1
        raise Exception("Target name must not have more than 223 characters")
    if not re.match(r"^[a-z0-9\.\-:]+$", target_name):
        raise Exception("Illegal target name")
    return True

class Target:
    """ class that represents an iSCSI target
    """
    def __init__(self, tid, options):
        """ constructor with the tid and target data given.
            The target data is formatted as the target monitor
            output
        """
        self.tid = tid
        self.options = options
        
        if self.options == None:
            raise Exception("Target options cannot be None")
        
    def validate(self):
        """ validates if the tid and the target name are valid
        """
        errors = []
        if not self.tid.isdigit():
            errors.append("tid is not numeric")
        if not "name" in self.options:
            errors.append("name not given")
        
        try:
            check_target_name(self.options["name"])
        except Exception as e:
            errors.append(e)
            
        return errors
    
    def name(self):
        """ returns the name of the target
        """
        return self.options.get("name", None)
    
    def users(self):
        """ returns the users of the target
        """
        return self.options.get("users", [])
    
    def params(self):
        """ returns the target iSCSI parameters
        """
        pl = self.options.get("params", [])
        pl = [p.partition("=") for p in pl]
        return [(p[0], p[2]) for p in pl]
    
    def volumes(self):
        """ returns the volumes of the target as a sorted list
        """
        v = self.options.get("volumes", [])
        vm = {}
        for vol in v:
            (volume_name, lun) = vol.split(":")
            vm[lun] = volume_name
        volume_list = [vm[lun] + ":" + lun for lun in sorted(vm.keys())]
        return volume_list

    def auth_username(self):
        if not "auth" in self.options:
            return None
        return self.options["auth"].get("name", None)
    
    def auth_secret_hash(self):
        if not "auth" in self.options:
            return None
        return self.options["auth"].get("secret", None)
        
    def __getitem__(self, k):
        """ returns the target data with the given key
        """
        return self.options[k]
    
    def all(self):
        """ returns all target data options
        """
        return deepcopy(self.options)
    
    def __str__(self):
        return "Target: %s, %s" % (self.tid, self.options)
    
    def __repr__(self):
        return "Target: %s, %s" % (self.tid, self.options)
