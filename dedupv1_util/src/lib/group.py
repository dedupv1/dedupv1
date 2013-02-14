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

import re

def read_group(monitor, name):
    """
    reads group data with the given group name from the
    dedupv1d
    """
    group_data = monitor.read("group")
    if not name in group_data:
        raise Exception("Group %s not existing" % (name))
    
    return Group(name, group_data[name])

def read_all_groups(monitor):
    """
    reads all group data from  the dedupv1d
    """
    group_data = monitor.read("group")
    if "ERROR" in group_data:
        raise Exception("Failed to get group list")

    groups = {}
    for name in sorted(group_data.keys()):
        data = group_data[name]
        groups[name] = (Group(name, data))
    return groups

def check_group_name(group_name):
    if group_name == None:
        raise Exception("Group name not given")
    if len(group_name) == 0:
        raise Exception("Group name is empty")
    if len(group_name) > 512: # (tested max: 1014)
        raise Exception("Group name must not have more than 512 characters")
    if group_name.find("Default") >= 0:
        raise Exception("Illegal group name (\"Default\" may not be used)")
    if not re.match(r"^[a-zA-Z0-9\.\-:_]+$", group_name):
        raise Exception("Illegal group name")
    return True

class Group:
    """ Class to represent a SCST security group
    """
    
    def __init__(self, name, options):
        """ inits the group with a name and a dictionary of options.
        The options have the same format as the output of the group monitor
        """
        self.name = name
        self.options = options
        
    def validate(self):
        """ validates the the group configuration is valid, e.g. if the name
            is ok
        """
        errors = []
        
        try:
            check_group_name(self.name)
        except Exception as e:
            errors.append(e)
        
        return errors
    
    def initiator_pattern(self):
        """ returns a list of all initiator pattern """
        return self.options.get("initiators", [])
    
    def volumes(self):
        """ returns a sorted list of volumes """
        v = self.options.get("volumes", [])
        vm = {}
        for vol in v:
            (volume_name, lun) = vol.split(":")
            vm[lun] = volume_name
        return [vm[lun] + ":" + lun for lun in sorted(vm.keys())]
    
    def __str__(self):
        """ returns a string representation """
        return "Group: %s, %s" % (self.name, self.options)
