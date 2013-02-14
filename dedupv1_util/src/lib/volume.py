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

def read_volume(monitor, id):
    """ Reads a volume that is configured in dedupv1d
    """
    volume_data = monitor.read("volume")
    if not id in volume_data:
        raise Exception("Volume %s not existing" % (id))
    
    if not volume_data[id]:
        return None
    return Volume(id, volume_data[id])

def read_volume_by_name(monitor, name):
    volume_data = monitor.read("volume")
    for (id, vol) in volume_data.items():
        if not vol:
            return None
        if vol["name"] == name:
            return Volume(id, vol)
    raise Exception("Volume %s not existing" % (name))

def read_all_volumes(monitor):
    """ Reads all volumes that are configured in dedupv1d
    """
    volume_data = monitor.read("volume")
    volumes = {}
    for id in sorted(volume_data.keys()):
        data = volume_data[id]
        if not data:
            volumes[id] = None
        else:
            volumes[id] = (Volume(id, data))
    return volumes

def check_volume_name(volume_name):
    if volume_name == None:
        raise Exception("Device name not given")
    if len(volume_name) == 0:
        raise Exception("Device name is empty")
    if len(volume_name) > 48: # (tested max: 48)
        raise Exception("Device name must not have more than 48 characters")
    if not re.match(r"^[a-zA-Z0-9\.\-_]+$", volume_name):
        raise Exception("Illegal device name")
    return True
                       
class Volume:
    """ A dedupv1d volume
    """
    def __init__(self, id, options):
        self.id = id
        self.options = options
        
    def validate(self):
        errors = []
        if not self.id.isdigit():
            errors.append("id is not numeric")
        if not "name" in self.options:
            errors.append("name not given")
        
        try:
            check_volume_name(self.options["name"])
        except Exception as e:
            errors.append(e)
        
        return errors
    
    def groups(self):
        return [(g["name"],g["lun"]) for g in self.options["groups"]]
    
    def targets(self):
        return [(g["name"],g["lun"]) for g in self.options["targets"]]

    def fast_copy(self):
        return self.options["fast copy"]
    
    def session_count(self):
        return self.options.get("sessions", 0)
    
    def name(self):
        return self.options.get("name", None)
    
    def state(self):
        return self.options.get("state", None)
    
    def logical_size(self):
        return int(self.options.get("logical size", "0"))
    
    def __getitem__(self, k):
        return self.options[k]
    
    def __str__(self):
        return "Volume: %s, %s" % (self.id, self.options)
