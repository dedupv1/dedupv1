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
import config

class Config:
    """ Holds and parses the deduplication configuration file
    """
    def __init__(self, filename):
        """ reads the given config file
        """
        self.config = self.read_config_file(filename)
        self.defaults = {
                         "daemon.lockfile": os.path.join(config.DEDUPV1_ROOT, "var/lock/dedupv1d"),
                         "daemon.dirtyfile": os.path.join(config.DEDUPV1_ROOT, "var/lib/dedupv1/dirty"),
                         "logging": os.path.join(config.DEDUPV1_ROOT, "etc/dedupv1/logging.xml")
        }
        
    def read_config_file(self, config_file):
        """ reads the config file and output a list of option_name/option_value pair """
        config = []
        for line in file(config_file):
            line = line.strip()
    
            if len(line) == 0:
                continue
            line = line.split("#")[0] # everything before the first #
            if len(line) == 0:
                continue
            e = line.split("=")
            if len(e) != 2:
                raise Exception("Illegal config line %s" % line)
            (key, value) = e
            (key, value) = (key.strip(), value.strip())
            config.append( (key, value) )
        return config
    
    def get_all(self, key, type=None):
        def get_value():
            l = []
            for (k,v) in self.config:
                if k == key:
                    l.append(v)
            
            if len(l) == 0:
                v =  self.defaults.get(key, None)
                if v:
                    l.append(v)
            return l
        value = get_value()
        if type == None:
            return value
        return [type(v) for v in value]
    
    def get(self, key, dft = None, type = None):
        """ returns the configured value for the given key.
            If the key is not configured, dft is returned.
            If dft is not set or None, the system default is returned
        """
        def get_value():
            for (k,v) in self.config:
                if k == key:
                    return v
            else:
                if dft:
                    return dft
                else:
                    return self.defaults.get(key, None)
        value = get_value()
        if type == None:
            return value
        return type(value)
    
    def items(self):
        """ returns all configured items, not including
            systems defaults
        """
        return self.config[:]
    
    def check_files(self):
        """ gets all filenames in the configuration file 
        (We rely on the assumption that every filename option as filename in it)
        and check if the filename is absolute.
        
        The function raises a Exception if a filename option is not absolute
        """
        for (key, value) in self.config:
            if not key.endswith("filename"):
                continue
            if not os.path.isabs(value):
                raise Exception("file %s for %s isn't absolute" % (value, key))
        return