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

# The module is called scst_user as user is a standard python module and naming collisions
# should be avoided
import re

def read_user(monitor, name):
    """ reads the data of a user that are configured in dedupv1d
    """
    user_data = monitor.read("user")
    if not name in user_data:
        raise Exception("User %s not existing" % (name))
    
    return User(name, user_data[name])

def read_all_users(monitor):
    """ reads the data of all users that are configured in dedupv1d
    """
    user_data = monitor.read("user")
    users = {}
    for name in sorted(user_data.keys()):
        data = user_data[name]
        users[name] = (User(name, data))
    return users

def check_user_name(user_name):
    if user_name == None:
        raise Exception("User name not given")
    if len(user_name) == 0:
        raise Exception("User name is empty")
    if len(user_name) > 512: # (tested max: 2048+)
        raise Exception("User name must not have more than 512 characters")
    if user_name.find("Default") >= 0:
        raise Exception("Illegal user name (\"Default\" may not be used)")
    if not re.match(r"^[a-zA-Z0-9\.\-:_]+$", user_name):
        raise Exception("Illegal user name")
    return True

class User:
    """ A user
    """
    def __init__(self, name, options):
        """ Inits a user
        """
        self.name = name
        self.options = options
        
    def validate(self):
        """ Validates a user
        """
        errors = [] 
        
        try:
            check_user_name(self.name)
        except Exception as e:
            errors.append(e)
            
        return errors
    
    def secret_hash(self):
        return self.options.get("secret hash", "")
    
    def targets(self):
        return self.options.get("targets", [])
    
    def __str__(self):
        return "User: %s, %s" % (self.name, self.options)