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


import command
import os
import target
import config
from scst import ScstException
import scst_user
import target
from time import sleep
import base64

def get_iscsi_scst_path():
    """ returns the iscsi scst path
    """
    return os.path.join(config.ISCST_SCST_TOOLS_PATH, "iscsi-scst-adm")

def start_iscsi(group_name, port = None, host = None):
    """ starts iscsi via the iscsi-scst init.d tool
    """
    try:
        if not is_iscsi_running(): 
            if not port:
                port = "3260"
            if not host:
                host = ""
            command.execute_cmd("/etc/init.d/iscsi-scst start %s %s %s" %
                                (group_name, port, host))
            sleep(2)
    except command.ExecutionError as e:
        raise ScstException("Failed to start iscsi-scst", e)

def validate_iscsi():
    """ validates if iscsi is running
    """
    if not is_iscsi_running():
        raise ScstException("iSCSI target is not running")
    
def stop_iscsi():
    """ stops iscsi
    """
    try:
        command.execute_cmd("/etc/init.d/iscsi-scst stop")
    except command.ExecutionError as e:
        raise ScstException("Failed to stop iscsi-scst", e)
    
def is_iscsi_running():
    """ checks if iscsi scst is running.
    """
    if not os.path.exists("/etc/init.d/iscsi-scst"):
        return False
    
    try:
        status = command.execute_cmd("ps ax|grep iscsi-scstd|grep -v grep|grep -v supervise")
        return status.find("iscsi-scstd") >= 0
    except command.ExecutionError as e:
        if e.result == 1:
            return False
        else:
            raise ScstException("Failed to check if iSCSI is running", e)
        
def register_target(target):
    """ registers a target
    
        target is a Target object
    """
    try:
        command.execute_cmd("%s --op new --tid=\"%s\" --params Name=\"%s\"" % 
                            (get_iscsi_scst_path(), target.tid, target.name())) 
        
        if len(target.params()) > 0:
            params = ["%s=\"%s\"" % (p[0], p[1]) for p in target.params()]
            params = ",".join(params)
            command.execute_cmd("%s --op update --tid=\"%s\" --params %s" % 
                                (get_iscsi_scst_path(), target.tid, params)) 
        
        if target.auth_username() or target.auth_secret_hash():
            try:
                secret = base64.standard_b64decode(target.auth_secret_hash())
                secret = [chr((ord(i) - 13) % 256) for i in secret]
                secret = "".join(secret)
                if secret[-1] == chr(0):
                    secret = secret[:-1]
                if len(secret) < 12 or len(secret) > 256:
                    raise Exception("Invalid secret")
                
                command.execute_cmd("%s --op new --tid=\"%s\" --user --params OutgoingUser=\"%s\",Password=\"%s\"" % (get_iscsi_scst_path(),
                                                                                                                      target.tid, target.auth_username(), secret)) 
            except Exception as e:
                raise ScstException("Failed to add user to target", e) 
    except Exception as e:
        raise ScstException("Failed to register target", e)

def is_target_registered(target):
    """ checks if the given target is registered at SCST
        
        target is a target Object
    """
    try:
        # This check is a kind of hack, but we have other way to see if
        # the target is registered
        command.execute_cmd("%s --op show --tid=\"%s\"" % (get_iscsi_scst_path(), target.tid))
        return True
    except command.ExecutionError as e:
        if not e.output:
            raise
        else:
            if e.output.find("Invalid argument") >= 0:
                # If Invalid argument is returned, SCST doesn't know the target
                return False
            else:
                raise # re-raise the ExecutionError
    
def unregister_target(target):
    """ unregisters a target
    """
    try:
        command.execute_cmd("%s --op delete --tid=\"%s\"" % (get_iscsi_scst_path(), target.tid)) 
    except Exception as e:
        raise ScstException("Failed to unregister target", e)

def add_user_to_target(user, target):
    """ adds a user to a target
    
        user is a User object
        target is a Target object
    """
    try:
        secret = base64.standard_b64decode(user.secret_hash())
        secret = [chr((ord(i) - 13) % 256) for i in secret]
        secret = "".join(secret)
        if secret[-1] == chr(0):
            secret = secret[:-1]
        if len(secret) < 12 or len(secret) > 256:
            raise Exception("Invalid secret")
        
        command.execute_cmd("%s --op new --tid=\"%s\" --user --params IncomingUser=\"%s\",Password=\"%s\"" % (get_iscsi_scst_path(),
                                                                                                              target.tid, user.name, secret)) 
    except Exception as e:
        raise ScstException("Failed to add user to target", e)
    
def change_target_auth(target):
    """ Change the target outgoing authentication
    
        target is a Target object
    """
    try:
        secret = base64.standard_b64decode(target.auth_secret_hash())
        secret = [chr((ord(i) - 13) % 256) for i in secret]
        secret = "".join(secret)
        if secret[-1] == chr(0):
            secret = secret[:-1]
        # this is totally a hack, but iscsi-scst-adm doesn't support to update user password
        
        try:
            command.execute_cmd("%s --op delete --tid=\"%s\" --user --params OutgoingUser=\"%s\"" % (get_iscsi_scst_path(), target.tid, target.auth_username())) 
        except:
            pass
        command.execute_cmd("%s --op new --tid=\"%s\" --user --params OutgoingUser=\"%s\",Password=\"%s\"" % (get_iscsi_scst_path(), 
                                                                                                              target.tid, target.auth_username(), secret)) 
    except Exception as e:
        raise ScstException("Failed to update user in target", e) 
    
def change_target_params(target, updated_params):
    """ update some params of a target
    
        Note: target is an updates version of the target
    """
    try:        
        param_str = ",".join(["%s=%s" % (key, update_params[key]) for key in updated_params.keys()])
        command.execute_cmd("%s --op update --tid=\"%s\" --params %s" % (get_iscsi_scst_path(), target.tid, param_str)) 
    except Exception as e:
        raise ScstException("Failed to update params in target", e)
        
def update_user_in_target(user, target):
    """ update a user in a target
    
        user is a User object
        target is a Target object
    """
    try:
        secret = base64.standard_b64decode(user.secret_hash())
        secret = [chr((ord(i) - 13) % 256) for i in secret]
        secret = "".join(secret)
        if secret[-1] == chr(0):
            secret = secret[:-1]
        # this is totally a hack, but iscsi-scst-adm doesn't support to update user password
        command.execute_cmd("%s --op delete --tid=\"%s\" --user --params IncomingUser=\"%s\"" % (get_iscsi_scst_path(), target.tid, user.name)) 
        command.execute_cmd("%s --op new --tid=\"%s\" --user --params IncomingUser=\"%s\",Password=\"%s\"" % (get_iscsi_scst_path(), 
                                                                                                              target.tid, user.name, secret)) 
    except Exception as e:
        raise ScstException("Failed to update user in target", e)
    
def rm_user_from_target(user, target):
    """ remove a user from a target
    
        user is a User object
        target is a target object
    """
    try:
        command.execute_cmd("%s --op delete --tid=\"%s\" --user --params IncomingUser=\"%s\"" % 
                            (get_iscsi_scst_path(), target.tid, user.name)) 
    except Exception as e:
        raise ScstException("Failed to remove user from target", e)

def is_user_in_target(user_name, target):
    """ checks if a users from a target
    
        user_name is a string containing the username of check
        target is a Target object
    """
    try:
        output = command.execute_cmd("%s --op show --tid=%s --user" % 
                                     (get_iscsi_scst_path(), target.tid))
        for line in output.split("\n"):
            p = line.partition(" ")
            if len(p[2]):
                if (p[2] == user_name):
                    return True
        return False
    except Exception as e:
        raise ScstException("Failed to check if user in target", e);
    
def get_users_in_target(target):
    """ get all users from a target
    
        target is a target object
        
        returns a list of User objects
    """
    try:
        output = command.execute_cmd("%s --op show --tid=%s --user" % 
                                     (get_iscsi_scst_path(), target.tid))
        users = []
        for line in output.split("\n"):
            p = line.partition(" ")
            user_name = p[2]
            if len(user_name):
                u = scst_user.User(user_name, {})
                users.append(u)
        return users
    except Exception as e:
        raise ScstException("Failed to get users in target", e);

def get_targets():
    """ get all targets that are configured in iscsi-scstd
    
        returns a list of Target objects
    """
    def get_users_for_target(tid):
        """ TODO (dmeister): WTF??
        """
        return []
    try:
        # iSCSI is not active
        if not os.path.exists("/proc/scsi_tgt/iscsi/session"):
            return {}
        
        data = open("/proc/scsi_tgt/iscsi/session").read()
        targets = {}
        t = {"sessions": []}
        for line in data.split("\n"):
            line = line.strip()
            if line.startswith("tid"):
                if len(t) > 1:
                    targets[t["tid"]] = target.Target(t["tid"], t)             
                t = {"sessions": []}
                elements = line.split(" ")
                for part in elements:  
                    p = part.partition(":")
                    t[p[0]] = p[2]
                t["users"] = get_users_for_target(t["tid"])   
                
            if line.startswith("sid"):
                session = {}
                elements = line.split(" ")
                for part in elements:  
                    p = part.partition(":")
                    session[p[0]] = p[2]
                t["sessions"].append(session)
        if len(t) > 1:
            targets[t["tid"]] = target.Target(t["tid"], t)   
        return targets
    except Exception as e:
        raise ScstException("Failed to get targets", e)

if __name__ == "__main__":
    print get_targets()
