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
import time
import stat
import sys
import grp
import pwd
from command import execute_cmd, sh_escape, ExecutionError
import scst
import signal
import socket
import shutil
from monitor import Monitor
from monitor import MonitorException
from monitor import MonitorJSONException
from dedupv1d_pb2 import DirtyFileData
from protobuf_util import read_sized_message
import iscsi_scst
import json
import target
import group
import scst_user
import volume
from dedupv1logging import log_error, log_info, log_warning, log_verbose
import config as cfg
from xml.dom import minidom

class Dedupv1Exception(Exception):
    """ base class for all dedupv1 related exceptions
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

def handle_exception(options, ex):
    """ Handles the given exceptions by printing an
        warning if force is active or raising it
    """
    if options.force:
        log_warning(options, ex)
    else:
        raise

def is_process_running(pid):
    """ Checks if the process with the given process id (pid)
        is running
    """
    try:
        o = execute_cmd("ps -p %s" % str(int(pid)))
        return o.find(str(pid)) >= 0
    except ExecutionError as e:
        if e.result == 1:
            return False
        else:
            raise

def system_status(monitor, options, config):
    """ function that determines the system status and prints it out
        on stdout
    """
    pid = is_running(config)
    if pid:
        if options.raw:
            print json.dumps({"state": "running"})
        else:
            log_info(options, "dedupv1d running (pid = %s)" % pid)
    else:
        if options.raw:
            print json.dumps({"state": "stopped"})
        else:
            log_info(options, "dedupv1d not running")

def get_daemon_pid(config):
    """ gets the pid of the daemon according to the daemon lock file
    """
    try:
        lock_filename = config.get("daemon.lockfile")
        content = file(lock_filename, "r").readline().strip()
        pid = int(content)
        return pid
    except Exception as e:
        raise scst.ScstException("Failed to get daemon pid", e)

def is_running(config, result_if_lockfile_missing=False):
    """ checks if dedupv1d seems to run based on the process id (pid) given in
        the standard lock file

        returns True if the daemon is running or False if it is not running"
    """
    lock_filename = config.get("daemon.lockfile")
    if not os.path.exists(lock_filename):
        return result_if_lockfile_missing;
    try:
        pid = get_daemon_pid(config)
        if is_process_running(pid):
            return pid
        else:
            return False
    except scst.ScstException as e:
        if isinstance(e.base, Exception):
            return result_if_lockfile_missing
        else:
            raise

def stop_device(dedupv1_root, monitor, options, config, writeback_stop = False):
    """ stops dedupv1d.
        This function can take a very long time to finish when writeback_stop is True. In this case
        the dedup1d is not existing before all open chunk and block index data is written into the persistent index
    """
    def on_stop():
        dirty_file = config.get("daemon.dirtyfile")

        if os.path.exists(dirty_file):
            dirty_data = DirtyFileData()
            content = open(dirty_file, "r").read()
            read_sized_message(dirty_data, content)
            if not dirty_data.stopped:
                raise Exception("dedupv1d stopped with errors")
        log_info(options, "\ndedupv1d stopped")

    lock_filename = config.get("daemon.lockfile")
    pid = None
    not_running = not is_running(config)
    if not_running:
        if options.force:
            log_warning(options, "dedupv1d not running")
        else:
            raise Exception("dedupv1d not running")
    else:
        pid = get_daemon_pid(config)

    try:
        session_count = 0
        for (tid, t) in iscsi_scst.get_targets().items():
            session_count = session_count + len(t["sessions"])
            for session in t["sessions"]:
                log_warning(options, "Target %s has still open session with initiator %s" % (t["name"], session["initiator"]))

        if session_count > 0:
            if options.force:
                log_warning(options, "iSCSI targets have still open sessions")
            else:
                raise Exception("iSCSI targets have still open sessions")

    except scst.ScstException as e:
        handle_exception(options, e)

    if options.force:
        try:
            unregister_users(monitor, options, config)
        except Exception as e:
            if not_running:
                log_verbose(options, "Unregister users failed")
            else:
                log_warning(options, "Unregister users failed")

        try:
            unregister_volumes(monitor, options, config)
        except Exception as e:
            if not_running:
                log_verbose(options, "Unregister volumes failed")
            else:
                log_warning(options, "Unregister volumes failed")

        try:
            unregister_targets(monitor, options, config)
        except:
            if not_running:
                log_verbose(options, "Unregister targets failed")
            else:
                log_warning(options, "Unregister targets failed")

        try:
            unregister_groups(monitor, options, config)
        except:
            if not_running:
                log_verbose(options, "Unregister groups failed")
            else:
                log_warning(options, "Unregister groups failed")
    else:
        unregister_users(monitor, options, config)
        unregister_volumes(monitor, options, config)
        unregister_targets(monitor, options, config)
        unregister_groups(monitor, options, config)

    try:
        new_state = "stop"
        if writeback_stop:
            new_state = "writeback-stop"
        try:
            monitor.read("status", [("change-state", new_state)])
        except MonitorException:
            if not options.force:
                raise

    except OSError:
        if options.force:
            log_warning(options, "dedupv1d crashed")
    try:
        if pid:
            # Here we do trick with with the dots so we dont
            # se log_info, but it should only be done when not in
            # raw mode
            if not options.raw:
                print "dedupv1d stopping",

            for i in xrange(128):
                if not is_process_running(pid):
                    on_stop()
                    break
                if not options.raw:
                    sys.stdout.write(".")
                    sys.stdout.flush()
                time.sleep(2 * i) #exp backoff
    finally:
        # We have to remove the lock file here, cause the daemon
        # might not have the permission to do it. The file is created by
        # the daemon starting process with the uid of this script, but the
        # daemon might run under a different uid.
        if os.path.exists(lock_filename):
            os.remove(lock_filename)

def check_connection(monitor):
    """ checks if the dedupv1d on the given host, port pair is in ok state by reading the status monitor
    """
    try:
        status = monitor.read("status")
        if status["state"] == "ok":
            return True
    except MonitorJSONException as e:
        print e.raw_text
    except Exception as e:
        pass
    return False

def unregister_volumes(monitor, options, config):
    """ unregisters the volumes of the dedupv1d
        not used anymore
    """
    try:
        for (volume_id, volume) in monitor.read("volume").items():
            try:
                if not volume:
                    continue
                log_verbose(options, "Device %s unregistered" % volume["name"])
            except scst.ScstException as e:
                handle_exception(options, e)
    except IOError:
        handle_exception(options, e)

def unregister_users_direct(options, config):
    """ unregisters the users configured at SCST. This
	function bypasses the information given by the monitor. Therefore
	this function can be used to unregister the users if dedupv1d is not
	available
    """
    try:
        for (tid, t) in iscsi_scst.get_targets().items():
            target_users = iscsi_scst.get_users_in_target(t)
            for u in target_users:
                try:
                    log_verbose(options, "Remove user %s from %s" % (u.name, t.name()))
                    iscsi_scst.rm_user_from_target(u, t)
                except scst.ScstException as e:
                    log_error(options, "Failed to remove user %s from %s: %s" % (u.name, t.name(), str(e)))
    except scst.ScstException as e:
        log_error(options, "Failed to remove users: %s" % (str(e)))

def unregister_groups_direct(options, config):
    """ unregisters the groups configured at SCST. This
	function bypasses the information given by the monitor. Therefore
	this function can be used to unregister the groups if dedupv1d is not
	available
    """
    try:
        for (group_name) in scst.get_scst_groups():
            try:
                if options.verbose:
                    log_info(options, "Remove group %s" % (group_name))
                for ip in scst.get_initiator_pattern_in_group(group_name):
                    scst.rm_initiator_pattern_from_group(ip, group_name)

                for device_name in scst.get_devices_in_group(group_name):
                    scst.rm_from_group(device_name, group_name)

                if group_name != "Default":
                    scst.rm_group(group_name)
            except scst.ScstException as e:
                log_error(options,  "Failed to remove group %s: %s" % (group_name, e))
    except scst.ScstException as e:
        log_error(options, "Failed to remove groups: %s" % (e))

def unregister_group_direct(option, config, groups):
    try:
        all_groups = scst.get_scst_groups()
        for (group_name) in groups:
            try:
                if options.verbose:
                    log_verbose(options, "Remove group %s" % (group_name))

                if not group_name in all_groups:
                    raise scst.ScstException("Group %s not existing" % (group_name))
                for ip in scst.get_initiator_pattern_in_group(group_name):
                    scst.rm_initiator_pattern_from_group(ip, group_name)

                if group_name != "Default":
                    scst.rm_group(group_name)
            except scst.ScstException as e:
                log_error(options, "Failed to remove group %s: %s" % (group_name, e))
    except scst.ScstException as e:
        log_error("Failed to remove groups: %s" % (e))

def unregister_targets_direct(options, config):
    """ unregister a given configured at SCST. This
	function bypasses the information given by the monitor. Therefore
	this function can be used to unregister a target if dedupv1d is not
	available
    """
    try:
        for (tid, t) in iscsi_scst.get_targets().items():
            try:
                log_verbose(options, "Remove target %s" % (t.name()))
                iscsi_scst.unregister_target(t)
            except scst.ScstException as e:
                log_warning(options, "Failed to unregister target %s: %s" % (t.name(), str(e)))
    except scst.ScstException as e:
        log_warning("Failed to unregister targets: %s" % (str(e)))

def unregister_targets(monitor, options, config):
    """ unregisters the targets configured at SCST. This
	function bypasses the information given by the monitor. Therefore
	this function can be used to unregister the targets if dedupv1d is not
	available
    """
    try:
        for (tid, t) in target.read_all_targets(monitor).items():
            try:
                if iscsi_scst.is_target_registered(t):
                    iscsi_scst.unregister_target(t)

                group_name = "Default_" + t.name()
                if len(t.volumes()) > 0 and scst.exists_group(group_name):
                    scst.rm_group(group_name)

                log_verbose(options, "Target %s unregistered" % t.name())
            except scst.ScstException as e:
                if options.force:
                    log_warning("Failed to unregister target %s: %s" % (t.name(), str(e)))
                else:
                    raise scst.ScstException("Failed to unregister target %s" % (t.name()), e)
    except IOError as e:
        handle_exception(options, e)

def unregister_groups(monitor, options, config):
    """ unregisters the groups of the dedupv1d
    """
    for (group_name, g) in group.read_all_groups(monitor).items():
        try:
            for pattern in g.initiator_pattern():
                scst.rm_initiator_pattern_from_group(pattern, group_name)
            if not group_name == "Default" and scst.exists_group(group_name):
                # The Default group cannot be deleted
                scst.rm_group(group_name)

            log_verbose(options, "Group %s unregistered" % group_name)
        except scst.ScstException as e:
            handle_exception(options, e)

def unregister_users(monitor, options, config):
    """ unregisters the users of the dedupv1d
    """
    for (user_name, u) in scst_user.read_all_users(monitor).items():
        try:
            for target_name in u.targets():
                t = target.read_target_by_name(monitor, target_name)

                if not iscsi_scst.is_target_registered(t):
                    continue
                if not iscsi_scst.is_user_in_target(user_name, t):
                    continue

                iscsi_scst.rm_user_from_target(u, t)

            log_verbose(options, "User %s unregistered" % user_name)
        except scst.ScstException as e:
            handle_exception(options, e)

def register_users(monitor, options):
    """ registers a SCST user
    """
    for (user_name, u) in scst_user.read_all_users(monitor).items():
        try:
            for target_name in u.targets():
                t = target.read_target_by_name(monitor, target_name)

                if not iscsi_scst.is_target_registered(t):
                    raise scst.ScstException("Target %s not registered" % target_name)

                iscsi_scst.add_user_to_target(u, t)

            log_verbose(options, "User %s registered" % user_name)
        except scst.ScstException as e:
            handle_exception(options, e)

def register_groups(monitor, options, config):
    """ register SCST groups
    """
    for (group_name, g) in group.read_all_groups(monitor).items():
        try:
            if not group_name == "Default":
                # The Default group is always there
                scst.add_group(group_name)
            for pattern in g.initiator_pattern():
                scst.add_initiator_pattern_to_group(pattern, group_name)

            log_verbose(options, "Group %s registered" % group_name)
        except scst.ScstException as e:
            handle_exception(options, e)

def register_targets(monitor, options, config):
    """ Register SCST targets
    """
    for (tid, t) in target.read_all_targets(monitor).items():
        try:
            iscsi_scst.register_target(t)
            if len(t.volumes()) > 0:
                scst.add_group("Default_" + t.name())

            log_verbose(options, "Target %s registered" % t.name())
        except scst.ScstException as e:
            handle_exception(options, e)

def register_volumes(monitor, options, config):
    """ Registers SCST volumes
    """
    for (volume_id, vol) in volume.read_all_volumes(monitor).items():
        try:
            if not vol:
                # the volume is currently in detaching mode and is only still listed without a value
                continue
            scst.register_volume(vol)

            log_verbose(options, "Volume %s registered" % vol.name())
        except scst.ScstException as e:
            handle_exception(options, e)

def bootstrap_system(dedupv1_root, monitor, options, config):
    """ bootstraps the SCST system
    """
    if not check_root():
        log_error(options, "Permission denied")
        sys.exit(1)

    if is_running(config):
        if options.force:
            log_info(options, "Lock file exists. Forcing start")
            lock_filename = config.get("daemon.lockfile")
            if os.path.exists(lock_filename):
                os.unlink(lock_filename)
        else:
            raise Exception("dedupv1d running")

    daemon_user = config.get("daemon.user")
    daemon_group = config.get("daemon.group")
    daemon_port = config.get("iscsi.port", None)
    daemon_host = config.get("iscsi.host", None)

    check_iscsi = True
    no_iscsi = config.get("daemon.no-iscsi", "False")
    if no_iscsi == "True" or no_iscsi == "true":
        check_iscsi = False

    validate_dedupv1(dedupv1_root, daemon_user, daemon_group)
    scst.check_scst(group_name = daemon_group)
    scst.validate_scst(group_name = daemon_group)
    if check_iscsi:
        # In root mode, use the root group
        if not daemon_group:
            daemon_group = "root"
        iscsi_scst.start_iscsi(group_name = daemon_group,
                port = daemon_port,
                host = daemon_host)
        iscsi_scst.validate_iscsi()

def validate_dedupv1(dedupv1_root, user_name, group_name):
    """ validates the dedupv1 installation
    """
    def check_user():
        """ checks if a user with the given username exists
        """
        if user_name:
            pw = pwd.getpwnam(user_name)
            if not pw:
                raise ScstException("user %s (daemon.user) doesn't exists" % (user_name))

        if group_name:
            g = grp.getgrnam(group_name)
            if not g:
                raise ScstException("Group %s missing" % group_name)
            if user_name:
                for gm in g.gr_mem:
                    if gm == user_name:
                        break
                else:
                    raise scst.ScstException("user %s (daemon.user) is not member of group %s" % (user_name, group_name))
    check_user()

    st = os.stat(os.path.join(dedupv1_root, "bin/dedupv1_starter"))
    if not st:
        raise scst.ScstException("dedupv1_starter is missing")
    if user_name or group_name:
        group = grp.getgrnam(group_name)
        if st.st_uid != 0:
            raise scst.ScstException("Wrong owner of dedupv1_starter")
        if group and st.st_gid != group.gr_gid:
            raise scst.ScstException("Wrong group for dedupv1_starter")
        if st.st_mode & stat.S_IXOTH:
            raise scst.ScstException("Wrong mode for dedupv1 starter (other execute check failed)")
        if not st.st_mode & stat.S_IXGRP:
            raise scst.ScstException("Wrong mode for dedupv1 starter (group execute check failed)")
        if not st.st_mode & stat.S_ISUID:
            raise scst.ScstException("Wrong mode for dedupv1 starter (uid execute failed)")

def start_device(dedupv1_root, monitor, options, config, bypass = False):
    """ starts the dedupv1 daemon
    """
    def get_logging_output_file(logging_config_file):
        data = minidom.parse(logging_config_file)
        for ref in data.getElementsByTagName("param"):
                param_name = ref.getAttribute("name")
                if param_name == "filename":
                    return ref.getAttribute("value")
        return None

    def uses_log4cxx():
        return "LOGGING_LOG4CXX" in dir(cfg) and cfg.LOGGING_LOG4CXX

    if is_running(config):
        if options.force:
            log_info(options, "Lock file exists. Forcing start")
            lock_filename = config.get("daemon.lockfile")
            if os.path.exists(lock_filename):
                os.unlink(lock_filename)
        else:
            raise Exception("dedupv1d running")

    check_group = False
    daemon_user = config.get("daemon.user")
    daemon_group = config.get("daemon.group")
    check_iscsi = True
    no_iscsi = config.get("daemon.no-iscsi", "False")
    if no_iscsi == "True" or no_iscsi == "true":
        check_iscsi = False

    if check_root() and not daemon_user and not daemon_group:
        bootstrap_system(dedupv1_root, monitor, options, config)

    # check and start scst
    validate_dedupv1(dedupv1_root, daemon_user, daemon_group)
    scst.validate_scst(group_name = daemon_group)
    if check_iscsi:
        iscsi_scst.validate_iscsi()

    logging_config_file = None
    if uses_log4cxx():
        logging_config_file = config.get("logging")
        if logging_config_file:
            if not os.path.exists(logging_config_file):
                raise Exception("Logging configuration file %s doesn't exists" % logging_config_file)
            if not os.path.isabs(logging_config_file):
                raise Exception("Logging configuration file %s must be absolute" % logging_config_file)

            # We are trying to create to logging output file with the correct user/group data if possible
            logging_output_file = get_logging_output_file(logging_config_file)
            if logging_output_file and uses_log4cxx():
                if not os.path.exists(logging_output_file):
                    execute_cmd("touch %s" % logging_output_file)
                    if daemon_group:
                        execute_cmd("chgrp %s %s" % (daemon_group, logging_output_file))
                        execute_cmd("chmod g+w %s" % (logging_output_file))
    config.check_files()

    if not bypass:
        command = " ".join([os.path.join(dedupv1_root, "bin/dedupv1_starter"), sh_escape(options.configfile)])
    else:
        # bypassing dedupv1_starter. Must run as root
        command = " ".join([os.path.join(dedupv1_root, "bin/dedupv1d"), sh_escape(options.configfile)])

    if options.create:
        command = command + " --create"
    if options.force:
        command = command + " --force"
    if logging_config_file:
        command = command + " --logging \"" + logging_config_file + "\""
    execute_cmd(command, direct_output = True)

    # Here we are doing tricks with starting dots
    if not options.raw:
        print "dedupv1d starting",

    try:
        found_running = False
        i = 0
        while True:
            if not options.raw:
                sys.stdout.write(".")
                sys.stdout.flush()

            run_state = is_running(config, result_if_lockfile_missing = None)
            if run_state != None:
                if run_state and not found_running:
                    found_running = True
                    # Now we have seen the system running
                elif not run_state and found_running:
                    raise Exception("Failed to start dedupv1")
                elif run_state and found_running:
                    if check_connection(monitor):
                        break
                elif not run_state and not found_running:
                    if i > 4:
                        raise Exception("Failed to start dedupv1")
            else:
                # If the system has not written a valid pid for so long, something bad as happened
                if i > 4:
                    raise Exception("Failed to start dedupv1: Failed to check run state")
            time.sleep(2 * i) # backoff
            i = i + 1

        # Cleanup SCST state, e.g. after a crash
        unregister_users_direct(options, config)
        unregister_targets_direct(options, config)
        unregister_groups_direct(options, config)

        register_groups(monitor, options, config)
        register_targets(monitor, options, config)
        register_volumes(monitor, options, config)
        register_users(monitor, options)
        if not options.raw:
            print
            print "dedupv1d started"
    except Exception as e:
        print "dedupv1 start (partly) failed. Shutting down remaining components."

        monitor_exception_raised = False;
        try:
            monitor.read("status", [("change-state", "fast-stop")])
        except:
            monitor_exception_raised = True

        # Normally we use raise, but in this situation
        # We really want raise e, because we want that the original exception
        # is propagated and not any exception from monitor.read() that is here
        # only used as a kind of cleanup
        if monitor_exception_raised:
            raise e
        else:
            # no one destroyed the re-raise exception
            raise

def parse_options(args):
    """ parse options
    """
    c = {}
    for arg in args:
        e = arg.split("=")
        if len(e) != 2:
            raise Exception("Illegal parameter " + arg)
        c[e[0]] = e[1]
    return c

def check_dedupv1_group(config = None):
    """ checks the dedupv1 group
    """
    group_name = "dedupv1"
    if config:
        group_name = config.get("daemon.group", "dedupv1")

    u = pwd.getpwuid(os.geteuid())
    g = grp.getgrnam(group_name)
    for gm in g.gr_mem:
        if gm == u.pw_name:
            return True
    return False

def check_root():
    """ checks if the executing user is root
    """
    return os.geteuid() == 0

def clean_device(monitor, options, config):
    """ cleans the dedupv1 data
    """
    import glob
    def remove_file(filename):
        """ removes a given file, when the file exists
        """
        if os.path.exists(filename):
            log_verbose(options, "Remove %s" % filename)
            if os.path.isdir(filename):
              shutil.rmtree(filename)
            else:
              os.remove(filename)

    if is_running(config):
        raise Exception("dedupv1d running")

    dirty_filename = config.get("daemon.dirtyfile")
    remove_file(dirty_filename)
    remove_file(dirty_filename + ".tmp")

    lock_filename = config.get("daemon.lockfile")
    remove_file(lock_filename)

    for (key, value) in config.items():
        if not key.endswith("filename"):
            continue
        remove_file(value)
        remove_file(value + "-meta")
        remove_file(value + "_trans")

        # Remove the sqlite write-ahead log and shared memory files if existing
        remove_file(value + "-wal")
        remove_file(value + "-shm")

        # Remove the tc write-ahead log files if existing
        remove_file(value + ".wal")

        # Special case for the detacher
        if key.endswith("volume-info.filename"):
            detaching_filename = value + "_detaching_state"
            remove_file(detaching_filename)
            remove_file(detaching_filename + ".wal")
            remove_file(detaching_filename + "-wal")
            remove_file(detaching_filename + "-shm")
