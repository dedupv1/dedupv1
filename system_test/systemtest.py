#!/usr/bin/python
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
import unittest
import glob

from time import sleep, time
import optparse
import tempfile
from run import Run
import os
import sys
import simplejson
import re
import grp
import stat
from md5sum import md5
import shutil
import signal
import hashlib
from DataTest import DataTest
from Dedupv1Test import Dedupv1Test
from ScstTest import ScstTest
from OpeniSCSITest import OpeniSCSITest
from SysTest import SysTest
import random
import socket

def print_json(d):
    print simplejson.dumps(d, sort_keys=True, indent=4)

def dict_diff(a, b):
    """Return differences from dictionaries a to b.

    From http://stackoverflow.com/questions/715234/python-dict-update-diff
    """
    r = dict()
    for key, value in a.iteritems():
        if key not in b:
            r[key] = (value, None)
        elif b[key] != value:
            if isinstance(value, dict) and isinstance(b[key], dict):
                r[key] = dict_diff(value, b[key])
            else:
                r[key] = (value, b[key])
    for key, value in b.iteritems():
        if key not in a:
            r[key] = (None, value)
        return r

class Dedupv1BaseSystemTest(unittest.TestCase):

    def __init__(self, configuration, output_dir, test_case_name):
      unittest.TestCase.__init__(self, test_case_name)
      self.configuration = configuration
      self.output_dir = output_dir

    def setUp(self):
        print
        self.device_name = self.configuration.get("device-name", "/dev/disk/by-path/scsi-0:0:0:0")
        self.mnt_point = self.configuration.get("moint-point", "/mnt/dedup")
        self.user = self.configuration.get("user", None)
        self.dedupv1_config_file = self.configuration.get("config", "/opt/dedupv1/etc/dedupv1/dedupv1.conf")
        if not self.dedupv1_config_file.startswith("/"):
            self.dedupv1_config_file = os.path.join(os.getcwd(), self.dedupv1_config_file)

        self.run = Run()
        self.sys = SysTest(self.run, device_name=self.device_name)
        self.dedupv1 = Dedupv1Test(self.run, sys=self.sys, user=self.user, config=self.dedupv1_config_file)
        self.scst = ScstTest()
        self.open_iscsi = OpeniSCSITest(self.run)
        self.data = DataTest(self.run, mnt_point=self.mnt_point, device_name=self.device_name)

        (process_name, pid) = self.get_listening_process(self.run, 9001)
        if (process_name):
            print "Found process %s (pid %s) listening on port 9001" % (process_name, pid)
            os.kill(int(pid), 9)

        self.dedupv1.stop("--force")
        if os.path.exists("/opt/dedupv1/var/log/dedupv1"):
            for file in glob.glob("/opt/dedupv1/var/log/dedupv1/*"):
                os.remove(file)

        self.dedupv1.clean()
        self.assertExitcode(0)

        # Check for default volume, we need this for our tests
        if open(self.dedupv1.config, "r").read().find("volume.id=0") < 0:
            self.adapt_config_file("\n$", """
group=Default
volume.id=0
volume.device-name=dedupv1
volume.group=Default:0
volume.logical-size=1T
volume.threads=24
        """)

        self.repair_during_tearDown = False

    def tearDown(self):
        sleep(15)
        self.sys.umount()
        sleep(15)

        if self.scst.get_pid():
            self.dedupv1.stop("-f")
            self.assertExitcode(0)

            if self.repair_during_tearDown:
                self.dedupv1.check("--logging=/opt/dedupv1/etc/dedupv1/logging.xml", "--repair")
                self.repair_during_tearDown = False
            else:
                self.dedupv1.check("--logging=/opt/dedupv1/etc/dedupv1/logging.xml")
            self.assertExitcode(0)

    def get_listening_process(self, run, port):
      l = run("lsof -i :%s" % port).split("\n")[1:]
      if len(l) > 0:
        for line in l:
            if line.find("LISTEN") < 0:
                continue
            col = line.split()
            process_name = col[0]
            pid = col[1]
            return (process_name, pid)
      return (None, None)

    def prepare_part(self, sys=None, mnt_point=None, size=500):
        if (sys == None):
            sys = self.sys
        if (mnt_point == None):
            mnt_point = self.mnt_point
        if not os.path.exists(mnt_point):
            os.mkdirs(mnt_point)
        self.assertTrue(os.path.exists(mnt_point))

        sys.mk_part("%sG" % size)
        self.assertExitcode(0)

        sys.mkfs_ext3()
        self.assertExitcode(0)

        sys.mount()
        self.assertExitcode(0)

        sleep(5)
        self.assertTrue(os.path.ismount(mnt_point))

    def start_default_system(self):
        self.dedupv1.start("--create")
        self.assertExitcode(0)
        self.sys.clear_cache()

    def assertNoLoggedErrors(self):
        self.assertEqual(self.dedupv1.monitor("trace")["logging"]["error count"], 0)

    def assertNoLoggedWarning(self):
        self.assertEqual(self.dedupv1.monitor("trace")["logging"]["warn count"], 0)

    def assertLoggedErrors(self, min=None, max=None):
        if min is not None:
            self.assertTrue(self.dedupv1.monitor("trace")["logging"]["error count"] >= min)
        if max is not None:
            self.assertTrue(self.dedupv1.monitor("trace")["logging"]["error count"] <= max)

    def assertLoggedWarning(self, min=None, max=None):
        if min is not None:
            self.assertTrue(self.dedupv1.monitor("trace")["logging"]["warn count"] >= min)
        if max is not None:
            self.assertTrue(self.dedupv1.monitor("trace")["logging"]["warn count"] <= max)

    def assertExitcode(self, n=0, msg=None):
        if msg == None:
            msg = self.run.last_command
        self.assertEqual(self.run.code, n, "%s: actual exit code %s, expected exit code %s" % (msg, self.run.code, n))

    def assertIsJson(self, data):
        try:
            simplejson.loads(data)
        except:
            self.fail("%s is not valid JSON" % s)

    def adapt_config_file(self, pattern, substitution):
        config_data = open(self.dedupv1.config, "r").read()

        new_config_data = re.sub(pattern, substitution, config_data, 1)

        if config_data == new_config_data:
            raise Exception("Pattern not found in config: pattern " + pattern + ", config " + config_data)

        self.dedupv1.config = self.copy_to_tmp(new_config_data, suffix=".conf")

    def run(self, result):
        if result == None:
            result = self.defaultTestResult()
        unittest.TestCase.run(self, result)

        if "dedupv1" in self.__dict__:
            filename = self.id() + ".zip"
            filename = ".".join(filename.split(".")[-2:]) # use only test_<test case name>
            report_file = os.path.join(self.output_dir, filename)
            self.dedupv1.report('"%s"' % report_file)
            print "Stored report", report_file

    def background_monitor(self, interval, monitor_name):
        from multiprocessing import Process, Queue
        def f(queue):
            while (True):
                sleep(interval)
                data = self.dedupv1.monitor(monitor_name)
                queue.put(data)

                if data["state"] == "ok":
                    break
        q = Queue()
        p = Process(target=f, args=(q,))
        p.start()
        return (p, q)

    def copy_to_tmp(self, contents, suffix=""):
        import hashlib
        h = hashlib.md5(contents).hexdigest()

        filename = "/tmp/%s%s" % (h, suffix)
        f = open(filename, "w")
        f.write(contents)

        os.chown(filename, -1, grp.getgrnam("dedupv1").gr_gid)  # Change group to dedupv1
        os.chmod(filename, os.stat(filename)[stat.ST_MODE] | stat.S_IRGRP) # Add read permissions for group

        return filename

    def get_urandom_file(self, size):
        test_data_dir = self.configuration["test data dir"]
        filename = os.path.join(test_data_dir, "random-%s" % size)
        if not os.path.exists(filename) or not os.path.getsize(filename) * (size * 1024 * 1024):
            self.run("dd if=/dev/urandom of=%s bs=1M count=%s" % (filename, size))
            self.assertEqual(os.path.getsize(filename), size * 1024 * 1024)
        return filename

    def timed_kill_daemon(self, seconds, signal):
        from multiprocessing import Process
        def f():
            sleep(seconds)
            pid = int(self.scst.get_pid())
            print "Kill process", pid
            os.kill(int(self.scst.get_pid()), int(signal))
            sleep(1)

        p = Process(target=f)
        p.start()
        return p

    def is_process_running(self, pid):
        l = self.run("ps -p %s" % str(int(pid)))
        return l.find(str(pid)) >= 0

class Dedupv1SystemTest(Dedupv1BaseSystemTest):

    def test_dedupv1_mon_illegal_mon(self):
        """ test_dedupv1_mon_illegal_mon

            Tests dedupv1_mon with monitor names that do not exists
        """
        self.dedupv1.monitor("nonexisting")
        self.assertExitcode(1)

        self.start_default_system()

        mon_output = self.dedupv1.monitor("nonexisting")
        self.assertExitcode(2)
        self.assertTrue("ERROR" in mon_output)
        self.assertTrue(mon_output["ERROR"].find("Unknown monitor") >= 0)

    def test_start_stop(self):
        """ test_start_stop:
            Simple start and stop of the system
        """
        self.start_default_system()

    def test_start_fast_stop(self):
        """ test_start_fast_stop:
            Simple start and fast-stop of the system
        """
        self.start_default_system()
        sleep(10)

        self.dedupv1.stop("-f")

    def test_start_idle_stop(self):
        """ test_start_idle_stop:
            Simple start, idle, and stop of the system

            We had problems with an empty system that was idle and
            then it was not possible to stop the gc. This test
            tries to regress the problem
        """
        self.start_default_system()

        sleep(600)

        self.dedupv1.stop()

    def test_start_stop_default_filter(self):
        """ test_start_stop_default_filter:
            Simple start and stop of the system with default filters
        """
        self.adapt_config_file("filter.*", "")

        self.start_default_system()

    def test_start_stop_raw(self):
        """ test_start_stop_raw

            Simple start/stop test with --raw flags
        """
        self.start_default_system()

        self.dedupv1.stop("--raw")

        self.dedupv1.start("--raw")

    def test_simple_utils(self):
        """ test_simple_utils

            Simple system tests that tries simple things with different utilities
        """
        self.start_default_system()

        self.dedupv1.admin("--help")
        self.dedupv1.users("--help")
        self.dedupv1.groups("--help")
        self.dedupv1.targets("--help")
        self.dedupv1.volumes("--help")
        self.dedupv1.support("--help")
        self.dedupv1.admin("--version")
        self.dedupv1.users("--version")
        self.dedupv1.groups("--version")
        self.dedupv1.targets("--version")
        self.dedupv1.volumes("--version")
        self.dedupv1.support("--version")

        self.dedupv1.users("show")
        self.dedupv1.groups("show")
        self.dedupv1.targets("show")
        self.dedupv1.volumes("show")

        self.dedupv1.monitor("stats")
        self.dedupv1.monitor("trace")
        self.dedupv1.monitor("profile")
        self.dedupv1.monitor("lock")
        self.dedupv1.monitor("idle")
        self.dedupv1.monitor("log")
        self.dedupv1.monitor("gc")
        self.dedupv1.monitor("error")

    def test_call_stop_on_stopped_system(self):
        """ test_call_stop_on_stopped_system

            Verifies that dedupv1_adm stop finishes with an exit code 1 when
            the system is already stopped
        """
        self.start_default_system()

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.stop()
        self.assertExitcode(1)

        self.dedupv1.start()
        self.assertExitcode(0)

    def test_double_start(self):
        """ test_double_start

            Tests that the dedupv1_adm start returns the error code 1 if the daemon is
            already started
        """
        self.start_default_system()

        self.dedupv1.start()
        self.assertExitcode(1)

    def test_restart_with_empty_log(self):
        """ test_restart_with_empty_log

            Tests restarting the system is a completly replayed log.
        """
        self.start_default_system()

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

        self.dedupv1.monitor("idle force-idle=true")
        sleep(10)

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

    def test_restart(self):
        """ test_restart

            Simple test with a fast restart.
            Tests if all configured targets are also added to SCST before and after the restart
        """
        def check_targets():
            scst_targets = self.scst.get_targets()
            for tid in self.dedupv1.monitor("target"):
                if not tid in scst_targets:
                    self.fail("Target not added to iscsi-scst")
        self.start_default_system()

        check_targets()

        sleep(5)

        self.dedupv1.restart()
        self.assertExitcode(0)

        check_targets()

        self.dedupv1.monitor("idle force-idle=true")
        sleep(10)

    def test_use_alternative_config(self):
        """ test_use_alternative_config

            Tests that it is possible to point to a different configuration file with --config
        """
        self.start_default_system()

        self.dedupv1.stop()
        self.assertExitcode(0)

        config_data = open("/opt/dedupv1/etc/dedupv1/dedupv1.conf", "r").read()
        self.dedupv1.config = self.copy_to_tmp(config_data, suffix=".conf")

        self.dedupv1.start()
        self.dedupv1.stop()

    def test_bad_config(self):
        """ test_bad_config

            Tests if dedupv1_adm start fails with an invalid configuration
        """
        self.adapt_config_file("block-size=.*", "block-size=17");

        self.dedupv1.start("--create")
        self.assertExitcode(1)

    def test_change_logging_config(self):
        """ test_change_logging_config

            Tests if it is possible to change the logging configuration file.
            This also tests if it is possible to change a configuration file and then accepting it with --force
        """
        self.start_default_system()

        self.dedupv1.stop()
        self.assertExitcode(0)

        if not os.path.exists("/opt/dedupv1/var/log/dedupv1/dedupv1.log"):
            print "Non-standard log file location. Skip test"
            return

        old_size = os.path.getsize("/opt/dedupv1/var/log/dedupv1/dedupv1.log")

        system_test_path = os.path.abspath(os.path.dirname(sys.argv[0]))
        logfile_path = os.path.join(system_test_path, "data/no_logging.xml")

        # We copy to a temp file and change the permission
        # The reason is that executing user (dedupv1admin/dedupv1d) might not be able
        # to access the user directory where data/no_logging.xml is stored)
        logfile_path = self.copy_to_tmp(open(logfile_path, "r").read(), suffix=".xml")
        self.adapt_config_file("\n$", "\nlogging=%s" % logfile_path);

        self.dedupv1.start()
        self.assertExitcode(1)

        # We have to use force as we changed the config file
        self.dedupv1.start("--force")
        self.assertExitcode(0)

        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        # now a normal start is ok
        self.dedupv1.start()
        self.assertExitcode(0)

        self.dedupv1.stop()
        self.assertExitcode(0)

        new_size = os.path.getsize("/opt/dedupv1/var/log/dedupv1/dedupv1.log")

        # The size of the log file hasn't changed in between
        self.assertEqual(old_size, new_size)

    def test_start_stop_by_sigint(self):
        """ test_start_stop_by_sigint:

            Tests if the system is correctly stopped by raising sigint
        """
        self.start_default_system()

        os.kill(self.scst.get_pid(), signal.SIGINT)

        status = self.dedupv1.monitor("status")
        print status
        self.assertEquals(status["state"], "shutting down")
        sleep(15)

    def test_change_logging_config_with_restart(self):
        """ test_change_logging_config_with_restart

            Tests if a new configuration is accepted if dedupv1_adm restart is used
        """
        self.start_default_system()

        system_test_path = os.path.abspath(os.path.dirname(sys.argv[0]))
        logfile_path = os.path.join(system_test_path, "data/no_logging.xml")
        # We copy to a temp file and change the permission
        # The reason is that executing user (dedupv1admin/dedupv1d) might not be able
        # to access the user directory where data/no_logging.xml is stored)
        logfile_path = self.copy_to_tmp(open(logfile_path, "r").read(), suffix=".xml")
        self.adapt_config_file("\n$", "\nlogging=%s" % logfile_path);

        # We have to use force as we changed the config file
        self.dedupv1.restart("--force")
        self.assertExitcode(0)

        self.dedupv1.stop()
        self.assertExitcode(0)

    def test_system_status_monitor(self):
        """ test_system_status_monitor

            Calls the status monitor every 10 seconds and verifies that the state is first starting and then switches to ok
        """
        (p, q) = self.background_monitor(1, "status")
        self.dedupv1.start("--create")
        self.assertExitcode(0)

        found_started = False
        found_ok = False
        p.join()
        while not q.empty():
            state = q.get()
            if state["state"] == "starting":
                found_started = True
            if state["state"] == "ok":
                found_ok = True
        self.assertTrue(found_started)
        self.assertTrue(found_ok)

    def test_persistent_stats(self):
        """ test_persistent_stats

            Tests if the statistics are persistent after a clean shutdown
        """
        def compare_ignored_stats_values(s1, s2):
            ignored_stats_values = [
                            [["core", "log", "log event count"], 10],
                            [["core", "log", "replayed log event count"], 10],
                            [["core", "log", "replayed log event type 32 count"], 10],
                            [["core", "log", "replayed log event type 33 count"], 10],
                            [["core", "log", "replayed log event type 34 count"], 10],
                            [["core", "log", "replayed log event type 35 count"], 10],
                            [["core", "log", "replayed log event type 51 count"], 10],
                            [["core", "log", "replayed log event type 52 count"], 10]
                            ]
            for keys, time in ignored_stats_values:
                stats1 = s1
                stats2 = s2
                for key in keys:
                    if key in stats1 and key in stats2:
                        stats1 = stats1[key]
                        stats2 = stats2[key]
                if type(stats1) is str:
                    stats1 = eval(stats1)
                if type(stats2) is str:
                    stats2 = eval(stats2)
                if (type(stats1) is int or type(stats1) is float) and (type(stats2) is int or type(stats2) is float):
                    print "Checking " + str(keys)
                    if not (stats1 >= stats2 - time) or not (stats1 <= stats2 + time):
                        print "previous value: " + str(stats1) + " current value: " + str(stats2)
                    self.assertTrue(stats1 >= stats2 - time)
                    self.assertTrue(stats1 <= stats2 + time)

        def delete_ignored_stats_values(s):
            del s["uptime"]
            del s["servicetime"]
            del s["core"]["log"]["log event count"]
            del s["core"]["log"]["replayed log event count"]
            del s["core"]["chunk index"]["index item count"]
            del s["core"]["chunk index"]["total index item count"]
            del s["volumes"]["0"]["commands"]["average response time"]
            del s["volumes"]["0"]["commands"]["average write response time"]
            del s["volumes"]["0"]["average throttle time"]
            del s["core"]["idle"]["idle time"]
            del s["core"]["log"]["replayed log event type 32 count"]
            del s["core"]["log"]["replayed log event type 33 count"]
            del s["core"]["log"]["replayed log event type 34 count"]
            del s["core"]["log"]["replayed log event type 35 count"]
            del s["core"]["log"]["replayed log event type 51 count"]
            del s["core"]["log"]["replayed log event type 52 count"]
        self.start_default_system()

        filename = self.get_urandom_file(128)
        self.data.copy_raw(filename, 128)

        sleep(5)
        self.dedupv1.monitor("flush")

        self.dedupv1.monitor("idle", "force-idle=true")

        # We should use enough time for a full log replay
        sleep(240)

        self.dedupv1.monitor("idle", "force-idle=false")

        stats1 = self.dedupv1.monitor("stats")

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()
        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

        stats2 = self.dedupv1.monitor("stats")

        compare_ignored_stats_values(stats1, stats2)

        delete_ignored_stats_values(stats1)
        delete_ignored_stats_values(stats2)

        diff = dict_diff(stats1, stats2)
        self.assertEqual(len(diff), 0, str(diff))

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_persistent_stats_after_crash(self):
        """ test_persistent_stats_after_crash:

            Tests if the statistics are persistent after a crash shutdown.
            The statistics don't have to persist the most recent value, but they should persist values that are not too old.
        """
        self.start_default_system()

        filename = self.get_urandom_file(256)
        self.data.copy_raw(filename, 256)
        self.dedupv1.monitor("flush")

        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(90)

        stats1 = self.dedupv1.monitor("stats")
        self.dedupv1.monitor("trace")

        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        self.dedupv1.start()
        self.assertExitcode(0)

        stats2 = self.dedupv1.monitor("stats")
        # Check if the statistics have been persisted
        self.assertTrue(int(stats2["volumes"]["0"]["commands"]["scsi commands"]["WRITE (10)"]) > 0)

    def test_restart_after_kill_with_groups(self):
        """ test_restart_after_kill_with_groups

            This is a regression test for issue #284. It tests if groups are correctly registered after a kill
        """
        self.start_default_system()

        self.dedupv1.groups("add name=Backup")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G group=Backup:0")
        self.assertExitcode(0)

        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        self.dedupv1.start()
        self.assertExitcode(0)

        self.assertTrue("Backup" in self.scst.get_groups())
        self.assertTrue("Backup2" in self.scst.get_devices_in_group("Backup"))

    def test_support_unregister_all(self):
        """ test_support_unregister_all

            This is a regression test for issue #284. It ensures that dedupv1_support unregister-all works.
        """
        self.start_default_system()

        self.dedupv1.groups("add name=Backup")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G group=Backup:0")
        self.assertExitcode(0)

        dedupv1d_pid = self.scst.get_pid()

        self.dedupv1.support("unregister-all -f")
        self.assertExitcode(0)

        # it is not possible to really stop the daemon after a unregister-all using the normal shutdown system
        os.kill(dedupv1d_pid, 9)
        sleep(5)

    def test_kill_during_startup(self):
        """ test_kill_during_startup
        """
        self.start_default_system()

        self.dedupv1.stop()
        self.assertExitcode(0)

        print "Kill daemon in 5 seconds"
        kill_proc = self.timed_kill_daemon(5, 9)
        self.dedupv1.start()
        kill_proc.join()

        status = self.dedupv1.admin("status")
        self.assertEquals(status.strip(), "dedupv1d not running")

        self.dedupv1.start()
        self.assertExitcode(0)

        self.assertNoLoggedErrors()

    def test_stop_error_reporting(self):
        """ test_stop_error_reporting

            Tests if dedupv1_adm stop correctly reports an error if the system is killed during the shutdown.
        """
        self.start_default_system()

        size = 256
        filename = self.get_urandom_file(size)
        self.data.copy_raw(filename, size)

        print "Kill daemon in 2 seconds"
        kill_proc = self.timed_kill_daemon(2, 9)
        self.dedupv1.stop()
        self.assertExitcode(1)

    def test_default_config_files(self):
        """ test_default_config_files
        """
        def get_dedupv1_src():
            mod = sys.modules[__name__]
            return os.path.normpath(os.path.join(os.path.dirname(mod.__file__), ".."))

        ignore_substring_set = set(["research", "firmware"])
        def is_ignored(filename):
            for substring in ignore_substring_set:
                if filename.find(substring) >= 0:
                    return True
            return False

        conf_dir = os.path.join(get_dedupv1_src(), "conf")
        if not os.path.exists(conf_dir):
            print "Cannot find configuration directory. Skipping"
            return

        for file in os.listdir(conf_dir):
            if file.endswith(".conf") and not is_ignored(file):
                print "Using", file
                path = os.path.join(conf_dir, file)
                self.dedupv1.config = self.copy_to_tmp(open(path, "r").read(), suffix=".conf")

                self.dedupv1.clean()
                self.assertExitcode(0)

                self.dedupv1.start("--create")
                self.assertExitcode(0)

                sleep(10)

                self.dedupv1.stop("-f")
                self.assertExitcode(0)

class Dedupv1DynamicConfigSystemTest(Dedupv1BaseSystemTest):

    def test_illegal_volume_name(self):
        """ test_illegal_volume_name
        """
        self.start_default_system()

        id = 1

        illegal_names = ["",
                          "1234567890123456789012345678901234567890123456789", # 49
                         u"Vol\u00f6ume",
                         u"Vol+ume",
                         u"Vol\$ume", # $ume would be replaced in shell otherwise
                         u"Vol@ume",
                          "Vol ume",
                          "Vol:ume"
                         ]

        for name in illegal_names:
            self.dedupv1.volumes("attach id=%s device-name=\"%s\" logical-size=400G" % (id, name))
            self.assertNotEqual(0, self.run.code)
            id += 1

        legal_names = ["a",
                        "This.is_my-2nd-Volume",
                        "123456789012345678901234567890123456789012345678" # 48
                       ]

        for name in legal_names:
            self.dedupv1.volumes("attach id=%s device-name=\"%s\" logical-size=500G" % (id, name))
            self.assertExitcode(0)
            id += 1

    def test_volume_custom_filter_chain(self):
        """ test_volume_custom_filter_chain

        Tests creating volumes with a custom filter chain
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0 filter=chunk-index-filter")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=3 device-name=Backup1 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:1 filter=block-index-filter filter=chunk-index-filter")
        self.assertExitcode(0)

        self.dedupv1.volumes("target remove id=3 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("target remove id=4 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=3")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=4")
        self.assertExitcode(0)

    def test_volume_change_custom_filter_chain(self):
        """ test_volume_change_custom_filter_chain

            Tests changing the custom filter chain of volumes
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0 filter=chunk-index-filter")
        self.assertExitcode(0)

        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("change-state id=4 state=maintenance")
        self.assertExitcode(0)

        self.dedupv1.volumes("change-options id=4 filter=block-index-filter filter=chunk-index-filter")
        self.assertExitcode(0)

        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("change-state id=4 state=running")
        self.assertExitcode(0)

        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("target remove id=4 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=4")
        self.assertExitcode(0)

    def test_volume_change_custom_chunking(self):
        """ test_volume_change_custom_chunking

            Tests changing the custom chunking of volumes.
            This tests also includes switches into the maintenance mode because we had issues there.
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0")
        self.assertExitcode(0)

        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("change-state id=4 state=maintenance")
        self.assertExitcode(0)

        self.dedupv1.volumes("change-options id=4 chunking=static")
        self.assertExitcode(0)

        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("change-state id=4 state=running")
        self.assertExitcode(0)

        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("target remove id=4 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=4")
        self.assertExitcode(0)

    def test_volume_custom_chunking(self):
        """ test_volume_custom_chunking

            Tests creating volumes with custom chunking.
            This tests also includes switches into the maintenance mode because we had issues there.
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.monitor("target")
        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0 chunking=static")
        self.assertExitcode(0)

        self.dedupv1.monitor("target")
        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("attach id=3 device-name=Backup1 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:1 chunking=rabin chunking.avg-chunk-size=8K")
        self.assertExitcode(0)

        self.dedupv1.volumes("change-state id=3 state=maintenance")
        self.assertExitcode(0)

        self.dedupv1.volumes("change-options id=3 chunking=static chunking.avg-chunk-size=8K")
        self.assertExitcode(0)

        self.dedupv1.volumes("change-state id=3 state=running")
        self.assertExitcode(0)

        self.dedupv1.monitor("target")
        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("target remove id=3 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.monitor("target")
        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("target remove id=4 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.monitor("target")
        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("detach id=3")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=4")
        self.assertExitcode(0)

    def test_illegal_group_name(self):
        """ test_illegal_group_name
        """
        self.start_default_system()

        illegal_names = ["",
                          "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123", # 513
                          "Default_Group",
                         u"Gr\u00f6up",
                         u"Gr+oup",
                         u"Gr\$oup", # $oup would be replaced in shell otherwise
                         u"Gr@oup",
                          "Gr oup"
                         ]

        for name in illegal_names:
            self.dedupv1.groups("add name=\"%s\"" % name)
            self.assertNotEqual(0, self.run.code)

        legal_names = ["a",
                        "This.is_my-2nd:Group",
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012" # 512
                       ]

        for name in legal_names:
            self.dedupv1.groups("add name=\"%s\"" % name)
            self.assertExitcode(0)

    def test_attach_and_detach_volume_groups(self):
        """ test_attach_and_detach_volume_groups

            Tests the attaching and detaching of volumes with groups
        """
        self.start_default_system()

        self.dedupv1.groups("add name=Backup")
        self.assertExitcode(0)

        self.assertTrue("Backup" in self.scst.get_groups())

        self.dedupv1.groups("add name=VM")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G group=Backup:0")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=3 device-name=Backup1 logical-size=500G group=Backup:1")
        self.assertExitcode(0)

        self.dedupv1.volumes("group add id=3 group=VM:1")
        self.assertExitcode(0)

        self.assertTrue("Backup1" in self.scst.get_devices_in_group("Backup"))
        self.assertTrue("Backup2" in self.scst.get_devices_in_group("Backup"))

        self.dedupv1.volumes("attach id=2 device-name=VM logical-size=500G group=VM:0")
        self.assertExitcode(0)

        self.dedupv1.volumes("group remove id=3 group=Backup --debug")
        self.assertExitcode(0)

        self.assertFalse("Backup1" in self.scst.get_devices_in_group("Backup"))

        self.dedupv1.volumes("group remove id=4 group=Backup")
        self.assertExitcode(0)

        self.dedupv1.volumes("group remove id=2 group=VM")
        self.assertExitcode(0)

        self.dedupv1.volumes("group remove id=3 group=VM")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=3")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=4")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=2")
        self.assertExitcode(0)

        self.dedupv1.groups("remove name=Backup")
        self.assertExitcode(0)

        self.dedupv1.groups("remove name=VM")
        self.assertExitcode(0)

    def test_attach_and_detach_volume_raw(self):
        """ test_attach_and_detach_volume_raw

            Tests attaching and detaching volumes with the --raw flag
        """
        self.start_default_system()

        output = self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special --raw")
        self.assertExitcode(0)
        self.assertIsJson(output)

        output = self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0 --raw")
        self.assertExitcode(0)
        self.assertIsJson(output)

        output = self.dedupv1.volumes("attach id=3 device-name=Backup1 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:1 --raw")
        self.assertExitcode(0)
        self.assertIsJson(output)

        output = self.dedupv1.volumes("target remove id=3 target=iqn.2005-03.info.christmann:backup:special --raw")
        self.assertExitcode(0)
        self.assertIsJson(output)

        output = self.dedupv1.volumes("target remove id=4 target=iqn.2005-03.info.christmann:backup:special --raw")
        self.assertExitcode(0)
        self.assertIsJson(output)

        output = self.dedupv1.volumes("detach id=3 --raw")
        self.assertExitcode(0)
        self.assertIsJson(output)

        output = self.dedupv1.volumes("detach id=4 --raw")
        self.assertExitcode(0)
        self.assertIsJson(output)

    def test_deferred_attach_and_detach_volume(self):
        """ test_deferred_attach_and_detach_volume

            Tests attaching and detaching volumes, but it adds the volumes to
            targets via extra dedupv1_volumes target add calls.
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G")
        self.assertExitcode(0)

        self.dedupv1.volumes("target add id=4 target=iqn.2005-03.info.christmann:backup:special:0")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=3 device-name=Backup1 logical-size=500G")
        self.assertExitcode(0)

        self.dedupv1.volumes("target add id=3 target=iqn.2005-03.info.christmann:backup:special:1")
        self.assertExitcode(0)

        self.dedupv1.volumes("target remove id=3 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("target remove id=4 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=3")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=4")
        self.assertExitcode(0)

    def test_use_gb_in_logical_size(self):
        """ test_use_gb_in_logical_size

            Tests it is possible to use alternative storage units as GB and tb (instead of the usually allowed G and t without the B suffix) are allowed to specify logical volumes sizes
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500GB target=iqn.2005-03.info.christmann:backup:special:0")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=5 device-name=Backup3 logical-size=1tb target=iqn.2005-03.info.christmann:backup:special:1")
        self.assertExitcode(0)

        volumes = self.dedupv1.monitor("volume")
        self.assertEqual(volumes["4"]["logical size"], 500 * 1024 * 1024 * 1024)
        self.assertEqual(volumes["5"]["logical size"], 1024 * 1024 * 1024 * 1024)

        self.dedupv1.volumes("target remove id=4 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)
        self.dedupv1.volumes("target remove id=5 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=4")
        self.assertExitcode(0)
        self.dedupv1.volumes("detach id=5")
        self.assertExitcode(0)

    def test_negative_logical_size(self):
        """ test_negative_logical_size

            Verifies the behavior of dedupv1_volumes when a volumes with a negative logical size is attached
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=-500G target=iqn.2005-03.info.christmann:backup:special:0")
        self.assertExitcode(1)

    def test_illegal_target_name(self):
        """ test_illegal_target_name

            Tests dedupv1_targets add with an illegal target name. There we use the example that caused problems at the VW show.
        """
        self.start_default_system()

        id = 1

        illegal_names = ["",
                          "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234", # 224
                         u"tar\u00f6get",
                         u"tar+get",
                         u"tar\$get", # $get would be replaced in shell otherwise
                         u"tar@get",
                          "tar get",
                          "tar_get",
                          "Target"
                         ]

        for name in illegal_names:
            print
            self.dedupv1.targets("add tid=%s name=\"%s\"" % (id, name))
            self.assertNotEqual(0, self.run.code)
            id += 1

        legal_names = ["a",
                        "this.ismy-2nd:target",
                        "iqn.2005-03.info.christmann:backup:special",
                        "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123" # 223
                       ]

        for name in legal_names:
            self.dedupv1.targets("add tid=%s name=\"%s\"" % (id, name))
            self.assertExitcode(0)
            id += 1

    def test_attach_and_detach_volume(self):
        """ test_attach_and_detach_volume

            Simple attaching and detaching of volumes
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.monitor("target")
        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0")
        self.assertExitcode(0)

        self.dedupv1.monitor("target")
        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("attach id=3 device-name=Backup1 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:1")
        self.assertExitcode(0)

        self.dedupv1.monitor("target")
        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("target remove id=3 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.monitor("target")
        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("target remove id=4 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.monitor("target")
        self.dedupv1.monitor("volume")

        self.dedupv1.volumes("detach id=3")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=4")
        self.assertExitcode(0)

    def test_clone_volume(self):
        """ test_clone_volume

            Tests the cloning of volumes. It comples a GB of data to a volumes, clones it and then verifies that the clone volume contains the same data
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G group=Default:1 chunking=rabin chunking.avg-chunk-size=16K chunking.max-chunk-size=32K")
        self.assertExitcode(0)

        self.sys.rm_scst_local()
        sleep(5)
        self.sys.add_scst_local()

        size = 1024
        filename = self.get_urandom_file(size)

        md5_1 = self.data.read_md5(size, filename)
        print "Data md5", md5_1

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:1"

        self.data.copy_raw(filename, size)
        md5_2 = self.data.read_md5(size)
        print "After write md5", md5_2

        self.sys.rm_scst_local()
        sleep(5)

        self.dedupv1.volumes("change-state id=1 state=maintenance")
        self.assertExitcode(0)

        self.dedupv1.volumes("clone id=2 src-id=1 device-name=backup2 group=Default:2")
        self.dedupv1.monitor("idle", "force-idle=true") # parallel idle to make it harder

        self.assertEqual(1, len(self.dedupv1.monitor("volume")["2"]["fast copy"]))

        self.dedupv1.volumes("change-state id=2 state=running")
        self.assertExitcode(1)

        volume_data = self.dedupv1.monitor("volume")
        while len(volume_data["2"]["fast copy"]) == 1:
            if volume_data["2"]["fast copy"][0]["state"] == "failed":
                break

            sleep(60)
            volume_data = self.dedupv1.monitor("volume")

        self.dedupv1.volumes("change-state id=2 state=running")
        self.assertExitcode(0)

        self.sys.add_scst_local()
        sleep(5)

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:2"

        md5_3 = self.data.read_md5(size)
        print "After clone md5", md5_3
        self.assertEqual(md5_2, md5_3)

        # The try to change the state of a the clone target produces an error message and an warn message
        self.assertEqual(self.dedupv1.monitor("trace")["logging"]["error count"], 1)
        self.assertEqual(self.dedupv1.monitor("trace")["logging"]["warn count"], 1)

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

    def test_fastcopy_volume(self):
        """ test_fastcopy_volume

            Tests the fast-copying of volumes. It comples a GB of data to a volumes, clones it and then verifies that the destination volume contains the same data
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G group=Default:1")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=2 device-name=backup2 logical-size=500G maintenance=true group=Default:2")
        self.assertExitcode(0)

        self.sys.rm_scst_local()
        sleep(5)
        self.sys.add_scst_local()

        size = 1024
        filename = self.get_urandom_file(size)

        md5_1 = self.data.read_md5(size, filename)
        print "Data md5", md5_1

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:1"

        self.data.copy_raw(filename, size)
        md5_2 = self.data.read_md5(size)
        print "After write md5", md5_2

        self.sys.rm_scst_local()

        self.dedupv1.volumes("change-state id=1 state=maintenance")
        self.assertExitcode(0)

        self.dedupv1.volumes("fast-copy src-id=1 target-id=2 size=1024M")
        self.dedupv1.monitor("idle", "force-idle=true") # parallel idle to make it harder

        self.dedupv1.volumes("change-state id=2 state=running")
        self.assertExitcode(1)

        volume_data = self.dedupv1.monitor("volume")
        while len(volume_data["2"]["fast copy"]) == 1:
            if volume_data["2"]["fast copy"][0]["state"] == "failed":
                break

            sleep(60)
            volume_data = self.dedupv1.monitor("volume")

        self.dedupv1.volumes("change-state id=2 state=running")
        self.assertExitcode(0)

        self.sys.add_scst_local()

        self.assertTrue("scsi-0:0:0:2" in self.sys.get_devices())

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:2"

        md5_3 = self.data.read_md5(size)
        print "After clone md5", md5_3
        self.assertEqual(md5_2, md5_3)

        # The try to change the state of a the clone target produces an error message and an warn message
        self.assertEqual(self.dedupv1.monitor("trace")["logging"]["error count"], 1)
        self.assertEqual(self.dedupv1.monitor("trace")["logging"]["warn count"], 1)

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

    def test_maintenance_mode(self):
        """ test_maintenance_mode

            Tests changing a volume into the maintenance mode and back
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0 group=Default:1")
        self.assertExitcode(0)

        self.assertTrue("Backup2" in self.scst.get_devices_in_group("Default_iqn.2005-03.info.christmann:backup:special"))
        self.assertEqual("Backup2:0", self.dedupv1.monitor("target")["3"]["volumes"][0])
        self.assertEqual("iqn.2005-03.info.christmann:backup:special", self.dedupv1.monitor("volume")["4"]["targets"][0]["name"])

        self.sys.rm_scst_local()
        self.sys.add_scst_local()

        self.assertTrue("scsi-0:0:0:1" in self.sys.get_devices())

        self.sys.rm_scst_local()

        self.dedupv1.volumes("change-state id=4 state=maintenance")
        self.assertExitcode(0)

        self.sys.add_scst_local()
        self.assertFalse("scsi-0:0:0:1" in self.sys.get_devices())
        self.sys.rm_scst_local()

        self.dedupv1.volumes("change-state id=4 state=running")
        self.assertExitcode(0)

        self.sys.add_scst_local()
        self.assertTrue("scsi-0:0:0:1" in self.sys.get_devices())
        self.sys.rm_scst_local()

        self.dedupv1.volumes("change-state id=4 state=running")
        self.assertExitcode(0)

        self.sys.add_scst_local()
        self.assertTrue("scsi-0:0:0:1" in self.sys.get_devices())
        self.sys.rm_scst_local()

        self.dedupv1.volumes("change-state id=4 state=maintenance")
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        # Maintenance mode is persistent
        self.dedupv1.restart()
        self.sys.add_scst_local()
        self.assertFalse("scsi-0:0:0:1" in self.sys.get_devices())
        self.sys.rm_scst_local()

        self.dedupv1.volumes("target remove id=4 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("group remove id=4 group=Default")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=4")
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_fastcopy_volume_restart(self):
        """ test_fastcopy_volume_restart

            Tests the fast-copying of volumes over a restart. It comples a GB of data to a volumes, clones it and then verifies that the destination volume contains the same data. During the fast-copy operation, the system is restarted.
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G group=Default:1")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=2 device-name=backup2 logical-size=500G maintenance=true group=Default:2")
        self.assertExitcode(0)

        self.sys.rm_scst_local()
        sleep(5)
        self.sys.add_scst_local()

        size = 1024
        filename = self.get_urandom_file(size)

        md5_1 = self.data.read_md5(size, filename)
        print "Data md5", md5_1

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:1"

        self.data.copy_raw(filename, size)
        md5_2 = self.data.read_md5(size)
        print "After write md5", md5_2

        self.sys.rm_scst_local()
        sleep(5)

        self.dedupv1.volumes("change-state id=1 state=maintenance")
        self.assertExitcode(0)

        self.dedupv1.volumes("fast-copy src-id=1 target-id=2 size=1024M")
        self.assertExitcode(0)

        self.dedupv1.monitor("idle", "force-idle=true") # parallel idle to make it harder

        sleep(10) # stop too early

        self.dedupv1.stop()
        self.assertExitcode(0)
        self.dedupv1.start()
        self.assertExitcode(0)

        sleep(90)

        self.dedupv1.volumes("change-state id=2 state=running")
        self.assertExitcode(0)

        self.sys.add_scst_local()
        sleep(5)

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:2"

        md5_3 = self.data.read_md5(size)
        print "After clone md5", md5_3
        self.assertEqual(md5_2, md5_3)

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

    def test_change_volume_size(self):
        """ test_change_volume_size

            Tests changing the logical size of a volume while the volume is in maintenance mode.
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G")
        self.assertExitcode(0)

        self.dedupv1.targets("add tid=2 name=iqn.2010.04:example")
        self.assertExitcode(0)

        self.dedupv1.volumes("target add id=1 target=iqn.2010.04:example:0")
        self.assertExitcode(0)

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)

        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

        sleep(10)
        devices = os.listdir("/dev/disk/by-path")
        self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2010.04:example-lun-0" in devices)
        volume_size = self.run("blockdev --getsize64 /dev/disk/by-path/ip-127.0.0.1:3260-iscsi-iqn.2010.04:example-lun-0")
        self.assertEqual("536870912000", volume_size.strip())

        sleep(5)
        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

        sleep(5)
        self.dedupv1.volumes("change-state id=1 state=maintenance")
        self.assertExitcode(0)

        sleep(5)
        self.dedupv1.volumes("change-size id=1 logical-size=1T")
        self.assertExitcode(0)

        sleep(5)
        self.dedupv1.volumes("change-state id=1 state=running")
        self.assertExitcode(0)

        sleep(10)
        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

        sleep(10)
        devices = os.listdir("/dev/disk/by-path")
        self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2010.04:example-lun-0" in devices)
        volume_size = self.run("blockdev --getsize64 /dev/disk/by-path/ip-127.0.0.1:3260-iscsi-iqn.2010.04:example-lun-0")
        self.assertEqual("1099511627776", volume_size.strip())

        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

    def test_change_volume_size_at_runtime(self):
        """ test_change_volume_size_at_runtime

            Tests changing the logical size of a volume while the volume is active, but without a session
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G")
        self.assertExitcode(0)

        self.dedupv1.targets("add tid=2 name=iqn.2010.04:example")
        self.assertExitcode(0)

        self.dedupv1.volumes("target add id=1 target=iqn.2010.04:example:0")
        self.assertExitcode(0)

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)

        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

        sleep(10)
        devices = os.listdir("/dev/disk/by-path")
        self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2010.04:example-lun-0" in devices)
        volume_size = self.run("blockdev --getsize64 /dev/disk/by-path/ip-127.0.0.1:3260-iscsi-iqn.2010.04:example-lun-0")
        self.assertEqual("536870912000", volume_size.strip())

        sleep(5)
        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

        sleep(5)
        self.dedupv1.volumes("change-size id=1 logical-size=1T")
        self.assertExitcode(0)

        sleep(10)
        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

        sleep(10)
        devices = os.listdir("/dev/disk/by-path")
        self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2010.04:example-lun-0" in devices)
        volume_size = self.run("blockdev --getsize64 /dev/disk/by-path/ip-127.0.0.1:3260-iscsi-iqn.2010.04:example-lun-0")
        self.assertEqual("1099511627776", volume_size.strip())

        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

    def test_change_volume_size_with_fs(self):
        """ test_change_volume_size_with_fs

            Tests changing the logical size of a volume with a filesystem on it. In this test, the partition and the fileystem are also resized to the new logical size of the volume.
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=120G")
        self.assertExitcode(0)

        self.dedupv1.targets("add tid=2 name=iqn.2010.04:example")
        self.assertExitcode(0)

        self.dedupv1.volumes("target add id=1 target=iqn.2010.04:example:0")
        self.assertExitcode(0)

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)

        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

        sleep(10)
        self.sys.parted("mklabel msdos")
        self.assertExitcode(0)

        self.sys.parted("mkpartfs primary ext2 40 120")
        self.assertExitcode(0)

        self.sys.mount()
        self.assertExitcode(0)

        statfs1 = self.sys.statfs()

        sleep(10)
        self.sys.umount()
        self.assertExitcode(0)

        sleep(5)
        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

        sleep(5)
        self.dedupv1.volumes("change-state id=1 state=maintenance")
        self.assertExitcode(0)

        sleep(5)
        self.dedupv1.volumes("change-size id=1 logical-size=200G")
        self.assertExitcode(0)

        sleep(5)
        self.dedupv1.volumes("change-state id=1 state=running")
        self.assertExitcode(0)

        sleep(10)
        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

        sleep(10)
        self.sys.parted("resize 1 40 200")
        self.assertExitcode(0)

        self.sys.mount()
        self.assertExitcode(0)

        statfs2 = self.sys.statfs()

        self.assertTrue(statfs1.f_bavail <= statfs2.f_bavail)

        sleep(10)
        self.sys.umount()
        self.assertExitcode(0)

        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

    def test_target_rename_not_existing(self):
        """ test_target_rename_not_existing

            Tests that the handling of a rename on a missing target is correct
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        # Oh here is a typo, it should be 3 not 4
        self.dedupv1.targets("change tid=4 name=iqn.2005-03.info.christmann:backup:normal")
        self.assertExitcode(1)

    def test_user_short_secret(self):
        """ test_user_short_secret

            Tests dedupv1_passwd with a too short password
        """
        self.start_default_system()

        secret = self.dedupv1.passwd("short")
        self.assertExitcode(1)

    def test_user_long_secret(self):
        """ test_user_long_secret

            Tests a long password. Long here means longer than 16 bytes, the maximal size of a password in the Windows
            iSCSI initiator. Linux and the iSCSI spec allow longer passwords.
        """
        self.start_default_system()

        secret = self.dedupv1.passwd("veryveryveryveryverylong")
        self.assertExitcode(0)

        self.dedupv1.users("add name=admin4 secret-hash=%s" % secret)
        self.assertExitcode(0)

    def test_user_very_long_secret(self):
        """ test_user_very_long_secret

            Tests a password longer than 256 byte, which should fail.
        """
        self.start_default_system()

        # generate a 257 char lon gpassword
        long_password = "".join(["A"] * 257)

        secret = self.dedupv1.passwd(long_password)
        self.assertExitcode(1)

    def test_system_status(self):
        """ test_system_status

            Verified different outputs of dedupv1_adm status
        """
        self.start_default_system()

        output = self.dedupv1.admin("status")
        self.assertTrue(output.strip().find("running") >= 0)
        self.assertExitcode(0)

        self.dedupv1.stop()
        self.assertExitcode(0)

        # Fall when finished
        output = self.dedupv1.admin("status")
        self.assertExitcode(0)
        self.assertTrue(output.strip().find("not running") >= 0, "status should be 'not running', but is '%s'" % output.strip())

        self.dedupv1.start()
        self.assertExitcode(0)

    def test_illegal_user_name(self):
        """ test_illegal_user_name
        """
        self.start_default_system()

        secret = self.dedupv1.passwd("1234567890123")
        self.assertExitcode(0)

        illegal_names = ["",
                          "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123", # 513
                          "Default_User",
                         u"U\u00f6ser",
                         u"U+ser",
                         u"U\$ser", # $ser would be replaced in shell otherwise
                         u"U@ser",
                          "U ser"
                         ]

        for name in illegal_names:
            self.dedupv1.users("add name=\"%s\" secret-hash=%s" % (name, secret))
            self.assertNotEqual(0, self.run.code)

        legal_names = ["a",
                        "This.is_my-2nd:User",
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012" # 512
                       ]

        for name in legal_names:
            self.dedupv1.users("add name=\"%s\" secret-hash=%s" % (name, secret))
            self.assertExitcode(0)

    def test_user_normal(self):
        """ test_user_normal

            Tests adding a user to a target
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=4 name=iqn.2005-05.de.jgu:systemtest")
        self.assertExitcode(0)

        secret = self.dedupv1.passwd("1234567890123")
        self.assertExitcode(0)

        self.dedupv1.users("add name=admin5 secret-hash=%s" % secret)
        self.assertExitcode(0)

        self.dedupv1.users("target add name=admin5 target=iqn.2005-05.de.jgu:systemtest")
        self.assertExitcode(0)

        users_output = self.dedupv1.users("show")
        self.assertExitcode(0)
        self.assertTrue(users_output.find("iqn.2005-05.de.jgu:systemtest") >= 0)

        targets_output = self.dedupv1.targets("show")
        self.assertExitcode(0)
        self.assertTrue(targets_output.find("admin5") >= 0)

        self.dedupv1.users("target remove name=admin5 target=iqn.2005-05.de.jgu:systemtest")
        self.assertExitcode(0)

        self.dedupv1.targets("remove tid=4")
        self.assertExitcode(0)

    def test_change_user(self):
        """ test_change_user

            Tests changing a user secret
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=4 name=iqn.2005-05.de.jgu:systemtest")
        self.assertExitcode(0)

        pwd = self.dedupv1.passwd("1234567890123")
        self.dedupv1.users("add name=admin5 secret-hash=%s" % pwd)
        self.assertExitcode(0)

        self.dedupv1.users("target add name=admin5 target=iqn.2005-05.de.jgu:systemtest")
        self.assertExitcode(0)

        users_in_target = self.scst.get_user_in_target("iqn.2005-05.de.jgu:systemtest")
        self.assertEqual(users_in_target[0].name, "admin5")

        pwd = self.dedupv1.passwd("abcdefghikalm")
        self.dedupv1.users("change name=admin5 secret-hash=%s" % pwd)
        self.assertExitcode(0)

        users_in_target = self.scst.get_user_in_target("iqn.2005-05.de.jgu:systemtest")
        self.assertEqual(users_in_target[0].name, "admin5")

        self.dedupv1.users("target remove name=admin5 target=iqn.2005-05.de.jgu:systemtest")
        self.assertExitcode(0)

        self.dedupv1.targets("remove tid=4")
        self.assertExitcode(0)

    def test_group_assignment(self):
        """ test_group_assignment

            Tests assigning volumes to a group with a initiator pattern
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=3 device-name=Backup1 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:1")
        self.assertExitcode(0)

        self.dedupv1.groups("add name=backup:special")
        self.assertExitcode(0)

        self.dedupv1.groups("initiator add name=backup:special initiator=iqn.1991-05.com.microsoft:winstoragesrv01")
        self.assertExitcode(0)

        self.dedupv1.volumes("group add id=3 group=backup:special:0")
        self.assertExitcode(0)

        groups = self.dedupv1.monitor("group")
        self.assertTrue("Backup1:0" in groups["backup:special"]["volumes"])

        volumes = self.dedupv1.monitor("volume")
        self.assertEqual(volumes["3"]["groups"][0]["name"], "backup:special")

    def test_remove_group_with_active_session(self):
        """ test_remove_group_with_active_session

            Verifies that it is not possible to remove a group with an active connection.
        """
        self.start_default_system()

        self.sys.rm_scst_local()
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G group=Default:1")
        self.assertExitcode(0)

        self.sys.add_scst_local()
        self.assertExitcode(0)

        print self.sys.get_devices()
        self.assertTrue("scsi-0:0:0:0" in self.sys.get_devices())
        self.assertTrue("scsi-0:0:0:1" in self.sys.get_devices())

        self.sys.rm_scst_local()
        self.assertExitcode(0)

        self.dedupv1.groups("add name=Backup")
        self.assertExitcode(0)

        self.dedupv1.volumes("group add id=1 group=Backup:0")
        self.assertExitcode(0)

        self.dedupv1.groups("initiator add name=Backup initiator=scst*")
        self.assertExitcode(0)

        self.sys.add_scst_local()
        self.assertExitcode(0)

        # Here we have only a single volume
        print self.sys.get_devices()
        self.assertTrue("scsi-0:0:0:0" in self.sys.get_devices())
        self.assertFalse("scsi-0:0:0:1" in self.sys.get_devices())

        self.dedupv1.volumes("group remove id=1 group=Backup")
        self.assertExitcode(1)

        self.dedupv1.groups("remove name=Backup")
        self.assertExitcode(1)

        self.sys.rm_scst_local()
        self.assertExitcode(0)

        self.dedupv1.volumes("group remove id=1 group=Backup")
        self.assertExitcode(0)

        self.dedupv1.groups("remove name=Backup")
        self.assertExitcode(0)

    def test_initiator_pattern(self):
        """ test_initiator_pattern

            Verifies the basic functionality of an initiator pattern.
        """
        self.start_default_system()

        self.sys.rm_scst_local()
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G group=Default:1")
        self.assertExitcode(0)

        self.sys.add_scst_local()
        self.assertExitcode(0)

        print self.sys.get_devices()
        self.assertTrue("scsi-0:0:0:0" in self.sys.get_devices())
        self.assertTrue("scsi-0:0:0:1" in self.sys.get_devices())

        self.sys.rm_scst_local()
        self.assertExitcode(0)

        self.dedupv1.groups("add name=Backup")
        self.assertExitcode(0)

        self.dedupv1.volumes("group add id=1 group=Backup:0")
        self.assertExitcode(0)

        self.dedupv1.groups("initiator add name=Backup initiator=scst*")
        self.assertExitcode(0)

        self.sys.add_scst_local()
        self.assertExitcode(0)

        # Here we have only a single volume
        print self.sys.get_devices()
        self.assertTrue("scsi-0:0:0:0" in self.sys.get_devices())
        self.assertFalse("scsi-0:0:0:1" in self.sys.get_devices())

        self.sys.rm_scst_local()
        self.assertExitcode(0)

        self.dedupv1.volumes("group remove id=1 group=Backup")
        self.assertExitcode(0)

        self.dedupv1.groups("remove name=Backup")
        self.assertExitcode(0)

    def test_initiator_pattern_negative(self):
        """ test_initiator_pattern_negative

            Verifies the functionality of an initiator pattern that a initiator that doesn't match the pattern does not see the volumes of the group.
        """
        self.start_default_system()

        self.sys.rm_scst_local()
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=2 device-name=backup2 logical-size=500G")
        self.assertExitcode(0)

        self.dedupv1.groups("add name=Backup")
        self.assertExitcode(0)

        self.dedupv1.volumes("group add id=1 group=Backup:0")
        self.assertExitcode(0)

        self.dedupv1.volumes("group add id=2 group=Backup:1")
        self.assertExitcode(0)

        self.dedupv1.groups("initiator add name=Backup initiator=iqn*")
        self.assertExitcode(0)

        self.sys.add_scst_local()
        self.assertExitcode(0)

        # Here we have only a single volume (we default volume)
        print self.sys.get_devices()
        self.assertTrue("scsi-0:0:0:0" in self.sys.get_devices())
        self.assertFalse("scsi-0:0:0:1" in self.sys.get_devices())

        self.sys.rm_scst_local()
        self.assertExitcode(0)

        self.dedupv1.volumes("group remove id=1 group=Backup")
        self.assertExitcode(0)

        self.dedupv1.volumes("group remove id=2 group=Backup")
        self.assertExitcode(0)

        self.dedupv1.groups("remove name=Backup")
        self.assertExitcode(0)

    def test_two_groups(self):
        """ test_two_groups

            Tests adding two groups.
        """
        self.start_default_system()

        self.dedupv1.groups("add name=TestGruppe")
        self.assertExitcode(0)

        self.dedupv1.groups("add name=Gruppetest2")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=2 device-name=test-bil logical-size=500G")
        self.assertExitcode(0)

        self.dedupv1.volumes("group add id=2 group=TestGruppe:14")
        self.assertExitcode(0)

        self.dedupv1.volumes("group add id=2 group=Gruppetest2:12")
        self.assertExitcode(0)

        groups = self.dedupv1.monitor("group")
        self.assertTrue("test-bil:12" in groups["Gruppetest2"]["volumes"])
        self.assertTrue("test-bil:14" in groups["TestGruppe"]["volumes"])

        self.dedupv1.volumes("group remove id=2 group=Gruppetest2")
        self.assertExitcode(0)

        groups = self.dedupv1.monitor("group")
        self.assertFalse("test-bil:12" in groups["Gruppetest2"]["volumes"])
        self.assertTrue("test-bil:14" in groups["TestGruppe"]["volumes"])

class Dedupv1ISCSISystemTest(Dedupv1BaseSystemTest):

    def test_iscsi_start_stop(self):
        """ test_iscsi_start_stop

            Tests basic iscsi connection and disconnection.
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0")
        self.assertExitcode(0)

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)

        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

    def test_iscsi_start_stop_with_user(self):
        """ test_iscsi_start_stop_with_user

            Tests basic iscsi connection and disconnection with user authentication
        """
        try:
            self.start_default_system()

            self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)

            secret = self.dedupv1.passwd("testtesttest1")
            self.assertExitcode(0)

            self.dedupv1.users("add name=test secret-hash=%s" % secret)
            self.assertExitcode(0)

            self.dedupv1.users("target add name=test target=iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)

            self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0")
            self.assertExitcode(0)

            self.open_iscsi.discover(server="127.0.0.1")
            self.assertExitcode(0)

            self.open_iscsi.client_auth(server="127.0.0.1",
                                 name="iqn.2005-03.info.christmann:backup:special",
                                 username="bla",
                                 password="bla")
            self.assertExitcode(0)

            self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertFalse(self.run.code == 0) # Loging failed

            self.open_iscsi.client_auth(server="127.0.0.1",
                                 name="iqn.2005-03.info.christmann:backup:special",
                                 username="test",
                                 password="testtesttest1")
            self.assertExitcode(0)

            self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)
        finally:
            self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")

            self.dedupv1.stop("-f")

    def test_iscsi_start_stop_with_server_auth(self):
        """ test_iscsi_start_stop_with_server_auth

            Tests basic iscsi connection and disconnection with mutual authentication
        """
        try:
            self.start_default_system()

            pwd = self.dedupv1.passwd("1234567890123")
            self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special auth.name=test auth.secret=%s" % pwd)
            self.assertExitcode(0)

            secret = self.dedupv1.passwd("testtesttest1")
            self.assertExitcode(0)

            self.dedupv1.users("add name=test secret-hash=%s" % secret)
            self.assertExitcode(0)

            self.dedupv1.users("target add name=test target=iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)

            self.dedupv1.monitor("target")

            self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0")
            self.assertExitcode(0)

            self.open_iscsi.client_auth(server="127.0.0.1",
                                 name="iqn.2005-03.info.christmann:backup:special",
                                 username="test",
                                 password="testtesttest1")
            self.open_iscsi.server_auth(server="127.0.0.1",
                                 name="iqn.2005-03.info.christmann:backup:special",
                                 username="test2",
                                 password="test2")
            self.assertExitcode(0)

            self.open_iscsi.discover(server="127.0.0.1")
            self.assertExitcode(0)

            self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertFalse(self.run.code == 0) # Login failed

            self.open_iscsi.client_auth(server="127.0.0.1",
                                 name="iqn.2005-03.info.christmann:backup:special",
                                 username="test",
                                 password="testtesttest1")
            self.open_iscsi.server_auth(server="127.0.0.1",
                                 name="iqn.2005-03.info.christmann:backup:special",
                                 username="test",
                                 password="1234567890123")
            self.assertExitcode(0)

            self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0) # correct password

        finally:
            self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")

            self.dedupv1.stop("-f")

    def test_lun_missing(self):
        """ test_lun_missing

        Tests adding and removing multiple volumes from a target and verifies the availability of the current set of volumes over an iSCSI connection. This is a regression test for #298.
        """
    	def check_available_lun(luns):
    	    self.open_iscsi.discover(server="127.0.0.1")
    	    self.assertExitcode(0)
    	    self.open_iscsi.connect(server="127.0.0.1", name="iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk")
    	    self.assertExitcode(0)
    	    try:
                sleep(5)
                devices = os.listdir("/dev/disk/by-path")

                for lun in luns:
                    self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk-lun-%s" % (lun) in devices)
            finally:
                  self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk")
                  self.assertExitcode(0)

        self.start_default_system()
        self.sys.rm_scst_local()

        self.dedupv1.targets("add tid=3 name=iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk")

        self.dedupv1.volumes("attach id=4 device-name=backup_jgu logical-size=500G target=iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk:0")
        self.assertExitcode(0)
        self.dedupv1.volumes("attach id=5 device-name=backup_bil logical-size=500G target=iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk:1")
        self.assertExitcode(0)

        check_available_lun(["0", "1"])

        self.dedupv1.volumes("attach id=6 device-name=dynamic logical-size=500G target=iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk:2")
        self.assertExitcode(0)
        self.dedupv1.volumes("attach id=7 device-name=dynamic1 logical-size=500G target=iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk:3")
        self.assertExitcode(0)

        check_available_lun(["0", "1", "2", "3"])

        print self.scst.get_devices_in_group("Default_iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk")

        self.dedupv1.volumes("target remove id=6 target=iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk --verbose --debug")
        self.assertExitcode(0)

        print self.scst.get_devices_in_group("Default_iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk")

        self.dedupv1.volumes("target remove id=7 target=iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk")
        self.assertExitcode(0)

        print self.scst.get_devices_in_group("Default_iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk")

        self.dedupv1.volumes("detach id=6")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=7")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=8 device-name=backup_kal logical-size=500G target=iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk:2")
        self.assertExitcode(0)

        print self.dedupv1.monitor("volume")
        print self.dedupv1.monitor("target")

        print self.scst.get_devices_in_group("Default_iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk")

        check_available_lun(["0", "1", "2"])

        self.dedupv1.volumes("target remove id=8 target=iqn.2007-08.neu.jgu:dedupv1-jgu.jgu.neu.disk")
        self.assertExitcode(0)

        check_available_lun(["0", "1"])

    def test_iscsi_start_stop_with_server_auth_changed(self):
        """ test_iscsi_start_stop_with_server_auth_changed

            Tests the basic mutual iSCSI authentication with changing target authentication settings.
        """
        try:
            self.start_default_system()

            self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)

            pwd = self.dedupv1.passwd("1234567890123")
            self.dedupv1.targets("change tid=3 auth.name=test auth.secret=%s" % pwd)

            secret = self.dedupv1.passwd("testtesttest1")
            self.assertExitcode(0)

            self.dedupv1.users("add name=test secret-hash=%s" % secret)
            self.assertExitcode(0)

            self.dedupv1.users("target add name=test target=iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)

            self.dedupv1.monitor("target")

            self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0")
            self.assertExitcode(0)

            self.open_iscsi.client_auth(server="127.0.0.1",
                                 name="iqn.2005-03.info.christmann:backup:special",
                                 username="test",
                                 password="testtesttest1")
            self.open_iscsi.server_auth(server="127.0.0.1",
                                 name="iqn.2005-03.info.christmann:backup:special",
                                 username="test2",
                                 password="test2")
            self.assertExitcode(0)

            self.open_iscsi.discover(server="127.0.0.1")
            self.assertExitcode(0)

            self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertFalse(self.run.code == 0) # Login failed

            self.open_iscsi.client_auth(server="127.0.0.1",
                                 name="iqn.2005-03.info.christmann:backup:special",
                                 username="test",
                                 password="testtesttest1")
            self.open_iscsi.server_auth(server="127.0.0.1",
                                 name="iqn.2005-03.info.christmann:backup:special",
                                 username="test",
                                 password="1234567890123")
            self.assertExitcode(0)

            self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0) # correct password

        finally:
            self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")

            self.dedupv1.stop("-f")

    def test_iscsi_with_params(self):
        """ test_iscsi_with_params

            Tests iSCSI functionality with explicit target iSCSI params.
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special param.QueuedCommands=16")
        self.assertExitcode(0)

        target_config = self.run("iscsi-scst-adm --op show --tid=3")
        self.assertExitcode(0)
        self.assertTrue(target_config.find("QueuedCommands=16") >= 0)

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

        target_config = self.run("iscsi-scst-adm --op show --tid=3")
        self.assertExitcode(0)
        self.assertTrue(target_config.find("QueuedCommands=16") >= 0)

    def test_iscsi_rm_from_target(self):
        """ test_iscsi_rm_from_target

            Verifies that it is not possible to remove a volume from a target while there is a iSCSI connection.
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0 group=Default:1")
        self.assertExitcode(0)

        # try to change name with an open connection
        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)
        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        sleep(10)
        self.dedupv1.volumes("target remove id=4 target=iqn.2005-03.info.christmann:backup:special")
        self.assertNotEqual(self.run.code, 0) # this should fail

        sleep(5)
        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        sleep(10)
        self.dedupv1.volumes("target remove id=4 target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

    def test_iscsi_connect_maintainance_volume_to_running(self):
        """ test_iscsi_connect_maintainance_volume_to_running

            Tests that it is possible to change a volume in maintainance mode with an active connection (this is possible!, the initiator simple doesn't see the volume) into the running mode.
        """
        self.start_default_system()
        self.sys.rm_scst_local()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0 group=Default:1")
        self.assertExitcode(0)

        self.dedupv1.volumes("change-state id=4 state=maintenance")
        self.assertExitcode(0)

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)
        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)
        try:
            sleep(5)
            devices = os.listdir("/dev/disk/by-path")
            self.assertFalse("ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special-lun-0" in devices)

            self.dedupv1.volumes("change-state id=4 state=running")
            self.assertExitcode(0)
        finally:
            self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)

        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)
        try:
            sleep(5)
            devices = os.listdir("/dev/disk/by-path")
            self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special-lun-0" in devices)
        finally:
            self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)

    def test_iscsi_connect_maintainance_volume(self):
        """ test_iscsi_connect_maintainance_volume

            Tests that it is possible to connect to a volume in maintainance mode. This is possible!, the initiator simple doesn't see the volume.
        """
        self.start_default_system()
        self.sys.rm_scst_local()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup2 logical-size=500G target=iqn.2005-03.info.christmann:backup:special:0 group=Default:1")
        self.assertExitcode(0)

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)
        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)
        try:
            sleep(5)
            devices = os.listdir("/dev/disk/by-path")
            self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special-lun-0" in devices)

            self.dedupv1.volumes("change-state id=4 state=maintenance")
            self.assertNotEqual(self.run.code, 0) # this should fail
        finally:
            self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)

        self.dedupv1.volumes("change-state id=4 state=maintenance")
        self.assertExitcode(0)

        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)
        try:
            sleep(5)
            devices = os.listdir("/dev/disk/by-path")
            self.assertFalse("ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special-lun-0" in devices)
        finally:
            self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)

    def test_iscsi_change_target_name(self):
        """ test_iscsi_change_target_name

            Tests that it is possible to change the name of an target. The test also verifies that it is not possible to change the target name while there is an iSCSI connection.
        """
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        # try to change name with an open connection
        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)
        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        sleep(10)
        self.dedupv1.targets("change tid=3 name=iqn.2005-03.info.christmann:backup:special2")
        self.assertNotEqual(self.run.code, 0) # this should fail

        sleep(5)
        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        sleep(10)
        self.dedupv1.targets("change tid=3 name=iqn.2005-03.info.christmann:backup:special2")
        self.assertExitcode(0)

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)

        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special2")
        self.assertExitcode(0)

        sleep(5)
        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special2")
        self.assertExitcode(0)

    def test_iscsi_change_target_name_with_volume_and_user(self):
        """ test_iscsi_change_target_name_with_volume_and_user:

            Tests the renaming a target with a volume and a user. The issue #262 is related.
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G")
        self.assertExitcode(0)

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("target add id=1 target=iqn.2005-03.info.christmann:backup:special:0")
        self.assertExitcode(0)

        secret = self.dedupv1.passwd("testtesttest1")
        self.assertExitcode(0)

        self.dedupv1.users("add name=test secret-hash=%s" % secret)
        self.assertExitcode(0)

        self.dedupv1.users("target add name=test target=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.targets("change tid=3 name=iqn.2005-03.info.christmann:backup:special2")
        self.assertExitcode(0)

        self.assertTrue("backup1" in self.scst.get_devices_in_group("Default_iqn.2005-03.info.christmann:backup:special2"))

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)

        self.open_iscsi.client_auth(server="127.0.0.1",
                     name="iqn.2005-03.info.christmann:backup:special2",
                     username="test",
                     password="testtesttest1")
        self.assertExitcode(0)

        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special2")
        self.assertExitcode(0)
        try:
            sleep(5)
            devices = os.listdir("/dev/disk/by-path")
            self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special2-lun-0" in devices)
            volume_size = self.run("blockdev --getsize64 /dev/disk/by-path/ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special2-lun-0")
            self.assertEqual("536870912000", volume_size.strip())

        finally:
           self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special2")
           self.assertExitcode(0)

        volume1 = self.dedupv1.monitor("volume")["1"]
        print volume1
        self.assertEquals(volume1["targets"][0]["name"], "iqn.2005-03.info.christmann:backup:special2")

        self.dedupv1.volumes("target remove id=1 target=iqn.2005-03.info.christmann:backup:special2")
        self.assertExitcode(0)


    def test_iscsi_change_target_name_with_volumes(self):
        """ test_iscsi_change_target_name_with_volumes

            Tests the renaming a target with a volume.
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G")
        self.assertExitcode(0)

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.dedupv1.volumes("target add id=1 target=iqn.2005-03.info.christmann:backup:special:0")
        self.assertExitcode(0)

        # try to change name with an open connection
        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)
        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        sleep(10)
        devices = os.listdir("/dev/disk/by-path")
        self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special-lun-0" in devices)
        volume_size = self.run("blockdev --getsize64 /dev/disk/by-path/ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special-lun-0")
        self.assertEqual("536870912000", volume_size.strip())

        self.dedupv1.targets("change tid=3 name=iqn.2005-03.info.christmann:backup:special2")
        self.assertNotEqual(self.run.code, 0) # this should fail

        sleep(5)
        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        sleep(10)
        self.dedupv1.targets("change tid=3 name=iqn.2005-03.info.christmann:backup:special2")
        self.assertExitcode(0)

        self.assertTrue("backup1" in self.scst.get_devices_in_group("Default_iqn.2005-03.info.christmann:backup:special2"))

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)

        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special2")
        self.assertExitcode(0)

        sleep(5)
        devices = os.listdir("/dev/disk/by-path")
        self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special2-lun-0" in devices)
        volume_size = self.run("blockdev --getsize64 /dev/disk/by-path/ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special2-lun-0")
        self.assertEqual("536870912000", volume_size.strip())

        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special2")
        self.assertExitcode(0)

        volume1 = self.dedupv1.monitor("volume")["1"]
        print volume1
        self.assertEquals(volume1["targets"][0]["name"], "iqn.2005-03.info.christmann:backup:special2")

        self.dedupv1.volumes("target remove id=1 target=iqn.2005-03.info.christmann:backup:special2")
        self.assertExitcode(0)

    def test_iscsi_two_volumes_per_target(self):
        """ test_iscsi_two_volumes_per_target

            Tests the iSCSI connection with two volumes in a target.
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=2 device-name=backup2 logical-size=500G")
        self.assertExitcode(0)

        self.dedupv1.targets("add tid=2 name=iqn.2010.04:example")
        self.assertExitcode(0)

        self.dedupv1.volumes("target add id=1 target=iqn.2010.04:example:0")
        self.assertExitcode(0)

        self.dedupv1.volumes("target add id=2 target=iqn.2010.04:example:1")
        self.assertExitcode(0)

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)

        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

        sleep(10)
        devices = os.listdir("/dev/disk/by-path")
        self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2010.04:example-lun-0" in devices)
        self.assertTrue("ip-127.0.0.1:3260-iscsi-iqn.2010.04:example-lun-1" in devices)

        self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2010.04:example")
        self.assertExitcode(0)

    def test_iscsi_copy_images_with_cp(self, idle_time=600):
        """ test_iscsi_copy_images_with_cp
        """
        self.adapt_config_file("log.max-log-size=.*",
                             "log.max-log-size=64M")
        self.start_default_system()

        copied_image_count = 0
        number_of_images = self.configuration.get("acronis image count", 12)
        test_data_dir = self.configuration.get("test data dir", None)
        image_path = os.path.join(test_data_dir, "acronis-images", "pc1")
        self.assertTrue(os.path.exists(image_path), "Image path doesn't exists")

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)
        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        try:
            self.data.device_name = "/dev/disk/by-path/ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special-lun-0"
            self.sys.device_name = "/dev/disk/by-path/ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special-lun-0"
            self.prepare_part()

            start_time = time()
            total_data_size = 0

            os.mkdir(os.path.join(self.mnt_point, "images"))
            for root, dirs, files in os.walk(image_path):
                for file in files:
                    pathname = os.path.join(root, file)

                    dest_file = os.path.join(self.mnt_point, "images", file)
                    print "Copy", file

                    cp_cmd = "cp %s %s" % (pathname, dest_file)
                    self.run(cp_cmd)
                    total_data_size += os.path.getsize(pathname)
                    copied_image_count += 1

                    if number_of_images and copied_image_count >= number_of_images:
                        break

            end_time = time()
            print "Copy time %s" % (end_time - start_time)
            print "Total data size %s MB" % (total_data_size / (1024 * 1024.0))

            self.sys.umount()
            sleep(10)

            self.sys.fsck_ext3()
            self.assertExitcode(0)

            self.assertNoLoggedErrors()
        finally:
            self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)

class Dedupv1OverflowSystemTest(Dedupv1BaseSystemTest):
    def test_memory_lower_parachute(self):
        """ test_memory_lower_parachute

            Tests that the system shuts down if there is less memory then needed for parachute.

            The test only tests if the start fails. It would be nice to be able to scan the log
            file for the following message:

                Malloc for memory parachute failed.
        """
        self.adapt_config_file("daemon.max-memory=.*",
                             "daemon.max-memory=512M")
        self.adapt_config_file("daemon.memory-parachute=.*",
                             "daemon.memory-parachute=1G")

        self.dedupv1.start("--create")
        self.assertExitcode(1)
        self.assertTrue("ERROR" in self.dedupv1.monitor("status"))

    def test_memory_to_small(self):
        """ test_memory_to_small

            Tests that the system shuts down if there is less memory then needed to start the
            whole system, but enough to stat the memory parachute.

            The test only tests if the start fails. It would be nice to be able to scan the log
            file for the following message:

                Machine is running out of memory, will shutdown now.
        """
        self.adapt_config_file("daemon.max-memory=.*",
                             "daemon.max-memory=512M")
        self.adapt_config_file("daemon.memory-parachute=.*",
                             "daemon.memory-parachute=256M")

        self.dedupv1.start("--create")
        self.assertExitcode(1)
        self.assertTrue("ERROR" in self.dedupv1.monitor("status"))

    def test_aux_block_index_full(self):
        """ test_aux_block_index_full

            Tests that a overfull auxiliary block index doesn't cause critical errors.
            This is testing by choosing a very small auxiliary index size and than
            writing a 1GB of data.
        """
        self.adapt_config_file("block-index.max-auxiliary-size=.*",
                             "block-index.max-auxiliary-size=2K")

        self.start_default_system()

        size = 1024
        filename = self.get_urandom_file(size)
        self.data.copy_raw(filename, size)

    def test_chunk_index_cache_full(self):
        """ test_chunk_index_cache_full

            Tests that a overfull chunk index cache doesn't cause critical errors.
            This is testing by choosing a very small auxiliary index size and than
            writing a 1GB of data.
        """
        self.adapt_config_file("chunk-index.persistent.write-cache.max-page-count=.*",
                             "chunk-index.persistent.write-cache.max-page-count=16K")
        self.adapt_config_file("chunk-index.persistent.page-lock-count=.*",
                             "chunk-index.persistent.page-lock-count=256")
        self.adapt_config_file("chunk-index.persistent.write-cache.bucket-count=.*",
                             "chunk-index.persistent.write-cache.bucket-count=512")

        self.start_default_system()

        size = 1024
        filename = self.get_urandom_file(size)
        self.data.copy_raw(filename, size)

        trace_data = self.dedupv1.monitor("trace")
        self.assertTrue(trace_data["core"]["chunk index"]["index"]["write back"]["evict count"] > 0, "Without eviction this is a bad test")

    def test_iscsi_log_overflow(self, idle_time=600):
        """ test_iscsi_log_overflow

            Tests a log overflow with copying data of iSCSI. The log
            is too small to process all requests without a (partial) log replay.
        """
        self.adapt_config_file("log.max-log-size=.*",
                             "log.max-log-size=64M")
        self.start_default_system()

        self.dedupv1.targets("add tid=3 name=iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        self.open_iscsi.discover(server="127.0.0.1")
        self.assertExitcode(0)
        self.open_iscsi.connect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
        self.assertExitcode(0)

        try:
            self.data.device_name = "/dev/disk/by-path/ip-127.0.0.1:3260-iscsi-iqn.2005-03.info.christmann:backup:special-lun-0"

            size = 4 * 1024
            filename = self.get_urandom_file(size)

            md5_1 = self.data.read_md5(size, filename)
            print "Data md5", md5_1

            self.data.copy_raw(filename, size)
            md5_2 = self.data.read_md5(size)
            print "After write md5", md5_2
            self.assertEqual(md5_1, md5_2)

            self.dedupv1.monitor("idle", "force-idle=true")
            sleep(idle_time)

            self.data.copy_raw(filename, size / 2)
            md5_3 = self.data.read_md5(size / 2)
            print "Overwrite md5", md5_3
            self.dedupv1.monitor("idle", "force-idle=true")
            sleep(idle_time / 2)

            md5_4 = self.data.read_md5(size / 2)
            self.assertEqual(md5_3, md5_4)
            print "After restart md5", md5_4

            self.assertNoLoggedErrors()
        finally:
            self.open_iscsi.disconnect(server="127.0.0.1", name="iqn.2005-03.info.christmann:backup:special")
            self.assertExitcode(0)

    def test_log_overflow_crash(self, idle_time=600):
        """ test_log_overflow_crash

            Tests the handling of a log overflow with a following crash. The log
            is too small to process all requests without a (partial) log replay.
            1 GB of data are copied.
        """
        self.adapt_config_file("log.max-log-size=.*",
                             "log.max-log-size=16M")
        self.start_default_system()

        size = 1 * 1024
        filename = self.get_urandom_file(size)

        md5_1 = self.data.read_md5(size, filename)
        print "Data md5", md5_1

        self.data.copy_raw(filename, size)
        md5_2 = self.data.read_md5(size)
        print "After write md5", md5_2
        self.assertEqual(md5_1, md5_2)

        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(idle_time)

        self.data.copy_raw(filename, size / 2)
        md5_3 = self.data.read_md5(size / 2)
        print "Overwrite md5", md5_3
        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(idle_time / 2)

        sleep(15)
        self.sys.rm_scst_local()
        sleep(15)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        self.dedupv1.start()
        self.assertExitcode(0)

        sleep(15)
        self.sys.add_scst_local()
        sleep(15)

        md5_4 = self.data.read_md5(size / 2)
        self.assertEqual(md5_3, md5_4)
        print "After restart md5", md5_4

        self.assertNoLoggedErrors()

    def test_log_overflow_crash_without_idle(self):
        """ test_log_overflow_crash_without_idle

            Tests the handling of a log overflow with a following crash, but without idle time for a nice log replay. The log is too small to process all requests without a (partial) log replay.
        """
        self.adapt_config_file("log.max-log-size=.*",
                             "log.max-log-size=16M")
        self.start_default_system()

        size = 1 * 1024
        filename = self.get_urandom_file(size)

        md5_1 = self.data.read_md5(size, filename)
        print "Data md5", md5_1

        self.data.copy_raw(filename, size)
        md5_2 = self.data.read_md5(size)
        print "After write md5", md5_2
        self.assertEqual(md5_1, md5_2)

        sleep(10)

        self.data.copy_raw(filename, size / 2)
        md5_3 = self.data.read_md5(size / 2)
        print "Overwrite md5", md5_3

        sleep(15)
        self.sys.rm_scst_local()
        sleep(15)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        self.dedupv1.start()
        self.assertExitcode(0)

        sleep(15)
        self.sys.add_scst_local()
        sleep(15)

        md5_4 = self.data.read_md5(size / 2)
        self.assertEqual(md5_3, md5_4)
        print "After restart md5", md5_4

        # Because of the kill we could have a warn if the first element to replay is written correctly.
        self.assertLoggedWarning(max=1);
        self.assertNoLoggedErrors()

    def test_block_index_full(self, idle_time=120):
        """ test_block_index_full

            Tests the handling of a full block index. We create a very small block index and then copy 1 GB of data.
        """
        config_data = open(self.dedupv1.config, "r").read()
        if re.search("block-index.persistent=tc-disk-.*", config_data):
            self.adapt_config_file("block-index.persistent.buckets=.*",
                          "block-index.persistent.buckets=128")
        elif re.search("block-index.persistent=sqlite-disk-btree", config_data):
            self.adapt_config_file("block-index.persistent.max-item-count=.*",
                          "block-index.persistent.max-item-count=256")
        else:
            self.fail("Illegal configuration for test_block_index_full")
        self.start_default_system()

        size = 1024
        filename = self.get_urandom_file(size)
        self.data.copy_raw(filename, size)

        sleep(idle_time)

        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        self.dedupv1.start()
        self.assertExitcode(0)

        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(idle_time)

        self.assertNoLoggedErrors()

    def test_chunk_index_full(self):
        """ test_chunk_index_full

            Tests the handling of a full chunk index.We create a very small chunk index and then copy 1 GB of data.
        """
        self.adapt_config_file("chunk-index.persistent.size=.*",
                          "chunk-index.persistent.size=4M")

        self.start_default_system()

        size = 1024
        filename = self.get_urandom_file(size)
        self.data.copy_raw(filename, size)

    def test_storage_full(self):
        """ test_storage_full

            Tests the handling of a full container storage. We create a very small container storage and then copy 1 GB of unique data.
        """
        self.adapt_config_file("storage.size=.*", "storage.size=1024M")

        self.start_default_system()

        size = 2048
        filename = self.get_urandom_file(size)
        self.data.copy_raw(filename, size)

class Dedupv1DataSystemTest(Dedupv1BaseSystemTest):

    def extract_kernel(self, extract=True):
        def extract_kernel_source(major, minor):
            name = "linux-2.6.%s.%s.tar" % (major, minor)
            self.run("tar -xf %s" % (os.path.join(kernel_path, name)), cwd=self.mnt_point)
            self.assertExitcode(0)

        def copy_kernel_tarball(major, minor):
            name = "linux-2.6.%s.%s.tar" % (major, minor)
            shutil.copy(os.path.join(kernel_path, name), self.mnt_point)

        test_data_dir = self.configuration.get("test data dir", None)
        kernel_path = os.path.join(test_data_dir, "kernel")
        kernel_versions = [(31, 1), (31, 2), (31, 3), (31, 4), (31, 5), (31, 6), (31, 7), (31, 8), (31, 9), (31, 10), (31, 11), (31, 12),
                   (32, 1), (32, 2), (32, 3), (32, 4), (32, 5), (32, 6), (32, 7), (32, 8)
                   ]

        self.sys.prepare_part()

        for (major, minor) in kernel_versions:
            if extract:
                extract_kernel_source(major, minor)
            else:
                copy_kernel_tarball(major, minor)

            self.sys.umount()
            self.assertExitcode(0)

            self.sys.fsck_ext3()
            self.assertExitcode(0)

            self.sys.mount()
            self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_extract_kernel(self):
        """ test_extract_kernel

            Extracts a series of kernel sources. The result is validated using a series of fsck runs.
        """
        self.start_default_system()
        self.extract_kernel()

    def test_extract_kernel_4k(self):
        """ test_extract_kernel_4k

            Extracts a series of kernel sources. The dedup system is configured with 4k chunks. The result is validated using a series of fsck runs.
        """
        self.adapt_config_file("chunking=rabin", "chunking=rabin\nchunking.min-chunk-size=1K\nchunking.avg-chunk-size=4K\nchunking.max-chunk-size=16K")

        self.start_default_system()
        self.extract_kernel()

    def test_copy_kernel(self):
        """ test_copy_kernel

            Copies a series of kernel sources archives to a filesystem. The result is validated using a series of fsck runs.
        """
        self.start_default_system()
        self.extract_kernel(extract=False)

    def test_copy_kernel_4k(self):
        """ test_copy_kernel_4k
        """
        self.adapt_config_file("chunking=rabin", "chunking=rabin\nchunking.min-chunk-size=1K\nchunking.avg-chunk-size=4K\nchunking.max-chunk-size=16K")

        self.start_default_system()
        self.extract_kernel(extract=False)

    def test_urandom_kill_after_replay(self):
        """ test_urandom_kill_after_replay
            tests if the data is valid after a full log replay, but with a fast shutdown.
            This ensures that the background log replay really persists all necessary data
        """
        self.start_default_system()

        size = 1024

        filename = self.get_urandom_file(size)

        md5_1 = self.data.read_md5(size, filename)
        print "Data md5", md5_1

        self.data.copy_raw(filename, size)
        md5_2 = self.data.read_md5(size)
        print "After write md5", md5_2
        self.assertEqual(md5_1, md5_2)

        self.dedupv1.monitor("flush")

        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(60)

        md5_3 = self.data.read_md5(size)
        print "After replay md5", md5_3
        self.assertEqual(md5_1, md5_3)
        self.sys.rm_scst_local()

        self.dedupv1.stop("-f")
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

        self.sys.add_scst_local()
        md5_4 = self.data.read_md5(size)
        print "After restart md5", md5_4
        self.assertEqual(md5_1, md5_4)

    def test_urandom_volume_detach(self):
        """ test_urandom_volume_detach
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G group=Default:1 chunking=rabin chunking.avg-chunk-size=16K chunking.max-chunk-size=32K")
        self.assertExitcode(0)

        self.sys.rm_scst_local()
        self.sys.add_scst_local()

        size = 1024
        filename = self.get_urandom_file(size)

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:1"

        self.data.copy_raw(filename, size)

        self.dedupv1.monitor("flush")

        sleep(5)
        self.sys.rm_scst_local()

        self.dedupv1.volumes("group remove id=1 group=Default")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=1")
        self.assertExitcode(0)

        self.dedupv1.monitor("idle", "force-idle=true")

        sleep(60)

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_urandom_kill_after_volume_detach(self):
        """ test_urandom_kill_after_volume_detach
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G group=Default:1 chunking=rabin chunking.avg-chunk-size=16K chunking.max-chunk-size=32K")
        self.assertExitcode(0)

        self.sys.rm_scst_local()
        self.sys.add_scst_local()

        size = 1024
        filename = self.get_urandom_file(size)

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:1"

        self.data.copy_raw(filename, size)

        self.dedupv1.monitor("flush")

        sleep(5)
        self.sys.rm_scst_local()

        self.dedupv1.volumes("group remove id=1 group=Default")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=1")
        self.assertExitcode(0)

        self.dedupv1.monitor("idle", "force-idle=true")

        kill_proc = self.timed_kill_daemon(10, 9)
        kill_proc.join()

        # We had very often here the problem, that dedupv1d was not able to attach to the port.
        # So we wait now until it is free, but no longer then 60 seconds

        times = 60
        (process_name, pid) = get_listening_process(self.run, 9001)
        while (process_name and times > 0):
            sleep(1)
            (process_name, pid) = get_listening_process(self.run, 9001)

        if (process_name):
            print "port 9001 is not free, so next call will most probably fail"
            # If start still fails because dedupv1 can not attach to port 9001 and this message
            # does not appear, we have to do something in dedupv1d (try connect and wait there)
            # or we just have to wait here for some seconds in any case.

        # As the above did not help I sleep now for 5 seconds in any case
        sleep(5)

        self.dedupv1.start("--force")
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_urandom_kill_after_volume_detach_no_kill(self):
        """ test_urandom_kill_after_volume_detach_no_kill

            This is actually a bit different from the kill situation
        """
        self.start_default_system()

        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G group=Default:1 chunking=rabin chunking.avg-chunk-size=16K chunking.max-chunk-size=32K")
        self.assertExitcode(0)

        self.sys.rm_scst_local()
        self.sys.add_scst_local()

        size = 1024
        timeout = 300
        filename = self.get_urandom_file(size)

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:1"

        self.data.copy_raw(filename, size)

        self.dedupv1.monitor("flush")

        sleep(5)
        self.sys.rm_scst_local()

        self.dedupv1.volumes("group remove id=1 group=Default")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=1")
        self.assertExitcode(0)

        self.dedupv1.monitor("idle", "force-idle=true")

        # Wait until all replayed
        open_event_count = self.dedupv1.monitor("log")["open events"]
        while open_event_count > 64:
            sleep(30)
            open_event_count = self.dedupv1.monitor("log")["open events"]

        # Wait a bit more
        sleep(timeout)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_urandom3x128_volume2_detach(self):
        """ test_urandom3x128_volume2_detach
        """
        self.start_default_system()

        print "creating volume #2"
        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G group=Default:1 chunking=rabin chunking.avg-chunk-size=16K chunking.max-chunk-size=32K")
        self.assertExitcode(0)

        print "creating volume #3"
        self.dedupv1.volumes("attach id=2 device-name=backup2 logical-size=500G group=Default:2 chunking=rabin chunking.avg-chunk-size=16K chunking.max-chunk-size=32K")
        self.assertExitcode(0)

        size = 128
        filename = self.get_urandom_file(size)
        md5_0 = self.data.read_md5(size, filename)
        print "Data md5", md5_0

        print "writing data on volume #1"
        self.data.copy_raw(filename, size)
        self.dedupv1.monitor("flush")

        md5_1 = self.data.read_md5(size)
        print "volume#1 md5", md5_1
        self.assertEqual(md5_0, md5_1)

        self.sys.rm_scst_local()
        self.sys.add_scst_local()

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:1"

        print "writing data on volume #2"
        self.data.copy_raw(filename, size)
        self.dedupv1.monitor("flush")

        md5_2 = self.data.read_md5(size)
        print "volume#2 md5", md5_2
        self.assertEqual(md5_0, md5_2)

        sleep(5)
        self.sys.rm_scst_local()

        print "detaching volume #2"
        self.dedupv1.volumes("group remove id=1 group=Default")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=1")
        self.assertExitcode(0)

        self.sys.rm_scst_local()
        self.sys.add_scst_local()

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:2"

        print "writing data on volume #3"
        self.data.copy_raw(filename, size)
        self.dedupv1.monitor("flush")

        md5_2 = self.data.read_md5(size)
        print "volume#3 md5", md5_2
        self.assertEqual(md5_0, md5_2)

        print "forcing idle replay"
        sleep(5)
        self.sys.rm_scst_local()
        self.dedupv1.monitor("idle", "force-idle=true")

        sleep(60)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_urandom3x128_volume2_detach_kill(self):
        """ test_urandom3x128_volume2_detach_kill
        """
        self.start_default_system()

        print "creating volume #2"
        self.dedupv1.volumes("attach id=1 device-name=backup1 logical-size=500G group=Default:1 chunking=rabin chunking.avg-chunk-size=16K chunking.max-chunk-size=32K")
        self.assertExitcode(0)

        print "creating volume #3"
        self.dedupv1.volumes("attach id=2 device-name=backup2 logical-size=500G group=Default:2 chunking=rabin chunking.avg-chunk-size=16K chunking.max-chunk-size=32K")
        self.assertExitcode(0)

        size = 128
        filename = self.get_urandom_file(size)
        md5_0 = self.data.read_md5(size, filename)
        print "Data md5", md5_0

        print "writing data on volume #1"
        self.data.copy_raw(filename, size)
        self.dedupv1.monitor("flush")

        md5_1 = self.data.read_md5(size)
        print "volume#1 md5", md5_1
        self.assertEqual(md5_0, md5_1)

        self.sys.rm_scst_local()
        self.sys.add_scst_local()

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:1"

        print "writing data on volume #2"
        self.data.copy_raw(filename, size)
        self.dedupv1.monitor("flush")

        md5_2 = self.data.read_md5(size)
        print "volume#2 md5", md5_2
        self.assertEqual(md5_0, md5_2)

        sleep(5)
        self.sys.rm_scst_local()

        print "detaching volume #2"
        self.dedupv1.volumes("group remove id=1 group=Default")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=1")
        self.assertExitcode(0)

        self.sys.rm_scst_local()
        self.sys.add_scst_local()

        self.data.device_name = "/dev/disk/by-path/scsi-0:0:0:2"

        print "writing data on volume #3"
        self.data.copy_raw(filename, size)
        self.dedupv1.monitor("flush")

        md5_2 = self.data.read_md5(size)
        print "volume#3 md5", md5_2
        self.assertEqual(md5_0, md5_2)

        print "killing dedupv1"
        kill_proc = self.timed_kill_daemon(1, 9)
        kill_proc.join()

        self.dedupv1.start("--force")
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_copy_urandom_overwrite_block_often(self):
        """ test_copy_urandom_overwrite_block_often
        """
        rewrite_count = 1000
        copy_size = 16
        input = self.get_urandom_file(32768)

        self.start_default_system()

        for i in xrange(0, rewrite_count):
            self.data.copy_raw(input, copy_size, skip=i * copy_size)

    def test_copy_urandom_overwrite_block_often_with_kills(self):
        """ test_copy_urandom_overwrite_block_often_with_kills
        """
        rewrite_count = 1000
        copy_size = 16
        input = self.get_urandom_file(32 * 1024)
        kill_interval = 100

        self.start_default_system()

        for i in xrange(0, rewrite_count):
            self.data.copy_raw(input, copy_size, skip=i * copy_size)
            pid = self.scst.get_pid()

            # kill every kill_interval's overwrite
            if ((i % kill_interval) == 0):

                if (i % kill_interval * 4 == 0):
                    self.dedupv1.monitor("idle", "force-idle=true")
                    sleep(10)
                else:
                    sleep(2)

                self.timed_kill_daemon(0, 9).join()

                wait_times = 0
                while self.is_process_running(pid) and (wait_times < 60):
                    wait_times += 1
                    print "After %s seconds pid is still existing..." % str(wait_times)
                    sleep(1)

                if self.is_process_running(pid):
                    print "Process is still running, so the following start will fail..."

                self.dedupv1.start()
                self.assertExitcode(0)
                self.assertNoLoggedErrors()

    def test_kill_after_sync(self):
        """ test_kill_after_sync
        """
        self.adapt_config_file("\n$", "\nstorage.timeout-commit-timeout=16")
        self.start_default_system()

        size = 128
        filename = self.get_urandom_file(size)

        print "writing data on volume #1"
        self.data.copy_raw(filename, size)
        self.dedupv1.monitor("flush", "flush=true")

        print "killing dedupv1"
        kill_proc = self.timed_kill_daemon(0, 9)
        kill_proc.join()

        self.dedupv1.start("--force")
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_kill_often(self, timeout_mean=32, step_size=128):
        """ test_kill_often
        """
        kill_count = 32

        self.start_default_system()

        for i in xrange(0, kill_count):
            print "Kill round", i
            pid = self.scst.get_pid()
            timeout = random.randint(timeout_mean / 2, timeout_mean * 3 / 2)
            kill_proc = self.timed_kill_daemon(timeout, 9)

            self.data.copy_raw("/dev/urandom", step_size, seek=i * step_size)

            kill_proc.join()

            # We had some issues, with failed restarts, because dedupv1d was still running
            # (the pid existed). I assume that the process could not be killed fast enough,
            # because there were to many IO/ops. So I check if it is still running and wait a little longer.
            wait_times = 0
            while self.is_process_running(pid) and (wait_times < 60):
                wait_times += 1
                print "After %s seconds pid is still existing..." % str(wait_times)
                sleep(1)

            if self.is_process_running(pid):
                print "Process is still running, so the following start will fail..."

            self.dedupv1.start()
            self.assertExitcode(0)
            self.assertNoLoggedErrors()

        #self.repair_during_tearDown = True


    def test_kill_often_idle(self):
        """ test_kill_often_idle

            Tests the crash recovery during idle time
        """
        kill_count = 32
        size = 2048
        timeout_mean = 180

        self.start_default_system()

        # Prepare the system with test data that can be gc'ed
        filename = self.get_urandom_file(size)
        for i in xrange(16):
            self.data.copy_raw(filename, size / 16, seek=i * 16)

        self.data.copy_raw("/dev/zero", size, seek=size / 16)

        for i in xrange(0, kill_count):
            print "Kill round", i

            self.dedupv1.monitor("idle", "force-idle=true")

            timeout = random.randint(timeout_mean / 2, timeout_mean * 3 / 2)
            kill_proc = self.timed_kill_daemon(timeout, 9)
            kill_proc.join()

            self.dedupv1.start()
            self.assertExitcode(0)
            self.assertNoLoggedErrors()

    def test_copy_urandom_overwrite(self, timeout=180):
        """ test_copy_urandom_overwrite

            Begin with a normal 1GB urandom copy and than
            overwrite small parts of the data.
        """
        self.start_default_system()

        self.dedupv1.monitor("idle", "change-idle-tick-interval=1")
        size = 1024

        filename = "/mnt/san/data/random"
        if not os.path.exists(filename) or not os.path.getsize(filename) * (size * 1024 * 1024):
            self.run("dd if=/dev/urandom of=%s bs=1M count=%s" % (filename, size))
            self.assertEqual(os.path.getsize(filename), size * 1024 * 1024)

        self.data.copy_raw(filename, size)

        for i in xrange(0, size, 10):
            self.data.copy_raw("/dev/urandom", 3, seek=i)

        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(timeout)

        for i in xrange(4, size, 10):
            self.data.copy_raw("/dev/urandom", 3, seek=i)

        self.dedupv1.monitor("idle", "force-idle=true")

        stats_mon = self.dedupv1.monitor("trace")
        self.assertEqual(stats_mon["logging"]["error count"], 0)

        self.sys.rm_scst_local()

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

    def test_read_while_replay(self, timeout=180):
        """ test_read_while_replay

            Begin with a normal 1GB urandom copy and than
            overwrite small parts of the data.
        """
        self.start_default_system()

        self.dedupv1.monitor("idle", "change-idle-tick-interval=1")

        size = 512
        filename = self.get_urandom_file(size)
        self.data.copy_raw(filename, size)

        for i in xrange(0, size, 8):
            self.data.copy_raw("/dev/urandom", 4, seek=i)

        md5_orig = self.data.read_md5(size)

        self.dedupv1.monitor("idle", "force-idle=true")

        # Wait until all replayed
        open_event_count = self.dedupv1.monitor("log")["open events"]
        while open_event_count > 10:
            md5_2 = self.data.read_md5(size)
            self.assertEqual(md5_orig, md5_2)

            open_event_count = self.dedupv1.monitor("log")["open events"]
            print "Wait until log replayed: ", open_event_count

        # Wait a bit more
        start_time = time()
        while (time() - start_time) < timeout:
            md5_2 = self.data.read_md5(size)
            self.assertEqual(md5_orig, md5_2)
            print "Wait until timeout: ", (time() - start_time)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        self.sys.rm_scst_local()

        self.dedupv1.stop()
        self.assertExitcode(0)

    def test_simple_filesystem(self):
        """ test_simple_filesystem
        """
        self.start_default_system()
        self.prepare_part()

        self.sys.umount()
        self.assertExitcode(0)

        self.sys.fsck_ext3()
        self.assertExitcode(0)

        self.sys.rm_scst_local()

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        self.dedupv1.stop("-f")
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

        self.sys.add_scst_local()

        self.sys.fsck_ext3()

    def test_copy_images_full_replay(self, idle_time=120):
        """ test_copy_images_full_replay

            Copies a series of backup images, followed by a restart, a complete replay and some time of garbage collection.
        """
        self.start_default_system()

        self.sys.prepare_part()

        number_of_images = self.configuration.get("acronis image count", 12)
        test_data_dir = self.configuration.get("test data dir", None)
        image_path = os.path.join(test_data_dir, "acronis-images", "pc1")

        copied_image_count = 0
        os.mkdir(os.path.join(self.mnt_point, "images"))
        for root, dirs, files in os.walk(image_path):
            for file in files:
                pathname = os.path.join(root, file)

                dest_file = os.path.join(self.mnt_point, "images", file)
                print "Copy", file

                shutil.copyfile(pathname, dest_file)
                copied_image_count += 1

                if number_of_images and copied_image_count >= number_of_images:
                    break

        for root, dirs, files in os.walk(os.path.join(self.mnt_point, "images")):
            for file in files:
                    pathname = os.path.join(root, file)
                    src_file = os.path.join(image_path, file)

                    src_md5 = self.data.read_md5_file(src_file)
                    dest_md5 = self.data.read_md5_file(pathname)
                    print "Src md5", src_md5, ", dest md5", dest_md5
                    self.assertEquals(src_md5, dest_md5)

        self.sys.umount()

        self.sys.fsck_ext3()
        self.assertExitcode(0)

        self.sys.rm_scst_local()
        sleep(4)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        self.dedupv1.stop("-f")
        self.assertExitcode(0)

        start_time = time()

        self.dedupv1.replay()
        self.assertExitcode(0)

        end_time = time()
        print "Replay time %s" % (end_time - start_time)

        # First check
        self.dedupv1.check()
        self.assertExitcode(0)

        # Let the gc do it work
        self.dedupv1.start()
        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(idle_time)

        # Recheck
        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_copy_images_replay(self, idle_time=300):
        """ test_copy_images_replay

            Copies a series of backup images with a severe replay time between the images
        """
        self.start_default_system()

        self.prepare_part()

        copied_image_count = 0
        number_of_images = self.configuration.get("acronis image count", 12)
        test_data_dir = self.configuration.get("test data dir", None)
        image_path = os.path.join(test_data_dir, "acronis-images", "pc1")

        os.mkdir(os.path.join(self.mnt_point, "images"))
        for root, dirs, files in os.walk(image_path):
            for file in files:
                pathname = os.path.join(root, file)

                dest_file = os.path.join(self.mnt_point, "images", file)
                print "Copy", file

                shutil.copyfile(pathname, dest_file)
                copied_image_count += 1

                self.dedupv1.monitor("idle", "force-idle=true")
                sleep(idle_time)

                if number_of_images and copied_image_count >= number_of_images:
                    break
        self.sys.umount()
        sleep(10)

        self.sys.fsck_ext3()
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(idle_time)
        self.dedupv1.monitor("idle", "force-idle=false")

        self.sys.mount()
        self.assertExitcode(0)
        self.assertTrue(os.path.ismount(self.mnt_point))

        for root, dirs, files in os.walk(os.path.join(self.mnt_point, "images")):
            for file in files:
                    pathname = os.path.join(root, file)
                    src_file = os.path.join(image_path, file)

                    src_md5 = self.data.read_md5_file(src_file)
                    dest_md5 = self.data.read_md5_file(pathname)
                    print "Src md5", src_md5, ", dest md5", dest_md5
                    self.assertEquals(src_md5, dest_md5)

        self.sys.umount()
        sleep(10)

        self.sys.fsck_ext3()
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_copy_images_bytecompare(self, idle_time=300):
        """ test_copy_images_bytecompare

            Copies a series of backup images with the bytecompare filter enabled.
        """
        self.adapt_config_file("filter=bytecompare-filter\nfilter.enabled=false",
            "filter=bytecompare-filter")
        self.start_default_system()

        self.sys.prepare_part()

        copied_image_count = 0
        number_of_images = 2
        test_data_dir = self.configuration.get("test data dir", None)
        image_path = os.path.join(test_data_dir, "acronis-images", "pc1")

        os.mkdir(os.path.join(self.mnt_point, "images"))
        for root, dirs, files in os.walk(image_path):
            for file in files:
                pathname = os.path.join(root, file)

                dest_file = os.path.join(self.mnt_point, "images", file)
                print "Copy", file
                shutil.copyfile(pathname, dest_file)
                copied_image_count += 1

                if number_of_images and copied_image_count >= number_of_images:
                    break
        self.sys.umount()
        sleep(10)

        self.sys.fsck_ext3()
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        self.dedupv1.stop()
        self.assertExitcode(0)

    def test_copy_images_double_copy(self, idle_time=300):
        """ test_copy_images_double_copy

            Copies a series of backup images, replayes the complete log and copies the same set of data again
        """
        def copy_images():
            copied_image_count = 0
            for root, dirs, files in os.walk(image_path):
                for file in files:
                    pathname = os.path.join(root, file)

                    dest_file = os.path.join(self.mnt_point, "images", file)
                    print "Copy", file

                    shutil.copyfile(pathname, dest_file)
                    copied_image_count += 1

                    if number_of_images and copied_image_count >= number_of_images:
                        break

        def validate_images():
            for root, dirs, files in os.walk(os.path.join(self.mnt_point, "images")):
                for file in files:
                        pathname = os.path.join(root, file)
                        src_file = os.path.join(image_path, file)

                        src_md5 = self.data.read_md5_file(src_file)
                        dest_md5 = self.data.read_md5_file(pathname)
                        print "Src md5", src_md5, ", dest md5", dest_md5
                        self.assertEquals(src_md5, dest_md5)
        self.start_default_system()

        self.prepare_part()

        number_of_images = self.configuration.get("acronis image count", 12)
        test_data_dir = self.configuration.get("test data dir", None)
        image_path = os.path.join(test_data_dir, "acronis-images", "pc1")

        os.mkdir(os.path.join(self.mnt_point, "images"))

        copy_images()

        self.sys.umount()
        sleep(10)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.replay()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

        # Recheck
        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(idle_time)

        self.assertNoLoggedErrors()

        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        self.dedupv1.start()
        self.assertExitcode(0)

        self.sys.mount()
        self.assertExitcode(0)
        self.assertTrue(os.path.ismount(self.mnt_point))

        # second copy
        copy_images()

        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(idle_time)

        validate_images()

        self.sys.umount()
        sleep(10)

        self.sys.fsck_ext3()
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_copy_images_small_aux_block_index(self, idle_time = 300):
        """ test_copy_images_small_aux_block_index

            Copy images test but with a pretty early import of the block index.
        """
        self.adapt_config_file("block-index.auxiliary-size-hard-limit=.*",
                             "block-index.auxiliary-size-hard-limit=32K")
        self.adapt_config_file("block-index.max-auxiliary-size=.*",
                             "block-index.max-auxiliary-size=24K")
        self.test_copy_images(idle_time)

    def test_copy_images(self, idle_time=300):
        """ test_copy_images

            Copies a series of backup images, crashes the system, restarts it and verifies the images.
        """
        self.start_default_system()

        self.prepare_part()

        start_time = time()
        total_data_size = 0

        copied_image_count = 0
        number_of_images = self.configuration.get("acronis image count", 12)
        test_data_dir = self.configuration.get("test data dir", None)
        image_path = os.path.join(test_data_dir, "acronis-images", "pc1")

        os.mkdir(os.path.join(self.mnt_point, "images"))
        for root, dirs, files in os.walk(image_path):
            for file in files:
                pathname = os.path.join(root, file)

                dest_file = os.path.join(self.mnt_point, "images", file)
                print "Copy", file

                shutil.copyfile(pathname, dest_file)
                total_data_size += os.path.getsize(pathname)
                copied_image_count += 1

                if number_of_images and copied_image_count >= number_of_images:
                    break

        end_time = time()
        print "Copy time %s" % (end_time - start_time)
        print "Total data size %s MB" % (total_data_size / (1024 * 1024.0))

        self.sys.umount()
        sleep(10)

        self.sys.fsck_ext3()
        self.assertExitcode(0)

        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(idle_time)

        self.assertNoLoggedErrors()

        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        self.dedupv1.start()
        self.assertExitcode(0)

        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(idle_time)
        self.dedupv1.monitor("idle", "force-idle=false")

        self.sys.mount()
        self.assertExitcode(0)
        self.assertTrue(os.path.ismount(self.mnt_point))

        for root, dirs, files in os.walk(os.path.join(self.mnt_point, "images")):
            for file in files:
                    pathname = os.path.join(root, file)
                    src_file = os.path.join(image_path, file)

                    src_md5 = self.data.read_md5_file(src_file)
                    dest_md5 = self.data.read_md5_file(pathname)
                    print "Src md5", src_md5, ", dest md5", dest_md5
                    self.assertEquals(src_md5, dest_md5)

        self.assertNoLoggedErrors()

    def test_copy_images_log_overflow(self):
        """ test_copy_images_log_overflow
        """
        self.adapt_config_file("log.max-log-size=.*",
                             "log.max-log-size=128M")
        self.start_default_system()
        self.prepare_part()

        copied_image_count = 0
        number_of_images = self.configuration.get("acronis image count", 5)
        test_data_dir = self.configuration.get("test data dir", None)
        image_path = os.path.join(test_data_dir, "acronis-images", "pc1")

        os.mkdir(os.path.join(self.mnt_point, "images"))
        for root, dirs, files in os.walk(image_path):
            for file in files:
                pathname = os.path.join(root, file)

                dest_file = os.path.join(self.mnt_point, "images", file)
                print "Copy", file

                shutil.copyfile(pathname, dest_file)
                copied_image_count += 1

                if number_of_images and copied_image_count >= number_of_images:
                    break

        for root, dirs, files in os.walk(os.path.join(self.mnt_point, "images")):
            for file in files:
                    pathname = os.path.join(root, file)
                    src_file = os.path.join(image_path, file)

                    src_md5 = self.data.read_md5_file(src_file)
                    dest_md5 = self.data.read_md5_file(pathname)
                    print "Src md5", src_md5, ", dest md5", dest_md5
                    self.assertEquals(src_md5, dest_md5)

        self.sys.umount()
        sleep(10)

        self.sys.fsck_ext3()
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_copy_images_double_replay(self, idle_time=300):
        """ test_copy_images_double_replay

            Copies a series of backup images and kill dedupv1d after the processing.
            During the crash log replay of the starting daemon, the daemon is killed
            a second time. At the end, we check if every data that has been written is
            recovered.
        """
        self.start_default_system()
        self.prepare_part()

        copied_image_count = 0
        number_of_images = self.configuration.get("acronis image count", 12)
        test_data_dir = self.configuration.get("test data dir", None)
        image_path = os.path.join(test_data_dir, "acronis-images", "pc1")

        os.mkdir(os.path.join(self.mnt_point, "images"))
        for root, dirs, files in os.walk(image_path):
            for file in files:
                pathname = os.path.join(root, file)

                dest_file = os.path.join(self.mnt_point, "images", file)
                print "Copy", file

                shutil.copyfile(pathname, dest_file)
                copied_image_count += 1

                if number_of_images and copied_image_count >= number_of_images:
                    break
        self.sys.umount()
        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(idle_time)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        # Kill the starting daemon in 60 seconds
        kill_proc = self.timed_kill_daemon(60, 9)
        self.dedupv1.start()
        kill_proc.join()

        self.dedupv1.start()
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        self.sys.mount()

        for root, dirs, files in os.walk(os.path.join(self.mnt_point, "images")):
            for file in files:
                    pathname = os.path.join(root, file)
                    src_file = os.path.join(image_path, file)

                    src_md5 = self.data.read_md5_file(src_file)
                    dest_md5 = self.data.read_md5_file(pathname)
                    print "Src md5", src_md5, ", dest md5", dest_md5
                    self.assertEquals(src_md5, dest_md5)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_copy_urandom(self):
        """ test_copy_urandom:

            Tests if a 8 GB (by default) simple sequential direct block write is valid.The system is cleanly shutdown.
        """
        self.start_default_system()

        size = int(self.configuration.get("max urandom size", 8 * 1024))

        filename = self.get_urandom_file(size)

        md5_1 = self.data.read_md5(size, filename)
        print "Data md5", md5_1

        self.data.copy_raw(filename, size)
        md5_2 = self.data.read_md5(size)
        print "After write md5", md5_2

        self.assertEqual(md5_1, md5_2)

        sleep(15)
        self.sys.rm_scst_local()
        sleep(15)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

        sleep(15)
        self.sys.add_scst_local()
        sleep(15)

        md5_3 = self.data.read_md5(size)
        self.assertEqual(md5_2, md5_3)
        print "After restart md5", md5_3

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()


    def test_copy_urandom_bytecompare(self):
        """ test_copy_urandom_bytecompare

            Copy urandom date with activates bytecompare filter
        """

        # Enable bytecompare filter
        self.adapt_config_file("filter=bytecompare-filter\nfilter.enabled=false",
            "filter=bytecompare-filter")
        self.start_default_system()

        size = 1 * 1024

        filename = self.get_urandom_file(size)

        md5_1 = self.data.read_md5(size, filename)
        print "Data md5", md5_1

        self.data.copy_raw(filename, size)
        md5_2 = self.data.read_md5(size)
        print "After write md5", md5_2

        self.assertEqual(md5_1, md5_2)

        sleep(15)
        self.sys.rm_scst_local()
        sleep(15)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        self.dedupv1.monitor("stats")
        self.dedupv1.monitor("trace")
        self.dedupv1.monitor("profile")
        self.dedupv1.monitor("lock")
        self.dedupv1.monitor("error")

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

        sleep(15)
        self.sys.add_scst_local()
        sleep(15)

        md5_3 = self.data.read_md5(size)
        self.assertEqual(md5_2, md5_3)
        print "After restart md5", md5_3

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_summary_statistics(self):
        """ test_summary_statistics

            Tests the stats output
        """
        self.adapt_config_file("volume.threads=24", "volume.threads=24\nvolume.id=1\nvolume.device-name=dedupv1_2\nvolume.group=Default:1\nvolume.logical-size=120G\nvolume.threads=24")

        device_name2 = "/dev/disk/by-path/scsi-0:0:0:1"
        mnt_point2 = "/mnt/dedup2"

        sys2 = SysTest(self.run, device_name=device_name2, mount_point=mnt_point2)
        data2 = DataTest(self.run, device_name=device_name2, mnt_point=mnt_point2)

        self.start_default_system()

        try:
            self.prepare_part()
            self.prepare_part(sys=sys2, mnt_point=mnt_point2)

            self.data.fspopulate(timeout=20)
            data2.fspopulate(timeout=20)

            stats = self.dedupv1.monitor("stats")

            scc = 0
            src = 0
            swc = 0
            rc = 0
            wt = 0.0
            rt = 0.0
            volumes = stats["volumes"]

            for volume_id in volumes:
                print volumes[volume_id]
                scc += volumes[volume_id]["commands"]["scsi command count"]
                src += volumes[volume_id]["commands"]["sector read count"]
                swc += volumes[volume_id]["commands"]["sector write count"]
                rc += volumes[volume_id]["commands"]["retry count"]
                wt += volumes[volume_id]["commands"]["average write throughput"]
                rt += volumes[volume_id]["commands"]["average read throughput"]

            volumesummary = stats["volumesummary"]
            print volumesummary
            scc_sum = volumesummary["cumulative scsi command count"]
            src_sum = volumesummary["cumulative sector read count"]
            swc_sum = volumesummary["cumulative sector write count"]
            rc_sum = volumesummary["cumulative retry count"]
            wt_sum = volumesummary["cumulative write throughput"]
            rt_sum = volumesummary["cumulative read throughput"]

            self.assertTrue(scc_sum <= scc * 1.1 and scc_sum >= scc * 0.9)
            self.assertTrue(src_sum <= src * 1.1 and src_sum >= src * 0.9)
            self.assertTrue(swc_sum <= swc * 1.1 and swc_sum >= swc * 0.9)
            self.assertTrue(rc_sum <= rc * 1.1 and rc_sum >= rc * 0.9)
            self.assertTrue(wt_sum <= wt * 1.1 and wt_sum >= wt * 0.9)
            self.assertTrue(rt_sum <= rt * 1.1 and rt_sum >= rt * 0.9)
        finally:
            self.sys.umount()
            self.assertExitcode(0)
            sys2.umount()
            self.assertExitcode(0)

        self.sys.fsck_ext3()
        self.assertExitcode(0)
        sys2.fsck_ext3()
        self.assertExitcode(0)

        self.sys.rm_scst_local()

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_copy_extra_images(self):
        """ test_copy_extra_images

            Copies a extra series of backup images.
        """
        def copy_images(image_path):
            for root, dirs, files in os.walk(image_path):
                for file in files:
                    pathname = os.path.join(root, file)

                    out_filepath = pathname[len(image_path) + 1:]

                    dest_file = os.path.join(self.mnt_point, "images", out_filepath)
                    print "Prepare %s (size %s MB)" % (out_filepath, os.path.getsize(pathname) / (1024 * 1024))

                    if not os.path.exists(os.path.dirname(dest_file)):
                        os.makedirs(os.path.dirname(dest_file))

                    # For performance reasons on dedupv1-build (defect SAN) we copy the data to a faster storage before copying the image to dedupv1
                    temp_filename = os.path.join(test_data_dir, "tmp.img")
                    shutil.copyfile(pathname, temp_filename)
                    sleep(10)

                    print "Copy %s" % (out_filepath)
                    shutil.copyfile(temp_filename, dest_file)

                    self.sys.sync()
                    os.unlink(temp_filename)

        extra_test_data_dir = self.configuration.get("extra test data dir", None)
        test_data_dir = self.configuration.get("test data dir", None)
        if extra_test_data_dir is None:
            print "Skip this test"
            return
        altiris_image_path = os.path.join(extra_test_data_dir, "AltirisImages")
        norton_image_path = os.path.join(extra_test_data_dir, "NortonGhostBackups")

        self.start_default_system()
        try:
            self.prepare_part(size=500) # 500 GB volumes
            os.mkdir(os.path.join(self.mnt_point, "images"))

            copy_images(altiris_image_path)
            copy_images(norton_image_path)
        finally:
            self.sys.umount()
            sleep(10)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        self.dedupv1.stop()
        self.assertExitcode(0)

class Dedupv1ContribSystemTest(Dedupv1BaseSystemTest):

    def test_check_while_running(self):
       """ test_check_while_running

        Tests that it is not possible to start dedupv1_check while a daemon is running.
       """
       self.start_default_system()

       sleep(5)

       self.dedupv1.monitor("status")

       self.dedupv1.check()
       self.assertExitcode(8)

       self.dedupv1.monitor("status")

    def test_replay_while_running(self):
       """ test_replay_while_running

            Tests that it is not possible to start dedupv1_replay while a daemon is running.
       """
       self.start_default_system()

       sleep(5)

       self.dedupv1.replay()
       self.assertExitcode(1)

    def test_restore_while_running(self):
       """ test_restore_while_running

        Tests that it is not possible to start dedupv1_restore while a daemon is running.
       """
       self.start_default_system()

       sleep(5)

       self.dedupv1.restore()
       self.assertExitcode(1)

    def test_dedupv1_debug(self):
        """ test_dedupv1_debug
        """
        self.start_default_system()

        size = 512
        filename = self.get_urandom_file(size)
        self.data.copy_raw(filename, size)
        sleep(5)

        self.sys.rm_scst_local()

        print "Crash the system"
        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        self.dedupv1.debug(stdin="quit\n")

        # restart afterwards
        self.dedupv1.start()

    def test_replayer(self):
        """ test_replayer
            Tests the dedupv1_replay contrib tool that
            allows the replay the complete log without starting
            the daemon
        """
        self.start_default_system()

        size = 2048
        filename = self.get_urandom_file(size)
        md5_1 = self.data.read_md5(size, filename)
        print "Data md5", md5_1
        self.data.copy_raw(filename, size)

        sleep(5)
        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(30)

        self.sys.rm_scst_local()

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

        print "Crash the system"
        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        print "Replay the log"
        t1 = time()
        self.dedupv1.replay()
        self.assertExitcode(0)
        t2 = time()
        print "Replayed log in %s seconds" % (t2 - t1)

        self.dedupv1.start()
        self.assertExitcode(0)
        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(30)

        self.sys.add_scst_local()
        md5_2 = self.data.read_md5(size)
        print "After replay md5", md5_2
        self.assertEqual(md5_1, md5_2)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_chunk_restorer(self):
        """ test_chunk_restorer:
            tests if a simple sequential direct block write is valid.
            The system is killed and the chunk index is deleted, the chunk
            restorer tool is used to restore the chunk index. At the end
            the test verifies that the chunk index is valid.
        """
        self.start_default_system()

        size = 512

        filename = self.get_urandom_file(size)

        md5_1 = self.data.read_md5(size, filename)
        print "Data md5", md5_1
        self.data.copy_raw(filename, size)

        self.sys.clear_cache()
        md5_2 = self.data.read_md5(size)
        print "After write md5", md5_2
        self.assertEqual(md5_1, md5_2)

        sleep(5)
        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(30)

        self.sys.clear_cache()
        md5_3 = self.data.read_md5(size)
        print "After idle md5", md5_3
        self.assertEqual(md5_1, md5_3)

        self.sys.rm_scst_local()

        print "Crash the system"
        os.kill(self.scst.get_pid(), 9)
        sleep(5)

        print "Delete chunk index"
        for line in open("/opt/dedupv1/etc/dedupv1/dedupv1.conf", "r"):
            m = re.match("chunk-index.persistent.filename=(.*)", line)
            if m:
                filename = m.groups()[0]
                os.remove(filename)

        print "Restore chunk index"
        self.dedupv1.restore()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)
        self.dedupv1.monitor("idle", "force-idle=true")
        sleep(30)

        self.sys.add_scst_local()

        self.sys.clear_cache()
        md5_4 = self.data.read_md5(size)
        print "After restore md5", md5_4
        self.assertEqual(md5_1, md5_4)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

    def test_dedupv1_read(self):
        """ test_dedupv1_read

            Test the dedupv1_read utility.
        """
        self.start_default_system()

        size = 1024

        filename = self.get_urandom_file(size)

        md5_1 = self.data.read_md5(size, filename)
        print "Data md5", md5_1

        self.data.copy_raw(filename, size)
        self.sys.rm_scst_local()
        self.assertNoLoggedErrors()

        self.dedupv1.stop()
        self.assertExitcode(0)

        md5_2 = self.dedupv1.read_md5(volume_id=0, size=size)
        print "Read md5", md5_2
        self.assertEqual(md5_1, md5_2)

    def test_self_test(self):
        """ test_self_test

            Tests the dedupv1_support self-test call.
        """
        self.start_default_system()

        self.sys.rm_scst_local()

        self.dedupv1.volumes("attach id=2 device-name=Backup2 logical-size=500GB group=Default:1")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=3 device-name=Backup3 logical-size=1tb")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=4 device-name=Backup4 logical-size=500GB")
        self.assertExitcode(0)

        self.dedupv1.volumes("attach id=5 device-name=Backup5 logical-size=1tb group=Default:2")
        self.assertExitcode(0)

        self.dedupv1.volumes("group remove id=2 group=Default")
        self.assertExitcode(0)

        self.dedupv1.self_test("--verbose")
        self.assertExitcode(0)

        self.dedupv1.volumes("group remove id=5 group=Default")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=2")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=3")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=4")
        self.assertExitcode(0)

        self.dedupv1.volumes("detach id=5")
        self.assertExitcode(0)

    def test_chunk_restorer_empty(self):
        """ test_chunk_restorer_empty

            Tests the chunk index restorer when there is no data in the system.
        """
        self.start_default_system()

        self.dedupv1.stop()

        print "Delete chunk index"
        for line in open("/opt/dedupv1/etc/dedupv1/dedupv1.conf", "r"):
            m = re.match("chunk-index.persistent.filename=(.*)", line)
            if m:
                filename = m.groups()[0]
                os.remove(filename)

        print "Restore chunk index"
        self.dedupv1.restore()
        self.assertExitcode(0)

        self.dedupv1.start()
        self.assertExitcode(0)

        self.assertNoLoggedWarning()
        self.assertNoLoggedErrors()

def get_dedupv1_src():
    mod = sys.modules[__name__]
    return os.path.normpath(os.path.join(os.path.dirname(mod.__file__), ".."))

def perform_systemtest(system_test_classes, option_args=None):
    def get_dedupv1_systemtest_root(path):
        mod = sys.modules[__name__]
        return os.path.normpath(os.path.join(os.path.dirname(mod.__file__), path))
    def decorate_parser(parser=None):
        if not parser:
            parser = optparse.OptionParser()
        parser.add_option("--xml",
            action="store_true",
            dest="xml",
            default=False)
        parser.add_option("--output",
            dest="output",
            default=".")
        parser.add_option("--repeat_until_error",
                          action="store_true",
                          dest="repeat_until_error",
                          default=False)
        parser.add_option("--config", dest="config", default=None)
        return parser

    def get_test_suite(argv, configuration, output_dir):
        suite = unittest.TestSuite()

        regex_match = []
        regex_notmatch = []

        for test_name_pattern in argv:
            match = not test_name_pattern.startswith("!")
            if match:
                regex_match.append(re.compile("^%s$" % test_name_pattern))
            else:
                test_name_pattern = test_name_pattern.lstrip("!")
                regex_notmatch.append(re.compile("^%s$" % test_name_pattern))

        if len(regex_match) == 0:
            regex_match.append(re.compile("^.*$")) # default pattern

        for system_test_class in system_test_classes:
            for test_case_name in unittest.defaultTestLoader.getTestCaseNames(system_test_class):
                append = False
                for regex in regex_match:
                    if regex.match(test_case_name):
                        append = True
                if append:
                    for regex in regex_notmatch:
                        if regex.match(test_case_name):
                            append = False
                            break
                if append:
                    suite.addTest(system_test_class(configuration,
                      output_dir,
                      test_case_name))

        return suite

    def get_test_runner():
        if options.xml:
            import xmlrunner
            return xmlrunner.XMLTestRunner(output=options.output, verbose=True)
        else:
            return unittest.TextTestRunner(verbosity=2)

    def get_configuration():
        if not options.config:
            config_dir = get_dedupv1_systemtest_root("conf")

            hostname = socket.gethostname()
            hostname_config_filename = os.path.join(config_dir, hostname + ".conf")
            if os.path.exists(hostname_config_filename):
                options.config = hostname_config_filename
            else:
                options.config = os.path.join(config_dir, "default.conf")

        print "Using configuration file", options.config
        return simplejson.loads(open(options.config, "r").read())

    global output_dir, configuration
    try:
        if not option_args:
            option_args = decorate_parser().parse_args()
        (options, argv) = option_args

        output_dir = options.output
        configuration = get_configuration()

        repeat_count = 1
        while True:
            if options.repeat_until_error:
                print "Iteration", repeat_count
            suite = get_test_suite(argv, configuration, output_dir)
            runner = get_test_runner()
            result = runner.run(suite)
            if not options.repeat_until_error or not result.wasSuccessful():
                break
            repeat_count += 1
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    # If possible, the order should be from the most severe to the lower priority/critical tests
    # This allows us, e.g. to kill a system test early
    perform_systemtest(
            [
                Dedupv1DataSystemTest,
                Dedupv1OverflowSystemTest,
                Dedupv1DynamicConfigSystemTest,
                Dedupv1ContribSystemTest,
                Dedupv1ISCSISystemTest,
                Dedupv1SystemTest
            ])
