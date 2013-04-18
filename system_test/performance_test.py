#!/usr/bin/python
#
# dedupv1 - iSCSI based Deduplication System for Linux
#
# (C) 2008 Dirk Meister
# (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
#

import unittest
import glob

from time import sleep, time
import optparse
import tempfile
from run import Run
from remote_run import RemoteRun
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
from RemoteSysTest import RemoteSysTest
import random
import socket
from pprint import pprint
from systemtest import Dedupv1BaseSystemTest
from systemtest import perform_systemtest

class Dedupv1BaseRemoteSystemTest(Dedupv1BaseSystemTest):

    def setUp(self):
        print
        self.device_name = self.configuration.get("device-name", "/dev/disk/by-path/scsi-0:0:0:0")
        self.mnt_point = self.configuration.get("moint-point", "/mnt/dedup")
        self.user = self.configuration.get("user", None)
        self.dedupv1_config_file = self.configuration.get("config", "/opt/dedupv1/etc/dedupv1/dedupv1.conf")
        if not self.dedupv1_config_file.startswith("/"):
            self.dedupv1_config_file = os.path.join(os.getcwd(), self.dedupv1_config_file)

        self.run = Run()
        self.client_run = RemoteRun("neon")
        self.sys = SysTest(self.run, device_name=self.device_name)
        self.dedupv1 = Dedupv1Test(self.run, sys=self.sys, user=self.user, config=self.dedupv1_config_file)
        self.scst = ScstTest()
        self.open_iscsi = OpeniSCSITest(self.client_run)
        self.client_sys = RemoteSysTest(self.client_run, device_name=self.device_name)
        self.data = DataTest(self.client_run, mnt_point=self.mnt_point, device_name=self.device_name)

        (process_name, pid) = self.get_listening_process(self.run, 9001)
        if (process_name):
            print "Found process %s (pid %s) listening on port 9001" % (process_name, pid)
            os.kill(int(pid), 9)

        self.dedupv1.stop("--force")
        if os.path.exists("/opt/dedupv1/var/log/dedupv1"):
            for file in glob.glob("/opt/dedupv1/var/log/dedupv1/*"):
                os.remove(file)

        self.sys.rm_scst_local()

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
volume.threads=16
        """)

    def tearDown(self):
        sleep(15)
        self.sys.umount()
        sleep(15)
        self.sys.rm_scst_local()

        if self.scst.get_pid():
            self.dedupv1.stop("-f")
            self.assertExitcode(0)

class Dedupv1PerformanceSystemTest(Dedupv1BaseRemoteSystemTest):
    """
    The performance test try to establish fixed and good test cases
    to evaluate the performance of dedupv1 and to observe
    performance regressions.
    """
    def test_basic_performance(self):
        """
        test_basic_performance

        The basic performance tests copies 128 GB backup files from a different
        computer to dedupv1 using iSCSI.
        It replays the log and restarts the system after each backup generation.
        """
        test_data_dir = self.configuration["test data dir"]
        iscsi_target_name = self.configuration["test iscsi target name"]
        iscsi_target_ip = self.configuration["test iscsi target ip"]
        performance_test_files = self.configuration.get("performance test files", [])
        self.start_default_system()
        self.sys.rm_scst_local()
        sleep(5)

        self.dedupv1.stop()

        self.sys.clear_cache()
        self.dedupv1.start()
        sleep(5)

        self.dedupv1.targets("add tid=3 name=%s" % (iscsi_target_name))
        self.assertExitcode(0)

        self.open_iscsi.discover(server=iscsi_target_ip)
        self.assertExitcode(0)

        generation_count = 1
        for performance_test_file in performance_test_files:
            self.open_iscsi.connect(server=iscsi_target_ip,
                name=iscsi_target_name)
            self.assertExitcode(0)

            try:
                self.data.device_name = "/dev/disk/by-path/ip-%s:3260-iscsi-%s-lun-0" % (
                    iscsi_target_ip, iscsi_target_name)
                self.client_sys.device_name = "/dev/disk/by-path/ip-%s:3260-iscsi-%s:special-lun-0" % (
                    iscsi_target_ip, iscsi_target_name)

                file = os.path.join(test_data_dir, performance_test_file)
                start_time = time()
                self.data.copy_raw(file,
                    count=128 * 1024,
                    seek=2 * 128 * 1024 * (generation_count - 1))
                generation_count += 1

                self.client_sys.clear_cache()

                end_time = time()
                print "Copy time (%s) %s" % (generation_count,
                    end_time - start_time)

                self.dedupv1.monitor("profile")
                self.dedupv1.monitor("trace")
                self.dedupv1.monitor("stats")
            finally:
                self.open_iscsi.disconnect(server=iscsi_target_ip,
                    name=iscsi_target_name)

            self.dedupv1.monitor("idle", "force-idle=true")

            # Wait until all replayed
            open_event_count = self.dedupv1.monitor("log")["open events"]
            while open_event_count > 64:
                sleep(30)
                open_event_count = self.dedupv1.monitor("log")["open events"]

            self.dedupv1.stop()
            self.assertExitcode(0)

            self.sys.clear_cache()

            self.dedupv1.start("--force")
            self.assertExitcode(0)

        self.dedupv1.stop()

    def test_basic_read_performance(self):
        """
        test_basic_read_performance
        """
        test_data_dir = self.configuration["test data dir"]
        iscsi_target_name = self.configuration["test iscsi target name"]
        iscsi_target_ip = self.configuration["test iscsi target ip"]
        performance_test_files = self.configuration.get("performance test files", [])

        self.data.device_name = "/dev/disk/by-path/ip-%s:3260-iscsi-%s-lun-0" % (
              iscsi_target_ip, iscsi_target_name)
        self.client_sys.device_name = "/dev/disk/by-path/ip-%s:3260-iscsi-%s:special-lun-0" % (
              iscsi_target_ip, iscsi_target_name)

        self.start_default_system()
        self.sys.rm_scst_local()
        sleep(5)

        self.dedupv1.stop()

        self.sys.clear_cache()
        self.dedupv1.start()
        sleep(5)

        self.dedupv1.targets("add tid=3 name=%s" % (iscsi_target_name))
        self.assertExitcode(0)

        self.open_iscsi.discover(server=iscsi_target_ip)
        self.assertExitcode(0)
        self.open_iscsi.connect(server=iscsi_target_ip,
                name=iscsi_target_name)
        self.assertExitcode(0)

        try:
            generation_count = 1
            for performance_test_file in performance_test_files:
                file = os.path.join(test_data_dir, performance_test_file)
                self.data.copy_raw(file,
                    count=128 * 1024,
                    seek=2 * 128 * 1024 * (generation_count - 1))
                generation_count += 1

                self.dedupv1.monitor("profile")
                self.dedupv1.monitor("trace")
                self.dedupv1.monitor("stats")
        finally:
            self.open_iscsi.disconnect(server=iscsi_target_ip,
                name=iscsi_target_name)

        self.dedupv1.monitor("idle", "force-idle=true")

        # Wait until all replayed
        open_event_count = self.dedupv1.monitor("log")["open events"]
        while open_event_count > 64:
            sleep(30)
            open_event_count = self.dedupv1.monitor("log")["open events"]

        self.dedupv1.stop()
        self.assertExitcode(0)

        self.sys.clear_cache()

        self.dedupv1.start("--force")
        self.assertExitcode(0)

        self.open_iscsi.connect(server=iscsi_target_ip,
                name=iscsi_target_name)
        self.assertExitcode(0)

        try:
            generation_count = 1
            for performance_test_file in performance_test_files:
                start_time = time()
                self.data.read_raw(
                    count=128 * 1024,
                    seek=2 * 128 * 1024 * (generation_count - 1))

                end_time = time()
                print "Read time (%s) %s" % (generation_count,
                    end_time - start_time)

                generation_count += 1

                self.dedupv1.monitor("profile")
                self.dedupv1.monitor("trace")
                self.dedupv1.monitor("stats")
        finally:
            self.open_iscsi.disconnect(server=iscsi_target_ip,
                name=iscsi_target_name)

        self.dedupv1.stop()
if __name__ == "__main__":
    perform_systemtest(
            [
                Dedupv1PerformanceSystemTest
            ])
