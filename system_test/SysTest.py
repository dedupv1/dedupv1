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
import sys
import subprocess
import simplejson
from time import sleep

class SysTest:
    def __init__(self,
        run,
        device_name = "/dev/disk/by-path/scsi-0:0:0:0",
        mount_point = "/mnt/dedup"):
        self.device_name = device_name
        self.mount_point = mount_point
        self.run = run

    def prepare_part(self):
        self.mk_part("40G")
        if self.run.code != 0:
            return

        self.mkfs_ext3()
        if self.run.code != 0:
            return

        self.mount()
        if self.run.code != 0:
            return

        sleep(5)
        if not os.path.ismount(self.mount_point):
            raise Exception("Mountpoint %s not mounted" % self.mount_mount)

    def kernel_module_loaded(self, module_name):
        """ checks if a given module is loaded into the kernel """
        return len(filter(lambda line: line.find(module_name) >= 0,
          file("/proc/modules").readlines())) > 0

    def mk_part(self, size):
        if not os.path.exists(self.device_name):
            raise Exception("Illegal device: %s" % self.device_name)
        r = self.run('fdisk %s' % self.device_name,
            stdin="n\np\n1\n\n+%s\nw" % size)
        sleep(5)
        return r

    def parted(self, cmd):
        return self.run('parted -s %s %s' % (self.device_name, cmd))

    def statfs(self):
        return os.statvfs(self.mount_point)

    def mkfs_ext3(self):
        full_device_name = "%s-part1" % self.device_name
        if not os.path.exists(full_device_name):
            raise Exception("Illegal device: %s" % full_device_name)
        r = self.run("mkfs.ext3 -q %s" % full_device_name)
        sleep(5)
        return r

    def fsck_ext3(self):
        return self.run("fsck.ext3 %s-part1 -n -f" % self.device_name)

    def mount(self):
        if not os.path.isdir(self.mount_point):
            raise Exception("Illegal mount point: %s" % self.mount_point)
        r = self.run("mount %s-part1 %s" % (self.device_name, self.mount_point))
        sleep(5)
        return r

    def get_devices(self):
        return os.listdir("/dev/disk/by-path")

    def sync(self):
        if os.path.exists("/bin/busybox"):
            return self.run("busybox sync")
        else:
            return self.run("sync")

    def clear_cache(self):
        if os.path.exists("/bin/busybox"):
            self.run("busybox sync")
        else:
            self.run("sync")

        # drop caches
        f = open("/proc/sys/vm/drop_caches", "w")
        f.write("3\n")

    def umount(self):
        if os.path.ismount(self.mount_point):
            r = self.run("umount %s" % self.mount_point)
            sleep(5)
            return r

    def rm_scst_local(self):
        if self.kernel_module_loaded("scst_local"):
            r = self.run("rmmod scst_local")
            sleep(5)
            return r

    def add_scst_local(self):
        if not self.kernel_module_loaded("scst_local"):
            r = self.run("modprobe scst_local")
            sleep(5)
            return r
