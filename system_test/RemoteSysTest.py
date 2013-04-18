#
# dedupv1 - iSCSI based Deduplication System for Linux
#
# (C) 2008 Dirk Meister
# (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
#

import os
import sys
import subprocess
import simplejson
from time import sleep

class RemoteSysTest:
    def __init__(self, run,
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

        self.run.py.time.sleep(5)
        if not self.run.py.os.path.ismount(self.mount_point):
            raise Exception("Mountpoint %s not mounted" % self.mount_mount)

    def kernel_module_loaded(self, module_name):
        """ checks if a given module is loaded into the kernel """
        return len(filter(lambda line: line.find(module_name) >= 0,
            file("/proc/modules").readlines())) > 0

    def mk_part(self, size):
        if not self.run.py.os.path.exists(self.device_name):
            raise Exception("Illegal device: %s" % self.device_name)
        r = self.run('fdisk %s' % self.device_name, stdin="n\np\n1\n\n+%s\nw" % size)
        self.run.py.time.sleep(5)
        return r

    def parted(self, cmd):
        return self.run('parted -s %s %s' % (self.device_name, cmd))

    def statfs(self):
        return self.run.py.os.statvfs(self.mount_point)

    def mkfs_ext3(self):
        full_device_name = "%s-part1" % self.device_name
        if not self.run.py.os.path.exists(full_device_name):
            raise Exception("Illegal device: %s" % full_device_name)
        r = self.run("mkfs.ext3 -q %s" % full_device_name)
        sleep(5)
        return r

    def fsck_ext3(self):
        return self.run("fsck.ext3 %s-part1 -n -f" % self.device_name)

    def mount(self):
        if not self.run.py.os.path.isdir(self.mount_point):
            raise Exception("Illegal mount point: %s" % self.mount_point)
        r = self.run("mount %s-part1 %s" % (self.device_name, self.mount_point))
        sleep(5)
        return r

    def get_devices(self):
        return self.run.py.os.listdir("/dev/disk/by-path")

    def sync(self):
        if self.run.py.os.path.exists("/bin/busybox"):
            return self.run("busybox sync")
        else:
            return self.run("sync")

    def clear_cache(self):
        if self.run.py.os.path.exists("/bin/busybox"):
            self.run("busybox sync")
        else:
            self.run("sync")

        # drop caches
        self.run("echo \"3\" > /proc/sys/vm/drop_caches")

    def umount(self):
        if self.run.py.os.path.ismount(self.mount_point):
            r = self.run("umount %s" % self.mount_point)
            self.run.py.time.sleep(5)
            return r

    def rm_scst_local(self):
        if self.kernel_module_loaded("scst_local"):
            r = self.run("rmmod scst_local")
            self.run.py.time.sleep(5)
            return r

    def add_scst_local(self):
        if not self.kernel_module_loaded("scst_local"):
            r = self.run("modprobe scst_local")
            sleep(5)
            return r
