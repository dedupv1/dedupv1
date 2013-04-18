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

def get_dedupv1_src():
    mod = sys.modules[__name__]
    return os.path.normpath(os.path.join(os.path.dirname(mod.__file__), ".."))

class DataTest:
    def __init__(self, run, mnt_point, device_name):
        self.run = run
        self.mnt_point = mnt_point
        self.device_name = device_name

    def copy_raw(self, filename, count, seek = None, skip = None):
        command = "dd if=%s of=%s bs=1M count=%s" % (filename, self.device_name, count)
        if seek:
            command += " seek=%s" % (seek)
        if skip:
            command += " skip=%s" % (skip)
        return self.run(command)

    def read_raw(self, count, seek = None, skip = None):
        command = "dd if=%s of=/dev/null bs=1M count=%s" % (
            self.device_name, count)
        if seek:
            command += " seek=%s" % (seek)
        if skip:
            command += " skip=%s" % (skip)
        command += " 2>/dev/null"
        return self.run(command)

    def read_md5(self, count, filename = None):
        cache_filename = None
        if filename == None:
            if not os.path.exists(self.device_name):
                raise Exception("Device %s doesn't exists" % self.device_name)
            filename = self.device_name
        else:
            # Read cached md5 hash
            cache_filename = "%s-%s.md5" % (filename, count)
            if os.path.exists(cache_filename):
                return open(cache_filename).read()
        d = None
        d = self.run("dd if=%s bs=1M count=%s 2>/dev/null | md5sum" % (filename, count))
        md5 = d.split(" ")[0]

        if os.path.isfile(filename) and filename.find("/dev") < 0 and cache_filename:
            try:
                # write md5 hash if possible
                open(cache_filename, "w").write(md5)
            except:
                # we do not are.
                pass

        return md5

    def read_md5_file(self, filename = None):
        if filename == None:
            filename = self.device_name
        d = None
        d = self.run("dd if=%s bs=1M 2>/dev/null | md5sum" % (filename))
        return d.split(" ")[0]

    def copy(self, dir, to = None, timeout = 300):
        if to == None:
            to = self.mnt_point
        if timeout:
            return self.run("timeout %s cp -r %s %s" % (timeout,  dir, self.mnt_point))
        else:
            return self.run("cp -r %s %s" % (dir, self.mnt_point))
