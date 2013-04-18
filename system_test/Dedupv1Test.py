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
from run import sh_escape

sys.path.append("/opt/dedupv1/lib/python")
import config_loader

class Dedupv1Test:
    def __init__(self,
        run,
        sys,
        config = "/opt/dedupv1/etc/dedupv1/dedupv1.conf",
        user = None):
        self.run = run
        self.config = config
        self.user = user
        self.sys = sys

    def open_config(self):
        return config_loader.Config(self.config)

    def insert_extra_params(self, kwargs):
        if self.user and not "user" in kwargs:
            kwargs["user"] = self.user

    def self_test(self, *args, **kwargs):
        # use root
        return self.run("/opt/dedupv1/bin/dedupv1_support self-test",
            "--force",
            "--config=%s" % sh_escape(self.config), *args, **kwargs)

    def clean(self, *args, **kwargs):
        self.insert_extra_params(kwargs)
        return self.run("/opt/dedupv1/bin/dedupv1_support clean",
            "--force",
            "--config=%s" % sh_escape(self.config), *args, **kwargs)

    def start(self, *args, **kwargs):
        self.insert_extra_params(kwargs)
        r = self.run("/opt/dedupv1/bin/dedupv1_adm start",
            "--config=%s" % sh_escape(self.config), *args, **kwargs)
        self.sys.add_scst_local()
        return r

    def restart(self, *args, **kwargs):
        self.insert_extra_params(kwargs)

        self.sys.rm_scst_local()
        r = self.run("/opt/dedupv1/bin/dedupv1_adm restart",
            "--config=%s" % sh_escape(self.config), *args, **kwargs)
        self.sys.add_scst_local()
        return r

    def start_bypassing(self, *args, **kwargs):
        self.insert_extra_params(kwargs)
        r = self.run("/opt/dedupv1/bin/dedupv1_support start-bypassing",
                        "--config=%s" % sh_escape(self.config), *args, **kwargs)
        self.sys.add_scst_local()
        return r

    def stop(self, *args, **kwargs):
        self.insert_extra_params(kwargs)

        self.sys.rm_scst_local()
        return self.run("/opt/dedupv1/bin/dedupv1_adm stop", "--config=%s" % sh_escape(self.config), *args, **kwargs)

    def admin(self, command, *args, **kwargs):
        self.insert_extra_params(kwargs)
        return self.run("/opt/dedupv1/bin/dedupv1_adm", command, *args, **kwargs)

    def users(self, command, *args, **kwargs):
        self.insert_extra_params(kwargs)
        return self.run("/opt/dedupv1/bin/dedupv1_users", command, *args, **kwargs)

    def targets(self, command, *args, **kwargs):
        self.insert_extra_params(kwargs)
        return self.run("/opt/dedupv1/bin/dedupv1_targets", command, *args, **kwargs)

    def volumes(self, command, *args, **kwargs):
        self.insert_extra_params(kwargs)
        return self.run("/opt/dedupv1/bin/dedupv1_volumes", command, *args, **kwargs)

    def groups(self, command, *args, **kwargs):
        self.insert_extra_params(kwargs)
        return self.run("/opt/dedupv1/bin/dedupv1_groups", command, *args, **kwargs)

    def support(self, command, *args, **kwargs):
        self.insert_extra_params(kwargs)
        return self.run("/opt/dedupv1/bin/dedupv1_support", command, *args, **kwargs)

    def monitor(self, name, *args, **kwargs):
        self.insert_extra_params(kwargs)
        monitor_data = self.run("/opt/dedupv1/bin/dedupv1_mon", name, *args, **kwargs)
        try:
            return simplejson.loads(monitor_data)
        except ValueError:
            return {"ERROR": monitor_data}

    def restore(self, *args, **kwargs):
        self.insert_extra_params(kwargs)
        return self.run("/opt/dedupv1/bin/dedupv1_chunk_restorer", "--logging=/opt/dedupv1/etc/dedupv1/logging.xml",
                "--config=%s" % sh_escape(self.config), *args, **kwargs)

    def report(self, filename, *args, **kwargs):
        # use original user
        return self.run("/opt/dedupv1/bin/dedupv1_report", filename, *args, **kwargs)

    def replay(self, *args, **kwargs):
        self.insert_extra_params(kwargs)
        return self.run("/opt/dedupv1/bin/dedupv1_replay",  "--logging=/opt/dedupv1/etc/dedupv1/logging.xml", "--config=%s" % sh_escape(self.config), *args, **kwargs)

    def debug(self, *args, **kwargs):
        self.insert_extra_params(kwargs)
        return self.run("/opt/dedupv1/bin/dedupv1_debug",  "--logging=/opt/dedupv1/etc/dedupv1/logging.xml", "--config=%s" % sh_escape(self.config), *args, **kwargs)

    def check(self, *args, **kwargs):
        self.insert_extra_params(kwargs)
        return self.run("/opt/dedupv1/bin/dedupv1_check", "--config=%s" % sh_escape(self.config), *args, **kwargs)

    def read_md5(self, volume_id, size):
        d = self.run("/opt/dedupv1/bin/dedupv1_read --config=%s --volume_id=%s --size=%s | md5sum" % (sh_escape(self.config), volume_id, (size * 1024 * 1024)))
        return d.split(" ")[0]

    def passwd(self, password):
        d = self.run("/opt/dedupv1/bin/dedupv1_passwd %s" % (password))
        return d.split(" ")[0]
