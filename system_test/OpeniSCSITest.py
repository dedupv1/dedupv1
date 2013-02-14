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

sys.path.append("/opt/dedupv1/lib/python")
import config

class OpeniSCSITest:
    def __init__(self, run):
        self.run = run

    def client_auth(self, server, name, username, password):
        self.run("iscsiadm   --mode node  --targetname \"%s\"  -p %s --op=update --name node.session.auth.authmethod --value=CHAP" 
                 % (name, server))
        self.run("iscsiadm   --mode node  --targetname \"%s\"  -p %s --op=update --name node.session.auth.username --value=%s" 
                 % (name, server, username))
        self.run("iscsiadm   --mode node  --targetname \"%s\"  -p %s --op=update --name node.session.auth.password --value=%s"
                 % (name, server, password))

    def server_auth(self, server, name, username, password):
        self.run("iscsiadm   --mode node  --targetname \"%s\"  -p %s --op=update --name node.session.auth.authmethod --value=CHAP" 
                 % (name, server))
        self.run("iscsiadm   --mode node  --targetname \"%s\"  -p %s --op=update --name node.session.auth.username_in --value=%s" 
                 % (name, server, username))
        self.run("iscsiadm   --mode node  --targetname \"%s\"  -p %s --op=update --name node.session.auth.password_in --value=%s"
                 % (name, server, password))

    def discover(self, server, *args, **kwargs):
        return self.run("iscsiadm -m discovery -t sendtargets -p %s" % (server), *args, **kwargs)

    def connect(self, server, name, *args, **kwargs):
        r = self.run("iscsiadm -m node -p %s -T %s -l" % (server, name), *args, **kwargs)
        sleep(5)
        return r
    
    def disconnect(self, server, name, *args, **kwargs):
        return self.run("iscsiadm -m node -p %s -T %s -u" % (server, name), *args, **kwargs)
    
    def show(self, server, name, *args, **kwargs):
        return self.run("iscsiadm -m node -p %s -T %s" % (server, name), *args, **kwargs)