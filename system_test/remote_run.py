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
import uuid
import rpyc
import re

def sh_escape(s):
    return s.replace("(","\\(").replace(")","\\)").replace(" ","\\ ")

class WindowsRemoteRun:
    """ remote execution of shell commands and python commands
        on a Windows machine using rpyc.
    """
    def __init__(self, hostname, verbose = True):
        self.status = ""
        self.error = ""
        self.verbose = verbose
        self.hostname = hostname
        self.last_command = ""
        
        self.c = rpyc.classic.connect(self.hostname)
        self.c.execute("import os")
        
        self.py = self.c.modules
        self.remote_username = self.py.getpass.getuser()
        
    def upload(self, local_file, remote_file):
        """ uploads a local file to the remote Windows
        """
        rpyc.classic.upload(self.c, local_file, remote_file)
        
    def new_temp_filename(self):
        temp_dir = "C:\\Users\\%s\\Lokale Einstellungen\\Temp\\" % self.remote_username; 
        temp_file = temp_dir + uuid.uuid4().hex
        return temp_file
        
    def write_tmp_file(self, contents):
        tmp_filename = self.new_temp_filename()
        f = self.py.__builtin__.open(tmp_filename, "w")
        
        # Add \r\n
        contents = re.sub("\r?\n", "\r\n", contents)
        f.write(contents)
        f.close()
        return tmp_filename
        
    def __call__(self, command, *args, **kwargs):
        """ executes the command on a Sheel in the remote system
        """
        def build_command():
            command_args = []
            command_args.extend([a for a in args])
            command_args.extend(["%s=%s" % (k, sh_escape(kwargs[k])) for k in kwargs])
            c = '%s %s' % (command, " ".join(command_args))
            if self.verbose:
                print ">", c
            return c     
        
        subprocess_module = self.c.modules.subprocess
        full_command = build_command()
        p = subprocess_module.Popen(full_command, shell = True, stdout = subprocess_module.PIPE, stderr = subprocess_module.STDOUT)
        self.status = p.stdout.read()
        p.wait()
        self.code = p.returncode
        self.last_command = full_command
        if self.verbose:
            for l in self.status.split("\n"):
                print "%s< %s" % (self.hostname, l)
        return self.status
    
if __name__ == "__main__":
    import optparse
    
    parser = optparse.OptionParser()
    parser.add_option("--host", dest="hostname", default="localhost")
    (options, args) = parser.parse_args()
        
    remote_run = RemoteRun(options.hostname)
    
    print remote_run(" ".join(args))