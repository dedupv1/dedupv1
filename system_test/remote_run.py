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
import uuid
import rpyc
import re

def sh_escape(s):
    return s.replace("(","\\(").replace(")","\\)").replace(" ","\\ ")

class RemoteRun:
    """ remote execution of shell commands and python commands
        using rpyc.
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
        stdin = kwargs.get("stdin")
        if stdin:
            del kwargs["stdin"]
            full_command = build_command()
            p = subprocess_module.Popen(full_command, shell = True, stdin = subprocess_module.PIPE, stdout = subprocess_module.PIPE, stderr = subprocess_module.STDOUT)
            for line in stdin.split("\n"):
                print ">", encode(line)
                p.stdin.write(line + "\n")
        else:
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
