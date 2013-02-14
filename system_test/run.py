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

def sh_escape(s):
    return s.replace("(","\\(").replace(")","\\)").replace(" ","\\ ")

class Run:
    def __init__(self, verbose = True):
        self.status = ""
        self.code = 0
        self.verbose = verbose
        self.last_command = ""
        
    def delay_call(self, delay, command, *args, **kwargs):
        from multiprocessing import Process
        def f():
            sleep(delay)
            self.__call__(command, *args, **kwargs)
        p = Process(target=f)
        p.start()
        return p
        
    def __call__(self, command, *args, **kwargs):
        def encode(s):
            if s:
                return s.encode("ascii", "replace")
            else:
                return s
            
        def build_command(user = None):
            command_args = []
            command_args.extend([a for a in args])
            command_args.extend(["%s=%s" % (k, sh_escape(kwargs[k])) for k in kwargs])
            c = '%s %s' % (command, " ".join(command_args))
            if user:
                c = "sudo -u %s %s" % (user, c)
            if self.verbose:
                print ">", encode(c)
            return c
        
        cwd = kwargs.get("cwd")
        if cwd:
            del kwargs["cwd"]
        user = kwargs.get("user")
        if user:
            del kwargs["user"]
        full_command = ""
        stdin = kwargs.get("stdin")
        if stdin:
            del kwargs["stdin"]
            full_command = build_command(user)
            p = subprocess.Popen(full_command, shell = True, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT, cwd = cwd)
            for line in stdin.split("\n"):
                print ">", encode(line)
                p.stdin.write(line + "\n")
        else:
            full_command = build_command(user)
            p = subprocess.Popen(full_command, shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT, cwd = cwd)
            self.status = p.stdout.read()
        p.wait()
        self.code = p.returncode
        self.last_command = full_command
        if self.verbose:
            for l in self.status.split("\n"):
                print "<",  encode(l)
        return self.status