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


import sys
import os
import subprocess

class ExecutionError(Exception):
    """ Exception class that is thrown when the
        execution of a shell command failed
    """
    def __init__(self, message, result = None, base = None, output = None):
        """ Constructor
        """
        self.msg = message
        self.base = base
        self.result = result
        self.output = output
        
    def __str__(self):
        if self.base == None:
            return self.msg
        else:
            return self.msg + ": " + self.base.__str__()

def run_cmd(cmd_line):
    """ runs the given command until completion and outputs the stdout """
    
    os.system(cmd_line)

def sh_escape(s):
    """ escape a string for the usage in a shell.
        In particular, spaces are replaces by "\ ".
    """
    return s.replace("(","\\(").replace(")","\\)").replace(" ","\\ ")

def execute_cmd(cmd_line, 
                cwd = None, 
                direct_output = False, 
                env = None, 
                join = True):
    """ executes the given command until completion and returns the output """
    try:
        output = []
        bufsize = -1
        if(direct_output):
            bufsize = 1
        if cwd:
            cwd = sh_escape(cwd)
        p = subprocess.Popen(cmd_line, 
                             shell = True, 
                             stdout = subprocess.PIPE, 
                             stderr = subprocess.STDOUT, 
                             bufsize = bufsize, 
                             cwd = cwd, 
                             env = env)
        line = ""
        try:
            line = p.stdout.readline()
            while(line):
                if direct_output:
                    print line.strip()
                output.append(line)
                line = p.stdout.readline()
            rc = p.wait()
            if not rc == 0:
                output = ("".join(output))
                raise ExecutionError(
                    "Execution %s failed with error code %s: %s" % (cmd_line, str(rc), output), rc, output = output)
        except KeyboardInterrupt:
            # when we see a Ctrl+C we also kill the subprocess
            p.kill()
            while(line):
                if direct_output:
                    print line.strip()
                output.append(line)
                line = p.stdout.readline()
            rc = p.wait()
            if not rc == 0:
                output = ("".join(output))
                raise ExecutionError(
                    "Execution %s failed: %s" % (cmd_line, output), rc, output = output)
            return "".join(output)
        if join:
            return "".join(output)
        return output
    except ExecutionError as e:
        raise
    except Exception as e:
        output = ("".join(output))
        raise ExecutionError(
            "Execution %s failed: %s" % (cmd_line, output), base = e, output = output)
