#!/usr/bin/python
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
import optparse
import time
import subprocess

lib_path = os.path.normpath(os.path.join("/opt/dedupv1/lib/dedupv1"))
sys.path.append(lib_path)
import dedupv1
import command
import monitor

def extract_pid(options):
    lock_file_data = open(options.lock_file, "r").read()
    pid = int(lock_file_data.strip())
    return pid

if __name__ == "__main__":
    if not dedupv1.check_root():
        print "Use as superuser e.g. via sudo"
        sys.exit(1)
        
    parser = optparse.OptionParser()
    parser.add_option("--lock-file",
        dest="lock_file",
        default="/opt/dedupv1/var/lock/dedupv1d")
    parser.add_option("--log-file",
        dest="log_file",
        default="/opt/dedupv1/var/log/dedupv1/dedupv1.log")
    parser.add_option("--kill-delay",
        dest="kill_delay",
        type="int",
        default=60)
    parser.add_option("-v", "--verbose",
        dest="verbose",
        action="store_true",
        default=False)
    (options, argv) = parser.parse_args()
    
    p = subprocess.Popen("tail -f %s" % (options.log_file), shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
    if options.verbose:
        print "Started watching log file", options.log_file
    for line in p.stdout:
        if line.find("ERROR") >= 0:
            # Found error
            break
    else:
        sys.exit(0)
    # found an error
    if options.verbose:
        print "Found error in log"
    time.sleep(options.kill_delay)
    if options.verbose:
        print "Kill dedupv1d"
    os.kill(pid, 9)
