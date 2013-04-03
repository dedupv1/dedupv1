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
import sys
import subprocess
import re
import optparse
import os

pattern_prefix = r"[0-9A-Za-z\-\_\\/]*"
pattern = [("dedupv1_base_obj/", "base/src/"),
               ("dedupv1_base_test/", "base/unit_test/"),
               ("dedupv1_core_test/", "core/unit_test/"),
               ("dedupv1_core_obj/", "core/src/"),
               ("dedupv1d_obj/", "dedupv1d/src/"),
               ("dedupv1d_test/", "dedupv1d/unit_test/"),
               ("dedupv1_check/", "contrib/dedupv1_check/"),
               ("dedupv1_contrib_test/", "contrib/unit_test/")]

compiled_pattern = [(re.compile(pattern_prefix + p[0]), p[1]) for p in pattern]

def convert(stream):
    line = stream.readline()

    gcc_pattern = re.compile(r"(.*g\+\+)(.*)")
    while(line):
        line = line.strip()

        m = gcc_pattern.match(line)
        if m:
            l = line.split(" ")
            if l[-1].endswith(".cc"):
                line = "%s %s" % (m.groups()[0], l[-1]) 
            else:
                # Linking
                line = "%s %s" % (m.groups()[0], l[2]) 
        for p in compiled_pattern:
            m = p[0].match(line)
            if m:
                print m.groups()
            line = p[0].sub(p[1], line)
        print line
        line = stream.readline()

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option("--stdin",
        dest="stdin",
        action="store_true",
        default=False)
    (options, argv) = parser.parse_args()

    try:
        if options.stdin:
            convert(sys.stdin)
        else:
            proc = subprocess.Popen(" ".join(sys.argv[1:]), shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)  
            convert(proc.stdout)
    except Exception as e:
        raise
        print str(e)
        sys.exit(1)
    sys.exit(0)
