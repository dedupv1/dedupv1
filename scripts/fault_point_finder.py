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
import os
import optparse
import re

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option("--src",
        dest="src_root",
        default=".")
    (options, argv) = parser.parse_args()
    
    prog = re.compile(".*FAULT_POINT\(\"(.*)\"\).*")
    
    for (dirpath, dirnames, filenames) in os.walk(options.src_root):
        # do not look in thirdpary dir
        if dirpath.find("thirdparty") >= 0 or dirpath.find("build") >= 0:
            continue
        if dirpath.find("src") >= 0:
            filenames = filter(lambda filename: filename.endswith(".cc") or filename.endswith(".h"), filenames)
            for filename in filenames:
                filename = os.path.join(dirpath, filename)
                lines = [line for line in open(filename, "r")]
                for line_no, line in zip(range(len(lines)), lines):
                    result = prog.match(line)
                    if result:
                        print "%s\t%s:line %s" % (result.group(1), filename, line_no)