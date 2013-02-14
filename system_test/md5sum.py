#!/bin/python
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

import os, sys
import hashlib

def md5(fileName):
    """Compute md5 hash of the specified file"""
    m = hashlib.md5()
    fd = open(fileName,"rb")
    content = fd.readlines()
    fd.close()
    for eachLine in content:
        m.update(eachLine)
    return m.hexdigest()

if __name__ == "__main__":
    for eachFile in sys.argv[1:]:
        print "%s %s" %(md5(eachFile), eachFile)
