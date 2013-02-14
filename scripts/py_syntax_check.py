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
import py_compile

def is_python_source_file(filename):
    """ checks if the given filename is a python file
    """
    if os.path.isdir(filename):
        return False
    if filename.endswith("/work"):
        return False
    if filename.endswith(".py"):
        return True
    first_line = open(filename, "r").readline()
    if first_line == "#!/usr/bin/python\n":
        return True
    return False

ignore = set(["thirdparty", "build", ".git"])

def filter_ignore_dirs(dirs):
    """ filter out all directories that should be ignores
    """
    dirs_to_ignore = []
    for dir in dirs:
        if dir in ignore:
            dirs_to_ignore.append(dir)
        
    for dir in dirs_to_ignore:
        dirs.remove(dir)
        
        
if __name__ == "__main__":
    # Runs through all files, searches python files and tries to compile them
    # pycompile does a similar thing as import, but takes the path name instead the module name
    # This does find syntax errors are using a illegal grammar or a wrong indention
    
    syntax_error_count = 0
    for root, dirs, files in os.walk('.'):
        filter_ignore_dirs(dirs)
        
        for f in files:
            if f in ignore:
                continue
            
            f = os.path.join(root, f)
            if not os.path.exists(f):
                continue
            if is_python_source_file(f):
                try:
                    py_compile.compile(f, doraise=True)
                except py_compile.PyCompileError as e:
                    print "%s: %s" % (f, str(e))
                    syntax_error_count += 1
                except Exception as e:
                    print "%s: %s" % (f, str(e))
    
    sys.exit(syntax_error_count)
