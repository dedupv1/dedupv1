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
import tempfile
import fnmatch
import re

def check_snippit(src):
	def run_gcc_syntax_check(filename):
		cmd = "gcc -fsyntax-only %s" % (filename)
		p = subprocess.Popen(cmd,
			shell = True,
        	stdout = subprocess.PIPE, 
        	stderr = subprocess.STDOUT)
		rc = p.wait()
		return rc
		
	file_src = """
	int main() {
	%s
	;return 0;
	}
	""" % (src)
	
	temp_file = tempfile.NamedTemporaryFile(suffix='.cc', delete=True)
	temp_file.write(file_src)
	temp_file.flush()
	
	return run_gcc_syntax_check(temp_file.name) == 0

def find_comments(filename, file_contents, callback):
	pattern = re.compile(r"(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|//(.*)")
	
	count = 0
	for comment in pattern.findall(file_contents):
		if comment[0]:
			comment = comment[0][2:-2]
		elif comment[-1]:
			comment = comment[-1]
		else:
			continue
		if callback(filename, comment):
			count = count + 1
	return count

def found_comment(filename, comment_text):
	if comment_text.find("NOLINT") < 0 and check_snippit(comment_text):
		print filename, comment_text
		return True

def walk(argv):
	count = 0
	for arg in argv:
		for path, dirs, files in os.walk(arg):
		    for source_file in [os.path.join(path, filename) for filename in files if (fnmatch.fnmatch(filename, '*.cc') or fnmatch.fnmatch(filename, '*.h'))]:	
				file_contents = open(source_file, "r").read()
				count = count + find_comments(source_file, file_contents, found_comment)
	return count

if __name__ == "__main__":
	try:
		argv = sys.argv[1:]
		if len(argv) == 0:
			argv = ["."]
		count = walk(argv)
		sys.exit(count)
	except KeyboardInterrupt:
		pass