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
from xml.dom import minidom
import os
import sys

m = {"dedupv1_base_cov.xml": {"base.include.base": "base.include", "build.debug-coverage.dedupv1_base_test.dedupv1_base_obj": "base.src"},
"dedupv1_contrib_cov.xml": {"contrib.dedupv1_replay.src": "dedupv1_replay.src", "contrib.dedupv1_check.src": "dedupv1_check.src", "contrib.dedupv1_read.src": "dedupv1_read.src", "contrib.dedupv1_chunk_restorer.src": "dedupv1_chunk_restorer.src"},
"dedupv1_core_cov.xml": {"build.debug-coverage.dedupv1_core_test.dedupv1_core_obj": "core.src", "core.include.core": "core.include"},
"dedupv1d_cov.xml": {"build.debug-coverage.dedupv1d_test.dedupv1d_obj": "dedupv1d.src", "dedupv1d.include": "dedupv1d.include"}
}

if __name__ == "__main__":
	for arg in sys.argv[1:]:
		file_data = m.get(os.path.basename(arg), None)
		if file_data:
			data = minidom.parse(arg)
			for ref in data.getElementsByTagName("package"):
				package_name = ref.getAttribute("name")
				if package_name in file_data:
					new_package_name = file_data[package_name]
					ref.setAttribute("name", new_package_name)
				else:
					# Unwanted package
					packagesRef = ref.parentNode
					packagesRef.removeChild(ref)	
			f = open(arg, "w")
			data.writexml(f)
		else:
			print arg
			data = minidom.parse(arg)
			for ref in data.getElementsByTagName("package"):
				package_name = ref.getAttribute("name")
				print package_name	
			sys.exit(1)
	sys.exit(0)