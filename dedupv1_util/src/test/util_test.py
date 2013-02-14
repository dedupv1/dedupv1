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
import unittest
import os
import optparse
import re

sys.path.append("../lib")
if "DEDUPV1_ROOT" not in os.environ:
    os.environ["DEDUPV1_ROOT"] = "/opt/dedupv1"
DEDUPV1_ROOT = os.environ["DEDUPV1_ROOT"]
sys.path.append(os.path.normpath(os.path.join(DEDUPV1_ROOT, "lib/python")))
for file in [f for f in os.listdir(os.path.join(DEDUPV1_ROOT, "lib/python2.6/site-packages/")) if f.endswith(".egg")]:
    sys.path.append(os.path.normpath(os.path.join(DEDUPV1_ROOT, "lib/python2.6/site-packages/", file)))

# Insert here all new test classes until we switch to Python 2.7 or Python 3.1
from dedupv1_util_test import Dedupv1UtilTest
from protobuf_util_test import ProtobufTest    

def perform_unittests(unit_test_classes, option_args=None):
    def decorate_parser(parser=None):
        if not parser:
            parser = optparse.OptionParser()        
        parser.add_option("--xml",
            action="store_true",
            dest="xml",
            default=False)
        parser.add_option("--output",
            dest="output",
            default=".")
        return parser
    
    def get_test_suite(argv):
        suite = unittest.TestSuite()

        test_name_pattern_list = [re.compile("^%s$" % p) for p in argv]
        if len(test_name_pattern_list) == 0:
            # default pattern
             test_name_pattern_list.append(re.compile("^.*$"))
             
        for unit_test_class in unit_test_classes:
            for test_case_name in unittest.defaultTestLoader.getTestCaseNames(unit_test_class):
                for test_name_pattern in test_name_pattern_list:
                    if test_name_pattern.match(test_case_name):
                        suite.addTest(unit_test_class(test_case_name))
        return suite
    
    def get_test_runner():
        if options.xml:
            import xmlrunner
            return xmlrunner.XMLTestRunner(output=options.output, verbose=True)
        else:
            return unittest.TextTestRunner(verbosity=2)    
                    
    global output_dir
    try:    
        if not option_args:
            option_args = decorate_parser().parse_args()
        (options, argv) = option_args
        
        suite = get_test_suite(argv)
        runner = get_test_runner()
        result = runner.run(suite)

    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    perform_unittests(
                       [Dedupv1UtilTest,
                        ProtobufTest]
                        )    