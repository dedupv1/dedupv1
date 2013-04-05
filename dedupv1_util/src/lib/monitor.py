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


import urllib
import json

class MonitorException(Exception):
    """ a base exception for all monitor replated exceptions
    """
    def __init__(self, message, base = None):
        """ inits a monitor exception
        """
        Exception.__init__(self)
        self.msg = message
        self.base = base

    def __str__(self):
        if self.base == None:
            return self.msg
        else:
            return self.msg + "\n\tCause: " + self.base.__str__()

class MonitorJSONException(MonitorException):
    """ Exception class that is raised if the return data from
        a monitor is not JSON-formatted.
    """

    def __init__(self, raw_text, base_exception = None):
        """ inits the monitor json exception
        """
        MonitorException.__init__(self, "Illegal JSON formatting", 
                                  base_exception)

        self.raw_text = raw_text

class Monitor:
    """ class of read data from the dedupv1d monitor system
    """

    def __init__(self, hostname, port):
        """ inits the monitor reader
        """
        self.hostname = hostname
        self.port = port

    def read(self, monitor, params = [], allow_non_json = False):
        """ reads the monitor with the given parameters
        """

        try:
            # Build the urls
            url = "http://%s:%s/%s?" % (self.hostname, self.port, monitor)
            url += urllib.urlencode(params)

            # Make the call
            buf = urllib.urlopen(url).read()
        except Exception as ex:
            raise MonitorException("Failed to read monitor %s, params %s" % (monitor, str(params)), ex)

        try:
            return json.loads(buf)
        except Exception as ex:
            if allow_non_json:
                return buf
            else:
                raise MonitorJSONException(buf, base_exception = ex)
