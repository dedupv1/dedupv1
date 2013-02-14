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


from monitor import MonitorException
import sys
from urllib import quote_plus
import re
import traceback
import syslog
import config
import simplejson

def is_syslog_enabled():
    """ checks if the syslog mode is enabled. The
        syslog mode is enabled if LOGGING_SYSLOG=True is defined in the auto-
        generated config.py
    """
    return "LOGGING_SYSLOG" in dir(config) and config.LOGGING_SYSLOG

def is_debug_options_enabled(options):
    """ checks if the debug option is set
    """
    return options and "debug" in dir(options) and options.debug

def is_verbose_enabled(options):
    """ checks if the verbose option is set
    
        if the verbose option is set, more output should be printed. This
        output is not necessary, but might help in case of problems.
    """
    return options and "verbose" in dir(options) and options.verbose

def is_raw_enabled(options):
    """ checks if the raw option is set
    
        If the raw option is set, only the main output should be printed in
        JSON format. If an error occured, the JSON should contain an object with an
        error key and the error message as text
    """
    return options and "raw" in dir(options) and options.raw

def log_info(options, message_or_exception):
    """ logs an information message
    """
    if not is_raw_enabled(options):
        print str(message_or_exception)
    if is_syslog_enabled():
        syslog.syslog(syslog.LOG_INFO, str(message_or_exception))
    
def log_warning(options, message_or_exception):
    """ logs a warning message
    """
    if not is_raw_enabled(options):
        print "WARNING", str(message_or_exception)
    if is_syslog_enabled():
        syslog.syslog(syslog.LOG_WARNING, str(message_or_exception))
    
def log_error(options, message_or_exception):
    """ logs an error message
    """
    if is_raw_enabled(options):
        print >> sys.stderr, simplejson.dumps({"ERROR": str(message_or_exception)})
    else:
        print >> sys.stderr, str(message_or_exception)
    if is_syslog_enabled():
        syslog.syslog(syslog.LOG_ERR, str(message_or_exception))
    if is_debug_options_enabled(options) and isinstance(message_or_exception, Exception):
        raise # reraise, hopefully no exception happened during the logging
        
def log_verbose(options, message_or_exception):
    """ logs a verbose message
    """
    if is_syslog_enabled():
        syslog.syslog(syslog.LOG_DEBUG, str(message_or_exception))
    if is_verbose_enabled(options):
        print str(message_or_exception)
    