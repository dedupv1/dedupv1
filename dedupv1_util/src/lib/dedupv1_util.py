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


def parse_params(config_params, args):
    for option in args:
        options = option.partition("=")
        option_pair = [options[0], options[2]]
        if len(option_pair[0]) == 0 or len(option_pair[1]) == 0:
            raise Exception("Illegal option: " + option);
        config_params.append((option_pair[0], option_pair[1]))
    return config_params

def format_storage_unit(n):
    """ a python port of dedupv1::base::strutil::FormatStorageUnit
    """
    if n >= 1024L * 1024 * 1024 * 1024:
        return "%sT" % (1.0 * n / (1024L * 1024 * 1024 * 1024))
    elif n >= 1024L * 1024 * 1024:
        return "%sG" % (1.0 * n / (1024L * 1024 * 1024))
    elif n >= 1024L * 1024:
        return "%sM" % (1.0 * n / (1024L * 1024))
    elif n >= 1024L:
        return "%sK" % (1.0 * n / 1024L)
    return str(n)

def to_storage_unit(input):
    """ a python port of dedupv1::base::strutil::ToStorageUnit
    """
    if len(input) == 0:
        raise Exception("Invalid storage value")
    multi = 1
    if input[-1] == 'K' or input[-1] == 'k':
        multi = 1024
    elif input[-1] == 'M' or input[-1] == 'm':
        multi = 1024 * 1024
    elif input[-1] == 'G' or input[-1] == 'g':
        multi = 1024 * 1024 * 1024
    elif input[-1] == 'T' or input[-1] == 't':
        multi = 1024L * 1024 * 1024 * 1024
    elif not input[-1].isdigit():
        raise Exception("Invalid storage value")
    
    if multi == 1:
        if not input.isdigit():
            raise Exception("Invalid storage value")    
        return int(input)
    else:
        if not input[:-1].isdigit():
            raise Exception("Invalid storage value")
        return multi * int(input[:-1])
    
def format_large_number(i):
    """ formats a large number to be more readable.
        This is a python port of dedupv1::base:strutil::FormatLargeNumber 
    """
    s = str(i)
    if len(s) > 5:
        i = len(s) - 3
        while i >= 1:
            s = s[:i] + "," + s[i:]
            i = i - 3
            
def to_large_number(input):
    input.replace(",", "")
    return int(input)