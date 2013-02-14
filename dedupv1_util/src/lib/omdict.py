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


class omdict():
    """ A simple version of an ordered dictionary combined with a multimap
    """
    def __init__(self, list = []):
        self.l = []
        
        for (k, v) in list:
            self.l.append((k,v))
    
    def items(self):
	""" returns all items
	"""
        return self.l[:]
    
    def get_multi(self, key):
        return [o[1] for o in filter(lambda (k,v): k == key, self.l)]
    
    def get(self, key, d = None):
        f = self.get_multi(key)
        if len(f) == 0:
            return d
        if len(f) > 1:
            raise Exception("Illegal get_single call")
        return f[0]
    
    def append(self, (k, v)):
        self.l.append( (k, v))
        
    def __len__(self):
        return len(self.l)
    
    def __contains__(self, key):
        return len(self.get_multi(key)) > 0
    
    def __setitem__(self, key, value):
        self.l.append((key, value))
        
    def __delitem__(self, key):
        def del_one_entry():
            for i in xrange(len(self.l)):
                if self.l[i][0] == key:
                    del self.l[i]
                    return True
            return False
        
        while del_one_entry():
            pass
    
    def __getitem__(self, key):
        return self.get(key)
    
    def __iter__(self):
        return self.l.__iter__()
