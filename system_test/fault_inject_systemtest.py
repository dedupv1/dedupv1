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

from systemtest import Dedupv1BaseSystemTest, perform_systemtest
from time import sleep

class Dedupv1FaultInjectSystemTest(Dedupv1BaseSystemTest):
    
    def setUp(self):
        super(Dedupv1FaultInjectSystemTest, self).setUp()   

    def is_fault_injection_enabled(self):
        return self.dedupv1.monitor("fault-inject")["fault injection"]
        
    def test_gc_process_after_chunk_index_delete(self):
        """ tests the behavior of the gc.process.after-chunk-index-delete fault point
        """
        def is_dedupv1d_running():
            m = self.dedupv1.monitor("status", "--raw")
            if "state" not in m:
                return False
            return "ok" == m["state"]
        
        self.start_default_system()
        self.assertTrue(self.is_fault_injection_enabled())
        
        self.dedupv1.monitor("fault-inject", "crash=gc.process.after-chunk-index-delete")
        
        size = 256
        filename = self.get_urandom_file(size)

        self.data.copy_raw(filename, size / 2)
        self.data.copy_raw(filename, size / 2, skip=size / 2) # overwrite
        
        sleep(5)
        self.dedupv1.monitor("idle", "force-idle=true")
    
        while is_dedupv1d_running():
            sleep(30)
        sleep(10)    
        
        self.dedupv1.start()
        self.assertExitcode(0)

    def test_container_storage_ack_container_merge_after_put(self):
        """ tests the behavior of the container-storage.ack.container-merge-after-put fault point
        """
        def is_dedupv1d_running():
            m = self.dedupv1.monitor("status", "--raw")
            if "state" not in m:
                return False
            return "ok" == m["state"]
        
        self.start_default_system()
        self.assertTrue(self.is_fault_injection_enabled())
        
        self.dedupv1.monitor("fault-inject", "crash=container-storage.ack.container-merge-after-put")
        
        size = 256
        filename = self.get_urandom_file(size)

        self.data.copy_raw(filename, size / 2)
        self.data.copy_raw(filename, size / 2, skip=size / 2) # overwrite
        
        sleep(5)
        self.dedupv1.monitor("idle", "force-idle=true")
    
        while is_dedupv1d_running():
            sleep(30)
        sleep(10)    
        
        self.dedupv1.start()
        self.assertExitcode(0)

    def test_container_storage_ack_container_merge_after_secondary_put_with_empty_containers(self):
        """ tests the behavior of the container-storage.ack.container-merge-after-put fault point
            Here we ensure that a lot of containers are nearly empty so that we have early merges
        """
        def is_dedupv1d_running():
            m = self.dedupv1.monitor("status", "--raw")
            if "state" not in m:
                return False
            return "ok" == m["state"]
        
        self.adapt_config_file("storage.gc=greedy",
                "storage.gc=greedy\nstorage.gc.eviction-timeout=0")
        self.start_default_system()
        self.assertTrue(self.is_fault_injection_enabled())
        
        self.dedupv1.monitor("fault-inject", "crash=container-storage.ack.container-merge-after-secondary-put hit-points=3")
        
        filename = self.get_urandom_file(1)
        self.data.copy_raw(filename, 1)
        self.dedupv1.monitor("flush") # We want nearly empty containers
        
        self.dedupv1.monitor("idle", "force-busy=true") # We want no idling here.
        
        output = self.dedupv1.monitor("container-gc", "merge=1:2")
        self.assertFalse("ERROR" in output)
        
        output = self.dedupv1.monitor("container-gc", "merge=3:4")
        self.assertFalse("ERROR" in output)
       
        output = self.dedupv1.monitor("container-gc", "merge=1:3")
        
        self.dedupv1.start()
        self.assertExitcode(0)

if __name__ == "__main__":
    perform_systemtest([Dedupv1FaultInjectSystemTest])