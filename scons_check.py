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
import subprocess
import os
import re
import platform

def _DefaultVersionCompare(expected_version, actual_version):
    """ 
        Default function to compare programm version.
        Assums a dot-delimited version.
        Accepts any actual version that is greater or equal to the expected version.
    """
    v1 = [int(v) for v in expected_version.split(".")]
    v2 = [int(v) for v in actual_version.split(".")]

    for (d1, d2) in zip(v1, v2):
        if d1 > d2:
            return False
        if d1 < d2:
            return True
    return True

def GenerateCheckLibraryVersionFunction(library, include_file, version_function, version_compare = _DefaultVersionCompare):
    """ 
        Function to generate library version check functions for any library that has a function that returns the
        version as char array (which are most).
    """
    def TrueFunction(context, version):
        # I have problems with the linking on the Mac, so for now the version checking is deactivated there
        context.Message('Checking for %s version >= %s... (disabled on Mac)' % (library, version))
        context.Result(True)
        return True
        
    def CheckLibraryVersion(context, version):    
        context.Message('Checking for %s version >= %s... ' % (library, version))
        ret = context.TryRun("""
        #include <stdio.h>
        #include <%s>

        int main() 
        {
            printf("%%s", %s());
            return 1;
        }
        """ % (include_file, version_function), '.cc')
        
        ret = False
        if context.lastTarget:
            # I have seen no way to get access to the programm output, so I execute it here
            actual_version = Execute(str(context.lastTarget))
            ret = version_compare(version, actual_version.strip())
        context.Result(ret)
        return ret
        
    if IsMac():
        return TrueFunction
    else:
        return CheckLibraryVersion

def CheckExecutable(context, executable):
    print "Checking for %s... " % (executable),
    p = None
    result = False
    try:
        p = subprocess.Popen("which %s" % executable, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    except OSError as e:
        print str(e)
        pass
    if p:
        p.wait()
        if p.returncode == 0:
            result = p.stdout.read().strip()
    context.Result(result)
    return (result != False)

def Execute(cmd, redirect_stderr = True):
    import subprocess
    if redirect_stderr:
        stderr = subprocess.STDOUT
    else:
        stderr = None
        
    p = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE, stderr = stderr)
    status = p.stdout.read()
    p.wait()
    return status
        
def CheckModprobe(context, module):
    print "Checking for kernel module %s... " % (module),
    p = None
    try:
        p = subprocess.Popen(["modprobe",module,"-n"])
        p.wait()
    except OSError:
        p = False
    result = p and p.returncode == 0
    context.Result(result)
    return result

def CheckModuleVersion(context, module, regex):
    print "Checking for kernel module version %s... " % (module),
    
    p = None
    try:
        p = subprocess.Popen(["/sbin/modinfo", module, "-F", "version"])
        p.wait()
    except OSError:
        p = None
    if not p:
        result = False
    else:
        version_str = p.read()
        if re.match(regex, version_str):
            result = True
        else:
            result = False
    context.Result(result)
    return result

def SconsVersion():
    def convert_if_possible(n):
        try:
            return int(n)
        except:
            return n
        
    import SCons
    return [convert_if_possible(n) for n in SCons.__version__.split(".")]

def IsMac():
    return platform.uname()[0] == "Darwin"

def print_config(msg, two_dee_iterable):
    # this function is handy and can be used for other configuration-printing tasks
    print
    print msg
    print
    for kv in two_dee_iterable:
        print "    %s" % kv
    print
    
def run(cmd):
    import subprocess
    try:
        p = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        return p.stdout.read()
    except:
        return None

class Config:
    def __init__(self, config):
        self.config = config[:]
        self.includes= []
        
    def append(self, item):
        self.config.append(item)
    
    def extend(self, items):
        self.config.extend(items)
        
    def add_include(self, include_file):
        self.includes.append(include_file)
        
    def BuildConfigModule(self, target, source):
        import re
        import hashlib
        import os
    
        defines = ""
        for kv in self.config:
            kv = kv.partition(" ")
            if not kv[2]:
                defines += "%s = True\n\n" % kv[0]
            else:
                defines += "%s = %s\n\n" % (kv[0], kv[2])
                
        subs = {"DEFINES": defines}
        for a_target, a_source in zip(target, source):
            
            config_h_in = file(str(a_source), "r")
            write_data = ""
            for read_data in config_h_in:
                for (s,r) in subs.items():
                    write_data += re.sub("\$\(%s\)" % s, r, read_data)
                    
            cs1 = hashlib.md5(write_data).hexdigest()
            if os.path.exists(a_target):
                old_data = file(str(a_target), "r").read()
                cs2 = hashlib.md5(old_data).hexdigest()
                if cs1 == cs2:
                    return
            print_config("Generating %s with the following settings:" % a_target,
                      self.config)
            
            config_h = file(str(a_target), "w")
            config_h.write(write_data)
            config_h_in.close()
            config_h.close()
        
    def BuildConfigHeader(self, target, source):
        import re
        import hashlib
        import os
    
        defines = []
        for kv in self.config:        
            kv2 = kv.partition(" ")
            defines.append("""#ifndef %s
#define %s
#endif
            """ % (kv2[0], kv))
        defines = "\n".join(defines)
        
        include_statements = []
        for include_file in self.includes:
            include_statements.append("""#include <%s>""" % include_file)
        include_statements = "\n".join(include_statements)  
                 
        subs = {"DEFINES": defines, "INCLUDES": include_statements}
        for a_target, a_source in zip(target, source):
            
            config_h_in = file(str(a_source), "r")
            write_data = ""
            for read_data in config_h_in:
                found = False
                for (s, r) in subs.items():
                    if re.match("\$\(%s\)" % s, read_data):
                        write_data += re.sub("\$\(%s\)" % s, r, read_data)
                        found = True
                        break
                if not found:
                    write_data += read_data
                    
            cs1 = hashlib.md5(write_data).hexdigest()
            if os.path.exists(a_target):
                old_data = file(str(a_target), "r").read()
                cs2 = hashlib.md5(old_data).hexdigest()
                if cs1 == cs2:
                    return
            print_config("Generating %s with the following settings:" % a_target,
                      self.config)
            
            config_h = file(str(a_target), "w")
            config_h.write(write_data)
            config_h_in.close()
            config_h.close()
