#!/usr/bin/python
#
# dedupv1 - iSCSI based Deduplication System for Linux
# 
# (C) 2008 Dirk Meister
# (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
# 

import os
import sys
import shutil
import optparse
import subprocess
import platform

def is_mac():
    return platform.uname()[0] == "Darwin"

def run(cmd, cwd = None, env = None):
    p = None
    rc = None
    try:
        new_env = {}
        new_env.update(os.environ)
        if env:
            new_env.update(env)
        p = subprocess.Popen(cmd, cwd = cwd, shell = True, env = new_env)
        rc = p.wait()
    except Exception as e:
        print e
        raise Exception(cmd)
    if not p:
        return None
    if rc != 0:
        raise Exception(cmd)

def get_cpu_count():
    import multiprocessing
    return multiprocessing.cpu_count()

def install(*args, **kwargs):
    def _install_fn(fcn):
        fcn.modules = args
        fcn.options = kwargs
        return fcn
    return _install_fn

@install()
def install_crcutil(options):
    print "Install crcutil"
    run("tar -zxf crcutil-1.0.tar.gz")
    run("./configure --prefix=/opt/dedupv1/", cwd="crcutil-1.0/")

    if not os.path.exists("crcutil-1.0/build"):
        os.mkdir("crcutil-1.0/build")
    run("for i in code/*.cc ; do g++ -DHAVE_CONFIG_H -I. -fPIC -DCRCUTIL_USE_MM_CRC32=1 -Wall -msse2 -Icode -g -O3 -fomit-frame-pointer -c -o build/`basename $i .cc`.o $i ; done", cwd="crcutil-1.0/")
    run("g++ -DHAVE_CONFIG_H -I. -fPIC -Iexamples -Itests  -DCRCUTIL_USE_MM_CRC32=1 -Wall -msse2 -Icode -g -O3 -fomit-frame-pointer -c -o build/interface.o examples/interface.cc", cwd="crcutil-1.0/")
    run("ar rcs libcrcutil.a *.o", cwd="crcutil-1.0/build/")
    run("cp libcrcutil.a /opt/dedupv1/lib", cwd="crcutil-1.0/build/")
    if not os.path.exists("/opt/dedupv1/include/crcutil"):
        os.mkdir("/opt/dedupv1/include/crcutil")
    run("cp *.h /opt/dedupv1/include/crcutil", cwd="crcutil-1.0/code/")
    run("cp interface.h /opt/dedupv1/include/crcutil", cwd="crcutil-1.0/examples/")
    run("cp aligned_alloc.h /opt/dedupv1/include/crcutil", cwd="crcutil-1.0/tests/")
    #run("sudo ldconfig", cwd="crcutil-1.0/")

@install()
def install_tc(options):
    print "Install tokyocabinet"
    run("tar -zxf dmeister-tokyocabinet-5ad8b9b.tar.gz") 
    run("LDFLAGS=-L/opt/dedupv1/lib  ./configure --enable-fastest --prefix=/opt/dedupv1", cwd = "dmeister-tokyocabinet-5ad8b9b/")
    run("make -j%s" % get_cpu_count(), cwd = "dmeister-tokyocabinet-5ad8b9b/")
    run("make install", cwd = "dmeister-tokyocabinet-5ad8b9b/")

@install()
def install_mhd(options):
    print "Install libmicrohttpd"
    run("tar -xzf libmicrohttpd-0.9.15.tar.gz")
    run("./configure --prefix=/opt/dedupv1", cwd="libmicrohttpd-0.9.15")
    run("make -j%s" % get_cpu_count(), cwd="libmicrohttpd-0.9.15")
    run("make install", cwd="libmicrohttpd-0.9.15")

@install()
def install_leveldb(options):
    print "Install leveldb"
    run("tar -xzf leveldb-1.9.0.tar.gz")
    run("make", cwd="leveldb-1.9.0")
    run("cp -r include/leveldb /opt/dedupv1/include", cwd="leveldb-1.9.0")
    run("cp libleveldb* /opt/dedupv1/lib", cwd="leveldb-1.9.0")

@install()
def install_apr(options):
    print "Install apr"
    run("tar -zxf apr-1.3.8.tar.gz")
    run("./configure --prefix=/opt/dedupv1", cwd="apr-1.3.8")
    run("make -j%s" % get_cpu_count(), cwd="apr-1.3.8")
    run("make install", cwd="apr-1.3.8")

@install()
def install_apr_util(options):
    print "Install apr-util"
    run("tar -zxf apr-util-1.3.9.tar.gz")
    run("./configure --prefix=/opt/dedupv1 --with-apr=/opt/dedupv1", cwd="apr-util-1.3.9")
    run("make -j%s" % get_cpu_count(), cwd="apr-util-1.3.9")
    run("make install", cwd="apr-util-1.3.9")

@install()
def install_log4cxx(options):
    print "Install log4cxx"
    run("tar -zxf apache-log4cxx-0.10.0.tar.gz")
    run("./configure --prefix=/opt/dedupv1 --with-apr=/opt/dedupv1 --with-apr-util=/opt/dedupv1", cwd="apache-log4cxx-0.10.0")
    # I don't know what is wrong here, but here is a patch
    run("patch -p 1 < ../apache-log4cxx-0.10.0.patch", cwd="apache-log4cxx-0.10.0")
    run("make -j%s" % get_cpu_count(), cwd="apache-log4cxx-0.10.0")
    run("make install", cwd="apache-log4cxx-0.10.0")

@install()
def install_protobuf(options):
    print "Install protobuf"
    run("tar -zxf protobuf-2.5.0.tar.gz")
    run("./configure --prefix=/opt/dedupv1", cwd="protobuf-2.5.0")
    run("make -j%s" % get_cpu_count(), cwd="protobuf-2.5.0")
    run("make install", cwd="protobuf-2.5.0")

    if not os.path.exists("/opt/dedupv1/lib/python2.7"):
        os.mkdir("/opt/dedupv1/lib/python2.7")
    if not os.path.exists("/opt/dedupv1/lib/python2.7/site-packages"):
        os.mkdir("/opt/dedupv1/lib/python2.7/site-packages")
    run("python2.7 setup.py build",
        cwd="protobuf-2.5.0/python",
        env = {"PYTHONPATH": "/opt/dedupv1/lib/python2.7/site-packages"})
    run("python2.7 setup.py install --prefix=/opt/dedupv1",
        cwd="protobuf-2.5.0/python",
        env = {"PYTHONPATH": "/opt/dedupv1/lib/python2.7/site-packages"})

@install("test")
def install_gtest(options):
    print "Install gtest"
    run("tar -xzf gtest-1.5.0.tar.gz")
    run("./configure --prefix=/opt/dedupv1", cwd="gtest-1.5.0")
    run("make", cwd="gtest-1.5.0")
    run("make install", cwd="gtest-1.5.0")

@install()
def install_gtest_prod(options):
    print "Install gtest Production header"

    if not os.path.exists("/opt/dedupv1/include/gtest/gtest_prod.h"):
        run("tar -xzf gtest-1.5.0.tar.gz")
        if not os.path.exists("/opt/dedupv1/include/test"):
            os.mkdir("/opt/dedupv1/include/gtest")
        shutil.copy("gtest-1.5.0/include/gtest/gtest_prod.h", "/opt/dedupv1/include/gtest/gtest_prod.h")

@install("test")
def install_gmock(options):
    print "Install gmock"
    run("tar -xzf gmock-1.5.0.tar.gz")
    run("./configure --prefix=/opt/dedupv1", cwd="gmock-1.5.0")
    run("make -j%s" % get_cpu_count(), cwd="gmock-1.5.0")
    run("make install", cwd="gmock-1.5.0")

@install(os="linux")
def install_scst(options):
    print "Install scst"
    run("tar -xf scst-r4758.tar.gz")

    run("patch -p 1 < ../scst_perm.patch", cwd="scst")
    run("patch -p 0 < ../scst_iscsi_cmnd_tx_end.patch", cwd="scst")

    # Activate expected values
    run('sed -i.aa s/"#EXTRA_CFLAGS += \-DCONFIG_SCST_USE_EXPECTED_VALUES"/"EXTRA_CFLAGS += \-DCONFIG_SCST_USE_EXPECTED_VALUES"/ Makefile',
        cwd = "scst/scst/src")
    
    run("make enable_proc", cwd="scst")
    if not options.debug:
        run("make 2perf", cwd = "scst/scst")
    run("make clean", cwd ="scst/scst")
    run("make -j%s" % get_cpu_count(), cwd = "scst/scst")
    run("sudo -E make install", cwd = "scst/scst")

    # scst_local
    if not options.debug:
        run("make 2perf", cwd = "scst/scst_local")
    if options.scst_sbin_dir: 
        run("sed -i 's;SBINDIR := /usr/local/sbin;SBINDIR := %s;ig' Makefile" % options.scst_sbin_dir, 
            cwd = "scst/scst_local")
    run("make clean", cwd = "scst/scst_local")
    run("make -j%s" % get_cpu_count(), cwd = "scst/scst_local")
    run("sudo -E make install", cwd = "scst/scst_local")

    # iscsi-scst
    if not options.debug:
        run("make 2perf", cwd = "scst/iscsi-scst")
    
    if options.scst_sbin_dir: 
        run("sed -i 's;SBINDIR := /usr/local/sbin;SBINDIR := %s;ig' Makefile_user_space_only" % options.scst_sbin_dir, 
            cwd = "scst/iscsi-scst")
        run("sed -i 's;SBINDIR := /usr/local/sbin;SBINDIR := %s;ig' Makefile" % options.scst_sbin_dir, 
            cwd = "scst/iscsi-scst")
    
    # dmeister: I have no clue why we need make clean here, after an update to Ubuntu 10.10 it is needed
    run("make clean", cwd="scst/iscsi-scst")
    run("make -j%s" % get_cpu_count(), cwd = "scst/iscsi-scst/")
    run("sudo -E make install", cwd = "scst/iscsi-scst/")
    run("sudo cp scst_iscsi_initd.debian /etc/init.d/iscsi-scst")

@install()
def install_jsoncpp(options):
    print "Install jsoncpp"
    run("tar -xzf jsoncpp-src-0.5.0.tar.gz")
    run("scons platform=linux-gcc", cwd="jsoncpp-src-0.5.0")
    
    p = subprocess.Popen("gcc -dumpversion",shell = True, stdout = subprocess.PIPE)
    cxx_version=p.stdout.read().strip()
    
    if os.path.exists("/opt/dedupv1/include/json"):
        shutil.rmtree("/opt/dedupv1/include/json")
        #os.mkdir("/opt/dedupv1/include/json")

    if not is_mac():
        shutil.copytree("jsoncpp-src-0.5.0/include/json", "/opt/dedupv1/include/json")
        shutil.copy("jsoncpp-src-0.5.0/libs/linux-gcc-%s/libjson_linux-gcc-%s_libmt.a" % (cxx_version, cxx_version), "/opt/dedupv1/lib/libjson.a")
        shutil.copy("jsoncpp-src-0.5.0/libs/linux-gcc-%s/libjson_linux-gcc-%s_libmt.so" % (cxx_version, cxx_version), "/opt/dedupv1/lib/libjson.so")
    else:
        shutil.copytree("jsoncpp-src-0.5.0/include/json", "/opt/dedupv1/include/json")
        shutil.copy("jsoncpp-src-0.5.0/libs/linux-gcc-%s/libjson_linux-gcc-%s_libmt.a" % (cxx_version, cxx_version), "/opt/dedupv1/lib/libjson.a")
        shutil.copy("jsoncpp-src-0.5.0/libs/linux-gcc-%s/libjson_linux-gcc-%s_libmt.dylib" % (cxx_version, cxx_version), "/opt/dedupv1/lib/libjson.dylib")
        
        # Fix library path
        # Without it the first build path (e.g. buildscons/linux-gcc-4.2.1/src/lib_json/libjson_linux-gcc-4.2.1_libmt.dylib)
        # is baked into the library, linking will succeed, but running will fail because
        # the library cannot be found. Here we change the path to the correct one
        # More information: http://blogs.sun.com/dipol/entry/dynamic_libraries_rpath_and_mac
        run("install_name_tool -id \"/opt/dedupv1/lib/libjson.dylib\" /opt/dedupv1/lib/libjson.dylib")
        
@install()
def install_bz2(options):
    print "Install bz2"
    run("tar -xzf bzip2-1.0.5.tar.gz")
    run("make -j%s CFLAGS=\"-fPIC -Wall -Winline -O2 -g -D_FILE_OFFSET_BITS=64\"" % get_cpu_count(), cwd="bzip2-1.0.5")
    run("make install PREFIX=/opt/dedupv1", cwd="bzip2-1.0.5")

@install()
def install_lz4(options):
    print "Install lz4"
    run("tar -xzf lz4-r53.tar.gz")
    shutil.copyfile("lz4-Makefile", "lz4-r53/Makefile")
    run("make -j%s CFLAGS=\"-fPIC -Wall -Winline -O2 -g -D_FILE_OFFSET_BITS=64\"" % get_cpu_count(), cwd="lz4-r53")
    run("make install PREFIX=/opt/dedupv1", cwd="lz4-r53")

    
@install()
def install_re2(options):
    print "Install re2"
    run("tar -xf re2-20130115.tgz")
    run("patch -p 0 < ../re2-Makefile.patch", cwd="re2")
    run("make", cwd="re2")
    run("make install", cwd="re2")
    
@install()
def install_icu(options):
    def copy_and_modify_astro_h():
        src = open("icu/source/i18n/astro.h")
        dest = open("icu/source/i18n/unicode/astro.h", "w")
        
        for line in src:
            if line.find("gregoimp.h") >= 0:
                continue
            dest.write(line)
    print "Install icu"
    run("tar -xf icu4c-4_8-src.tgz")
    
    run("./configure --prefix=/opt/dedupv1", cwd="icu/source")
    run("make", cwd="icu/source")
    
    copy_and_modify_astro_h()
    
    run("make install", cwd="icu/source")
    shutil.rmtree("/opt/dedupv1/include/layout")
    
@install()
def install_tbb(options):
    tbb_version = "tbb30_131oss"
    print "Install Intel Threading Building Blocks (TBB)"
    run("tar -xzf %s_src.tgz" % tbb_version)
    run("make -j%s tbb_build_prefix=tbb" % get_cpu_count(), cwd=tbb_version)
    run("cp build/tbb_release/libtbb* /opt/dedupv1/lib", cwd=tbb_version)
    run("cp -r include/tbb /opt/dedupv1/include", cwd=tbb_version)
        
@install()
def install_sparsehash(options):
    print "Install sparsehash"
    run("tar -xzf sparsehash-1.7.tar.gz")
    run("./configure --prefix=/opt/dedupv1", cwd="sparsehash-1.7")
    run("make -j%s" % get_cpu_count(), cwd="sparsehash-1.7")
    run("make install", cwd="sparsehash-1.7")
    
@install()
def install_gflags(options):
    print "Install gflags"
    run("tar -xzf gflags-1.4.tar.gz")
    run("./configure --prefix=/opt/dedupv1", cwd="gflags-1.4")
    run("patch -p1 < ../gflags_stripped_help.patch", cwd="gflags-1.4")
    run("make -j%s" % get_cpu_count(), cwd="gflags-1.4")
    run("make install", cwd="gflags-1.4")
    
@install()
def install_sqlite(options):
    print "Install sqlite"
    run("tar -xf sqlite-autoconf-3070602.tar.gz")
    run("./configure --prefix=/opt/dedupv1", cwd="sqlite-autoconf-3070602")
    run("make -j%s" % get_cpu_count(), cwd="sqlite-autoconf-3070602")
    run("make install", cwd="sqlite-autoconf-3070602")

@install()
def install_snappy(options):
    print "install snappy"
    run("tar -xf snappy-1.0.4.tar.gz")
    run("./configure --prefix=/opt/dedupv1", cwd="snappy-1.0.4", env = {"CXXFLAGS": "-O3 -DNDEBUG"})
    run("make -j%s" % get_cpu_count(), cwd="snappy-1.0.4")
    run("make check", cwd="snappy-1.0.4")
    run("make install", cwd="snappy-1.0.4")
    
@install("test")
def install_xmlreporting(options):
    print "Install xml reporting (py)"
    run("tar -xzf  dmeister-unittest-xml-reporting-1.0.3-9-gb6b1ce5.tar.gz")
    run("sudo python2.7 setup.py install", cwd="dmeister-unittest-xml-reporting-b6b1ce5")

@install()
def install_tcb_py(options):
  print "Install tokyo cabinet bindungs (py)"
  run("tar -xzf py-tcdb-0.3.tar.gz")
  run("patch -p 1 < ../py-tcdb.patch", cwd="py-tcdb-0.3")
  run("python2.7 setup.py install --prefix=/opt/dedupv1", cwd="py-tcdb-0.3")

@install()
def install_linenoise(options):
    print "Install linenoise"
    run("tar -xzf dmeister-linenoise-17b7547.tar.gz")
    run("scons install --prefix=/opt/dedupv1", cwd="dmeister-linenoise-17b7547")

@install()
def install_protobuf_json(options):
    print "Install protobuf-json (py)"
    run ("tar -xf protobuf-json.tar.gz")

    dir_name = "/opt/dedupv1/lib/python2.7/site-packages/protobuf-json.egg"
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name) 
    shutil.copy("protobuf-json/protobuf_json.py", dir_name)

@install()
def install_valgrind_prod(options):
    print "Install valgrind production header"
    run ("tar -xf valgrind-header.tar.gz")
    shutil.copy("valgrind-header/valgrind/valgrind.h", "/opt/dedupv1/include")

success = []
fails = []

def execute(c, options):
    try:
        if not options.fake:
            c.fn(options)
        else:
            print "Install %s" % c.name
        success.append(c.name)
    except Exception as e:
        print str(e)
        fails.append((c.name, e))

def do(name, options):
    try:
        c = get_install_component(name)
        execute(c, options)
    except Exception as e:
        print str(e)
        fails.append((name, e))        

class Component:
    def __init__(self, name, f_name, fn):
        self.name = name
        self.f_name = f_name
        self.fn = fn
        
    def modules(self):
        return self.fn.__dict__.get("modules", None)
    
    def option(self, key):
        return self.fn.__dict__.get("options", {}).get(key, None)

def get_install_component(name):
    try:
        mod = sys.modules[__name__]
        f_name = "install_" + name
        if f_name in dir(mod):
            fn = mod.__dict__[f_name]

            if "modules" not in fn.__dict__:
                raise Exception("Component not marked as installable: " + name)
            return Component(name, f_name, fn)
    except Exception as e:
        print "Invalid component %s: %s" % (name, str(e))
        raise
    raise Exception("No valid component: %s" % name)

def get_install_compontents():
    mod = sys.modules[__name__]
    l = []
    for f_name in dir(mod):
        if f_name.startswith("install_") and f_name != "install_all":
            name = f_name[len("install_"):]
            l.append(get_install_component(name))
    return l

def list_install_compontents(options):
    for c in get_install_compontents():
        print c.name

def check_extra_modules(c, extra):
    if len(c.modules()) == 0:
        return True
    for module in c.modules():
        if extra != None and module in extra:
            return True
    return False

def check_os(c):
    if not c.option("os"):
        return True
    os = c.option("os")
    if os == "linux" and not is_mac():
        return True
    if os == "mac" and is_mac():
        return True
    return False

def install_all(options, excludes, modules):
    for c in get_install_compontents():
        if not excludes or not c.name in excludes:
            if check_extra_modules(c, modules) and check_os(c):
                execute(c, options)
    
if __name__ == "__main__":
    def parse_excludes(option, opt, value, parser):
        current = getattr(parser.values, option.dest)
        if not current:
            current = []
        current.extend([v.strip() for v in value.split(',')])
        setattr(parser.values, option.dest, current)
        
    def parse_extra_modules(option, opt, value, parser):
        current = getattr(parser.values, option.dest)
        if not current:
            current = []
        current.extend([v.strip() for v in value.split(',')])
        setattr(parser.values, option.dest, current)
                
    usage = """usage: %prog
    
    Examples:
    %prog 
    %prog apr
    %prog --exclude apr
    %prog --exclude apr, apr_util
    %prog --exclude apr --exclude apr_util
    %prog list
"""
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("--scst_sbin_dir",
        dest="scst_sbin_dir")
    parser.add_option("--debug",
        action="store_true",
        dest="debug",
        default=False)
    parser.add_option("--fake",
            action="store_true",
            dest="fake",
            default=False)
    parser.add_option("-e", "--exclude",
                  type='string',
                  dest = "excludes",
                  action='callback',
                  callback=parse_excludes)
    parser.add_option("--module",
                  type='string',
                  dest = "modules",
                  action='callback',
                  callback=parse_extra_modules)
    (options, argv) = parser.parse_args()
    
    if not os.path.exists("/opt/dedupv1") or not os.path.isdir("/opt/dedupv1"):
        print "Create user-writable directory /opt/dedupv1"
        sys.exit(1)
    
    if len(argv) == 0 or ("all" in argv):
        install_all(options, options.excludes, options.modules)
    elif len(argv) == 1 and argv[0] == "list":
        list_install_compontents(options)
        sys.exit(0)
    else:
        if options.excludes and len(options.excludes):
            parser.print_help()
            sys.exit(1)
        if options.modules and len(options.modules):
            parser.print_help()
            sys.exit(1)
        components = set(argv)
        for component in components:
            do(component, options)
            
    print
    print "Successful installed libs:"
    if len(success):
        for l in success:
            print l
    else:
        print "None"
    print
    
    if len(fails):
        print "Failed to install libs:"
        for (name, e) in fails:
            print name, ":", str(e)
            print    
    
    sys.exit(len(fails))
    
