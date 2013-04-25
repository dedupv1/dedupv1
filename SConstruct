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
from os.path import join
import os
from scons_check import run, CheckModprobe, CheckExecutable, SconsVersion, Config
from scons_check import Execute, IsMac, GenerateCheckLibraryVersionFunction
import platform
import multiprocessing
import re
import sys

# This Regular Expression is usred to check if params do not contain special characters
printableRegEx = re.compile(r"^[\w !\$%&/\(\)\[\]\{\}=\?\*\+\-#<>-_\.,:;]*$")

EnsurePythonVersion(2,6)

SetOption('implicit_cache', 1)
SetOption('num_jobs', multiprocessing.cpu_count())

config = Config(Split("_GNU_SOURCE POSIX_PTHREAD_SEMANTICS"))
config.append("_XOPEN_SOURCE 600") # for Single UNIX Specification, Version 3
# kyoto carbinet needs the C-Konstant INT32_MAX. Using C++ it is only available 
# when __STDC_LIMIT_MACROS is declared.
cflags = Split("-fPIC -D__STDC_LIMIT_MACROS")

debug_cflags = ['-ggdb', '-g']
# In release we also include the DebugStrings, but we will separate them later
release_cflags = ['-O3', '-DNDEBUG', '-DNVALGRIND', '-g'] 
ldflags = ["-Wl,-rpath,/opt/dedupv1/lib"]

os_libs = ["m", "pthread"]
libs = ["microhttpd", "curl", "apr-1", "aprutil-1", 
  "protobuf", "tokyocabinet", "z", "bz2", "json", 
  "cryptopp", "tbb", "sqlite3", "gflags", "snappy",
  "lz4", "leveldb", "crcutil"]
build_test = True
build_dedupv1d = True
build_contrib = True
run_test = False
release_mode = False
build_mode = None
support_tcmalloc = True # Use Google's TCMalloc
support_tbbmalloc = True # but tcmalloc has higher priority
support_deadlock_check = True
use_ccache = False
default_dedupv1_config = None

AddOption('--prefix',
  dest='prefix',
  type='string',
  nargs=1,
  action='store',
  metavar='DIR',
  help='installation prefix', default="/opt/dedupv1")

# Uses the syslog logging facility instead of log4cxx. Release versions should 
# usually use syslog because log4cxx is known to crash in some situations.
AddOption("--force_syslog", dest="force_syslog", action="store_true", default=False)
AddOption("--console_for_ndaemon", dest="console_for_ndaemon", action="store_true", default=False)

# Avoids compiling unit tests.
# TODO (dmeister): What is the difference between scons with default targets and 
# --no-tests
AddOption("--no-test", dest="no-test", action="store_true", default=False)

# The dedupv1 build system tries to use distributed builds using distcc when available. 
# If distcc is not available (distcc is not installed or no remote distcc servers could be found), 
# a local compiler is used. If this flag is set, distcc is not used even when it is available.
AddOption("--no-distcc", dest="no_distcc", action="store_true", default=False)

# The dedupv1 build system tries to use ccache to avoid redundant compiles. 
# If the flag is set, the use of ccache is avoided even when ccache is available
AddOption("--no-ccache", dest="no_ccache", action="store_true", default=False)

# Compiles die target in release mode. The release mode used gcc optimizations,
# removes some debugging-only
# code, and removes all TRACE logging.
AddOption("--release", dest="release_mode", action="store_true", default=False)

# Include DCHECK statements in binary. Default in non-release builds, but with flags also includes
# the statements in release builds
AddOption("--with-dcheck", dest="with_dcheck", action="store_true", default=False)
AddOption("--didedupv1", dest="didedupv1_mode", action="store_true", default=False)
AddOption("--perftools-profile", dest="perftools_profile", action="store_true", default=False)
AddOption("--gnu-profile", dest="gnu_profile_mode", action="store_true", default=False)
AddOption("--coverage", dest="coverage", action="store_true", default=False)
AddOption("--fault-inject", dest="fault_inject", action="store_true", default=False)
AddOption("--no-tc-malloc", dest="no_tc_malloc", action="store_true", default=False)
AddOption("--no-tbb-malloc", dest="no_tbb_malloc", action="store_true", default=False)
AddOption("--deadlock-avoidance-timeout", dest="deadlock_avoidance_timeout", type="int", default=None)
AddOption("--no-starter-install", dest="no_starter_install", action="store_true", default=False)

AddOption("--default_config", dest="default_dedupv1_config", default=None)
AddOption("--default_monitor_port", dest="default_monitor_port", type="int", default=9001)

AddOption("--force_scst", dest="force_scst", action="store_true", default=False)

# Use clang compiler
AddOption("--clang", dest="clang", action="store_true", default=False)
# Use icc compiler
AddOption("--icc", dest="icc", action="store_true", default=False)

if GetOption("no-test"):
  build_test = False

if GetOption("no_tc_malloc"):
  support_tcmalloc = False
if GetOption("no_tbb_malloc"):
  support_tbbmalloc = False

# release mode
if GetOption("release_mode"):
  release_mode = True
  build_mode = "release"
  cflags.extend(release_cflags)
else:
  build_mode = "debug"
  cflags.extend(debug_cflags)

# logging mode
if GetOption("force_syslog"):
  config.append("LOGGING_SYSLOG")
  print "Tests are not supported with syslog logging"
  build_test = False
else:
  config.append("LOGGING_LOG4CXX")
  libs.append("log4cxx")
if GetOption("console_for_ndaemon"):
  config.append("LOGGING_CONSOLE_FOR_NDAEMON")

# profile mode
if GetOption("perftools_profile") == True:
  libs.append("profiler")
  config.append("PERFTOOLS_PROFILE")
if GetOption("gnu_profile_mode"):
  cflags.extend(Split("-g -pg"))
  ldflags.append("-pg")

# di-dedupv1 mode
if GetOption("didedupv1_mode"):
  config.append("DI_DEDUPV1")

if GetOption('default_dedupv1_config'):
  default_dedupv1_config = GetOption("default_dedupv1_config")

# fault inject mode
if GetOption("fault_inject"):
  if GetOption("release_mode"):
    print "Provide only one of --release (x)or --fault-inject"
    Exit(1)
  config.append("FAULT_INJECTION")

if (GetOption("with_dcheck") or build_mode == "debug") and not GetOption("coverage"):
  config.append("WITH_DCHECK")

config.append("DEDUPV1_ROOT \"%s\"" % GetOption('prefix'))
config.append("DEDUPV1_DEFAULT_MONITOR_PORT %s" % GetOption('default_monitor_port'))

if not default_dedupv1_config:
  default_dedupv1_config = os.path.normpath(os.path.join(GetOption('prefix'),
    "etc/dedupv1/dedupv1.conf"))
config.append("DEDUPV1_DEFAULT_CONFIG \"%s\"" % default_dedupv1_config)

print "Checking for iSCSI-SCST tools...",
for p in ["/usr/sbin", "/usr/local/sbin"]:
  if os.path.exists(os.path.join(p, "iscsi-scst-adm")):
    config.append("ISCST_SCST_TOOLS_PATH \"%s\"" % p)
    print p
    break
else:
    print "not found"

# Version handling
version_str = "<not set>"
if os.path.exists("version-date.txt"):
  version_str = open("version-date.txt").read().strip()
if printableRegEx.match(version_str) == None:
  print "Could not build dedupv1: version-date.txt contains special charaters."
  sys.exit(1)
config.append("DEDUPV1_VERSION_STR \"%s\"" % version_str)

revision_str = run("git rev-parse HEAD")
if not revision_str or revision_str.find("fatal:") == 0:
  revision_str = "<not set>"
else:
  revision_str = revision_str.strip()
config.append("DEDUPV1_REVISION_STR \"%s\"" % revision_str)

revision_date_str = "<not set>"
try:
  git_log = run("git log -1 %s --date=short" % revision_str)
  for line in git_log.split("\n"):
    if line.startswith("Date:"):
      revision_date_str = line.split(" ")[-1]
except:
  pass
config.append("DEDUPV1_REVISION_DATE_STR \"%s\"" % revision_date_str)

env = Environment(PREFIX = GetOption('prefix'), ENV = os.environ, tools = ["default"]) 

env["CPPFLAGS"] = cflags[:]
env["CPPPATH"] = ["/usr/local/include/scst", join(env["PREFIX"], "include")]
env["LIBPATH"] = [join(env["PREFIX"], "lib")]
env["LINKFLAGS"] = ldflags[:]
env.SConsignFile()

# Generate library version check function
CheckMHDVersion = GenerateCheckLibraryVersionFunction("microhttpd",
  "microhttpd.h",
  "MHD_get_version")

conf = Configure(env, custom_tests = {
  "CheckExecutable": CheckExecutable,
  "CheckModprobe": CheckModprobe,
  "CheckMHDVersion": CheckMHDVersion})
if not conf.CheckExecutable("python2.7"):
  print "Python 2.7 must be installed"
  Exit(1)
if GetOption("clang") and GetOption("icc"):
  print "Do not provide --clang and --icc. Pick one compiler"
  Exit(1)

if GetOption("clang"):
  env["RAW_CXX"] = "clang++"
  env["RAW_CC"] = "clang"
elif GetOption("icc"):
  env["RAW_CXX"] = "icc"
  env["RAW_CC"] = "icc"
else:
  env["RAW_CXX"] = "g++"
  env["RAW_CC"] = "gcc"

if not conf.CheckExecutable(env["RAW_CXX"]):
  print "C++ compiler %s is not available" % env["RAW_CXX"]
  Exit(1)
if not conf.CheckExecutable(env["RAW_CC"]):
  print "C compiler %s is not available" % env["RAW_CC"]
  Exit(1)

env["CXX"] = env["RAW_CXX"]
env["CC"] = env["RAW_CC"]
if conf.CheckExecutable("ccache") and not GetOption("no_ccache"):
  env["CXX"] = "ccache %s" % env.get("RAW_CXX")
  env["CC"] = "ccache %s" % env.get("RAW_CC")
  use_ccache = True
if build_mode != "release":
  # distcc is not supported in release mode since gcc/clang is forced to do native tuning
  if conf.CheckExecutable("distcc") and not GetOption("no_distcc"):
    distcc_hosts = filter(lambda line: len(line) > 0, 
      Execute("distcc --show-hosts", redirect_stderr=False).split("\n"))
    if len(distcc_hosts) >= 1:
      if use_ccache:
        env["CXX"] = "ccache distcc %s" % env.get("RAW_CXX")
        env["CC"] = "ccache distcc %s" % env.get("RAW_CC");SetOption('num_jobs', 
          2 * int(Execute("distcc -j")))
      else:
        env["CXX"] = "distcc %s" % env.get("RAW_CXX")
        env["CC"] = "distcc %s" % env.get("RAW_CC");SetOption('num_jobs', 
          2 * int(Execute("distcc -j")))

if not conf.CheckModprobe("scst"):
  if GetOption("force_scst"):
    print "SCST must be installed"
    Exit(1)
  else:
    print "SCST should be installed. Skipping daemon, contrib, dedupv1d_test only partially supported"
    build_dedupv1d = False
    config.append("NO_SCST")

for lib in os_libs:
  if not conf.CheckLib(lib):
    print "%s library must be installed" % lib
    Exit(1)
for lib in libs:
  if not conf.CheckLib(lib):
    print "%s library must be installed" % lib
    Exit(1)
if not conf.CheckHeader("valgrind.h"):
  print "Valgrind production header valgrind.h not available"
  config.append("NVALGRIND")
if not conf.CheckHeader("sys/sysinfo.h"):
  config.append("NO_SYS_SYSINFO_H")
if not conf.CheckHeader("gtest/gtest_prod.h"):
  print "gtest Production header gtest_prod.h not available. Install via install-libs.py gtest_prod"
  Exit(1)
if conf.CheckHeader("endian.h"):
  config.add_include("endian.h")
elif conf.CheckHeader("sys/endian.h"):
  config.add_include("sys/endian.h")
elif conf.CheckHeader("machinfo/endian.h"):
  config.add_include("machinfo/endian.h")
else:
  print "endian.h not found: Use own endian function"
  config.append("USE_OWN_ENDIAN")
if not conf.CheckHeader("pthread.h"):
  Exit(1)
if conf.CheckFunc("fallocate"):
  config.append("HAS_FALLOCATE");
if conf.CheckFunc("pthread_setname_np"):
  config.append("HAS_PTHREAD_SETNAME_NP");
if conf.CheckHeader("sys/prctl.h") and conf.CheckFunc("prctl"):
  config.append("HAS_PRCTL")
if not conf.CheckMHDVersion("0.9.15"):
  print "Update libmicrohttpd to at least 0.9.15"
  Exit(1)
if not conf.CheckFunc("tcmdbputproc") or not conf.CheckFunc("tcmdbcas"):
  print "Update tokyo cabinet"
  Exit(1)
if not conf.CheckFunc("pthread_condattr_setclock"):
  config.append("NO_PTHREAD_CONDATTR_SETCLOCK")
  if build_mode == "release":
    print "pthread_condattr_setclock is necessary in release mode"
    Exit(1)
if not conf.CheckHeader("time.h"):
  Exit(1)

# malloc mode
if support_tcmalloc:
  if conf.CheckLib("tcmalloc"):
    support_tbbmalloc = False
if support_tbbmalloc:
  if not conf.CheckLib("tbbmalloc"):
    print "No multithreaded malloc library available. Application performance will be reduced."

if build_test and not (conf.CheckLib("gtest") and conf.CheckLib("gmock") and conf.CheckCXXHeader("gtest/gtest.h")):
  print "gtest library not found. Skipping unit tests"
  build_test = False
if not conf.CheckCXXHeader("tbb/concurrent_unordered_map.h"):
  print "Update version of tbb to at least 3.0"
  Exit(1)
# re2 library. For strange reasons this switch between OS X and Linux is necessary
if IsMac():
  env["LINKFLAGS"].append(join(env["PREFIX"], "lib/libre2.a"))
  config.append("_DARWIN_C_SOURCE")
else:
  if not conf.CheckLib("re2"):
    print "re2 not installed."
    Exit(1)
if not conf.CheckLib("crcutil"):
    print "crcutil is not installed. Application performance might be reduced."
    config.append("CRYPTO_CRC")
if GetOption("coverage"):
  # do not do this before the lib checks
  if not conf.CheckExecutable("lcov"):
    print "lcov not found. Coverage not possible"
    Exit(1)
  if not conf.CheckLib("gcov"):
    print "gcov not found. Coverage not possible"
    Exit(1)
  if build_mode == "release":
    print "coverage is not supported in release mode"
    Exit(1)
  env["CPPFLAGS"].extend(Split("-fprofile-arcs -ftest-coverage --coverage"))
  env["LINKFLAGS"].extend(Split("--coverage"))
  build_mode = build_mode + "-coverage"
  # e.g. DCHECK are removed in coverage mode.
  # This is similar to the ALWAYS, NEVER macros
  # sqlite: http://www.sqlite.org/testing.html
  config.append("COVERAGE_MODE")
  # We deactivate the deadlock check in coverage mode
  # to remove these lines from the reporting
  support_deadlock_check = False

print "Checking for machine architecture...", platform.machine()
if platform.machine() == 'i686' or platform.machine() == "x86_64":
  env["CPPFLAGS"].append("-m64")
  if release_mode:
    env["CPPFLAGS"].append("-mtune=native")

print "Checking for OS...", platform.uname()[0]

if support_deadlock_check:
  if IsMac():
    print "Deadlock check not supported on Mac"
  elif build_mode == "release":
    print "Deadlock check is not supported in release mode"
  else:
    timeout = GetOption("deadlock_avoidance_timeout")
    if timeout != None:
      # Option is set
      if timeout == 0:
        # No timeout
        pass
      else:
        config.extend(["DEADLOCK_CHECK", "DEADLOCK_CHECK_TIMEOUT=%s" % (timeout)])
    else:
      config.append("DEADLOCK_CHECK")
else:
  print "Found 32-bit machine. Skipping daemon"
  build_dedupv1d = False


env["CFLAGS"].extend(Split("""-Wall
  -Wmissing-prototypes -Wcast-align -Wpointer-arith -Wreturn-type
  -Wformat -Wformat-security -Wnested-externs
  -Wwrite-strings -Wstrict-prototypes"""))
env["CPPFLAGS"].extend(Split("""-Wall
  -Wcast-align -Wpointer-arith -Wreturn-type
  -Wformat -Wformat-security -Wwrite-strings -Wno-sign-compare"""))

env = conf.Finish()

Export("env", "build_mode")

test_targets = []
all_targets = []
contrib_targets = []

config.BuildConfigHeader(["base/include/base/config.h"], 
    ["base/include/base/config.h.in"])
config.BuildConfigModule(["dedupv1_util/src/lib/config.py"], 
    ["dedupv1_util/src/lib/config.py.in"])

# Build libraries
dedupv1_base_lib = SConscript("base/SConscript",
  variant_dir = join("build", build_mode, "dedupv1_base_lib"))
env.Alias("libdedupv1_base", dedupv1_base_lib)
env["LIBPATH"].append(join("#build", build_mode, "dedupv1_base_lib"))

dedupv1_test_util_lib = SConscript("test_util/SConscript",
    variant_dir = join("build", build_mode, "dedupv1_test_util_lib"))
env["LIBPATH"].append(join("#build", build_mode, "dedupv1_test_util_lib"))
env.Alias("libdedupv1_test_util", dedupv1_test_util_lib)

dedupv1_core_lib = SConscript("core/SConscript",
  variant_dir = join("build", build_mode, "dedupv1_core_lib"))
env["LIBPATH"].append(join("#build", build_mode, "dedupv1_core_lib"))
env.Alias("libdedupv1_core", dedupv1_core_lib)
env.Depends(dedupv1_core_lib, dedupv1_base_lib)

dedupv1d_lib = SConscript("dedupv1d/SConscript",
  variant_dir = join("build", build_mode, "dedupv1d_lib"))
env["LIBPATH"].append(join("#build", build_mode, "dedupv1d_lib"))
env.Alias("libdedupv1d", dedupv1d_lib)
env.Depends(dedupv1d_lib, dedupv1_base_lib)
env.Depends(dedupv1d_lib, dedupv1_core_lib)

if build_test:
  dedupv1_base_test_prog = SConscript("base/SConscript_Test",
    variant_dir = join("build", build_mode, "dedupv1_base_test"))
  env.Depends(dedupv1_base_test_prog, dedupv1_test_util_lib)
  test_targets.append(dedupv1_base_test_prog)
  all_targets.append(dedupv1_base_test_prog)

  dedupv1_core_test_prog = SConscript("core/SConscript_Test",
    variant_dir = join("build", build_mode, "dedupv1_core_test"))
  env.Depends(dedupv1_core_test_prog, dedupv1_base_lib)
  env.Depends(dedupv1_core_test_prog, dedupv1_test_util_lib)
  test_targets.append(dedupv1_core_test_prog)
  all_targets.append(dedupv1_core_test_prog)

  dedupv1d_test_prog = SConscript("dedupv1d/SConscript_Test",
    variant_dir = join("build", build_mode, "dedupv1d_test"))
  env.Depends(dedupv1d_test_prog, dedupv1_base_lib)
  env.Depends(dedupv1d_test_prog, dedupv1_core_lib)
  env.Depends(dedupv1d_test_prog, dedupv1_test_util_lib)
  all_targets.append(dedupv1d_test_prog)
  test_targets.append(dedupv1d_test_prog)

  dedupv1_contrib_test_prog = SConscript("contrib/SConscript_Test",
    variant_dir = join("build", build_mode, "dedupv1_contrib_test"))
  env.Depends(dedupv1_contrib_test_prog, dedupv1_base_lib)
  env.Depends(dedupv1_contrib_test_prog, dedupv1_core_lib)
  env.Depends(dedupv1_contrib_test_prog, dedupv1d_lib)
  env.Depends(dedupv1_contrib_test_prog, dedupv1_test_util_lib)
  test_targets.append(dedupv1_contrib_test_prog)
  all_targets.append(dedupv1_contrib_test_prog)

if build_dedupv1d:
  dedupv1d_prog = SConscript("dedupv1d/SConscript_Daemon", 
    variant_dir = join("build", build_mode, "dedupv1d"))
  env.Depends(dedupv1d_prog, dedupv1_base_lib)
  env.Depends(dedupv1d_prog, dedupv1_core_lib)
  env.Depends(dedupv1d_prog, dedupv1d_lib)
  env.Alias("dedupv1d", dedupv1d_prog)
  all_targets.append(dedupv1d_prog)
  env.Install("$PREFIX/bin", dedupv1d_prog)

  env.Install("$PREFIX/bin", "dedupv1_util/src/dedupv1_adm")
  env.Install("$PREFIX/bin", "dedupv1_util/src/dedupv1_support")
  env.Install("$PREFIX/lib/python", Glob("dedupv1_util/src/lib/*"))
  env.Install("$PREFIX/lib/python", Glob("base/resources/*.py"))
  env.Install("$PREFIX/lib/python", Glob("core/resources/*.py"))
  env.Install("$PREFIX/lib/python", Glob("dedupv1d/resources/*.py"))
  env.Install("$PREFIX/lib/", dedupv1_base_lib)
  env.Install("$PREFIX/lib", dedupv1_core_lib)
  env.Install("$PREFIX/lib", dedupv1d_lib)
  env.Install("$PREFIX/bin", "dedupv1_util/src/dedupv1_mon")
  env.Install("$PREFIX/bin", "dedupv1_util/src/dedupv1_volumes")
  env.Install("$PREFIX/bin", "dedupv1_util/src/dedupv1_targets")
  env.Install("$PREFIX/bin", "dedupv1_util/src/dedupv1_groups")
  env.Install("$PREFIX/bin", "dedupv1_util/src/dedupv1_users")
  env.Install("$PREFIX/bin", "dedupv1_util/src/dedupv1_report")

  env.Install("$PREFIX/etc/dedupv1", Glob("conf/*"))

  env.Command("$PREFIX/var/log/dedupv1", [], Mkdir("$TARGET"))
  env.Command("$PREFIX/var/lock/", [], Mkdir("$TARGET"))
  env.Command("$PREFIX/var/lib/dedupv1", [], Mkdir("$TARGET"))
  env.Command("$PREFIX/var/lib/dedupv1/block-index", [], Mkdir("$TARGET"))
  env.Command("$PREFIX/var/lib/dedupv1/chunk-index", [], Mkdir("$TARGET"))
  env.Command("$PREFIX/var/lib/dedupv1/container", [], Mkdir("$TARGET"))
  env.Command("$PREFIX/var/crash/dedupv1", [], Mkdir("$TARGET"))
  env.Command("$PREFIX/var/crash/dedupv1", [], Mkdir("$TARGET"))
  env.Command("$PREFIX/var/profile/dedupv1", [], Mkdir("$TARGET"))

  env.Alias("install", "$PREFIX")

if build_contrib:
  # We need a special handling for dedupv1_starter
  dedupv1_starter_prog = SConscript("contrib/dedupv1_starter/SConscript", 
    variant_dir = join("build", build_mode, "dedupv1_starter"))
  env.Alias("dedupv1_starter", dedupv1_starter_prog)
  contrib_targets.append(dedupv1_starter_prog)
  all_targets.append(dedupv1_starter_prog)
  if not GetOption("no_starter_install"):
    env.Install("$PREFIX/bin", dedupv1_starter_prog)

  # Add all other contrib tools
  contribs = ["dedupv1_replay",
    "dedupv1_debug",
    "dedupv1_read",
    "dedupv1_chunk_restorer",
    "dedupv1_check",
    "dedupv1_passwd",
    "dedupv1_dump"]

  for contrib_prog_name in contribs:
    dedupv1_contrib_prog = SConscript("contrib/%s/SConscript" % contrib_prog_name,
      variant_dir = join("build", build_mode, contrib_prog_name))
    env.Depends(dedupv1_contrib_prog, dedupv1_base_lib)
    env.Depends(dedupv1_contrib_prog, dedupv1_core_lib)
    env.Depends(dedupv1_contrib_prog, dedupv1d_lib)
    env.Alias(contrib_prog_name, dedupv1_contrib_prog)
    all_targets.append(dedupv1_contrib_prog)
    contrib_targets.append(dedupv1_contrib_prog)
    env.Install("$PREFIX/bin", dedupv1_contrib_prog)

env.Alias("all", all_targets)
env.Alias("tests", test_targets)
env.Alias("contrib", contrib_targets)

if not build_dedupv1d and not build_test:
  print "No valid target given"
  Exit(1)

