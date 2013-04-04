/*
 * dedupv1 - iSCSI based Deduplication System for Linux
 *
 * (C) 2008 Dirk Meister
 * (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
 * (C) 2012 Dirk Meister, Johannes Gutenberg University Mainz
 *
 * This file is part of dedupv1.
 *
 * dedupv1 is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * dedupv1 is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with dedupv1. If not, see http://www.gnu.org/licenses/.
 */

/**
 * This source file is used to print out a stack-trace when your program
 * segfaults. It is relatively reliable and spot-on accurate.
 *
 * This code is in the public domain. Use it as you see fit, some credit
 * would be appreciated, but is not a prerequisite for usage. Feedback
 * on it's use would encourage further development and maintenance.
 *
 * Due to a bug in gcc-4.x.x you currently have to compile as C++ if you want
 * demangling to work.
 *
 * Please note that it's been ported into my ULS library, thus the check for
 * HAS_ULSLIB and the use of the sigsegv_outp macro based on that define.
 *
 * Author: Jaco Kroon <jaco@kroon.co.za>
 *
 * Copyright (C) 2005 - 2010 Jaco Kroon
 *
 * Source: http://tlug.up.ac.za/wiki/index.php/Obtaining_a_stack_trace_in_C_upon_SIGSEGV#How_to_use_this.3F
 */

// only on Linux
#ifndef __APPLE__

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/* Bug in gcc prevents from using CPP_DEMANGLE in pure "C" */
#if !defined(__cplusplus) && !defined(NO_CPP_DEMANGLE)
#define NO_CPP_DEMANGLE
#endif

#include <base/stacktrace.h>
#include <base/logging.h>

#include <memory.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <ucontext.h>
#include <dlfcn.h>
#ifndef NO_CPP_DEMANGLE
#include <cxxabi.h>
#ifdef __cplusplus
using __cxxabiv1::__cxa_demangle;
#endif
#endif
#if defined(REG_RIP)
# define SIGSEGV_STACK_IA64
# define REGFORMAT "%016lx"
#elif defined(REG_EIP)
# define SIGSEGV_STACK_X86
# define REGFORMAT "%08x"
#else
# define SIGSEGV_STACK_GENERIC
# define REGFORMAT "%x"
#endif

LOGGER("Stacktrace");

namespace dedupv1 {
namespace base {
namespace internal {

static void signal_segv(int signum, siginfo_t* info, void*ptr) {
    static const char *si_codes[3] = {"", "SEGV_MAPERR", "SEGV_ACCERR"};

    int i, f = 0;
    ucontext_t *ucontext = (ucontext_t *) ptr;
    Dl_info dlinfo;
    void **bp = 0;
    void *ip = 0;

    ERROR("Segmentation Fault!");
    ERROR("info.si_signo = " << signum);
    ERROR("info.si_errno = " << info->si_errno);
    ERROR("info.si_code  = " << info->si_code << " " << si_codes[info->si_code]);
    ERROR("info.si_addr  = " << info->si_addr);
    for (i = 0; i < NGREG; i++) {
        ERROR("reg[" << i << "]       = " << ucontext->uc_mcontext.gregs[i]);
    }

#ifndef SIGSEGV_NOSTACK
#if defined(SIGSEGV_STACK_IA64) || defined(SIGSEGV_STACK_X86)
#if defined(SIGSEGV_STACK_IA64)
    ip = (void *) ucontext->uc_mcontext.gregs[REG_RIP];
    bp = (void * *) ucontext->uc_mcontext.gregs[REG_RBP];
#elif defined(SIGSEGV_STACK_X86)
    ip = (void *) ucontext->uc_mcontext.gregs[REG_EIP];
    bp = (void * *) ucontext->uc_mcontext.gregs[REG_EBP];
#endif

    ERROR("Stack trace:");
    while (bp && ip) {
        if (!dladdr(ip, &dlinfo)) {
            break;
        }

        const char *symname = dlinfo.dli_sname;

#ifndef NO_CPP_DEMANGLE
        int status;
        char * tmp = __cxa_demangle(symname, NULL, 0, &status);

        if (status == 0 && tmp) {
            symname = tmp;
        }
#endif

        ERROR("" << ++f << ": " << ip << symname << "+" << (unsigned long) ip - (unsigned long) dlinfo.dli_saddr << "(" << dlinfo.dli_fname << ")");

#ifndef NO_CPP_DEMANGLE
        if (tmp) {
            free(tmp);
        }
#endif

        if (dlinfo.dli_sname && !strcmp(dlinfo.dli_sname, "main")) {
            break;
        }

        ip = bp[1];
        bp = (void * *) bp[0];
    }
#else
    ERROR("Stack trace (non-dedicated):");
    sz = backtrace(bt, 20);
    strings = backtrace_symbols(bt, sz);
    for (i = 0; i < sz; ++i) {
        ERROR("" << strings[i]);
    }
#endif
    ERROR("End of stack trace.");
#else
    ERROR("Not printing stack strace.");
#endif
    abort();
}
}

bool setup_sigsegv() {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = dedupv1::base::internal::signal_segv;
    action.sa_flags = SA_SIGINFO;
    CHECK(sigaction(SIGSEGV, &action, NULL) >= 0, "Failed to register segfault handler");
    return true;
}

}
}

#endif
