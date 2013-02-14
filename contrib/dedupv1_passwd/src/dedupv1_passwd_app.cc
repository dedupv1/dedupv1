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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>

#include <iostream>
#include <string>
#include <vector>

#include <base/strutil.h>
#include <base/base64.h>
#include <base/rot13.h>
#include <gflags/gflags.h>

using std::cout;
using std::cin;
using std::cerr;
using std::getline;
using std::string;
using std::vector;
using dedupv1::base::ToRot13;
using dedupv1::base::ToBase64;
using dedupv1::base::make_bytestring;

int main(int argc, char * argv[]) {
    google::SetUsageMessage("password1, password2");
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc == 1) {
        fprintf(stderr, "No arguments\n");
        return 32;
    }
    int rc = 0;
    for(int i = 1; i < argc; i++) {
        string raw_password = argv[i];
        if (raw_password.size() > 256) {
            cerr << raw_password << ": Password too long" << std::endl;
            rc++;
        } else if (raw_password.size() < 12) {
            cerr << raw_password << ": Password not long enough" << std::endl;
            rc++;
        } else {
            string secret = ToBase64(ToRot13(make_bytestring(reinterpret_cast<const byte*>(raw_password.data()), raw_password.size() + 1)));
            cout << secret << std::endl;
        }
    }
    cout << std::endl;
    return rc;
}


