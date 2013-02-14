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

#include <base/base64.h>
#include <base/logging.h>
#include <base/strutil.h>

#include <apr-1/apr.h>
#include <apr-1/apu.h>
#include <apr-1/apr_errno.h>
#include <apr-1/apr_base64.h>

using std::string;
using dedupv1::base::strutil::ToHexString;

LOGGER("Base64");

namespace dedupv1 {
namespace base {

std::string ToBase64(const bytestring& data) {
    int rs = apr_base64_encode_len(data.size());
    char* b = new char[rs];
    apr_base64_encode_binary(b, data.data(), data.size());
    string r;
    r = b;
    delete[] b;
    TRACE(ToHexString(data.data(), data.size()) << "=>" << r);
    return r;
}

bytestring FromBase64(const std::string& data) {
    int rs = apr_base64_decode_len(data.c_str());
    bytestring r;
    r.resize(rs);
    int s = apr_base64_decode_binary(&(*r.begin()), data.c_str());
    r.resize(s); // TODO (dmeister) I currently do not now why this resize is necessary, but I remember
    // that is was necessary
    TRACE(data << "=>" << ToHexString(r.data(), r.size()));
    return r;
}

}
}

