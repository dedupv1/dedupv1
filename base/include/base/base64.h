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
#ifndef BASE64_H_
#define BASE64_H_

#include <string>
#include <base/base.h>

namespace dedupv1 {
namespace base {

/**
 * Encodes a byte string via Base64.
 *
 * Our base64 uses the base64 implementation from APR (Apache Portable Runtime)
 *
 * @param data
 * @return
 */
std::string ToBase64(const bytestring& data);

/**
 * Decodes a base64 string into a normal string.
 * TODO (dmeister) Can this function fail?
 *
 * Our base64 uses the base64 implementation from APR (Apache Portable Runtime)
 *
 * @param data
 * @return
 */
bytestring FromBase64(const std::string& data);

}
}

#endif /* BASE64_H_ */
