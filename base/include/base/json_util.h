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
 * @file json_util.h
 * Utility functions about JSON ("Javascript Object Notation")
 *
 * @see http://www.json.org
 */

#ifndef JSON_UTIL_H_
#define JSON_UTIL_H_

#include <base/base.h>
#include <base/option.h>

#include <json/json.h>

namespace dedupv1 {
namespace base {

/**
 * Parses a string and returns it as JSON value if the string forms a valid
 * JSON string.
 */
Option<Json::Value> AsJson(const std::string& s);

}
}


#endif /* JSON_UTIL_H_ */
