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
 * @file strutil.h
 * Functions for advanched string handling
 */
#ifndef STRUTIL_H__
#define STRUTIL_H__

#include <base/base.h>
#include <base/option.h>
#include <string>
#include <vector>
#include <sstream>
#include <list>

namespace dedupv1 {
namespace base {

/**
 * \namespace dedupv1::base::strutil
 * Namespace for utility classes related to string handling
 */
namespace strutil {

/**
 * Converts a string to a 64-bit integer with recognizing
 * the scaling suffixes.
 *
 * The implementation handles:
 * - "k"/"K" as kilo (2^10)
 * - "m"/"M" as mega (2^20)
 * - "g"/"G" as giga (2^30)
 * - "t"/"T" as tera (2^40).
 *
 * @param s a string to convert
 * @return
 */
dedupv1::base::Option<int64_t> ToStorageUnit(const std::string& s);

/**
 * Converts a string to the given type using the C++ build-in
 * stream conversion.
 *
 * @param input
 * @return
 */
template< class T> dedupv1::base::Option<T> To(const std::string& input) {
        std::istringstream sstr(input);
        T value;
        sstr >> value;
        if (sstr.fail() || !sstr.eof()) {
            return false;
        }
        return dedupv1::base::make_option(value);
}

/**
 * Converts a string to a byte.
 * If the string is "true", true is returned, otherwise
 * false.
 *
 * @param input
 * @return
 */
template<> dedupv1::base::Option<byte> To(const std::string& input);
template<> dedupv1::base::Option<int8_t> To(const std::string& input);

/**
 * Converts a string into a bool value.
 * Currently only the string "true" is seen as true value.
 *
 */
template<> dedupv1::base::Option<bool> To(const std::string& input);

/**
 * Converts the template value i to a string via the
 * C++ stream conversion.
 *
 * @param i
 * @return
 */
template<class T> std::string ToString(T i) {
        std::stringstream ss;
        ss << i;
        return ss.str();
}

std::string ToStringAsFixedDecimal(double d, int precision = 3);

/**
 * Converts a byte to a string in a human-readable
 * form.
 * @param b
 * @return
 */
template<> std::string ToString(bool b);

/**
 * print a array as series of hex values.
 * @param data
 * @param data_size
 * @return
 */
std::string ToHexString(const void* data, size_t data_size);

/**
 * prints a variable as hex value.
 * @param i
 * @return
 */
template<class T> std::string ToHexString(T i) {
        std::stringstream ss;
        ss << std::hex << i;
        return ss.str();
}

template<byte> std::string ToHexString(byte b);

/**
         * @return true iff ok, otherwise an error has occurred
 */
bool FromHexString(const std::string& s, bytestring* bs);

/**
 * Parse a hex value from a string.
 * @param s
 * @return
 */
template<class T> T FromHexString(std::string s) {
        T i;
        std::stringstream ss;
        ss << std::hex << s;
        ss >> i;
        return i;
}

/**
 * Format a large number.
 * In contrast to the ToString method, this
 * method inserts "." for 1000s.
 * @param i
 * @return
 */
std::string FormatLargeNumber(uint64_t i);

/**
 * Format a storage unit.
 * The data is used as floating point value with an appended
 * "K","M","G", "T" in an 2^x way.
 *
 * @param n
 * @return
 */
std::string FormatStorageUnit (int64_t n);

/**
 * returns the index at which a given pattern starts inside
 * a string
 *
 * @param str
 * @param pattern
 * @return
 */
dedupv1::base::Option<size_t> Index(const std::string& str, const std::string& pattern);

/**
 * Checks if the input string starts with the pattern string.
 *
 * @param input
 * @param pattern
 * @return
 */
bool StartsWith(const std::string& input, const std::string& pattern);

/**
 * Checks if the input string ends with the given pattern string.
 *
 * @param input
 * @param pattern
 * @return
 */
bool EndsWith(const std::string& input, const std::string& pattern);

/**
 * Checks if the search pattern is contained in the text string.
 *
 * @param text
 * @param search
 * @return
 */
bool Contains(const std::string& text, const std::string& search);

/**
 * Checks if the string consists only of numbers.
 * @param text
 * @return
 */
bool IsNumeric(const std::string& text);

/**
 * Trim the string.
 * @param s
 * @return
 */
std::string Trim(const std::string& s);

/**
 * Checks if all characters of the string s are printable.
 * Our definition of printable includes in contrast, e.g. to the ASCSI
 * regular expression definition the tab character.
 *
 * @param s
 * @return
 */
bool IsPrintable(const std::string& s);

/**
 * Split the string at the given delimiter.
 *
 * @param input
 * @param delimiter
 * @param results
 * @param includeEmpties
 * @return
 */
bool Split(const std::string& input,
        const std::string& delimiter, std::vector<std::string>* results,
        bool includeEmpties = true);

/**
 * Splits the string at the given delimiter.
 * This method only works if the input is splitted into
 * at most two parts.
 *
 * @param input
 * @param delimiter
 * @param string1
 * @param string2
 * @return
 */
bool Split(const std::string& input,
        const std::string& delimiter, std::string* string1, std::string* string2);

/**
 * Joins a string.
 * @param start iterator start
 * @param end iterator end
 * @param delimiter
 * @return
 */
template<class T> std::string Join(T start, T end, const std::string& delimiter)  {
        std::string s;
        T i;
        for (i = start; i != end; i++) {
            if (i != start) {
                s += delimiter;
            }
            s.append(ToString(*i));
        }
        return s;
}

/**
 * @pattern regular expression pattern
 */
dedupv1::base::Option<std::string> ReplaceAll(const std::string& input, const std::string& pattern, const std::string replacement);

/**
 * Similar to std::string substr but without throwing an exception when pos and npos are larger than the string
 */
std::string FriendlySubstr(const std::string& str, size_t pos = 0, size_t n = std::string::npos, const std::string& cut_suffix = "");

}
}
}

#endif  // STRUTIL_H__
