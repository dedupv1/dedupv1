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
#include <base/strutil.h>
#include <base/logging.h>

#include <re2/re2.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include <sstream>

using std::string;
using std::stringstream;
using std::vector;
using dedupv1::base::Option;
using dedupv1::base::make_option;

LOGGER("Strutil");

namespace dedupv1 {
namespace base {
namespace strutil {

std::string FormatLargeNumber(uint64_t i) {
    string s = ToString(i);
    if (s.size() > 5) {
        for (int i = s.size() - 3; i >= 1; i -= 3) {
            s.insert(i, ",");
        }
    }
    return s;
}

string FormatStorageUnit(int64_t n) {
    stringstream sstr;
    bool is_neg = (n < 0);
    if (is_neg) {
        n *= -1; // make positive
        sstr << "-";
    }
    if (n >= 1024L * 1024 * 1024 * 1024) {
        sstr << (1.0 * n / (1024L * 1024 * 1024 * 1024));
        sstr << "T";
    } else if (n >= 1024L * 1024 * 1024) {
        sstr << (1.0 * n / (1024L * 1024 * 1024));
        sstr << "G";
    } else if (n >= 1024L * 1024) {
        sstr << (1.0 * n / (1024L * 1024));
        sstr << "M";
    } else if (n >= 1024L) {
        sstr << (1.0 * n / 1024L);
        sstr << "K";
    } else {
        sstr << n;
    }
    return sstr.str();
}

Option<int64_t> ToStorageUnit(const string& input) {
    int l = input.size();
    int64_t multi = 1;
    Option<int64_t> i = 0;

    if (input[l - 1] == 'K' || input[l - 1] == 'k') {
        multi = 1024;
    } else if (input[l - 1] == 'M' || input[l - 1] == 'm') {
        multi = 1024 * 1024;
    } else if (input[l - 1] == 'G' || input[l - 1] == 'g') {
        multi = 1024 * 1024 * 1024;
    } else if (input[l - 1] == 'T' || input[l - 1] == 't') {
        multi = 1024UL * 1024 * 1024 * 1024;
    }
    if (multi == 1) {
        i = To<int64_t>(input);
    } else {
        string value(input);
        value.resize(l - 1);
        i = To<int64_t>(value);
    }
    if (!i.valid()) {
        return false;
    }
    return make_option(i.value() * multi);
}

template<> Option<byte> To(const std::string& input) {
    std::istringstream sstr(input);
    uint16_t value;
    sstr >> value;
    if (sstr.fail() || !sstr.eof()) {
        return false;
    }
    if (value > 255) {
        return false;
    }
    return make_option(static_cast<byte>(value));
}

template<> Option<int8_t> To(const std::string& input) {
    std::istringstream sstr(input);
    int16_t value;
    sstr >> value;
    if (sstr.fail() || !sstr.eof()) {
        return false;
    }
    if (value < -128 || value > 127) {
        return false;
    }
    return make_option(static_cast<int8_t>(value));
}

template<> Option<bool> To(const std::string& input) {
    if (input == "true") {
        return make_option(true);
    } else if (input == "false") {
        return make_option(false);
    }
    return false;
}

Option<size_t> Index(const string& str, const string& pattern) {
    size_t pos = str.find(pattern, 0);
    if (pos == string::npos) {
        return false;
    }
    return make_option(pos);
}

bool StartsWith(const string& input, const string& pattern) {
    int pos = input.find(pattern, 0);
    return pos == 0;
}

bool EndsWith(const string& input, const string& pattern) {
    int pos = input.find(pattern, 0);
    if (pos == std::string::npos) {
        return false;
    }
    return pos == input.size() - pattern.size();
}

bool Contains(const string& text, const string& search) {
    int pos = text.find(search, 0);
    return pos >= 0;
}

bool IsNumeric(const string& text) {
    if (text.size() == 0U) {
        return false;
    }
    for (size_t i = 0; i < text.size(); i++) {
        if (text[i] < '0' || text[i] > '9') {
            return false;
        }
    }
    return true;
}

bool IsWhitespace(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

string Trim(const string& s) {
    string tmp = s;
    while (tmp.size() > 0 && IsWhitespace(tmp[tmp.size() - 1])) {
        tmp = tmp.substr(0, tmp.size() - 1);
    }
    while (tmp.size() > 0 && IsWhitespace(tmp[0])) {
        tmp = tmp.substr(1);
    }
    return tmp;
}

template<> std::string ToString(bool b) {
    if (b) {
        return "true";
    } else {
        return "false";
    }
}

std::string ToHexString(const void* data, size_t data_size) {
    std::stringstream s;
    unsigned int i = 0;
    const byte* d = static_cast<const byte*>(data);
    char buf[10];
    for (i = 0; i < data_size; i++) {
        snprintf(buf, sizeof(buf), "%02x", d[i]);
        s << buf;
    }
    return s.str();
}

template<> std::string ToHexString(byte b) {
    std::stringstream ss;
    ss << std::hex << static_cast<uint32_t>(b);
    return ss.str();
}

bool Split(const string& input, const string& delimiter, vector<string>* results, bool includeEmpties) {
    CHECK_RETURN(results, -1, "Results not set");
    // code from http://www.codeproject.com/string/stringsplit.asp
    int iPos = 0;
    int newPos = -1;
    size_t sizeS2 = delimiter.size();
    size_t isize = input.size();

    if ((isize == 0) || (sizeS2 == 0)) {
        return true;
    }

    vector<int> positions;

    newPos = input.find(delimiter, 0);

    if (newPos < 0) {
        if (includeEmpties) {
            results->push_back(input);
        }
        return true;
    }

    int numFound = 0;

    while (newPos >= iPos) {
        numFound++;
        positions.push_back(newPos);
        iPos = newPos;
        newPos = input.find(delimiter, iPos + sizeS2);
    }

    if (numFound == 0) {
        return true;
    }

    for (int i = 0; i <= (int) positions.size(); ++i) {
        string s("");
        if (i == 0) {
            s = input.substr(i, positions[i]);
        } else {
            size_t offset = positions[i - 1] + sizeS2;
            if (offset < isize) {
                if (i == (int) positions.size()) {
                    s = input.substr(offset);
                } else if (i > 0) {
                    s = input.substr(positions[i - 1] + sizeS2, positions[i] - positions[i - 1] - sizeS2);
                }
            }
        }
        if (includeEmpties || (s.size() > 0)) {
            results->push_back(s);
        }
    }
    return true;
}

bool Split(const string& input, const string& delimiter, string* string1, string* string2) {
    CHECK(string1 && string2, "Results not set");
    vector<string> results;
    bool returnValue = Split(input, delimiter, &results, false);
    if (!returnValue) {
        return false;
    }
    *string1 = "";
    *string2 = "";
    if (results.size() == 2) {
        *string1 = results[0];
        *string2 = results[1];
        return true;
    } else {
        return false;
    }
}

bool IsPrintable(const string& s) {
    return RE2::FullMatch(s, "[[:print:]]*");
}

dedupv1::base::Option<std::string> ReplaceAll(const std::string& input, const std::string& pattern, const std::string replacement) {
    string output = input;
    CHECK(RE2::GlobalReplace(&output, pattern, replacement),
        "Failed to replace: input " << input << ", pattern " << pattern << ", sub " << replacement);
    return make_option(output);
}

bool FromHexString(const std::string& s, bytestring* bs) {
    for (int i = 0; i + 1 < s.size(); i += 2) {
        std::stringstream ss;
        ss << std::hex << s.at(i) << s.at(i + 1);
        int b;
        ss >> b;
        byte b2 = b;
        bs->append(&b2, 1);
        CHECK(!ss.bad(), "Failed to parse hex string");
    }
    return true;
}

string FriendlySubstr(const string& str, size_t pos, size_t n, const std::string& cut_suffix) {
    if (str.size() > (n + pos)) {
        string s = str.substr(pos, n);
        if (!cut_suffix.empty()) {
            s.append(cut_suffix);
        }
        return s;
    } else {
        return str.substr(pos, str.size() - pos);
    }
}

std::string ToStringAsFixedDecimal(double d, int precision) {
    std::stringstream ss;
    ss.setf(std::ios::fixed, std::ios::floatfield);
    if (precision) {
        ss.setf(std::ios::showpoint);
    }
    ss.precision(precision);
    ss << d;
    return ss.str();
}

}
}
}
