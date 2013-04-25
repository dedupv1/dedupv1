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

#include <base/config_loader.h>
#include <base/fileutil.h>
#include <base/logging.h>
#include <base/strutil.h>

using std::string;
using dedupv1::base::strutil::Index;
using dedupv1::base::strutil::Trim;
using dedupv1::base::strutil::IsPrintable;
using dedupv1::base::strutil::ToString;

LOGGER("Config");

namespace dedupv1 {
namespace base {

ConfigLoader::ConfigLoader(Callback2<bool, const std::string&,
                                     const std::string&>* option_callback,
                           Callback1<void, const std::string&>* error_callback) {
    option_callback_ = option_callback;
    error_callback_ = error_callback;
    if (error_callback_ == NULL) {
        error_callback_ = NewVoidCallback(&ConfigLoader::NullErrorHandler);
    }
}

ConfigLoader::~ConfigLoader() {
    if (option_callback_) {
        delete option_callback_;
    }
    delete error_callback_;
}

void ConfigLoader::NullErrorHandler(const string& error_message) {
    ERROR(error_message);
}

bool ConfigLoader::ProcessLine(const std::string& configuration_line, uint32_t line_index) {
    CHECK(this->option_callback_, "Option callback not set");
    string line = configuration_line;

    // Check if comment
    size_t i = line.find("#");
    if (i != string::npos) {
        line = line.substr(0, i);
    }

    line = Trim(line);
    if (line.size() == 0) {
        return true;
    }

    if (!IsPrintable(line)) {
        error_callback_->Call("Illegal configuration line " + ToString(line_index) + ": line contains non-printable characters");
    }

    // Split key/value pair
    Option<size_t> li = Index(line, "=");
    if (!li.valid()) {
        error_callback_->Call("Illegal configuration line " + ToString(line_index) + ": " + line);
        return false;
    }
    line[li.value()] = 0;
    string option_name = Trim(line.substr(0, li.value()));
    string option = Trim(line.substr(li.value() + 1));

    if (!this->option_callback_->Call(option_name, option)) {
        error_callback_->Call("Configuration failed, line " + ToString(line_index) + ": " + option_name + "=" + option);
        return false;
    }
    return true;
}

bool ConfigLoader::ProcessFile(const std::string& filename) {
    CHECK(this->option_callback_, "Option callback not set");
    File* file = File::Open(filename, 0, 0);
    CHECK(file, "Cannot open configuration file: " << filename);

    int offset = 0;
    string buffer;
    int line_index = 1;
    while (file->GetLine(&offset, &buffer, 1024)) {
        if (!ProcessLine(buffer, line_index)) {
            delete file;
            return false;
        }
        config_ += buffer + "\n";
        line_index++;
    }
    delete file;
    return true;
}

}
}

