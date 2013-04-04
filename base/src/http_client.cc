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

#define CURL_DISABLE_TYPECHECK

#include <base/http_client.h>
#include <base/memchunk.h>
#include <curl/curl.h>
#include <curl/easy.h>
#include <base/logging.h>

using std::string;
using dedupv1::base::Memchunk;

#define check_curl_error(x) {CURLcode e = (x); if (e != CURLE_OK) { ERROR(curl_easy_strerror(e)); goto error; }}

LOGGER("HttpResult");

namespace dedupv1 {
namespace base {

size_t HttpResult::GetUrlCallback(void* ptr, size_t size, size_t nmemb, void* data) {
    size_t realsize = size * nmemb;
    Memchunk* mc = (Memchunk *) data;
    CHECK(mc, "Memory buffer not set");
    CHECK(mc->size() >= 0, "Illegal memory buffer");

    size_t old_size = mc->size();
    CHECK(mc->Realloc(mc->size() + realsize + 1), "HTTP buffer realloc failed");
    byte* value = (byte *) mc->value();
    memcpy(&(value[old_size]), ptr, realsize);
    value[mc->size() - 1] = 0;

    return realsize;
}

HttpResult::HttpResult() {
    code_ = 0;
    content_size_ = 0;
    content_ = NULL;
}

HttpResult* HttpResult::GetUrl(const string& url) {
    CURL* curl_handle = curl_easy_init();
    HttpResult* result = new HttpResult();
    Memchunk* mc = NULL;
    char* content_type = NULL;

    CHECK_GOTO(result, "Alloc failed");

    mc = new Memchunk(NULL, 0, false);
    CHECK_GOTO(mc, "Alloc memchunk failed");

    DEBUG("Requesting " << url);

    check_curl_error(curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str()));
    check_curl_error(curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, &HttpResult::GetUrlCallback));
    check_curl_error(curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *) mc));

    check_curl_error(curl_easy_perform(curl_handle));
    check_curl_error(curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &result->code_));

    check_curl_error(curl_easy_getinfo(curl_handle, CURLINFO_CONTENT_TYPE, &content_type));
    if (content_type) {
        result->content_type_ = content_type;
    }
    curl_easy_cleanup(curl_handle);
    curl_handle = NULL;

    CHECK_GOTO(mc->size() >= 0, "Memory error during http request");
    result->content_ = mc->value();
    result->content_size_ = mc->size();
    delete mc;

    DEBUG("HTTP GET " << result->code());
    return result;
error:

    if (result) {
        delete result;
        result = NULL;
    }
    if (mc) {
        delete mc;
        mc = NULL;
    }
    if (curl_handle) {
        curl_easy_cleanup(curl_handle);
        curl_handle = NULL;
    }
    return NULL;
}

HttpResult::~HttpResult() {
    if (this->content_) {
        delete[] this->content_;
    }
}

}
}
