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
#ifndef HTTP_CLIENT_H__
#define HTTP_CLIENT_H__

#include <base/base.h>

#include <string>

namespace dedupv1 {
namespace base {

/**
* Easy to to perform a high-level http request.
* The HttpResult represents the result of a http request.
*/
class HttpResult {
    private:
        DISALLOW_COPY_AND_ASSIGN(HttpResult);
        /**
        * http response status code
        */
        long code_;

        /**
        * http content size
        */
        size_t content_size_;

        /**
        * http response content
        */
        byte* content_;

        /**
        * http content type.
        */
        std::string content_type_;

        /**
        * curl callback method.
        * Private implementation detail.
        * @param ptr
        * @param size
        * @param nmemb
        * @param data
        * @return
        */
        static size_t GetUrlCallback(void* ptr, size_t size, size_t nmemb, void* data);

        /**
        * Private constructor
        * @return
        */
        HttpResult();
    public:
        /**
        * returns the content size
        * @return
        */
        inline size_t content_size() const;

        /**
        * returns the content type
        * @return
        */
        inline const std::string content_type() const;

        /**
        * returns the content as uninterpreted byte array
        * @return
        */
        inline const byte* content() const;

        /**
        * returns the http status code
        * @return
        */
        inline long code() const;

        /**
        * Destructor
        * @return
        */
        ~HttpResult();

        /**
        * performs a http request.
        * The client is responsible to delete the http result
        *
        * @param url
        * @return
        */
        static HttpResult* GetUrl(const std::string& url);
};

size_t HttpResult::content_size() const {
    return content_size_;
}
const std::string HttpResult::content_type() const {
    return content_type_;
}
const byte* HttpResult::content() const {
    return content_;
}

long HttpResult::code() const {
    return code_;
}

}
}

#endif  // HTTP_CLIENT_H__
