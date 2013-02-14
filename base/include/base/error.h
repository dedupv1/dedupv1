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
 * @file error.h
 * Class for the advanced error handing with the error context
 */

#ifndef ERROR_H_
#define ERROR_H_

#include <string>
#include <base/base.h>

namespace dedupv1 {
namespace base {

#define NO_EC NULL

/**
 * An error context is used for error handling.
 *
 * The error context is handled up and two the callstack.
 * If an error occurs the system is able to put important details
 * about the error here, e.g. if the error has been related to
 * the overflow of disk storage.
 *
 * This method is here better than exceptions as with exceptions
 * such informations might be lost before the informations has reached
 * an client that needs an information.
 *
 * At the moment only the usage within a single thread is
 * supported.
 */
class ErrorContext {
    private:
	    /**
		 * if set, an occur occurred due to the fact that a storage component is full
		 */
        bool full_;

		/**
		 * if set, an fatal error occurred
		 */
        bool fatal_;

		/**
		 * if set, an error occurred due to an checksum error
		 */
        bool checksum_error_;
    public:
        /**
         * Constructor.
         * All error types are set to false.
         * @return
         */
        inline ErrorContext();

        /**
         * sets the error type that some component is full
         */
        inline void set_full();

        /**
         * sets that a fatal error occurred
         */
        inline void set_fatal();

        /**
         * sets that an checksum error occured
         */
        inline void set_checksum_error();

        /**
         * returns true iff the error had an checksum error
         */
        inline bool has_checksum_error() const;

        /**
         * returns true iff there was an error because a component was full
         */
        inline bool is_full() const;

        /**
         * returns true iff there was an fatal error
         */
        inline bool is_fatal() const;

        /**
         * returns a developer-readable represention of the object.
         */
        std::string DebugString() const;
};

ErrorContext::ErrorContext() : full_(false), fatal_(false), checksum_error_(false) {
}

void ErrorContext::set_full() {
    full_ = true;
}

void ErrorContext::set_fatal() {
    fatal_ = true;
}

void ErrorContext::set_checksum_error() {
    checksum_error_ = true;
}

bool ErrorContext::has_checksum_error() const {
    return checksum_error_;
}

bool ErrorContext::is_full() const {
    return full_;
}

bool ErrorContext::is_fatal() const {
    return fatal_;
}

}
}

#endif /* ERROR_H_ */
