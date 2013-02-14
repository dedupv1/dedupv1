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

#ifndef OPTION_H__
#define OPTION_H__

#include <base/base.h>
#include <string>
#include <sstream>

namespace dedupv1 {
namespace base {

/**
 * The option class is used in the error handling to
 * return an error flag (true/false) and an optional (therefore
 * the name) value.
 *
 * It is modeled similar to Scala's Option class.
 */
template <class T> class Option {
    private:
        /**
         * flag if the option value is set
         */
        bool set_;

        /**
         * optional value
         */
        T value_;
    public:
        /**
         * Default parameter. By default, the option
         * doesn't contain an optional value
         * @return
         */
        Option() {
            set_ = false;
        }

        /**
         * Explicit constructor from bool.
         * This constructor is mainly used to
         * convert a false return value to an empty option.
         *
         * @param b
         * @return
         */
        Option(bool b) {
            set_ = b;
        }

        /**
         * creates a new option.
         * Usually a helper method make_option should be used.
         * @param b
         * @param value
         * @return
         */
        Option(bool b, T value) {
            set_ = b;
            value_ = value;
        }

        /**
         * return a reference to the value
         * @return
         */
        const T& value() const {
            return value_;
        }

        /*
         * Returns true iff the option is set.
         * The option is therefore valid.
         */
        bool valid() const {
            return set_;
        }

        /**
         * returns a developer-readable representation
         * of the option.
         *
         * @return
         */
        std::string DebugString() const {
            if (!set_) {
                return "[not set]";
            }
            std::stringstream ss;
            ss << "[" << value() << "]";
            return ss.str();
        }
};

/**
 * Like the Some method in Scala.
 * Generated a new Option with the parameter as value
 */
template<class T> Option<T> Some(T v) {
        return typename Option<T>::Option(true, v);
}

/**
 * Creates a new options with the given value set
 */
template<class T> Option<T> make_option(T v) {
        return typename Option<T>::Option(true, v);
}

}
}

#endif // OPTION_H__
