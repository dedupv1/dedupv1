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

#ifndef DEDUPV1D_TARGET_H__
#define DEDUPV1D_TARGET_H__

#include <stdint.h>

#include <core/dedup.h>
#include <base/option.h>

#include "dedupv1d.pb.h"

#include <string>
#include <set>
#include <list>

namespace dedupv1d {

/**
 *
 * Represents a SCST target managed by dedupv1d.
 *
 * \ingroup dedupv1d
 */
class Dedupv1dTarget {
        // Copy and assignment is ok
    private:
        /**
         * true iff the target is preconfigured.
         */
        bool preconfigured_;

        /** 
         *Target id
         */
        uint32_t tid_;

        /**
         * name of the target
         */
        std::string name_;

        /**
         * List of additional parameters
         */
        std::list<std::pair<std::string, std::string> > params_;

        /**
         * static constant set of allowed parameter names
         */
        static const std::set<std::string> kAllowedParams;

        /**
         * Username of the target used for mutual authentication
         */
        std::string auth_username_;

        /**
         * Secret hash of the target used for mutual authentication
         */
        std::string auth_secret_hash_;

        static std::set<std::string> MakeAllowedParamSet();
    public:
        /**
         * Constructor
         */
        explicit Dedupv1dTarget(bool preconfigured);
        
        /**
         * Constructor
         */
        Dedupv1dTarget();

        /**
         *
         * Available options:
         * - name: String
         * - tid: uint32_t
         * - param.*: String
         * - auth.name: String
         * - auth.secret: String
         *
     * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        bool ParseFrom(const TargetInfoData& data);

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        bool SerializeTo(TargetInfoData* data);

        /**
         * Destructor
         */
        ~Dedupv1dTarget();

        /**
         * returns the name of the target
         */
        inline const std::string& name() const;

        /**
         * returns a list of all additional parameters
         */
        inline const std::list<std::pair<std::string, std::string> >& params() const;

        dedupv1::base::Option<std::string> param(const std::string& param) const;

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        bool ChangeParam(const std::string& param_key, const std::string& param_value);

        /**
         * returns the tid of the target
         */
        inline uint32_t tid() const;

        inline bool is_preconfigured() const;

        inline const std::string& auth_username() const {
            return auth_username_;
        }

        inline const std::string& auth_secret_hash() const {
            return auth_secret_hash_;
        }

        std::string DebugString() const;
};

const std::string& Dedupv1dTarget::name() const {
    return this->name_;
}

const std::list<std::pair<std::string, std::string> >& Dedupv1dTarget::params() const {
    return params_;
}

uint32_t Dedupv1dTarget::tid() const {
    return tid_;
}

bool Dedupv1dTarget::is_preconfigured() const {
    return this->preconfigured_;
}

}

#endif /* DEDUPV1D_GROUP_H_ */
