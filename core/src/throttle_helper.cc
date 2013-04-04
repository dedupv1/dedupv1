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

#include <core/throttle_helper.h>
#include <base/thread.h>
#include <base/strutil.h>
#include <base/logging.h>
#include "math.h"

using std::string;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToString;
using dedupv1::base::ThreadUtil;
using dedupv1::base::Option;
using dedupv1::base::make_option;

LOGGER("Throttle");

namespace dedupv1 {

ThrottleHelper::ThrottleHelper() {
    soft_limit_factor_ = kDefaultSoftLimitFactor;
    hard_limit_factor_ = kDefaultHardLimitFactor;
    throttle_factor_ = kDefaultThrottleFactor;
    throttle_wait_time_ = kThrottleWaitTime;
    enabled_ = true;
}

Option<bool> ThrottleHelper::Throttle(
    double fill_ratio,
    double thread_ratio) {

    if (!enabled_) {
        return make_option(false);
    }
    bool throttled = false;
    if (fill_ratio > this->soft_limit_factor_) {
        double scaled_fill_ratio = fill_ratio / hard_limit_factor_;
        double throttle_ratio = exp(scaled_fill_ratio * throttle_factor_) / exp(throttle_factor_);

        // the basic idea is to throttle down a specific number of threads
        // the throttle ratio is a number between 0 and unlimited, but it reached 1.0 when the hard limit
        // factor is hit. We more or less shutdown the system when that point is reached.
        if (throttle_ratio > thread_ratio) {
            throttled = true;
            ThreadUtil::Sleep(throttle_wait_time_, ThreadUtil::MILLISECONDS);
        }

        DEBUG("Throttling, "
            "fill ratio " << fill_ratio <<
            ", scaled fill ratio " << scaled_fill_ratio <<
            ", throttle ratio " << throttle_ratio <<
            ", thread_ratio " << thread_ratio <<
            ", throttled " << ToString(throttled));
    }
    return make_option(throttled);
}

bool ThrottleHelper::SetOption(const string& option_name, const string& option) {
    if (option_name == "enabled") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->enabled_ = To<bool>(option).value();

        if (!enabled_) {
            WARNING("Throttling disabled");
        }
        return true;
    }
    if (option_name == "factor") {
        CHECK(To<double>(option).valid(), "Illegal option " << option);
        double c = To<double>(option).value();
        CHECK(c >= 0, "Illegal throttle factor");
        this->throttle_factor_ = c;
        return true;
    } else if (option_name == "soft-limit") {
        CHECK(To<double>(option).valid(), "Illegal option " << option);
        double c = To<double>(option).value();
        CHECK(c > 0.0 && c < 1.0, "Illegal soft limit");
        this->soft_limit_factor_ = c;
        return true;
    } else if (option_name == "hard-limit") {
        CHECK(To<double>(option).valid(), "Illegal option " << option);
        double c = To<double>(option).value();
        CHECK(c > 0.0 && c < 1.0, "Illegal hard limit");
        this->hard_limit_factor_ = c;
        return true;
    }
    ERROR("Illegal option name: " << option_name);
    return false;
}

}
