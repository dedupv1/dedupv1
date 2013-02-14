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

#ifndef THROTTLE_HELPER_H__
#define THROTTLE_HELPER_H__

#include <core/dedup.h>
#include <base/option.h>

namespace dedupv1 {

    /**
     * Simple class that encapsulated the throttling logic
     */
    class ThrottleHelper {

            static const double kDefaultThrottleFactor = 1.0;

            static const double kDefaultHardLimitFactor = 0.9;

            static const double kDefaultSoftLimitFactor = 0.5;

            /**
             * 100ms
             */
            static const double kThrottleWaitTime = 100;

        private:
            double throttle_factor_;

            double hard_limit_factor_;

            double soft_limit_factor_;

            uint32_t throttle_wait_time_;

            bool enabled_;
        public:
            /**
             * Constructor
             */
            ThrottleHelper();

            /**
             *
             * Available options:
             * - enabled: Boolean
             * - factor: Double
             * - soft-limit: Double
             * - hard-limit: Double
             */
            bool SetOption(const std::string& option_name, const std::string& option);

            dedupv1::base::Option<bool> Throttle(
                    double fill_ratio,
                    double thread_ratio);

            void set_throttle_factor(double v) {
                throttle_factor_ = v;
            }

            void set_hard_limit_factor(double v) {
                hard_limit_factor_ = v;
            }

            void set_soft_limit_factor(double v) {
                soft_limit_factor_ = v;
            }

            void set_throttle_wait_time(uint32_t v) {
                throttle_wait_time_ = v;
            }

            double soft_limit_factor() const {
                return soft_limit_factor_;
            }

            double hard_limit_factor() const {
                return hard_limit_factor_;
            }

            double throttle_factor() const {
                return throttle_factor_;
            }

            uint32_t throttle_wait_time() const {
                return throttle_wait_time_;
            }
    };
}

#endif
