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

#ifndef LOG_MONITOR_H__
#define LOG_MONITOR_H__

#include <core/dedup.h>

#include "default_monitor.h"
#include "dedupv1d.h"
#include "log_replayer.h"

#include <string>

namespace dedupv1d {
namespace monitor {

/**
 * Monitor to show log information and to change
 * the state of the log replay.
 *
 * \ingroup monitor
 */
class LogMonitorAdapter : public DefaultMonitorAdapter {
    private:
    dedupv1d::Dedupv1d* ds_;
    dedupv1d::LogReplayer* log_replayer_;

    std::string message_;
    public:
    /**
     * Constructor
     * @param ds
     * @return
     */
    explicit LogMonitorAdapter(dedupv1d::Dedupv1d* ds);
    virtual std::string Monitor();
    virtual bool ParseParam(const std::string& key, const std::string& value);
};

}
}

#endif  // LOG_MONITOR_H__
