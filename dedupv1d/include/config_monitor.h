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

#ifndef CONFIG_MONITOR_H__
#define CONFIG_MONITOR_H__

#include <core/dedup.h>
#include "default_monitor.h"
#include "dedupv1d.h"

namespace dedupv1d {
namespace monitor {

/**
 * The config monitor display the currently used configuration.
 *
 * \ingroup monitor
 */
class ConfigMonitorAdapter : public DefaultMonitorAdapter {
    private:
    DISALLOW_COPY_AND_ASSIGN(ConfigMonitorAdapter);
    /**
     * Pointer to the global deduplication system.
     */
    dedupv1d::Dedupv1d* ds_;
    public:
    /**
     * Constructor
     * @param ds
     * @return
     */
    explicit ConfigMonitorAdapter(dedupv1d::Dedupv1d* ds);

    /**
     * Shows the current configuration as a text/plain site.
     *
     */
    virtual std::string Monitor();
};

}
}

#endif  // CONFIG_MONITOR_H__
