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

#ifndef VOLUME_MONITOR_H__
#define VOLUME_MONITOR_H__

#include <core/dedup.h>

#include "default_monitor.h"
#include "dedupv1d.h"
#include "dedupv1d_volume.h"

#include <list>
#include <string>


namespace dedupv1d {
namespace monitor {

/**
 * The volume monitor reports informations about the currently
 * configured volume to the user.
 *
 * \ingroup monitor
 */
class VolumeMonitorAdapter : public MonitorAdapter {
        friend class VolumeMonitorAdapterRequest;
   private:
        /**
         * Reference to the dedupv1d daemon
         */
        dedupv1d::Dedupv1d* ds_;
    public:
        /**
         * Constructor.
         *
         * @param ds
         * @return
         */
        explicit VolumeMonitorAdapter(dedupv1d::Dedupv1d* ds);

        /**
         * Destructor.
         * @return
         */
        virtual ~VolumeMonitorAdapter();

        /**
         * Creates a new volume monitor request.
         * @return
         */
        virtual MonitorAdapterRequest* OpenRequest();
};

/**
 * A volume adapter request.
 *
 * \ingroup monitor
 */
class VolumeMonitorAdapterRequest : public MonitorAdapterRequest {
    private:
        /**
         * pointer to the parent monitor
         */
        VolumeMonitorAdapter* adapter;

        /**
         * list of options pairs.
         */
        std::list< std::pair<std::string, std::string> > options;

        /**
         * key of the operation.
         */
        std::string operation;

        /**
         * writes informations about a volume in a JSON
         * format.
         *
         * @param volume
         * @return
         */
        std::string WriteVolume(Dedupv1dVolumeInfo* info, Dedupv1dVolume* volume);

        std::string WriteAllVolumes(dedupv1d::Dedupv1d* ds);
    public:
        /**
         * Constructor.
         * @param adapter
         * @return
         */
        explicit VolumeMonitorAdapterRequest(VolumeMonitorAdapter* adapter);

        /**
         * Destructor.
         * @return
         */
        virtual ~VolumeMonitorAdapterRequest();

        virtual std::string Monitor();

        /**
     * @return true iff ok, otherwise an error has occurred
         */
        virtual bool ParseParam(const std::string& key, const std::string& value);
};

}
}

#endif  // VOLUME_MONITOR_H__
