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

#ifndef INSPECT_H_
#define INSPECT_H_

#include <dedupv1d.h>

namespace dedupv1d {

/**
 * Class that helps inspecting the internal data of the system.
 * This class is used by the dedupv1_debug utility and the inspect monitor
 */
class Inspect {
    private:
        Dedupv1d* ds_;
    public:

        /**
         * Constructor
         * @param ds
         * @return
         */
        Inspect(Dedupv1d* ds);

        /**
         * shows JSON representation of a given log id
         *
         * @param log id to show
         */
        std::string ShowLog(uint64_t log_id);

        /**
         * shows a JSON representation of the log in general
         */
        std::string ShowLogInfo();

        /**
         * shows the container data of a given container. It is possible
         * to filter the shown container items by an fingerprint.
         */
        std::string ShowContainer(uint64_t container_id, bytestring* fp_filter);

        /**
         * shows the container header of a given container. The container head
         * contains various information about the container, but not the container items
         */
        std::string ShowContainerHeader(uint64_t container_id);

        /**
         * shows information about a given block
         */
        std::string ShowBlock(uint64_t block_id);

        /**
         * shows information about the chunk index data of a chunk.
         *
         * @param fingerprint of the chunk to show
         */
        std::string ShowChunk(const bytestring& fp);
};

}

#endif /* INSPECT_H_ */
