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

#ifndef DEDUPV1_REPLAYER_H_
#define DEDUPV1_REPLAYER_H_

#include <string>

#include <core/chunk_index.h>
#include <core/container_storage.h>
#include "dedupv1d.h"

#include <string>

#include <core/chunk_index.h>
#include <core/container_storage.h>
#include "dedupv1d.h"

namespace dedupv1 {
namespace contrib {
namespace replay {

/**
 * Class that replays the operations log
 *
 */
class Dedupv1Replayer {
    public:
        Dedupv1Replayer();

        ~Dedupv1Replayer();

        /**
         * Pause the gc while replaying.
         * This is usually necessary for testing purposes
         */
        bool PauseGC();

        /**
         * Resume the gc while replaying.
         * PauseGC() should be called before callung UnPauseGC().
         *
         * This is usually necessary for testing purposes
         */
        bool UnPauseGC();

        bool Initialize(const std::string& filename);

        bool Replay();

        bool Stop();

    private:
        dedupv1d::Dedupv1d* system_;

        bool started_;

        bool gc_paused_;
};

}
}
}

#endif /* DEDUPV1_REPLAYER_H_ */
