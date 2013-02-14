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
#ifndef MEMORY_NEW_HANDLER_H__
#define MEMORY_NEW_HANDLER_H__

#include <base/base.h>

namespace dedupv1 {
namespace base {
namespace memory {

/**
 * The new handler is an observer that is called when
 * the system runs out of memory.
 */
class NewHandlerListener {
    public:
        /**
         * Constructor
         * @return
         */
        NewHandlerListener();

        /**
         * Destructor.
         * @return
         */
        virtual ~NewHandlerListener();

        /**
         * This method is called if an out of memory event is caused by the
         * new handler.
         */
        virtual bool ReceiveOutOfMemoryEvent() = 0;
};

/**
 * Registers a new handler which releases a given amount of memory if the
 * new-operator fails because of memory issues.
 * This must be called from a single thread only and it should complete before
 * any calls to new are issued from other threads.
 * @return true iff ok, otherwise an error has occurred
 */
bool RegisterMemoryParachute(int64_t parachute_size);

/**
 * Adds a new listener to the list the new handler informs.
 * @return true iff ok, otherwise an error has occurred
 */
bool AddMemoryParachuteListener(NewHandlerListener* listener);

/**
 * Removes a given memory parachute listener.
 *
 * @param listener
 * @return true iff ok, otherwise an error has occurred
 */
bool RemoveMemoryParachuteListener(NewHandlerListener* listener);

/**
 * Potentially unsafe
 * TODO (dmeister): Why???
 * @return true iff ok, otherwise an error has occurred
 */
bool ClearMemoryParachute();

}
}
}

#endif  // MEMORY_NEW_HANDLER_H__
