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
#include <base/memory_new_handler.h>

#include <new>
#include <pthread.h>
#include <stdlib.h>
#include <list>
#include <vector>

#include <base/logging.h>

LOGGER("Memory");

namespace dedupv1 {
namespace base {
namespace memory {

NewHandlerListener::NewHandlerListener() {
}

NewHandlerListener:: ~NewHandlerListener() {
}

class MemoryParachute {
private:
    /**
     * Mutex for ensuring thread safety of the new_handler. Note that we're not using the
     * version from locks.h since it may potentially dynamically reserve memory during runtime
     * at some point in future.
     */
    pthread_mutex_t mutex;

    /**
     *  Void ptr for holding the memory parachute.
     */
    std::vector<void*> parachute_memory;

    /**
     * List of listeners (no pun intended) to inform in case of a out of memory event.
     */
    std::list<NewHandlerListener*> listeners;

    bool valid_state;

    /**
     * the custom parachute new handler
     */
    static void new_handler();
public:
    MemoryParachute() {
        // Initialize the lock used to guarantee thread safety of the new_handler
        int lockresult = pthread_mutex_init(&this->mutex, NULL);
        if (lockresult == 0) {
            valid_state = true;
        } else {
            ERROR("Lock init failed: " << strerror(lockresult));
            valid_state = false;
        }
    }

    ~MemoryParachute() {
        Clear();
        pthread_mutex_destroy(&this->mutex);
    }

    bool Start(uint64_t parachute_size) {
        if (!parachute_memory.empty()) {
            DEBUG("Parachute already open");
            return true;
        }
        CHECK(valid_state, "Memory parachute not valid")

        int mem_chunk_count = (parachute_size / (64 * 1024 * 1024LLU) + 1);
        int mem_per_chunk = parachute_size / mem_chunk_count;

        DEBUG("Allocate " << mem_chunk_count << " memory chunks a " << mem_per_chunk << " bytes");
        // Initialize the memory parachute.
        // we use malloc directly here to avoid irritations with new

        parachute_memory.resize(mem_chunk_count);
        for (int i = 0; i < mem_chunk_count; i++) {
            CHECK(parachute_memory[i] = malloc(mem_per_chunk),
                "Malloc for memory parachute failed.");
        }
        DEBUG("Allocation done");
        std::set_new_handler(&MemoryParachute::new_handler);
        return true;
    }

    /**
     * Potentially unsafe
     * @return
     */
    bool Clear() {
        std::set_new_handler(NULL);
        for (int i = 0; i < parachute_memory.size(); i++) {
            if (parachute_memory[i]) {
                free(parachute_memory[i]);
                parachute_memory[i] = NULL;
            }
        }
        parachute_memory.clear();
        return true;
    }

    bool RemoveMemoryParachuteListener(NewHandlerListener* listener) {
        // tried to use std::find here, but didn't compile and no fun to make it so
        std::list<NewHandlerListener*>::iterator i;
        for (i = listeners.begin(); i != listeners.end(); i++) {
            if (*i == listener) {
                listeners.erase(i);
                return true;
            }
        }
        return false;
    }

    bool AddMemoryParachuteListener(NewHandlerListener* listener) {
        listeners.push_back(listener);
        return true;
    }
};

static MemoryParachute memory_parachute;

bool ClearMemoryParachute() {
    return memory_parachute.Clear();
}

bool RegisterMemoryParachute(int64_t parachute_size) {
    return memory_parachute.Start(parachute_size);
}

bool RemoveMemoryParachuteListener(NewHandlerListener* listener) {
    return memory_parachute.RemoveMemoryParachuteListener(listener);
}

bool AddMemoryParachuteListener(NewHandlerListener* listener) {
    return memory_parachute.AddMemoryParachuteListener(listener);
}

void MemoryParachute::new_handler() {
    // we simply access the static instance in the memory handler

    int lockresult = pthread_mutex_lock(&memory_parachute.mutex);
    if (lockresult != 0) {
        // Failure to lock for some reason.
        ERROR("Could not get mutex for parachute.");
        std::set_new_handler(NULL);
        return;
    }
    for (int i = 0; i < memory_parachute.parachute_memory.size(); i++) {
        if (memory_parachute.parachute_memory[i]) {
            // A memory parachute exists, so release it.
            free(memory_parachute.parachute_memory[i]);
            memory_parachute.parachute_memory[i] = NULL;
        }
    }
    // Inform the listeners. Should happen exactly once.
    std::list<NewHandlerListener*>::iterator iter;
    for (iter = memory_parachute.listeners.begin(); iter != memory_parachute.listeners.end(); iter++) {
        (void) (*iter)->ReceiveOutOfMemoryEvent();
    }

    pthread_mutex_unlock(&memory_parachute.mutex);

    // Memory parachute has been released so we can
    // switch back to the default new handler and hope for the best.
    std::set_new_handler(NULL);
}

}
}
}
