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

#ifndef MONITOR_H__
#define MONITOR_H__

#include <stdint.h>
#include <core/dedup.h>
#include <base/locks.h>
#include <base/startup.h>
#include <base/profile.h>
#include "pthread.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <microhttpd.h>
#include <vector>

#include <tbb/atomic.h>
#include <tbb/concurrent_hash_map.h>

#include <map>
#include <string>
#include <vector>

namespace dedupv1d {
namespace monitor {
    class MonitorSystem;

/**
 * \defgroup monitor Monitor
 *
 * The monitor system is the way to report out-of-band (namely not over SCSI) data as statistics,
 * current configurations state. It is also used to change the state of the system, e.g. by
 * creating new groups or targets.
 *
 * The monitor system is HTTP based and very output is formatted as JSON
 *
 * @sa http://en.wikipedia.org/wiki/JSON
 * @sa http://en.wikipedia.org/wiki/HTTP
 */

#define CHECK_RETURN_JSON(x, m) { if (unlikely(!(x))) { WARNING(m);std::stringstream s; s << "{\"ERROR\": \"" << m << "\"}"; return s.str();}}

/**
 * A monitor adapter request is the abstract subclass monitor implementations
 * can use for their request specific data, e.g. options.
 *
 * The monitor adapter request is an implementation of the
 * template pattern.
 *
 * @sa http://en.wikipedia.org/wiki/Template_Pattern
 *
 * \ingroup monitor
 */
class MonitorAdapterRequest {
        DISALLOW_COPY_AND_ASSIGN(MonitorAdapterRequest);

        /**
         * buffer that holds the monitor output.
         *
         * To make the development of monitor implementations easier, the complete
         * output data is generated at once. This limits the size that a monitor
         * output can have.
         */
        std::string buffer_;

        /**
         * flag that denotes if the monitor data of the request
         * has already be gathered. This prevents that the
         * monitor is executed multiple times.
         */
        bool monitor_called_;
    public:
        /**
         * Constructor
         * @return
         */
        MonitorAdapterRequest();

        /**
         * Destructor
         * @return
         */
        virtual ~MonitorAdapterRequest();

        /**
         * Returns the monitor data.
         *
         * A call should not take long as the execution of
         * monitors is currently single-threaded due to a monitor
         * system lock.
         *
         * @return
         */
        virtual std::string Monitor() = 0;

        /**
         * Callback called by MHD when the request should provide
         * monitor data.
         *
         * @param pos
         * @param buf
         * @param count
         * @return
         */
        int PerformRequest(uint64_t pos, char* buf, uint32_t count);

        /**
         * Parses the POST and GET parameter of the request.
         *
         * @param key
         * @param value
         * @return
         */
        virtual bool ParseParam(const std::string& key, const std::string& value);
};

/**
 * Abstract subclass for all monitor implementations.
 *
 * \ingroup monitor
 */
class MonitorAdapter {
        DISALLOW_COPY_AND_ASSIGN(MonitorAdapter);
    public:
        MonitorAdapter();
        virtual ~MonitorAdapter();

        virtual MonitorAdapterRequest* OpenRequest() = 0;
        virtual std::string GetContentType();
};

/**
 * A monitor request is used to process and active HTTP request to an
 * registered monitor.
 *
 * Pyton urlib Note: We have seen very strange behavior using Pythons urllib http client.
 * We got two callbacks with position 0. The skip_first_callback flag is used, to totally
 * skip the first callback and only the second and later callbacks are delivered to the monitor.
 * To get two (identical) callbacks is a real problem for monitors that change data.
 *
 * \ingroup monitor
 */
class MonitorRequest {
        DISALLOW_COPY_AND_ASSIGN(MonitorRequest);

        MonitorSystem* monitor_system_;
        MonitorAdapterRequest* request_;
        dedupv1::base::MutexLock lock_;
    public:
        MonitorRequest(MonitorSystem* monitor_system, MonitorAdapterRequest* request);
        ~MonitorRequest();
        inline MonitorAdapterRequest* request();

        static void RequestCallbackFree(void* cls);
        static ssize_t RequestCallback(void *cls, uint64_t pos, char *buf, size_t max);
        static int KeyValueIteratorCallback(void *cls, enum MHD_ValueKind kind, const char *key, const char *value);
};

MonitorAdapterRequest* MonitorRequest::request() {
    return this->request_;
}

/**
 * The monitor system is a flexible external interface for the daemon to the
 * outside world, e.g. utilities.
 *
 * The monitor system is http based.
 *
 * \ingroup monitor
 */
class MonitorSystem {
    public:
        enum monitor_state {
            MONITOR_STATE_CREATED,
            MONITOR_STATE_STARTED,
            MONITOR_STATE_STOPPED,
            MONITOR_STATE_FAILED
        };

        /**
         * Maximal length of a monitor name
         */
        static const size_t kMaxMonitorName;

        /**
         * Default TCP/IP port of the monitor.
         */
        static const int kDefaultMonitorPort;
    private:
        DISALLOW_COPY_AND_ASSIGN(MonitorSystem);


        /**
         * Statistics
         */
        class Statistics {
        public:
            Statistics();
            tbb::atomic<uint64_t> call_count_;
            dedupv1::base::Profile timing_;
        };

        /**
         * Statistics about the monitor system
         */
        Statistics stats_;

        /**
         * Vector of all configured monitor adapter instances
         */
        std::map<std::string, MonitorAdapter*> instances_;

        /**
         * number of registered monitor adapters.
         *
         * We need a separate counter as the monitor_count() method is not allowed
         * to acquire a lock. However, changing this value is only allowed with a
         * lock held.
         *
         */
        tbb::atomic<size_t> monitor_count_;

        /**
         * Reference to the MHD http server
         */
        struct MHD_Daemon* http_server_;

        /**
         * configured port to use
         */
        int port_;

        /**
         * if set, the monitor only listen on the interface of the given host.
         */
        std::string host_;

        bool port_auto_assign_;

        /**
         * State of the monitor system.
         */
        enum monitor_state state_;

        /**
         * Lock to protect the state
         */
        dedupv1::base::MutexLock lock_;

        /**
         * Panic handler of the microhttpd daemon that is called
         * in critical situations by the http server.
         *
         * @param cls
         * @param file
         * @param line
         * @param reason
         */
        static void MHDPanicHandler(void *cls, const char *file, unsigned int line, const char *reason);

        static void MHDLogHandler(void *cls, const char * fmt, va_list a);

        int DoRequestCallback(struct MHD_Connection *connection,
            const char *url,
            const char *method,
            const char *version,
            const char *upload_data,
            size_t *upload_data_size,
            void **con_cls);
    protected:
        /**
         * Finds a monitor adapter of the given type.
         *
         * The monitor lock should be held when calling this method.
         *
         * @param monitor_type
         * @return
         */
        MonitorAdapter* FindAdapter(const std::string& monitor_type);
    public:
        /**
         * Constructor.
         * @return
         */
        MonitorSystem();

        virtual ~MonitorSystem();

        /**
         * Registers a new monitor adapter under the given name.
         * The monitor system takes over the ownership over the adapter
         *
         * @param name
         * @param adapter
         * @return
         */
        bool Add(const std::string& name, MonitorAdapter* adapter);

        /**
         * Removes all adapter instances.
         * @return
         */
        bool RemoveAll();

        /**
         * Removes the monitor adapter with the given name.
         * @param name
         * @return
         */
        bool Remove(const std::string& name);

        /**
         * Starts the monitor system.
         *
         * @param start_context
         * @return
         */
        bool Start(const dedupv1::StartContext& start_context);

        /**
         * Configures the monitor system.
         *
         * Available options:
         * - port: "auto" / int
         * - host: "any" / String
         *
         * @param option_name
         * @param option
         * @return
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Stops the monitor system.
         * @param stop_context
         * @return
         */
        bool Stop(const dedupv1::StopContext& stop_context);

        /**
         * returns the current state of the monitor
         * @return
         */
        inline enum monitor_state state() const;

        /**
         * returns the currently configured port of the monitor.
         * The value should not change after the monitor as been
         * started.
         *
         * @return
         */
        inline int port() const;

        /**
         * returns the number of monitor adapters that are
         * registered at the monitor system.
         *
         * The value might change in all states.
         *
         * @return
         */
        inline uint16_t monitor_count() const;

        /**
         * get a vector with the names of all enabled monitors
         *
         * @return a vector with the names of all enabled monitors
         */
        std::vector<std::string> GetMonitorNames();

    std::string PrintTrace();

    std::string PrintProfile();

    public:
        static int AccessCallback(void *cls, const struct sockaddr *addr, socklen_t addrlen);
        static int RequestCallback(void* cls,struct MHD_Connection *connection, const char *url, const char *method, const char *version, const char *upload_data, size_t *upload_data_size, void **con_cls);

        friend class MonitorRequest;
};

MonitorSystem::monitor_state MonitorSystem::state() const {
    return this->state_;
}

int MonitorSystem::port() const{
    return this->port_;
}

uint16_t MonitorSystem::monitor_count() const {
    return this->monitor_count_;
}

}
}

#endif  // MONITOR_H__
