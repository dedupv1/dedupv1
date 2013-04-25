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

#ifndef SOCKET_H__
#define SOCKET_H__

#include <base/base.h>
#include <base/option.h>
#include <netinet/in.h>

#include <string>

namespace dedupv1 {
namespace base {

/**
 * A Socket is used for network programming.
 *
 * For more information, see "Advanced programming of the
 * UNIX environment".
 */
class Socket {
        /**
         * Type for the socket state
         */
        enum socket_state {
                SOCKET_STATE_CREATED, //!< SOCKET_STATE_CREATED
                SOCKET_STATE_BIND,    //!< SOCKET_STATE_BIND
                SOCKET_STATE_CONNECTED//!< SOCKET_STATE_CONNECTED
        };
        /**
         * File descriptor of the socket
         */
        int fd;

        /**
         * Port of the socket
         */
        int port;

        /**
         * State of the socket
         */
        enum socket_state state;

        /**
         * Private constructor used in
         * Accept()
         *
         * @param fd
         * @param port
         * @param state
         * @return
         */
        Socket(int fd, int port, enum socket_state state);
    public:
        /**
         * Opens a new socket.
         * @return
         */
        Socket();

        ~Socket();

        /**
        * Creates a new socket.
        * @param domain Protocol family. One of AF_INET, AF_INET6, AF_LOCAL, AF_ROUTE, AF_KEY
        * @param type Type of protocol. One of SOCK_STREAM, SOCK_DGRAM, SOCK_SEQPACKET, SOCK_RAW
        * @param protocol One of IPPROTO_TCP, IPPROTO_UDP, IPPROTO_SCTP. Often 0 is the best choice.
        *
         * @return true iff ok, otherwise an error has occurred
        */
        bool Init(int domain, int type, int protocol);

        /**
         * Listen for new incoming connections with a given backlog.
         *
         * @param backlog
         * @return true iff ok, otherwise an error has occurred
         */
        bool Listen(int backlog);

        /**
         * Binds the socket on the given port.
         * @param port
         * @return true iff ok, otherwise an error has occurred
         */
        bool Bind(in_port_t port);

        /**
         * Get the socket address for the given host
         */
        static dedupv1::base::Option<sockaddr_in> GetAddress(const std::string& host, int family = AF_INET);

        /**
         * Bind the socket to the given host.
         * @return true iff ok, otherwise an error has occurred
         */
        bool Bind(const std::string& host, in_port_t port);

        /**
         * Connects a socket to the given host and port.
         * @param host
         * @param port
         * @return true iff ok, otherwise an error has occurred
         */
        bool Connect(const std::string& host, in_port_t port);

        /**
         * Wait until new incoming connections are accepted.
         * @param addr
         * @return
         */
        Socket* Accept(struct sockaddr_in* addr);

        /**
         * Read len bytes from the socket connection.
         *
         * @param buffer
         * @param len
         * @return
         */
        int Read(void* buffer, size_t len);

		/**
		 * Read nbytes from socket connection
		 */
		int Recv(void *buff, size_t nbytes, int flags);

        /**
         * Write len bytes to the socket connection.
         *
         * @param buffer
         * @param len
         * @return
         */
        int Write(const void* buffer, size_t len);

        /**
         * Polls the socket connection for new data.
         * @param timeout
         * @return ???
         */
        int Poll(int timeout);

		/**
		 * Sets the given socket option.
         * @return true iff ok, otherwise an error has occurred
		 */
		bool SetSockOpt(int level, int optname, const void* optval, socklen_t optlen );

		/**
		 * Gets the given socket option.
         * @return true iff ok, otherwise an error has occurred
		 */
		bool GetSockOpt(int level, int optname, void* optval, socklen_t* optlen );

		/**
		 * Sets the Socket to nonblocking mode
		 * @return: True, if successful
		 */
		bool SetNonblocking();
};

}
}

#endif  // SOCKET_H__
