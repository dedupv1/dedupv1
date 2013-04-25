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
#include <base/socket.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <fcntl.h>

LOGGER("Socket");

using std::string;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::strutil::ToHexString;

namespace dedupv1 {
namespace base {

Socket::Socket() {
    fd = -1;
    port = 0;
}

Socket::Socket(int fd, int port, enum socket_state state) {
    this->fd = fd;
    this->port = port;
    this->state = state;
}

bool Socket::Init(int domain, int type, int protocol) {
    this->fd = socket(domain, type, protocol);
    CHECK_ERRNO(this->fd, "Failed to create socket: ");
    this->state = SOCKET_STATE_CREATED;
    return true;
}

bool Socket::Listen(int backlog) {
    CHECK_ERRNO(listen(this->fd, backlog), "Failed to listen to socket: ");
    return true;
}

Option<sockaddr_in> Socket::GetAddress(const string& host, int family) {
    struct addrinfo *ailist = NULL;
    struct addrinfo hint;

    memset(&hint, 0, sizeof(hint));
    hint.ai_flags = AI_CANONNAME;
    hint.ai_family = family;
    CHECK_ERRNO(getaddrinfo(host.c_str(), NULL, &hint, &ailist), "Failed got get address info: host " << host << ", message ");
    if (!ailist) {
        return false;
    }

    sockaddr_in addr;
    CHECK(sizeof(sockaddr_in) == ailist->ai_addrlen, "Address length mismatch");
    memcpy(&addr, ailist->ai_addr, ailist->ai_addrlen);

    freeaddrinfo(ailist);
    return make_option(addr);
}

bool Socket::Bind(const std::string& host, in_port_t port) {
    dedupv1::base::Option<sockaddr_in> addr = GetAddress(host);
    CHECK(addr.valid(), "Failed to get address: host " << host);

    sockaddr_in raw_addr = addr.value();
    raw_addr.sin_port = htons(port);

    CHECK_ERRNO(bind(this->fd, (struct sockaddr *) &(raw_addr), sizeof(raw_addr)), "Failed to bind socket: ");
    this->port = static_cast<int>(port);
    this->state = SOCKET_STATE_BIND;
    return true;
}

bool Socket::Bind(in_port_t port) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    CHECK_ERRNO(bind(this->fd, (struct sockaddr *) &addr, sizeof(addr)), "Failed to bind socket: ");
    this->port = static_cast<int>(port);
    this->state = SOCKET_STATE_BIND;
    return true;
}

bool Socket::Connect(const string& host, in_port_t port) {
    struct sockaddr_in addr;
    struct hostent *hp;

    hp = gethostbyname(host.c_str());
    CHECK(hp, "Cannot get host for " << host);
    memcpy(&addr.sin_addr, hp->h_addr_list[0], hp->h_length);
    addr.sin_port = htons(port);
    addr.sin_family = AF_INET;

    CHECK_ERRNO(connect(this->fd, (struct sockaddr *) &addr, sizeof(struct sockaddr_in)),
        "Failed to connect to socket: ");

    this->state = SOCKET_STATE_CONNECTED;
    return true;
}

Socket::~Socket() {
    if (this->state == SOCKET_STATE_BIND || this->state == SOCKET_STATE_CONNECTED) {
        if (shutdown(this->fd, 0) == -1) {
            WARNING("Failed to shutdown socket: " << strerror(errno));
        }
    }
}

Socket* Socket::Accept(struct sockaddr_in* addr) {
    socklen_t size = sizeof(sockaddr_in);
    socklen_t* psize = &size;
    int fd = -1;
    Socket* client_socket = NULL;

    CHECK_RETURN(this->fd != -1, NULL, "Socket file descriptor not set");

    if (addr == NULL) {
        psize = NULL;
    }
    fd = accept(this->fd, (struct sockaddr *) addr, psize);
    if (fd < 0) {
        ERROR("Failed to accept socket: " << strerror(errno));
        return NULL;
    }
    client_socket = new Socket(fd, 0, SOCKET_STATE_CONNECTED);
    if (!client_socket) {
        WARNING("Alloc client socket failed: " << strerror(errno));
        close(fd);
        return NULL;
    }
    return client_socket;
}

int Socket::Poll(int timeout) {
    struct pollfd pollfd;
    int err = 0;

    memset(&pollfd, 0, sizeof(pollfd));
    pollfd.fd = this->fd;
    pollfd.events = POLLIN;
    err = poll(&pollfd, 1, timeout);
    CHECK_ERRNO(err, "Failed to poll socket: ");
    return err;
}

int Socket::Read(void* buffer, size_t len) {
    return read(this->fd, buffer, len);
}

int Socket::Recv(void *buff, size_t nbytes, int flags) {
    return recv(this->fd, buff, nbytes, flags);
}

int Socket::Write(const void* buffer, size_t len) {
    return write(this->fd, buffer, len);
}

bool Socket::SetSockOpt(int level, int optname, const void *optval, socklen_t optlen) {
    int err = setsockopt(this->fd, level, optname, optval, optlen);
    CHECK_ERRNO(err, "Failed to set socket option: ");
    if (err == 0) {
        return true;
    }
    return false;
}

bool Socket::GetSockOpt(int level, int optname, void *optval, socklen_t *optlen) {
    int err = getsockopt(this->fd, level, optname,  optval, optlen);
    CHECK_ERRNO(err, "Failed to get socket option: ");
    if (err == 0) {
        return true;
    }
    return false;
}

bool Socket::SetNonblocking() {
    int flags;
    if ((flags = fcntl(fd, F_GETFL, 0)) < 0) {
        // cout << "F_GETFL error" << endl;
        return false;
    }
    flags |= O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) < 0) {
        // cout << "F_SETFL error" << endl;
        return false;
    }
    return true;
}

}
}
