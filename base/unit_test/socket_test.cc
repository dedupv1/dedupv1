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
#include <gtest/gtest.h>
#include <base/socket.h>
#include <base/thread.h>
#include <base/runnable.h>
#include <test_util/log_assert.h>
#include <base/option.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <arpa/inet.h>

using dedupv1::base::Socket;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using dedupv1::base::Option;
using dedupv1::base::strutil::ToHexString;

LOGGER("SocketTest");

static Socket* socket_test_accept(Socket* s);
static bool socket_test_connect(Socket* s, int port);

class SocketTest : public testing::Test {
protected:
    USE_LOGGING_EXPECTATION();

    Socket* socket;
    Socket* client_socket;

    static int port;

    virtual void SetUp() {
        socket = NULL;
        client_socket = NULL;
        port++;
    }

    virtual void TearDown() {
        if (socket) {
            delete socket;
            socket = NULL;
        }

        if (client_socket) {
            delete client_socket;
            client_socket = NULL;
        }
    }
};

int SocketTest::port = 8112;

TEST_F(SocketTest, CreateTCP) {
    socket = new Socket();
    socket->Init(AF_INET, SOCK_STREAM, 0);
}

Socket* socket_test_accept(Socket* s) {
    Socket* cs;
    cs = s->Accept(NULL);
    return cs;
}

bool socket_test_connect(Socket* s, int port) {
    return s->Connect("localhost", port);
}

TEST_F(SocketTest, BindListenAccept) {
    socket = new Socket();
    socket->Init(AF_INET, SOCK_STREAM, 0);
    client_socket = new Socket();
    client_socket->Init(AF_INET, SOCK_STREAM, 0);

    ASSERT_TRUE(socket->Bind(port)) << "Failed to bind to port " << port;
    ASSERT_TRUE(socket->Listen(16));

    Thread<Socket*> t1(NewRunnable(socket_test_accept, socket), "accept");
    ASSERT_TRUE(t1.Start());
    sleep(1);

    Thread<bool> t2(NewRunnable(socket_test_connect, client_socket, port), "connect");
    ASSERT_TRUE(t2.Start());
    sleep(1);

    Socket* client_socket2 = NULL;
    ASSERT_TRUE(t1.Join(&client_socket2));

    bool r;
    ASSERT_TRUE(t2.Join(&r));
    ASSERT_TRUE(r);

    ASSERT_TRUE(client_socket2);
    if (client_socket2) {
        delete client_socket2;
        client_socket2 = NULL;
    }
}

TEST_F(SocketTest, GetAddress) {
    Option<sockaddr_in> addr = Socket::GetAddress("localhost");
    ASSERT_TRUE(addr.valid());

    sockaddr_in a = addr.value();
    ASSERT_EQ(ToHexString(&(a.sin_addr.s_addr), sizeof(in_addr_t)), "7f000001");

    addr = Socket::GetAddress("127.0.0.1");
    ASSERT_TRUE(addr.valid());

    a = addr.value();
    ASSERT_EQ(ToHexString(&(a.sin_addr.s_addr), sizeof(in_addr_t)), "7f000001");

    addr = Socket::GetAddress("127.0.0.1.123123");
    ASSERT_FALSE(addr.valid());
}
