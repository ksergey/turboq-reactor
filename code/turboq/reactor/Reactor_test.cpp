// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <type_traits>

#include <doctest/doctest.h>

#include "Reactor.h"

namespace turboq::reactor::testing {

TEST_SUITE("Reactor") {

    TEST_CASE("io_uring is the default backend") {
        static_assert(std::is_same_v<Reactor<>, Reactor<IoUringBackend>>);
        static_assert(std::is_same_v<TCPConnection<>, TCPConnection<IoUringBackend>>);
        static_assert(std::is_same_v<WebsocketConnection<>, WebsocketConnection<IoUringBackend>>);

        Reactor reactor;
        static_assert(std::is_same_v<decltype(reactor), Reactor<IoUringBackend>>);
        Reactor configured{{.taskRunMode = TaskRunMode::Cooperative}};
        static_assert(std::is_same_v<decltype(configured), Reactor<IoUringBackend>>);

        TCPConnection tcp{reactor, {.endpoint = Endpoint{IPv4Address::loopback(), 1}}};
        static_assert(std::is_same_v<decltype(tcp), TCPConnection<IoUringBackend>>);
        REQUIRE_EQ(tcp.state(), ConnectionState::Idle);
    }

    TEST_CASE("connections deduce the backend from the reactor") {
        Reactor<EpollBackend> reactor;

        TCPConnection tcp{reactor, {.endpoint = Endpoint{IPv4Address::loopback(), 1}}};
        TLSConnection tls{reactor, {.endpoint = Endpoint{IPv4Address::loopback(), 1}}, {.serverName = "localhost"}};
        UDPConnection udp{reactor, {}};
        WebsocketConnection ws{reactor, {.url = "ws://localhost:1/"}};

        static_assert(std::is_same_v<decltype(tcp), TCPConnection<EpollBackend>>);
        static_assert(std::is_same_v<decltype(tls), TLSConnection<EpollBackend>>);
        static_assert(std::is_same_v<decltype(udp), UDPConnection<EpollBackend>>);
        static_assert(std::is_same_v<decltype(ws), WebsocketConnection<EpollBackend>>);
        REQUIRE_EQ(tcp.state(), ConnectionState::Idle);
    }
}

} // namespace turboq::reactor::testing
