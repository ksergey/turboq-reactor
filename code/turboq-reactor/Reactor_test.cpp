// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <system_error>
#include <type_traits>

#include <doctest/doctest.h>

#include <turboq-reactor/Reactor.h>
#include <turboq-reactor/TcpConnection.h>
#include <turboq-reactor/UdpConnection.h>
#include <turboq-reactor/XdpConnection.h>
#if defined(TURBOQ_REACTOR_HAS_OPENSSL)
#include <turboq-reactor/TLSConnection.h>
#include <turboq-reactor/WebsocketConnection.h>
#endif

namespace turboq::reactor::testing {

TEST_SUITE("Reactor") {

#if TURBOQ_REACTOR_IO_URING
    TEST_CASE("io_uring is the default backend") {
        static_assert(std::is_same_v<Reactor<>, Reactor<IoUringBackend>>);
        static_assert(std::is_same_v<TcpConnection<>, TcpConnection<IoUringBackend>>);
#if defined(TURBOQ_REACTOR_HAS_OPENSSL)
        static_assert(std::is_same_v<WebsocketConnection<>, WebsocketConnection<IoUringBackend>>);
#endif

        Reactor reactor;
        static_assert(std::is_same_v<decltype(reactor), Reactor<IoUringBackend>>);
        Reactor configured{{.taskRunMode = TaskRunMode::Cooperative}};
        static_assert(std::is_same_v<decltype(configured), Reactor<IoUringBackend>>);

        TcpConnection tcp{reactor, {.endpoint = Endpoint{IPv4Address::loopback(), 1}}};
        static_assert(std::is_same_v<decltype(tcp), TcpConnection<IoUringBackend>>);
        REQUIRE_EQ(tcp.state(), ConnectionState::Idle);
    }
#else
    TEST_CASE("epoll is the default backend without io_uring") {
        static_assert(std::is_same_v<Reactor<>, Reactor<EpollBackend>>);
        static_assert(std::is_same_v<TcpConnection<>, TcpConnection<EpollBackend>>);

        Reactor reactor;
        static_assert(std::is_same_v<decltype(reactor), Reactor<EpollBackend>>);
        TcpConnection tcp{reactor, {.endpoint = Endpoint{IPv4Address::loopback(), 1}}};
        static_assert(std::is_same_v<decltype(tcp), TcpConnection<EpollBackend>>);
        REQUIRE_EQ(tcp.state(), ConnectionState::Idle);
    }
#endif

    TEST_CASE("epoll backend: fds from EpollOptions::maxFds on are refused") {
        REQUIRE_THROWS_AS(Reactor<EpollBackend>{{.maxFds = 0}}, std::system_error);

        // Any new socket gets an fd above 0, 1, 2.
        Reactor<EpollBackend> reactor{{.maxFds = 3}};
        UdpConnection udp{reactor, {}};
        REQUIRE_FALSE(udp.open());
    }

    TEST_CASE("connection handles own their core: movable, not copyable") {
        auto const check = []<typename Connection>(std::type_identity<Connection>) {
            static_assert(!std::is_copy_constructible_v<Connection>);
            static_assert(!std::is_copy_assignable_v<Connection>);
            static_assert(std::is_nothrow_move_constructible_v<Connection>);
            static_assert(std::is_nothrow_move_assignable_v<Connection>);
            static_assert(std::is_nothrow_destructible_v<Connection>);
        };
        check(std::type_identity<TcpConnection<EpollBackend>>{});
        check(std::type_identity<UdpConnection<EpollBackend>>{});
#if defined(TURBOQ_REACTOR_HAS_OPENSSL)
        check(std::type_identity<TLSConnection<EpollBackend>>{});
#endif
#if defined(TURBOQ_REACTOR_HAS_OPENSSL)
        check(std::type_identity<WebsocketConnection<EpollBackend>>{});
#endif
        check(std::type_identity<XdpConnection<EpollBackend>>{});

        // Moving hands the core over; the moved-from handle is empty and destroys as a no-op.
        Reactor<EpollBackend> reactor;
        UdpConnection first{reactor, {}};
        REQUIRE(first.open());
        UdpConnection second = std::move(first);
        REQUIRE_EQ(second.state(), ConnectionState::Ready);
        UdpConnection third{reactor, {}};
        third = std::move(second); // third's own core is released
        REQUIRE_EQ(third.state(), ConnectionState::Ready);
    }

    TEST_CASE("connections deduce the backend from the reactor") {
        Reactor<EpollBackend> reactor;

        TcpConnection tcp{reactor, {.endpoint = Endpoint{IPv4Address::loopback(), 1}}};
        UdpConnection udp{reactor, {}};
        static_assert(std::is_same_v<decltype(tcp), TcpConnection<EpollBackend>>);
        static_assert(std::is_same_v<decltype(udp), UdpConnection<EpollBackend>>);
#if defined(TURBOQ_REACTOR_HAS_OPENSSL)
        TLSConnection tls{reactor, {.endpoint = Endpoint{IPv4Address::loopback(), 1}}, {.serverName = "localhost"}};
        static_assert(std::is_same_v<decltype(tls), TLSConnection<EpollBackend>>);
#endif
#if defined(TURBOQ_REACTOR_HAS_OPENSSL)
        WebsocketConnection ws{reactor, {.url = "ws://localhost:1/"}};
        static_assert(std::is_same_v<decltype(ws), WebsocketConnection<EpollBackend>>);
#endif
        XdpConnection xdp{reactor, {.interface = "lo", .udpPorts = {1}}};
        static_assert(std::is_same_v<decltype(xdp), XdpConnection<EpollBackend>>);
        REQUIRE_EQ(tcp.state(), ConnectionState::Idle);
    }
}

} // namespace turboq::reactor::testing
