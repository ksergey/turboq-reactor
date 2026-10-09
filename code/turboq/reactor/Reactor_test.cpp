// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <type_traits>

#include <doctest/doctest.h>

#include "Reactor.h"

namespace turboq::reactor::testing {

TEST_SUITE("Reactor") {

#if TURBOQ_REACTOR_IO_URING
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
#else
    TEST_CASE("epoll is the default backend without io_uring") {
        static_assert(std::is_same_v<Reactor<>, Reactor<EpollBackend>>);
        static_assert(std::is_same_v<TCPConnection<>, TCPConnection<EpollBackend>>);

        Reactor reactor;
        static_assert(std::is_same_v<decltype(reactor), Reactor<EpollBackend>>);
        TCPConnection tcp{reactor, {.endpoint = Endpoint{IPv4Address::loopback(), 1}}};
        static_assert(std::is_same_v<decltype(tcp), TCPConnection<EpollBackend>>);
        REQUIRE_EQ(tcp.state(), ConnectionState::Idle);
    }
#endif

    TEST_CASE("connection handles own their core: movable, not copyable") {
        auto const check = []<typename Connection>(std::type_identity<Connection>) {
            static_assert(!std::is_copy_constructible_v<Connection>);
            static_assert(!std::is_copy_assignable_v<Connection>);
            static_assert(std::is_nothrow_move_constructible_v<Connection>);
            static_assert(std::is_nothrow_move_assignable_v<Connection>);
            static_assert(std::is_nothrow_destructible_v<Connection>);
        };
        check(std::type_identity<TCPConnection<EpollBackend>>{});
        check(std::type_identity<TLSConnection<EpollBackend>>{});
        check(std::type_identity<UDPConnection<EpollBackend>>{});
        check(std::type_identity<WebsocketConnection<EpollBackend>>{});
        check(std::type_identity<XDPConnection<EpollBackend>>{});

        // Moving hands the core over; the moved-from handle is empty and destroys as a no-op.
        Reactor<EpollBackend> reactor;
        UDPConnection first{reactor, {}};
        REQUIRE(first.open());
        UDPConnection second = std::move(first);
        REQUIRE_EQ(second.state(), ConnectionState::Ready);
        UDPConnection third{reactor, {}};
        third = std::move(second); // third's own core is released
        REQUIRE_EQ(third.state(), ConnectionState::Ready);
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
