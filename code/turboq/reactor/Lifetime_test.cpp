// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <doctest/doctest.h>

#include "Error.h"
#include "Reactor.h"

namespace turboq::reactor::testing {
namespace {

using namespace std::chrono_literals;

template <typename Pred>
[[nodiscard]] auto pollUntil(Reactor& reactor, Pred pred, std::chrono::milliseconds timeout = 2000ms) -> bool {
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        reactor.poll();
    }
    return true;
}

[[nodiscard]] auto asBytes(std::string_view data) -> std::span<std::byte const> {
    return {reinterpret_cast<std::byte const*>(data.data()), data.size()};
}

class Listener {
private:
    int fd_{-1};
    std::uint16_t port_{0};

public:
    Listener() {
        fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        REQUIRE(fd_ >= 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        REQUIRE_EQ(::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
        REQUIRE_EQ(::listen(fd_, 64), 0);
        socklen_t len = sizeof(addr);
        ::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
    }

    ~Listener() {
        ::close(fd_);
    }

    [[nodiscard]] auto port() const noexcept -> std::uint16_t {
        return port_;
    }

    [[nodiscard]] auto accept() const -> int {
        int const fd = ::accept4(fd_, nullptr, nullptr, SOCK_CLOEXEC);
        REQUIRE(fd >= 0);
        return fd;
    }

    [[nodiscard]] auto url() const -> std::string {
        return "ws://127.0.0.1:" + std::to_string(port_) + "/";
    }
};

/// Read on a blocking socket until EOF; returns everything received.
[[nodiscard]] auto readUntilEof(int fd) -> std::string {
    std::string result;
    char buffer[4096];
    while (true) {
        auto const rc = ::recv(fd, buffer, sizeof(buffer), 0);
        if (rc <= 0) {
            return result;
        }
        result.append(buffer, static_cast<std::size_t>(rc));
    }
}

/// Accept one connection, complete the WebSocket upgrade, return the socket.
[[nodiscard]] auto acceptWebSocket(Listener const& listener) -> int {
    int const fd = listener.accept();
    std::string request;
    char buffer[4096];
    while (request.find("\r\n\r\n") == std::string::npos) {
        auto const rc = ::recv(fd, buffer, sizeof(buffer), 0);
        REQUIRE(rc > 0);
        request.append(buffer, static_cast<std::size_t>(rc));
    }
    auto const pos = request.find("Sec-WebSocket-Key: ");
    REQUIRE(pos != std::string::npos);
    auto const key = request.substr(pos + 19, request.find("\r\n", pos) - pos - 19);
    std::string const response = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                                 "Sec-WebSocket-Accept: " +
                                 detail::computeWsAccept(key) + "\r\n\r\n";
    REQUIRE_EQ(::send(fd, response.data(), response.size(), MSG_NOSIGNAL), static_cast<ssize_t>(response.size()));
    return fd;
}

} // namespace

TEST_SUITE("Lifetime") {

    TEST_CASE("TCP connection destroyed with a receive in flight") {
        Listener listener;
        Reactor reactor;
        int peer = -1;
        {
            TcpConnection conn{reactor, {.host = "127.0.0.1", .port = listener.port()}};
            REQUIRE(conn.connect());
            REQUIRE(pollUntil(reactor, [&] {
                return conn.state() == ConnectionState::Ready;
            }));
            peer = listener.accept();
        } // recv is armed: the reactor keeps the memory until the kernel lets go
        REQUIRE(pollUntil(reactor, [&] {
            return reactor.retiredCount() == 0;
        }));
        char byte;
        REQUIRE_EQ(::recv(peer, &byte, 1, 0), 0); // the socket was closed
        ::close(peer);
    }

    TEST_CASE("TCP connection destroyed before its connect was even submitted") {
        Listener listener;
        Reactor reactor;
        {
            TcpConnection conn{reactor, {.host = "127.0.0.1", .port = listener.port()}};
            REQUIRE(conn.connect()); // connect + linked timeout queued, not submitted yet
        }
        REQUIRE_EQ(reactor.retiredCount(), 1);
        REQUIRE(pollUntil(reactor, [&] {
            return reactor.retiredCount() == 0;
        }));
    }

    TEST_CASE("never connected connection is deleted right away") {
        Reactor reactor;
        {
            TcpConnection tcp{reactor, {.host = "127.0.0.1", .port = 1}};
            UdpConnection udp{reactor, {.localAddress = "127.0.0.1"}};
        }
        REQUIRE_EQ(reactor.retiredCount(), 0);
    }

    TEST_CASE("TCP connection destroyed with committed, unsent data") {
        Listener listener;
        Reactor reactor;
        {
            TcpConnection conn{reactor, {.host = "127.0.0.1", .port = listener.port(), .directSend = false}};
            REQUIRE(conn.connect());
            REQUIRE(pollUntil(reactor, [&] {
                return conn.state() == ConnectionState::Ready;
            }));
            REQUIRE(conn.tx.push(asBytes("never sent"))); // sits in the reactor's pending-tx list
        }
        for (int i = 0; i < 20; ++i) {
            reactor.poll();
        }
        REQUIRE(pollUntil(reactor, [&] {
            return reactor.retiredCount() == 0;
        }));
    }

    TEST_CASE("UDP connection destroyed with multishot receive armed; its buffer group is reused") {
        Reactor reactor;
        for (int round = 0; round < 3; ++round) {
            CAPTURE(round);
            {
                UdpConnection conn{reactor, {.localAddress = "127.0.0.1", .bufferCount = 64}};
                REQUIRE(conn.open());
                UdpConnection sender{reactor,
                    {.localAddress = "127.0.0.1", .remoteAddress = "127.0.0.1", .remotePort = conn.localPort()}};
                REQUIRE(sender.open());
                REQUIRE(sender.tx.push(asBytes("hello")));
                sender.tx.flush();
                REQUIRE(pollUntil(reactor, [&] {
                    return !conn.rx.empty();
                }));
            }
            REQUIRE(pollUntil(reactor, [&] {
                return reactor.retiredCount() == 0;
            }));
        }
        // Buffer groups were unregistered and recycled: many sequential sockets do not run out.
        for (int i = 0; i < 100; ++i) {
            UdpConnection conn{reactor, {.localAddress = "127.0.0.1", .bufferCount = 1}};
            REQUIRE(conn.open());
            conn.close();
            REQUIRE(pollUntil(reactor, [&] {
                return conn.state() == ConnectionState::Closed;
            }));
        }
    }

    TEST_CASE("WebSocket destroyed while Ready sends Close 1001") {
        Listener listener;
        Reactor reactor;
        std::string received;
        std::thread server;
        {
            WsConnection ws{reactor, {.url = listener.url()}};
            REQUIRE(ws.connect());
            server = std::thread{[&] {
                int const fd = acceptWebSocket(listener);
                received = readUntilEof(fd);
                ::close(fd);
            }};
            REQUIRE(pollUntil(reactor, [&] {
                return ws.state() == ConnectionState::Ready;
            }));
        }
        REQUIRE(pollUntil(reactor, [&] {
            return reactor.retiredCount() == 0;
        }));
        server.join();
        // Masked Close frame: 0x88, 0x80 | 2, 4-byte key, 2 masked status bytes.
        REQUIRE_EQ(received.size(), 8);
        REQUIRE_EQ(static_cast<unsigned char>(received[0]), 0x88);
        REQUIRE_EQ(static_cast<unsigned char>(received[1]), 0x82);
        auto const status = static_cast<unsigned>(static_cast<unsigned char>(received[6] ^ received[2])) << 8 |
                            static_cast<unsigned char>(received[7] ^ received[3]);
        REQUIRE_EQ(status, 1001);
    }

    TEST_CASE("WebSocket destroyed during the upgrade") {
        Listener listener; // never answers: the upgrade timer and the receive are in flight
        Reactor reactor;
        {
            WsConnection ws{reactor, {.url = listener.url()}};
            REQUIRE(ws.connect());
            REQUIRE(pollUntil(reactor, [&] {
                return ws.state() == ConnectionState::Handshaking;
            }));
        }
        REQUIRE(pollUntil(reactor, [&] {
            return reactor.retiredCount() == 0;
        }));
    }

    TEST_CASE("connections can be moved and kept in containers") {
        Listener listener;
        Reactor reactor;
        std::vector<TcpConnection> connections;
        constexpr int kCount = 16;
        for (int i = 0; i < kCount; ++i) {
            // Reallocations move the handles while connects are in flight.
            connections.emplace_back(reactor, TcpOptions{.host = "127.0.0.1", .port = listener.port()});
            REQUIRE(connections.back().connect());
        }
        REQUIRE(pollUntil(reactor, [&] {
            return std::ranges::all_of(connections, [](auto const& c) {
                return c.state() == ConnectionState::Ready;
            });
        }));
        std::vector<int> peers;
        for (int i = 0; i < kCount; ++i) {
            peers.push_back(listener.accept());
        }

        // Every moved handle still drives its own connection.
        for (int i = 0; i < kCount; ++i) {
            auto const text = std::to_string(i);
            REQUIRE(connections[i].tx.push(asBytes(text)));
            connections[i].tx.flush();
        }
        reactor.poll();
        std::vector<std::string> got;
        for (int fd : peers) {
            char buffer[16];
            auto const rc = ::recv(fd, buffer, sizeof(buffer), 0);
            REQUIRE(rc > 0);
            got.emplace_back(buffer, static_cast<std::size_t>(rc));
        }
        std::ranges::sort(got);
        std::vector<std::string> expected;
        for (int i = 0; i < kCount; ++i) {
            expected.push_back(std::to_string(i));
        }
        std::ranges::sort(expected);
        REQUIRE_EQ(got, expected);

        // Move-assigning over a live connection releases it.
        connections[0] = std::move(connections[1]);
        REQUIRE_EQ(connections[0].state(), ConnectionState::Ready);
        connections.clear();
        REQUIRE(pollUntil(reactor, [&] {
            return reactor.retiredCount() == 0;
        }));
        for (int fd : peers) {
            ::close(fd);
        }
    }

    TEST_CASE("a user class owns its connections") {
        struct Connector {
            UdpConnection lineA;
            UdpConnection lineB;

            explicit Connector(Reactor& reactor)
                : lineA{reactor, {.localAddress = "127.0.0.1"}}, lineB{reactor, {.localAddress = "127.0.0.1"}} {
                REQUIRE(lineA.open());
                REQUIRE(lineB.open());
            }
        };
        Reactor reactor;
        std::optional<Connector> connector;
        connector.emplace(reactor);
        auto moved = std::move(*connector);
        connector.reset();
        REQUIRE_EQ(moved.lineA.state(), ConnectionState::Ready);
        REQUIRE_NE(moved.lineA.localPort(), moved.lineB.localPort());
    }

    TEST_CASE("reactor destroyed before its connections") {
        Listener listener;
        Listener wsListener;
        auto reactor = std::make_unique<Reactor>();
        TcpConnection tcp{*reactor, {.host = "127.0.0.1", .port = listener.port()}};
        UdpConnection udp{*reactor, {.localAddress = "127.0.0.1"}};
        WsConnection ws{*reactor, {.url = wsListener.url()}};
        REQUIRE(tcp.connect());
        REQUIRE(udp.open());
        REQUIRE(ws.connect());
        REQUIRE(pollUntil(*reactor, [&] {
            return tcp.state() == ConnectionState::Ready;
        }));
        int const peer = listener.accept();
        reactor.reset(); // operations in flight are cancelled with the ring
        // Destroying the connections is the only thing left to do, and it must be safe (ASan).
        {
            [[maybe_unused]] auto a = std::move(tcp);
            [[maybe_unused]] auto b = std::move(udp);
            [[maybe_unused]] auto c = std::move(ws);
        }
        char byte;
        REQUIRE_EQ(::recv(peer, &byte, 1, 0), 0);
        ::close(peer);
    }
}

} // namespace turboq::reactor::testing
