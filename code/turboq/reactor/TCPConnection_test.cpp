// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstring>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

#include <doctest/doctest.h>

#include "Error.h"
#include "Reactor.h"
#include "TestBackend.h"

namespace turboq::reactor::testing {
namespace {

using namespace std::chrono_literals;

/// Plain blocking loopback listener, the "exchange" side of the tests.
class Listener {
private:
    int fd_{-1};
    std::uint16_t port_{0};

public:
    Listener() {
        fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        REQUIRE(fd_ >= 0);
        int one = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        REQUIRE_EQ(::bind(fd_, std::bit_cast<sockaddr*>(&addr), sizeof(addr)), 0);
        REQUIRE_EQ(::listen(fd_, 16), 0);
        socklen_t len = sizeof(addr);
        REQUIRE_EQ(::getsockname(fd_, std::bit_cast<sockaddr*>(&addr), &len), 0);
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
};

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

void writeAll(int fd, std::string_view data) {
    while (!data.empty()) {
        auto const rc = ::send(fd, data.data(), data.size(), MSG_NOSIGNAL);
        REQUIRE(rc > 0);
        data.remove_prefix(static_cast<std::size_t>(rc));
    }
}

[[nodiscard]] auto readExactly(int fd, std::size_t size) -> std::string {
    std::string result(size, '\0');
    std::size_t done = 0;
    while (done < size) {
        auto const rc = ::recv(fd, result.data() + done, size - done, 0);
        REQUIRE(rc > 0);
        done += static_cast<std::size_t>(rc);
    }
    return result;
}

[[nodiscard]] auto asString(std::span<std::byte const> data) -> std::string {
    return {std::bit_cast<char const*>(data.data()), data.size()};
}

[[nodiscard]] auto asBytes(std::string_view data) -> std::span<std::byte const> {
    return {std::bit_cast<std::byte const*>(data.data()), data.size()};
}

} // namespace

TEST_SUITE("TCPConnection") {

    TEST_CASE("connect and exchange data in both directions") {
        for (auto const& config : kReactorConfigs) {
            CAPTURE(&config - kReactorConfigs.data());
            for (bool const directSend : {true, false}) {
                CAPTURE(directSend);
                Listener listener;
                Reactor reactor{config};
                TCPConnection conn{
                    reactor, {.endpoint = {IPv4Address::loopback(), listener.port()}, .directSend = directSend}};
                REQUIRE(conn.connect());
                REQUIRE_EQ(conn.state(), ConnectionState::Connecting);

                // Written before the connection is established: must go out right after connect.
                REQUIRE(conn.tx.push(asBytes("logon;")));

                REQUIRE(pollUntil(reactor, [&] {
                    return conn.state() == ConnectionState::Ready;
                }));
                int const peer = listener.accept();
                reactor.poll(); // connect completion has started the send; make sure it is submitted

                REQUIRE_EQ(readExactly(peer, 6), "logon;");

                // tx: explicit flush
                REQUIRE(conn.tx.push(asBytes("order-1;")));
                conn.tx.flush();
                reactor.poll();
                REQUIRE_EQ(readExactly(peer, 8), "order-1;");

                // tx: commit only, sent by the next poll()
                auto buffer = conn.tx.prepare(8);
                REQUIRE_EQ(buffer.size(), 8);
                std::memcpy(buffer.data(), "order-2;", 8);
                conn.tx.commit();
                reactor.poll();
                REQUIRE_EQ(readExactly(peer, 8), "order-2;");

                // rx: partial consume keeps the tail
                writeAll(peer, "8=FIX.4.4|35=0|8=FIX");
                REQUIRE(pollUntil(reactor, [&] {
                    return conn.rx.fetch().size() == 20;
                }));
                REQUIRE(conn.rx.timestamp() != Timestamp{});
                REQUIRE_EQ(asString(conn.rx.fetch()), "8=FIX.4.4|35=0|8=FIX");
                conn.rx.consume(15);
                REQUIRE_EQ(asString(conn.rx.fetch()), "8=FIX");

                writeAll(peer, ".4.4|35=1|");
                REQUIRE(pollUntil(reactor, [&] {
                    return conn.rx.fetch().size() == 15;
                }));
                REQUIRE_EQ(asString(conn.rx.fetch()), "8=FIX.4.4|35=1|");
                conn.rx.consume();
                REQUIRE(conn.rx.empty());

                ::close(peer);
            }
        }
    }

    TEST_CASE("peer close keeps unread data") {
        for (auto const& config : kReactorConfigs) {
            CAPTURE(&config - kReactorConfigs.data());
            Listener listener;
            Reactor reactor{config};
            TCPConnection conn{reactor, {.endpoint = {IPv4Address::loopback(), listener.port()}}};
            REQUIRE(conn.connect());
            REQUIRE(pollUntil(reactor, [&] {
                return conn.state() == ConnectionState::Ready;
            }));
            int const peer = listener.accept();

            writeAll(peer, "last words");
            ::close(peer);

            REQUIRE(pollUntil(reactor, [&] {
                return conn.state() == ConnectionState::Closed;
            }));
            REQUIRE_EQ(conn.error(), makeErrorCode(Error::ClosedByPeer));
            REQUIRE_EQ(asString(conn.rx.fetch()), "last words");
            REQUIRE(conn.tx.prepare(1).empty());
        }
    }

    TEST_CASE("connection refused") {
        std::uint16_t port;
        {
            Listener listener; // grab a free port and release it
            port = listener.port();
        }
        Reactor reactor;
        TCPConnection conn{reactor, {.endpoint = {IPv4Address::loopback(), port}}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Closed;
        }));
        REQUIRE_EQ(conn.error(), makePosixErrorCode(ECONNREFUSED));
    }

    TEST_CASE("an endpoint without address or port fails synchronously") {
        Reactor reactor;
        for (auto const& endpoint : {Endpoint{}, Endpoint{IPv4Address::loopback(), 0}}) {
            TCPConnection conn{reactor, {.endpoint = endpoint}};
            auto const result = conn.connect();
            REQUIRE_FALSE(result);
            REQUIRE_EQ(result.error(), makeErrorCode(Error::InvalidAddress));
            REQUIRE_EQ(conn.state(), ConnectionState::Closed);
            REQUIRE_EQ(conn.error(), makeErrorCode(Error::InvalidAddress));
        }
    }

    TEST_CASE("IPv6 loopback") {
        int const fd = ::socket(AF_INET6, SOCK_STREAM | SOCK_CLOEXEC, 0);
        sockaddr_in6 addr{};
        addr.sin6_family = AF_INET6;
        addr.sin6_addr = in6addr_loopback;
        if (fd < 0 || ::bind(fd, std::bit_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            if (fd >= 0) {
                ::close(fd);
            }
            MESSAGE("no IPv6 loopback here, skipping");
            return;
        }
        REQUIRE_EQ(::listen(fd, 1), 0);
        socklen_t len = sizeof(addr);
        ::getsockname(fd, std::bit_cast<sockaddr*>(&addr), &len);

        Reactor reactor;
        TCPConnection conn{reactor, {.endpoint = {IPv6Address::loopback(), ntohs(addr.sin6_port)}}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Ready;
        }));
        ::close(fd);
    }

    TEST_CASE("user close and reconnect") {
        Listener listener;
        Reactor reactor;
        TCPConnection conn{reactor, {.endpoint = {IPv4Address::loopback(), listener.port()}}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Ready;
        }));
        int peer = listener.accept();

        conn.close();
        REQUIRE_EQ(conn.state(), ConnectionState::Closing);
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Closed;
        }));
        REQUIRE_FALSE(conn.error());
        REQUIRE_EQ(conn.nativeHandle(), -1);

        char byte;
        REQUIRE_EQ(::recv(peer, &byte, 1, 0), 0); // peer sees EOF
        ::close(peer);

        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Ready;
        }));
        peer = listener.accept();
        writeAll(peer, "again");
        REQUIRE(pollUntil(reactor, [&] {
            return conn.rx.fetch().size() == 5;
        }));
        REQUIRE_EQ(asString(conn.rx.fetch()), "again");
        ::close(peer);
    }

    TEST_CASE("close() sends committed data first") {
        Listener listener;
        Reactor reactor;
        TCPConnection conn{reactor, {.endpoint = {IPv4Address::loopback(), listener.port()}, .directSend = false}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Ready;
        }));
        int const peer = listener.accept();
        REQUIRE(conn.tx.push(asBytes("goodbye"))); // committed, not sent: no poll() before close()
        conn.close();
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Closed;
        }));
        REQUIRE_EQ(readExactly(peer, 7), "goodbye");
        char byte;
        REQUIRE_EQ(::recv(peer, &byte, 1, 0), 0);
        ::close(peer);
    }

    TEST_CASE("connect() is rejected while connected") {
        Listener listener;
        Reactor reactor;
        TCPConnection conn{reactor, {.endpoint = {IPv4Address::loopback(), listener.port()}}};
        REQUIRE(conn.connect());
        auto const result = conn.connect();
        REQUIRE_FALSE(result);
        REQUIRE_EQ(result.error(), makeErrorCode(Error::InvalidState));
    }

    TEST_CASE("full rx ring applies backpressure and resumes after consume") {
        Listener listener;
        Reactor reactor;
        TCPConnection conn{reactor, {.endpoint = {IPv4Address::loopback(), listener.port()}, .rxBufferSize = 4096}};
        REQUIRE(conn.connect());
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Ready;
        }));
        int const peer = listener.accept();

        // Much more than the 4 KiB ring; the excess waits in socket buffers.
        constexpr std::size_t kTotal = 256 * 1024;
        std::vector<char> payload(kTotal);
        for (std::size_t i = 0; i < kTotal; ++i) {
            payload[i] = static_cast<char>('a' + i % 26);
        }
        writeAll(peer, {payload.data(), payload.size()});

        REQUIRE(pollUntil(reactor, [&] {
            return conn.rx.fetch().size() == 4096;
        }));
        for (int i = 0; i < 10; ++i) {
            reactor.poll();
        }
        REQUIRE_EQ(conn.rx.fetch().size(), 4096); // stalled, nothing lost

        std::size_t received = 0;
        REQUIRE(pollUntil(
            reactor,
            [&] {
                auto data = conn.rx.fetch();
                // Consume in odd chunks to exercise the wrap point.
                auto const take = std::min<std::size_t>(data.size(), 1000);
                REQUIRE_EQ(std::memcmp(data.data(), payload.data() + received, take), 0);
                received += take;
                conn.rx.consume(take);
                return received == kTotal;
            },
            5000ms));
        ::close(peer);
    }

    TEST_CASE("large tx is delivered completely") {
        for (bool const directSend : {true, false}) {
            CAPTURE(directSend);
            Listener listener;
            Reactor reactor;
            TCPConnection conn{reactor, {.endpoint = {IPv4Address::loopback(), listener.port()},
                                            .txBufferSize = 1u << 20,
                                            .directSend = directSend,
                                            .socketSendBufferSize = 4096}};
            REQUIRE(conn.connect());
            REQUIRE(pollUntil(reactor, [&] {
                return conn.state() == ConnectionState::Ready;
            }));
            int const peer = listener.accept();

            constexpr std::size_t kTotal = 512 * 1024;
            auto buffer = conn.tx.prepare(kTotal);
            REQUIRE_EQ(buffer.size(), kTotal);
            for (std::size_t i = 0; i < kTotal; ++i) {
                buffer[i] = static_cast<std::byte>(i % 251);
            }
            conn.tx.commit();
            conn.tx.flush();

            std::vector<std::byte> received;
            received.reserve(kTotal);
            std::array<std::byte, 65536> chunk;
            REQUIRE(pollUntil(
                reactor,
                [&] {
                    auto const rc = ::recv(peer, chunk.data(), chunk.size(), MSG_DONTWAIT);
                    if (rc > 0) {
                        received.insert(received.end(), chunk.begin(), chunk.begin() + rc);
                    }
                    return received.size() == kTotal;
                },
                5000ms));
            for (std::size_t i = 0; i < kTotal; ++i) {
                REQUIRE_EQ(received[i], static_cast<std::byte>(i % 251));
            }
            // The last send completion may still be on its way after the peer got the bytes.
            REQUIRE(pollUntil(reactor, [&] {
                return conn.tx.size() == 0;
            }));
            ::close(peer);
        }
    }

    TEST_CASE("reactor can be created on one thread and polled on another") {
        Listener listener;
        auto reactor = std::make_unique<Reactor>(kReactorConfigs.back());
        TCPConnection conn{*reactor, {.endpoint = {IPv4Address::loopback(), listener.port()}}};
        REQUIRE(conn.connect());

        bool ready = false;
        std::thread worker{[&] {
            ready = pollUntil(*reactor, [&] {
                return conn.state() == ConnectionState::Ready;
            });
        }};
        worker.join();
        REQUIRE(ready);
    }
}

} // namespace turboq::reactor::testing
