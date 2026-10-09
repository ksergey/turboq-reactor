// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <bit>
#include <chrono>
#include <cstring>
#include <string>
#include <string_view>

#include <doctest/doctest.h>

#include "Error.h"
#include "Reactor.h"
#include "TestBackend.h"

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

[[nodiscard]] auto asString(std::span<std::byte const> data) -> std::string {
    return {std::bit_cast<char const*>(data.data()), data.size()};
}

[[nodiscard]] auto asBytes(std::string_view data) -> std::span<std::byte const> {
    return {std::bit_cast<std::byte const*>(data.data()), data.size()};
}

/// Plain UDP socket sending to 127.0.0.1:port, the "exchange" side.
class Sender {
private:
    int fd_;
    sockaddr_in to_{};

public:
    explicit Sender(std::uint16_t port, char const* address = "127.0.0.1") {
        fd_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
        REQUIRE(fd_ >= 0);
        to_.sin_family = AF_INET;
        to_.sin_port = htons(port);
        REQUIRE_EQ(::inet_pton(AF_INET, address, &to_.sin_addr), 1);
        int loop = 1;
        ::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
    }

    ~Sender() {
        ::close(fd_);
    }

    void send(std::string_view data) const {
        REQUIRE_EQ(::sendto(fd_, data.data(), data.size(), 0, std::bit_cast<sockaddr const*>(&to_), sizeof(to_)),
            static_cast<ssize_t>(data.size()));
    }

    [[nodiscard]] auto receive() const -> std::string {
        std::array<char, 65536> buffer;
        auto const rc = ::recv(fd_, buffer.data(), buffer.size(), 0);
        REQUIRE(rc >= 0);
        return {buffer.data(), static_cast<std::size_t>(rc)};
    }

    [[nodiscard]] auto localPort() const -> std::uint16_t {
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        REQUIRE_EQ(::getsockname(fd_, std::bit_cast<sockaddr*>(&addr), &len), 0);
        return ntohs(addr.sin_port);
    }
};

[[nodiscard]] auto sourcePort(DatagramInfo const& info) -> std::uint16_t {
    REQUIRE(info.source.address.isV4());
    return info.source.port;
}

} // namespace

TEST_SUITE("UDPConnection") {

    TEST_CASE("unicast exchange between two connections") {
        for (auto const& config : kReactorConfigs) {
            CAPTURE(&config - kReactorConfigs.data());
            for (bool const directSend : {true, false}) {
                CAPTURE(directSend);
                Reactor reactor{config};

                UDPConnection server{reactor, {.local = Endpoint{IPv4Address::loopback()}}};
                REQUIRE(server.open());
                REQUIRE_EQ(server.state(), ConnectionState::Ready);
                REQUIRE_NE(server.localEndpoint().port, 0);
                REQUIRE(server.tx.prepare(1).empty()); // no destination

                UDPConnection client{
                    reactor, {.local = Endpoint{IPv4Address::loopback()},
                                 .remote = Endpoint{IPv4Address::loopback(), server.localEndpoint().port},
                                 .directSend = directSend}};
                REQUIRE(client.open());
                REQUIRE_EQ(client.state(), ConnectionState::Ready);

                // The kernel switches receive timestamping on asynchronously (a global static key
                // flipped from a workqueue, shared with every other process): datagrams arriving
                // right after the first socket asked for it may come without a timestamp.
                REQUIRE(client.tx.push(asBytes("warm-up")));
                client.tx.flush();
                REQUIRE(pollUntil(reactor, [&] {
                    if (!server.rx.empty() && server.rx.info().softwareTimestamp == Timestamp{}) {
                        server.rx.consume();
                        REQUIRE(client.tx.push(asBytes("warm-up")));
                        client.tx.flush();
                    }
                    return !server.rx.empty();
                }));
                server.rx.consume();

                REQUIRE(client.tx.push(asBytes("first")));
                REQUIRE(client.tx.push(asBytes("second")));
                client.tx.flush();
                auto buffer = client.tx.prepare(5);
                REQUIRE_EQ(buffer.size(), 5);
                std::memcpy(buffer.data(), "third", 5);
                client.tx.commit(); // sent by poll()

                REQUIRE(pollUntil(reactor, [&] {
                    return server.rx.size() == 3;
                }));
                REQUIRE_EQ(client.tx.size(), 0);
                REQUIRE_EQ(client.tx.errors(), 0);

                for (auto const* expected : {"first", "second", "third"}) {
                    REQUIRE_FALSE(server.rx.empty());
                    REQUIRE_EQ(asString(server.rx.fetch()), expected);
                    auto const& info = server.rx.info();
                    REQUIRE_EQ(sourcePort(info), client.localEndpoint().port);
                    REQUIRE(info.receiveTime != Timestamp{});
                    REQUIRE(info.softwareTimestamp != Timestamp{});
                    REQUIRE_FALSE(info.truncated);
                    server.rx.consume();
                }
                REQUIRE(server.rx.empty());
                REQUIRE(server.rx.fetch().empty());
            }
        }
    }

    TEST_CASE("timestamping can be disabled") {
        Reactor reactor;
        UDPConnection conn{reactor, {.local = Endpoint{IPv4Address::loopback()}, .timestamping = Timestamping::None}};
        REQUIRE(conn.open());
        Sender sender{conn.localEndpoint().port};
        sender.send("x");
        REQUIRE(pollUntil(reactor, [&] {
            return !conn.rx.empty();
        }));
        REQUIRE(conn.rx.info().softwareTimestamp == Timestamp{});
        REQUIRE(conn.rx.info().hardwareTimestamp == Timestamp{});
    }

    TEST_CASE("long datagrams are truncated") {
        Reactor reactor;
        UDPConnection conn{reactor, {.local = Endpoint{IPv4Address::loopback()}, .maxDatagramSize = 64}};
        REQUIRE(conn.open());
        Sender sender{conn.localEndpoint().port};
        sender.send(std::string(100, 'z'));
        REQUIRE(pollUntil(reactor, [&] {
            return !conn.rx.empty();
        }));
        REQUIRE(conn.rx.info().truncated);
        REQUIRE_EQ(conn.rx.fetch().size(), 64);
    }

    TEST_CASE("running out of buffers stalls and resumes without loss") {
        for (auto const& config : kReactorConfigs) {
            CAPTURE(&config - kReactorConfigs.data());
            Reactor reactor{config};
            UDPConnection conn{reactor, {.local = Endpoint{IPv4Address::loopback()}, .bufferCount = 8}};
            REQUIRE(conn.open());
            Sender sender{conn.localEndpoint().port};

            constexpr int kCount = 100; // fits easily into the default socket receive buffer
            for (int i = 0; i < kCount; ++i) {
                sender.send(std::to_string(i));
            }

            REQUIRE(pollUntil(reactor, [&] {
                return conn.rx.size() == 8;
            }));
            for (int i = 0; i < 10; ++i) {
                reactor.poll();
            }
            REQUIRE_EQ(conn.rx.size(), 8); // every buffer is with the user

            int expected = 0;
            REQUIRE(pollUntil(reactor, [&] {
                while (!conn.rx.empty()) {
                    REQUIRE_EQ(asString(conn.rx.fetch()), std::to_string(expected));
                    conn.rx.consume();
                    ++expected;
                }
                return expected == kCount;
            }));
            REQUIRE_EQ(conn.rx.drops(), 0);
        }
    }

    TEST_CASE("kernel drops are reported") {
        Reactor reactor;
        UDPConnection conn{
            reactor, {.local = Endpoint{IPv4Address::loopback()}, .bufferCount = 1, .socketRecvBufferSize = 4096}};
        REQUIRE(conn.open());
        Sender sender{conn.localEndpoint().port};

        // Nobody reads while these are sent into a tiny socket buffer: most get dropped by the kernel.
        for (int i = 0; i < 2000; ++i) {
            sender.send(std::string(256, 'd'));
        }
        // Drain what made it.
        REQUIRE(pollUntil(reactor, [&] {
            if (!conn.rx.empty()) {
                conn.rx.consume();
            }
            return conn.rx.empty() && reactor.poll() == 0;
        }));
        REQUIRE_EQ(conn.rx.drops(), 0); // queued before the drops happened: carries no counter

        // The counter is attached to datagrams queued after the drops.
        sender.send("after");
        REQUIRE(pollUntil(reactor, [&] {
            return !conn.rx.empty();
        }));
        REQUIRE_EQ(asString(conn.rx.fetch()), "after");
        REQUIRE_GT(conn.rx.drops(), 0);
    }

    TEST_CASE("close keeps received datagrams, open drops them") {
        Reactor reactor;
        UDPConnection conn{reactor, {.local = Endpoint{IPv4Address::loopback()}}};
        REQUIRE(conn.open());
        auto const port = conn.localEndpoint().port;
        Sender sender{port};
        sender.send("kept");
        REQUIRE(pollUntil(reactor, [&] {
            return !conn.rx.empty();
        }));

        conn.close();
        REQUIRE(pollUntil(reactor, [&] {
            return conn.state() == ConnectionState::Closed;
        }));
        REQUIRE_FALSE(conn.error());
        REQUIRE_EQ(conn.nativeHandle(), -1);
        REQUIRE_EQ(asString(conn.rx.fetch()), "kept");

        REQUIRE(conn.open());
        REQUIRE(conn.rx.empty());
        Sender sender2{conn.localEndpoint().port};
        sender2.send("fresh");
        REQUIRE(pollUntil(reactor, [&] {
            return !conn.rx.empty();
        }));
        REQUIRE_EQ(asString(conn.rx.fetch()), "fresh");
        conn.rx.consume();

        auto const again = conn.open();
        REQUIRE_FALSE(again);
        REQUIRE_EQ(again.error(), makeErrorCode(Error::InvalidState));
    }

    TEST_CASE("invalid configuration is reported") {
        Reactor reactor;
        UDPConnection notMulticast{reactor, {.group = Endpoint{IPv4Address{10, 0, 0, 1}, 30000}}};
        REQUIRE_FALSE(notMulticast.open());
        REQUIRE_EQ(notMulticast.state(), ConnectionState::Closed);
        REQUIRE_EQ(notMulticast.error(), makeErrorCode(Error::InvalidOptions));

        UDPConnection sourceWithoutGroup{reactor, {.source = IPv4Address{10, 0, 0, 1}}};
        REQUIRE_FALSE(sourceWithoutGroup.open());
        REQUIRE_EQ(sourceWithoutGroup.error(), makeErrorCode(Error::InvalidOptions));

        UDPConnection badInterface{
            reactor, {.group = Endpoint{IPv4Address{239, 1, 1, 1}, 30000}, .interface = "no-such-if0"}};
        REQUIRE_FALSE(badInterface.open());
        REQUIRE_EQ(badInterface.state(), ConnectionState::Closed);

        UDPConnection mixedFamilies{
            reactor, {.local = Endpoint{IPv6Address::loopback()}, .remote = Endpoint{IPv4Address::loopback(), 1}}};
        REQUIRE_FALSE(mixedFamilies.open());
        REQUIRE_EQ(mixedFamilies.state(), ConnectionState::Closed);
        REQUIRE_EQ(mixedFamilies.error(), makeErrorCode(Error::InvalidOptions));

        REQUIRE_THROWS_AS(UDPConnection(reactor, {.maxDatagramSize = 0}), std::system_error);
    }

    TEST_CASE("tx queue depth limits queued datagrams") {
        Reactor reactor;
        UDPConnection sink{reactor, {.local = Endpoint{IPv4Address::loopback()}}};
        REQUIRE(sink.open());
        UDPConnection conn{reactor, {.local = Endpoint{IPv4Address::loopback()},
                                        .remote = Endpoint{IPv4Address::loopback(), sink.localEndpoint().port},
                                        .txQueueDepth = 4}};
        REQUIRE(conn.open());
        for (int i = 0; i < 4; ++i) {
            REQUIRE(conn.tx.push(asBytes("q")));
        }
        REQUIRE_FALSE(conn.tx.push(asBytes("q")));
        REQUIRE_EQ(conn.tx.size(), 4);
        reactor.poll();
        REQUIRE(pollUntil(reactor, [&] {
            return sink.rx.size() == 4;
        }));
        REQUIRE(conn.tx.push(asBytes("q")));
    }

    TEST_CASE("multicast: each socket sees only its own group") {
        Reactor reactor;
        constexpr std::uint16_t kPort = kFixedPortBase + 37;
        UDPConnection lineA{reactor, {.group = Endpoint{IPv4Address{239, 255, 10, 1}, kPort}}};
        auto const openedA = lineA.open();
        UDPConnection lineB{reactor, {.group = Endpoint{IPv4Address{239, 255, 10, 2}, kPort}}};
        auto const openedB = lineB.open();
        if (!openedA || !openedB) {
            MESSAGE("multicast is not available here, skipping: ", lineA.error().message());
            return;
        }

        Sender toA{kPort, "239.255.10.1"};
        Sender toB{kPort, "239.255.10.2"};
        toA.send("A1");
        toB.send("B1");
        toA.send("A2");

        if (!pollUntil(
                reactor,
                [&] {
                    return lineA.rx.size() == 2 && lineB.rx.size() == 1;
                },
                1000ms)) {
            MESSAGE("multicast loopback does not work here, skipping");
            return;
        }
        for (int i = 0; i < 10; ++i) {
            reactor.poll();
        }
        REQUIRE_EQ(lineA.rx.size(), 2);
        REQUIRE_EQ(lineB.rx.size(), 1);
        REQUIRE_EQ(asString(lineA.rx.fetch()), "A1");
        lineA.rx.consume();
        REQUIRE_EQ(asString(lineA.rx.fetch()), "A2");
        REQUIRE_EQ(asString(lineB.rx.fetch()), "B1");
        REQUIRE_EQ(sourcePort(lineB.rx.info()), toB.localPort());
    }

    TEST_CASE("multicast: send to a group through the tx queue") {
        Reactor reactor;
        constexpr std::uint16_t kPort = kFixedPortBase + 38;
        UDPConnection receiver{reactor, {.group = Endpoint{IPv4Address{239, 255, 10, 3}, kPort}}};
        if (!receiver.open()) {
            MESSAGE("multicast is not available here, skipping: ", receiver.error().message());
            return;
        }
        UDPConnection publisher{reactor, {.remote = Endpoint{IPv4Address{239, 255, 10, 3}, kPort}}};
        REQUIRE(publisher.open());
        REQUIRE_EQ(publisher.state(), ConnectionState::Ready);
        REQUIRE(publisher.tx.push(asBytes("snapshot request")));
        publisher.tx.flush();

        if (!pollUntil(
                reactor,
                [&] {
                    return !receiver.rx.empty();
                },
                1000ms)) {
            MESSAGE("multicast loopback does not work here, skipping");
            return;
        }
        REQUIRE_EQ(asString(receiver.rx.fetch()), "snapshot request");
        REQUIRE_EQ(sourcePort(receiver.rx.info()), publisher.localEndpoint().port);
    }
}

} // namespace turboq::reactor::testing
