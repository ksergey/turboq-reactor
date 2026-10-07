// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstring>
#include <string>
#include <string_view>

#include <doctest/doctest.h>

#include "Error.h"
#include "Reactor.h"

namespace turboq::reactor::testing {
namespace {

using namespace std::chrono_literals;

constexpr std::array kTaskRunModes = {TaskRunMode::Interrupt, TaskRunMode::Cooperative, TaskRunMode::Deferred};

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
    return {reinterpret_cast<char const*>(data.data()), data.size()};
}

[[nodiscard]] auto asBytes(std::string_view data) -> std::span<std::byte const> {
    return {reinterpret_cast<std::byte const*>(data.data()), data.size()};
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
        REQUIRE_EQ(::sendto(fd_, data.data(), data.size(), 0, reinterpret_cast<sockaddr const*>(&to_), sizeof(to_)),
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
        REQUIRE_EQ(::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len), 0);
        return ntohs(addr.sin_port);
    }
};

[[nodiscard]] auto sourcePort(DatagramInfo const& info) -> std::uint16_t {
    REQUIRE(info.source != nullptr);
    REQUIRE_EQ(info.source->sa_family, AF_INET);
    return ntohs(reinterpret_cast<sockaddr_in const*>(info.source)->sin_port);
}

} // namespace

TEST_SUITE("UdpConnection") {

    TEST_CASE("unicast exchange between two connections") {
        for (auto const mode : kTaskRunModes) {
            CAPTURE(static_cast<int>(mode));
            for (bool const directSend : {true, false}) {
                CAPTURE(directSend);
                Reactor reactor{{.taskRunMode = mode}};

                UdpConnection server{reactor, {.localAddress = "127.0.0.1"}};
                REQUIRE(server.open());
                REQUIRE_EQ(server.state(), ConnectionState::Ready);
                REQUIRE_NE(server.localPort(), 0);
                REQUIRE(server.tx.prepare(1).empty()); // no destination

                UdpConnection client{reactor, {.localAddress = "127.0.0.1",
                                                  .remoteAddress = "127.0.0.1",
                                                  .remotePort = server.localPort(),
                                                  .directSend = directSend}};
                REQUIRE(client.open());
                REQUIRE_EQ(client.state(), ConnectionState::Ready);

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
                    REQUIRE_EQ(sourcePort(info), client.localPort());
                    REQUIRE_NE(info.receiveTimeNs, 0);
                    REQUIRE_NE(info.softwareTimestampNs, 0);
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
        UdpConnection conn{reactor, {.localAddress = "127.0.0.1", .timestamping = Timestamping::None}};
        REQUIRE(conn.open());
        Sender sender{conn.localPort()};
        sender.send("x");
        REQUIRE(pollUntil(reactor, [&] {
            return !conn.rx.empty();
        }));
        REQUIRE_EQ(conn.rx.info().softwareTimestampNs, 0);
        REQUIRE_EQ(conn.rx.info().hardwareTimestampNs, 0);
    }

    TEST_CASE("long datagrams are truncated") {
        Reactor reactor;
        UdpConnection conn{reactor, {.localAddress = "127.0.0.1", .maxDatagramSize = 64}};
        REQUIRE(conn.open());
        Sender sender{conn.localPort()};
        sender.send(std::string(100, 'z'));
        REQUIRE(pollUntil(reactor, [&] {
            return !conn.rx.empty();
        }));
        REQUIRE(conn.rx.info().truncated);
        REQUIRE_EQ(conn.rx.fetch().size(), 64);
    }

    TEST_CASE("running out of buffers stalls and resumes without loss") {
        for (auto const mode : kTaskRunModes) {
            CAPTURE(static_cast<int>(mode));
            Reactor reactor{{.taskRunMode = mode}};
            UdpConnection conn{reactor, {.localAddress = "127.0.0.1", .bufferCount = 8}};
            REQUIRE(conn.open());
            Sender sender{conn.localPort()};

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
        UdpConnection conn{reactor, {.localAddress = "127.0.0.1", .bufferCount = 1, .socketRecvBufferSize = 4096}};
        REQUIRE(conn.open());
        Sender sender{conn.localPort()};

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
        UdpConnection conn{reactor, {.localAddress = "127.0.0.1", .localPort = 0}};
        REQUIRE(conn.open());
        auto const port = conn.localPort();
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
        Sender sender2{conn.localPort()};
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
        UdpConnection badGroup{reactor, {.group = "not-an-address", .localPort = 30000}};
        REQUIRE_FALSE(badGroup.open());
        REQUIRE_EQ(badGroup.state(), ConnectionState::Closed);
        REQUIRE_EQ(badGroup.error(), makeErrorCode(Error::AddressResolutionFailed));

        UdpConnection badInterface{reactor, {.group = "239.1.1.1", .interface = "no-such-if0", .localPort = 30000}};
        REQUIRE_FALSE(badInterface.open());
        REQUIRE_EQ(badInterface.state(), ConnectionState::Closed);

        UdpConnection mixedFamilies{reactor, {.localAddress = "::1", .remoteAddress = "127.0.0.1", .remotePort = 1}};
        REQUIRE_FALSE(mixedFamilies.open());
        REQUIRE_EQ(mixedFamilies.state(), ConnectionState::Closed);
        REQUIRE_EQ(mixedFamilies.error(), makeErrorCode(Error::InvalidOptions));

        REQUIRE_THROWS_AS(UdpConnection(reactor, {.maxDatagramSize = 0}), std::system_error);
    }

    TEST_CASE("tx queue depth limits queued datagrams") {
        Reactor reactor;
        UdpConnection sink{reactor, {.localAddress = "127.0.0.1"}};
        REQUIRE(sink.open());
        UdpConnection conn{reactor, {.localAddress = "127.0.0.1",
                                        .remoteAddress = "127.0.0.1",
                                        .remotePort = sink.localPort(),
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
        constexpr std::uint16_t kPort = 31337;
        UdpConnection lineA{reactor, {.group = "239.255.10.1", .localPort = kPort}};
        auto const openedA = lineA.open();
        UdpConnection lineB{reactor, {.group = "239.255.10.2", .localPort = kPort}};
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
        constexpr std::uint16_t kPort = 31338;
        UdpConnection receiver{reactor, {.group = "239.255.10.3", .localPort = kPort}};
        if (!receiver.open()) {
            MESSAGE("multicast is not available here, skipping: ", receiver.error().message());
            return;
        }
        UdpConnection publisher{reactor, {.remoteAddress = "239.255.10.3", .remotePort = kPort}};
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
        REQUIRE_EQ(sourcePort(receiver.rx.info()), publisher.localPort());
    }
}

} // namespace turboq::reactor::testing
