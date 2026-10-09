// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

// The tests need CAP_NET_ADMIN, CAP_BPF and CAP_SYS_ADMIN (root): each test binary moves into a
// network namespace of its own, creates a veth pair there, attaches to one end and injects
// hand-made Ethernet frames into the other through an AF_PACKET socket. Without the privileges
// they are skipped.

#include <arpa/inet.h>
#include <linux/if_ether.h>
#include <linux/if_link.h>
#include <linux/if_packet.h>
#include <linux/rtnetlink.h>
#include <linux/veth.h>
#include <net/if.h>
#include <sched.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <bit>
#include <chrono>
#include <cstring>
#include <optional>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <doctest/doctest.h>

#include "Error.h"
#include "Reactor.h"
#include "TestBackend.h"
#include "detail/XdpProgram.h"

namespace turboq::reactor::testing {
namespace {

using namespace std::chrono_literals;
using detail::UniqueFd;

constexpr char const* kXdpSide = "xdp0";  // the XDPConnection attaches here
constexpr char const* kPeerSide = "xdp1"; // frames are injected here

// ---------------------------------------------------------------------------------------------
// Test network: a veth pair in a network namespace of this process.

class NetlinkRequest {
private:
    alignas(nlmsghdr) std::array<std::byte, 1024> buffer_{};
    std::vector<std::size_t> nests_;

    [[nodiscard]] auto header() noexcept -> nlmsghdr* {
        return std::bit_cast<nlmsghdr*>(buffer_.data());
    }

public:
    NetlinkRequest(std::uint16_t type, std::uint16_t flags) {
        header()->nlmsg_len = NLMSG_LENGTH(0);
        header()->nlmsg_type = type;
        header()->nlmsg_flags = flags;
    }

    void append(void const* data, std::size_t size) {
        auto const at = NLMSG_ALIGN(header()->nlmsg_len);
        REQUIRE(at + size <= buffer_.size());
        std::memcpy(buffer_.data() + at, data, size);
        header()->nlmsg_len = static_cast<std::uint32_t>(at + size);
    }

    void attribute(std::uint16_t type, void const* data, std::size_t size) {
        rtattr attr{};
        attr.rta_type = type;
        attr.rta_len = static_cast<std::uint16_t>(RTA_LENGTH(size));
        this->append(&attr, sizeof(attr));
        this->append(data, size);
    }

    void attribute(std::uint16_t type, char const* text) {
        this->attribute(type, text, std::strlen(text) + 1);
    }

    void beginNest(std::uint16_t type) {
        nests_.push_back(NLMSG_ALIGN(header()->nlmsg_len));
        this->attribute(type, nullptr, 0);
    }

    void endNest() {
        auto const at = nests_.back();
        nests_.pop_back();
        auto* const attr = std::bit_cast<rtattr*>(buffer_.data() + at);
        attr->rta_len = static_cast<std::uint16_t>(header()->nlmsg_len - at);
    }

    /// Send and wait for the acknowledgement. Returns the error (0: success).
    [[nodiscard]] auto execute() -> int {
        UniqueFd fd{::socket(AF_NETLINK, SOCK_RAW | SOCK_CLOEXEC, NETLINK_ROUTE)};
        REQUIRE(fd);
        if (::send(fd.get(), buffer_.data(), header()->nlmsg_len, 0) < 0) {
            return errno;
        }
        alignas(nlmsghdr) std::array<std::byte, 4096> reply{};
        auto const n = ::recv(fd.get(), reply.data(), reply.size(), 0);
        REQUIRE(n >= static_cast<ssize_t>(NLMSG_LENGTH(sizeof(nlmsgerr))));
        auto const* const answer = std::bit_cast<nlmsghdr const*>(reply.data());
        REQUIRE_EQ(answer->nlmsg_type, NLMSG_ERROR);
        return -static_cast<nlmsgerr const*>(NLMSG_DATA(answer))->error;
    }
};

[[nodiscard]] auto createVethPair(char const* name, char const* peer) -> int {
    NetlinkRequest request{RTM_NEWLINK, NLM_F_REQUEST | NLM_F_CREATE | NLM_F_EXCL | NLM_F_ACK};
    ifinfomsg info{};
    request.append(&info, sizeof(info));
    request.attribute(IFLA_IFNAME, name);
    request.beginNest(IFLA_LINKINFO);
    request.attribute(IFLA_INFO_KIND, "veth");
    request.beginNest(IFLA_INFO_DATA);
    request.beginNest(VETH_INFO_PEER);
    request.append(&info, sizeof(info));
    request.attribute(IFLA_IFNAME, peer);
    request.endNest();
    request.endNest();
    request.endNest();
    return request.execute();
}

void setUp(char const* name) {
    UniqueFd fd{::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0)};
    ifreq request{};
    std::strncpy(request.ifr_name, name, IFNAMSIZ - 1);
    REQUIRE_EQ(::ioctl(fd.get(), SIOCGIFFLAGS, &request), 0);
    request.ifr_flags = static_cast<short>(request.ifr_flags | IFF_UP);
    REQUIRE_EQ(::ioctl(fd.get(), SIOCSIFFLAGS, &request), 0);
}

[[nodiscard]] auto macAddress(char const* name) -> std::array<std::uint8_t, 6> {
    UniqueFd fd{::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0)};
    ifreq request{};
    std::strncpy(request.ifr_name, name, IFNAMSIZ - 1);
    REQUIRE_EQ(::ioctl(fd.get(), SIOCGIFHWADDR, &request), 0);
    std::array<std::uint8_t, 6> mac;
    std::memcpy(mac.data(), request.ifr_hwaddr.sa_data, mac.size());
    return mac;
}

/// Sets up the test network once per process. False (and a message) without the privileges.
[[nodiscard]] auto testNetwork() -> bool {
    static std::optional<bool> ready;
    if (!ready) {
        if (::unshare(CLONE_NEWNET) != 0) {
            MESSAGE("no network namespace (", std::string{std::strerror(errno)}, "): AF_XDP tests need root, skipping");
            ready = false;
        } else if (int const error = createVethPair(kXdpSide, kPeerSide); error != 0) {
            MESSAGE("can't create veth (", std::string{std::strerror(error)}, "), skipping");
            ready = false;
        } else {
            setUp(kXdpSide);
            setUp(kPeerSide);
            setUp("lo");
            ready = true;
        }
    }
    return *ready;
}

// ---------------------------------------------------------------------------------------------
// Frames

[[nodiscard]] auto checksum(std::span<std::uint8_t const> data) -> std::uint16_t {
    std::uint32_t sum = 0;
    for (std::size_t i = 0; i + 1 < data.size(); i += 2) {
        sum += static_cast<std::uint32_t>(data[i] << 8 | data[i + 1]);
    }
    while (sum >> 16) {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    return static_cast<std::uint16_t>(~sum);
}

void put16(std::vector<std::uint8_t>& frame, std::size_t at, std::uint16_t value) {
    frame[at] = static_cast<std::uint8_t>(value >> 8);
    frame[at + 1] = static_cast<std::uint8_t>(value);
}

[[nodiscard]] auto ethernet(std::uint16_t etherType) -> std::vector<std::uint8_t> {
    std::vector<std::uint8_t> frame(14);
    auto const destination = macAddress(kXdpSide);
    auto const source = macAddress(kPeerSide);
    std::memcpy(frame.data(), destination.data(), 6);
    std::memcpy(frame.data() + 6, source.data(), 6);
    put16(frame, 12, etherType);
    return frame;
}

/// IPv4 + UDP frame with `payload`. fragmentOffset in 8-byte units.
[[nodiscard]] auto udp4(std::uint16_t port, std::string_view payload, std::uint16_t fragmentOffset = 0,
    std::uint8_t protocol = IPPROTO_UDP) -> std::vector<std::uint8_t> {
    auto frame = ethernet(ETH_P_IP);
    std::size_t const ip = frame.size();
    frame.resize(ip + 20 + 8 + payload.size());
    frame[ip] = 0x45;
    put16(frame, ip + 2, static_cast<std::uint16_t>(20 + 8 + payload.size()));
    put16(frame, ip + 6, fragmentOffset);
    frame[ip + 8] = 64;
    frame[ip + 9] = protocol;
    std::array<std::uint8_t, 8> const addresses{10, 0, 0, 1, 10, 0, 0, 2};
    std::memcpy(frame.data() + ip + 12, addresses.data(), addresses.size());
    put16(frame, ip + 10, checksum({frame.data() + ip, 20}));
    std::size_t const udp = ip + 20;
    put16(frame, udp, 40000);
    put16(frame, udp + 2, port);
    put16(frame, udp + 4, static_cast<std::uint16_t>(8 + payload.size()));
    std::memcpy(frame.data() + udp + 8, payload.data(), payload.size());
    return frame;
}

[[nodiscard]] auto udp6(std::uint16_t port, std::string_view payload) -> std::vector<std::uint8_t> {
    auto frame = ethernet(ETH_P_IPV6);
    std::size_t const ip = frame.size();
    frame.resize(ip + 40 + 8 + payload.size());
    frame[ip] = 0x60;
    put16(frame, ip + 4, static_cast<std::uint16_t>(8 + payload.size()));
    frame[ip + 6] = IPPROTO_UDP;
    frame[ip + 7] = 64;
    frame[ip + 8] = 0xfd; // fd00::1 -> fd00::2
    frame[ip + 23] = 1;
    frame[ip + 24] = 0xfd;
    frame[ip + 39] = 2;
    std::size_t const udp = ip + 40;
    put16(frame, udp, 40000);
    put16(frame, udp + 2, port);
    put16(frame, udp + 4, static_cast<std::uint16_t>(8 + payload.size()));
    std::memcpy(frame.data() + udp + 8, payload.data(), payload.size());
    return frame;
}

/// Injects frames on the peer side of the veth.
class Injector {
private:
    UniqueFd fd_{::socket(AF_PACKET, SOCK_RAW | SOCK_CLOEXEC, 0)};
    sockaddr_ll address_{};

public:
    Injector() {
        REQUIRE(fd_);
        address_.sll_family = AF_PACKET;
        address_.sll_ifindex = static_cast<int>(::if_nametoindex(kPeerSide));
        address_.sll_halen = 6;
    }

    void send(std::vector<std::uint8_t> const& frame) {
        REQUIRE_EQ(::sendto(fd_.get(), frame.data(), frame.size(), 0, std::bit_cast<sockaddr const*>(&address_),
                       sizeof(address_)),
            static_cast<ssize_t>(frame.size()));
    }
};

/// Frames the kernel stack got on the XDP side (an AF_PACKET tap: sees what XDP passed, not what
/// it redirected).
class KernelTap {
private:
    UniqueFd fd_{::socket(AF_PACKET, SOCK_RAW | SOCK_CLOEXEC | SOCK_NONBLOCK, htons(ETH_P_ALL))};

public:
    KernelTap() {
        REQUIRE(fd_);
        sockaddr_ll address{};
        address.sll_family = AF_PACKET;
        address.sll_protocol = htons(ETH_P_ALL);
        address.sll_ifindex = static_cast<int>(::if_nametoindex(kXdpSide));
        REQUIRE_EQ(::bind(fd_.get(), std::bit_cast<sockaddr const*>(&address), sizeof(address)), 0);
    }

    /// Payload markers of the frames seen so far whose last bytes are `tag` + digit.
    [[nodiscard]] auto drain(std::string_view tag) -> std::vector<std::string> {
        std::vector<std::string> seen;
        std::array<char, 2048> buffer;
        while (true) {
            auto const n = ::recv(fd_.get(), buffer.data(), buffer.size(), 0);
            if (n <= 0) {
                break;
            }
            std::string_view const frame{buffer.data(), static_cast<std::size_t>(n)};
            if (frame.size() > tag.size() && frame.substr(frame.size() - tag.size() - 1, tag.size()) == tag) {
                seen.emplace_back(frame.substr(frame.size() - tag.size() - 1));
            }
        }
        return seen;
    }
};

template <typename Pred>
[[nodiscard]] auto waitUntil(Reactor& reactor, Pred pred, std::chrono::milliseconds timeout = 2000ms) -> bool {
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        reactor.wait(50ms);
    }
    return true;
}

/// The last bytes of each received frame (the markers), consumed.
[[nodiscard]] auto drain(XDPConnection& conn, std::size_t markerSize) -> std::vector<std::string> {
    std::vector<std::string> seen;
    while (!conn.rx.empty()) {
        auto const frame = conn.rx.fetch();
        REQUIRE(frame.size() >= markerSize);
        seen.emplace_back(std::bit_cast<char const*>(frame.data() + frame.size() - markerSize), markerSize);
        conn.rx.consume();
    }
    return seen;
}

[[nodiscard]] auto openOrSkip(XDPConnection& conn) -> bool {
    auto result = conn.open();
    if (!result) {
        MESSAGE("open failed: ", result.error().message(), " | ", conn.diagnostic());
    }
    return result.has_value();
}

} // namespace

TEST_SUITE("XDPConnection") {

    TEST_CASE("invalid options") {
        Reactor reactor{kReactorConfigs.front()};
        auto const make = [&](XDPOptions options) {
            XDPConnection conn{reactor, std::move(options)};
        };
        REQUIRE_THROWS(make({.interface = "", .udpPorts = {1}}));
        REQUIRE_THROWS(make({.interface = "x"}));                                       // no ports, no redirectAll
        REQUIRE_THROWS(make({.interface = "x", .udpPorts = {1}, .redirectAll = true})); // both
        REQUIRE_THROWS(make({.interface = "x", .udpPorts = {1}, .frameSize = 3000}));
        REQUIRE_THROWS(make({.interface = "x", .udpPorts = {1}, .frameSize = 1024}));
        REQUIRE_THROWS(make({.interface = "x", .udpPorts = {1}, .groups = {IPAddress{IPv4Address{10, 0, 0, 1}}}}));
        REQUIRE_NOTHROW(make({.interface = "x", .udpPorts = {1}}));

        XDPConnection missing{reactor, {.interface = "no-such-if0", .udpPorts = {1}}};
        REQUIRE_FALSE(missing.open());
        REQUIRE_EQ(missing.state(), ConnectionState::Closed);
    }

    TEST_CASE("UDP to the listed ports comes as raw frames, everything else goes to the kernel") {
        if (!testNetwork()) {
            return;
        }
        for (auto const& config : kReactorConfigs) {
            CAPTURE(&config - kReactorConfigs.data());
            Reactor reactor{config};
            XDPConnection conn{reactor, {.interface = kXdpSide, .udpPorts = {30001, 30002}, .frameCount = 64}};
            if (!openOrSkip(conn)) {
                return;
            }
            MESSAGE(conn.diagnostic());
            REQUIRE_EQ(conn.state(), ConnectionState::Ready);

            KernelTap tap;
            Injector injector;
            injector.send(udp4(30001, "#a1"));                 // taken
            injector.send(udp6(30002, "#a2"));                 // taken
            injector.send(udp4(30003, "#a3"));                 // other port
            injector.send(udp4(30001, "#a4", 0x2000));         // more-fragments flag, offset 0: taken
            injector.send(udp4(30001, "#a5", 0x0010));         // later fragment: no UDP header
            injector.send(udp4(30001, "#a6", 0, IPPROTO_TCP)); // not UDP
            injector.send(udp4(30002, "#a7"));                 // taken

            std::vector<std::string> taken;
            REQUIRE(waitUntil(reactor, [&] {
                for (auto& marker : drain(conn, 3)) {
                    taken.push_back(marker);
                }
                return taken.size() >= 4;
            }));
            REQUIRE_EQ(taken, std::vector<std::string>{"#a1", "#a2", "#a4", "#a7"});

            std::vector<std::string> passed;
            REQUIRE(waitUntil(reactor, [&] {
                for (auto& marker : tap.drain("#a")) {
                    passed.push_back(marker);
                }
                return passed.size() >= 3;
            }));
            REQUIRE_EQ(passed, std::vector<std::string>{"#a3", "#a5", "#a6"});

            conn.close();
            REQUIRE(waitUntil(reactor, [&] {
                return conn.state() == ConnectionState::Closed;
            }));
            REQUIRE_FALSE(conn.error());
        }
    }

    TEST_CASE("frame contents and metadata") {
        if (!testNetwork()) {
            return;
        }
        Reactor reactor{kReactorConfigs.front()};
        XDPConnection conn{reactor, {.interface = kXdpSide, .udpPorts = {30010}, .hardwareTimestamps = true}};
        if (!openOrSkip(conn)) {
            return;
        }
        MESSAGE(conn.diagnostic());
        Injector injector;
        auto const frame = udp4(30010, "payload#b1");
        auto const before = std::chrono::system_clock::now();
        injector.send(frame);
        REQUIRE(waitUntil(reactor, [&] {
            return !conn.rx.empty();
        }));
        auto const received = conn.rx.fetch();
        REQUIRE_EQ(received.size(), frame.size());
        REQUIRE_EQ(std::memcmp(received.data(), frame.data(), frame.size()), 0);
        auto const info = conn.rx.info();
        REQUIRE(info.receiveTime >= std::chrono::time_point_cast<std::chrono::nanoseconds>(before));
        REQUIRE(info.receiveTime <= std::chrono::system_clock::now());
        // veth has no hardware clock: the metadata path runs, the timestamp stays empty.
        REQUIRE_EQ(info.hardwareTimestamp, Timestamp{});
        conn.rx.consume();
        REQUIRE(conn.rx.empty());
        REQUIRE(conn.rx.fetch().empty());
    }

    TEST_CASE("redirectAll takes every frame") {
        if (!testNetwork()) {
            return;
        }
        Reactor reactor{kReactorConfigs.front()};
        XDPConnection conn{reactor, {.interface = kXdpSide, .redirectAll = true}};
        if (!openOrSkip(conn)) {
            return;
        }
        Injector injector;
        injector.send(udp4(1, "#c1"));
        injector.send(udp4(2, "#c2", 0, IPPROTO_TCP));
        auto arp = ethernet(ETH_P_ARP);
        arp.resize(42, 0);
        arp.insert(arp.end(), {'#', 'c', '3'});
        injector.send(arp);
        std::vector<std::string> taken;
        REQUIRE(waitUntil(reactor, [&] {
            for (auto& marker : drain(conn, 3)) {
                taken.push_back(marker);
            }
            return taken.size() >= 3;
        }));
        REQUIRE_EQ(taken, std::vector<std::string>{"#c1", "#c2", "#c3"});
    }

    TEST_CASE("Reactor::wait() wakes up when frames arrive") {
        if (!testNetwork()) {
            return;
        }
        Reactor reactor{kReactorConfigs.front()};
        XDPConnection conn{reactor, {.interface = kXdpSide, .udpPorts = {30020}}};
        if (!openOrSkip(conn)) {
            return;
        }
        REQUIRE(conn.rx.empty()); // arms the wake-up
        std::thread sender{[] {
            std::this_thread::sleep_for(100ms);
            Injector{}.send(udp4(30020, "#d1"));
        }};
        auto const start = std::chrono::steady_clock::now();
        reactor.wait(3s);
        auto const elapsed = std::chrono::steady_clock::now() - start;
        sender.join();
        CHECK(elapsed < 2s);
        REQUIRE(waitUntil(reactor, [&] {
            return !conn.rx.empty();
        }));
    }

    TEST_CASE("a full queue drops in the kernel and recovers once consumed") {
        if (!testNetwork()) {
            return;
        }
        Reactor reactor{kReactorConfigs.front()};
        XDPConnection conn{reactor, {.interface = kXdpSide, .udpPorts = {30030}, .frameCount = 64}};
        if (!openOrSkip(conn)) {
            return;
        }
        Injector injector;
        for (int i = 0; i < 100; ++i) {
            injector.send(udp4(30030, "#e1"));
        }
        REQUIRE(waitUntil(reactor, [&] {
            return conn.rx.size() == 64;
        }));
        std::this_thread::sleep_for(50ms);
        REQUIRE_EQ(conn.rx.size(), 64);
        auto const stats = conn.statistics();
        REQUIRE_GT(stats.dropped + stats.rxQueueFull + stats.fillQueueEmpty, 0);
        REQUIRE_EQ(drain(conn, 3).size(), 64);

        injector.send(udp4(30030, "#e2"));
        REQUIRE(waitUntil(reactor, [&] {
            return !conn.rx.empty();
        }));
        REQUIRE_EQ(drain(conn, 3), std::vector<std::string>{"#e2"});
    }

    TEST_CASE("close detaches the program, open attaches it again") {
        if (!testNetwork()) {
            return;
        }
        Reactor reactor{kReactorConfigs.front()};
        XDPConnection conn{reactor, {.interface = kXdpSide, .udpPorts = {30040}}};
        if (!openOrSkip(conn)) {
            return;
        }
        // One program per interface: a second connection can't attach.
        XDPConnection second{reactor, {.interface = kXdpSide, .udpPorts = {30041}}};
        REQUIRE_FALSE(second.open());
        REQUIRE_EQ(second.state(), ConnectionState::Closed);

        conn.close();
        REQUIRE(waitUntil(reactor, [&] {
            return conn.state() == ConnectionState::Closed;
        }));
        KernelTap tap;
        Injector injector;
        injector.send(udp4(30040, "#f1"));
        std::vector<std::string> passed;
        REQUIRE(waitUntil(reactor, [&] {
            for (auto& marker : tap.drain("#f")) {
                passed.push_back(marker);
            }
            return !passed.empty();
        }));
        REQUIRE_EQ(passed, std::vector<std::string>{"#f1"}); // no program: the kernel got it

        REQUIRE(conn.open());
        injector.send(udp4(30040, "#f2"));
        REQUIRE(waitUntil(reactor, [&] {
            return !conn.rx.empty();
        }));
        REQUIRE_EQ(drain(conn, 3), std::vector<std::string>{"#f2"});
    }

    TEST_CASE("modes") {
        if (!testNetwork()) {
            return;
        }
        Reactor reactor{kReactorConfigs.front()};

        // veth has no zero-copy support.
        XDPConnection zeroCopy{reactor, {.interface = kXdpSide, .udpPorts = {30050}, .zeroCopy = XDPZeroCopy::Require}};
        auto const result = zeroCopy.open();
        REQUIRE_FALSE(result);
        REQUIRE_EQ(result.error(), makeErrorCode(Error::XdpZeroCopyUnavailable));

        XDPConnection generic{reactor, {.interface = kXdpSide,
                                           .udpPorts = {30050},
                                           .groups = {IPAddress{IPv4Address{239, 255, 40, 1}}},
                                           .attachMode = XDPAttachMode::Generic,
                                           .busyPoll = std::chrono::microseconds{50}}};
        if (!openOrSkip(generic)) {
            return;
        }
        REQUIRE_FALSE(generic.nativeMode());
        REQUIRE_FALSE(generic.zeroCopy());
        Injector{}.send(udp4(30050, "#g1"));
        REQUIRE(waitUntil(reactor, [&] {
            return !generic.rx.empty();
        }));
        REQUIRE_EQ(drain(generic, 3), std::vector<std::string>{"#g1"});
    }

    TEST_CASE("connection is movable and outlives nothing") {
        if (!testNetwork()) {
            return;
        }
        auto reactor = std::make_unique<Reactor>(kReactorConfigs.back());
        std::vector<XDPConnection> connections;
        connections.emplace_back(*reactor, XDPOptions{.interface = kXdpSide, .udpPorts = {30060}});
        if (!openOrSkip(connections.front())) {
            return;
        }
        XDPConnection moved = std::move(connections.front());
        connections.clear();
        REQUIRE_EQ(moved.state(), ConnectionState::Ready);
        Injector{}.send(udp4(30060, "#h1"));
        REQUIRE(waitUntil(*reactor, [&] {
            return !moved.rx.empty();
        }));
        reactor.reset(); // the reactor goes first: the connection still cleans up
    }
}

} // namespace turboq::reactor::testing
