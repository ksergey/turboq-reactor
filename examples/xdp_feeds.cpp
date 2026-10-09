// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT
//
// multicast_feeds over AF_XDP: frames of the given UDP feeds are taken from a NIC receive queue
// before the kernel network stack sees them. The library hands out raw Ethernet frames; this
// example parses Ethernet / IPv4 / IPv6 / UDP itself and prints, for every datagram, the first four
// payload bytes as a little-endian uint32_t, the NIC hardware timestamp and the user receive time.
// (There is no kernel software timestamp: the kernel stack is bypassed.)
//
// Needs root (CAP_NET_ADMIN + CAP_BPF). Steer the feeds to one receive queue first, e.g.:
//
//   ethtool -N eth1 flow-type udp4 dst-port 5001 action 3
//   ethtool -N eth1 flow-type udp4 dst-port 5002 action 3
//   xdp_feeds --interface eth1 --queue 3 --feed 239.1.1.1:5001 --feed 239.1.1.2:5002 --timestamps
//
// Hardware timestamps also need RX timestamping on: hwstamp_ctl -i eth1 -r 1.

#include <arpa/inet.h>
#include <linux/if_ether.h>

#include <bit>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <format>
#include <optional>
#include <print>
#include <stdexcept>
#include <string>
#include <vector>

#include <cxxopts.hpp>

#include <turboq/reactor/Reactor.h>

namespace {

using namespace turboq::reactor;

volatile std::sig_atomic_t stopRequested = 0;

void onSignal(int) {
    stopRequested = 1;
}

[[nodiscard]] auto load16(std::span<std::byte const> data, std::size_t at) noexcept -> std::uint16_t {
    std::uint16_t value;
    std::memcpy(&value, data.data() + at, sizeof(value));
    return ntohs(value);
}

/// A UDP datagram inside an Ethernet frame.
struct Datagram {
    Endpoint source;
    Endpoint destination;
    std::span<std::byte const> payload;
};

/// Ethernet (optionally one 802.1Q tag) / IPv4 or IPv6 (no extension headers) / UDP.
[[nodiscard]] auto parseUdp(std::span<std::byte const> frame) noexcept -> std::optional<Datagram> {
    std::size_t offset = 14;
    if (frame.size() < offset) {
        return std::nullopt;
    }
    auto etherType = load16(frame, 12);
    if (etherType == ETH_P_8021Q) {
        if (frame.size() < offset + 4) {
            return std::nullopt;
        }
        etherType = load16(frame, 16);
        offset += 4;
    }

    Datagram datagram;
    if (etherType == ETH_P_IP) {
        if (frame.size() < offset + 20) {
            return std::nullopt;
        }
        auto const headerLength = (std::to_integer<std::size_t>(frame[offset]) & 0x0f) * 4;
        if (std::to_integer<int>(frame[offset + 9]) != IPPROTO_UDP || headerLength < 20 ||
            frame.size() < offset + headerLength + 8) {
            return std::nullopt;
        }
        std::array<std::uint8_t, 4> source;
        std::array<std::uint8_t, 4> destination;
        std::memcpy(source.data(), frame.data() + offset + 12, 4);
        std::memcpy(destination.data(), frame.data() + offset + 16, 4);
        datagram.source.address = IPv4Address{source};
        datagram.destination.address = IPv4Address{destination};
        offset += headerLength;
    } else if (etherType == ETH_P_IPV6) {
        if (frame.size() < offset + 40 + 8 || std::to_integer<int>(frame[offset + 6]) != IPPROTO_UDP) {
            return std::nullopt;
        }
        std::array<std::uint8_t, 16> source;
        std::array<std::uint8_t, 16> destination;
        std::memcpy(source.data(), frame.data() + offset + 8, 16);
        std::memcpy(destination.data(), frame.data() + offset + 24, 16);
        datagram.source.address = IPv6Address{source};
        datagram.destination.address = IPv6Address{destination};
        offset += 40;
    } else {
        return std::nullopt;
    }

    datagram.source.port = load16(frame, offset);
    datagram.destination.port = load16(frame, offset + 2);
    auto const udpLength = load16(frame, offset + 4);
    if (udpLength < 8 || offset + udpLength > frame.size()) {
        return std::nullopt;
    }
    datagram.payload = frame.subspan(offset + 8, udpLength - 8u); // without Ethernet padding
    return datagram;
}

/// The first four bytes as a little-endian uint32_t, none if the payload is shorter.
[[nodiscard]] auto readUint32LE(std::span<std::byte const> payload) noexcept -> std::optional<std::uint32_t> {
    if (payload.size() < sizeof(std::uint32_t)) {
        return std::nullopt;
    }
    std::uint32_t value;
    std::memcpy(&value, payload.data(), sizeof(value));
    if constexpr (std::endian::native == std::endian::big) {
        value = std::byteswap(value);
    }
    return value;
}

[[nodiscard]] auto formatTime(Timestamp time) -> std::string {
    if (time == Timestamp{}) {
        return "-";
    }
    return std::format("{:%H:%M:%S}", time);
}

} // namespace

auto main(int argc, char** argv) -> int {
    try {
        cxxopts::Options options{"xdp_feeds", "Print UDP feeds received through AF_XDP"};
        // clang-format off
        options.add_options()
            ("i,interface", "interface", cxxopts::value<std::string>())
            ("q,queue", "receive queue the feeds are steered to", cxxopts::value<unsigned>()->default_value("0"))
            ("f,feed", "multicast group and port, e.g. 239.1.1.1:5001, or :port for unicast (repeat)", cxxopts::value<std::vector<std::string>>())
            ("timestamps", "NIC hardware timestamps (XDP metadata)")
            ("zero-copy", "prefer, require or disable", cxxopts::value<std::string>()->default_value("prefer"))
            ("attach", "auto, native or generic", cxxopts::value<std::string>()->default_value("auto"))
            ("busy-poll", "busy poll with this SO_BUSY_POLL timeout in microseconds (0: interrupts)", cxxopts::value<unsigned>()->default_value("0"))
            ("h,help", "print usage");
        // clang-format on

        auto const args = options.parse(argc, argv);
        if (args.count("help") || !args.count("interface") || !args.count("feed")) {
            std::println("{}", options.help());
            return args.count("help") ? 0 : 1;
        }

        XDPOptions xdp{
            .interface = args["interface"].as<std::string>(),
            .queue = args["queue"].as<unsigned>(),
            .hardwareTimestamps = args.count("timestamps") != 0,
        };
        for (auto const& text : args["feed"].as<std::vector<std::string>>()) {
            if (text.starts_with(':')) {
                xdp.udpPorts.push_back(static_cast<std::uint16_t>(std::stoul(text.substr(1))));
                continue;
            }
            auto const feed = Endpoint::parse(text);
            if (!feed) {
                throw std::invalid_argument{"--feed: expected group:port or :port, got " + text};
            }
            xdp.udpPorts.push_back(feed->port);
            if (feed->address.isMulticast()) {
                xdp.groups.push_back(feed->address);
            }
        }
        auto const zeroCopy = args["zero-copy"].as<std::string>();
        xdp.zeroCopy = zeroCopy == "require"   ? XDPZeroCopy::Require
                       : zeroCopy == "disable" ? XDPZeroCopy::Disable
                                               : XDPZeroCopy::Prefer;
        auto const attach = args["attach"].as<std::string>();
        xdp.attachMode = attach == "native"    ? XDPAttachMode::Native
                         : attach == "generic" ? XDPAttachMode::Generic
                                               : XDPAttachMode::Auto;
        if (auto const busyPoll = args["busy-poll"].as<unsigned>(); busyPoll != 0) {
            xdp.busyPoll = std::chrono::microseconds{busyPoll};
            xdp.wakeReactor = false; // the loop below spins on rx and never sleeps
        }
        bool const busyPolling = xdp.busyPoll.has_value();

        Reactor reactor;
        XDPConnection conn{reactor, std::move(xdp)};
        if (auto result = conn.open(); !result) {
            throw std::runtime_error{std::format("open: {} ({})", result.error().message(), conn.diagnostic())};
        }
        std::println("receiving on {} queue {}: {}", conn.options().interface, conn.options().queue, conn.diagnostic());

        std::signal(SIGINT, onSignal);
        std::signal(SIGTERM, onSignal);

        std::uint64_t datagrams = 0;
        while (!stopRequested) {
            if (!busyPolling) {
                reactor.wait(std::chrono::milliseconds{100});
            }
            while (!conn.rx.empty()) {
                auto const frame = conn.rx.fetch();
                auto const info = conn.rx.info();
                if (auto const datagram = parseUdp(frame)) {
                    auto const value = readUint32LE(datagram->payload);
                    auto const nicToUser =
                        info.hardwareTimestamp != Timestamp{}
                            ? std::format(" (+{}ns)", (info.receiveTime - info.hardwareTimestamp).count())
                            : std::string{};
                    std::println("{} -> {} len={} value={} | nic {} | user {}{}", datagram->source,
                        datagram->destination, datagram->payload.size(),
                        value ? std::to_string(*value) : std::string{"-"}, formatTime(info.hardwareTimestamp),
                        formatTime(info.receiveTime), nicToUser);
                    ++datagrams;
                }
                conn.rx.consume();
            }
            if (conn.state() != ConnectionState::Ready) {
                throw std::runtime_error{"connection lost: " + conn.error().message()};
            }
        }

        auto const stats = conn.statistics();
        std::println(stderr, "datagrams {}, kernel drops: dropped {} rx queue full {} fill queue empty {}", datagrams,
            stats.dropped, stats.rxQueueFull, stats.fillQueueEmpty);
    } catch (std::exception const& e) {
        std::println(stderr, "error: {}", e.what());
        return 1;
    }
    return 0;
}
