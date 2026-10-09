// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT
//
// Receive several multicast groups (an A/B feed pair, say) with one reactor and print every
// datagram: the first four bytes as a little-endian uint32_t (a sequence number in most exchange
// feeds) and all three receive timestamps:
//
//   nic     NIC hardware timestamp (needs --timestamping hardware and hardware RX timestamping
//           enabled on the interface: `hwstamp_ctl -i eth1 -r 1`, CAP_NET_ADMIN)
//   kernel  kernel software timestamp, when the packet entered the network stack
//   user    Reactor::now() of the poll that handed the datagram to this program
//
// with the delays between them. The NIC clock is only comparable with the other two when it is
// synchronized to the system clock (phc2sys); otherwise its delay is meaningless.
//
//   multicast_feeds --feed 239.1.1.1:5001 --feed 239.1.1.2:5002 --interface eth1
//   multicast_feeds --feed 239.1.1.1:5001 --feed 239.1.1.2:5002 --interface eth1 --timestamping hardware

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

[[nodiscard]] auto parseTimestamping(std::string const& value) -> Timestamping {
    if (value == "none") {
        return Timestamping::None;
    } else if (value == "software") {
        return Timestamping::Software;
    } else if (value == "hardware") {
        return Timestamping::Hardware;
    }
    throw std::invalid_argument{"unknown timestamping mode: " + value};
}

/// The first four bytes as a little-endian uint32_t, none if the datagram is shorter.
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

/// "HH:MM:SS.nnnnnnnnn" (UTC), "-" if the timestamp is not available.
[[nodiscard]] auto formatTime(Timestamp time) -> std::string {
    if (time == Timestamp{}) {
        return std::string(18, ' ').replace(0, 1, "-");
    }
    return std::format("{:%H:%M:%S}", time);
}

/// " (+123ns)": the delay from the previous timestamp, empty if either is not available.
[[nodiscard]] auto formatDelay(Timestamp from, Timestamp to) -> std::string {
    if (from == Timestamp{} || to == Timestamp{}) {
        return {};
    }
    return std::format(" (+{}ns)", (to - from).count());
}

struct Feed {
    std::string name;
    UDPConnection<> connection;
    std::uint64_t datagrams = 0;
};

void print(Feed const& feed, std::span<std::byte const> payload, DatagramInfo const& info) {
    auto const value = readUint32LE(payload);
    // The kernel timestamp follows the NIC one, the user time follows the kernel one.
    auto const kernelFrom = info.hardwareTimestamp;
    auto const userFrom = info.softwareTimestamp != Timestamp{} ? info.softwareTimestamp : info.hardwareTimestamp;
    std::println("[{}] {} len={}{} value={} | nic {} | kernel {}{} | user {}{}", feed.name, info.source, payload.size(),
        info.truncated ? " truncated" : "", value ? std::to_string(*value) : std::string{"-"},
        formatTime(info.hardwareTimestamp), formatTime(info.softwareTimestamp),
        formatDelay(kernelFrom, info.softwareTimestamp), formatTime(info.receiveTime),
        formatDelay(userFrom, info.receiveTime));
}

} // namespace

auto main(int argc, char** argv) -> int {
    try {
        cxxopts::Options options{"multicast_feeds", "Print datagrams of several multicast groups with timestamps"};
        // clang-format off
        options.add_options()
            ("f,feed", "multicast group and port, e.g. 239.1.1.1:5001 (repeat for each group)", cxxopts::value<std::vector<std::string>>())
            ("i,interface", "interface to join the groups on (default: by routing table)", cxxopts::value<std::string>())
            ("source", "source address for source-specific multicast", cxxopts::value<std::string>())
            ("timestamping", "none, software or hardware", cxxopts::value<std::string>()->default_value("hardware"))
            ("busy-poll", "spin on poll() instead of sleeping in wait(): lower and steadier user latency")
            ("h,help", "print usage");
        // clang-format on

        auto const args = options.parse(argc, argv);
        if (args.count("help") || !args.count("feed")) {
            std::println("{}", options.help());
            return args.count("help") ? 0 : 1;
        }

        std::optional<std::string> interface;
        if (args.count("interface")) {
            interface = args["interface"].as<std::string>();
        }
        std::optional<IPAddress> source;
        if (args.count("source")) {
            auto const text = args["source"].as<std::string>();
            auto const address = IPAddress::parse(text);
            if (!address) {
                throw std::invalid_argument{"--source: not an IP address: " + text};
            }
            source = *address;
        }
        auto const timestamping = parseTimestamping(args["timestamping"].as<std::string>());
        auto const busyPoll = args.count("busy-poll") != 0;

        Reactor reactor;

        // Connections are movable: a vector of them is fine, nothing keeps their address.
        std::vector<Feed> feeds;
        for (auto const& text : args["feed"].as<std::vector<std::string>>()) {
            auto const group = Endpoint::parse(text);
            if (!group) {
                throw std::invalid_argument{"--feed: expected group:port, got " + text};
            }
            auto const name = std::string(1, static_cast<char>('A' + feeds.size()));
            feeds.push_back(Feed{.name = name,
                .connection = UDPConnection{reactor, {
                                                         .group = *group,
                                                         .source = source,
                                                         .interface = interface,
                                                         .timestamping = timestamping,
                                                     }}});
            if (auto result = feeds.back().connection.open(); !result) {
                throw std::runtime_error{std::format("{}: can't join: {}", text, result.error().message())};
            }
            std::println("[{}] joined {}{}", name, *group, interface ? " on " + *interface : std::string{});
        }

        std::signal(SIGINT, onSignal);
        std::signal(SIGTERM, onSignal);

        while (!stopRequested) {
            if (busyPoll) {
                reactor.poll();
            } else {
                reactor.wait(std::chrono::milliseconds{100});
            }
            for (auto& feed : feeds) {
                auto& rx = feed.connection.rx;
                while (!rx.empty()) {
                    print(feed, rx.fetch(), rx.info());
                    ++feed.datagrams;
                    rx.consume();
                }
                if (feed.connection.state() != ConnectionState::Ready) {
                    throw std::runtime_error{
                        std::format("[{}] connection lost: {}", feed.name, feed.connection.error().message())};
                }
            }
        }

        for (auto const& feed : feeds) {
            std::println(
                stderr, "[{}] datagrams {}, kernel drops {}", feed.name, feed.datagrams, feed.connection.rx.drops());
        }
    } catch (std::exception const& e) {
        std::println(stderr, "error: {}", e.what());
        return 1;
    }
    return 0;
}
