// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT
//
// Subscribe to a UDP (multicast) feed and print per-second statistics: datagram rate, kernel drops
// and the latency from the kernel receive timestamp to the moment the reactor handed the datagram
// to user code (and from the NIC to the kernel when hardware timestamps are on).
//
//   udp_listen --group 239.1.1.1 --port 5001 --interface eth1 --cpu 3
//   udp_listen --group 239.1.1.1 --port 5001 --interface eth1 --timestamping hardware

#include <sched.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <print>
#include <stdexcept>
#include <string>
#include <vector>

#include <cxxopts.hpp>

#include <turboq/Utils.h>
#include <turboq/reactor/Reactor.h>

namespace {

using namespace turboq::reactor;

void pinToCpu(int cpu) {
    if (cpu < 0) {
        return;
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (::sched_setaffinity(0, sizeof(set), &set) != 0) {
        throw std::runtime_error{"sched_setaffinity failed"};
    }
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

[[nodiscard]] auto parseTaskRunMode(std::string const& value) -> TaskRunMode {
    if (value == "interrupt") {
        return TaskRunMode::Interrupt;
    } else if (value == "cooperative") {
        return TaskRunMode::Cooperative;
    } else if (value == "deferred") {
        return TaskRunMode::Deferred;
    }
    throw std::invalid_argument{"unknown taskrun mode: " + value};
}

/// The value of an option given on the command line, none otherwise.
template <typename T>
[[nodiscard]] auto optionalArg(cxxopts::ParseResult const& args, char const* name) -> std::optional<T> {
    if (args.count(name) == 0) {
        return std::nullopt;
    }
    return args[name].as<T>();
}

/// "p50 / p99 / max" of the samples, sorts in place.
[[nodiscard]] auto summarize(std::vector<std::chrono::nanoseconds>& samples) -> std::string {
    if (samples.empty()) {
        return "-";
    }
    std::ranges::sort(samples);
    auto const at = [&](double q) {
        return samples[std::min(samples.size() - 1, static_cast<std::size_t>(q * static_cast<double>(samples.size())))];
    };
    return std::format("{} / {} / {}", at(0.50), at(0.99), samples.back());
}

template <typename Backend>
void listen(
    typename Backend::Options const& reactorOptions, UDPOptions const& udpOptions, std::chrono::seconds duration) {
    Reactor<Backend> reactor{reactorOptions};
    UDPConnection feed{reactor, udpOptions};
    if (auto result = feed.open(); !result) {
        throw std::runtime_error{"open failed: " + result.error().message()};
    }
    std::println("listening on {}", feed.localEndpoint());

    auto const startTime = std::chrono::steady_clock::now();
    auto nextReport = startTime + std::chrono::seconds{1};

    std::uint64_t datagrams = 0;
    std::uint64_t bytes = 0;
    std::uint64_t truncated = 0;
    std::uint32_t lastDrops = 0;
    std::vector<std::chrono::nanoseconds> kernelToUser;
    std::vector<std::chrono::nanoseconds> nicToKernel;
    kernelToUser.reserve(1 << 20);
    nicToKernel.reserve(1 << 20);

    while (true) {
        reactor.poll();
        while (!feed.rx.empty()) {
            auto const& info = feed.rx.info();
            ++datagrams;
            bytes += feed.rx.fetch().size();
            truncated += info.truncated ? 1 : 0;
            if (info.softwareTimestamp != Timestamp{}) {
                kernelToUser.push_back(info.receiveTime - info.softwareTimestamp);
                // Meaningful only if the NIC clock is synchronized to CLOCK_REALTIME (phc2sys).
                if (info.hardwareTimestamp != Timestamp{}) {
                    nicToKernel.push_back(info.softwareTimestamp - info.hardwareTimestamp);
                }
            }
            feed.rx.consume();
        }

        auto const now = std::chrono::steady_clock::now();
        if (now >= nextReport) {
            auto const drops = feed.rx.drops();
            std::println("datagrams {} | bytes {} | drops +{} | truncated {} | kernel->user p50/p99/max {} | "
                         "nic->kernel {}",
                datagrams, bytes, drops - lastDrops, truncated, summarize(kernelToUser), summarize(nicToKernel));
            datagrams = bytes = truncated = 0;
            lastDrops = drops;
            kernelToUser.clear();
            nicToKernel.clear();
            nextReport += std::chrono::seconds{1};
            if (duration != duration.zero() && now - startTime >= duration) {
                break;
            }
        }
        if (feed.state() != ConnectionState::Ready) {
            throw std::runtime_error{"connection lost: " + feed.error().message()};
        }
    }
}

} // namespace

auto main(int argc, char** argv) -> int {
    try {
        cxxopts::Options options{"udp_listen", "UDP/multicast feed statistics through turboq::reactor"};
        // clang-format off
        options.add_options()
            ("g,group", "multicast group (empty for unicast)", cxxopts::value<std::string>()->default_value(""))
            ("source", "source address for source-specific multicast", cxxopts::value<std::string>()->default_value(""))
            ("i,interface", "interface to join on", cxxopts::value<std::string>())
            ("l,local", "local address for unicast", cxxopts::value<std::string>()->default_value(""))
            ("p,port", "port", cxxopts::value<std::uint16_t>())
            ("buffers", "receive buffer count", cxxopts::value<unsigned>()->default_value("4096"))
            ("rcvbuf", "SO_RCVBUF size (default: the system's)", cxxopts::value<int>())
            ("timestamping", "none, software or hardware", cxxopts::value<std::string>()->default_value("software"))
            ("backend", "io_uring or epoll", cxxopts::value<std::string>()->default_value("io_uring"))
            ("taskrun", "[io_uring] interrupt, cooperative or deferred", cxxopts::value<std::string>()->default_value("deferred"))
            ("d,duration", "seconds to run, 0 = forever", cxxopts::value<unsigned>()->default_value("0"))
            ("cpu", "pin to this CPU (-1 = no pinning)", cxxopts::value<int>()->default_value("-1"))
            ("h,help", "print usage");
        // clang-format on

        auto const args = options.parse(argc, argv);
        if (args.count("help") || !args.count("port")) {
            std::println("{}", options.help());
            return args.count("help") ? 0 : 1;
        }

        pinToCpu(args["cpu"].as<int>());

        auto const port = args["port"].as<std::uint16_t>();
        auto parseOptional = [&](char const* name) -> std::optional<IPAddress> {
            auto const text = args[name].as<std::string>();
            if (text.empty()) {
                return std::nullopt;
            }
            auto address = IPAddress::parse(text);
            if (!address) {
                throw std::invalid_argument{std::string{"--"} + name + ": not an IP address: " + text};
            }
            return *address;
        };
        auto const group = parseOptional("group");
        auto const local = parseOptional("local");

        auto const udpOptions = UDPOptions{
            .group = group.transform([port](IPAddress const& address) {
                return Endpoint{address, port};
            }),
            .source = parseOptional("source"),
            .interface = optionalArg<std::string>(args, "interface"),
            .local = Endpoint{local.value_or(IPAddress{}), port},
            .bufferCount = args["buffers"].as<unsigned>(),
            .timestamping = parseTimestamping(args["timestamping"].as<std::string>()),
            .socketRecvBufferSize = optionalArg<int>(args, "rcvbuf"),
        };
        auto const duration = std::chrono::seconds{args["duration"].as<unsigned>()};
        auto const backend = args["backend"].as<std::string>();
        if (backend == "io_uring") {
            listen<IoUringBackend>(
                {.taskRunMode = parseTaskRunMode(args["taskrun"].as<std::string>())}, udpOptions, duration);
        } else if (backend == "epoll") {
            listen<EpollBackend>({}, udpOptions, duration);
        } else {
            throw std::invalid_argument{"unknown backend: " + backend};
        }
    } catch (std::exception const& e) {
        std::println(stderr, "error: {}", e.what());
        return 1;
    }
    return 0;
}
