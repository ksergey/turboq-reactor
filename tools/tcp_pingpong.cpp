// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT
//
// TCP round-trip latency: an echo server plus two clients, one built on turboq::reactor and one on
// plain non-blocking sockets (the baseline every reactor setting should be compared against).
//
//   tcp_pingpong --role server   --port 9000 --cpu 2
//   tcp_pingpong --role reactor  --port 9000 --cpu 3 --taskrun deferred
//   tcp_pingpong --role reactor  --port 9000 --cpu 3 --backend epoll
//   tcp_pingpong --role baseline --port 9000 --cpu 3

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sched.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <print>
#include <stdexcept>
#include <string>
#include <vector>

#include <cxxopts.hpp>

#include <turboq/Utils.h>
#include <turboq/reactor/Reactor.h>

namespace {

using namespace turboq::reactor;

using Clock = std::chrono::steady_clock;

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

void printReport(std::vector<std::chrono::nanoseconds>& samples) {
    if (samples.empty()) {
        std::println("no samples");
        return;
    }
    std::ranges::sort(samples);
    auto const at = [&](double q) {
        return samples[std::min(samples.size() - 1, static_cast<std::size_t>(q * static_cast<double>(samples.size())))];
    };
    std::println("round trips: {}", samples.size());
    std::println("rtt: min {} | p50 {} | p90 {} | p99 {} | p99.9 {} | p99.99 {} | max {}", samples.front(), at(0.50),
        at(0.90), at(0.99), at(0.999), at(0.9999), samples.back());
}

void runServer(std::uint16_t port, std::size_t size) {
    int const listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    int one = 1;
    ::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(listener, std::bit_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || ::listen(listener, 1) != 0) {
        throw std::runtime_error{"bind/listen failed"};
    }
    std::println("server: listening on port {}", port);

    std::vector<char> buffer(std::max<std::size_t>(size, 65536));
    while (true) {
        int const fd = ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
        if (fd < 0) {
            continue;
        }
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        std::println("server: client connected");
        while (true) {
            // Busy polling, so the server side does not add scheduler wake-up latency.
            auto const rc = ::recv(fd, buffer.data(), buffer.size(), MSG_DONTWAIT);
            if (rc == 0 || (rc < 0 && errno != EAGAIN && errno != EINTR)) {
                break;
            }
            if (rc > 0) {
                std::size_t sent = 0;
                while (sent < static_cast<std::size_t>(rc)) {
                    auto const n = ::send(fd, buffer.data() + sent, static_cast<std::size_t>(rc) - sent, MSG_NOSIGNAL);
                    if (n <= 0) {
                        break;
                    }
                    sent += static_cast<std::size_t>(n);
                }
            }
        }
        ::close(fd);
        std::println("server: client disconnected");
    }
}

template <typename Backend>
void runReactorClient(std::string const& host, std::uint16_t port, std::size_t size, std::uint64_t count,
    std::uint64_t warmup, typename Backend::Options const& reactorOptions, bool directSend) {
    Reactor<Backend> reactor{reactorOptions};
    auto endpoints = resolve(host, port);
    if (!endpoints) {
        throw std::runtime_error{"can't resolve " + host + ": " + endpoints.error().message()};
    }
    TCPConnection conn{reactor, {.endpoint = endpoints->front(), .directSend = directSend}};
    if (auto result = conn.connect(); !result) {
        throw std::runtime_error{"connect failed: " + result.error().message()};
    }
    while (conn.state() == ConnectionState::Connecting) {
        reactor.poll();
    }
    if (conn.state() != ConnectionState::Ready) {
        throw std::runtime_error{"connect failed: " + conn.error().message()};
    }

    std::vector<std::chrono::nanoseconds> samples;
    samples.reserve(count);
    for (std::uint64_t seq = 0; seq < warmup + count; ++seq) {
        auto buffer = conn.tx.prepare(size);
        std::memset(buffer.data(), 0, size);
        std::memcpy(buffer.data(), &seq, std::min(size, sizeof(seq)));
        auto const start = Clock::now();
        conn.tx.commit();
        conn.tx.flush();

        while (conn.rx.fetch().size() < size) {
            reactor.poll();
            if (conn.state() != ConnectionState::Ready) [[unlikely]] {
                throw std::runtime_error{"connection lost: " + conn.error().message()};
            }
            turboq::cpuRelax();
        }
        auto const end = Clock::now();
        conn.rx.consume(size);
        if (seq >= warmup) {
            samples.push_back(end - start);
        }
    }
    printReport(samples);
}

void runBaselineClient(
    std::string const& host, std::uint16_t port, std::size_t size, std::uint64_t count, std::uint64_t warmup) {
    int const fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        throw std::invalid_argument{"baseline client needs a numeric IPv4 address"};
    }
    if (::connect(fd, std::bit_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        throw std::runtime_error{"connect failed"};
    }

    std::vector<char> tx(size, 0);
    std::vector<char> rx(size);
    std::vector<std::chrono::nanoseconds> samples;
    samples.reserve(count);
    for (std::uint64_t seq = 0; seq < warmup + count; ++seq) {
        std::memcpy(tx.data(), &seq, std::min(size, sizeof(seq)));
        auto const start = Clock::now();
        if (::send(fd, tx.data(), size, MSG_NOSIGNAL) != static_cast<ssize_t>(size)) {
            throw std::runtime_error{"short send"};
        }
        std::size_t received = 0;
        while (received < size) {
            auto const rc = ::recv(fd, rx.data() + received, size - received, MSG_DONTWAIT);
            if (rc > 0) {
                received += static_cast<std::size_t>(rc);
            } else if (rc == 0 || (errno != EAGAIN && errno != EINTR)) {
                throw std::runtime_error{"connection lost"};
            }
        }
        auto const end = Clock::now();
        if (seq >= warmup) {
            samples.push_back(end - start);
        }
    }
    ::close(fd);
    printReport(samples);
}

} // namespace

auto main(int argc, char** argv) -> int {
    try {
        cxxopts::Options options{"tcp_pingpong", "TCP round-trip latency: turboq::reactor vs plain sockets"};
        // clang-format off
        options.add_options()
            ("role", "server, reactor or baseline", cxxopts::value<std::string>())
            ("host", "server address (client roles)", cxxopts::value<std::string>()->default_value("127.0.0.1"))
            ("p,port", "server port", cxxopts::value<std::uint16_t>()->default_value("9000"))
            ("s,size", "message size in bytes", cxxopts::value<std::size_t>()->default_value("64"))
            ("c,count", "measured round trips", cxxopts::value<std::uint64_t>()->default_value("100000"))
            ("w,warmup", "round trips before measuring", cxxopts::value<std::uint64_t>()->default_value("10000"))
            ("backend", "[reactor] io_uring or epoll", cxxopts::value<std::string>()->default_value("io_uring"))
            ("taskrun", "[reactor/io_uring] interrupt, cooperative or deferred", cxxopts::value<std::string>()->default_value("deferred"))
            ("direct-send", "[reactor] synchronous send() in flush()", cxxopts::value<bool>()->default_value("true"))
            ("cpu", "pin to this CPU (-1 = no pinning)", cxxopts::value<int>()->default_value("-1"))
            ("h,help", "print usage");
        // clang-format on
        options.parse_positional({"role"});

        auto const args = options.parse(argc, argv);
        if (args.count("help") || !args.count("role")) {
            std::println("{}", options.help());
            return args.count("help") ? 0 : 1;
        }

        auto const role = args["role"].as<std::string>();
        auto const host = args["host"].as<std::string>();
        auto const port = args["port"].as<std::uint16_t>();
        auto const size = std::max<std::size_t>(args["size"].as<std::size_t>(), 1);
        auto const count = args["count"].as<std::uint64_t>();
        auto const warmup = args["warmup"].as<std::uint64_t>();

        pinToCpu(args["cpu"].as<int>());

        if (role == "server") {
            runServer(port, size);
        } else if (role == "reactor") {
            auto const backend = args["backend"].as<std::string>();
            auto const directSend = args["direct-send"].as<bool>();
            if (backend == "io_uring") {
                runReactorClient<IoUringBackend>(host, port, size, count, warmup,
                    {.taskRunMode = parseTaskRunMode(args["taskrun"].as<std::string>())}, directSend);
            } else if (backend == "epoll") {
                runReactorClient<EpollBackend>(host, port, size, count, warmup, {}, directSend);
            } else {
                throw std::invalid_argument{"unknown backend: " + backend};
            }
        } else if (role == "baseline") {
            runBaselineClient(host, port, size, count, warmup);
        } else {
            throw std::invalid_argument{"unknown role: " + role};
        }
    } catch (std::exception const& e) {
        std::println(stderr, "error: {}", e.what());
        return 1;
    }
    return 0;
}
