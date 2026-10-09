[![C++](https://img.shields.io/badge/C++-23-blue.svg)](https://isocpp.org/)
[![Platform](https://img.shields.io/badge/platform-Linux-orange)]()
[![License](https://img.shields.io/github/license/ksergey/turboq-reactor)](LICENSE)
[![CMake](https://img.shields.io/badge/build-CMake-informational.svg)](https://cmake.org)
[![CI](https://github.com/ksergey/turboq-reactor/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/ksergey/turboq-reactor/actions/workflows/build-and-test.yml)

> turboq-reactor is a single-threaded io_uring (or epoll) reactor that exposes every network connection as a
> pair of zero-copy queues: one to read from, one to write to. Built for low-latency exchange
> connectivity (TCP, multicast, kernel TLS, WebSocket), part of the [turboq](https://github.com/ksergey/turboq) family.

## Requirements

Linux 6.1+, a C++23 compiler (GCC 14+, Clang 20+), CMake 3.24+, OpenSSL 3.0+. liburing is
fetched and built via CPM like the other dependencies.
For kernel TLS: `sudo modprobe tls` (otherwise TLS runs in OpenSSL in userspace).

## Integration

```cmake
include(cmake/GetCPM.cmake)
CPMAddPackage(
    NAME turboq-reactor
    GITHUB_REPOSITORY ksergey/turboq-reactor
    GIT_TAG master
    OPTIONS "turboq_reactor_BUILD_TESTS OFF" "turboq_reactor_TOOLS OFF" "turboq_reactor_EXAMPLES OFF")

target_link_libraries(your_app PRIVATE turboq::reactor)
```

The io_uring backend is controlled by `turboq_reactor_IO_URING`: `AUTO` (default, built when
liburing is available), `ON` (required) or `OFF` (epoll only, no liburing at all; epoll becomes
the default backend). `turboq_reactor_SYSTEM_LIBURING=ON` uses the system liburing (pkg-config)
instead of building it.

## Example

```cpp
#include <print>
#include <turboq/reactor/Reactor.h>

using namespace turboq::reactor;

int main() {
    Reactor reactor;
    WebsocketConnection ws{reactor, {.url = "wss://stream.binance.com:9443/ws/btcusdt@trade"}};
    if (auto result = ws.connect(); !result) {
        std::println(stderr, "connect: {}", result.error().message());
        return 1;
    }

    while (ws.state() != ConnectionState::Closed) {
        reactor.poll();
        while (!ws.rx.empty()) {
            auto const message = ws.rx.fetch();
            std::println("{}", std::string_view{reinterpret_cast<char const*>(message.data()), message.size()});
            ws.rx.consume();
        }
    }
    std::println(stderr, "closed: {}", ws.error().message());
}
```

`TCPConnection`, `TLSConnection` and `UDPConnection` (unicast and multicast) work the same way:
`rx.fetch()` / `rx.consume()` to read, `tx.prepare()` / `tx.commit()` / `tx.flush()` to write.
[`examples/binance_market_data.cpp`](examples/binance_market_data.cpp) adds reconnection with
backoff, a liveness watchdog and graceful shutdown.
[`examples/multicast_feeds.cpp`](examples/multicast_feeds.cpp) receives several multicast groups
and prints each datagram with its NIC, kernel and user receive timestamps.

The backend is a template parameter defaulting to io_uring. For epoll declare
`Reactor<EpollBackend> reactor;` — connections deduce the backend from the reactor they are given
(`TCPConnection conn{reactor, {...}}`); as class members write `TCPConnection<>` (io_uring) or
`TCPConnection<EpollBackend>`.

## AF_XDP

`XDPConnection` takes raw Ethernet frames straight from a NIC receive queue, before the kernel
network stack: a small built-in XDP program (no libbpf needed) hands UDP to the chosen ports to an
AF_XDP socket, zero-copy where the driver supports it, everything else goes to the kernel as usual.

```cpp
Reactor reactor;
XDPConnection feed{reactor, {.interface = "eth1", .queue = 3, .udpPorts = {5001},
                             .groups = {IPv4Address{239, 1, 1, 1}}, .hardwareTimestamps = true}};
feed.open();
while (true) {
    reactor.poll();
    while (!feed.rx.empty()) {
        handleFrame(feed.rx.fetch(), feed.rx.info().hardwareTimestamp); // Ethernet header included
        feed.rx.consume();
    }
}
```

Needs root (CAP_NET_ADMIN + CAP_BPF) and the feeds steered to the queue
(`ethtool -N eth1 flow-type udp4 dst-port 5001 action 3`). See
[`examples/xdp_feeds.cpp`](examples/xdp_feeds.cpp).
