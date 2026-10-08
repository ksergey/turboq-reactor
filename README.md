[![C++](https://img.shields.io/badge/C++-23-blue.svg)](https://isocpp.org/)
[![Platform](https://img.shields.io/badge/platform-Linux-orange)]()
[![License](https://img.shields.io/github/license/ksergey/turboq-reactor)](LICENSE)
[![CMake](https://img.shields.io/badge/build-CMake-informational.svg)](https://cmake.org)
[![CI](https://github.com/ksergey/turboq-reactor/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/ksergey/turboq-reactor/actions/workflows/build-and-test.yml)

> turboq-reactor is a single-threaded io_uring (or epoll) reactor that exposes every network connection as a
> pair of zero-copy queues: one to read from, one to write to. Built for low-latency exchange
> connectivity (TCP, multicast, kernel TLS, WebSocket), part of the [turboq](https://github.com/ksergey/turboq) family.

## Requirements

Linux 6.1+, a C++23 compiler (GCC 14+, Clang 20+), CMake 3.24+, liburing 2.5+, OpenSSL 3.0+.
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

The backend is a template parameter defaulting to io_uring. For epoll declare
`Reactor<EpollBackend> reactor;` — connections deduce the backend from the reactor they are given
(`TCPConnection conn{reactor, {...}}`); as class members write `TCPConnection<>` (io_uring) or
`TCPConnection<EpollBackend>`.
