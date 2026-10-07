[![C++](https://img.shields.io/badge/C++-23-blue.svg)](https://isocpp.org/)
[![Platform](https://img.shields.io/badge/platform-Linux-orange)]()
[![License](https://img.shields.io/github/license/ksergey/turboq-reactor)](LICENSE)
[![CMake](https://img.shields.io/badge/build-CMake-informational.svg)](https://cmake.org)
[![CI](https://github.com/ksergey/turboq-reactor/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/ksergey/turboq-reactor/actions/workflows/build-and-test.yml)

> turboq-reactor is a single-threaded io_uring reactor that exposes every network connection as a
> pair of zero-copy queues: one to read from, one to write to. Built for low-latency exchange
> connectivity (TCP, multicast, kernel TLS, WebSocket), part of the [turboq](https://github.com/ksergey/turboq) family.

## Status

| Component | State |
|---|---|
| Reactor core (io_uring, task-run modes, SQPOLL) | done |
| `MirroredBuffer` (double-mapped ring) | done |
| `TcpConnection` | done |
| `UdpConnection` / multicast | done |
| kernel TLS (`TcpOptions::tls`) | done |
| `WsConnection` | done |

## Model

```
            poll()                               your code
 socket ─────────────► rx ring ──── fetch() / consume(n) ────►
 socket ◄───────────── tx ring ◄─── prepare(n) / commit() ────
```

* The reactor never calls user code. `poll()` only moves bytes between sockets and the rx/tx rings
  and advances connection state machines; parsing, sequencing and arbitration stay in your code.
* `rx.fetch()` returns **all** unread bytes as one contiguous span, even across the end of the
  ring (the ring memory is mapped twice back-to-back). `rx.consume(n)` releases only what your
  parser used; the tail stays and new data is appended to it.
* `tx.prepare(n)` gives a contiguous span inside the tx ring to serialize into, `tx.commit()`
  queues it, the next `poll()` sends it. `tx.flush()` sends right away (by default with a
  synchronous non-blocking `send()`, falling back to io_uring for what does not fit).
* A full rx ring stops reading from the socket: TCP flow control pushes back on the peer, nothing
  is dropped.
* After a disconnect, unread rx data stays available until `connect()` is called again.

## Quick Start

### Dependencies

- Linux 6.1+ (`TaskRunMode::Deferred` needs `IORING_SETUP_DEFER_TASKRUN`)
- C++23 compiler (GCC 14+, Clang 20+)
- CMake 3.24+, pkg-config
- [liburing](https://github.com/axboe/liburing) 2.5+ (`apt install liburing-dev`)
- OpenSSL 3.0+ built with ktls (`apt install libssl-dev`; Debian/Ubuntu packages are) and the `tls`
  kernel module (`modprobe tls`) for TLS connections

Fetched automatically via [CPM.cmake](https://github.com/cpm-cmake/CPM.cmake):
[turboq](https://github.com/ksergey/turboq) (utilities),
[doctest](https://github.com/doctest/doctest) (tests),
[cxxopts](https://github.com/jarro2783/cxxopts) (tools).

### CMake options

| Option | Default | Effect |
|---|---|---|
| `turboq_reactor_BUILD_TESTS` | `ON` | Build `*_test.cpp` files as doctest executables and register them with `ctest`. |
| `turboq_reactor_TOOLS` | `ON` | Build tools under [`tools/`](tools/). |
| `turboq_reactor_EXAMPLES` | `ON` | Build examples under [`examples/`](examples/). |
| `turboq_reactor_SANITIZER` | `OFF` | Build with ASan/UBSan/LeakSanitizer. |

```bash
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build/code --output-on-failure
```

### Integration

```cmake
include(cmake/GetCPM.cmake)
CPMAddPackage(
    NAME turboq-reactor
    GITHUB_REPOSITORY ksergey/turboq-reactor
    GIT_TAG master
    OPTIONS "turboq_reactor_BUILD_TESTS OFF" "turboq_reactor_TOOLS OFF")

target_link_libraries(your_app PRIVATE turboq::reactor)
```

### Usage

```cpp
#include <turboq/reactor/Reactor.h>

using namespace turboq::reactor;

Reactor reactor{{.taskRunMode = TaskRunMode::Deferred}};
TcpConnection fix{reactor, {.host = "10.0.0.1", .port = 9000}};
fix.connect();

// Allowed before the connection is up: goes out right after connect completes.
auto buffer = fix.tx.prepare(256);
fix.tx.commit(encodeLogon(buffer));

while (running) {
    reactor.poll();

    if (auto data = fix.rx.fetch(); !data.empty()) {
        fix.rx.consume(parseFix(data)); // parser returns the number of bytes it fully consumed
    }

    if (fix.state() == ConnectionState::Closed) {
        handleDisconnect(fix.error());   // rx data received before the disconnect was read above
        fix.connect();                   // reconnect policy is yours
    }
}
```

## TLS

TLS is an option of a TCP connection. OpenSSL performs the handshake (driven by io_uring poll
requests, with its own timeout), then the kernel takes over both directions (kTLS): `rx`/`tx`
carry plaintext and the data path is byte for byte the plain TCP one, including direct `send()` in
`flush()`.

```cpp
TcpConnection ws{reactor, {.host = "stream.example.com", .port = 443,
                           .tls = {.enabled = true}}};       // SNI + verification against host
ws.connect();
// state(): Connecting -> Handshaking -> Ready
```

* Only ciphers the kernel implements are offered: AES-128/256-GCM and ChaCha20-Poly1305, TLS 1.2
  and 1.3. Renegotiation, compression and session resumption are off.
* OpenSSL older than 3.2 enables kTLS for TLS 1.3 in the send direction only; the receive side
  is then installed by the library from the server traffic secret (RFC 8446 key schedule,
  verified in tests by decrypting a real OpenSSL server's records).
* Non-data records arriving after the handshake are handled by the reactor: session tickets are
  skipped, `close_notify` closes the connection with `Error::ClosedByPeer`, other alerts close it
  with a `getTlsAlertCategory()` error. `close()` sends `close_notify`.
* A TLS 1.3 KeyUpdate from the server can't be followed once the kernel owns the keys: the
  connection fails with `Error::TlsKeyUpdateUnsupported`. Exchanges rarely send them; reconnect.
* No userspace fallback: if the kernel can't take over, the connection fails with
  `Error::KernelTlsUnavailable` right after the handshake.
* Errors: `getX509ErrorCategory()` for certificate verification, `getTlsErrorCategory()` for
  OpenSSL errors.
* SIGPIPE raised by OpenSSL's writes during the handshake is blocked and swallowed.

## WebSocket

`WsConnection` is a client on top of `TcpConnection` (kernel TLS for `wss://`) with message
queues of the same shape:

```cpp
WsConnection md{reactor, {.url = "wss://stream.example.com:9443/ws/btcusdt@bookTicker",
                          .headers = {{"X-API-Key", key}}}};
md.connect();

while (running) {
    reactor.poll();
    if (md.state() == ConnectionState::Ready && !subscribed) {
        subscribed = md.tx.push(R"({"method":"SUBSCRIBE","params":["btcusdt@trade"],"id":1})");
    }
    while (!md.rx.empty()) {
        onMessage(md.rx.fetch(), md.rx.opcode(), md.rx.timestamp());   // one complete message
        md.rx.consume();
    }
}
```

* Zero copy on receive: frames are parsed in `poll()`, payloads stay where the kernel wrote them
  in the TCP rx ring. Fragmented messages are joined in place (one `memmove` per continuation
  frame), so `fetch()` always returns one contiguous message.
* `tx.prepare(n)` reserves the frame header in front of the payload; `tx.commit(n, opcode)`
  writes the header and masks the payload in place (8 bytes at a time). `WsMasking::Zero` skips
  masking entirely (RFC violation, opt-in).
* Ping is answered with Pong automatically (deferred while a `prepare()` is open, so a user's
  message is never split); a server Close is echoed and closes the connection with a
  `getWsCloseCategory()` error and `closeReason()`; `close()` sends Close and closes without
  waiting for the echo.
* `tx.prepare()` fails until the upgrade completed: subscribe when `state()` becomes `Ready`.
* No extensions (permessage-deflate would cost latency); UTF-8 of text messages is not validated.
* Limits: a message (with everything unconsumed before it) must fit into `rxBufferSize`;
  `maxQueuedMessages` bounds the message index (parsing pauses until `consume()` when full);
  `maxMessageSize` rejects larger messages with close code 1009.

## UDP and multicast

`UdpConnection` exposes datagram queues with the same shape: `rx.fetch()` returns the payload of
the oldest datagram, `rx.info()` its metadata, `rx.consume()` releases it.

```cpp
UdpConnection lineA{reactor, {.group = "239.1.1.1", .interface = "eth1", .localPort = 5001,
                              .timestamping = Timestamping::Hardware}};
UdpConnection lineB{reactor, {.group = "239.1.2.1", .interface = "eth2", .localPort = 5001}};
lineA.open();
lineB.open();

while (running) {
    reactor.poll();
    while (!lineA.rx.empty()) {
        auto const& info = lineA.rx.info();      // timestamps, sender, truncation
        arbiter.feed(LineA, lineA.rx.fetch(), info);
        lineA.rx.consume();
    }
    // same for lineB; arbitration and gap detection are yours
}
```

* Receive is a single multishot `recvmsg` with a provided buffer ring: no submission per
  datagram. `bufferCount` buffers exist per connection, and that is also the rx queue depth.
* When every buffer is queued for the user, new datagrams wait in the socket buffer and are
  dropped by the kernel when it overflows. `rx.drops()` reports the `SO_RXQ_OVFL` counter; the
  kernel attaches it to datagrams queued *after* the drops, so it updates with the next datagram.
* `IP_MULTICAST_ALL` is always turned off and the socket binds to the group address by default:
  a socket only sees its own group even if other sockets on the host joined other groups on the
  same port.
* Source-specific multicast via `source`, IPv4 and IPv6, `SO_TIMESTAMPING` software/hardware.
  Hardware RX timestamps additionally need the NIC configured (`SIOCSHWTSTAMP`, e.g.
  `hwstamp_ctl -i eth1 -r 1`).
* UDP send errors don't close the connection: the datagram is dropped and counted in
  `tx.errors()`.

`tools/udp_listen` subscribes to a feed and prints rate, drops and kernel→user latency per second.

## Task-run modes

How io_uring delivers completions decides whether `poll()` enters the kernel, which is the main
latency knob of the reactor:

| `TaskRunMode` | Syscall per `poll()` | Notes |
|---|---|---|
| `Interrupt` | only when there is something to submit | kernel interrupts the polling thread (IPI) to post completions |
| `Cooperative` | always | no IPIs, completions posted on kernel entry |
| `Deferred` (default) | always | completions processed only inside `poll()`; ring bound to the first polling thread |

There is no universally best choice; measure on your hardware with `tools/tcp_pingpong`
(`--role server` on one core, `--role reactor --taskrun ...` and `--role baseline` on another).

## Example: Binance market data

[`examples/binance_market_data.cpp`](examples/binance_market_data.cpp) streams Binance spot
market data and prints every message with its receive timestamp. It is a template for a real
connector: reconnect with exponential backoff and jitter (Binance also drops every connection
after 24 hours), a 60 s pause on HTTP 429/418, a liveness watchdog based on
`WsConnection::lastReceiveTime()` (server pings count, so quiet streams are not mistaken for dead
ones), re-subscription after every reconnect (`--live-subscribe`) and a graceful Close on
SIGINT/SIGTERM.

```bash
sudo modprobe tls
./build/examples/binance_market_data --streams btcusdt@trade,ethusdt@bookTicker
./build/examples/binance_market_data --streams btcusdt@depth@100ms --live-subscribe --busy-poll --cpu 3
```

## Ownership

Connections are ordinary objects owned by your code: create them with the reactor that will
serve them, keep them as members, in containers, move them around.

```cpp
class BinanceConnector {
    WsConnection trades_;
    WsConnection depth_;
public:
    explicit BinanceConnector(Reactor& reactor)
        : trades_{reactor, {.url = "wss://.../btcusdt@trade"}},
          depth_{reactor, {.url = "wss://.../btcusdt@depth"}} {}
};
```

* Constructors only allocate (and, for UDP, register the receive buffers); nothing starts until
  `connect()` / `open()`.
* Destruction never blocks, even with operations in flight: the connection is closed (WebSocket
  sends Close 1001, TLS sends close_notify) and handed to the reactor, which frees its memory in
  the `poll()` where the kernel's last completion for it arrives (`Reactor::retiredCount()`).
* Moving a connection keeps everything in flight; the moved-from object may only be destroyed or
  assigned to.
* The reactor must outlive its connections. If it does not, destroying the connections is still
  safe (the ring and everything in flight go away with the reactor), but nothing else is.

## Threading

A reactor and its connections belong to one thread. With `TaskRunMode::Deferred` the ring is
created disabled and bound to the thread that calls `poll()`/`wait()` first, so a reactor can be
set up on one thread and run on another. Hand data to other threads with a
[turboq](https://github.com/ksergey/turboq) queue.
