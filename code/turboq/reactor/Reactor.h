// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <chrono>
#include <cstddef>

#include "EpollBackend.h"
#include "Error.h"
#include "IoUringBackend.h"
#include "TCPConnection.h"
#include "TLSConnection.h"
#include "UDPConnection.h"
#include "WebsocketConnection.h"

namespace turboq::reactor {

/// Single-threaded event loop owning a set of connections, on top of a Backend chosen at compile
/// time: IoUringBackend (the default) or EpollBackend.
///
/// Typical loop:
///
///   Reactor reactor;
///   TCPConnection conn{reactor, {.endpoint = {IPv4Address::loopback(), 9000}}};
///   conn.connect();
///   while (running) {
///       reactor.poll();
///       if (auto data = conn.rx.fetch(); !data.empty()) {
///           conn.rx.consume(parse(data));
///       }
///   }
///
/// The same with epoll: Reactor<EpollBackend> reactor; the connection lines stay as they are,
/// connections take the backend from the reactor they are constructed with (CTAD). Where there is
/// nothing to deduce from (class members, containers) spell it out: TCPConnection<> is io_uring,
/// TCPConnection<EpollBackend> is epoll.
///
/// The reactor never calls user code: poll() only moves data between sockets and the connections'
/// rx/tx queues and advances connection state machines.
template <typename Backend>
class Reactor {
private:
    Backend backend_;

public:
    using Options = typename Backend::Options;

    Reactor(Reactor const&) = delete;
    Reactor& operator=(Reactor const&) = delete;

    /// Throws std::system_error on error.
    explicit Reactor(Options const& options = {}) : backend_{options} {}

    /// Non-blocking iteration: send committed tx data, start queued operations, process all
    /// available completions. Returns the number of processed completions.
    auto poll() -> std::size_t {
        return backend_.poll();
    }

    /// Like poll() but blocks until at least one completion arrives or the timeout expires.
    auto wait(std::chrono::nanoseconds timeout) -> std::size_t {
        return backend_.wait(timeout);
    }

    /// Hand queued operations to the kernel now (io_uring; nothing to do for epoll).
    void submit() {
        backend_.submit();
    }

    /// Wall clock time of the last poll()/wait(), taken right before completions are processed.
    [[nodiscard]] auto now() const noexcept -> Timestamp {
        return backend_.now();
    }

    /// Connections whose handle is gone but whose memory the kernel may still use; freed by
    /// poll()/wait() as their last operations complete.
    [[nodiscard]] auto retiredCount() const noexcept -> std::size_t {
        return backend_.retiredCount();
    }

    /// Internal: what connections are built on.
    [[nodiscard]] auto backend() noexcept -> Backend& {
        return backend_;
    }
};

} // namespace turboq::reactor
