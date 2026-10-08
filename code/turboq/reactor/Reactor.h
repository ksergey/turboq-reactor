// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

#include "Error.h"
#include "TcpConnection.h"
#include "UdpConnection.h"
#include "WsConnection.h"
#include "detail/IoHandler.h"
#include "detail/Ring.h"

namespace turboq::reactor {

/// Single-threaded io_uring event loop owning a set of connections.
///
/// Typical loop:
///
///   Reactor reactor;
///   TcpConnection conn{reactor, {.host = "127.0.0.1", .port = 9000}};
///   conn.connect();
///   while (running) {
///       reactor.poll();
///       if (auto data = conn.rx.fetch(); !data.empty()) {
///           conn.rx.consume(parse(data));
///       }
///   }
///
/// The reactor never calls user code: poll() only moves data between sockets and the connections'
/// rx/tx queues and advances connection state machines.
///
/// With TaskRunMode::Deferred the ring is created disabled and bound to the thread that calls
/// poll()/wait() first, so a reactor may be constructed on one thread and run on another.
class Reactor {
private:
    detail::Ring ring_;

public:
    Reactor(Reactor const&) = delete;
    Reactor& operator=(Reactor const&) = delete;

    /// Create io_uring instance. Throws std::system_error on error.
    explicit Reactor(ReactorOptions const& options = {}) : ring_{options} {}

    /// Non-blocking iteration: send committed tx data, submit queued operations, process all
    /// available completions. Returns the number of processed completions.
    auto poll() -> std::size_t {
        return ring_.poll();
    }

    /// Like poll() but blocks until at least one completion arrives or the timeout expires.
    auto wait(std::chrono::nanoseconds timeout) -> std::size_t {
        return ring_.wait(timeout);
    }

    /// Submit queued operations to the kernel now.
    void submit() {
        ring_.submit();
    }

    /// CLOCK_REALTIME (ns) captured in the last poll()/wait(), right before completions are processed.
    [[nodiscard]] auto now() const noexcept -> std::uint64_t {
        return ring_.now();
    }

    /// Connections whose handle is gone but whose memory the kernel may still use; freed by
    /// poll()/wait() as their last operations complete.
    [[nodiscard]] auto retiredCount() const noexcept -> std::size_t {
        return ring_.retiredCount();
    }

    /// Internal: what connections are built on.
    [[nodiscard]] auto ring() noexcept -> detail::Ring& {
        return ring_;
    }
};

} // namespace turboq::reactor
