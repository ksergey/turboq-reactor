// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <liburing.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "TcpConnection.h"
#include "UdpConnection.h"
#include "WsConnection.h"
#include "detail/IoHandler.h"

namespace turboq::reactor {

/// How io_uring runs completion task work. This decides whether poll() has to enter the kernel to
/// see new completions, which is the main latency trade-off of the reactor. Measure on your
/// hardware (tools/tcp_pingpong --taskrun=...).
enum class TaskRunMode : std::uint8_t {
    /// Default io_uring behaviour: the kernel interrupts the polling thread (IPI) to post
    /// completions. poll() is a pure userspace check when there is nothing to submit, at the cost of
    /// being interrupted.
    Interrupt,
    /// IORING_SETUP_COOP_TASKRUN: no interrupts, completions are posted on the next kernel entry,
    /// so every poll() makes one io_uring_enter() syscall.
    Cooperative,
    /// IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN: completions are processed only
    /// inside io_uring_enter(GETEVENTS), so every poll() makes one syscall, but there is no
    /// interference from task work at any other point. Recommended by the io_uring authors for
    /// single-threaded event loops. The ring is bound to the thread that polls it first.
    Deferred,
};

struct ReactorOptions {
    unsigned sqEntries = 256;
    /// 0 means 2 * sqEntries.
    unsigned cqEntries = 0;
    TaskRunMode taskRunMode = TaskRunMode::Deferred;
    /// Kernel submission thread (IORING_SETUP_SQPOLL). Not compatible with TaskRunMode::Deferred.
    bool sqPoll = false;
    unsigned sqPollIdleMs = 1000;
    /// CPU to pin the SQPOLL thread to, -1 for no pinning.
    int sqPollCpu = -1;
};

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
    friend class detail::TcpCore;
    friend class detail::UdpCore;
    friend class detail::WsCore;
    friend void detail::attachCore(Reactor& reactor, detail::IoHandler* core) noexcept;
    friend void detail::releaseCore(detail::IoHandler* core) noexcept;

    io_uring ring_{};
    bool enabled_{false};
    bool needsEnter_{false};
    std::uint64_t now_{0};
    std::vector<detail::IoHandler*> pendingTx_;
    detail::IoHandler* live_{nullptr};        // connections whose handle exists (intrusive list)
    std::vector<detail::IoHandler*> retired_; // handle gone, waiting for the kernel to let go
    std::uint16_t nextBufferGroupId_{0};
    std::vector<std::uint16_t> freeBufferGroups_;

public:
    Reactor(Reactor const&) = delete;
    Reactor& operator=(Reactor const&) = delete;

    /// Create io_uring instance. Throws std::system_error on error.
    explicit Reactor(ReactorOptions const& options = {});

    ~Reactor() noexcept;

    /// Non-blocking iteration: send committed tx data, submit queued operations, process all
    /// available completions. Returns the number of processed completions.
    auto poll() -> std::size_t;

    /// Like poll() but blocks until at least one completion arrives or the timeout expires.
    auto wait(std::chrono::nanoseconds timeout) -> std::size_t;

    /// Submit queued operations to the kernel now.
    void submit();

    /// CLOCK_REALTIME (ns) captured in the last poll()/wait(), right before completions are processed.
    [[nodiscard]] auto now() const noexcept -> std::uint64_t {
        return now_;
    }

    /// Connections whose handle is gone but whose memory the kernel may still use; freed by
    /// poll()/wait() as their last operations complete.
    [[nodiscard]] auto retiredCount() const noexcept -> std::size_t {
        return retired_.size();
    }

private:
    void attach(detail::IoHandler* core) noexcept;
    void retire(detail::IoHandler* core) noexcept;
    void collectRetired() noexcept;
    auto allocateBufferGroup() -> std::uint16_t;
    void releaseBufferGroup(std::uint16_t id) noexcept;
    void enable();
    void submitInternal(bool getEvents);

    /// submit() for noexcept paths (connections). A failure here resurfaces in the next poll().
    void submitNoThrow() noexcept {
        if (enabled_ && ::io_uring_sq_ready(&ring_) > 0) {
            ::io_uring_submit(&ring_);
        }
    }
    auto reap() noexcept -> std::size_t;
    void flushPendingTx() noexcept;

    /// Get an SQE, submitting the queue if it is full. nullptr if the queue is still full.
    [[nodiscard]] auto getSqe() noexcept -> io_uring_sqe*;

    /// Make sure `count` SQEs can be taken without an intermediate submit (needed for linked SQEs).
    [[nodiscard]] auto ensureSqSpace(unsigned count) noexcept -> bool;
};

} // namespace turboq::reactor
