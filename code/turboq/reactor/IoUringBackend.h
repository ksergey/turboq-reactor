// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <liburing.h>
#include <linux/time_types.h>
#include <sys/socket.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include <turboq/MappedRegion.h>
#include <turboq/Platform.h>

#include "Types.h"
#include "detail/IoHandler.h"
#include "detail/Scheduler.h"

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

struct IoUringOptions {
    unsigned sqEntries = 256;
    /// Completion queue size. None: 2 * sqEntries.
    std::optional<unsigned> cqEntries{};
    TaskRunMode taskRunMode = TaskRunMode::Deferred;
    /// Kernel submission thread (IORING_SETUP_SQPOLL). Not compatible with TaskRunMode::Deferred.
    bool sqPoll = false;
    /// How long the SQPOLL thread spins without work before it sleeps.
    std::chrono::milliseconds sqPollIdle{1000};
    /// CPU to pin the SQPOLL thread to. None: no pinning.
    std::optional<unsigned> sqPollCpu{};
};

/// Reactor backend on io_uring (completion based). The default: see Reactor.
///
/// Operations (used by connection cores): each one ends with exactly one handler->onCompletion()
/// with its OpCode, except recvMultishot(), which completes once per datagram (kCompletionMore set
/// on all but the last). They return false when the operation can't be queued (submission queue
/// full). Memory passed in (buffers, msghdr, timespec) must stay valid until the completion.
class IoUringBackend : public detail::Scheduler {
public:
    using Options = IoUringOptions;
    class BufferPool;

private:
    io_uring ring_{};
    bool enabled_{false};
    bool needsEnter_{false};
    std::uint16_t nextBufferGroupId_{0};
    std::vector<std::uint16_t> freeBufferGroups_;

public:
    /// Create the io_uring instance. Throws std::system_error.
    explicit IoUringBackend(Options const& options);

    /// Tears the ring down first: everything in flight is cancelled and waited for.
    ~IoUringBackend() noexcept;

    // Event loop (see Reactor).
    auto poll() -> std::size_t;
    auto wait(std::chrono::nanoseconds timeout) -> std::size_t;
    void submit();

    /// submit() for noexcept paths. A failure here resurfaces in the next poll().
    void submitNoThrow() noexcept {
        if (enabled_ && ::io_uring_sq_ready(&ring_) > 0) {
            ::io_uring_submit(&ring_);
        }
    }

    // Operations.

    /// Connect, failing with -ECANCELED after `timeout`. OpCode::Connect.
    [[nodiscard]] auto connect(detail::IoHandler* handler, int fd, sockaddr const* address, socklen_t length,
        __kernel_timespec const* timeout) noexcept -> bool;
    /// Wait for POLLIN / POLLOUT, failing with -ECANCELED after `timeout`. OpCode::Poll, res = revents.
    [[nodiscard]] auto pollFd(
        detail::IoHandler* handler, int fd, unsigned events, __kernel_timespec const* timeout) noexcept -> bool;
    /// OpCode::Recv, res = bytes (0: EOF).
    [[nodiscard]] auto recv(detail::IoHandler* handler, int fd, std::span<std::byte> buffer) noexcept -> bool;
    /// OpCode::Recv.
    [[nodiscard]] auto recvMsg(detail::IoHandler* handler, int fd, msghdr* message) noexcept -> bool;
    /// Datagrams into buffers of `pool`, laid out as io_uring_recvmsg_out + name + control + payload
    /// sized by `layout`. OpCode::Recv, flags: kCompletionBuffer | id << kCompletionBufferShift.
    /// Ends (no kCompletionMore) with -ENOBUFS once the pool is empty.
    [[nodiscard]] auto recvMultishot(
        detail::IoHandler* handler, int fd, msghdr const* layout, BufferPool& pool) noexcept -> bool;
    /// OpCode::Send, res = bytes sent.
    [[nodiscard]] auto send(detail::IoHandler* handler, int fd, std::span<std::byte const> data) noexcept -> bool;
    /// OpCode::Send.
    [[nodiscard]] auto sendMsg(detail::IoHandler* handler, int fd, msghdr const* message) noexcept -> bool;
    /// OpCode::Timer: -ETIME when it fires, -ECANCELED after cancelTimer(). One timer per handler.
    [[nodiscard]] auto timer(detail::IoHandler* handler, __kernel_timespec const* timeout) noexcept -> bool;
    void cancelTimer(detail::IoHandler* handler) noexcept;
    /// Everything of `handler` on `fd` completes (-ECANCELED unless already done). OpCode::Cancel.
    [[nodiscard]] auto cancel(detail::IoHandler* handler, int fd) noexcept -> bool;
    /// `fd` is about to be closed, nothing is in flight on it.
    void forget([[maybe_unused]] int fd) noexcept {}

    [[nodiscard]] auto native() noexcept -> io_uring* {
        return &ring_;
    }

private:
    void enable();
    void submitInternal(bool getEvents);
    auto reap() noexcept -> std::size_t;
    [[nodiscard]] auto getSqe() noexcept -> io_uring_sqe*;
    [[nodiscard]] auto ensureSqSpace(unsigned count) noexcept -> bool;
};

/// Receive buffers handed to the kernel (provided buffer ring). `count` buffers of `length`
/// usable bytes, `stride` apart from `base`; count is a power of two, at most 32768.
class IoUringBackend::BufferPool {
private:
    IoUringBackend* backend_;
    std::uint16_t groupId_;
    MappedRegion ringMemory_;
    io_uring_buf_ring* bufferRing_{nullptr};
    std::byte* base_;
    std::size_t stride_;
    unsigned length_;
    unsigned count_;
    bool registered_{false};

public:
    BufferPool(BufferPool const&) = delete;
    BufferPool& operator=(BufferPool const&) = delete;

    /// Throws std::system_error. Must be created on the polling thread (or before the first poll).
    BufferPool(IoUringBackend& backend, std::byte* base, std::size_t stride, unsigned length, unsigned count);

    ~BufferPool() noexcept = default;

    /// Unregister from the backend (while it exists). Required before destruction unless the
    /// backend is gone.
    void release() noexcept;

    [[nodiscard]] auto groupId() const noexcept -> std::uint16_t {
        return groupId_;
    }

    [[nodiscard]] auto buffer(std::uint16_t id) const noexcept -> std::byte* {
        return base_ + std::size_t{id} * stride_;
    }

    /// Give buffer `id` back to the kernel.
    TURBOQ_FORCE_INLINE void recycle(std::uint16_t id) noexcept {
        ::io_uring_buf_ring_add(bufferRing_, this->buffer(id), length_, id, ::io_uring_buf_ring_mask(count_), 0);
        ::io_uring_buf_ring_advance(bufferRing_, 1);
    }
};

} // namespace turboq::reactor
