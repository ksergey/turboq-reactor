// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <linux/time_types.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include <turboq/Platform.h>

#include "Types.h"
#include "detail/IoHandler.h"
#include "detail/Scheduler.h"

namespace turboq::reactor {

struct EpollOptions {
    /// Events taken from the kernel per epoll_wait().
    unsigned maxEvents = 256;
};

/// Reactor backend on epoll (readiness based), for systems where io_uring is not available
/// (containers with the default seccomp profile, kernel.io_uring_disabled, old kernels) or to
/// compare against. Same operations and semantics as IoUringBackend (see there), emulated:
/// sockets stay registered (level-triggered) and the interest only changes when the set of
/// pending operations does; sends are tried right away and wait for EPOLLOUT only after EAGAIN.
///
/// Operations complete inside poll()/wait(), never inside the call that started them.
class EpollBackend : public detail::Scheduler {
public:
    using Options = EpollOptions;
    class BufferPool;

private:
    enum class RecvKind : std::uint8_t { None, Recv, RecvMsg, Multishot };
    enum class SendKind : std::uint8_t { None, Send, SendMsg };

    struct FdState {
        std::uint32_t generation{0}; // bumped by forget(): events of a closed fd are dropped
        std::uint32_t interest{0};   // registered epoll events
        bool registered{false};
        bool dirty{false}; // queued in dirty_
        bool sendQueued{false};

        detail::IoHandler* recvHandler{nullptr};
        RecvKind recvKind{RecvKind::None};
        std::span<std::byte> recvBuffer{};
        msghdr* recvMessage{nullptr};
        msghdr const* layout{nullptr};
        BufferPool* pool{nullptr};

        detail::IoHandler* sendHandler{nullptr};
        SendKind sendKind{SendKind::None};
        bool sendWaiting{false}; // EAGAIN seen: wait for EPOLLOUT
        std::span<std::byte const> sendData{};
        msghdr const* sendMessage{nullptr};

        detail::IoHandler* connectHandler{nullptr};

        detail::IoHandler* pollHandler{nullptr};
        unsigned pollEvents{0};
    };

    struct Completion {
        detail::IoHandler* handler;
        detail::OpCode op;
        std::int32_t res;
        std::uint32_t flags;
    };

    struct Timer {
        std::chrono::steady_clock::time_point deadline;
        detail::IoHandler* handler;
        detail::OpCode op; // Timer, or the operation (Connect, Poll) it limits
        int fd;
    };

    int epollFd_{-1};
    std::vector<epoll_event> events_;
    std::vector<FdState> fds_;
    std::vector<int> dirty_;     // fds whose epoll interest may have to change
    std::vector<int> sendQueue_; // fds with a send to try in the next poll
    std::vector<int> sendScratch_;
    std::vector<Completion> completions_;
    std::vector<Completion> delivering_;
    std::vector<Timer> timers_;

public:
    /// Create the epoll instance. Throws std::system_error.
    explicit EpollBackend(Options const& options);

    ~EpollBackend() noexcept;

    // Event loop (see Reactor).
    auto poll() -> std::size_t;
    auto wait(std::chrono::nanoseconds timeout) -> std::size_t;
    /// Nothing to do: operations are started by poll().
    void submit() noexcept {}
    void submitNoThrow() noexcept {}

    // Operations (see IoUringBackend).
    [[nodiscard]] auto connect(detail::IoHandler* handler, int fd, sockaddr const* address, socklen_t length,
        __kernel_timespec const* timeout) noexcept -> bool;
    [[nodiscard]] auto pollFd(
        detail::IoHandler* handler, int fd, unsigned events, __kernel_timespec const* timeout) noexcept -> bool;
    [[nodiscard]] auto recv(detail::IoHandler* handler, int fd, std::span<std::byte> buffer) noexcept -> bool;
    [[nodiscard]] auto recvMsg(detail::IoHandler* handler, int fd, msghdr* message) noexcept -> bool;
    [[nodiscard]] auto recvMultishot(
        detail::IoHandler* handler, int fd, msghdr const* layout, BufferPool& pool) noexcept -> bool;
    [[nodiscard]] auto send(detail::IoHandler* handler, int fd, std::span<std::byte const> data) noexcept -> bool;
    [[nodiscard]] auto sendMsg(detail::IoHandler* handler, int fd, msghdr const* message) noexcept -> bool;
    [[nodiscard]] auto timer(detail::IoHandler* handler, __kernel_timespec const* timeout) noexcept -> bool;
    void cancelTimer(detail::IoHandler* handler) noexcept;
    [[nodiscard]] auto cancel(detail::IoHandler* handler, int fd) noexcept -> bool;
    void forget(int fd) noexcept;

private:
    auto run(std::chrono::nanoseconds timeout) -> std::size_t;

    [[nodiscard]] auto state(int fd) -> FdState&;
    void markDirty(int fd) noexcept;
    void applyInterest() noexcept;
    void attemptSends() noexcept;
    auto deliverQueued() noexcept -> std::size_t;
    auto handleEvent(int fd, std::uint32_t events) noexcept -> std::size_t;
    auto fireTimers() noexcept -> std::size_t;
    [[nodiscard]] auto nextTimerIn() const noexcept -> std::chrono::nanoseconds;

    void queue(detail::IoHandler* handler, detail::OpCode op, std::int32_t res, std::uint32_t flags = 0) {
        completions_.push_back({handler, op, res, flags});
    }

    void addTimer(detail::IoHandler* handler, detail::OpCode op, int fd, __kernel_timespec const* timeout);
    void removeTimer(detail::OpCode op, int fd) noexcept;

    auto doRecv(int fd) noexcept -> std::size_t;
    auto doSend(int fd) noexcept -> std::size_t;
};

/// Receive buffers for recvMultishot(): a free list of ids; see IoUringBackend::BufferPool.
class EpollBackend::BufferPool {
private:
    std::byte* base_;
    std::size_t stride_;
    unsigned length_;
    std::vector<std::uint16_t> free_;

public:
    BufferPool(BufferPool const&) = delete;
    BufferPool& operator=(BufferPool const&) = delete;

    BufferPool(EpollBackend& backend, std::byte* base, std::size_t stride, unsigned length, unsigned count);

    void release() noexcept {}

    [[nodiscard]] auto buffer(std::uint16_t id) const noexcept -> std::byte* {
        return base_ + std::size_t{id} * stride_;
    }

    [[nodiscard]] auto length() const noexcept -> unsigned {
        return length_;
    }

    [[nodiscard]] auto empty() const noexcept -> bool {
        return free_.empty();
    }

    [[nodiscard]] auto take() noexcept -> std::uint16_t {
        auto const id = free_.back();
        free_.pop_back();
        return id;
    }

    TURBOQ_FORCE_INLINE void recycle(std::uint16_t id) noexcept {
        free_.push_back(id);
    }
};

} // namespace turboq::reactor
