// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

// Implementation of EpollBackend.h, included at its end.

#pragma once

#include <fcntl.h>
#include <poll.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <system_error>

#include <turboq-reactor/Error.h>
#include <turboq-reactor/detail/RecvMsg.h>

namespace turboq::reactor {
namespace detail::epoll {

/// Datagrams handled per readiness event and multishot receive before other sockets get a turn
/// (level-triggered: the rest comes with the next poll).
inline constexpr int kMaxDatagramsPerEvent = 64;

/// Capacity per fd slot of the timers (Connect and Poll limits, the handler's own timer) and of
/// the queued completions (each pending operation ends with one, cancel() adds its own).
inline constexpr std::size_t kTimersPerFd = 3;
inline constexpr std::size_t kCompletionsPerFd = 8;

[[nodiscard]] inline auto realtimeNow() noexcept -> Timestamp {
    timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    return Timestamp{std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec}};
}

[[nodiscard]] inline auto toDuration(__kernel_timespec const* ts) noexcept -> std::chrono::nanoseconds {
    return std::chrono::seconds{ts->tv_sec} + std::chrono::nanoseconds{ts->tv_nsec};
}

[[nodiscard]] constexpr auto encode(int fd, std::uint32_t generation) noexcept -> std::uint64_t {
    return std::uint64_t{generation} << 32 | static_cast<std::uint32_t>(fd);
}

} // namespace detail::epoll

inline EpollBackend::EpollBackend(Options const& options) {
    if (options.maxEvents == 0 || options.maxFds == 0) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "EpollBackend"};
    }
    // Everything is allocated here, before the epoll fd (nothing to clean up if it throws):
    // operations never allocate, they return false when full.
    std::size_t const maxFds = options.maxFds;
    events_.resize(options.maxEvents);
    fds_.resize(maxFds);
    dirty_.reserve(maxFds);     // an fd is listed at most once (FdState::dirty)
    sendQueue_.reserve(maxFds); // likewise (FdState::sendQueued)
    sendScratch_.reserve(maxFds);
    completions_.reserve(maxFds * detail::epoll::kCompletionsPerFd);
    delivering_.reserve(maxFds * detail::epoll::kCompletionsPerFd);
    std::vector<std::chrono::steady_clock::time_point> deadlines;
    std::vector<Timer> timers;
    deadlines.reserve(maxFds * detail::epoll::kTimersPerFd);
    timers.reserve(maxFds * detail::epoll::kTimersPerFd);
    timers_.replace(std::move(deadlines), std::move(timers));
    expired_.reserve(maxFds * detail::epoll::kTimersPerFd);
    epollFd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epollFd_ < 0) {
        throw std::system_error{makePosixErrorCode(errno), "epoll_create1"};
    }
}

inline EpollBackend::~EpollBackend() noexcept {
    // Nothing is in flight in the kernel: pending operations are just bookkeeping here.
    ::close(epollFd_);
}

inline auto EpollBackend::state(int fd) noexcept -> FdState* {
    if (fd < 0 || static_cast<std::size_t>(fd) >= fds_.size()) [[unlikely]] {
        return nullptr; // above EpollOptions::maxFds
    }
    return &fds_[static_cast<std::size_t>(fd)];
}

inline void EpollBackend::markDirty(int fd) noexcept {
    auto& st = fds_[static_cast<std::size_t>(fd)];
    if (!st.dirty) {
        st.dirty = true;
        dirty_.push_back(fd);
    }
}

inline void EpollBackend::applyInterest() noexcept {
    for (int const fd : dirty_) {
        auto& st = fds_[static_cast<std::size_t>(fd)];
        st.dirty = false;
        std::uint32_t want = 0;
        if (st.recvHandler || (st.pollHandler && (st.pollEvents & POLLIN))) {
            want |= EPOLLIN;
        }
        if (st.connectHandler || st.sendWaiting || (st.pollHandler && (st.pollEvents & POLLOUT))) {
            want |= EPOLLOUT;
        }
        if (want == 0) {
            // Not just "no events": EPOLLERR / EPOLLHUP are always reported, which would spin.
            if (st.registered) {
                ::epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr);
                st.registered = false;
                st.interest = 0;
            }
            continue;
        }
        if (st.registered && want == st.interest) {
            continue;
        }
        epoll_event event{};
        event.events = want | EPOLLRDHUP;
        event.data.u64 = detail::epoll::encode(fd, st.generation);
        ::epoll_ctl(epollFd_, st.registered ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, fd, &event);
        st.registered = true;
        st.interest = want;
    }
    dirty_.clear();
}

inline auto EpollBackend::poll() -> std::size_t {
    this->flushPendingTx();
    return this->run(std::chrono::nanoseconds::zero());
}

inline auto EpollBackend::wait(std::chrono::nanoseconds timeout) -> std::size_t {
    this->flushPendingTx();
    return this->run(timeout);
}

inline auto EpollBackend::run(std::chrono::nanoseconds timeout) -> std::size_t {
    this->attemptSends();
    this->applyInterest();

    if (!completions_.empty()) {
        timeout = std::chrono::nanoseconds::zero();
    } else if (!timers_.empty()) {
        timeout = std::min(timeout, this->nextTimerIn());
    }
    auto ts = detail::toKernelTimespec(timeout);
    timespec const waitFor{static_cast<time_t>(ts.tv_sec), static_cast<long>(ts.tv_nsec)};
    int const ready = ::epoll_pwait2(epollFd_, events_.data(), static_cast<int>(events_.size()), &waitFor, nullptr);
    if (ready < 0 && errno != EINTR) [[unlikely]] {
        throw std::system_error{makePosixErrorCode(errno), "epoll_pwait2"};
    }

    now_ = detail::epoll::realtimeNow();
    std::size_t count = this->deliverQueued();
    for (int i = 0; i < ready; ++i) {
        auto const data = events_[static_cast<std::size_t>(i)].data.u64;
        int const fd = static_cast<int>(static_cast<std::uint32_t>(data));
        if (fds_[static_cast<std::size_t>(fd)].generation != static_cast<std::uint32_t>(data >> 32)) {
            continue; // the fd was closed (and maybe reused) by a handler earlier in this batch
        }
        count += this->handleEvent(fd, events_[static_cast<std::size_t>(i)].events);
    }
    if (!timers_.empty()) {
        count += this->fireTimers();
    }
    // Operations started by the handlers above: sends go out in this poll, like submitted SQEs.
    this->attemptSends();
    this->applyInterest();
    this->collectRetired();
    return count;
}

inline auto EpollBackend::deliverQueued() noexcept -> std::size_t {
    if (completions_.empty()) {
        return 0;
    }
    // Handlers may queue more: those are delivered by the next poll, like new CQEs.
    delivering_.swap(completions_);
    for (auto const& completion : delivering_) {
        completion.handler->onCompletion(completion.op, completion.res, completion.flags);
    }
    std::size_t const count = delivering_.size();
    delivering_.clear();
    return count;
}

inline auto EpollBackend::handleEvent(int fd, std::uint32_t events) noexcept -> std::size_t {
    std::size_t count = 0;
    bool const failed = (events & (EPOLLERR | EPOLLHUP)) != 0;
    bool const readable = failed || (events & (EPOLLIN | EPOLLRDHUP)) != 0;
    bool const writable = failed || (events & EPOLLOUT) != 0;

    // Handlers run below may start or cancel operations on this fd: read the state anew after each.
    if (auto* handler = fds_[static_cast<std::size_t>(fd)].connectHandler; handler && writable) {
        int error = 0;
        socklen_t length = sizeof(error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) != 0) {
            error = errno;
        }
        fds_[static_cast<std::size_t>(fd)].connectHandler = nullptr;
        this->removeTimer(detail::OpCode::Connect, fd);
        this->markDirty(fd);
        handler->onCompletion(detail::OpCode::Connect, -error, 0);
        ++count;
    }
    if (auto* handler = fds_[static_cast<std::size_t>(fd)].pollHandler; handler) {
        auto const wanted = fds_[static_cast<std::size_t>(fd)].pollEvents;
        unsigned revents = 0;
        revents |= (readable && (wanted & POLLIN)) ? POLLIN : 0;
        revents |= (writable && (wanted & POLLOUT)) ? POLLOUT : 0;
        revents |= (events & EPOLLERR) ? POLLERR : 0;
        revents |= (events & EPOLLHUP) ? POLLHUP : 0;
        if (revents != 0) {
            fds_[static_cast<std::size_t>(fd)].pollHandler = nullptr;
            this->removeTimer(detail::OpCode::Poll, fd);
            this->markDirty(fd);
            handler->onCompletion(detail::OpCode::Poll, static_cast<std::int32_t>(revents), 0);
            ++count;
        }
    }
    if (writable && fds_[static_cast<std::size_t>(fd)].sendHandler && fds_[static_cast<std::size_t>(fd)].sendWaiting) {
        count += this->doSend(fd);
    }
    if (readable && fds_[static_cast<std::size_t>(fd)].recvHandler) {
        count += this->doRecv(fd);
    }
    return count;
}

inline auto EpollBackend::doRecv(int fd) noexcept -> std::size_t {
    auto& st = fds_[static_cast<std::size_t>(fd)];
    auto* const handler = st.recvHandler;

    if (st.recvKind != RecvKind::Multishot) {
        ssize_t rc;
        if (st.recvKind == RecvKind::Recv) {
            rc = ::recv(fd, st.recvBuffer.data(), st.recvBuffer.size(), MSG_DONTWAIT);
        } else {
            rc = ::recvmsg(fd, st.recvMessage, MSG_DONTWAIT);
        }
        if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return 0; // spurious: stay armed
        }
        auto const res = rc >= 0 ? static_cast<std::int32_t>(rc) : -errno;
        st.recvHandler = nullptr;
        st.recvKind = RecvKind::None;
        this->markDirty(fd);
        handler->onCompletion(detail::OpCode::Recv, res, 0);
        return 1;
    }

    // Multishot: one completion per datagram, each in a buffer of the pool, laid out like
    // io_uring does it: detail::RecvMsgOut | name | control | payload.
    std::size_t count = 0;
    for (int i = 0; i < detail::epoll::kMaxDatagramsPerEvent; ++i) {
        auto& cur = fds_[static_cast<std::size_t>(fd)];
        if (cur.recvHandler != handler || cur.recvKind != RecvKind::Multishot) {
            break; // cancelled or re-armed by the handler
        }
        auto& pool = *cur.pool;
        auto const* layout = cur.layout;
        if (pool.empty()) {
            cur.recvHandler = nullptr;
            cur.recvKind = RecvKind::None;
            this->markDirty(fd);
            handler->onCompletion(detail::OpCode::Recv, -ENOBUFS, 0);
            return count + 1;
        }
        auto const id = pool.take();
        auto* const buffer = pool.buffer(id);
        std::size_t const header = sizeof(detail::RecvMsgOut) + layout->msg_namelen + layout->msg_controllen;
        iovec iov{buffer + header, pool.length() - header};
        msghdr message{};
        message.msg_name = buffer + sizeof(detail::RecvMsgOut);
        message.msg_namelen = layout->msg_namelen;
        message.msg_control = buffer + sizeof(detail::RecvMsgOut) + layout->msg_namelen;
        message.msg_controllen = layout->msg_controllen;
        message.msg_iov = &iov;
        message.msg_iovlen = 1;
        auto const rc = ::recvmsg(fd, &message, MSG_DONTWAIT);
        if (rc < 0) {
            int const error = errno;
            pool.recycle(id);
            if (error == EAGAIN || error == EWOULDBLOCK) {
                break; // drained: stay armed
            }
            cur.recvHandler = nullptr;
            cur.recvKind = RecvKind::None;
            this->markDirty(fd);
            handler->onCompletion(detail::OpCode::Recv, -error, 0);
            return count + 1;
        }
        detail::RecvMsgOut out{};
        out.namelen = message.msg_namelen;
        out.controllen = static_cast<unsigned>(message.msg_controllen);
        out.payloadlen = static_cast<unsigned>(rc);
        out.flags = static_cast<unsigned>(message.msg_flags);
        std::memcpy(buffer, &out, sizeof(out));
        auto const flags =
            detail::kCompletionBuffer | detail::kCompletionMore | (std::uint32_t{id} << detail::kCompletionBufferShift);
        handler->onCompletion(
            detail::OpCode::Recv, static_cast<std::int32_t>(header + static_cast<std::size_t>(rc)), flags);
        ++count;
    }
    return count;
}

inline auto EpollBackend::doSend(int fd) noexcept -> std::size_t {
    auto& st = fds_[static_cast<std::size_t>(fd)];
    auto* const handler = st.sendHandler;
    ssize_t rc;
    if (st.sendKind == SendKind::Send) {
        rc = ::send(fd, st.sendData.data(), st.sendData.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
    } else {
        rc = ::sendmsg(fd, st.sendMessage, MSG_DONTWAIT | MSG_NOSIGNAL);
    }
    if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (!st.sendWaiting) {
            st.sendWaiting = true; // socket buffer full: wait for EPOLLOUT
            this->markDirty(fd);
        }
        return 0;
    }
    auto const res = rc >= 0 ? static_cast<std::int32_t>(rc) : -errno;
    if (st.sendWaiting) {
        this->markDirty(fd);
    }
    st.sendHandler = nullptr;
    st.sendKind = SendKind::None;
    st.sendWaiting = false;
    handler->onCompletion(detail::OpCode::Send, res, 0);
    return 1;
}

inline void EpollBackend::attemptSends() noexcept {
    if (sendQueue_.empty()) {
        return;
    }
    // Sends started by the handlers called here go out in the next round.
    sendScratch_.swap(sendQueue_);
    for (int const fd : sendScratch_) {
        auto& st = fds_[static_cast<std::size_t>(fd)];
        st.sendQueued = false;
        if (st.sendHandler && !st.sendWaiting) {
            this->doSend(fd);
        }
    }
    sendScratch_.clear();
}

// Timers.

inline auto EpollBackend::addTimer(
    detail::IoHandler* handler, detail::OpCode op, int fd, __kernel_timespec const* timeout) noexcept -> bool {
    if (!timeout) {
        return true; // no time limit
    }
    if (timers_.size() == timers_.keys().capacity()) [[unlikely]] {
        return false;
    }
    timers_.emplace(std::chrono::steady_clock::now() + detail::epoll::toDuration(timeout), Timer{handler, op, fd});
    return true;
}

inline void EpollBackend::removeTimer(detail::OpCode op, int fd) noexcept {
    std::erase_if(timers_, [&](auto const& entry) {
        return entry.second.op == op && entry.second.fd == fd;
    });
}

inline auto EpollBackend::nextTimerIn() const noexcept -> std::chrono::nanoseconds {
    return std::max(std::chrono::nanoseconds::zero(), std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                          timers_.keys().front() - std::chrono::steady_clock::now()));
}

inline auto EpollBackend::fireTimers() noexcept -> std::size_t {
    // The expired timers are the ones up to the first with a later deadline. They are taken out
    // before any handler runs: handlers may add or remove timers.
    auto const end = timers_.upper_bound(std::chrono::steady_clock::now());
    if (end == timers_.begin()) {
        return 0;
    }
    expired_.clear();
    for (auto it = timers_.begin(); it != end; ++it) {
        expired_.push_back(it->second);
    }
    timers_.erase(timers_.begin(), end);

    std::size_t count = 0;
    for (auto const& timer : expired_) {
        if (timer.op == detail::OpCode::Timer) {
            timer.handler->onCompletion(detail::OpCode::Timer, -ETIME, 0);
            ++count;
            continue;
        }
        // A limit on an operation: cancel it, if it is still pending.
        auto& st = fds_[static_cast<std::size_t>(timer.fd)];
        if (timer.op == detail::OpCode::Connect && st.connectHandler == timer.handler) {
            st.connectHandler = nullptr;
        } else if (timer.op == detail::OpCode::Poll && st.pollHandler == timer.handler) {
            st.pollHandler = nullptr;
        } else {
            continue;
        }
        this->markDirty(timer.fd);
        timer.handler->onCompletion(timer.op, -ECANCELED, 0);
        ++count;
    }
    return count;
}

// Operations.

inline auto EpollBackend::connect(detail::IoHandler* handler, int fd, sockaddr const* address, std::size_t length,
    __kernel_timespec const* timeout) noexcept -> bool {
    auto* const st = this->state(fd);
    if (!st || !this->canQueue(1)) [[unlikely]] {
        return false;
    }
    int const flags = ::fcntl(fd, F_GETFL);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        this->queue(handler, detail::OpCode::Connect, -errno);
        return true;
    }
    if (::connect(fd, address, length) == 0) {
        this->queue(handler, detail::OpCode::Connect, 0);
        return true;
    }
    if (errno != EINPROGRESS) {
        this->queue(handler, detail::OpCode::Connect, -errno);
        return true;
    }
    if (!this->addTimer(handler, detail::OpCode::Connect, fd, timeout)) [[unlikely]] {
        return false;
    }
    st->connectHandler = handler;
    this->markDirty(fd);
    return true;
}

inline auto EpollBackend::pollFd(
    detail::IoHandler* handler, int fd, unsigned events, __kernel_timespec const* timeout) noexcept -> bool {
    auto* const st = this->state(fd);
    if (!st || !this->addTimer(handler, detail::OpCode::Poll, fd, timeout)) [[unlikely]] {
        return false;
    }
    st->pollHandler = handler;
    st->pollEvents = events;
    this->markDirty(fd);
    return true;
}

inline auto EpollBackend::recv(detail::IoHandler* handler, int fd, std::span<std::byte> buffer) noexcept -> bool {
    auto* const st = this->state(fd);
    if (!st) [[unlikely]] {
        return false;
    }
    st->recvHandler = handler;
    st->recvKind = RecvKind::Recv;
    st->recvBuffer = buffer;
    this->markDirty(fd);
    return true;
}

inline auto EpollBackend::recvMsg(detail::IoHandler* handler, int fd, msghdr* message) noexcept -> bool {
    auto* const st = this->state(fd);
    if (!st) [[unlikely]] {
        return false;
    }
    st->recvHandler = handler;
    st->recvKind = RecvKind::RecvMsg;
    st->recvMessage = message;
    this->markDirty(fd);
    return true;
}

inline auto EpollBackend::recvMultishot(
    detail::IoHandler* handler, int fd, msghdr const* layout, BufferPool& pool) noexcept -> bool {
    auto* const st = this->state(fd);
    if (!st) [[unlikely]] {
        return false;
    }
    st->recvHandler = handler;
    st->recvKind = RecvKind::Multishot;
    st->layout = layout;
    st->pool = &pool;
    this->markDirty(fd);
    return true;
}

inline auto EpollBackend::send(detail::IoHandler* handler, int fd, std::span<std::byte const> data) noexcept -> bool {
    auto* const st = this->state(fd);
    if (!st) [[unlikely]] {
        return false;
    }
    st->sendHandler = handler;
    st->sendKind = SendKind::Send;
    st->sendData = data;
    st->sendWaiting = false;
    if (!st->sendQueued) {
        st->sendQueued = true;
        sendQueue_.push_back(fd);
    }
    return true;
}

inline auto EpollBackend::sendMsg(detail::IoHandler* handler, int fd, msghdr const* message) noexcept -> bool {
    auto* const st = this->state(fd);
    if (!st) [[unlikely]] {
        return false;
    }
    st->sendHandler = handler;
    st->sendKind = SendKind::SendMsg;
    st->sendMessage = message;
    st->sendWaiting = false;
    if (!st->sendQueued) {
        st->sendQueued = true;
        sendQueue_.push_back(fd);
    }
    return true;
}

inline auto EpollBackend::timer(detail::IoHandler* handler, __kernel_timespec const* timeout) noexcept -> bool {
    return this->addTimer(handler, detail::OpCode::Timer, -1, timeout);
}

inline void EpollBackend::cancelTimer(detail::IoHandler* handler) noexcept {
    // One timer per handler. No room for its completion: it stays and fires (as with io_uring
    // when there is no SQE for the removal).
    if (!this->canQueue(1)) [[unlikely]] {
        return;
    }
    auto const removed = std::erase_if(timers_, [&](auto const& entry) {
        return entry.second.op == detail::OpCode::Timer && entry.second.handler == handler;
    });
    for (std::size_t i = 0; i < removed; ++i) {
        this->queue(handler, detail::OpCode::Timer, -ECANCELED);
    }
}

inline auto EpollBackend::cancel(detail::IoHandler* handler, int fd) noexcept -> bool {
    auto* const st = this->state(fd);
    if (!st || !this->canQueue(5)) [[unlikely]] { // up to four operations and the cancel itself
        return false;
    }
    if (st->connectHandler == handler) {
        st->connectHandler = nullptr;
        this->removeTimer(detail::OpCode::Connect, fd);
        this->queue(handler, detail::OpCode::Connect, -ECANCELED);
    }
    if (st->pollHandler == handler) {
        st->pollHandler = nullptr;
        this->removeTimer(detail::OpCode::Poll, fd);
        this->queue(handler, detail::OpCode::Poll, -ECANCELED);
    }
    if (st->recvHandler == handler) {
        st->recvHandler = nullptr;
        st->recvKind = RecvKind::None;
        this->queue(handler, detail::OpCode::Recv, -ECANCELED); // multishot: final, no kCompletionMore
    }
    if (st->sendHandler == handler) {
        st->sendHandler = nullptr;
        st->sendKind = SendKind::None;
        st->sendWaiting = false;
        this->queue(handler, detail::OpCode::Send, -ECANCELED);
    }
    this->queue(handler, detail::OpCode::Cancel, 0);
    this->markDirty(fd);
    return true;
}

inline void EpollBackend::forget(int fd) noexcept {
    if (fd < 0 || static_cast<std::size_t>(fd) >= fds_.size()) {
        return;
    }
    auto& st = fds_[static_cast<std::size_t>(fd)];
    if (st.registered) {
        ::epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr);
    }
    auto const generation = st.generation + 1;
    bool const dirty = st.dirty;
    bool const sendQueued = st.sendQueued;
    st = FdState{};
    st.generation = generation;
    st.dirty = dirty;           // still listed in dirty_: applyInterest() finds nothing to do
    st.sendQueued = sendQueued; // still listed in sendQueue_: nothing to send
}

// Buffers.

inline EpollBackend::BufferPool::BufferPool(
    [[maybe_unused]] EpollBackend& backend, std::byte* base, std::size_t stride, std::size_t length, std::size_t count)
    : base_{base}, stride_{stride}, length_{length} {
    if (count == 0 || count > 65536) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "EpollBackend::BufferPool"};
    }
    free_.reserve(count);
    for (unsigned i = count; i > 0; --i) {
        free_.push_back(static_cast<std::uint16_t>(i - 1)); // take() hands out 0 first
    }
}

} // namespace turboq::reactor
