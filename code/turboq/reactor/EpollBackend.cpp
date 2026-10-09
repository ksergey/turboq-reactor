// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "EpollBackend.h"

#include <fcntl.h>
#include <poll.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <system_error>

#include "Error.h"
#include "detail/RecvMsg.h"

namespace turboq::reactor {
namespace {

using detail::IoHandler;
using detail::OpCode;

/// Datagrams handled per readiness event and multishot receive before other sockets get a turn
/// (level-triggered: the rest comes with the next poll).
constexpr int kMaxDatagramsPerEvent = 64;

[[nodiscard]] auto realtimeNow() noexcept -> Timestamp {
    timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    return Timestamp{std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec}};
}

[[nodiscard]] auto toDuration(__kernel_timespec const* ts) noexcept -> std::chrono::nanoseconds {
    return std::chrono::seconds{ts->tv_sec} + std::chrono::nanoseconds{ts->tv_nsec};
}

[[nodiscard]] constexpr auto encode(int fd, std::uint32_t generation) noexcept -> std::uint64_t {
    return std::uint64_t{generation} << 32 | static_cast<std::uint32_t>(fd);
}

} // namespace

EpollBackend::EpollBackend(Options const& options) {
    if (options.maxEvents == 0) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "EpollBackend"};
    }
    epollFd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epollFd_ < 0) {
        throw std::system_error{makePosixErrorCode(errno), "epoll_create1"};
    }
    events_.resize(options.maxEvents);
    fds_.resize(1024);
}

EpollBackend::~EpollBackend() noexcept {
    // Nothing is in flight in the kernel: pending operations are just bookkeeping here.
    ::close(epollFd_);
}

auto EpollBackend::state(int fd) -> FdState& {
    if (static_cast<std::size_t>(fd) >= fds_.size()) [[unlikely]] {
        fds_.resize(std::max<std::size_t>(fds_.size() * 2, static_cast<std::size_t>(fd) + 1));
    }
    return fds_[static_cast<std::size_t>(fd)];
}

void EpollBackend::markDirty(int fd) noexcept {
    auto& st = fds_[static_cast<std::size_t>(fd)];
    if (!st.dirty) {
        st.dirty = true;
        dirty_.push_back(fd);
    }
}

void EpollBackend::applyInterest() noexcept {
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
        event.data.u64 = encode(fd, st.generation);
        ::epoll_ctl(epollFd_, st.registered ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, fd, &event);
        st.registered = true;
        st.interest = want;
    }
    dirty_.clear();
}

auto EpollBackend::poll() -> std::size_t {
    this->flushPendingTx();
    return this->run(std::chrono::nanoseconds::zero());
}

auto EpollBackend::wait(std::chrono::nanoseconds timeout) -> std::size_t {
    this->flushPendingTx();
    return this->run(timeout);
}

auto EpollBackend::run(std::chrono::nanoseconds timeout) -> std::size_t {
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

    now_ = realtimeNow();
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

auto EpollBackend::deliverQueued() noexcept -> std::size_t {
    if (completions_.empty()) {
        return 0;
    }
    // Handlers may queue more: those are delivered by the next poll, like new CQEs.
    delivering_.swap(completions_);
    for (auto const& completion : delivering_) {
        completion.handler->onCompletion(completion.op, completion.res, completion.flags);
    }
    auto const count = delivering_.size();
    delivering_.clear();
    return count;
}

auto EpollBackend::handleEvent(int fd, std::uint32_t events) noexcept -> std::size_t {
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
        this->removeTimer(OpCode::Connect, fd);
        this->markDirty(fd);
        handler->onCompletion(OpCode::Connect, -error, 0);
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
            this->removeTimer(OpCode::Poll, fd);
            this->markDirty(fd);
            handler->onCompletion(OpCode::Poll, static_cast<std::int32_t>(revents), 0);
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

auto EpollBackend::doRecv(int fd) noexcept -> std::size_t {
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
        handler->onCompletion(OpCode::Recv, res, 0);
        return 1;
    }

    // Multishot: one completion per datagram, each in a buffer of the pool, laid out like
    // io_uring does it: detail::RecvMsgOut | name | control | payload.
    std::size_t count = 0;
    for (int i = 0; i < kMaxDatagramsPerEvent; ++i) {
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
            handler->onCompletion(OpCode::Recv, -ENOBUFS, 0);
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
            handler->onCompletion(OpCode::Recv, -error, 0);
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
        handler->onCompletion(OpCode::Recv, static_cast<std::int32_t>(header + static_cast<std::size_t>(rc)), flags);
        ++count;
    }
    return count;
}

auto EpollBackend::doSend(int fd) noexcept -> std::size_t {
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
    handler->onCompletion(OpCode::Send, res, 0);
    return 1;
}

void EpollBackend::attemptSends() noexcept {
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

void EpollBackend::addTimer(IoHandler* handler, OpCode op, int fd, __kernel_timespec const* timeout) {
    if (!timeout) {
        return; // no time limit
    }
    timers_.push_back({std::chrono::steady_clock::now() + toDuration(timeout), handler, op, fd});
}

void EpollBackend::removeTimer(OpCode op, int fd) noexcept {
    std::erase_if(timers_, [&](Timer const& timer) {
        return timer.op == op && timer.fd == fd;
    });
}

auto EpollBackend::nextTimerIn() const noexcept -> std::chrono::nanoseconds {
    auto earliest = timers_.front().deadline;
    for (auto const& timer : timers_) {
        earliest = std::min(earliest, timer.deadline);
    }
    return std::max(std::chrono::nanoseconds::zero(),
        std::chrono::duration_cast<std::chrono::nanoseconds>(earliest - std::chrono::steady_clock::now()));
}

auto EpollBackend::fireTimers() noexcept -> std::size_t {
    auto const now = std::chrono::steady_clock::now();
    std::size_t count = 0;
    // Expired timers are taken out first: handlers may add or remove timers.
    std::vector<Timer> expired;
    std::erase_if(timers_, [&](Timer const& timer) {
        if (timer.deadline > now) {
            return false;
        }
        expired.push_back(timer);
        return true;
    });
    for (auto const& timer : expired) {
        if (timer.op == OpCode::Timer) {
            timer.handler->onCompletion(OpCode::Timer, -ETIME, 0);
            ++count;
            continue;
        }
        // A limit on an operation: cancel it, if it is still pending.
        auto& st = fds_[static_cast<std::size_t>(timer.fd)];
        if (timer.op == OpCode::Connect && st.connectHandler == timer.handler) {
            st.connectHandler = nullptr;
        } else if (timer.op == OpCode::Poll && st.pollHandler == timer.handler) {
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

auto EpollBackend::connect(IoHandler* handler, int fd, sockaddr const* address, socklen_t length,
    __kernel_timespec const* timeout) noexcept -> bool {
    int const flags = ::fcntl(fd, F_GETFL);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        this->queue(handler, OpCode::Connect, -errno);
        return true;
    }
    if (::connect(fd, address, length) == 0) {
        this->queue(handler, OpCode::Connect, 0);
        return true;
    }
    if (errno != EINPROGRESS) {
        this->queue(handler, OpCode::Connect, -errno);
        return true;
    }
    try {
        this->state(fd).connectHandler = handler;
        this->addTimer(handler, OpCode::Connect, fd, timeout);
    } catch (...) {
        return false;
    }
    this->markDirty(fd);
    return true;
}

auto EpollBackend::pollFd(
    IoHandler* handler, int fd, unsigned events, __kernel_timespec const* timeout) noexcept -> bool {
    try {
        auto& st = this->state(fd);
        st.pollHandler = handler;
        st.pollEvents = events;
        this->addTimer(handler, OpCode::Poll, fd, timeout);
    } catch (...) {
        return false;
    }
    this->markDirty(fd);
    return true;
}

auto EpollBackend::recv(IoHandler* handler, int fd, std::span<std::byte> buffer) noexcept -> bool {
    try {
        auto& st = this->state(fd);
        st.recvHandler = handler;
        st.recvKind = RecvKind::Recv;
        st.recvBuffer = buffer;
    } catch (...) {
        return false;
    }
    this->markDirty(fd);
    return true;
}

auto EpollBackend::recvMsg(IoHandler* handler, int fd, msghdr* message) noexcept -> bool {
    try {
        auto& st = this->state(fd);
        st.recvHandler = handler;
        st.recvKind = RecvKind::RecvMsg;
        st.recvMessage = message;
    } catch (...) {
        return false;
    }
    this->markDirty(fd);
    return true;
}

auto EpollBackend::recvMultishot(IoHandler* handler, int fd, msghdr const* layout, BufferPool& pool) noexcept -> bool {
    try {
        auto& st = this->state(fd);
        st.recvHandler = handler;
        st.recvKind = RecvKind::Multishot;
        st.layout = layout;
        st.pool = &pool;
    } catch (...) {
        return false;
    }
    this->markDirty(fd);
    return true;
}

auto EpollBackend::send(IoHandler* handler, int fd, std::span<std::byte const> data) noexcept -> bool {
    try {
        auto& st = this->state(fd);
        st.sendHandler = handler;
        st.sendKind = SendKind::Send;
        st.sendData = data;
        st.sendWaiting = false;
        if (!st.sendQueued) {
            st.sendQueued = true;
            sendQueue_.push_back(fd);
        }
    } catch (...) {
        return false;
    }
    return true;
}

auto EpollBackend::sendMsg(IoHandler* handler, int fd, msghdr const* message) noexcept -> bool {
    try {
        auto& st = this->state(fd);
        st.sendHandler = handler;
        st.sendKind = SendKind::SendMsg;
        st.sendMessage = message;
        st.sendWaiting = false;
        if (!st.sendQueued) {
            st.sendQueued = true;
            sendQueue_.push_back(fd);
        }
    } catch (...) {
        return false;
    }
    return true;
}

auto EpollBackend::timer(IoHandler* handler, __kernel_timespec const* timeout) noexcept -> bool {
    try {
        this->addTimer(handler, OpCode::Timer, -1, timeout);
    } catch (...) {
        return false;
    }
    return true;
}

void EpollBackend::cancelTimer(IoHandler* handler) noexcept {
    auto const removed = std::erase_if(timers_, [&](Timer const& timer) {
        return timer.op == OpCode::Timer && timer.handler == handler;
    });
    for (std::size_t i = 0; i < removed; ++i) {
        this->queue(handler, OpCode::Timer, -ECANCELED);
    }
}

auto EpollBackend::cancel(IoHandler* handler, int fd) noexcept -> bool {
    try {
        auto& st = this->state(fd);
        if (st.connectHandler == handler) {
            st.connectHandler = nullptr;
            this->removeTimer(OpCode::Connect, fd);
            this->queue(handler, OpCode::Connect, -ECANCELED);
        }
        if (st.pollHandler == handler) {
            st.pollHandler = nullptr;
            this->removeTimer(OpCode::Poll, fd);
            this->queue(handler, OpCode::Poll, -ECANCELED);
        }
        if (st.recvHandler == handler) {
            st.recvHandler = nullptr;
            st.recvKind = RecvKind::None;
            this->queue(handler, OpCode::Recv, -ECANCELED); // multishot: final, no kCompletionMore
        }
        if (st.sendHandler == handler) {
            st.sendHandler = nullptr;
            st.sendKind = SendKind::None;
            st.sendWaiting = false;
            this->queue(handler, OpCode::Send, -ECANCELED);
        }
        this->queue(handler, OpCode::Cancel, 0);
    } catch (...) {
        return false;
    }
    this->markDirty(fd);
    return true;
}

void EpollBackend::forget(int fd) noexcept {
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

EpollBackend::BufferPool::BufferPool(
    [[maybe_unused]] EpollBackend& backend, std::byte* base, std::size_t stride, unsigned length, unsigned count)
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
