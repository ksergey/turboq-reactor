// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "IoUringBackend.h"

#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include <limits>
#include <system_error>

#include <turboq/Math.h>

#include "Error.h"

namespace turboq::reactor {
namespace {

using detail::encodeUserData;
using detail::IoHandler;
using detail::OpCode;

static_assert(detail::kCompletionBuffer == IORING_CQE_F_BUFFER);
static_assert(detail::kCompletionMore == IORING_CQE_F_MORE);
static_assert(detail::kCompletionBufferShift == IORING_CQE_BUFFER_SHIFT);

/// user_data of SQEs whose completion nobody waits for (linked timeouts, timeout removal): reap()
/// skips it, so it may arrive after the handler is gone.
constexpr std::uint64_t kIgnored = 0;

[[nodiscard]] auto realtimeNow() noexcept -> Timestamp {
    timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    return Timestamp{std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec}};
}

[[nodiscard]] constexpr auto isTransientSubmitError(int rc) noexcept -> bool {
    return rc == -EINTR || rc == -EAGAIN || rc == -EBUSY || rc == -ETIME;
}

[[nodiscard]] auto mapAnonymous(std::size_t size) -> MappedRegion {
    void* const addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    if (addr == MAP_FAILED) {
        throw std::system_error{makePosixErrorCode(errno), "mmap"};
    }
    return MappedRegion{static_cast<std::byte*>(addr), size};
}

} // namespace

IoUringBackend::IoUringBackend(Options const& options) {
    if (options.sqEntries == 0 || (options.sqPoll && options.taskRunMode == TaskRunMode::Deferred)) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "IoUringBackend"};
    }

    io_uring_params params{};
    params.flags = IORING_SETUP_CLAMP | IORING_SETUP_SUBMIT_ALL;

    if (options.cqEntries) {
        params.flags |= IORING_SETUP_CQSIZE;
        params.cq_entries = *options.cqEntries;
    }

    switch (options.taskRunMode) {
    case TaskRunMode::Interrupt: break;
    case TaskRunMode::Cooperative: params.flags |= IORING_SETUP_COOP_TASKRUN | IORING_SETUP_TASKRUN_FLAG; break;
    case TaskRunMode::Deferred:
        // R_DISABLED: SINGLE_ISSUER binds the ring to the thread that enables it, so enabling is
        // postponed until the first poll()/wait(), letting the reactor be built on another thread.
        params.flags |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_R_DISABLED;
        break;
    }

    if (options.sqPoll) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = static_cast<unsigned>(options.sqPollIdle.count());
        if (options.sqPollCpu) {
            params.flags |= IORING_SETUP_SQ_AFF;
            params.sq_thread_cpu = *options.sqPollCpu;
        }
    }

    if (int const rc = ::io_uring_queue_init_params(options.sqEntries, &ring_, &params); rc < 0) {
        throw std::system_error{makePosixErrorCode(-rc), "io_uring_queue_init_params"};
    }

    enabled_ = (params.flags & IORING_SETUP_R_DISABLED) == 0;
    needsEnter_ = options.taskRunMode != TaskRunMode::Interrupt;
}

IoUringBackend::~IoUringBackend() noexcept {
    // Cancels and waits for everything in flight, so the kernel references no connection memory
    // when ~Scheduler() hands out or deletes the cores.
    ::io_uring_queue_exit(&ring_);
}

void IoUringBackend::enable() {
    if (int const rc = ::io_uring_enable_rings(&ring_); rc < 0) {
        throw std::system_error{makePosixErrorCode(-rc), "io_uring_enable_rings"};
    }
    enabled_ = true;
}

auto IoUringBackend::poll() -> std::size_t {
    if (!enabled_) [[unlikely]] {
        this->enable();
    }
    this->flushPendingTx();
    this->submitInternal(needsEnter_);
    // Taken after entering the kernel, so completions reaped below are not newer than now().
    now_ = realtimeNow();
    auto const count = this->reap();
    // Completion handling re-arms receives and continues sends: push them out immediately rather
    // than one poll later.
    if (::io_uring_sq_ready(&ring_) > 0) {
        this->submitInternal(false);
    }
    this->collectRetired();
    return count;
}

auto IoUringBackend::wait(std::chrono::nanoseconds timeout) -> std::size_t {
    if (!enabled_) [[unlikely]] {
        this->enable();
    }
    this->flushPendingTx();

    auto ts = detail::toKernelTimespec(timeout);
    io_uring_cqe* cqe = nullptr;
    if (int const rc = ::io_uring_submit_and_wait_timeout(&ring_, &cqe, 1, &ts, nullptr);
        rc < 0 && !isTransientSubmitError(rc)) [[unlikely]] {
        throw std::system_error{makePosixErrorCode(-rc), "io_uring_submit_and_wait_timeout"};
    }

    now_ = realtimeNow();
    auto const count = this->reap();
    if (::io_uring_sq_ready(&ring_) > 0) {
        this->submitInternal(false);
    }
    this->collectRetired();
    return count;
}

void IoUringBackend::submit() {
    if (!enabled_) [[unlikely]] {
        // Nothing can be submitted before the owning thread enables the ring; the queued SQEs go
        // out with the first poll().
        return;
    }
    this->submitInternal(false);
}

void IoUringBackend::submitInternal(bool getEvents) {
    int rc = 0;
    if (getEvents) {
        rc = ::io_uring_submit_and_get_events(&ring_);
    } else if (::io_uring_sq_ready(&ring_) > 0) {
        rc = ::io_uring_submit(&ring_);
    }
    if (rc < 0 && !isTransientSubmitError(rc)) [[unlikely]] {
        throw std::system_error{makePosixErrorCode(-rc), "io_uring_submit"};
    }
}

auto IoUringBackend::reap() noexcept -> std::size_t {
    unsigned head;
    io_uring_cqe* cqe;
    std::size_t count = 0;
    io_uring_for_each_cqe(&ring_, head, cqe) {
        ++count;
        if (cqe->user_data == kIgnored || cqe->user_data == LIBURING_UDATA_TIMEOUT) [[unlikely]] {
            continue;
        }
        detail::decodeHandler(cqe->user_data)->onCompletion(detail::decodeOpCode(cqe->user_data), cqe->res, cqe->flags);
    }
    ::io_uring_cq_advance(&ring_, static_cast<unsigned>(count));
    return count;
}

auto IoUringBackend::getSqe() noexcept -> io_uring_sqe* {
    auto* sqe = ::io_uring_get_sqe(&ring_);
    if (!sqe && enabled_) [[unlikely]] {
        ::io_uring_submit(&ring_);
        sqe = ::io_uring_get_sqe(&ring_);
    }
    return sqe;
}

auto IoUringBackend::ensureSqSpace(unsigned count) noexcept -> bool {
    if (::io_uring_sq_space_left(&ring_) >= count) {
        return true;
    }
    if (enabled_) {
        ::io_uring_submit(&ring_);
    }
    return ::io_uring_sq_space_left(&ring_) >= count;
}

// Operations.

auto IoUringBackend::connect(IoHandler* handler, int fd, sockaddr const* address, socklen_t length,
    __kernel_timespec const* timeout) noexcept -> bool {
    // connect + linked timeout must land in the same submission.
    if (!this->ensureSqSpace(2)) {
        return false;
    }
    auto* sqe = this->getSqe();
    ::io_uring_prep_connect(sqe, fd, address, length);
    sqe->flags |= IOSQE_IO_LINK;
    ::io_uring_sqe_set_data64(sqe, encodeUserData(handler, OpCode::Connect));
    auto* timeoutSqe = this->getSqe();
    ::io_uring_prep_link_timeout(timeoutSqe, const_cast<__kernel_timespec*>(timeout), 0);
    ::io_uring_sqe_set_data64(timeoutSqe, kIgnored); // the connect CQE carries -ECANCELED
    return true;
}

auto IoUringBackend::pollFd(
    IoHandler* handler, int fd, unsigned events, __kernel_timespec const* timeout) noexcept -> bool {
    if (!this->ensureSqSpace(2)) {
        return false;
    }
    auto* sqe = this->getSqe();
    ::io_uring_prep_poll_add(sqe, fd, events);
    sqe->flags |= IOSQE_IO_LINK;
    ::io_uring_sqe_set_data64(sqe, encodeUserData(handler, OpCode::Poll));
    auto* timeoutSqe = this->getSqe();
    ::io_uring_prep_link_timeout(timeoutSqe, const_cast<__kernel_timespec*>(timeout), 0);
    ::io_uring_sqe_set_data64(timeoutSqe, kIgnored);
    return true;
}

auto IoUringBackend::recv(IoHandler* handler, int fd, std::span<std::byte> buffer) noexcept -> bool {
    auto* sqe = this->getSqe();
    if (!sqe) [[unlikely]] {
        return false;
    }
    ::io_uring_prep_recv(sqe, fd, buffer.data(), buffer.size(), 0);
    ::io_uring_sqe_set_data64(sqe, encodeUserData(handler, OpCode::Recv));
    return true;
}

auto IoUringBackend::recvMsg(IoHandler* handler, int fd, msghdr* message) noexcept -> bool {
    auto* sqe = this->getSqe();
    if (!sqe) [[unlikely]] {
        return false;
    }
    ::io_uring_prep_recvmsg(sqe, fd, message, 0);
    ::io_uring_sqe_set_data64(sqe, encodeUserData(handler, OpCode::Recv));
    return true;
}

auto IoUringBackend::recvMultishot(
    IoHandler* handler, int fd, msghdr const* layout, BufferPool& pool) noexcept -> bool {
    auto* sqe = this->getSqe();
    if (!sqe) [[unlikely]] {
        return false;
    }
    ::io_uring_prep_recvmsg_multishot(sqe, fd, const_cast<msghdr*>(layout), 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = pool.groupId();
    ::io_uring_sqe_set_data64(sqe, encodeUserData(handler, OpCode::Recv));
    return true;
}

auto IoUringBackend::send(IoHandler* handler, int fd, std::span<std::byte const> data) noexcept -> bool {
    auto* sqe = this->getSqe();
    if (!sqe) [[unlikely]] {
        return false;
    }
    ::io_uring_prep_send(sqe, fd, data.data(), data.size(), MSG_NOSIGNAL);
    ::io_uring_sqe_set_data64(sqe, encodeUserData(handler, OpCode::Send));
    return true;
}

auto IoUringBackend::sendMsg(IoHandler* handler, int fd, msghdr const* message) noexcept -> bool {
    auto* sqe = this->getSqe();
    if (!sqe) [[unlikely]] {
        return false;
    }
    ::io_uring_prep_sendmsg(sqe, fd, message, MSG_NOSIGNAL);
    ::io_uring_sqe_set_data64(sqe, encodeUserData(handler, OpCode::Send));
    return true;
}

auto IoUringBackend::timer(IoHandler* handler, __kernel_timespec const* timeout) noexcept -> bool {
    auto* sqe = this->getSqe();
    if (!sqe) [[unlikely]] {
        return false;
    }
    ::io_uring_prep_timeout(sqe, const_cast<__kernel_timespec*>(timeout), 0, 0);
    ::io_uring_sqe_set_data64(sqe, encodeUserData(handler, OpCode::Timer));
    return true;
}

void IoUringBackend::cancelTimer(IoHandler* handler) noexcept {
    // The timer's own CQE (-ECANCELED) tells the handler; the removal's CQE is not waited for.
    if (auto* sqe = this->getSqe(); sqe) {
        ::io_uring_prep_timeout_remove(sqe, encodeUserData(handler, OpCode::Timer), 0);
        ::io_uring_sqe_set_data64(sqe, kIgnored);
    }
}

auto IoUringBackend::cancel(IoHandler* handler, int fd) noexcept -> bool {
    auto* sqe = this->getSqe();
    if (!sqe) [[unlikely]] {
        return false;
    }
    ::io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
    ::io_uring_sqe_set_data64(sqe, encodeUserData(handler, OpCode::Cancel));
    return true;
}

// Provided buffers.

IoUringBackend::BufferPool::BufferPool(
    IoUringBackend& backend, std::byte* base, std::size_t stride, unsigned length, unsigned count)
    : backend_{&backend}, groupId_{0}, base_{base}, stride_{stride}, length_{length}, count_{count} {
    if (count == 0 || count > 32768 || (count & (count - 1)) != 0) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "IoUringBackend::BufferPool"};
    }
    if (!backend.freeBufferGroups_.empty()) {
        groupId_ = backend.freeBufferGroups_.back();
        backend.freeBufferGroups_.pop_back();
    } else if (backend.nextBufferGroupId_ == std::numeric_limits<std::uint16_t>::max()) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "out of io_uring buffer group ids"};
    } else {
        groupId_ = backend.nextBufferGroupId_++;
    }

    auto const pageSize = static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
    try {
        // Our memory (page aligned), so nothing but the registration has to be undone.
        ringMemory_ = mapAnonymous(alignUp<std::size_t>(count * sizeof(io_uring_buf), pageSize));
    } catch (...) {
        backend.freeBufferGroups_.push_back(groupId_);
        throw;
    }
    bufferRing_ = reinterpret_cast<io_uring_buf_ring*>(ringMemory_.data());
    ::io_uring_buf_ring_init(bufferRing_);
    io_uring_buf_reg reg{};
    reg.ring_addr = reinterpret_cast<std::uint64_t>(bufferRing_);
    reg.ring_entries = count;
    reg.bgid = groupId_;
    if (int const rc = ::io_uring_register_buf_ring(&backend.ring_, &reg, 0); rc < 0) {
        backend.freeBufferGroups_.push_back(groupId_);
        throw std::system_error{makePosixErrorCode(-rc), "io_uring_register_buf_ring"};
    }
    registered_ = true;
    auto const mask = ::io_uring_buf_ring_mask(count);
    for (unsigned i = 0; i < count; ++i) {
        ::io_uring_buf_ring_add(bufferRing_, this->buffer(static_cast<std::uint16_t>(i)), length,
            static_cast<unsigned short>(i), mask, static_cast<int>(i));
    }
    ::io_uring_buf_ring_advance(bufferRing_, static_cast<int>(count));
}

void IoUringBackend::BufferPool::release() noexcept {
    if (registered_) {
        ::io_uring_unregister_buf_ring(&backend_->ring_, groupId_);
        backend_->freeBufferGroups_.push_back(groupId_);
        registered_ = false;
    }
}

} // namespace turboq::reactor
