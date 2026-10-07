// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "Reactor.h"

#include <time.h>

#include <limits>
#include <system_error>

#include "Error.h"

namespace turboq::reactor {
namespace {

[[nodiscard]] auto realtimeNs() noexcept -> std::uint64_t {
    timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000u + static_cast<std::uint64_t>(ts.tv_nsec);
}

[[nodiscard]] constexpr auto isTransientSubmitError(int rc) noexcept -> bool {
    return rc == -EINTR || rc == -EAGAIN || rc == -EBUSY || rc == -ETIME;
}

} // namespace

Reactor::Reactor(ReactorOptions const& options) {
    if (options.sqEntries == 0 || (options.sqPoll && options.taskRunMode == TaskRunMode::Deferred)) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "Reactor"};
    }

    io_uring_params params{};
    params.flags = IORING_SETUP_CLAMP | IORING_SETUP_SUBMIT_ALL;

    if (options.cqEntries != 0) {
        params.flags |= IORING_SETUP_CQSIZE;
        params.cq_entries = options.cqEntries;
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
        params.sq_thread_idle = options.sqPollIdleMs;
        if (options.sqPollCpu >= 0) {
            params.flags |= IORING_SETUP_SQ_AFF;
            params.sq_thread_cpu = static_cast<unsigned>(options.sqPollCpu);
        }
    }

    if (int const rc = ::io_uring_queue_init_params(options.sqEntries, &ring_, &params); rc < 0) {
        throw std::system_error{makePosixErrorCode(-rc), "io_uring_queue_init_params"};
    }

    enabled_ = (params.flags & IORING_SETUP_R_DISABLED) == 0;
    needsEnter_ = options.taskRunMode != TaskRunMode::Interrupt;
}

Reactor::~Reactor() noexcept {
    // Tear down the ring first: this cancels and waits for everything in flight, so the kernel
    // references no connection memory afterwards.
    ::io_uring_queue_exit(&ring_);
    // Connections that outlive the reactor are handed to their handles, which delete them (their
    // sockets get closed then). Destroying such a connection is the only valid operation left.
    for (auto* core = live_; core != nullptr; core = core->liveNext_) {
        core->orphaned_ = true;
        core->owner_ = nullptr;
    }
    for (auto* core : retired_) {
        delete core;
    }
}

void Reactor::enable() {
    if (int const rc = ::io_uring_enable_rings(&ring_); rc < 0) {
        throw std::system_error{makePosixErrorCode(-rc), "io_uring_enable_rings"};
    }
    enabled_ = true;
}

auto Reactor::poll() -> std::size_t {
    if (!enabled_) [[unlikely]] {
        this->enable();
    }
    this->flushPendingTx();
    this->submitInternal(needsEnter_);
    // Taken after entering the kernel, so completions reaped below are not newer than now().
    now_ = realtimeNs();
    auto const count = this->reap();
    // Completion handling re-arms receives and continues sends: push them out immediately rather
    // than one poll later.
    if (::io_uring_sq_ready(&ring_) > 0) {
        this->submitInternal(false);
    }
    this->collectRetired();
    return count;
}

auto Reactor::wait(std::chrono::nanoseconds timeout) -> std::size_t {
    if (!enabled_) [[unlikely]] {
        this->enable();
    }
    this->flushPendingTx();

    auto const seconds = std::chrono::duration_cast<std::chrono::seconds>(timeout);
    __kernel_timespec ts{
        .tv_sec = seconds.count(),
        .tv_nsec = (timeout - seconds).count(),
    };
    io_uring_cqe* cqe = nullptr;
    if (int const rc = ::io_uring_submit_and_wait_timeout(&ring_, &cqe, 1, &ts, nullptr);
        rc < 0 && !isTransientSubmitError(rc)) [[unlikely]] {
        throw std::system_error{makePosixErrorCode(-rc), "io_uring_submit_and_wait_timeout"};
    }

    now_ = realtimeNs();
    auto const count = this->reap();
    if (::io_uring_sq_ready(&ring_) > 0) {
        this->submitInternal(false);
    }
    this->collectRetired();
    return count;
}

void Reactor::submit() {
    if (!enabled_) [[unlikely]] {
        // Nothing can be submitted before the owning thread enables the ring; the queued SQEs go
        // out with the first poll().
        return;
    }
    this->submitInternal(false);
}

void Reactor::submitInternal(bool getEvents) {
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

auto Reactor::reap() noexcept -> std::size_t {
    unsigned head;
    io_uring_cqe* cqe;
    std::size_t count = 0;
    io_uring_for_each_cqe(&ring_, head, cqe) {
        ++count;
        if (cqe->user_data == 0 || cqe->user_data == LIBURING_UDATA_TIMEOUT) [[unlikely]] {
            continue;
        }
        detail::decodeHandler(cqe->user_data)->onCompletion(detail::decodeOpCode(cqe->user_data), cqe->res, cqe->flags);
    }
    ::io_uring_cq_advance(&ring_, static_cast<unsigned>(count));
    return count;
}

void Reactor::flushPendingTx() noexcept {
    if (pendingTx_.empty()) {
        return;
    }
    for (auto* handler : pendingTx_) {
        if (handler->txDirty_) {
            handler->txDirty_ = false;
            handler->onTxReady();
        }
    }
    pendingTx_.clear();
}

auto Reactor::getSqe() noexcept -> io_uring_sqe* {
    auto* sqe = ::io_uring_get_sqe(&ring_);
    if (!sqe && enabled_) [[unlikely]] {
        ::io_uring_submit(&ring_);
        sqe = ::io_uring_get_sqe(&ring_);
    }
    return sqe;
}

auto Reactor::ensureSqSpace(unsigned count) noexcept -> bool {
    if (::io_uring_sq_space_left(&ring_) >= count) {
        return true;
    }
    if (enabled_) {
        ::io_uring_submit(&ring_);
    }
    return ::io_uring_sq_space_left(&ring_) >= count;
}

void Reactor::attach(detail::IoHandler* core) noexcept {
    core->owner_ = this;
    core->livePrev_ = nullptr;
    core->liveNext_ = live_;
    if (live_) {
        live_->livePrev_ = core;
    }
    live_ = core;
}

void Reactor::retire(detail::IoHandler* core) noexcept {
    if (core->livePrev_) {
        core->livePrev_->liveNext_ = core->liveNext_;
    } else {
        live_ = core->liveNext_;
    }
    if (core->liveNext_) {
        core->liveNext_->livePrev_ = core->livePrev_;
    }
    core->livePrev_ = core->liveNext_ = nullptr;

    core->beginRetire();
    // pendingTx_ may hold the core (or a core it owns) even with txDirty_ cleared.
    core->unlinkPendingTx(pendingTx_);
    if (core->retirable()) {
        core->onRetired();
        delete core;
    } else {
        retired_.push_back(core);
    }
}

void Reactor::collectRetired() noexcept {
    if (retired_.empty()) [[likely]] {
        return;
    }
    std::erase_if(retired_, [this](detail::IoHandler* core) {
        if (!core->retirable()) {
            return false;
        }
        core->unlinkPendingTx(pendingTx_);
        core->onRetired();
        delete core;
        return true;
    });
}

auto Reactor::allocateBufferGroup() -> std::uint16_t {
    if (!freeBufferGroups_.empty()) {
        auto const id = freeBufferGroups_.back();
        freeBufferGroups_.pop_back();
        return id;
    }
    if (nextBufferGroupId_ == std::numeric_limits<std::uint16_t>::max()) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "out of io_uring buffer group ids"};
    }
    return nextBufferGroupId_++;
}

void Reactor::releaseBufferGroup(std::uint16_t id) noexcept {
    freeBufferGroups_.push_back(id);
}

namespace detail {

void attachCore(Reactor& reactor, IoHandler* core) noexcept {
    reactor.attach(core);
}

void releaseCore(IoHandler* core) noexcept {
    if (!core) {
        return;
    }
    if (core->orphaned_ || !core->owner_) {
        // The reactor is gone (its ring with it): nothing can reference the core any more.
        delete core;
        return;
    }
    core->owner_->retire(core);
}

} // namespace detail

} // namespace turboq::reactor
