// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <liburing.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "../Types.h"
#include "IoHandler.h"

namespace turboq::reactor::detail {

/// The machinery behind Reactor: the io_uring instance, completion dispatch, deferred sends and
/// the life cycle of connection cores. Connection cores use it directly; applications use Reactor.
class Ring {
private:
    io_uring ring_{};
    bool enabled_{false};
    bool needsEnter_{false};
    std::uint64_t now_{0};
    std::vector<IoHandler*> pendingTx_; // handlers whose onTxReady() runs in the next poll()
    IoHandler* live_{nullptr};          // connections whose handle exists (intrusive list)
    std::vector<IoHandler*> retired_;   // handle gone, waiting for the kernel to let go
    std::uint16_t nextBufferGroupId_{0};
    std::vector<std::uint16_t> freeBufferGroups_;

public:
    Ring(Ring const&) = delete;
    Ring& operator=(Ring const&) = delete;

    /// Throws std::system_error.
    explicit Ring(ReactorOptions const& options);
    ~Ring() noexcept;

    // Event loop (see Reactor).
    auto poll() -> std::size_t;
    auto wait(std::chrono::nanoseconds timeout) -> std::size_t;
    void submit();

    [[nodiscard]] auto now() const noexcept -> std::uint64_t {
        return now_;
    }

    [[nodiscard]] auto retiredCount() const noexcept -> std::size_t {
        return retired_.size();
    }

    // Submission.

    [[nodiscard]] auto native() noexcept -> io_uring* {
        return &ring_;
    }

    /// Get an SQE, submitting the queue if it is full. nullptr if the queue is still full.
    [[nodiscard]] auto getSqe() noexcept -> io_uring_sqe*;

    /// Make sure `count` SQEs can be taken without an intermediate submit (needed for linked SQEs).
    [[nodiscard]] auto ensureSqSpace(unsigned count) noexcept -> bool;

    /// submit() for noexcept paths (connections). A failure here resurfaces in the next poll().
    void submitNoThrow() noexcept {
        if (enabled_ && ::io_uring_sq_ready(&ring_) > 0) {
            ::io_uring_submit(&ring_);
        }
    }

    /// Call handler->onTxReady() in the next poll()/wait() (sets txDirty_).
    void schedule(IoHandler* handler) {
        handler->txDirty_ = true;
        pendingTx_.push_back(handler);
    }

    // Connection life cycle.

    /// Start tracking a freshly created connection core.
    void attach(IoHandler* core) noexcept;

    /// The core's handle is gone: close it and delete it once the kernel is done with it.
    void retire(IoHandler* core) noexcept;

    // Provided buffer ring group ids (UDP).
    auto allocateBufferGroup() -> std::uint16_t;
    void releaseBufferGroup(std::uint16_t id) noexcept;

private:
    void enable();
    void submitInternal(bool getEvents);
    auto reap() noexcept -> std::size_t;
    void flushPendingTx() noexcept;
    void collectRetired() noexcept;
};

} // namespace turboq::reactor::detail
