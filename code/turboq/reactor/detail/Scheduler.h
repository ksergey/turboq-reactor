// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <cstddef>
#include <vector>

#include "../Types.h"
#include "IoHandler.h"

namespace turboq::reactor::detail {

/// The backend-independent part of a reactor: deferred sends, the poll timestamp and the life cycle
/// of connection cores. Backends (IoUringBackend, EpollBackend) derive from it.
class Scheduler {
private:
    std::vector<IoHandler*> pendingTx_; // handlers whose onTxReady() runs in the next poll()
    IoHandler* live_{nullptr};          // connections whose handle exists (intrusive list)
    std::vector<IoHandler*> retired_;   // handle gone, waiting for the backend to let go

protected:
    Timestamp now_{};

public:
    Scheduler(Scheduler const&) = delete;
    Scheduler& operator=(Scheduler const&) = delete;

    Scheduler() = default;

    /// Orphans live cores (their handles delete them) and deletes retired ones. Backends must have
    /// released every operation in flight before this runs (their destructor bodies run first).
    ~Scheduler() noexcept;

    /// Wall clock time of the last poll()/wait(), taken right before completions are processed.
    [[nodiscard]] auto now() const noexcept -> Timestamp {
        return now_;
    }

    [[nodiscard]] auto retiredCount() const noexcept -> std::size_t {
        return retired_.size();
    }

    /// Call handler->onTxReady() in the next poll()/wait() (sets txDirty_).
    void schedule(IoHandler* handler) {
        handler->txDirty_ = true;
        pendingTx_.push_back(handler);
    }

    /// Start tracking a freshly created connection core.
    void attach(IoHandler* core) noexcept;

    /// The core's handle is gone: close it and delete it once nothing is in flight.
    void retire(IoHandler* core) noexcept;

protected:
    void flushPendingTx() noexcept;
    void collectRetired() noexcept;
};

} // namespace turboq::reactor::detail
