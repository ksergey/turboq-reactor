// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <linux/time_types.h>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include <turboq/Platform.h>

namespace turboq::reactor::detail {

class Scheduler;

/// Operation kind of a completion (see IoHandler::onCompletion()). The io_uring backend stores it
/// in the low bits of an SQE's user_data.
enum class OpCode : std::uint8_t {
    Connect = 1, // 0 is reserved: user_data 0 marks completions nobody waits for
    Timer,
    Recv,
    Send,
    Cancel,
    Poll,
};

/// Completion flags (the values io_uring uses, so its CQE flags pass through unchanged).
inline constexpr std::uint32_t kCompletionBuffer = 1u << 0; // a buffer from a BufferPool was used
inline constexpr std::uint32_t kCompletionMore = 1u << 1;   // multishot: more completions follow
inline constexpr unsigned kCompletionBufferShift = 16;      // buffer id in the upper 16 bits

/// Internal interface of everything that owns in-flight I/O operations (connection cores).
///
/// Completions are dispatched through one indirect call per CQE. That is the only dynamic dispatch
/// in the library and it is per completion, not per message: a single recv completion usually
/// carries many messages, which the user then reads straight from the connection's rx queue
/// without any further indirection.
class IoHandler {
public:
    /// Set while the handler sits in the scheduler's pending-tx list (Scheduler::schedule()).
    bool txDirty_{false};

    // Life cycle, managed by the scheduler (see Scheduler::attach(), releaseCore()).
    Scheduler* owner_{nullptr};    // scheduler tracking this handler, nullptr if not attached
    bool orphaned_{false};         // the reactor was destroyed first: the owning handle deletes us
    IoHandler* livePrev_{nullptr}; // intrusive list of attached, not yet released handlers
    IoHandler* liveNext_{nullptr};

    virtual ~IoHandler() = default;

    /// True when the kernel holds no reference to this handler or its memory any more (closed, no
    /// operation in flight): it can be deleted.
    [[nodiscard]] virtual auto retirable() const noexcept -> bool = 0;

    /// The owning handle is gone: close for good.
    virtual void beginRetire() noexcept = 0;

    /// Called right before deletion while the reactor is alive (unregister reactor resources).
    virtual void onRetired() noexcept {}

    /// Remove this handler (and handlers it owns) from the reactor's pending-tx list.
    virtual void unlinkPendingTx(std::vector<IoHandler*>& list) noexcept {
        std::erase(list, this);
    }

    /// Called from Reactor::poll()/wait() for every CQE that belongs to this handler.
    virtual void onCompletion(OpCode op, std::int32_t res, std::uint32_t flags) noexcept = 0;

    /// Called from Reactor::poll()/wait() for handlers marked txDirty_: start sending committed data.
    virtual void onTxReady() noexcept = 0;
};

/// Release a handler whose handle is gone: deleted right away if the kernel is done with it,
/// otherwise closed and handed to the reactor, which deletes it once its last completion arrived.
/// Deleted directly if the reactor no longer exists (or the core was never attached to one).
void releaseCore(std::unique_ptr<IoHandler> core) noexcept;

/// Deleter of the core owned by a connection handle: releaseCore() instead of delete, because the
/// kernel may still be using the core's memory.
struct CoreReleaser {
    CoreReleaser() noexcept = default;

    /// Takes over from the std::unique_ptr returned by a core's create().
    template <typename Core>
    CoreReleaser(std::default_delete<Core>) noexcept {}

    void operator()(IoHandler* core) const noexcept {
        releaseCore(std::unique_ptr<IoHandler>{core});
    }
};

/// A connection handle's core.
template <typename Core>
using CorePtr = std::unique_ptr<Core, CoreReleaser>;

/// io_uring timeouts take a __kernel_timespec. Negative durations become zero.
[[nodiscard]] TURBOQ_FORCE_INLINE auto toKernelTimespec(std::chrono::nanoseconds duration) noexcept
    -> __kernel_timespec {
    duration = std::max(duration, std::chrono::nanoseconds::zero());
    auto const seconds = std::chrono::floor<std::chrono::seconds>(duration);
    return {.tv_sec = seconds.count(), .tv_nsec = (duration - seconds).count()};
}

inline constexpr std::uint64_t kOpCodeMask = 0x7;

static_assert(alignof(IoHandler) > kOpCodeMask, "low bits of IoHandler* are used for OpCode");

[[nodiscard]] TURBOQ_FORCE_INLINE auto encodeUserData(IoHandler* handler, OpCode op) noexcept -> std::uint64_t {
    return std::bit_cast<std::uintptr_t>(handler) | static_cast<std::uint64_t>(op);
}

[[nodiscard]] TURBOQ_FORCE_INLINE auto decodeHandler(std::uint64_t userData) noexcept -> IoHandler* {
    return std::bit_cast<IoHandler*>(static_cast<std::uintptr_t>(userData & ~kOpCodeMask));
}

[[nodiscard]] TURBOQ_FORCE_INLINE auto decodeOpCode(std::uint64_t userData) noexcept -> OpCode {
    return static_cast<OpCode>(userData & kOpCodeMask);
}

} // namespace turboq::reactor::detail
