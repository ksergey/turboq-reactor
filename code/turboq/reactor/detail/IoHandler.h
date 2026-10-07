// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <bit>
#include <cstdint>
#include <vector>

namespace turboq::reactor {
class Reactor;
}

namespace turboq::reactor::detail {

/// Operation kind, stored in the low bits of an SQE's user_data.
enum class OpCode : std::uint8_t {
    Connect = 0,
    ConnectTimeout,
    Recv,
    Send,
    Cancel,
    HandshakePoll,
};

/// Internal interface of everything that owns in-flight io_uring operations (connections).
///
/// Completions are dispatched through one indirect call per CQE. That is the only dynamic dispatch
/// in the library and it is per completion, not per message: a single recv completion usually
/// carries many messages, which the user then reads straight from the connection's rx queue
/// without any further indirection.
class IoHandler {
public:
    /// Set while the handler sits in Reactor's pending-tx list with committed data to send.
    bool txDirty_{false};

    // Life cycle, managed by the reactor (see attachCore()/releaseCore()).
    Reactor* owner_{nullptr};      // reactor tracking this handler, nullptr if not attached
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

/// Start tracking a freshly created handler (connection handle constructor).
void attachCore(Reactor& reactor, IoHandler* core) noexcept;

/// Release a handler whose handle is gone: deleted right away if the kernel is done with it,
/// otherwise closed and deleted by the reactor once its last completion arrived. Deleted directly
/// if the reactor no longer exists. nullptr is ignored.
void releaseCore(IoHandler* core) noexcept;

inline constexpr std::uint64_t kOpCodeMask = 0x7;

static_assert(alignof(IoHandler) > kOpCodeMask, "low bits of IoHandler* are used for OpCode");

[[nodiscard]] inline auto encodeUserData(IoHandler* handler, OpCode op) noexcept -> std::uint64_t {
    return std::bit_cast<std::uintptr_t>(handler) | static_cast<std::uint64_t>(op);
}

[[nodiscard]] inline auto decodeHandler(std::uint64_t userData) noexcept -> IoHandler* {
    return std::bit_cast<IoHandler*>(static_cast<std::uintptr_t>(userData & ~kOpCodeMask));
}

[[nodiscard]] inline auto decodeOpCode(std::uint64_t userData) noexcept -> OpCode {
    return static_cast<OpCode>(userData & kOpCodeMask);
}

} // namespace turboq::reactor::detail
