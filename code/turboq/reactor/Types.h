// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdint>
#include <string_view>

namespace turboq::reactor {

/// Connection life cycle.
///
///   Idle -> Connecting -> [Handshaking] -> Ready -> Closing -> Closed
///                \              \                     ^
///                 `--------------`--------------------'   (error / timeout / close())
///
/// Handshaking is the TLS handshake of a TLS-enabled TCP connection.
///
/// Closed -> Connecting again via reconnect(). Data that was received before the connection got
/// closed stays readable in rx until reconnect().
enum class ConnectionState : std::uint8_t {
    Idle,
    Connecting,
    Handshaking,
    Ready,
    Closing,
    Closed,
};

[[nodiscard]] constexpr auto toStringView(ConnectionState state) noexcept -> std::string_view {
    switch (state) {
    case ConnectionState::Idle: return "Idle";
    case ConnectionState::Connecting: return "Connecting";
    case ConnectionState::Handshaking: return "Handshaking";
    case ConnectionState::Ready: return "Ready";
    case ConnectionState::Closing: return "Closing";
    case ConnectionState::Closed: return "Closed";
    }
    return "?";
}

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

struct ReactorOptions {
    unsigned sqEntries = 256;
    /// 0 means 2 * sqEntries.
    unsigned cqEntries = 0;
    TaskRunMode taskRunMode = TaskRunMode::Deferred;
    /// Kernel submission thread (IORING_SETUP_SQPOLL). Not compatible with TaskRunMode::Deferred.
    bool sqPoll = false;
    unsigned sqPollIdleMs = 1000;
    /// CPU to pin the SQPOLL thread to, -1 for no pinning.
    int sqPollCpu = -1;
};

} // namespace turboq::reactor
