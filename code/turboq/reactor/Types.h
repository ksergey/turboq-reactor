// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <chrono>
#include <cstdint>
#include <string_view>

#include <turboq/reactor/Config.h>

namespace turboq::reactor {

class IoUringBackend;
class EpollBackend;

/// The backend of Reactor<>, TCPConnection<>, ...: io_uring, or epoll when the library is built
/// without io_uring (TURBOQ_REACTOR_IO_URING, see turboq_reactor_IO_URING in CMake).
#if TURBOQ_REACTOR_IO_URING
using DefaultBackend = IoUringBackend;
#else
using DefaultBackend = EpollBackend;
#endif

template <typename Backend = DefaultBackend>
class Reactor;
template <typename Backend = DefaultBackend>
class TCPConnection;
template <typename Backend = DefaultBackend>
class TLSConnection;
template <typename Backend = DefaultBackend>
class UDPConnection;
template <typename Backend = DefaultBackend>
class WebsocketConnection;
template <typename Backend = DefaultBackend>
class XDPConnection;

/// Wall clock time (CLOCK_REALTIME) with nanosecond resolution. A default-constructed value (the
/// epoch) means "not available".
using Timestamp = std::chrono::sys_time<std::chrono::nanoseconds>;

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

} // namespace turboq::reactor
