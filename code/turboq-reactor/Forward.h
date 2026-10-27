// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

// Declarations of the reactor and the connections with their default backend: EpollBackend, or
// IoUringBackend when TURBOQ_REACTOR_IO_URING=1 is defined (for the whole program; the program then
// links liburing).

#pragma once

#ifndef TURBOQ_REACTOR_IO_URING
#define TURBOQ_REACTOR_IO_URING 0
#endif

namespace turboq::reactor {

class IoUringBackend;
class EpollBackend;

/// The backend of Reactor<>, TCPConnection<>, ...: epoll, or io_uring with TURBOQ_REACTOR_IO_URING=1.
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

} // namespace turboq::reactor
