// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

// Declarations of the reactor and the connections with their default backend. Kept apart from
// Types.h so that compiled library code, which never uses the defaults, does not depend on which
// modules the user links.

#pragma once

#ifndef TURBOQ_REACTOR_IO_URING
#define TURBOQ_REACTOR_IO_URING 0
#endif

namespace turboq::reactor {

class IoUringBackend;
class EpollBackend;

/// The backend of Reactor<>, TCPConnection<>, ...: io_uring when turboq-reactor::uring is linked
/// (the target defines TURBOQ_REACTOR_IO_URING=1), epoll otherwise.
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
