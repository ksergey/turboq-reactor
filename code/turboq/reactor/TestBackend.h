// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

// Tests of connections are built once per backend: TURBOQ_REACTOR_TEST_EPOLL selects epoll (and
// so does a library built without io_uring). The aliases below shadow the defaults inside
// turboq::reactor::testing.

#include <cstdint>
#include <vector>

#include <turboq/reactor/Reactor.h>
#include <turboq/reactor/TCPConnection.h>
#include <turboq/reactor/UDPConnection.h>
#include <turboq/reactor/XDPConnection.h>
#if defined(TURBOQ_REACTOR_HAS_OPENSSL)
#include <turboq/reactor/TLSConnection.h>
#include <turboq/reactor/WebsocketConnection.h>
#endif

namespace turboq::reactor::testing {

#if defined(TURBOQ_REACTOR_TEST_EPOLL) || !TURBOQ_REACTOR_IO_URING
using TestBackend = EpollBackend;

/// Every reactor configuration worth running a test against.
inline auto const kReactorConfigs = std::vector<EpollOptions>{{}};

/// Fixed (multicast) ports differ per backend: both test binaries may run at the same time.
inline constexpr std::uint16_t kFixedPortBase = 31400;
#else
using TestBackend = IoUringBackend;

inline auto const kReactorConfigs = std::vector<IoUringOptions>{
    {.taskRunMode = TaskRunMode::Interrupt},
    {.taskRunMode = TaskRunMode::Cooperative},
    {.taskRunMode = TaskRunMode::Deferred},
};

inline constexpr std::uint16_t kFixedPortBase = 31300;
#endif

using Reactor = ::turboq::reactor::Reactor<TestBackend>;
using TCPConnection = ::turboq::reactor::TCPConnection<TestBackend>;
using UDPConnection = ::turboq::reactor::UDPConnection<TestBackend>;
#if defined(TURBOQ_REACTOR_HAS_OPENSSL)
using TLSConnection = ::turboq::reactor::TLSConnection<TestBackend>;
#endif
#if defined(TURBOQ_REACTOR_HAS_OPENSSL)
using WebsocketConnection = ::turboq::reactor::WebsocketConnection<TestBackend>;
#endif
using XDPConnection = ::turboq::reactor::XDPConnection<TestBackend>;

} // namespace turboq::reactor::testing
