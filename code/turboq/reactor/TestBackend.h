// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

// Tests of connections are built once per backend: TURBOQ_REACTOR_TEST_EPOLL selects epoll. The
// aliases below shadow the io_uring defaults inside turboq::reactor::testing.

#include <cstdint>
#include <vector>

#include "Reactor.h"

namespace turboq::reactor::testing {

#if defined(TURBOQ_REACTOR_TEST_EPOLL)
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
using TLSConnection = ::turboq::reactor::TLSConnection<TestBackend>;
using UDPConnection = ::turboq::reactor::UDPConnection<TestBackend>;
using WebsocketConnection = ::turboq::reactor::WebsocketConnection<TestBackend>;

} // namespace turboq::reactor::testing
