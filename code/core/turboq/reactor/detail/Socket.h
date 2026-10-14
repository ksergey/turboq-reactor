// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <sys/socket.h>
#include <time.h>

#include <chrono>
#include <cstddef>
#include <system_error>

#include <turboq/MappedRegion.h>

#include <turboq/reactor/Types.h>

namespace turboq::reactor::detail {

/// setsockopt(); the errno as an error code.
[[nodiscard]] auto setOption(
    int fd, int level, int name, void const* value, socklen_t length) noexcept -> std::error_code;

[[nodiscard]] inline auto setIntOption(int fd, int level, int name, int value) noexcept -> std::error_code {
    return setOption(fd, level, name, &value, sizeof(value));
}

/// Set or clear O_NONBLOCK.
[[nodiscard]] auto setNonBlocking(int fd, bool enabled) noexcept -> std::error_code;

/// Anonymous, pre-faulted memory owned by a MappedRegion. Throws std::system_error.
[[nodiscard]] auto mapAnonymous(std::size_t size) -> MappedRegion;

[[nodiscard]] inline auto toTimestamp(timespec const& ts) noexcept -> Timestamp {
    return Timestamp{std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec}};
}

} // namespace turboq::reactor::detail
