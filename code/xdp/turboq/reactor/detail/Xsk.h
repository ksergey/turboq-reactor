// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <linux/if_xdp.h>
#include <sys/types.h>

#include <cstddef>
#include <expected>
#include <system_error>

#include <turboq/MappedRegion.h>

namespace turboq::reactor::detail {

inline constexpr unsigned kMaxXskFrameCount = 1u << 20;
/// The completion ring is mandatory but only used for transmit, which XDPConnection does not do.
inline constexpr int kXskCompletionRingSize = 64;

/// bind() an AF_XDP socket; returns the errno (0: success). EBUSY is retried for a while: a socket
/// closed just before (by this process too) releases the queue asynchronously, from a kernel work
/// queue.
[[nodiscard]] auto bindXskSocket(int fd, sockaddr_xdp const& address) noexcept -> int;

/// mmap() of one of an AF_XDP socket's rings.
[[nodiscard]] auto mapXskRing(
    int fd, std::size_t size, off_t offset) noexcept -> std::expected<MappedRegion, std::error_code>;

} // namespace turboq::reactor::detail
