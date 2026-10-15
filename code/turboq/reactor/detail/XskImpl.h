// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

// Implementation of Xsk.h, included at its end.

#pragma once

#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bit>
#include <cerrno>

#include <turboq/reactor/Error.h>

namespace turboq::reactor::detail {

inline auto bindXskSocket(int fd, sockaddr_xdp const& address) noexcept -> int {
    constexpr int kAttempts = 50;
    for (int attempt = 0;; ++attempt) {
        if (::bind(fd, std::bit_cast<sockaddr const*>(&address), sizeof(address)) == 0) {
            return 0;
        }
        if (errno != EBUSY || attempt + 1 == kAttempts) {
            return errno;
        }
        ::usleep(10'000);
    }
}

inline auto mapXskRing(
    int fd, std::size_t size, off_t offset) noexcept -> std::expected<MappedRegion, std::error_code> {
    void* const addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, offset);
    if (addr == MAP_FAILED) {
        return std::unexpected(makePosixErrorCode(errno));
    }
    return MappedRegion{static_cast<std::byte*>(addr), size};
}

} // namespace turboq::reactor::detail
