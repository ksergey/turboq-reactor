// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include <turboq/reactor/detail/Socket.h>

#include <fcntl.h>
#include <sys/mman.h>

#include <cerrno>

#include <turboq/reactor/Error.h>

namespace turboq::reactor::detail {

auto setOption(int fd, int level, int name, void const* value, socklen_t length) noexcept -> std::error_code {
    if (::setsockopt(fd, level, name, value, length) != 0) {
        return makePosixErrorCode(errno);
    }
    return {};
}

auto setNonBlocking(int fd, bool enabled) noexcept -> std::error_code {
    int const flags = ::fcntl(fd, F_GETFL);
    if (flags < 0 || ::fcntl(fd, F_SETFL, enabled ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK)) < 0) {
        return makePosixErrorCode(errno);
    }
    return {};
}

auto mapAnonymous(std::size_t size) -> MappedRegion {
    void* const addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    if (addr == MAP_FAILED) {
        throw std::system_error{makePosixErrorCode(errno), "mmap"};
    }
    return MappedRegion{static_cast<std::byte*>(addr), size};
}

} // namespace turboq::reactor::detail
