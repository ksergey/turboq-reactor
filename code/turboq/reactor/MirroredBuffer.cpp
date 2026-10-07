// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "MirroredBuffer.h"

#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>

#include <turboq/File.h>
#include <turboq/Math.h>
#include <turboq/ScopeGuard.h>

#include "Error.h"

namespace turboq::reactor {

MirroredBuffer::~MirroredBuffer() noexcept {
    if (data_) {
        ::munmap(data_, capacity_ * 2);
    }
}

auto MirroredBuffer::create(Options const& options) noexcept -> std::expected<MirroredBuffer, std::error_code> {
    auto const pageSize = static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
    // A power of two that is >= pageSize is always a multiple of pageSize (page sizes are powers of two).
    auto const capacity = upperPow2(std::max(options.capacityHint, pageSize));

    auto file = File::anonymous("turboq-reactor-ring");
    if (!file) {
        return std::unexpected(file.error());
    }
    if (auto const rc = file->tryTruncate(capacity); !rc) {
        return std::unexpected(rc.error());
    }

    // Reserve 2 * capacity of address space, then map the same file over both halves.
    void* const reserved = ::mmap(nullptr, capacity * 2, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (reserved == MAP_FAILED) {
        return std::unexpected(makePosixErrorCode(errno));
    }
    auto* const base = static_cast<std::byte*>(reserved);
    auto unmapGuard = ScopeGuard{[&]() noexcept {
        ::munmap(base, capacity * 2);
    }};

    int const flags = MAP_SHARED | MAP_FIXED | (options.populate ? MAP_POPULATE : 0);
    for (std::size_t half = 0; half < 2; ++half) {
        void* const addr = ::mmap(base + half * capacity, capacity, PROT_READ | PROT_WRITE, flags, file->get(), 0);
        if (addr == MAP_FAILED) {
            return std::unexpected(makePosixErrorCode(errno));
        }
    }
    unmapGuard.release();

    // The fd is no longer needed: the mappings keep the memfd pages alive.
    MirroredBuffer buffer;
    buffer.data_ = base;
    buffer.capacity_ = capacity;
    buffer.mask_ = capacity - 1;
    return {std::move(buffer)};
}

} // namespace turboq::reactor
