// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <system_error>
#include <utility>

#include <turboq/Platform.h>

namespace turboq::reactor {

/// Byte ring buffer whose memory is mapped twice back-to-back (the same memfd pages at
/// [data, data + capacity) and at [data + capacity, data + 2 * capacity)). Any range of up to
/// `capacity` bytes starting anywhere in the first half is therefore contiguous in virtual memory:
/// readable() and writable() always return a single span, even when the data wraps around the end
/// of the ring. This is what lets a TCP connection hand the whole unread stream to a parser as one
/// span and lets a serializer write a message into the tx ring without caring about wrap-around.
///
/// Single-threaded: head/tail are plain integers, not atomics.
class MirroredBuffer {
private:
    std::byte* data_{nullptr};
    std::size_t capacity_{0};
    std::size_t mask_{0};
    std::uint64_t head_{0}; // read position (monotonic)
    std::uint64_t tail_{0}; // write position (monotonic)

public:
    struct Options {
        /// Requested capacity. Rounded up to a power of two and to at least one page.
        std::size_t capacityHint = 1u << 20;
        /// Pre-fault the pages (MAP_POPULATE) so the first writes do not page-fault on the hot path.
        bool populate = true;
    };

    MirroredBuffer(MirroredBuffer const&) = delete;
    MirroredBuffer& operator=(MirroredBuffer const&) = delete;

    MirroredBuffer() = default;

    MirroredBuffer(MirroredBuffer&& other) noexcept
        : data_{std::exchange(other.data_, nullptr)}, capacity_{std::exchange(other.capacity_, 0)},
          mask_{std::exchange(other.mask_, 0)}, head_{std::exchange(other.head_, 0)},
          tail_{std::exchange(other.tail_, 0)} {}

    MirroredBuffer& operator=(MirroredBuffer&& other) noexcept {
        if (this != &other) {
            this->~MirroredBuffer();
            new (this) MirroredBuffer{std::move(other)};
        }
        return *this;
    }

    ~MirroredBuffer() noexcept;

    /// Create buffer.
    [[nodiscard]] static auto create(Options const& options) noexcept -> std::expected<MirroredBuffer, std::error_code>;

    /// Return true if initialized.
    [[nodiscard]] explicit operator bool() const noexcept {
        return data_ != nullptr;
    }

    /// Ring capacity in bytes.
    [[nodiscard]] TURBOQ_FORCE_INLINE auto capacity() const noexcept -> std::size_t {
        return capacity_;
    }

    /// Number of readable bytes.
    [[nodiscard]] TURBOQ_FORCE_INLINE auto size() const noexcept -> std::size_t {
        return static_cast<std::size_t>(tail_ - head_);
    }

    /// Number of writable bytes.
    [[nodiscard]] TURBOQ_FORCE_INLINE auto available() const noexcept -> std::size_t {
        return capacity_ - this->size();
    }

    [[nodiscard]] TURBOQ_FORCE_INLINE auto empty() const noexcept -> bool {
        return head_ == tail_;
    }

    [[nodiscard]] TURBOQ_FORCE_INLINE auto full() const noexcept -> bool {
        return this->size() == capacity_;
    }

    /// All unread bytes as one contiguous span.
    [[nodiscard]] TURBOQ_FORCE_INLINE auto readable() const noexcept -> std::span<std::byte const> {
        return {data_ + (head_ & mask_), this->size()};
    }

    /// All free space as one contiguous span.
    [[nodiscard]] TURBOQ_FORCE_INLINE auto writable() noexcept -> std::span<std::byte> {
        return {data_ + (tail_ & mask_), this->available()};
    }

    /// Mark `size` bytes at the beginning of writable() as written.
    TURBOQ_FORCE_INLINE void produce(std::size_t size) noexcept {
        assert(size <= this->available());
        tail_ += size;
    }

    /// Release `size` bytes at the beginning of readable().
    TURBOQ_FORCE_INLINE void consume(std::size_t size) noexcept {
        assert(size <= this->size());
        head_ += size;
    }

    /// Drop all data. Keeps the positions within the ring so cache-warm pages stay in use.
    TURBOQ_FORCE_INLINE void clear() noexcept {
        head_ = tail_;
    }
};

} // namespace turboq::reactor
