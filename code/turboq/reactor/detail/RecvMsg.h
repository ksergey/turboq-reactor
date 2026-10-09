// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <sys/socket.h>

#include <bit>
#include <cstddef>
#include <cstdint>

namespace turboq::reactor::detail {

/// Header of a datagram received by a multishot recvmsg: the kernel's struct io_uring_recvmsg_out
/// (linux/io_uring.h). Defined here so that the layout does not need liburing: EpollBackend
/// produces the same layout, IoUringBackend.cpp checks that the two match.
///
/// A receive buffer is laid out as RecvMsgOut | name | control | payload with no padding, where
/// the name and control sizes are msg_namelen and msg_controllen of the msghdr the receive was
/// started with. The functions below are liburing's io_uring_recvmsg_*() helpers.
struct RecvMsgOut {
    std::uint32_t namelen;
    std::uint32_t controllen;
    std::uint32_t payloadlen;
    std::uint32_t flags;
};

/// The header of a completed receive of `length` bytes (the completion result), or nullptr if the
/// result is an error or too short for the layout.
[[nodiscard]] inline auto recvMsgValidate(
    std::byte* buffer, std::int32_t length, msghdr const& layout) noexcept -> RecvMsgOut* {
    auto const size = static_cast<std::size_t>(static_cast<std::uint32_t>(length));
    auto const header = sizeof(RecvMsgOut);
    if (length < 0 || size < header) {
        return nullptr;
    }
    // Each addition is checked separately to avoid integer overflow.
    if (layout.msg_namelen > size - header || layout.msg_controllen > size - header - layout.msg_namelen) {
        return nullptr;
    }
    return std::bit_cast<RecvMsgOut*>(buffer);
}

/// The sender address (out->namelen bytes are valid, up to msg_namelen).
[[nodiscard]] inline auto recvMsgName(RecvMsgOut* out) noexcept -> std::byte* {
    return std::bit_cast<std::byte*>(out + 1);
}

[[nodiscard]] inline auto recvMsgPayload(RecvMsgOut* out, msghdr const& layout) noexcept -> std::byte* {
    return recvMsgName(out) + layout.msg_namelen + layout.msg_controllen;
}

/// Payload bytes in a completed receive of `length` bytes.
[[nodiscard]] inline auto recvMsgPayloadLength(
    RecvMsgOut* out, std::int32_t length, msghdr const& layout) noexcept -> std::uint32_t {
    if (length < 0) {
        return 0;
    }
    auto const* const start = recvMsgPayload(out, layout);
    auto const* const end = std::bit_cast<std::byte*>(out) + length;
    return start < end ? static_cast<std::uint32_t>(end - start) : 0;
}

[[nodiscard]] inline auto recvMsgFirstCmsg(RecvMsgOut* out, msghdr const& layout) noexcept -> cmsghdr* {
    if (out->controllen < sizeof(cmsghdr)) {
        return nullptr;
    }
    return std::bit_cast<cmsghdr*>(recvMsgName(out) + layout.msg_namelen);
}

[[nodiscard]] inline auto recvMsgNextCmsg(RecvMsgOut* out, msghdr const& layout, cmsghdr* cmsg) noexcept -> cmsghdr* {
    if (cmsg->cmsg_len < sizeof(cmsghdr)) {
        return nullptr;
    }
    auto const* const end = recvMsgName(out) + layout.msg_namelen + out->controllen;
    auto* const next = std::bit_cast<std::byte*>(cmsg) + CMSG_ALIGN(cmsg->cmsg_len);
    if (next + sizeof(cmsghdr) > end) {
        return nullptr;
    }
    auto* const result = std::bit_cast<cmsghdr*>(next);
    if (next + CMSG_ALIGN(result->cmsg_len) > end) {
        return nullptr;
    }
    return result;
}

} // namespace turboq::reactor::detail
