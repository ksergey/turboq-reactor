// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <string>
#include <string_view>
#include <system_error>

#include <turboq/Platform.h>

namespace turboq::reactor::detail {

/// Parsed ws:// or wss:// URL.
struct WsUrl {
    bool secure{false};
    std::string host;       // without brackets for IPv6
    std::uint16_t port{0};  // explicit or 80 / 443
    std::string target;     // path + query, at least "/"
    std::string hostHeader; // value of the Host header (port only if not the default one)
};

[[nodiscard]] auto parseWsUrl(std::string_view url) -> std::expected<WsUrl, std::error_code>;

/// Sec-WebSocket-Accept for a Sec-WebSocket-Key (base64(SHA-1(key + GUID))).
[[nodiscard]] auto computeWsAccept(std::string_view key) -> std::string;

/// Random 16-byte nonce, base64 encoded (Sec-WebSocket-Key).
[[nodiscard]] auto makeWsKey() -> std::string;

/// Fill `data` with random bytes from the kernel.
void fillRandom(void* data, std::size_t size) noexcept;

/// Size of a client (masked) frame header for a payload of `size` bytes.
[[nodiscard]] TURBOQ_FORCE_INLINE constexpr auto wsClientHeaderSize(std::size_t size) noexcept -> std::size_t {
    return 2 + (size <= 125 ? 0 : size <= 0xFFFF ? 2 : 8) + 4;
}

/// Largest client frame header.
inline constexpr std::size_t kWsMaxClientHeaderSize = 14;

/// Write a masked client frame header, returns its size (== wsClientHeaderSize(size)).
TURBOQ_FORCE_INLINE auto writeWsClientHeader(
    std::byte* out, bool fin, std::uint8_t opcode, std::size_t size, std::uint32_t maskKey) noexcept -> std::size_t {
    out[0] = static_cast<std::byte>((fin ? 0x80 : 0x00) | opcode);
    std::size_t pos = 2;
    if (size <= 125) {
        out[1] = static_cast<std::byte>(0x80 | size);
    } else if (size <= 0xFFFF) {
        out[1] = static_cast<std::byte>(0x80 | 126);
        out[2] = static_cast<std::byte>(size >> 8);
        out[3] = static_cast<std::byte>(size);
        pos = 4;
    } else {
        out[1] = static_cast<std::byte>(0x80 | 127);
        for (int i = 0; i < 8; ++i) {
            out[2 + i] = static_cast<std::byte>(static_cast<std::uint64_t>(size) >> (56 - 8 * i));
        }
        pos = 10;
    }
    std::memcpy(out + pos, &maskKey, sizeof(maskKey));
    return pos + sizeof(maskKey);
}

/// XOR `data` with the 4-byte masking key as laid out in memory (RFC 6455 5.3), 8 bytes at a time.
TURBOQ_FORCE_INLINE void applyWsMask(std::byte* data, std::size_t size, std::uint32_t maskKey) noexcept {
    if (maskKey == 0) {
        return;
    }
    std::uint64_t key8;
    std::memcpy(&key8, &maskKey, 4);
    std::memcpy(std::bit_cast<char*>(&key8) + 4, &maskKey, 4);
    std::size_t i = 0;
    for (; i + 8 <= size; i += 8) {
        std::uint64_t word;
        std::memcpy(&word, data + i, 8);
        word ^= key8;
        std::memcpy(data + i, &word, 8);
    }
    auto const* key = std::bit_cast<unsigned char const*>(&maskKey);
    for (; i < size; ++i) {
        data[i] ^= static_cast<std::byte>(key[i & 3]);
    }
}

} // namespace turboq::reactor::detail
