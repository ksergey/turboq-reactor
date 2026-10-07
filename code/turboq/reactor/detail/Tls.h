// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <linux/tls.h>
#include <sys/socket.h>

#include <array>
#include <cstdint>
#include <expected>
#include <span>
#include <system_error>

struct ssl_st;
struct ssl_ctx_st;

namespace turboq::reactor {

struct TlsOptions;

/// OpenSSL library errors (ERR_get_error()), packed as (lib << 23) | reason.
[[nodiscard]] auto getTlsErrorCategory() noexcept -> std::error_category const&;

/// Certificate verification errors (X509_V_ERR_*).
[[nodiscard]] auto getX509ErrorCategory() noexcept -> std::error_category const&;

/// Fatal alerts received from the peer (TLS AlertDescription).
[[nodiscard]] auto getTlsAlertCategory() noexcept -> std::error_category const&;

namespace detail {

/// Pop the most relevant error from OpenSSL's thread-local error queue (and clear the rest).
/// Returns `fallback` when the queue is empty.
[[nodiscard]] auto popTlsError(std::error_code fallback) noexcept -> std::error_code;

/// Client SSL_CTX configured for kernel TLS: SSL_OP_ENABLE_KTLS, only ciphers the kernel
/// implements (AES-GCM-128/256, ChaCha20-Poly1305), TLS 1.2+, peer verification per options.
[[nodiscard]] auto createClientContext(
    TlsOptions const& options) noexcept -> std::expected<ssl_ctx_st*, std::error_code>;

void freeContext(ssl_ctx_st* ctx) noexcept;
void freeSsl(ssl_st* ssl) noexcept;

/// Largest TLS 1.3 traffic secret (SHA-384).
inline constexpr std::size_t kMaxTrafficSecretSize = 48;

/// setsockopt(SOL_TLS, TLS_RX/TLS_TX) argument: one of the tls12_crypto_info_* structs from
/// <linux/tls.h> (selected by the leading tls_crypto_info::cipher_type), `size` bytes long.
struct KernelTlsCryptoInfo {
    alignas(8) std::array<unsigned char, 64> bytes{};
    socklen_t size{0};
};

/// Kernel crypto state for one direction of a TLS 1.3 connection: derives key and IV from the
/// traffic secret (RFC 8446 7.3, HKDF-Expand-Label) and lays them out the way the kernel expects.
/// `cipherSuite` is the IANA id (0x1301 TLS_AES_128_GCM_SHA256, 0x1302 TLS_AES_256_GCM_SHA384,
/// 0x1303 TLS_CHACHA20_POLY1305_SHA256), `sequence` the number of records already processed with
/// this secret.
[[nodiscard]] auto makeTls13CryptoInfo(std::uint16_t cipherSuite, std::span<std::uint8_t const> trafficSecret,
    std::uint64_t sequence) noexcept -> std::expected<KernelTlsCryptoInfo, std::error_code>;

} // namespace detail
} // namespace turboq::reactor
