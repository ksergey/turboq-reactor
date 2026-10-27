// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <system_error>

namespace turboq::reactor {

enum class TLSVersion : std::uint8_t { Tls12, Tls13 };

/// What to do when the kernel can't take over a TLS session (kTLS).
enum class KernelTls : std::uint8_t {
    /// Kernel TLS when possible, otherwise OpenSSL in userspace (per direction).
    Prefer,
    /// Kernel TLS or fail with a kernel TLS error (isKernelTlsError()).
    Require,
    /// Always OpenSSL in userspace.
    Disable,
};

/// Which directions of an established TLS session the kernel handles.
enum class KernelTlsOffload : std::uint8_t { None, SendOnly, ReceiveOnly, Full };

struct TLSOptions {
    /// SNI and the host name the certificate is verified against. None (or a numeric address):
    /// no SNI, the certificate must list the IP address (TcpOptions::endpoint) in its SANs.
    std::optional<std::string> serverName{};
    bool verifyPeer = true;
    /// Trusted CAs (PEM file, hashed directory). Both none: the system default store.
    std::optional<std::string> caFile{};
    std::optional<std::string> caPath{};
    TLSVersion minVersion = TLSVersion::Tls12;
    TLSVersion maxVersion = TLSVersion::Tls13;
    std::chrono::milliseconds handshakeTimeout{5000};
    KernelTls kernelTls = KernelTls::Prefer;
};

// TLS error categories and kernel TLS diagnostics (implemented in detail/Tls.cpp).

/// OpenSSL library errors (ERR_get_error()), packed as (lib << 23) | reason.
[[nodiscard]] auto getTlsErrorCategory() noexcept -> std::error_category const&;

/// Certificate verification errors (X509_V_ERR_*).
[[nodiscard]] auto getX509ErrorCategory() noexcept -> std::error_category const&;

/// Fatal alerts received from the peer (TLS AlertDescription).
[[nodiscard]] auto getTlsAlertCategory() noexcept -> std::error_category const&;

/// setsockopt(TLS_RX) rejected by the kernel; the value is the errno.
[[nodiscard]] auto getKernelTlsRxErrorCategory() noexcept -> std::error_category const&;

/// One line describing what kernel TLS needs and what is there, for logs when a TLS connection
/// fails with a kernel TLS error: OpenSSL version and whether it was built with ktls, whether the
/// tls module is loaded, whether this process could load it on demand (CAP_NET_ADMIN).
[[nodiscard]] auto describeKernelTlsSupport() -> std::string;

/// True for the errors that mean "kernel TLS is not usable here" (as opposed to network or
/// certificate problems).
[[nodiscard]] auto isKernelTlsError(std::error_code ec) noexcept -> bool;

} // namespace turboq::reactor
