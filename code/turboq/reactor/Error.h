// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <string>
#include <string_view>
#include <system_error>

#include <turboq/Error.h>
#include <turboq/Platform.h>

namespace turboq::reactor {

using turboq::makePosixErrorCode;

enum class Error {
    InvalidOptions = 1,
    InvalidState,
    AddressResolutionFailed,
    InvalidAddress,
    ConnectTimeout,
    ClosedByPeer,
    SubmissionQueueFull,
    TlsHandshakeFailed,
    TlsHandshakeTimeout,
    KernelTlsModuleMissing,
    OpenSslWithoutKtls,
    KernelTlsSendUnavailable,
    KernelTlsReceiveUnavailable,
    KernelTlsCipherUnsupported,
    TlsKeyUpdateUnsupported,
    TlsUnexpectedRecord,
    WsInvalidUrl,
    WsHandshakeFailed,
    WsHandshakeTimeout,
    WsProtocolError,
    WsMessageTooBig,
    XdpZeroCopyUnavailable,
    XdpProgramRejected,
    XdpProgramAttachFailed,
};

struct ErrorCategory final : public std::error_category {
    constexpr ErrorCategory() = default;

    [[nodiscard]] auto name() const noexcept -> char const* override {
        return "turboq::reactor::Error";
    }

    [[nodiscard]] auto message(int error) const -> std::string override {
        switch (static_cast<Error>(error)) {
        case Error::InvalidOptions: return "invalid options";
        case Error::InvalidState: return "operation is not allowed in the current state";
        case Error::AddressResolutionFailed: return "failed to resolve address";
        case Error::InvalidAddress: return "invalid IP address or endpoint";
        case Error::ConnectTimeout: return "connect timeout";
        case Error::ClosedByPeer: return "connection closed by peer";
        case Error::SubmissionQueueFull: return "io_uring submission queue is full";
        case Error::TlsHandshakeFailed: return "TLS handshake failed";
        case Error::TlsHandshakeTimeout: return "TLS handshake timeout";
        case Error::KernelTlsModuleMissing:
            return "the tls kernel module is not loaded (sudo modprobe tls; to load it at boot: "
                   "echo tls | sudo tee /etc/modules-load.d/tls.conf)";
        case Error::OpenSslWithoutKtls: return "OpenSSL was built without kernel TLS support (OPENSSL_NO_KTLS)";
        case Error::KernelTlsSendUnavailable:
            return "OpenSSL did not hand the TLS session to the kernel (see describeKernelTlsSupport())";
        case Error::KernelTlsReceiveUnavailable: return "kernel TLS receive side could not be set up";
        case Error::KernelTlsCipherUnsupported: return "the negotiated cipher is not supported by kernel TLS";
        case Error::TlsKeyUpdateUnsupported:
            return "peer requested a TLS 1.3 key update, not supported with kernel TLS";
        case Error::TlsUnexpectedRecord: return "unexpected TLS record";
        case Error::WsInvalidUrl: return "invalid WebSocket URL";
        case Error::WsHandshakeFailed: return "WebSocket upgrade rejected or invalid";
        case Error::WsHandshakeTimeout: return "WebSocket upgrade timeout";
        case Error::WsProtocolError: return "WebSocket protocol violation by the server";
        case Error::WsMessageTooBig: return "WebSocket message does not fit into the receive buffer";
        case Error::XdpZeroCopyUnavailable: return "AF_XDP zero-copy is not supported by the driver of this interface";
        case Error::XdpProgramRejected: return "the kernel rejected the XDP program (see XDPConnection::diagnostic())";
        case Error::XdpProgramAttachFailed:
            return "the XDP program could not be attached to the interface (see XDPConnection::diagnostic())";
        default: return "?";
        }
    }
};

[[nodiscard]] constexpr auto getErrorCategory() noexcept -> std::error_category const& {
    static ErrorCategory errorCategory;
    return errorCategory;
}

[[nodiscard]] TURBOQ_FORCE_INLINE auto makeErrorCode(Error e) noexcept -> std::error_code {
    return {static_cast<int>(e), getErrorCategory()};
}

/// Close codes received in a WebSocket Close frame (RFC 6455 7.4).
struct WsCloseCategory final : public std::error_category {
    constexpr WsCloseCategory() = default;

    [[nodiscard]] auto name() const noexcept -> char const* override {
        return "turboq::reactor::WsClose";
    }

    [[nodiscard]] auto message(int code) const -> std::string override {
        std::string text = "WebSocket closed by peer: " + std::to_string(code);
        switch (code) {
        case 1000: return text + " (normal closure)";
        case 1001: return text + " (going away)";
        case 1002: return text + " (protocol error)";
        case 1003: return text + " (unsupported data)";
        case 1005: return text + " (no status code)";
        case 1007: return text + " (invalid payload data)";
        case 1008: return text + " (policy violation)";
        case 1009: return text + " (message too big)";
        case 1010: return text + " (mandatory extension)";
        case 1011: return text + " (internal error)";
        case 1012: return text + " (service restart)";
        case 1013: return text + " (try again later)";
        default: return text;
        }
    }
};

[[nodiscard]] constexpr auto getWsCloseCategory() noexcept -> std::error_category const& {
    static WsCloseCategory category;
    return category;
}

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
