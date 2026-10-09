// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <linux/time_types.h>
#include <sys/socket.h>
#include <sys/uio.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "TCPConnection.h"

struct ssl_st;
struct ssl_ctx_st;
struct bio_st;
struct bio_method_st;

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
    /// no SNI, the certificate must list the IP address (TCPOptions::endpoint) in its SANs.
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

namespace detail {

/// Implementation of TLSConnection (see there): TLS as a layer on top of TCPCore.
template <typename Backend>
class TLSCore final : public TCPCore<Backend> {
private:
    using Base = TCPCore<Backend>;
    using Base::allocateRing;
    using Base::checkClosed;
    using Base::drainOnClose;
    using Base::fail;
    using Base::fd_;
    using Base::handleRecvResult;
    using Base::inflight_;
    using Base::observer_;
    using Base::options_;
    using Base::recvInFlight_;
    using Base::ring_;
    using Base::rxBuffer_;
    using Base::rxStalled_;
    using Base::rxTimestamp_;
    using Base::sendInFlight_;
    using Base::sendNow;
    using Base::startStreaming;
    using Base::state_;
    using Base::submitRecv;
    using Base::submitSend;
    using Base::txBuffer_;
    using Base::txDirty_;
    using Base::validate;

    TLSOptions tlsOptions_;

    // With full kernel offload ssl_ lives only during the handshake; with userspace TLS in either
    // direction it stays until the next connect() (rx draining continues after a close).
    ssl_ctx_st* sslContext_{nullptr};
    ssl_st* ssl_{nullptr};
    bool tlsActive_{false};
    bool tlsUserRx_{false}; // OpenSSL decrypts: socket -> cipherRx_ -> SSL_read -> rxBuffer_
    bool tlsUserTx_{false}; // OpenSSL encrypts: txBuffer_ -> SSL_write -> cipherTx_ -> socket
    bool rxEof_{false};     // the peer closed the TCP stream (userspace TLS rx)
    bool observerPending_{false};
    MirroredBuffer cipherRx_; // allocated unless KernelTls::Require
    MirroredBuffer cipherTx_;
    std::string_view tlsVersion_{};
    std::string_view tlsCipher_{};
    std::chrono::steady_clock::time_point handshakeDeadline_{};
    __kernel_timespec handshakeTimeout_{};
    std::array<std::uint8_t, 48> serverTrafficSecret_{};
    std::size_t serverTrafficSecretSize_{0};
    // kTLS receive goes through recvmsg: the record type of non-data records (alerts, session
    // tickets, key updates) arrives as a control message.
    iovec recvIov_{};
    msghdr recvMsg_{};
    alignas(cmsghdr) unsigned char recvControl_[CMSG_SPACE(sizeof(unsigned char))]{};

public:
    ~TLSCore() noexcept override;

    /// Validate options, allocate the rings. Throws std::system_error.
    [[nodiscard]] static auto create(Backend& ring, TCPOptions tcp, TLSOptions tls) -> std::unique_ptr<TLSCore>;

    [[nodiscard]] auto tlsOptions() const noexcept -> TLSOptions const& {
        return tlsOptions_;
    }

    [[nodiscard]] auto tlsVersion() const noexcept -> std::string_view {
        return tlsVersion_;
    }

    [[nodiscard]] auto tlsCipher() const noexcept -> std::string_view {
        return tlsCipher_;
    }

    [[nodiscard]] auto kernelTlsOffload() const noexcept -> KernelTlsOffload {
        if (!tlsActive_) {
            return KernelTlsOffload::None;
        }
        if (tlsUserRx_) {
            return tlsUserTx_ ? KernelTlsOffload::None : KernelTlsOffload::SendOnly;
        }
        return tlsUserTx_ ? KernelTlsOffload::ReceiveOnly : KernelTlsOffload::Full;
    }

private:
    TLSCore(Backend& ring, TCPOptions tcp, TLSOptions tls, MirroredBuffer rxBuffer, MirroredBuffer txBuffer,
        MirroredBuffer cipherRx, MirroredBuffer cipherTx) noexcept;

    void onCompletion(OpCode op, std::int32_t res, std::uint32_t flags) noexcept override;
    void onTxReady() noexcept override;

    // TCPCore hooks.
    void onConnected() noexcept override;
    void armRecv() noexcept override;
    void resumeRecv() noexcept override;
    void startSend() noexcept override;
    void sendDirect() noexcept override;
    void onCloseRequested() noexcept override;
    void onClosed() noexcept override;
    void resetSession() noexcept override;

    // Handshake.
    void startHandshake() noexcept;
    void driveHandshake() noexcept;
    void armHandshakePoll(unsigned events) noexcept;
    void onHandshakePoll(std::int32_t res) noexcept;
    void completeHandshake() noexcept;
    [[nodiscard]] auto installKernelRx() noexcept -> std::error_code;
    static void onKeylog(ssl_st const* ssl, char const* line) noexcept;
    void releaseSsl() noexcept;

    // Kernel TLS.
    void onKernelRecv(std::int32_t res) noexcept;
    void onTlsControlRecord(unsigned char recordType, std::size_t size) noexcept;
    void sendCloseNotify() noexcept;

    // Userspace TLS (fallback).
    [[nodiscard]] auto attachUserspaceBio() noexcept -> bool;
    void onUserspaceRecv(std::int32_t res) noexcept;
    void decryptPending() noexcept;
    void encryptPending() noexcept;
    void scheduleObserverNotify() noexcept;
    [[nodiscard]] static auto bioMethod() noexcept -> bio_method_st*;
    static auto bioRead(bio_st* bio, char* data, std::size_t size, std::size_t* done) noexcept -> int;
    static auto bioWrite(bio_st* bio, char const* data, std::size_t size, std::size_t* done) noexcept -> int;
};

} // namespace detail

/// TLS client connection: the same two byte queues as TCPConnection, carrying plaintext.
///
/// OpenSSL performs the handshake (state Handshaking). Then, preferably, the kernel (kTLS) takes
/// over: the data path is exactly the plain TCP one. When it can't (no `tls` module, OpenSSL built
/// without ktls), OpenSSL keeps encrypting and decrypting in userspace between the socket and the
/// same rings: one more copy per direction and the crypto on the polling thread, nothing else
/// changes. See TLSOptions::kernelTls, kernelTlsOffload().
template <typename Backend>
class TLSConnection {
private:
    detail::CorePtr<detail::TLSCore<Backend>> core_;

public:
    typename detail::TCPCore<Backend>::Rx rx;
    typename detail::TCPCore<Backend>::Tx tx;

    /// Create a connection served by `reactor` (which must outlive it). Does not connect: call
    /// connect(). Throws std::system_error on invalid options or if the rings can't be allocated.
    TLSConnection(Reactor<Backend>& reactor, TCPOptions tcp, TLSOptions tls = {});

    TLSConnection(TLSConnection const&) = delete;
    TLSConnection& operator=(TLSConnection const&) = delete;

    /// Moves the connection, in-flight operations included. The moved-from object may only be
    /// destroyed or assigned to.
    TLSConnection(TLSConnection&& other) noexcept = default;

    TLSConnection& operator=(TLSConnection&& other) noexcept = default;

    /// Sends close_notify if Ready and closes. Never blocks (see TCPConnection::~TCPConnection()).
    ~TLSConnection() noexcept = default;

    /// Connect and handshake: Connecting -> Handshaking -> Ready. Allowed in Idle and Closed states
    /// (i.e. this is also "reconnect"). Clears both queues.
    auto connect() -> std::expected<void, std::error_code> {
        return core_->connect();
    }

    /// Start closing: committed data, then close_notify, as far as the socket takes them without
    /// blocking. Data received before the close stays readable.
    void close() noexcept {
        core_->close();
    }

    [[nodiscard]] auto state() const noexcept -> ConnectionState {
        return core_->state();
    }

    /// Why the connection got closed. Empty after a user initiated close().
    [[nodiscard]] auto error() const noexcept -> std::error_code {
        return core_->error();
    }

    [[nodiscard]] auto options() const noexcept -> TCPOptions const& {
        return core_->options();
    }

    [[nodiscard]] auto tlsOptions() const noexcept -> TLSOptions const& {
        return core_->tlsOptions();
    }

    /// Negotiated TLS version and cipher ("TLSv1.3", "TLS_AES_128_GCM_SHA256"), empty before the
    /// handshake completed.
    [[nodiscard]] auto tlsVersion() const noexcept -> std::string_view {
        return core_->tlsVersion();
    }

    [[nodiscard]] auto tlsCipher() const noexcept -> std::string_view {
        return core_->tlsCipher();
    }

    /// Which directions the kernel handles (kTLS); the rest is OpenSSL in userspace. None before the
    /// handshake completed.
    [[nodiscard]] auto kernelTlsOffload() const noexcept -> KernelTlsOffload {
        return core_->kernelTlsOffload();
    }

    /// Native socket descriptor (-1 when closed). For setsockopt()/getsockopt() only: do not read
    /// from or write to it directly.
    [[nodiscard]] auto nativeHandle() const noexcept -> int {
        return core_->nativeHandle();
    }
};

} // namespace turboq::reactor
