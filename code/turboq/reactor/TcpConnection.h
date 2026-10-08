// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <linux/time_types.h>
#include <sys/socket.h>
#include <sys/uio.h>

#include <array>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <turboq/Platform.h>

#include "MirroredBuffer.h"
#include "Types.h"
#include "detail/IoHandler.h"
#include "detail/StreamObserver.h"

struct ssl_st;
struct ssl_ctx_st;
struct bio_st;

namespace turboq::reactor {

class Reactor;

enum class TlsVersion : std::uint8_t { Tls12, Tls13 };

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

/// TLS on top of a TCP connection. OpenSSL performs the handshake. Then, preferably, the kernel
/// (kTLS) takes over: rx/tx carry plaintext and the data path is exactly the same as for plain
/// TCP. When it can't (no `tls` module, OpenSSL built without ktls, unsupported cipher), OpenSSL
/// keeps encrypting and decrypting in userspace between the socket and the same rx/tx rings: one
/// more copy per direction and the crypto on the polling thread, nothing else changes for the
/// user. See KernelTls, TcpConnection::kernelTlsOffload().
struct TlsOptions {
    bool enabled = false;
    /// SNI and the name the certificate is verified against. Empty means TcpOptions::host.
    std::string serverName{};
    bool verifyPeer = true;
    /// Trusted CAs. Both empty means the system default store.
    std::string caFile{};
    std::string caPath{};
    TlsVersion minVersion = TlsVersion::Tls12;
    TlsVersion maxVersion = TlsVersion::Tls13;
    std::chrono::milliseconds handshakeTimeout{5000};
    KernelTls kernelTls = KernelTls::Prefer;
};

struct TcpOptions {
    /// Host name or numeric IPv4/IPv6 address. Resolved synchronously (getaddrinfo) on every
    /// connect(), so prefer numeric addresses on latency-critical threads.
    std::string host{};
    std::uint16_t port = 0;
    /// Receive ring size. When it is full, the reactor stops reading from the socket and TCP flow
    /// control pushes back on the peer until the user consumes data.
    std::size_t rxBufferSize = 1u << 20;
    /// Transmit ring size. prepare() fails when there is no room left.
    std::size_t txBufferSize = 1u << 20;
    std::chrono::milliseconds connectTimeout{5000};
    bool noDelay = true;
    /// flush() first tries a synchronous non-blocking ::send() and falls back to io_uring only for
    /// the part that did not fit into the socket buffer. Usually the lowest latency for small
    /// messages; disable to always go through the ring.
    bool directSend = true;
    /// SO_RCVBUF / SO_SNDBUF, 0 keeps the system default.
    int socketRecvBufferSize = 0;
    int socketSendBufferSize = 0;
    TlsOptions tls{};
};

namespace detail {

/// Implementation of TcpConnection (see there). Owned by the handle, released through the reactor.
class TcpCore final : public IoHandler {
public:
    class Rx {
    private:
        friend class TcpCore;
        TcpCore* conn_;

        explicit Rx(TcpCore* conn) noexcept : conn_{conn} {}

    public:
        /// All received and not yet consumed bytes. Empty span if there are none.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto fetch() const noexcept -> std::span<std::byte const> {
            return conn_->rxBuffer_.readable();
        }

        /// Release all fetched bytes.
        TURBOQ_FORCE_INLINE void consume() noexcept {
            this->consume(conn_->rxBuffer_.size());
        }

        /// Release the first `size` bytes, keep the rest.
        TURBOQ_FORCE_INLINE void consume(std::size_t size) noexcept {
            conn_->rxBuffer_.consume(size);
            if (conn_->rxStalled_ && size > 0) [[unlikely]] {
                conn_->resumeRecv();
            }
        }

        [[nodiscard]] TURBOQ_FORCE_INLINE auto empty() const noexcept -> bool {
            return conn_->rxBuffer_.empty();
        }

        /// Ring capacity in bytes.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto capacity() const noexcept -> std::size_t {
            return conn_->rxBuffer_.capacity();
        }

        /// CLOCK_REALTIME (ns) of the reactor poll that delivered the most recent data.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto timestamp() const noexcept -> std::uint64_t {
            return conn_->rxTimestamp_;
        }
    };

    class Tx {
    private:
        friend class TcpCore;
        TcpCore* conn_;

        explicit Tx(TcpCore* conn) noexcept : conn_{conn} {}

    public:
        /// Reserve `size` contiguous bytes. Returns an empty span when there is not enough room or
        /// the connection is closing/closed. Writing before the connection is Ready is allowed:
        /// the data goes out as soon as connect (and the TLS handshake) completes.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto prepare(std::size_t size) noexcept -> std::span<std::byte> {
            if (conn_->state_ >= ConnectionState::Closing || size > conn_->txBuffer_.available()) [[unlikely]] {
                return {};
            }
            conn_->prepared_ = size;
            return conn_->txBuffer_.writable().first(size);
        }

        /// Commit everything reserved by the last prepare().
        TURBOQ_FORCE_INLINE void commit() noexcept {
            this->commit(conn_->prepared_);
        }

        /// Commit the first `size` bytes reserved by the last prepare().
        TURBOQ_FORCE_INLINE void commit(std::size_t size) noexcept {
            assert(size <= conn_->prepared_);
            conn_->txBuffer_.produce(size);
            conn_->prepared_ = 0;
            if (!conn_->txDirty_) [[unlikely]] {
                conn_->markTxDirty();
            }
        }

        /// prepare() + memcpy + commit(). Returns false if there is no room.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto push(std::span<std::byte const> data) noexcept -> bool {
            auto buffer = this->prepare(data.size());
            if (buffer.size() != data.size()) [[unlikely]] {
                return false;
            }
            if (!data.empty()) {
                std::memcpy(buffer.data(), data.data(), data.size());
            }
            this->commit(data.size());
            return true;
        }

        /// Send all committed data now instead of waiting for the next Reactor::poll().
        void flush() noexcept {
            conn_->flushTx();
        }

        /// Committed bytes not yet accepted by the kernel (with userspace TLS: not yet encrypted).
        [[nodiscard]] TURBOQ_FORCE_INLINE auto size() const noexcept -> std::size_t {
            return conn_->txBuffer_.size();
        }

        /// Free space in the tx ring.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto available() const noexcept -> std::size_t {
            return conn_->txBuffer_.available();
        }
    };

private:
    Ring& ring_;
    TcpOptions options_;
    detail::StreamObserver* observer_{nullptr};
    MirroredBuffer rxBuffer_;
    MirroredBuffer txBuffer_;

    int fd_{-1};
    ConnectionState state_{ConnectionState::Idle};
    std::error_code error_;

    std::uint64_t rxTimestamp_{0};
    std::size_t prepared_{0};
    std::uint32_t inflight_{0};
    bool recvInFlight_{false};
    bool sendInFlight_{false};
    bool rxStalled_{false};

    sockaddr_storage address_{};
    socklen_t addressLength_{0};
    __kernel_timespec connectTimeout_{};

    // TLS. With full kernel offload ssl_ lives only during the handshake; with userspace TLS in
    // either direction it stays until the next connect() (rx draining continues after a close).
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
    Rx rx{this};
    Tx tx{this};

    TcpCore(TcpCore const&) = delete;
    TcpCore& operator=(TcpCore const&) = delete;

    ~TcpCore() noexcept override;

    /// Validate options, allocate the rings. Throws std::system_error.
    [[nodiscard]] static auto create(Ring& ring, TcpOptions options) -> TcpCore*;

    /// Start connecting. Allowed in Idle and Closed states (i.e. this is also "reconnect").
    /// Clears both queues. Synchronous failures (resolution, socket()) are returned and also leave
    /// the connection in Closed state with error() set; asynchronous ones arrive via state().
    auto connect() -> std::expected<void, std::error_code>;

    /// Start closing. The connection becomes Closed once all in-flight operations have completed
    /// (observable after one of the next polls). Unread rx data stays available.
    void close() noexcept;

    [[nodiscard]] auto state() const noexcept -> ConnectionState {
        return state_;
    }

    /// Why the connection got closed. Empty after a user initiated close().
    [[nodiscard]] auto error() const noexcept -> std::error_code {
        return error_;
    }

    [[nodiscard]] auto options() const noexcept -> TcpOptions const& {
        return options_;
    }

    /// Negotiated TLS version and cipher ("TLSv1.3", "TLS_AES_128_GCM_SHA256"), empty for plain TCP or
    /// before the handshake completed.
    [[nodiscard]] auto tlsVersion() const noexcept -> std::string_view {
        return tlsVersion_;
    }

    [[nodiscard]] auto tlsCipher() const noexcept -> std::string_view {
        return tlsCipher_;
    }

    /// Layering hook (WebSocket): notified about readiness, data and close from inside the
    /// reactor's completion processing.
    void setObserver(StreamObserver* observer) noexcept {
        observer_ = observer;
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

    /// Native socket descriptor (-1 when closed). For setsockopt()/getsockopt() only: do not read
    /// from or write to it directly.
    [[nodiscard]] auto nativeHandle() const noexcept -> int {
        return fd_;
    }

private:
    TcpCore(Ring& ring, TcpOptions options, MirroredBuffer rxBuffer, MirroredBuffer txBuffer) noexcept;

    void onCompletion(detail::OpCode op, std::int32_t res, std::uint32_t flags) noexcept override;
    void onTxReady() noexcept override;

    [[nodiscard]] auto retirable() const noexcept -> bool override {
        return (state_ == ConnectionState::Closed || state_ == ConnectionState::Idle) && inflight_ == 0;
    }

    void beginRetire() noexcept override {
        observer_ = nullptr;
        this->close();
    }

    auto failSync(std::error_code ec) noexcept -> std::unexpected<std::error_code>;
    void fail(std::error_code ec) noexcept;
    void beginClose() noexcept;
    void finishClose() noexcept;

    void armRecv() noexcept;
    void resumeRecv() noexcept;

    void startHandshake() noexcept;
    void driveHandshake() noexcept;
    void armHandshakePoll(unsigned events) noexcept;
    void completeHandshake() noexcept;
    void onTlsControlRecord(unsigned char recordType, std::size_t size) noexcept;
    void sendCloseNotify() noexcept;
    void releaseSsl() noexcept;
    static void onKeylog(ssl_st const* ssl, char const* line) noexcept;

    // Userspace TLS (fallback).
    [[nodiscard]] auto installKernelRx() noexcept -> std::error_code;
    [[nodiscard]] auto attachUserspaceBio() noexcept -> bool;
    void decryptPending() noexcept;
    void encryptPending() noexcept;
    void sendUserspaceCloseNotify() noexcept;
    void scheduleObserverNotify() noexcept;
    [[nodiscard]] auto wireTx() noexcept -> MirroredBuffer& {
        return tlsUserTx_ ? cipherTx_ : txBuffer_;
    }
    static auto bioRead(bio_st* bio, char* data, std::size_t size, std::size_t* done) noexcept -> int;
    static auto bioWrite(bio_st* bio, char const* data, std::size_t size, std::size_t* done) noexcept -> int;

    void markTxDirty() noexcept;
    void flushTx() noexcept;
    void startSend() noexcept;
    void sendDirect() noexcept;
};

} // namespace detail

/// TCP connection exposed as two byte queues.
///
///   rx.fetch()      -> all unread bytes as one contiguous span (valid until consume())
///   rx.consume(n)   -> release the first n bytes; the rest stays and new data is appended to it
///   tx.prepare(n)   -> contiguous writable span of n bytes inside the tx ring
///   tx.commit(n)    -> make n bytes ready; they are sent on the next Reactor::poll()
///   tx.flush()      -> send everything committed right now
///
/// Owned by the user (movable); the reactor it was created with must outlive it.
/// Not thread-safe: use a connection only from the thread that polls its reactor.
class TcpConnection {
private:
    detail::TcpCore* core_;

public:
    detail::TcpCore::Rx rx;
    detail::TcpCore::Tx tx;

    /// Create a connection served by `reactor` (which must outlive it). Does not connect: call
    /// connect(). Throws std::system_error on invalid options or if the rings can't be allocated.
    TcpConnection(Reactor& reactor, TcpOptions options);

    TcpConnection(TcpConnection const&) = delete;
    TcpConnection& operator=(TcpConnection const&) = delete;

    /// Moves the connection, in-flight operations included. The moved-from object may only be
    /// destroyed or assigned to.
    TcpConnection(TcpConnection&& other) noexcept
        : core_{std::exchange(other.core_, nullptr)}, rx{other.rx}, tx{other.tx} {}

    TcpConnection& operator=(TcpConnection&& other) noexcept {
        if (this != &other) {
            detail::releaseCore(core_);
            core_ = std::exchange(other.core_, nullptr);
            rx = other.rx;
            tx = other.tx;
        }
        return *this;
    }

    /// Closes the connection if needed. Never blocks: when operations are still in flight the
    /// reactor finishes them and frees the connection's memory in a later poll().
    ~TcpConnection() noexcept {
        detail::releaseCore(core_);
    }

    /// Start connecting. Allowed in Idle and Closed states (i.e. this is also "reconnect").
    /// Clears both queues. Synchronous failures (resolution, socket()) are returned and also leave
    /// the connection in Closed state with error() set; asynchronous ones arrive via state().
    auto connect() -> std::expected<void, std::error_code> {
        return core_->connect();
    }

    /// Start closing. The connection becomes Closed once all in-flight operations have completed
    /// (observable after one of the next polls). Unread rx data stays available.
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

    [[nodiscard]] auto options() const noexcept -> TcpOptions const& {
        return core_->options();
    }

    /// Negotiated TLS version and cipher ("TLSv1.3", "TLS_AES_128_GCM_SHA256"), empty for plain TCP
    /// or before the handshake completed.
    [[nodiscard]] auto tlsVersion() const noexcept -> std::string_view {
        return core_->tlsVersion();
    }

    [[nodiscard]] auto tlsCipher() const noexcept -> std::string_view {
        return core_->tlsCipher();
    }

    /// Which directions the kernel handles (kTLS); the rest is OpenSSL in userspace. None for
    /// plain TCP or before the handshake completed.
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
