// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "TcpConnection.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>

#include <algorithm>
#include <string>

#include "Error.h"
#include "Reactor.h"
#include "detail/Tls.h"

namespace turboq::reactor {
namespace {

[[nodiscard]] auto resolve(std::string const& host, std::uint16_t port, sockaddr_storage& address,
    socklen_t& addressLength) noexcept -> std::error_code {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICSERV;

    auto const service = std::to_string(port);
    addrinfo* result = nullptr;
    if (::getaddrinfo(host.c_str(), service.c_str(), &hints, &result) != 0 || result == nullptr) {
        return makeErrorCode(Error::AddressResolutionFailed);
    }
    std::memcpy(&address, result->ai_addr, result->ai_addrlen);
    addressLength = result->ai_addrlen;
    ::freeaddrinfo(result);
    return {};
}

[[nodiscard]] auto setIntOption(int fd, int level, int name, int value) noexcept -> std::error_code {
    if (::setsockopt(fd, level, name, &value, sizeof(value)) != 0) {
        return makePosixErrorCode(errno);
    }
    return {};
}

[[nodiscard]] auto setNonBlocking(int fd, bool enabled) noexcept -> std::error_code {
    int const flags = ::fcntl(fd, F_GETFL);
    if (flags < 0 || ::fcntl(fd, F_SETFL, enabled ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK)) < 0) {
        return makePosixErrorCode(errno);
    }
    return {};
}

[[nodiscard]] auto isIpLiteral(std::string const& host) noexcept -> bool {
    in6_addr addr;
    return ::inet_pton(AF_INET, host.c_str(), &addr) == 1 || ::inet_pton(AF_INET6, host.c_str(), &addr) == 1;
}

[[nodiscard]] auto hexValue(char c) noexcept -> int {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

/// OpenSSL's socket BIO writes with write(), which raises SIGPIPE when the peer is gone. Block
/// SIGPIPE for the duration of an OpenSSL call and swallow it if it was raised, so the library never
/// kills a process that did not ignore SIGPIPE. Handshake only: the data path uses MSG_NOSIGNAL.
class SigPipeGuard {
private:
    sigset_t previous_;
    bool wasPending_{false};

public:
    SigPipeGuard() noexcept {
        sigset_t pending;
        sigemptyset(&pending);
        ::sigpending(&pending);
        wasPending_ = sigismember(&pending, SIGPIPE) == 1;

        sigset_t block;
        sigemptyset(&block);
        sigaddset(&block, SIGPIPE);
        ::pthread_sigmask(SIG_BLOCK, &block, &previous_);
    }

    ~SigPipeGuard() noexcept {
        if (!wasPending_) {
            sigset_t pending;
            sigemptyset(&pending);
            ::sigpending(&pending);
            if (sigismember(&pending, SIGPIPE) == 1) {
                sigset_t pipe;
                sigemptyset(&pipe);
                sigaddset(&pipe, SIGPIPE);
                timespec const zero{0, 0};
                while (::sigtimedwait(&pipe, nullptr, &zero) == -1 && errno == EINTR) {}
            }
        }
        ::pthread_sigmask(SIG_SETMASK, &previous_, nullptr);
    }
};

/// Ciphertext rings must hold at least one full TLS record (16 KiB + overhead) to make progress.
constexpr std::size_t kMinCipherRingSize = 64u * 1024;

// TLS record content types and handshake message types (RFC 8446).
constexpr unsigned char kRecordAlert = 21;
constexpr unsigned char kRecordHandshake = 22;
constexpr unsigned char kRecordApplicationData = 23;
constexpr unsigned char kHandshakeNewSessionTicket = 4;
constexpr unsigned char kHandshakeKeyUpdate = 24;
constexpr unsigned char kAlertCloseNotify = 0;

} // namespace

namespace detail {

auto TcpCore::create(Ring& ring, TcpOptions options) -> TcpCore* {
    if (options.host.empty() || options.port == 0 || options.rxBufferSize == 0 || options.txBufferSize == 0) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "TcpConnection"};
    }
    auto rxBuffer = MirroredBuffer::create({.capacityHint = options.rxBufferSize});
    if (!rxBuffer) {
        throw std::system_error{rxBuffer.error(), "MirroredBuffer::create (rx)"};
    }
    auto txBuffer = MirroredBuffer::create({.capacityHint = options.txBufferSize});
    if (!txBuffer) {
        throw std::system_error{txBuffer.error(), "MirroredBuffer::create (tx)"};
    }
    MirroredBuffer cipherRx;
    MirroredBuffer cipherTx;
    if (options.tls.enabled && options.tls.kernelTls != KernelTls::Require) {
        auto rx = MirroredBuffer::create({.capacityHint = std::max(options.rxBufferSize, kMinCipherRingSize)});
        auto tx = MirroredBuffer::create({.capacityHint = std::max(options.txBufferSize, kMinCipherRingSize)});
        if (!rx || !tx) {
            throw std::system_error{!rx ? rx.error() : tx.error(), "MirroredBuffer::create (TLS)"};
        }
        cipherRx = std::move(*rx);
        cipherTx = std::move(*tx);
    }
    auto* core = new TcpCore{ring, std::move(options), std::move(*rxBuffer), std::move(*txBuffer)};
    core->cipherRx_ = std::move(cipherRx);
    core->cipherTx_ = std::move(cipherTx);
    return core;
}

TcpCore::TcpCore(Ring& ring, TcpOptions options, MirroredBuffer rxBuffer, MirroredBuffer txBuffer) noexcept
    : ring_{ring}, options_{std::move(options)}, rxBuffer_{std::move(rxBuffer)}, txBuffer_{std::move(txBuffer)} {
    auto const seconds = std::chrono::duration_cast<std::chrono::seconds>(options_.connectTimeout);
    connectTimeout_.tv_sec = seconds.count();
    connectTimeout_.tv_nsec =
        std::chrono::duration_cast<std::chrono::nanoseconds>(options_.connectTimeout - seconds).count();
}

TcpCore::~TcpCore() noexcept {
    this->releaseSsl();
    if (sslContext_) {
        detail::freeContext(sslContext_);
    }
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

auto TcpCore::connect() -> std::expected<void, std::error_code> {
    if (state_ != ConnectionState::Idle && state_ != ConnectionState::Closed) {
        return std::unexpected(makeErrorCode(Error::InvalidState));
    }

    rxBuffer_.clear();
    txBuffer_.clear();
    prepared_ = 0;
    rxTimestamp_ = 0;
    rxStalled_ = false;
    txDirty_ = false;
    error_.clear();
    tlsActive_ = false;
    tlsUserRx_ = false;
    tlsUserTx_ = false;
    rxEof_ = false;
    observerPending_ = false;
    cipherRx_.clear();
    cipherTx_.clear();
    tlsVersion_ = {};
    tlsCipher_ = {};
    this->releaseSsl();

    if (auto ec = resolve(options_.host, options_.port, address_, addressLength_)) {
        return this->failSync(ec);
    }

    fd_ = ::socket(address_.ss_family, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
    if (fd_ < 0) {
        return this->failSync(makePosixErrorCode(errno));
    }
    if (options_.noDelay) {
        if (auto ec = setIntOption(fd_, IPPROTO_TCP, TCP_NODELAY, 1)) {
            return this->failSync(ec);
        }
    }
    if (options_.socketRecvBufferSize > 0) {
        if (auto ec = setIntOption(fd_, SOL_SOCKET, SO_RCVBUF, options_.socketRecvBufferSize)) {
            return this->failSync(ec);
        }
    }
    if (options_.socketSendBufferSize > 0) {
        if (auto ec = setIntOption(fd_, SOL_SOCKET, SO_SNDBUF, options_.socketSendBufferSize)) {
            return this->failSync(ec);
        }
    }

    // connect + linked timeout must land in the same submission: reserve both SQEs up front.
    if (!ring_.ensureSqSpace(2)) {
        return this->failSync(makeErrorCode(Error::SubmissionQueueFull));
    }

    auto* sqe = ring_.getSqe();
    ::io_uring_prep_connect(sqe, fd_, reinterpret_cast<sockaddr const*>(&address_), addressLength_);
    sqe->flags |= IOSQE_IO_LINK;
    ::io_uring_sqe_set_data64(sqe, detail::encodeUserData(this, detail::OpCode::Connect));

    auto* timeoutSqe = ring_.getSqe();
    ::io_uring_prep_link_timeout(timeoutSqe, &connectTimeout_, 0);
    ::io_uring_sqe_set_data64(timeoutSqe, detail::encodeUserData(this, detail::OpCode::ConnectTimeout));

    inflight_ += 2;
    state_ = ConnectionState::Connecting;
    return {};
}

void TcpCore::close() noexcept {
    switch (state_) {
    case ConnectionState::Idle: state_ = ConnectionState::Closed; break;
    case ConnectionState::Connecting: [[fallthrough]];
    case ConnectionState::Handshaking: this->beginClose(); break;
    case ConnectionState::Ready:
        // Committed data goes out before close_notify / FIN (best effort, never blocks).
        this->flushOnClose();
        if (tlsActive_) {
            if (tlsUserTx_) {
                this->sendUserspaceCloseNotify();
            } else {
                this->sendCloseNotify();
            }
        }
        this->beginClose();
        break;
    case ConnectionState::Closing: [[fallthrough]];
    case ConnectionState::Closed: break;
    }
}

void TcpCore::onCompletion(detail::OpCode op, std::int32_t res, [[maybe_unused]] std::uint32_t flags) noexcept {
    assert(inflight_ > 0);
    --inflight_;

    switch (op) {
    case detail::OpCode::Connect:
        if (state_ != ConnectionState::Connecting) {
            break; // closing
        }
        if (res == 0) {
            if (options_.tls.enabled) {
                this->startHandshake();
            } else {
                state_ = ConnectionState::Ready;
                this->armRecv();
                // Data committed while connecting goes out right away.
                this->startSend();
                if (observer_) {
                    observer_->onStreamReady();
                }
            }
        } else if (res == -ECANCELED) {
            // Cancelled by the linked timeout (a user close() would have moved us to Closing).
            this->fail(makeErrorCode(Error::ConnectTimeout));
        } else {
            this->fail(makePosixErrorCode(-res));
        }
        break;

    case detail::OpCode::ConnectTimeout:
        // -ETIME: the timeout fired (the connect CQE carries -ECANCELED);
        // -ECANCELED / -ENOENT: connect finished first. Nothing to do either way.
        break;

    case detail::OpCode::HandshakePoll:
        if (state_ != ConnectionState::Handshaking) {
            break; // closing
        }
        if (res == -ECANCELED) {
            this->fail(makeErrorCode(Error::TlsHandshakeTimeout));
        } else if (res < 0) {
            this->fail(makePosixErrorCode(-res));
        } else {
            this->driveHandshake();
        }
        break;

    case detail::OpCode::Recv:
        recvInFlight_ = false;
        if (tlsUserRx_) {
            // Userspace TLS: ciphertext lands in cipherRx_, OpenSSL decrypts into rxBuffer_.
            if (res > 0) [[likely]] {
                cipherRx_.produce(static_cast<std::size_t>(res));
                rxTimestamp_ = ring_.now();
                auto const before = rxBuffer_.size();
                this->decryptPending();
                if (state_ == ConnectionState::Ready) {
                    this->armRecv();
                }
                if (observer_ && rxBuffer_.size() != before) {
                    observer_->onStreamData();
                }
            } else if (res == 0) {
                // Decrypt what is left (close_notify included) before reporting the close.
                rxEof_ = true;
                auto const before = rxBuffer_.size();
                this->decryptPending();
                if (observer_ && rxBuffer_.size() != before) {
                    observer_->onStreamData();
                }
                this->fail(makeErrorCode(Error::ClosedByPeer));
            } else if (res == -EINTR || res == -EAGAIN) {
                if (state_ == ConnectionState::Ready) {
                    this->armRecv();
                }
            } else if (res != -ECANCELED) {
                this->fail(makePosixErrorCode(-res));
            }
            break;
        }
        if (res > 0 && tlsActive_) {
            // kTLS reports the record type of what was just read in a control message. The buffer
            // was zeroed before the receive, so a missing control message reads as cmsg_len == 0.
            unsigned char recordType = kRecordApplicationData;
            auto const* cmsg = reinterpret_cast<cmsghdr const*>(recvControl_);
            if (cmsg->cmsg_len != 0 && cmsg->cmsg_level == SOL_TLS && cmsg->cmsg_type == TLS_GET_RECORD_TYPE) {
                recordType = *CMSG_DATA(cmsg);
            }
            if (recordType != kRecordApplicationData) [[unlikely]] {
                this->onTlsControlRecord(recordType, static_cast<std::size_t>(res));
                if (state_ == ConnectionState::Ready) {
                    this->armRecv();
                }
                break;
            }
        }
        if (res > 0) [[likely]] {
            rxBuffer_.produce(static_cast<std::size_t>(res));
            rxTimestamp_ = ring_.now();
            if (state_ == ConnectionState::Ready) {
                this->armRecv();
            }
            if (observer_) {
                observer_->onStreamData();
            }
        } else if (res == 0) {
            this->fail(makeErrorCode(Error::ClosedByPeer));
        } else if (res == -EINTR || res == -EAGAIN) {
            if (state_ == ConnectionState::Ready) {
                this->armRecv();
            }
        } else if (res != -ECANCELED) {
            this->fail(makePosixErrorCode(-res));
        }
        break;

    case detail::OpCode::Send:
        sendInFlight_ = false;
        if (res > 0) [[likely]] {
            this->wireTx().consume(static_cast<std::size_t>(res));
            if (tlsUserRx_ && !cipherRx_.empty()) {
                this->decryptPending(); // OpenSSL may have been waiting for room to answer (key update)
            }
            // Short send or data committed meanwhile: keep going.
            this->startSend();
        } else if (res == -EINTR || res == -EAGAIN) {
            this->startSend();
        } else if (res != -ECANCELED) {
            this->fail(makePosixErrorCode(-res));
        }
        break;

    case detail::OpCode::Cancel: break;
    }

    if (state_ == ConnectionState::Closing && inflight_ == 0) {
        this->finishClose();
    }
}

auto TcpCore::failSync(std::error_code ec) noexcept -> std::unexpected<std::error_code> {
    this->releaseSsl();
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    state_ = ConnectionState::Closed;
    error_ = ec;
    return std::unexpected(ec);
}

void TcpCore::fail(std::error_code ec) noexcept {
    if (state_ == ConnectionState::Closing || state_ == ConnectionState::Closed) {
        return;
    }
    error_ = ec;
    this->beginClose();
}

void TcpCore::beginClose() noexcept {
    state_ = ConnectionState::Closing;
    prepared_ = 0;
    txDirty_ = false;

    if (fd_ >= 0) {
        // Wakes up a pending recv (returns 0) and send (EPIPE) on an established connection.
        ::shutdown(fd_, SHUT_RDWR);
    }
    if (inflight_ == 0) {
        this->finishClose();
        return;
    }
    // Covers what shutdown() does not wake up, e.g. a connect in progress.
    if (auto* sqe = ring_.getSqe(); sqe) {
        ::io_uring_prep_cancel_fd(sqe, fd_, IORING_ASYNC_CANCEL_ALL);
        ::io_uring_sqe_set_data64(sqe, detail::encodeUserData(this, detail::OpCode::Cancel));
        ++inflight_;
    }
}

void TcpCore::finishClose() noexcept {
    if (!tlsUserRx_) {
        this->releaseSsl(); // with userspace rx, kept so that consume() can drain what arrived
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    recvInFlight_ = false;
    sendInFlight_ = false;
    state_ = ConnectionState::Closed;
    if (observer_) {
        observer_->onStreamClosed();
    }
}

void TcpCore::armRecv() noexcept {
    // Userspace TLS receives ciphertext into its own ring; rxStalled_ then tracks the plaintext
    // ring (set by decryptPending()).
    auto const buffer = tlsUserRx_ ? cipherRx_.writable() : rxBuffer_.writable();
    if (tlsUserRx_ && buffer.empty()) {
        return; // both rings full: consume() decrypts and re-arms
    }
    if (buffer.empty()) {
        // Ring is full: stop reading, TCP flow control pushes back on the peer. consume() resumes.
        rxStalled_ = true;
        return;
    }
    if (!tlsUserRx_) {
        rxStalled_ = false;
    }

    auto* sqe = ring_.getSqe();
    if (!sqe) [[unlikely]] {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
        return;
    }
    if (tlsActive_ && !tlsUserRx_) {
        std::memset(recvControl_, 0, sizeof(recvControl_));
        recvIov_.iov_base = buffer.data();
        recvIov_.iov_len = buffer.size();
        recvMsg_ = msghdr{};
        recvMsg_.msg_iov = &recvIov_;
        recvMsg_.msg_iovlen = 1;
        recvMsg_.msg_control = recvControl_;
        recvMsg_.msg_controllen = sizeof(recvControl_);
        ::io_uring_prep_recvmsg(sqe, fd_, &recvMsg_, 0);
    } else {
        ::io_uring_prep_recv(sqe, fd_, buffer.data(), buffer.size(), 0);
    }
    ::io_uring_sqe_set_data64(sqe, detail::encodeUserData(this, detail::OpCode::Recv));
    recvInFlight_ = true;
    ++inflight_;
}

void TcpCore::resumeRecv() noexcept {
    if (tlsUserRx_ && ssl_) {
        // Decrypt what is already buffered (in cipherRx_ or inside OpenSSL). Also after a close,
        // so that everything received before it can still be read.
        auto const before = rxBuffer_.size();
        this->decryptPending();
        if (observer_ && rxBuffer_.size() != before) {
            this->scheduleObserverNotify(); // not from inside the observer's own consume()
        }
    }
    if (state_ == ConnectionState::Ready && !recvInFlight_) {
        this->armRecv();
    }
}

void TcpCore::startHandshake() noexcept {
    state_ = ConnectionState::Handshaking;

    // Can kernel TLS work at all? With Require, fail before talking to the server (every
    // pointless handshake costs the exchange's connection rate limit); with Prefer, don't let
    // OpenSSL try in vain.
    auto const support = detail::probeKernelTlsSupport();
    bool const kernelPossible = support.opensslKtls && (support.moduleLoaded || support.canLoadModule);
    if (options_.tls.kernelTls == KernelTls::Require && !kernelPossible) {
        this->fail(makeErrorCode(support.opensslKtls ? Error::KernelTlsModuleMissing : Error::OpenSslWithoutKtls));
        return;
    }
    bool const tryKernel = options_.tls.kernelTls != KernelTls::Disable && kernelPossible;

    // OpenSSL drives the handshake on the socket itself; io_uring only tells us when to retry.
    if (auto ec = setNonBlocking(fd_, true)) {
        this->fail(ec);
        return;
    }
    if (!sslContext_) {
        auto ctx = detail::createClientContext(options_.tls);
        if (!ctx) {
            this->fail(ctx.error());
            return;
        }
        sslContext_ = *ctx;
        ::SSL_CTX_set_keylog_callback(sslContext_, &TcpCore::onKeylog);
    }

    ssl_ = ::SSL_new(sslContext_);
    if (!ssl_) {
        this->fail(detail::popTlsError(makeErrorCode(Error::TlsHandshakeFailed)));
        return;
    }
    SSL_set_app_data(ssl_, this);
    serverTrafficSecretSize_ = 0;
    if (!tryKernel) {
        ::SSL_clear_options(ssl_, SSL_OP_ENABLE_KTLS);
    }

    auto const& serverName = options_.tls.serverName.empty() ? options_.host : options_.tls.serverName;
    bool const ipLiteral = isIpLiteral(serverName);
    bool ok = ::SSL_set_fd(ssl_, fd_) == 1;
    if (ok && !ipLiteral) {
        ok = SSL_set_tlsext_host_name(ssl_, serverName.c_str()) == 1; // SNI is for names only
    }
    if (ok && options_.tls.verifyPeer) {
        ok = ipLiteral ? ::X509_VERIFY_PARAM_set1_ip_asc(::SSL_get0_param(ssl_), serverName.c_str()) == 1
                       : ::SSL_set1_host(ssl_, serverName.c_str()) == 1;
    }
    if (!ok) {
        this->fail(detail::popTlsError(makeErrorCode(Error::InvalidOptions)));
        return;
    }
    ::SSL_set_connect_state(ssl_);

    handshakeDeadline_ = std::chrono::steady_clock::now() + options_.tls.handshakeTimeout;
    this->driveHandshake();
}

void TcpCore::driveHandshake() noexcept {
    ::ERR_clear_error();
    int rc;
    {
        SigPipeGuard guard;
        rc = ::SSL_do_handshake(ssl_);
    }
    if (rc == 1) {
        this->completeHandshake();
        return;
    }
    switch (::SSL_get_error(ssl_, rc)) {
    case SSL_ERROR_WANT_READ: this->armHandshakePoll(POLLIN); break;
    case SSL_ERROR_WANT_WRITE: this->armHandshakePoll(POLLOUT); break;
    case SSL_ERROR_ZERO_RETURN: this->fail(makeErrorCode(Error::ClosedByPeer)); break;
    case SSL_ERROR_SYSCALL: {
        int const savedErrno = errno;
        auto fallback = savedErrno != 0 ? makePosixErrorCode(savedErrno) : makeErrorCode(Error::ClosedByPeer); // EOF
        this->fail(detail::popTlsError(fallback));
        break;
    }
    default: {
        if (long const verify = ::SSL_get_verify_result(ssl_); verify != X509_V_OK) {
            ::ERR_clear_error();
            this->fail({static_cast<int>(verify), getX509ErrorCategory()});
        } else {
            this->fail(detail::popTlsError(makeErrorCode(Error::TlsHandshakeFailed)));
        }
        break;
    }
    }
}

void TcpCore::armHandshakePoll(unsigned events) noexcept {
    auto const remaining = handshakeDeadline_ - std::chrono::steady_clock::now();
    if (remaining <= std::chrono::steady_clock::duration::zero()) {
        this->fail(makeErrorCode(Error::TlsHandshakeTimeout));
        return;
    }
    auto const seconds = std::chrono::duration_cast<std::chrono::seconds>(remaining);
    handshakeTimeout_.tv_sec = seconds.count();
    handshakeTimeout_.tv_nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(remaining - seconds).count();

    if (!ring_.ensureSqSpace(2)) {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
        return;
    }
    auto* sqe = ring_.getSqe();
    ::io_uring_prep_poll_add(sqe, fd_, events);
    sqe->flags |= IOSQE_IO_LINK;
    ::io_uring_sqe_set_data64(sqe, detail::encodeUserData(this, detail::OpCode::HandshakePoll));

    auto* timeoutSqe = ring_.getSqe();
    ::io_uring_prep_link_timeout(timeoutSqe, &handshakeTimeout_, 0);
    ::io_uring_sqe_set_data64(timeoutSqe, detail::encodeUserData(this, detail::OpCode::ConnectTimeout));

    inflight_ += 2;
    // Handshake round trips are on the connection's critical path: don't wait for the next poll.
    ring_.submitNoThrow();
}

void TcpCore::completeHandshake() noexcept {
    tlsVersion_ = ::SSL_get_version(ssl_);
    tlsCipher_ = ::SSL_CIPHER_get_name(::SSL_get_current_cipher(ssl_));
    bool const require = options_.tls.kernelTls == KernelTls::Require;

    // Send side: OpenSSL either switched the socket to kernel TLS or silently did not.
    bool const kernelTx = BIO_get_ktls_send(::SSL_get_wbio(ssl_)) == 1;
    if (!kernelTx && require) {
        // The usual reason is a module that could not be loaded (e.g. a kernel without
        // CONFIG_TLS); otherwise the OpenSSL build or the cipher.
        auto const support = detail::probeKernelTlsSupport();
        this->fail(
            makeErrorCode(support.moduleLoaded ? Error::KernelTlsSendUnavailable : Error::KernelTlsModuleMissing));
        return;
    }

    // Receive side: OpenSSL < 3.2 enables kernel TLS for TLS 1.3 in the send direction only; then
    // the receive side is installed here from the server traffic secret.
    bool kernelRx = BIO_get_ktls_recv(::SSL_get_rbio(ssl_)) == 1;
    if (!kernelRx && kernelTx) {
        auto const ec = this->installKernelRx();
        if (ec && require) {
            this->fail(ec);
            return;
        }
        kernelRx = !ec;
    }
    ::OPENSSL_cleanse(serverTrafficSecret_.data(), serverTrafficSecret_.size());
    serverTrafficSecretSize_ = 0;

    tlsUserRx_ = !kernelRx;
    tlsUserTx_ = !kernelTx;
    if (tlsUserRx_ || tlsUserTx_) {
        // Userspace TLS for what the kernel does not handle: OpenSSL talks to our rings through a
        // custom BIO, the socket is driven by io_uring as usual.
        if (!this->attachUserspaceBio()) {
            this->fail(detail::popTlsError(makeErrorCode(Error::TlsHandshakeFailed)));
            return;
        }
    } else {
        this->releaseSsl(); // the kernel owns the session
    }

    // Back to blocking mode: io_uring then waits for readiness itself instead of returning EAGAIN.
    if (auto ec = setNonBlocking(fd_, false)) {
        this->fail(ec);
        return;
    }
    tlsActive_ = true;
    state_ = ConnectionState::Ready;
    if (tlsUserRx_) {
        this->decryptPending(); // records OpenSSL may already hold
    }
    this->armRecv();
    this->startSend();
    if (observer_) {
        observer_->onStreamReady();
        if (!rxBuffer_.empty()) {
            observer_->onStreamData();
        }
    }
}

auto TcpCore::installKernelRx() noexcept -> std::error_code {
    // Nothing has been read with the server traffic secret yet (no read-ahead, the handshake just
    // finished), so the record sequence is 0.
    if (::SSL_version(ssl_) != TLS1_3_VERSION || serverTrafficSecretSize_ == 0 || ::SSL_has_pending(ssl_)) {
        return makeErrorCode(Error::KernelTlsReceiveUnavailable);
    }
    auto const cipherSuite = static_cast<std::uint16_t>(::SSL_CIPHER_get_id(::SSL_get_current_cipher(ssl_)));
    auto info =
        detail::makeTls13CryptoInfo(cipherSuite, std::span{serverTrafficSecret_}.first(serverTrafficSecretSize_), 0);
    if (!info) {
        return info.error();
    }
    int const rc = ::setsockopt(fd_, SOL_TLS, TLS_RX, info->bytes.data(), info->size);
    int const savedErrno = errno;
    ::OPENSSL_cleanse(&*info, sizeof(*info));
    if (rc != 0) {
        return {savedErrno, getKernelTlsRxErrorCategory()};
    }
    return {};
}

namespace {

[[nodiscard]] auto userspaceBioMethod() noexcept -> BIO_METHOD* {
    static BIO_METHOD* const method = [] {
        BIO_METHOD* m = ::BIO_meth_new(::BIO_get_new_index() | BIO_TYPE_SOURCE_SINK, "turboq-reactor");
        if (m) {
            ::BIO_meth_set_create(m, [](BIO* bio) {
                ::BIO_set_init(bio, 1);
                return 1;
            });
            ::BIO_meth_set_ctrl(m, [](BIO*, int cmd, long, void*) -> long {
                return cmd == BIO_CTRL_FLUSH ? 1 : 0;
            });
        }
        return m;
    }();
    return method;
}

} // namespace

auto TcpCore::attachUserspaceBio() noexcept -> bool {
    BIO_METHOD* method = userspaceBioMethod();
    if (!method) {
        return false;
    }
    // Set once; the lambdas above can't reach TcpCore's private members.
    static bool const callbacksSet = [method] {
        ::BIO_meth_set_read_ex(method, &TcpCore::bioRead);
        ::BIO_meth_set_write_ex(method, &TcpCore::bioWrite);
        return true;
    }();
    (void)callbacksSet;

    BIO* bio = ::BIO_new(method);
    if (!bio) {
        return false;
    }
    ::BIO_set_data(bio, this);
    if (tlsUserRx_ && tlsUserTx_) {
        ::SSL_set_bio(ssl_, bio, bio); // one reference for both directions
    } else if (tlsUserRx_) {
        ::SSL_set0_rbio(ssl_, bio); // send side stays on the kernel TLS socket
    } else {
        ::SSL_set0_wbio(ssl_, bio);
    }
    // SSL_write() returns after a partial write when cipherTx_ is full and accepts a retry from
    // a different address (the tx ring may have more data by then).
    ::SSL_set_mode(ssl_, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    return true;
}

auto TcpCore::bioRead(bio_st* bio, char* data, std::size_t size, std::size_t* done) noexcept -> int {
    auto* self = static_cast<TcpCore*>(::BIO_get_data(bio));
    BIO_clear_retry_flags(bio);
    auto const available = self->cipherRx_.readable();
    if (available.empty()) {
        if (!self->rxEof_) {
            BIO_set_retry_read(bio);
        }
        *done = 0;
        return 0;
    }
    auto const n = std::min(size, available.size());
    std::memcpy(data, available.data(), n);
    self->cipherRx_.consume(n);
    *done = n;
    return 1;
}

auto TcpCore::bioWrite(bio_st* bio, char const* data, std::size_t size, std::size_t* done) noexcept -> int {
    auto* self = static_cast<TcpCore*>(::BIO_get_data(bio));
    BIO_clear_retry_flags(bio);
    auto const space = self->cipherTx_.writable();
    if (space.empty()) {
        BIO_set_retry_write(bio);
        *done = 0;
        return 0;
    }
    auto const n = std::min(size, space.size());
    std::memcpy(space.data(), data, n);
    self->cipherTx_.produce(n);
    *done = n;
    return 1;
}

void TcpCore::decryptPending() noexcept {
    while (true) {
        auto const out = rxBuffer_.writable();
        if (out.empty()) {
            rxStalled_ = true; // consume() resumes
            break;
        }
        rxStalled_ = false;
        std::size_t n = 0;
        ::ERR_clear_error();
        int const rc = ::SSL_read_ex(ssl_, out.data(), out.size(), &n);
        if (rc == 1) [[likely]] {
            rxBuffer_.produce(n);
            continue;
        }
        switch (::SSL_get_error(ssl_, rc)) {
        case SSL_ERROR_WANT_READ: break;  // need more ciphertext
        case SSL_ERROR_WANT_WRITE: break; // cipherTx_ full (key update answer): resumes after a send
        case SSL_ERROR_ZERO_RETURN: this->fail(makeErrorCode(Error::ClosedByPeer)); break; // close_notify
        default:
            // A TCP close without close_notify reads as "unexpected EOF".
            this->fail(rxEof_ ? makeErrorCode(Error::ClosedByPeer)
                              : detail::popTlsError(makeErrorCode(Error::TlsUnexpectedRecord)));
            break;
        }
        break;
    }
    ::ERR_clear_error();
    // Reading may have produced records to send (key update answer, alerts).
    if (tlsUserTx_ && !cipherTx_.empty() && state_ == ConnectionState::Ready && !sendInFlight_) {
        this->startSend();
    }
}

void TcpCore::encryptPending() noexcept {
    while (!txBuffer_.empty()) {
        auto const data = txBuffer_.readable();
        std::size_t written = 0;
        ::ERR_clear_error();
        int const rc = ::SSL_write_ex(ssl_, data.data(), data.size(), &written);
        if (rc == 1) [[likely]] {
            txBuffer_.consume(written); // partial writes allowed: stops when cipherTx_ is full
            continue;
        }
        if (::SSL_get_error(ssl_, rc) != SSL_ERROR_WANT_WRITE) {
            this->fail(detail::popTlsError(makeErrorCode(Error::TlsUnexpectedRecord)));
        }
        break; // cipherTx_ full: continues after the next send completion
    }
}

void TcpCore::flushOnClose() noexcept {
    // Not while an io_uring send is in flight: the rest of the stream belongs after it.
    if (sendInFlight_) {
        return;
    }
    if (tlsUserTx_) {
        this->encryptPending();
    }
    auto& wire = this->wireTx();
    while (!wire.empty()) {
        auto const data = wire.readable();
        auto const rc = ::send(fd_, data.data(), data.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
        if (rc <= 0) {
            break; // socket buffer full or broken: closing anyway
        }
        wire.consume(static_cast<std::size_t>(rc));
    }
}

void TcpCore::sendUserspaceCloseNotify() noexcept {
    ::ERR_clear_error();
    ::SSL_shutdown(ssl_); // writes the alert to cipherTx_
    ::ERR_clear_error();
    this->flushOnClose();
}

void TcpCore::scheduleObserverNotify() noexcept {
    observerPending_ = true;
    if (!txDirty_) {
        ring_.schedule(this); // onTxReady() delivers it in the next poll()
    }
}

void TcpCore::onTlsControlRecord(unsigned char recordType, std::size_t size) noexcept {
    // The record content was written to the free part of the rx ring; it is not produced.
    auto const* data = static_cast<unsigned char const*>(recvIov_.iov_base);
    switch (recordType) {
    case kRecordAlert:
        if (size >= 2 && data[1] == kAlertCloseNotify) {
            this->fail(makeErrorCode(Error::ClosedByPeer));
        } else {
            this->fail({size >= 2 ? data[1] : 0, getTlsAlertCategory()});
        }
        break;
    case kRecordHandshake:
        if (size >= 1 && data[0] == kHandshakeNewSessionTicket) {
            break; // resumption is not used
        }
        if (size >= 1 && data[0] == kHandshakeKeyUpdate) {
            this->fail(makeErrorCode(Error::TlsKeyUpdateUnsupported));
            break;
        }
        this->fail(makeErrorCode(Error::TlsUnexpectedRecord));
        break;
    default: this->fail(makeErrorCode(Error::TlsUnexpectedRecord)); break;
    }
}

void TcpCore::sendCloseNotify() noexcept {
    // Best effort, non-blocking: an alert record (type 21) via kTLS.
    unsigned char alert[2] = {1 /* warning */, kAlertCloseNotify};
    alignas(cmsghdr) unsigned char control[CMSG_SPACE(sizeof(unsigned char))]{};
    iovec iov{alert, sizeof(alert)};
    msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);
    auto* cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_TLS;
    cmsg->cmsg_type = TLS_SET_RECORD_TYPE;
    cmsg->cmsg_len = CMSG_LEN(sizeof(unsigned char));
    *CMSG_DATA(cmsg) = kRecordAlert;
    [[maybe_unused]] auto const rc = ::sendmsg(fd_, &msg, MSG_DONTWAIT | MSG_NOSIGNAL);
}

void TcpCore::releaseSsl() noexcept {
    if (ssl_) {
        detail::freeSsl(ssl_);
        ssl_ = nullptr;
    }
}

void TcpCore::onKeylog(ssl_st const* ssl, char const* line) noexcept {
    // "SERVER_TRAFFIC_SECRET_0 <client random hex> <secret hex>" (NSS key log format)
    constexpr std::string_view kLabel = "SERVER_TRAFFIC_SECRET_0 ";
    std::string_view text{line};
    if (!text.starts_with(kLabel)) {
        return;
    }
    auto* self = static_cast<TcpCore*>(SSL_get_app_data(ssl));
    if (!self) {
        return;
    }
    text.remove_prefix(kLabel.size());
    auto const space = text.find(' ');
    if (space == std::string_view::npos) {
        return;
    }
    text.remove_prefix(space + 1);
    if (text.size() % 2 != 0 || text.size() / 2 > self->serverTrafficSecret_.size()) {
        return;
    }
    for (std::size_t i = 0; i < text.size() / 2; ++i) {
        int const hi = hexValue(text[2 * i]);
        int const lo = hexValue(text[2 * i + 1]);
        if (hi < 0 || lo < 0) {
            return;
        }
        self->serverTrafficSecret_[i] = static_cast<std::uint8_t>(hi << 4 | lo);
    }
    self->serverTrafficSecretSize_ = text.size() / 2;
}

void TcpCore::markTxDirty() noexcept {
    // While connecting the connect completion starts sending; once closing nothing is sent.
    if (state_ == ConnectionState::Ready) {
        ring_.schedule(this);
    }
}

void TcpCore::onTxReady() noexcept {
    if (observerPending_) {
        observerPending_ = false;
        if (observer_) {
            observer_->onStreamData();
        }
    }
    this->startSend();
}

void TcpCore::flushTx() noexcept {
    if (state_ != ConnectionState::Ready || sendInFlight_) {
        // Not connected yet, or a send is in flight: its completion picks up the new data.
        return;
    }
    if (!observerPending_) {
        // A stale entry may remain in the ring's pending-tx list, it is skipped there. With an observer
        // notification pending, the entry must stay live: poll() delivers it.
        txDirty_ = false;
    }
    if (txBuffer_.empty() && this->wireTx().empty()) {
        return;
    }
    if (options_.directSend) {
        this->sendDirect();
    } else {
        this->startSend();
        ring_.submitNoThrow();
    }
}

void TcpCore::startSend() noexcept {
    if (state_ != ConnectionState::Ready || sendInFlight_) {
        return;
    }
    if (tlsUserTx_) {
        this->encryptPending();
        if (state_ != ConnectionState::Ready) {
            return;
        }
    }
    auto& wire = this->wireTx();
    if (wire.empty()) {
        return;
    }
    auto* sqe = ring_.getSqe();
    if (!sqe) [[unlikely]] {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
        return;
    }
    // Only one send in flight per socket: concurrent sends on a stream socket may interleave
    // partial writes. The range is a snapshot; data committed later goes with the next send.
    auto const data = wire.readable();
    ::io_uring_prep_send(sqe, fd_, data.data(), data.size(), MSG_NOSIGNAL);
    ::io_uring_sqe_set_data64(sqe, detail::encodeUserData(this, detail::OpCode::Send));
    sendInFlight_ = true;
    ++inflight_;
}

void TcpCore::sendDirect() noexcept {
    if (tlsUserTx_) {
        this->encryptPending();
        if (state_ != ConnectionState::Ready) {
            return;
        }
    }
    auto& wire = this->wireTx();
    auto const data = wire.readable();
    if (data.empty()) {
        return;
    }
    auto const rc = ::send(fd_, data.data(), data.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
    if (rc > 0) {
        wire.consume(static_cast<std::size_t>(rc));
    } else if (rc < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        this->fail(makePosixErrorCode(errno));
        return;
    }
    if (!wire.empty() || !txBuffer_.empty()) {
        // Socket buffer is full: hand the rest to io_uring, it waits for writability.
        this->startSend();
        ring_.submitNoThrow();
    }
}

} // namespace detail

TcpConnection::TcpConnection(Reactor& reactor, TcpOptions options)
    : core_{detail::TcpCore::create(reactor.ring(), std::move(options))}, rx{core_->rx}, tx{core_->tx} {
    reactor.ring().attach(core_);
}

} // namespace turboq::reactor
