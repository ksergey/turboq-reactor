// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "TLSConnection.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>

#include <algorithm>
#include <optional>
#include <span>
#include <string>

#include "Error.h"
#include "Reactor.h"
#include "detail/Tls.h"

namespace turboq::reactor {
namespace {

[[nodiscard]] auto setNonBlocking(int fd, bool enabled) noexcept -> std::error_code {
    int const flags = ::fcntl(fd, F_GETFL);
    if (flags < 0 || ::fcntl(fd, F_SETFL, enabled ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK)) < 0) {
        return makePosixErrorCode(errno);
    }
    return {};
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

// Construction and life cycle.

template <typename Backend>
auto TLSCore<Backend>::create(Backend& ring, TCPOptions tcp, TLSOptions tls) -> TLSCore* {
    validate(tcp);
    auto rxBuffer = allocateRing(tcp.rxBufferSize, "MirroredBuffer::create (rx)");
    auto txBuffer = allocateRing(tcp.txBufferSize, "MirroredBuffer::create (tx)");
    MirroredBuffer cipherRx;
    MirroredBuffer cipherTx;
    if (tls.kernelTls != KernelTls::Require) {
        cipherRx = allocateRing(std::max(tcp.rxBufferSize, kMinCipherRingSize), "MirroredBuffer::create (TLS rx)");
        cipherTx = allocateRing(std::max(tcp.txBufferSize, kMinCipherRingSize), "MirroredBuffer::create (TLS tx)");
    }
    return new TLSCore{ring, std::move(tcp), std::move(tls), std::move(rxBuffer), std::move(txBuffer),
        std::move(cipherRx), std::move(cipherTx)};
}

template <typename Backend>
TLSCore<Backend>::TLSCore(Backend& ring, TCPOptions tcp, TLSOptions tls, MirroredBuffer rxBuffer,
    MirroredBuffer txBuffer, MirroredBuffer cipherRx, MirroredBuffer cipherTx) noexcept
    : Base{ring, std::move(tcp), std::move(rxBuffer), std::move(txBuffer)}, tlsOptions_{std::move(tls)},
      cipherRx_{std::move(cipherRx)}, cipherTx_{std::move(cipherTx)} {}

template <typename Backend>
TLSCore<Backend>::~TLSCore() noexcept {
    this->releaseSsl();
    if (sslContext_) {
        freeContext(sslContext_);
    }
}

template <typename Backend>
void TLSCore<Backend>::resetSession() noexcept {
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
}

template <typename Backend>
void TLSCore<Backend>::releaseSsl() noexcept {
    if (ssl_) {
        freeSsl(ssl_);
        ssl_ = nullptr;
    }
}

template <typename Backend>
void TLSCore<Backend>::onCompletion(OpCode op, std::int32_t res, std::uint32_t flags) noexcept {
    switch (op) {
    case OpCode::Poll:
        assert(inflight_ > 0);
        --inflight_;
        this->onHandshakePoll(res);
        this->checkClosed();
        return;

    case OpCode::Recv:
        if (tlsActive_) {
            assert(inflight_ > 0);
            --inflight_;
            recvInFlight_ = false;
            if (tlsUserRx_) {
                this->onUserspaceRecv(res);
            } else {
                this->onKernelRecv(res);
            }
            this->checkClosed();
            return;
        }
        break;

    case OpCode::Send:
        Base::onCompletion(op, res, flags);
        if (res > 0 && tlsUserRx_ && ssl_ && !cipherRx_.empty()) {
            // OpenSSL may have been waiting for room in cipherTx_ to answer (key update).
            this->decryptPending();
        }
        return;

    default: break;
    }
    Base::onCompletion(op, res, flags);
}

template <typename Backend>
void TLSCore<Backend>::onClosed() noexcept {
    if (!tlsUserRx_) {
        this->releaseSsl(); // with userspace rx, kept so that consume() can drain what arrived
    }
}

template <typename Backend>
void TLSCore<Backend>::onCloseRequested() noexcept {
    // Committed data, then close_notify; best effort, never blocks.
    if (tlsUserTx_) {
        this->encryptPending();
        this->drainOnClose(cipherTx_);
        ::ERR_clear_error();
        ::SSL_shutdown(ssl_); // writes the alert to cipherTx_
        ::ERR_clear_error();
        this->drainOnClose(cipherTx_);
    } else {
        this->drainOnClose(txBuffer_);
        this->sendCloseNotify();
    }
}

// Handshake.

template <typename Backend>
void TLSCore<Backend>::onConnected() noexcept {
    this->startHandshake();
}

template <typename Backend>
void TLSCore<Backend>::startHandshake() noexcept {
    state_ = ConnectionState::Handshaking;

    // Can kernel TLS work at all? With Require, fail before talking to the server (every
    // pointless handshake costs the exchange's connection rate limit); with Prefer, don't let
    // OpenSSL try in vain.
    auto const support = probeKernelTlsSupport();
    bool const kernelPossible = support.opensslKtls && (support.moduleLoaded || support.canLoadModule);
    if (tlsOptions_.kernelTls == KernelTls::Require && !kernelPossible) {
        this->fail(makeErrorCode(support.opensslKtls ? Error::KernelTlsModuleMissing : Error::OpenSslWithoutKtls));
        return;
    }
    bool const tryKernel = tlsOptions_.kernelTls != KernelTls::Disable && kernelPossible;

    // OpenSSL drives the handshake on the socket itself; the backend only tells us when to retry.
    if (auto ec = setNonBlocking(fd_, true)) {
        this->fail(ec);
        return;
    }
    if (!sslContext_) {
        auto ctx = createClientContext(tlsOptions_);
        if (!ctx) {
            this->fail(ctx.error());
            return;
        }
        sslContext_ = *ctx;
        ::SSL_CTX_set_keylog_callback(sslContext_, &TLSCore::onKeylog);
    }

    ssl_ = ::SSL_new(sslContext_);
    if (!ssl_) {
        this->fail(popTlsError(makeErrorCode(Error::TlsHandshakeFailed)));
        return;
    }
    SSL_set_app_data(ssl_, this);
    serverTrafficSecretSize_ = 0;
    if (!tryKernel) {
        ::SSL_clear_options(ssl_, SSL_OP_ENABLE_KTLS);
    }

    // A server name: SNI and host name verification. None, or a numeric one: verify the
    // certificate's IP SAN against the address (no SNI, it is for names only).
    auto const& serverName = tlsOptions_.serverName;
    std::optional<IPAddress> ip;
    if (!serverName) {
        ip = options_.endpoint.address;
    } else if (auto parsed = IPAddress::parse(*serverName)) {
        ip = *parsed;
    }
    bool ok = ::SSL_set_fd(ssl_, fd_) == 1;
    if (ok && !ip) {
        ok = SSL_set_tlsext_host_name(ssl_, serverName->c_str()) == 1;
    }
    if (ok && tlsOptions_.verifyPeer) {
        if (ip) {
            auto const bytes = ip->visit([](auto const& value) {
                return std::span<std::uint8_t const>{value.bytes()};
            });
            ok = ::X509_VERIFY_PARAM_set1_ip(::SSL_get0_param(ssl_), bytes.data(), bytes.size()) == 1;
        } else {
            ok = ::SSL_set1_host(ssl_, serverName->c_str()) == 1;
        }
    }
    if (!ok) {
        this->fail(popTlsError(makeErrorCode(Error::InvalidOptions)));
        return;
    }
    ::SSL_set_connect_state(ssl_);

    handshakeDeadline_ = std::chrono::steady_clock::now() + tlsOptions_.handshakeTimeout;
    this->driveHandshake();
}

template <typename Backend>
void TLSCore<Backend>::onHandshakePoll(std::int32_t res) noexcept {
    if (state_ != ConnectionState::Handshaking) {
        return; // closing
    }
    if (res == -ECANCELED) {
        this->fail(makeErrorCode(Error::TlsHandshakeTimeout));
    } else if (res < 0) {
        this->fail(makePosixErrorCode(-res));
    } else {
        this->driveHandshake();
    }
}

template <typename Backend>
void TLSCore<Backend>::driveHandshake() noexcept {
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
        this->fail(popTlsError(fallback));
        break;
    }
    default: {
        if (long const verify = ::SSL_get_verify_result(ssl_); verify != X509_V_OK) {
            ::ERR_clear_error();
            this->fail({static_cast<int>(verify), getX509ErrorCategory()});
        } else {
            this->fail(popTlsError(makeErrorCode(Error::TlsHandshakeFailed)));
        }
        break;
    }
    }
}

template <typename Backend>
void TLSCore<Backend>::armHandshakePoll(unsigned events) noexcept {
    auto const remaining = handshakeDeadline_ - std::chrono::steady_clock::now();
    if (remaining <= std::chrono::steady_clock::duration::zero()) {
        this->fail(makeErrorCode(Error::TlsHandshakeTimeout));
        return;
    }
    handshakeTimeout_ = toKernelTimespec(remaining);

    if (!ring_.pollFd(this, fd_, events, &handshakeTimeout_)) {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
        return;
    }
    ++inflight_;
    // Handshake round trips are on the connection's critical path: don't wait for the next poll.
    ring_.submitNoThrow();
}

template <typename Backend>
void TLSCore<Backend>::completeHandshake() noexcept {
    tlsVersion_ = ::SSL_get_version(ssl_);
    tlsCipher_ = ::SSL_CIPHER_get_name(::SSL_get_current_cipher(ssl_));
    bool const require = tlsOptions_.kernelTls == KernelTls::Require;

    // Send side: OpenSSL either switched the socket to kernel TLS or silently did not.
    bool const kernelTx = BIO_get_ktls_send(::SSL_get_wbio(ssl_)) == 1;
    if (!kernelTx && require) {
        // The usual reason is a module that could not be loaded (e.g. a kernel without
        // CONFIG_TLS); otherwise the OpenSSL build or the cipher.
        auto const support = probeKernelTlsSupport();
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
        // custom BIO, the socket is driven by the backend as usual.
        if (!this->attachUserspaceBio()) {
            this->fail(popTlsError(makeErrorCode(Error::TlsHandshakeFailed)));
            return;
        }
    } else {
        this->releaseSsl(); // the kernel owns the session
    }

    // Back to blocking mode: io_uring then waits for readiness itself instead of returning EAGAIN
    // (the epoll backend passes MSG_DONTWAIT on every call).
    if (auto ec = setNonBlocking(fd_, false)) {
        this->fail(ec);
        return;
    }
    tlsActive_ = true;
    state_ = ConnectionState::Ready;
    if (tlsUserRx_) {
        this->decryptPending(); // records OpenSSL may already hold
        if (state_ != ConnectionState::Ready) {
            return;
        }
    }
    this->startStreaming();
}

template <typename Backend>
auto TLSCore<Backend>::installKernelRx() noexcept -> std::error_code {
    // Nothing has been read with the server traffic secret yet (no read-ahead, the handshake just
    // finished), so the record sequence is 0.
    if (::SSL_version(ssl_) != TLS1_3_VERSION || serverTrafficSecretSize_ == 0 || ::SSL_has_pending(ssl_)) {
        return makeErrorCode(Error::KernelTlsReceiveUnavailable);
    }
    auto const cipherSuite = static_cast<std::uint16_t>(::SSL_CIPHER_get_id(::SSL_get_current_cipher(ssl_)));
    auto info = makeTls13CryptoInfo(cipherSuite, std::span{serverTrafficSecret_}.first(serverTrafficSecretSize_), 0);
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

template <typename Backend>
void TLSCore<Backend>::onKeylog(ssl_st const* ssl, char const* line) noexcept {
    // "SERVER_TRAFFIC_SECRET_0 <client random hex> <secret hex>" (NSS key log format)
    constexpr std::string_view kLabel = "SERVER_TRAFFIC_SECRET_0 ";
    std::string_view text{line};
    if (!text.starts_with(kLabel)) {
        return;
    }
    auto* self = static_cast<TLSCore*>(SSL_get_app_data(ssl));
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

// Receiving.

template <typename Backend>
void TLSCore<Backend>::armRecv() noexcept {
    if (tlsUserRx_) {
        // Ciphertext goes to its own ring; rxStalled_ tracks the plaintext ring (decryptPending()).
        auto const buffer = cipherRx_.writable();
        if (!buffer.empty()) {
            this->submitRecv(buffer);
        }
        return; // both rings full: consume() decrypts and re-arms
    }

    auto const buffer = rxBuffer_.writable();
    if (buffer.empty()) {
        rxStalled_ = true;
        return;
    }
    rxStalled_ = false;
    // kTLS: recvmsg, so that the record type of non-data records arrives as a control message.
    std::memset(recvControl_, 0, sizeof(recvControl_));
    recvIov_.iov_base = buffer.data();
    recvIov_.iov_len = buffer.size();
    recvMsg_ = msghdr{};
    recvMsg_.msg_iov = &recvIov_;
    recvMsg_.msg_iovlen = 1;
    recvMsg_.msg_control = recvControl_;
    recvMsg_.msg_controllen = sizeof(recvControl_);
    if (!ring_.recvMsg(this, fd_, &recvMsg_)) [[unlikely]] {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
        return;
    }
    recvInFlight_ = true;
    ++inflight_;
}

template <typename Backend>
void TLSCore<Backend>::onKernelRecv(std::int32_t res) noexcept {
    if (res > 0) {
        // The buffer was zeroed before the receive, so a missing control message reads as
        // cmsg_len == 0.
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
            return;
        }
    }
    this->handleRecvResult(res);
}

template <typename Backend>
void TLSCore<Backend>::onTlsControlRecord(unsigned char recordType, std::size_t size) noexcept {
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

template <typename Backend>
void TLSCore<Backend>::onUserspaceRecv(std::int32_t res) noexcept {
    // Ciphertext lands in cipherRx_, OpenSSL decrypts into rxBuffer_.
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
}

template <typename Backend>
void TLSCore<Backend>::resumeRecv() noexcept {
    if (tlsUserRx_ && ssl_) {
        // Decrypt what is already buffered (in cipherRx_ or inside OpenSSL). Also after a close,
        // so that everything received before it can still be read.
        auto const before = rxBuffer_.size();
        this->decryptPending();
        if (observer_ && rxBuffer_.size() != before) {
            this->scheduleObserverNotify(); // not from inside the observer's own consume()
        }
    }
    Base::resumeRecv();
}

template <typename Backend>
void TLSCore<Backend>::scheduleObserverNotify() noexcept {
    observerPending_ = true;
    if (!txDirty_) {
        ring_.schedule(this); // onTxReady() delivers it in the next poll()
    }
}

template <typename Backend>
void TLSCore<Backend>::onTxReady() noexcept {
    if (observerPending_) {
        observerPending_ = false;
        if (observer_) {
            observer_->onStreamData();
        }
    }
    Base::onTxReady();
}

// Sending.

template <typename Backend>
void TLSCore<Backend>::startSend() noexcept {
    if (!tlsUserTx_) {
        this->submitSend(txBuffer_); // plaintext, the kernel encrypts
        return;
    }
    if (state_ != ConnectionState::Ready || sendInFlight_) {
        return;
    }
    this->encryptPending();
    this->submitSend(cipherTx_);
}

template <typename Backend>
void TLSCore<Backend>::sendDirect() noexcept {
    if (!tlsUserTx_) {
        this->sendNow(txBuffer_);
        return;
    }
    this->encryptPending();
    if (state_ == ConnectionState::Ready) {
        this->sendNow(cipherTx_);
    }
}

template <typename Backend>
void TLSCore<Backend>::sendCloseNotify() noexcept {
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

// Userspace TLS.

template <typename Backend>
auto TLSCore<Backend>::bioMethod() noexcept -> BIO_METHOD* {
    // One method per backend: its callbacks cast the BIO data to this very TLSCore type.
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
            ::BIO_meth_set_read_ex(m, &TLSCore::bioRead);
            ::BIO_meth_set_write_ex(m, &TLSCore::bioWrite);
        }
        return m;
    }();
    return method;
}

template <typename Backend>
auto TLSCore<Backend>::attachUserspaceBio() noexcept -> bool {
    BIO_METHOD* method = bioMethod();
    if (!method) {
        return false;
    }
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

template <typename Backend>
auto TLSCore<Backend>::bioRead(bio_st* bio, char* data, std::size_t size, std::size_t* done) noexcept -> int {
    auto* self = static_cast<TLSCore*>(::BIO_get_data(bio));
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

template <typename Backend>
auto TLSCore<Backend>::bioWrite(bio_st* bio, char const* data, std::size_t size, std::size_t* done) noexcept -> int {
    auto* self = static_cast<TLSCore*>(::BIO_get_data(bio));
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

template <typename Backend>
void TLSCore<Backend>::decryptPending() noexcept {
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
            this->fail(
                rxEof_ ? makeErrorCode(Error::ClosedByPeer) : popTlsError(makeErrorCode(Error::TlsUnexpectedRecord)));
            break;
        }
        break;
    }
    ::ERR_clear_error();
    // Reading may have produced records to send (key update answer, alerts).
    if (tlsUserTx_ && !cipherTx_.empty() && state_ == ConnectionState::Ready && !sendInFlight_) {
        this->submitSend(cipherTx_);
    }
}

template <typename Backend>
void TLSCore<Backend>::encryptPending() noexcept {
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
            this->fail(popTlsError(makeErrorCode(Error::TlsUnexpectedRecord)));
        }
        break; // cipherTx_ full: continues after the next send completion
    }
}

} // namespace detail

template <typename Backend>
TLSConnection<Backend>::TLSConnection(Reactor<Backend>& reactor, TCPOptions tcp, TLSOptions tls)
    : core_{detail::TLSCore<Backend>::create(reactor.backend(), std::move(tcp), std::move(tls))}, rx{core_->rx},
      tx{core_->tx} {
    reactor.backend().attach(core_);
}

// The backends this library is built with.
namespace detail {
template class TLSCore<IoUringBackend>;
template class TLSCore<EpollBackend>;
} // namespace detail
template class TLSConnection<IoUringBackend>;
template class TLSConnection<EpollBackend>;

} // namespace turboq::reactor
