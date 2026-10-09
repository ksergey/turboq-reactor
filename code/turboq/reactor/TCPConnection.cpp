// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "TCPConnection.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <bit>
#include <string>

#include "Error.h"
#include "Reactor.h"

namespace turboq::reactor {
namespace {

[[nodiscard]] auto setIntOption(int fd, int level, int name, int value) noexcept -> std::error_code {
    if (::setsockopt(fd, level, name, &value, sizeof(value)) != 0) {
        return makePosixErrorCode(errno);
    }
    return {};
}

} // namespace

namespace detail {

template <typename Backend>
void TCPCore<Backend>::validate(TCPOptions const& options) {
    if (options.rxBufferSize == 0 || options.txBufferSize == 0) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "TCPConnection"};
    }
}

template <typename Backend>
auto TCPCore<Backend>::allocateRing(std::size_t size, char const* what) -> MirroredBuffer {
    auto buffer = MirroredBuffer::create({.capacityHint = size});
    if (!buffer) {
        throw std::system_error{buffer.error(), what};
    }
    return std::move(*buffer);
}

template <typename Backend>
auto TCPCore<Backend>::create(Backend& ring, TCPOptions options) -> TCPCore* {
    validate(options);
    auto rxBuffer = allocateRing(options.rxBufferSize, "MirroredBuffer::create (rx)");
    auto txBuffer = allocateRing(options.txBufferSize, "MirroredBuffer::create (tx)");
    return new TCPCore{ring, std::move(options), std::move(rxBuffer), std::move(txBuffer)};
}

template <typename Backend>
TCPCore<Backend>::TCPCore(Backend& ring, TCPOptions options, MirroredBuffer rxBuffer, MirroredBuffer txBuffer) noexcept
    : ring_{ring}, options_{std::move(options)}, rxBuffer_{std::move(rxBuffer)}, txBuffer_{std::move(txBuffer)} {
    connectTimeout_ = toKernelTimespec(options_.connectTimeout);
}

template <typename Backend>
TCPCore<Backend>::~TCPCore() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

template <typename Backend>
auto TCPCore<Backend>::connect() -> std::expected<void, std::error_code> {
    if (state_ != ConnectionState::Idle && state_ != ConnectionState::Closed) {
        return std::unexpected(makeErrorCode(Error::InvalidState));
    }

    rxBuffer_.clear();
    txBuffer_.clear();
    prepared_ = 0;
    rxTimestamp_ = {};
    rxStalled_ = false;
    txDirty_ = false;
    sendSource_ = nullptr;
    error_.clear();
    this->resetSession();

    // An endpoint with a zero port or address is still a valid object (a layer may set it before
    // each connect), it just can't be connected to.
    if (options_.endpoint.port == 0 || options_.endpoint.address.isUnspecified()) {
        return this->failSync(makeErrorCode(Error::InvalidAddress));
    }
    addressLength_ = options_.endpoint.toSockaddr(address_);

    fd_ = ::socket(address_.ss_family, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
    if (fd_ < 0) {
        return this->failSync(makePosixErrorCode(errno));
    }
    if (options_.noDelay) {
        if (auto ec = setIntOption(fd_, IPPROTO_TCP, TCP_NODELAY, 1)) {
            return this->failSync(ec);
        }
    }
    if (options_.socketRecvBufferSize) {
        if (auto ec = setIntOption(fd_, SOL_SOCKET, SO_RCVBUF, *options_.socketRecvBufferSize)) {
            return this->failSync(ec);
        }
    }
    if (options_.socketSendBufferSize) {
        if (auto ec = setIntOption(fd_, SOL_SOCKET, SO_SNDBUF, *options_.socketSendBufferSize)) {
            return this->failSync(ec);
        }
    }

    if (!ring_.connect(this, fd_, std::bit_cast<sockaddr const*>(&address_), addressLength_, &connectTimeout_)) {
        return this->failSync(makeErrorCode(Error::SubmissionQueueFull));
    }
    ++inflight_;
    state_ = ConnectionState::Connecting;
    return {};
}

template <typename Backend>
void TCPCore<Backend>::close() noexcept {
    switch (state_) {
    case ConnectionState::Idle: state_ = ConnectionState::Closed; break;
    case ConnectionState::Connecting: [[fallthrough]];
    case ConnectionState::Handshaking: this->beginClose(); break;
    case ConnectionState::Ready:
        // Committed data goes out before FIN (best effort, never blocks).
        this->onCloseRequested();
        this->beginClose();
        break;
    case ConnectionState::Closing: [[fallthrough]];
    case ConnectionState::Closed: break;
    }
}

template <typename Backend>
void TCPCore<Backend>::onCompletion(OpCode op, std::int32_t res, [[maybe_unused]] std::uint32_t flags) noexcept {
    assert(inflight_ > 0);
    --inflight_;

    switch (op) {
    case OpCode::Connect:
        if (state_ != ConnectionState::Connecting) {
            break; // closing
        }
        if (res == 0) {
            this->onConnected();
        } else if (res == -ECANCELED) {
            // Cancelled by the linked timeout (a user close() would have moved us to Closing).
            this->fail(makeErrorCode(Error::ConnectTimeout));
        } else {
            this->fail(makePosixErrorCode(-res));
        }
        break;

    case OpCode::Recv:
        recvInFlight_ = false;
        this->handleRecvResult(res);
        break;

    case OpCode::Send:
        sendInFlight_ = false;
        if (res > 0) [[likely]] {
            sendSource_->consume(static_cast<std::size_t>(res));
            // Short send or data committed meanwhile: keep going.
            this->startSend();
        } else if (res == -EINTR || res == -EAGAIN) {
            this->startSend();
        } else if (res != -ECANCELED) {
            this->fail(makePosixErrorCode(-res));
        }
        break;

    case OpCode::Cancel: [[fallthrough]];
    case OpCode::Timer: [[fallthrough]];
    case OpCode::Poll: break;
    }

    this->checkClosed();
}

template <typename Backend>
void TCPCore<Backend>::handleRecvResult(std::int32_t res) noexcept {
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
}

template <typename Backend>
void TCPCore<Backend>::onConnected() noexcept {
    state_ = ConnectionState::Ready;
    this->startStreaming();
}

template <typename Backend>
void TCPCore<Backend>::startStreaming() noexcept {
    this->armRecv();
    // Data committed while connecting goes out right away.
    this->startSend();
    if (observer_ && state_ == ConnectionState::Ready) {
        observer_->onStreamReady();
        if (!rxBuffer_.empty()) {
            observer_->onStreamData();
        }
    }
}

template <typename Backend>
auto TCPCore<Backend>::failSync(std::error_code ec) noexcept -> std::unexpected<std::error_code> {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    state_ = ConnectionState::Closed;
    error_ = ec;
    return std::unexpected(ec);
}

template <typename Backend>
void TCPCore<Backend>::fail(std::error_code ec) noexcept {
    if (state_ == ConnectionState::Closing || state_ == ConnectionState::Closed) {
        return;
    }
    error_ = ec;
    this->beginClose();
}

template <typename Backend>
void TCPCore<Backend>::beginClose() noexcept {
    state_ = ConnectionState::Closing;
    prepared_ = 0;

    if (fd_ >= 0) {
        // Wakes up a pending recv (returns 0) and send (EPIPE) on an established connection.
        ::shutdown(fd_, SHUT_RDWR);
    }
    if (inflight_ == 0) {
        this->finishClose();
        return;
    }
    // Covers what shutdown() does not wake up, e.g. a connect in progress.
    if (ring_.cancel(this, fd_)) {
        ++inflight_;
    }
}

template <typename Backend>
void TCPCore<Backend>::finishClose() noexcept {
    this->onClosed();
    if (fd_ >= 0) {
        ring_.forget(fd_);
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

template <typename Backend>
void TCPCore<Backend>::armRecv() noexcept {
    auto const buffer = rxBuffer_.writable();
    if (buffer.empty()) {
        // Ring is full: stop reading, TCP flow control pushes back on the peer. consume() resumes.
        rxStalled_ = true;
        return;
    }
    rxStalled_ = false;
    this->submitRecv(buffer);
}

template <typename Backend>
void TCPCore<Backend>::submitRecv(std::span<std::byte> buffer) noexcept {
    if (!ring_.recv(this, fd_, buffer)) [[unlikely]] {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
        return;
    }
    recvInFlight_ = true;
    ++inflight_;
}

template <typename Backend>
void TCPCore<Backend>::resumeRecv() noexcept {
    if (state_ == ConnectionState::Ready && !recvInFlight_) {
        this->armRecv();
    }
}

template <typename Backend>
void TCPCore<Backend>::onCloseRequested() noexcept {
    this->drainOnClose(txBuffer_);
}

template <typename Backend>
void TCPCore<Backend>::drainOnClose(MirroredBuffer& source) noexcept {
    // Not while an io_uring send is in flight: the rest of the stream belongs after it.
    if (sendInFlight_) {
        return;
    }
    while (!source.empty()) {
        auto const data = source.readable();
        auto const rc = ::send(fd_, data.data(), data.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
        if (rc <= 0) {
            break; // socket buffer full or broken: closing anyway
        }
        source.consume(static_cast<std::size_t>(rc));
    }
}

template <typename Backend>
void TCPCore<Backend>::markTxDirty() noexcept {
    // While connecting the connect completion starts sending; once closing nothing is sent.
    if (state_ == ConnectionState::Ready) {
        ring_.schedule(this);
    }
}

template <typename Backend>
void TCPCore<Backend>::onTxReady() noexcept {
    this->startSend();
}

template <typename Backend>
void TCPCore<Backend>::flushTx() noexcept {
    if (state_ != ConnectionState::Ready || sendInFlight_) {
        // Not connected yet, or a send is in flight: its completion picks up the new data.
        return;
    }
    // A pending-tx entry the commit left in the ring stays: the next poll() finds nothing to send.
    if (options_.directSend) {
        this->sendDirect();
    } else {
        this->startSend();
        ring_.submitNoThrow();
    }
}

template <typename Backend>
void TCPCore<Backend>::startSend() noexcept {
    this->submitSend(txBuffer_);
}

template <typename Backend>
void TCPCore<Backend>::submitSend(MirroredBuffer& source) noexcept {
    if (state_ != ConnectionState::Ready || sendInFlight_ || source.empty()) {
        return;
    }
    // Only one send in flight per socket: concurrent sends on a stream socket may interleave
    // partial writes. The range is a snapshot; data committed later goes with the next send.
    if (!ring_.send(this, fd_, source.readable())) [[unlikely]] {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
        return;
    }
    sendSource_ = &source;
    sendInFlight_ = true;
    ++inflight_;
}

template <typename Backend>
void TCPCore<Backend>::sendDirect() noexcept {
    this->sendNow(txBuffer_);
}

template <typename Backend>
void TCPCore<Backend>::sendNow(MirroredBuffer& source) noexcept {
    auto const data = source.readable();
    if (data.empty()) {
        return;
    }
    auto const rc = ::send(fd_, data.data(), data.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
    if (rc > 0) {
        source.consume(static_cast<std::size_t>(rc));
    } else if (rc < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        this->fail(makePosixErrorCode(errno));
        return;
    }
    if (!source.empty() || !txBuffer_.empty()) {
        // Socket buffer is full (or a layer has more to process): io_uring waits for writability.
        this->startSend();
        ring_.submitNoThrow();
    }
}

} // namespace detail

template <typename Backend>
TCPConnection<Backend>::TCPConnection(Reactor<Backend>& reactor, TCPOptions options)
    : core_{detail::TCPCore<Backend>::create(reactor.backend(), std::move(options))}, rx{core_->rx}, tx{core_->tx} {
    reactor.backend().attach(core_);
}

// The backends this library is built with.
namespace detail {
template class TCPCore<IoUringBackend>;
template class TCPCore<EpollBackend>;
} // namespace detail
template class TCPConnection<IoUringBackend>;
template class TCPConnection<EpollBackend>;

} // namespace turboq::reactor
