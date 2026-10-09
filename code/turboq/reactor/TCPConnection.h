// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <linux/time_types.h>
#include <sys/socket.h>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <utility>

#include <turboq/Platform.h>

#include "Address.h"
#include "MirroredBuffer.h"
#include "Types.h"
#include "detail/IoHandler.h"
#include "detail/StreamObserver.h"

namespace turboq::reactor {

struct TCPOptions {
    /// Where to connect. For a host name, resolve() it first (blocking, keep it off hot threads).
    Endpoint endpoint{};
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
    /// SO_RCVBUF / SO_SNDBUF. None: the system default.
    std::optional<int> socketRecvBufferSize{};
    std::optional<int> socketSendBufferSize{};
};

namespace detail {

/// Implementation of TCPConnection (see there): a TCP socket between the rx/tx rings. Owned by the
/// handle, released through the reactor.
///
/// Also the base of stream layers that sit on the socket (TLSCore). They plug in through a few
/// virtual hooks; the plain TCP data path calls none of them except the ones named below:
///   onConnected()     once per connection
///   armRecv()         when receiving (re)starts after a stall or a handshake
///   resumeRecv()      when consume() unblocks a full rx ring
///   startSend(), sendDirect()  per flush() / poll() with committed data
///   onCloseRequested(), onClosed(), resetSession()  connection life cycle
template <typename Backend>
class TCPCore : public IoHandler {
public:
    class Rx {
    private:
        friend class TCPCore;
        TCPCore* conn_;

        explicit Rx(TCPCore* conn) noexcept : conn_{conn} {}

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

        /// Reactor::now() of the poll that delivered the most recent data.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto timestamp() const noexcept -> Timestamp {
            return conn_->rxTimestamp_;
        }
    };

    class Tx {
    private:
        friend class TCPCore;
        TCPCore* conn_;

        explicit Tx(TCPCore* conn) noexcept : conn_{conn} {}

    public:
        /// Reserve `size` contiguous bytes. Returns an empty span when there is not enough room or
        /// the connection is closing/closed. Writing before the connection is Ready is allowed:
        /// the data goes out as soon as the connection is established.
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

protected:
    Backend& ring_;
    TCPOptions options_;
    StreamObserver* observer_{nullptr};
    MirroredBuffer rxBuffer_;
    MirroredBuffer txBuffer_;

    int fd_{-1};
    ConnectionState state_{ConnectionState::Idle};
    std::error_code error_;

    Timestamp rxTimestamp_{};
    std::size_t prepared_{0};
    std::uint32_t inflight_{0};
    bool recvInFlight_{false};
    bool sendInFlight_{false};
    bool rxStalled_{false};
    MirroredBuffer* sendSource_{nullptr}; // ring the in-flight send reads from

    sockaddr_storage address_{};
    socklen_t addressLength_{0};
    __kernel_timespec connectTimeout_{};

public:
    Rx rx{this};
    Tx tx{this};

    TCPCore(TCPCore const&) = delete;
    TCPCore& operator=(TCPCore const&) = delete;

    ~TCPCore() noexcept override;

    /// Validate options, allocate the rings. Throws std::system_error.
    [[nodiscard]] static auto create(Backend& ring, TCPOptions options) -> std::unique_ptr<TCPCore>;

    /// Start connecting. Allowed in Idle and Closed states (i.e. this is also "reconnect").
    /// Clears both queues. Synchronous failures (resolution, socket()) are returned and also leave
    /// the connection in Closed state with error() set; asynchronous ones arrive via state().
    auto connect() -> std::expected<void, std::error_code>;

    /// Start closing; see TCPConnection::close().
    void close() noexcept;

    [[nodiscard]] auto state() const noexcept -> ConnectionState {
        return state_;
    }

    /// Why the connection got closed. Empty after a user initiated close().
    [[nodiscard]] auto error() const noexcept -> std::error_code {
        return error_;
    }

    [[nodiscard]] auto options() const noexcept -> TCPOptions const& {
        return options_;
    }

    /// Change where the next connect() goes (layers that resolve a host name per connect).
    void setEndpoint(Endpoint const& endpoint) noexcept {
        options_.endpoint = endpoint;
    }

    /// Layering hook (WebSocket): notified about readiness, data and close from inside the
    /// reactor's completion processing.
    void setObserver(StreamObserver* observer) noexcept {
        observer_ = observer;
    }

    /// Native socket descriptor (-1 when closed). For setsockopt()/getsockopt() only: do not read
    /// from or write to it directly.
    [[nodiscard]] auto nativeHandle() const noexcept -> int {
        return fd_;
    }

protected:
    TCPCore(Backend& ring, TCPOptions options, MirroredBuffer rxBuffer, MirroredBuffer txBuffer) noexcept;

    /// Throws std::system_error for options no TCP connection can work with.
    static void validate(TCPOptions const& options);
    [[nodiscard]] static auto allocateRing(std::size_t size, char const* what) -> MirroredBuffer;

    void onCompletion(OpCode op, std::int32_t res, std::uint32_t flags) noexcept override;
    void onTxReady() noexcept override;

    [[nodiscard]] auto retirable() const noexcept -> bool override {
        return (state_ == ConnectionState::Closed || state_ == ConnectionState::Idle) && inflight_ == 0;
    }

    void beginRetire() noexcept override {
        observer_ = nullptr;
        this->close();
    }

    // Hooks for stream layers.

    /// TCP connect completed. Plain TCP: Ready, then startStreaming().
    virtual void onConnected() noexcept;
    /// Arm a receive into the rx ring (or wherever the layer receives into).
    virtual void armRecv() noexcept;
    /// consume() freed space in a full rx ring.
    virtual void resumeRecv() noexcept;
    /// Submit an io_uring send of committed data, if any and none is in flight.
    virtual void startSend() noexcept;
    /// flush() with directSend: synchronous non-blocking send, the rest via startSend().
    virtual void sendDirect() noexcept;
    /// close() on a Ready connection, right before shutdown: send what can be sent.
    virtual void onCloseRequested() noexcept;
    /// The socket is closed (state is about to become Closed).
    virtual void onClosed() noexcept {}
    /// connect() starts a new session: drop the state of the previous one.
    virtual void resetSession() noexcept {}

    // Building blocks for layers.

    /// Just became Ready: start receiving, send what was committed meanwhile, tell the observer.
    void startStreaming() noexcept;
    void submitRecv(std::span<std::byte> buffer) noexcept;
    void submitSend(MirroredBuffer& source) noexcept;
    void sendNow(MirroredBuffer& source) noexcept;
    /// Non-blocking send of everything `source` holds, as far as the socket takes it.
    void drainOnClose(MirroredBuffer& source) noexcept;
    void handleRecvResult(std::int32_t res) noexcept;
    /// After a completion was handled: finish a pending close once nothing is in flight.
    void checkClosed() noexcept {
        if (state_ == ConnectionState::Closing && inflight_ == 0) {
            this->finishClose();
        }
    }

    auto failSync(std::error_code ec) noexcept -> std::unexpected<std::error_code>;
    void fail(std::error_code ec) noexcept;
    void beginClose() noexcept;
    void finishClose() noexcept;

    void markTxDirty() noexcept;
    void flushTx() noexcept;
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
/// For TLS see TLSConnection.
template <typename Backend>
class TCPConnection {
private:
    detail::CorePtr<detail::TCPCore<Backend>> core_;

public:
    typename detail::TCPCore<Backend>::Rx rx;
    typename detail::TCPCore<Backend>::Tx tx;

    /// Create a connection served by `reactor` (which must outlive it). Does not connect: call
    /// connect(). Throws std::system_error on invalid options or if the rings can't be allocated.
    TCPConnection(Reactor<Backend>& reactor, TCPOptions options);

    TCPConnection(TCPConnection const&) = delete;
    TCPConnection& operator=(TCPConnection const&) = delete;

    /// Moves the connection, in-flight operations included. The moved-from object may only be
    /// destroyed or assigned to.
    TCPConnection(TCPConnection&& other) noexcept = default;

    TCPConnection& operator=(TCPConnection&& other) noexcept = default;

    /// Closes the connection if needed. Never blocks: when operations are still in flight the
    /// reactor finishes them and frees the connection's memory in a later poll().
    ~TCPConnection() noexcept = default;

    /// Start connecting. Allowed in Idle and Closed states (i.e. this is also "reconnect").
    /// Clears both queues. Synchronous failures (resolution, socket()) are returned and also leave
    /// the connection in Closed state with error() set; asynchronous ones arrive via state().
    auto connect() -> std::expected<void, std::error_code> {
        return core_->connect();
    }

    /// Start closing. Committed tx data is sent first as far as the socket takes it without
    /// blocking. The connection becomes Closed once all in-flight operations have completed
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

    [[nodiscard]] auto options() const noexcept -> TCPOptions const& {
        return core_->options();
    }

    /// Native socket descriptor (-1 when closed). For setsockopt()/getsockopt() only: do not read
    /// from or write to it directly.
    [[nodiscard]] auto nativeHandle() const noexcept -> int {
        return core_->nativeHandle();
    }
};

} // namespace turboq::reactor
