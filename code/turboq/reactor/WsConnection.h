// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <linux/time_types.h>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <turboq/Platform.h>

#include "TcpConnection.h"
#include "Types.h"
#include "detail/IoHandler.h"
#include "detail/StreamObserver.h"
#include "detail/WsProtocol.h"

namespace turboq::reactor {

class Reactor;

enum class WsOpcode : std::uint8_t {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
};

/// Masking of client frames. RFC 6455 requires a fresh unpredictable key per frame; Zero skips
/// the XOR pass entirely but violates the RFC (servers rarely check, proxies may care).
enum class WsMasking : std::uint8_t {
    /// Per-frame key from a fast PRNG seeded from getrandom().
    Random,
    Zero,
};

struct WsOptions {
    /// ws://host[:port]/path?query or wss://... (wss enables TLS with the `tls` settings below).
    std::string url{};
    /// Extra request headers for the upgrade (Origin, API keys, ...).
    std::vector<std::pair<std::string, std::string>> headers{};
    /// Sec-WebSocket-Protocol to request; the server must accept it. Empty for none.
    std::string subprotocol{};
    /// TLS settings for wss:// (`enabled` is implied by the scheme).
    TlsOptions tls{};

    /// Underlying TCP rings. The largest message (all its fragments, plus unconsumed messages
    /// before it) must fit into the rx ring.
    std::size_t rxBufferSize = 1u << 20;
    std::size_t txBufferSize = 1u << 20;
    /// Received messages that can wait for the user. When full, parsing pauses until consume().
    std::size_t maxQueuedMessages = 65536;
    /// Reject messages above this size (close 1009). 0 means limited only by rxBufferSize.
    std::size_t maxMessageSize = 0;

    std::chrono::milliseconds connectTimeout{5000};
    /// Timeout for the HTTP upgrade (after TCP connect and TLS handshake).
    std::chrono::milliseconds handshakeTimeout{5000};
    bool noDelay = true;
    bool directSend = true;
    WsMasking masking = WsMasking::Random;
};

namespace detail {

/// Implementation of WsConnection (see there). Owned by the handle, released through the reactor.
class WsCore final : public IoHandler, private StreamObserver {
private:
    struct Entry {
        std::uint64_t begin;   // stream position of the message's first frame header
        std::uint64_t payload; // stream position of the payload
        std::uint64_t timestamp;
        std::uint32_t size;
        WsOpcode opcode;
    };

public:
    class Rx {
    private:
        friend class WsCore;
        WsCore* conn_;

        explicit Rx(WsCore* conn) noexcept : conn_{conn} {}

    public:
        /// Payload of the oldest message, empty span if there is none (check empty() to tell an
        /// empty message apart). Valid until consume().
        [[nodiscard]] TURBOQ_FORCE_INLINE auto fetch() const noexcept -> std::span<std::byte const> {
            if (conn_->rxHead_ == conn_->rxTail_) {
                return {};
            }
            auto const& entry = conn_->front();
            return {conn_->streamPointer(entry.payload), entry.size};
        }

        /// Text or Binary. The queue must not be empty.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto opcode() const noexcept -> WsOpcode {
            assert(!this->empty());
            return conn_->front().opcode;
        }

        /// CLOCK_REALTIME (ns) of the poll that received the message's last byte.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto timestamp() const noexcept -> std::uint64_t {
            assert(!this->empty());
            return conn_->front().timestamp;
        }

        /// Release the oldest message.
        void consume() noexcept {
            conn_->consumeFront();
        }

        [[nodiscard]] TURBOQ_FORCE_INLINE auto empty() const noexcept -> bool {
            return conn_->rxHead_ == conn_->rxTail_;
        }

        [[nodiscard]] TURBOQ_FORCE_INLINE auto size() const noexcept -> std::size_t {
            return static_cast<std::size_t>(conn_->rxTail_ - conn_->rxHead_);
        }
    };

    class Tx {
    private:
        friend class WsCore;
        WsCore* conn_;

        explicit Tx(WsCore* conn) noexcept : conn_{conn} {}

    public:
        /// Reserve space for a message of up to `size` bytes. Empty span if the connection is not
        /// Ready or the tx ring is full. Do not call Reactor::poll() between prepare() and commit().
        [[nodiscard]] auto prepare(std::size_t size) noexcept -> std::span<std::byte> {
            return conn_->prepareMessage(size);
        }

        /// Send the first `size` bytes of the last prepare() as one message.
        void commit(std::size_t size, WsOpcode opcode = WsOpcode::Text) noexcept {
            conn_->commitMessage(size, opcode);
        }

        /// Commit everything reserved by the last prepare().
        void commit(WsOpcode opcode = WsOpcode::Text) noexcept {
            conn_->commitMessage(conn_->prepared_, opcode);
        }

        /// prepare() + memcpy + commit(). Returns false if the message can't be queued.
        [[nodiscard]] auto push(std::span<std::byte const> data, WsOpcode opcode = WsOpcode::Text) noexcept -> bool {
            auto buffer = this->prepare(data.size());
            if (buffer.size() != data.size()) [[unlikely]] {
                return false;
            }
            if (!data.empty()) {
                std::memcpy(buffer.data(), data.data(), data.size());
            }
            this->commit(data.size(), opcode);
            return true;
        }

        /// \overload
        [[nodiscard]] auto push(std::string_view text) noexcept -> bool {
            return this->push({reinterpret_cast<std::byte const*>(text.data()), text.size()}, WsOpcode::Text);
        }

        /// Send queued frames now instead of waiting for the next Reactor::poll().
        void flush() noexcept {
            conn_->flushTx();
        }
    };

private:
    friend class ::turboq::reactor::Reactor;

    Reactor& reactor_;
    std::unique_ptr<TcpCore> tcpCore_; // owned: lives and dies with this connection
    TcpCore& tcp_;
    WsOptions options_;
    WsUrl url_;

    ConnectionState state_{ConnectionState::Idle};
    std::error_code error_;
    int httpStatus_{0};
    std::string closeReason_;
    std::string handshakeKey_;

    // Upgrade timeout (IORING_OP_TIMEOUT owned by this connection).
    __kernel_timespec timeout_{};
    bool timeoutArmed_{false};
    std::uint32_t inflight_{0};

    // rx: positions are absolute offsets in the TCP byte stream of the current session.
    std::vector<Entry> entries_;
    std::uint64_t entriesMask_{0};
    std::uint64_t rxHead_{0};
    std::uint64_t rxTail_{0};
    std::uint64_t streamHead_{0}; // first byte still held in the TCP rx ring
    std::uint64_t parsePos_{0};   // first byte not parsed yet
    bool parseBlocked_{false};
    bool assembling_{false};
    WsOpcode assemblyOpcode_{WsOpcode::Text};
    std::uint64_t assemblyBegin_{0};
    std::uint64_t assemblyPayload_{0};
    std::size_t assemblySize_{0};
    std::size_t maxMessageSize_{0};

    // tx
    std::byte* reservation_{nullptr};
    std::size_t reservedHeader_{0};
    std::size_t prepared_{0};
    std::vector<std::byte> pendingControl_; // framed control messages waiting for a frame boundary
    std::uint64_t prngState_{0};

public:
    Rx rx{this};
    Tx tx{this};

    WsCore(WsCore const&) = delete;
    WsCore& operator=(WsCore const&) = delete;

    ~WsCore() noexcept override = default;

    /// Connect: TCP, TLS (wss), HTTP upgrade. Allowed in Idle and Closed states. Drops messages
    /// still queued from the previous session.
    auto connect() -> std::expected<void, std::error_code>;

    /// Send a Close frame (when Ready) and close the connection without waiting for the echo.
    void close(std::uint16_t code = 1000, std::string_view reason = {}) noexcept;

    /// Send a Ping (payload up to 125 bytes). Returns false if not Ready or too long.
    auto ping(std::span<std::byte const> payload = {}) noexcept -> bool;

    /// Connecting (TCP), Handshaking (TLS handshake or HTTP upgrade), Ready, Closing, Closed.
    [[nodiscard]] auto state() const noexcept -> ConnectionState {
        if (state_ == ConnectionState::Connecting) {
            // TCP/TLS phase: report the transport's progress (it may already be failing).
            auto const transport = tcp_.state();
            if (transport == ConnectionState::Handshaking || transport == ConnectionState::Closing) {
                return transport;
            }
        }
        return state_;
    }

    /// Why the connection closed: a getWsCloseCategory() code for a Close from the server,
    /// Error::Ws* for protocol problems, the TCP/TLS error otherwise. Empty after close().
    [[nodiscard]] auto error() const noexcept -> std::error_code {
        return error_;
    }

    /// HTTP status of the upgrade response (101 on success, e.g. 403 when rejected).
    [[nodiscard]] auto httpStatus() const noexcept -> int {
        return httpStatus_;
    }

    /// Reason text of the server's Close frame.
    [[nodiscard]] auto closeReason() const noexcept -> std::string_view {
        return closeReason_;
    }

    [[nodiscard]] auto options() const noexcept -> WsOptions const& {
        return options_;
    }

    [[nodiscard]] auto lastReceiveTime() const noexcept -> std::uint64_t {
        return tcp_.rx.timestamp();
    }

    [[nodiscard]] auto tlsVersion() const noexcept -> std::string_view {
        return tcp_.tlsVersion();
    }

    [[nodiscard]] auto tlsCipher() const noexcept -> std::string_view {
        return tcp_.tlsCipher();
    }

    [[nodiscard]] auto nativeHandle() const noexcept -> int {
        return tcp_.nativeHandle();
    }

private:
    WsCore(Reactor& reactor, TcpCore* tcp, WsOptions options, WsUrl url);

public:
    /// Parse the URL, create the TCP layer. Throws std::system_error.
    [[nodiscard]] static auto create(Reactor& reactor, WsOptions options) -> WsCore*;

private:
    void onCompletion(detail::OpCode op, std::int32_t res, std::uint32_t flags) noexcept override;
    void onTxReady() noexcept override {}

    [[nodiscard]] auto retirable() const noexcept -> bool override {
        return (state_ == ConnectionState::Closed || state_ == ConnectionState::Idle) && inflight_ == 0 &&
               tcp_.retirable();
    }

    void beginRetire() noexcept override {
        this->close(1001); // going away
    }

    void unlinkPendingTx(std::vector<IoHandler*>& list) noexcept override {
        std::erase(list, this);
        std::erase(list, static_cast<IoHandler*>(&tcp_));
    }

    void onStreamReady() noexcept override;
    void onStreamData() noexcept override;
    void onStreamClosed() noexcept override;

    [[nodiscard]] TURBOQ_FORCE_INLINE auto front() const noexcept -> Entry const& {
        return entries_[rxHead_ & entriesMask_];
    }

    /// Address of a stream position that is still in the TCP rx ring.
    [[nodiscard]] TURBOQ_FORCE_INLINE auto streamPointer(std::uint64_t position) const noexcept -> std::byte* {
        return const_cast<std::byte*>(tcp_.rxBuffer_.readable().data()) + (position - streamHead_);
    }

    void consumeFront() noexcept;
    void releaseTo(std::uint64_t position) noexcept;
    void parseFrames() noexcept;
    void completeUpgrade() noexcept;

    auto prepareMessage(std::size_t size) noexcept -> std::span<std::byte>;
    void commitMessage(std::size_t size, WsOpcode opcode) noexcept;
    void flushTx() noexcept;
    void queueControl(WsOpcode opcode, std::span<std::byte const> payload) noexcept;
    void flushControl() noexcept;
    [[nodiscard]] auto nextMaskKey() noexcept -> std::uint32_t;

    void fail(std::error_code ec, std::uint16_t closeCode) noexcept;
    void beginClose() noexcept;
    void finishCloseIfDone() noexcept;
    void armTimeout() noexcept;
    void disarmTimeout() noexcept;
};

} // namespace detail

/// WebSocket client connection exposed as two message queues, on top of a TCP connection (with
/// kernel TLS for wss://).
///
///   rx.fetch()          -> payload of the oldest complete message (fragments already joined)
///   rx.opcode()         -> Text or Binary
///   rx.consume()        -> release it
///   tx.prepare(n)       -> writable span for a message of up to n bytes (frame header reserved)
///   tx.commit(n, op)    -> frame (header + masking in place) and queue it
///   tx.flush()          -> send now
///
/// Frames are parsed in Reactor::poll(): payloads stay where the kernel put them in the TCP rx
/// ring (zero copy; fragmented messages are joined in place). Ping is answered with Pong, a Close
/// from the server is echoed and closes the connection. UTF-8 of text messages is not validated.
class WsConnection {
private:
    detail::WsCore* core_;

public:
    detail::WsCore::Rx rx;
    detail::WsCore::Tx tx;

    /// Create a connection served by `reactor` (which must outlive it). Does not connect: call
    /// connect(). Throws std::system_error on an invalid URL/options or allocation failure.
    WsConnection(Reactor& reactor, WsOptions options);

    WsConnection(WsConnection const&) = delete;
    WsConnection& operator=(WsConnection const&) = delete;

    WsConnection(WsConnection&& other) noexcept
        : core_{std::exchange(other.core_, nullptr)}, rx{other.rx}, tx{other.tx} {}

    WsConnection& operator=(WsConnection&& other) noexcept {
        if (this != &other) {
            detail::releaseCore(core_);
            core_ = std::exchange(other.core_, nullptr);
            rx = other.rx;
            tx = other.tx;
        }
        return *this;
    }

    /// Sends Close if Ready and closes. Never blocks (see TcpConnection::~TcpConnection()).
    ~WsConnection() noexcept {
        detail::releaseCore(core_);
    }

    /// Connect: TCP, TLS (wss), HTTP upgrade. Allowed in Idle and Closed states. Drops messages
    /// still queued from the previous session.
    auto connect() -> std::expected<void, std::error_code> {
        return core_->connect();
    }

    /// Send a Close frame (when Ready) and close the connection without waiting for the echo.
    void close(std::uint16_t code = 1000, std::string_view reason = {}) noexcept {
        core_->close(code, reason);
    }

    /// Send a Ping (payload up to 125 bytes). Returns false if not Ready or too long.
    auto ping(std::span<std::byte const> payload = {}) noexcept -> bool {
        return core_->ping(payload);
    }

    /// Connecting (TCP), Handshaking (TLS handshake or HTTP upgrade), Ready, Closing, Closed.
    [[nodiscard]] auto state() const noexcept -> ConnectionState {
        return core_->state();
    }

    /// Why the connection closed: a getWsCloseCategory() code for a Close from the server,
    /// Error::Ws* for protocol problems, the TCP/TLS error otherwise. Empty after close().
    [[nodiscard]] auto error() const noexcept -> std::error_code {
        return core_->error();
    }

    /// HTTP status of the upgrade response (101 on success, e.g. 403 when rejected).
    [[nodiscard]] auto httpStatus() const noexcept -> int {
        return core_->httpStatus();
    }

    /// Reason text of the server's Close frame.
    [[nodiscard]] auto closeReason() const noexcept -> std::string_view {
        return core_->closeReason();
    }

    [[nodiscard]] auto options() const noexcept -> WsOptions const& {
        return core_->options();
    }

    /// CLOCK_REALTIME (ns, Reactor::now() clock) of the poll that last received anything on this
    /// connection, server pings included; 0 before the first byte. For liveness watchdogs: a quiet
    /// stream with regular pings is alive.
    [[nodiscard]] auto lastReceiveTime() const noexcept -> std::uint64_t {
        return core_->lastReceiveTime();
    }

    /// TLS version and cipher for wss://, empty otherwise.
    [[nodiscard]] auto tlsVersion() const noexcept -> std::string_view {
        return core_->tlsVersion();
    }

    [[nodiscard]] auto tlsCipher() const noexcept -> std::string_view {
        return core_->tlsCipher();
    }

    /// Native socket descriptor (-1 when closed). For setsockopt()/getsockopt() only.
    [[nodiscard]] auto nativeHandle() const noexcept -> int {
        return core_->nativeHandle();
    }
};

} // namespace turboq::reactor
