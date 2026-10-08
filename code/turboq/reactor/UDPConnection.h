// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <netinet/in.h>
#include <sys/socket.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <turboq/MappedRegion.h>
#include <turboq/Platform.h>

#include "Address.h"
#include "MirroredBuffer.h"
#include "Types.h"
#include "detail/IoHandler.h"

namespace turboq::reactor {

/// Kernel receive timestamps (SO_TIMESTAMPING).
enum class Timestamping : std::uint8_t {
    None,
    /// Kernel software timestamp taken when the packet enters the network stack.
    Software,
    /// NIC hardware timestamp (plus the software one). Hardware RX timestamping must also be enabled
    /// on the interface itself (SIOCSHWTSTAMP, e.g. `hwstamp_ctl -i eth0 -r 1`, needs
    /// CAP_NET_ADMIN), otherwise only the software timestamp is filled.
    Hardware,
};

struct UDPOptions {
    /// Multicast feed to subscribe to: the group to join and the port it is published on.
    std::optional<Endpoint> group{};
    /// Source for source-specific multicast (MCAST_JOIN_SOURCE_GROUP). None for any source.
    std::optional<IPAddress> source{};
    /// Interface name used to join the group and to send multicast. None: the kernel chooses by
    /// routing table.
    std::optional<std::string> interface{};
    /// Bind to the group address instead of `local`. Together with IP_MULTICAST_ALL = 0 (always
    /// set) this guarantees the socket only sees datagrams of its own group, even when other
    /// sockets in the system joined other groups on the same port (classic A/B feed setup).
    bool bindToGroup = true;

    /// Local address and port to bind to, taken as is (0.0.0.0 and port 0 included). None: the
    /// wildcard address of the socket's family and an ephemeral port (fine for send-only sockets).
    /// With a group the port is always the group's; the address is used when bindToGroup is off.
    std::optional<Endpoint> local{};

    /// Default destination for tx (unicast or multicast). None: the socket is receive-only and
    /// tx.prepare() always fails.
    std::optional<Endpoint> remote{};

    /// Receive buffers handed to the kernel (provided buffer ring). This is also the rx queue depth:
    /// once all buffers are queued for the user, the kernel holds new datagrams in the socket
    /// buffer and drops them when it overflows (see Rx::drops()). Rounded up to a power of two,
    /// at most 32768.
    unsigned bufferCount = 4096;
    /// Largest payload a receive buffer can hold. Longer datagrams are truncated (Datagram::truncated).
    std::size_t maxDatagramSize = 2048;

    /// Transmit ring size in bytes and maximum number of queued datagrams.
    std::size_t txBufferSize = 64u * 1024;
    unsigned txQueueDepth = 1024;
    /// flush() sends synchronously with sendto(MSG_DONTWAIT), falling back to io_uring on EAGAIN.
    bool directSend = true;

    Timestamping timestamping = Timestamping::Software;

    /// SO_RCVBUF / SO_SNDBUF. None: the system default. The receive buffer is set with
    /// SO_RCVBUFFORCE first (ignores net.core.rmem_max, needs CAP_NET_ADMIN), then SO_RCVBUF.
    std::optional<int> socketRecvBufferSize{};
    std::optional<int> socketSendBufferSize{};

    /// SO_REUSEADDR: several sockets/processes may bind the same group and port.
    bool reuseAddress = true;
    int multicastTtl = 1;
    bool multicastLoop = true;
};

/// Metadata of a received datagram.
struct DatagramInfo {
    /// Reactor::now() of the poll that delivered the datagram.
    Timestamp receiveTime{};
    /// Kernel software receive timestamp, Timestamp{} if not available.
    Timestamp softwareTimestamp{};
    /// NIC hardware receive timestamp, Timestamp{} if not available. Taken by the NIC clock: comparable
    /// with the others only when that clock is synchronized to the system one (phc2sys).
    Timestamp hardwareTimestamp{};
    /// Sender.
    Endpoint source{};
    /// Datagram was longer than UDPOptions::maxDatagramSize.
    bool truncated{false};
};

namespace detail {

/// Implementation of UDPConnection (see there). Owned by the handle, released through the reactor.
template <typename Backend>
class UDPCore final : public IoHandler {
private:
    struct Entry {
        std::span<std::byte const> payload;
        DatagramInfo info;
        std::uint16_t bufferId;
    };

public:
    class Rx {
    private:
        friend class UDPCore;
        UDPCore* conn_;

        explicit Rx(UDPCore* conn) noexcept : conn_{conn} {}

    public:
        /// Payload of the oldest datagram. Empty span if the queue is empty (a received empty
        /// datagram is reported too: check empty() to tell them apart).
        [[nodiscard]] TURBOQ_FORCE_INLINE auto fetch() const noexcept -> std::span<std::byte const> {
            if (conn_->rxHead_ == conn_->rxTail_) {
                return {};
            }
            return conn_->front().payload;
        }

        /// Metadata of the oldest datagram. The queue must not be empty.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto info() const noexcept -> DatagramInfo const& {
            assert(!this->empty());
            return conn_->front().info;
        }

        /// Release the oldest datagram and give its buffer back to the kernel.
        TURBOQ_FORCE_INLINE void consume() noexcept {
            assert(!this->empty());
            conn_->recycle(conn_->front().bufferId);
            ++conn_->rxHead_;
            if (conn_->rxStalled_) [[unlikely]] {
                conn_->resumeRecv();
            }
        }

        [[nodiscard]] TURBOQ_FORCE_INLINE auto empty() const noexcept -> bool {
            return conn_->rxHead_ == conn_->rxTail_;
        }

        /// Number of queued datagrams.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto size() const noexcept -> std::size_t {
            return static_cast<std::size_t>(conn_->rxTail_ - conn_->rxHead_);
        }

        /// Datagrams the kernel dropped on this socket because its receive buffer was full
        /// (SO_RXQ_OVFL), as of the last received datagram. Monotonic.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto drops() const noexcept -> std::uint32_t {
            return conn_->kernelDrops_;
        }
    };

    class Tx {
    private:
        friend class UDPCore;
        UDPCore* conn_;

        explicit Tx(UDPCore* conn) noexcept : conn_{conn} {}

    public:
        /// Reserve space for one datagram. Empty span if the connection has no destination, is
        /// not Ready, or the tx queue is full.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto prepare(std::size_t size) noexcept -> std::span<std::byte> {
            if (conn_->state_ != ConnectionState::Ready || conn_->remoteLength_ == 0 ||
                conn_->txTail_ - conn_->txHead_ == conn_->txLengths_.size() || size > conn_->txBuffer_.available())
                [[unlikely]] {
                return {};
            }
            conn_->prepared_ = size;
            return conn_->txBuffer_.writable().first(size);
        }

        /// Queue the datagram reserved by the last prepare().
        TURBOQ_FORCE_INLINE void commit() noexcept {
            this->commit(conn_->prepared_);
        }

        /// Queue the first `size` bytes reserved by the last prepare() as one datagram.
        TURBOQ_FORCE_INLINE void commit(std::size_t size) noexcept {
            assert(size <= conn_->prepared_);
            conn_->txBuffer_.produce(size);
            conn_->txLengths_[conn_->txTail_ & conn_->txMask_] = static_cast<std::uint32_t>(size);
            ++conn_->txTail_;
            conn_->prepared_ = 0;
            if (!conn_->txDirty_) [[unlikely]] {
                conn_->markTxDirty();
            }
        }

        /// prepare() + memcpy + commit(). Returns false if the datagram can't be queued.
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

        /// Send queued datagrams now instead of waiting for the next Reactor::poll().
        void flush() noexcept {
            conn_->flushTx();
        }

        /// Queued datagrams not yet handed to the kernel.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto size() const noexcept -> std::size_t {
            return static_cast<std::size_t>(conn_->txTail_ - conn_->txHead_);
        }

        /// Datagrams dropped because sending failed (the error is in lastError()). UDP send errors
        /// (ICMP unreachable, ENOBUFS, ...) do not close the connection.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto errors() const noexcept -> std::uint64_t {
            return conn_->txErrors_;
        }

        [[nodiscard]] TURBOQ_FORCE_INLINE auto lastError() const noexcept -> std::error_code {
            return conn_->txLastError_;
        }
    };

private:
    Backend& ring_;
    UDPOptions options_;

    int fd_{-1};
    ConnectionState state_{ConnectionState::Idle};
    std::error_code error_;
    std::uint32_t inflight_{0};
    bool recvInFlight_{false};
    bool sendInFlight_{false};
    bool rxStalled_{false};

    // Provided buffer ring: kernel-shared ring of buffer descriptors + the buffers themselves.
    unsigned bufferCount_;
    std::size_t bufferStride_{0}; // distance between buffers (cache line aligned)
    unsigned bufferLength_{0};    // usable length given to the kernel: exactly maxDatagramSize of payload
    MappedRegion buffers_;
    std::optional<typename Backend::BufferPool> pool_; // after buffers_: destroyed first
    msghdr recvTemplate_{}; // describes name/control sizes for multishot recvmsg, must outlive it

    // Received datagrams waiting for the user, at most bufferCount_ (each holds a buffer).
    std::unique_ptr<Entry[]> rxEntries_;
    std::uint64_t rxHead_{0};
    std::uint64_t rxTail_{0};
    std::uint32_t kernelDrops_{0};

    // Datagrams waiting to be sent: bytes in a ring, lengths in a parallel queue.
    MirroredBuffer txBuffer_;
    std::vector<std::uint32_t> txLengths_;
    std::uint64_t txMask_{0};
    std::uint64_t txHead_{0};
    std::uint64_t txTail_{0};
    std::size_t prepared_{0};
    std::uint64_t txErrors_{0};
    std::error_code txLastError_;
    sockaddr_storage remote_{};
    socklen_t remoteLength_{0};
    iovec sendIov_{};
    msghdr sendMsg_{};

public:
    Rx rx{this};
    Tx tx{this};

    UDPCore(UDPCore const&) = delete;
    UDPCore& operator=(UDPCore const&) = delete;

    ~UDPCore() noexcept override;

    /// Create the socket, bind, join the group and start receiving. Synchronous: on success the
    /// connection is Ready, on failure Closed with error() set. Allowed in Idle and Closed states.
    /// Drops datagrams still queued from a previous session.
    auto open() -> std::expected<void, std::error_code>;

    /// Start closing; Closed after in-flight operations completed (one of the next polls).
    /// Datagrams already received stay readable until open().
    void close() noexcept;

    [[nodiscard]] auto state() const noexcept -> ConnectionState {
        return state_;
    }

    [[nodiscard]] auto error() const noexcept -> std::error_code {
        return error_;
    }

    [[nodiscard]] auto options() const noexcept -> UDPOptions const& {
        return options_;
    }

    /// Bound local address and port (useful with port 0). Default Endpoint when closed.
    [[nodiscard]] auto localEndpoint() const noexcept -> Endpoint;

    /// Native socket descriptor (-1 when closed). For setsockopt()/getsockopt() only.
    [[nodiscard]] auto nativeHandle() const noexcept -> int {
        return fd_;
    }

private:
    UDPCore(Backend& ring, UDPOptions options);

public:
    /// Validate options, allocate buffers, register the buffer ring. Throws std::system_error.
    [[nodiscard]] static auto create(Backend& ring, UDPOptions options) -> UDPCore*;

private:
    void onCompletion(detail::OpCode op, std::int32_t res, std::uint32_t flags) noexcept override;
    void onTxReady() noexcept override;

    [[nodiscard]] auto retirable() const noexcept -> bool override {
        return (state_ == ConnectionState::Closed || state_ == ConnectionState::Idle) && inflight_ == 0;
    }

    void beginRetire() noexcept override {
        this->close();
    }

    void onRetired() noexcept override;

    [[nodiscard]] TURBOQ_FORCE_INLINE auto front() const noexcept -> Entry const& {
        return rxEntries_[rxHead_ & (bufferCount_ - 1)];
    }

    TURBOQ_FORCE_INLINE void recycle(std::uint16_t bufferId) noexcept {
        pool_->recycle(bufferId);
    }

    void onDatagram(std::int32_t res, std::uint32_t flags) noexcept;

    auto failSync(std::error_code ec) noexcept -> std::unexpected<std::error_code>;
    void fail(std::error_code ec) noexcept;
    void beginClose() noexcept;
    void finishClose() noexcept;

    void armRecv() noexcept;
    void resumeRecv() noexcept;

    void markTxDirty() noexcept;
    void flushTx() noexcept;
    void startSend() noexcept;
    void sendDirect() noexcept;
    void popDatagram() noexcept;
    void dropDatagram(int error) noexcept;
};

} // namespace detail

/// UDP socket (unicast or multicast) exposed as two datagram queues.
///
///   rx.fetch()      -> payload of the oldest received datagram (empty span if none)
///   rx.info()       -> its metadata: timestamps, sender
///   rx.consume()    -> release it, the buffer goes back to the kernel
///   tx.prepare(n)   -> writable span for one datagram of n bytes
///   tx.commit(n)    -> queue it; sent on the next Reactor::poll() or by tx.flush()
///
/// Receive uses multishot recvmsg with a provided buffer ring: one armed request delivers every
/// datagram straight into a buffer from the ring, no per-datagram submission.
///
/// Sequencing, gap detection and A/B arbitration are not done here: use two connections and
/// arbitrate in your code.
template <typename Backend>
class UDPConnection {
private:
    detail::UDPCore<Backend>* core_;

public:
    typename detail::UDPCore<Backend>::Rx rx;
    typename detail::UDPCore<Backend>::Tx tx;

    /// Create a socket served by `reactor` (which must outlive it): allocates and registers the
    /// receive buffers. Does not open: call open(). Throws std::system_error on invalid options,
    /// allocation or registration failure. With TaskRunMode::Deferred create it before the first
    /// poll() or on the polling thread.
    UDPConnection(Reactor<Backend>& reactor, UDPOptions options);

    UDPConnection(UDPConnection const&) = delete;
    UDPConnection& operator=(UDPConnection const&) = delete;

    UDPConnection(UDPConnection&& other) noexcept
        : core_{std::exchange(other.core_, nullptr)}, rx{other.rx}, tx{other.tx} {}

    UDPConnection& operator=(UDPConnection&& other) noexcept {
        if (this != &other) {
            detail::releaseCore(core_);
            core_ = std::exchange(other.core_, nullptr);
            rx = other.rx;
            tx = other.tx;
        }
        return *this;
    }

    /// Closes the socket if needed. Never blocks (see TCPConnection::~TCPConnection()).
    ~UDPConnection() noexcept {
        detail::releaseCore(core_);
    }

    /// Create the socket, bind, join the group and start receiving. Synchronous: on success the
    /// connection is Ready, on failure Closed with error() set. Allowed in Idle and Closed states.
    /// Drops datagrams still queued from a previous session.
    auto open() -> std::expected<void, std::error_code> {
        return core_->open();
    }

    /// Start closing; Closed after in-flight operations completed (one of the next polls).
    /// Datagrams already received stay readable until open().
    void close() noexcept {
        core_->close();
    }

    [[nodiscard]] auto state() const noexcept -> ConnectionState {
        return core_->state();
    }

    [[nodiscard]] auto error() const noexcept -> std::error_code {
        return core_->error();
    }

    [[nodiscard]] auto options() const noexcept -> UDPOptions const& {
        return core_->options();
    }

    /// Bound local address and port (useful with port 0). Default Endpoint when closed.
    [[nodiscard]] auto localEndpoint() const noexcept -> Endpoint {
        return core_->localEndpoint();
    }

    /// Native socket descriptor (-1 when closed). For setsockopt()/getsockopt() only.
    [[nodiscard]] auto nativeHandle() const noexcept -> int {
        return core_->nativeHandle();
    }
};

} // namespace turboq::reactor
