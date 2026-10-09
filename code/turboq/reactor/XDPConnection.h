// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#pragma once

#include <linux/if_xdp.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <turboq/MappedRegion.h>
#include <turboq/Platform.h>

#include "Address.h"
#include "Types.h"
#include "detail/IoHandler.h"
#include "detail/XdpProgram.h"

namespace turboq::reactor {

/// How the XDP program is attached to the interface.
enum class XDPAttachMode : std::uint8_t {
    /// Native (driver) mode if the driver supports XDP, generic otherwise.
    Auto,
    /// Native mode only: frames are taken in the driver, before any skb is allocated.
    Native,
    /// Generic (skb) mode only: works with every driver, but after the skb is built; little gain
    /// over a normal socket. For testing.
    Generic,
};

/// AF_XDP zero-copy: the NIC writes frames straight into the UMEM.
enum class XDPZeroCopy : std::uint8_t {
    /// Zero-copy if the driver supports it, copy mode otherwise.
    Prefer,
    /// Zero-copy or open() fails with Error::XdpZeroCopyUnavailable.
    Require,
    /// Copy mode: the kernel copies each frame into the UMEM.
    Disable,
};

struct XDPOptions {
    /// Interface to receive from.
    std::string interface;
    /// Receive queue of the interface. Frames arriving on other queues go to the kernel as usual:
    /// steer the feeds to this queue (ethtool -N <if> flow-type udp4 dst-port <port> action <queue>)
    /// or use an interface with one queue.
    unsigned queue = 0;

    /// Frames taken from the kernel: UDP datagrams (IPv4, or IPv6 without extension headers) to
    /// these destination ports; everything else goes to the kernel stack. Fragments after the
    /// first one and VLAN-tagged frames (when the NIC does not strip the tag) go to the kernel.
    std::vector<std::uint16_t> udpPorts{};
    /// Take every frame of the queue instead (udpPorts must be empty). The kernel then sees none of
    /// them, ARP and IGMP/MLD included: only for a queue that carries nothing but the feeds.
    bool redirectAll = false;

    /// Multicast groups to join on the interface (IGMP/MLD), so that the NIC accepts the frames
    /// and the switch forwards them. The memberships are held by ordinary sockets that never read.
    std::vector<IPAddress> groups{};
    /// Source for source-specific multicast. None for any source.
    std::optional<IPAddress> source{};

    /// UMEM frames: the receive queue depth (frames queued for the user plus frames the NIC can
    /// fill). Rounded up to a power of two.
    unsigned frameCount = 4096;
    /// UMEM frame size: the largest frame received. Power of two, 2048 to the page size.
    unsigned frameSize = 4096;

    XDPAttachMode attachMode = XDPAttachMode::Auto;
    XDPZeroCopy zeroCopy = XDPZeroCopy::Prefer;

    /// Hardware receive timestamps (FrameInfo::hardwareTimestamp) through XDP metadata
    /// (bpf_xdp_metadata_rx_timestamp, Linux 6.3+, native mode, drivers: mlx5, ice, igc, stmmac,
    /// veth, ...). Best effort: without support the frames have no timestamp (see
    /// XDPConnection::hardwareTimestamps()). As with sockets, the NIC must have hardware RX
    /// timestamping enabled (SIOCSHWTSTAMP, hwstamp_ctl -r 1).
    bool hardwareTimestamps = false;

    /// Busy polling: SO_PREFER_BUSY_POLL + SO_BUSY_POLL with this timeout, and every rx check that
    /// finds the queue empty drives the driver from this thread (a recvfrom() syscall) instead of
    /// waiting for an interrupt. Pair with `napi_defer_hard_irqs` / `gro_flush_timeout` on the
    /// interface. None: interrupt driven.
    std::optional<std::chrono::microseconds> busyPoll{};
    /// SO_BUSY_POLL_BUDGET: frames per busy poll.
    unsigned busyPollBudget = 64;

    /// Wake Reactor::wait() when frames arrive (one poll operation per burst). Off for pure
    /// busy-polling loops that only call Reactor::poll() and rx.
    bool wakeReactor = true;
};

/// Metadata of a received frame.
struct FrameInfo {
    /// Wall clock time when the rx queue first saw the frame (one clock read per batch of frames).
    Timestamp receiveTime{};
    /// NIC hardware receive timestamp, Timestamp{} if not available (XDPOptions::hardwareTimestamps).
    /// Taken by the NIC clock: comparable with the system clock only when synchronized (phc2sys).
    Timestamp hardwareTimestamp{};
};

/// Kernel counters of an AF_XDP socket (XDP_STATISTICS).
struct XDPStatistics {
    /// Frames dropped for other reasons (e.g. larger than a UMEM frame).
    std::uint64_t dropped = 0;
    /// The rx queue was full: the user did not consume fast enough.
    std::uint64_t rxQueueFull = 0;
    /// The NIC had no free frame to receive into (same cause as rxQueueFull, seen by the driver).
    std::uint64_t fillQueueEmpty = 0;
    std::uint64_t invalidDescriptors = 0;
};

namespace detail {

/// Implementation of XDPConnection (see there). Owned by the handle, released through the reactor.
template <typename Backend>
class XDPCore final : public IoHandler {
public:
    class Rx {
    private:
        friend class XDPCore;
        XDPCore* conn_;

        explicit Rx(XDPCore* conn) noexcept : conn_{conn} {}

    public:
        /// The oldest received frame (Ethernet header included). Empty span if there is none.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto fetch() const noexcept -> std::span<std::byte const> {
            if (this->empty()) {
                return {};
            }
            auto const& desc = conn_->front();
            return {conn_->umem_.data() + desc.addr, desc.len};
        }

        /// Metadata of the oldest frame. The queue must not be empty.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto info() const noexcept -> FrameInfo {
            assert(!this->empty());
            FrameInfo info{.receiveTime = conn_->batchTime_};
            if (conn_->timestamps_) {
                std::uint64_t nanoseconds;
                std::memcpy(
                    &nanoseconds, conn_->umem_.data() + conn_->front().addr - kXdpMetadataSize, sizeof(nanoseconds));
                if (nanoseconds != 0) {
                    info.hardwareTimestamp = Timestamp{std::chrono::nanoseconds{nanoseconds}};
                }
            }
            return info;
        }

        /// Release the oldest frame: its UMEM frame goes back to the NIC.
        TURBOQ_FORCE_INLINE void consume() noexcept {
            assert(!this->empty());
            conn_->release();
        }

        /// True if no frame is queued. Checks the kernel's queue: this is where new frames show up.
        [[nodiscard]] TURBOQ_FORCE_INLINE auto empty() const noexcept -> bool {
            return conn_->rxHead_ == conn_->rxCachedTail_ && conn_->refresh() == 0;
        }

        /// Frames queued for the user (checks the kernel's queue).
        [[nodiscard]] TURBOQ_FORCE_INLINE auto size() const noexcept -> std::size_t {
            if (conn_->rxHead_ == conn_->rxCachedTail_) {
                return conn_->refresh();
            }
            return conn_->rxCachedTail_ - conn_->rxHead_;
        }
    };

private:
    // One of the rings shared with the kernel (XDP_MMAP_OFFSETS).
    struct Ring {
        MappedRegion memory;
        std::uint32_t* producer{nullptr};
        std::uint32_t* consumer{nullptr};
        std::uint32_t* flags{nullptr};
        void* entries{nullptr};
    };

    Backend& ring_;
    XDPOptions options_;

    int fd_{-1};
    ConnectionState state_{ConnectionState::Idle};
    std::error_code error_;
    std::string diagnostic_;
    std::uint32_t inflight_{0};
    bool pollArmed_{false};
    bool zeroCopy_{false};
    bool native_{false};
    bool timestamps_{false};
    bool needWakeup_{false};

    unsigned frameCount_;
    unsigned frameSize_;
    std::uint32_t mask_;
    MappedRegion umem_;
    Ring rxRing_;
    Ring fillRing_;
    std::optional<XdpProgram> program_;
    std::vector<UniqueFd> memberships_;

    // rx ring: [rxHead_, rxCachedTail_) are frames seen by the user, the kernel may have produced
    // more. Free-running, as the kernel's indices.
    std::uint32_t rxHead_{0};
    std::uint32_t rxCachedTail_{0};
    Timestamp batchTime_{};
    // fill ring producer index.
    std::uint32_t fillTail_{0};

public:
    Rx rx{this};

    XDPCore(XDPCore const&) = delete;
    XDPCore& operator=(XDPCore const&) = delete;

    ~XDPCore() noexcept override;

    auto open() -> std::expected<void, std::error_code>;
    void close() noexcept;

    [[nodiscard]] auto state() const noexcept -> ConnectionState {
        return state_;
    }

    [[nodiscard]] auto error() const noexcept -> std::error_code {
        return error_;
    }

    [[nodiscard]] auto diagnostic() const noexcept -> std::string const& {
        return diagnostic_;
    }

    [[nodiscard]] auto options() const noexcept -> XDPOptions const& {
        return options_;
    }

    [[nodiscard]] auto zeroCopy() const noexcept -> bool {
        return zeroCopy_;
    }

    [[nodiscard]] auto nativeMode() const noexcept -> bool {
        return native_;
    }

    [[nodiscard]] auto hardwareTimestamps() const noexcept -> bool {
        return timestamps_;
    }

    [[nodiscard]] auto statistics() const noexcept -> XDPStatistics;

    [[nodiscard]] auto nativeHandle() const noexcept -> int {
        return fd_;
    }

private:
    XDPCore(Backend& ring, XDPOptions options);

public:
    /// Validate options and allocate the UMEM. Throws std::system_error.
    [[nodiscard]] static auto create(Backend& ring, XDPOptions options) -> std::unique_ptr<XDPCore>;

private:
    void onCompletion(OpCode op, std::int32_t res, std::uint32_t flags) noexcept override;

    void onTxReady() noexcept override {}

    [[nodiscard]] auto retirable() const noexcept -> bool override {
        return (state_ == ConnectionState::Closed || state_ == ConnectionState::Idle) && inflight_ == 0;
    }

    void beginRetire() noexcept override {
        this->close();
    }

    [[nodiscard]] TURBOQ_FORCE_INLINE auto front() const noexcept -> xdp_desc const& {
        return static_cast<xdp_desc const*>(rxRing_.entries)[rxHead_ & mask_];
    }

    /// Look for frames the kernel produced since the last look; returns how many are queued now.
    /// Out of line: only runs when the user has drained what was seen.
    auto refresh() noexcept -> std::size_t;

    TURBOQ_FORCE_INLINE void release() noexcept {
        auto const address = this->front().addr & ~std::uint64_t{frameSize_ - 1};
        static_cast<std::uint64_t*>(fillRing_.entries)[fillTail_ & mask_] = address;
        ++fillTail_;
        std::atomic_ref{*fillRing_.producer}.store(fillTail_, std::memory_order_release);
        ++rxHead_;
        std::atomic_ref{*rxRing_.consumer}.store(rxHead_, std::memory_order_release);
        if (needWakeup_ && (std::atomic_ref{*fillRing_.flags}.load(std::memory_order_relaxed) & XDP_RING_NEED_WAKEUP))
            [[unlikely]] {
            this->wakeup();
        }
    }

    /// Let the driver run (it waits for that when XDP_RING_NEED_WAKEUP is set, or busy polling).
    void wakeup() noexcept;
    void armPoll() noexcept;

    auto failSync(std::error_code ec) noexcept -> std::unexpected<std::error_code>;
    void fail(std::error_code ec) noexcept;
    void beginClose() noexcept;
    void finishClose() noexcept;
};

} // namespace detail

/// Raw Ethernet frames straight from a NIC receive queue through AF_XDP:
///
///   rx.fetch()      -> the oldest frame, Ethernet header included (empty span if none)
///   rx.info()       -> its receive time and NIC hardware timestamp
///   rx.consume()    -> release it, its buffer goes back to the NIC
///
/// open() attaches a small XDP program to the interface that hands frames of the chosen queue
/// (UDP to the chosen ports, or everything) to this socket; the rest of the traffic goes through
/// the kernel stack as usual. In zero-copy mode the NIC writes frames into memory shared with
/// this process (the UMEM) and fetch() points right into it: no copy, no syscall per frame.
///
/// New frames are found by rx.empty()/rx.fetch() reading the kernel's ring directly, so a busy
/// loop needs no Reactor::poll() at all; Reactor::wait() wakes up on new frames unless
/// XDPOptions::wakeReactor is off. Headers are not parsed: the frame is yours.
///
/// Requirements: CAP_NET_ADMIN and CAP_BPF (or root), Linux 5.9+ (5.11+ for busy polling, 6.3+
/// for hardware timestamps). One XDP program per interface: open() fails with EBUSY if another
/// one (or another XDPConnection on the same interface) is attached.
template <typename Backend>
class XDPConnection {
private:
    detail::CorePtr<detail::XDPCore<Backend>> core_;

public:
    typename detail::XDPCore<Backend>::Rx rx;

    /// Create a socket served by `reactor` (which must outlive it): allocates the UMEM. Does not
    /// open: call open(). Throws std::system_error on invalid options or allocation failure.
    XDPConnection(Reactor<Backend>& reactor, XDPOptions options);

    XDPConnection(XDPConnection const&) = delete;
    XDPConnection& operator=(XDPConnection const&) = delete;

    XDPConnection(XDPConnection&& other) noexcept = default;

    XDPConnection& operator=(XDPConnection&& other) noexcept = default;

    /// Closes the socket and detaches the program if needed. Never blocks.
    ~XDPConnection() noexcept = default;

    /// Create the socket, attach the XDP program, join the groups. Synchronous: on success the
    /// connection is Ready, on failure Closed with error() set (and diagnostic() explaining
    /// XDP program failures). Allowed in Idle and Closed states. Drops frames still queued.
    auto open() -> std::expected<void, std::error_code> {
        return core_->open();
    }

    /// Start closing; Closed after in-flight operations completed (one of the next polls). The
    /// program is detached and the groups are left. Frames still queued are dropped.
    void close() noexcept {
        core_->close();
    }

    [[nodiscard]] auto state() const noexcept -> ConnectionState {
        return core_->state();
    }

    [[nodiscard]] auto error() const noexcept -> std::error_code {
        return core_->error();
    }

    /// What open() did or why it failed: XDP program attach attempts, the verifier log.
    [[nodiscard]] auto diagnostic() const noexcept -> std::string const& {
        return core_->diagnostic();
    }

    [[nodiscard]] auto options() const noexcept -> XDPOptions const& {
        return core_->options();
    }

    /// The socket runs in zero-copy mode (as of the last open()).
    [[nodiscard]] auto zeroCopy() const noexcept -> bool {
        return core_->zeroCopy();
    }

    /// The program is attached in native (driver) mode (as of the last open()).
    [[nodiscard]] auto nativeMode() const noexcept -> bool {
        return core_->nativeMode();
    }

    /// Frames carry NIC hardware timestamps (as of the last open()).
    [[nodiscard]] auto hardwareTimestamps() const noexcept -> bool {
        return core_->hardwareTimestamps();
    }

    /// Kernel counters (a getsockopt() call). Zero when closed.
    [[nodiscard]] auto statistics() const noexcept -> XDPStatistics {
        return core_->statistics();
    }

    /// Native socket descriptor (-1 when closed).
    [[nodiscard]] auto nativeHandle() const noexcept -> int {
        return core_->nativeHandle();
    }
};

} // namespace turboq::reactor
