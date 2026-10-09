// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "XDPConnection.h"

#include <net/if.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <bit>
#include <format>

#include <turboq/Math.h>

#include "Error.h"
#include "Reactor.h"

#ifndef SO_PREFER_BUSY_POLL
#define SO_PREFER_BUSY_POLL 69 // asm-generic/socket.h, Linux 5.11
#endif
#ifndef SO_BUSY_POLL_BUDGET
#define SO_BUSY_POLL_BUDGET 70
#endif

namespace turboq::reactor {
namespace {

constexpr unsigned kMaxFrameCount = 1u << 20;
// The completion ring is mandatory but only used for transmit, which this socket does not do.
constexpr int kCompletionRingSize = 64;

[[nodiscard]] auto setOption(
    int fd, int level, int name, void const* value, socklen_t length) noexcept -> std::error_code {
    if (::setsockopt(fd, level, name, value, length) != 0) {
        return makePosixErrorCode(errno);
    }
    return {};
}

[[nodiscard]] auto setIntOption(int fd, int level, int name, int value) noexcept -> std::error_code {
    return setOption(fd, level, name, &value, sizeof(value));
}

/// bind() an AF_XDP socket. EBUSY is retried for a while: a socket closed just before (by this
/// process too) releases the queue asynchronously, from a kernel work queue.
[[nodiscard]] auto bindSocket(int fd, sockaddr_xdp const& address) noexcept -> int {
    constexpr int kAttempts = 50;
    for (int attempt = 0;; ++attempt) {
        if (::bind(fd, std::bit_cast<sockaddr const*>(&address), sizeof(address)) == 0) {
            return 0;
        }
        if (errno != EBUSY || attempt + 1 == kAttempts) {
            return errno;
        }
        ::usleep(10'000);
    }
}

/// mmap() of one of the socket's rings.
[[nodiscard]] auto mapRing(
    int fd, std::size_t size, off_t offset) noexcept -> std::expected<MappedRegion, std::error_code> {
    void* const addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, offset);
    if (addr == MAP_FAILED) {
        return std::unexpected(makePosixErrorCode(errno));
    }
    return MappedRegion{static_cast<std::byte*>(addr), size};
}

} // namespace

namespace detail {

template <typename Backend>
XDPCore<Backend>::XDPCore(Backend& ring, XDPOptions options)
    : ring_{ring}, options_{std::move(options)},
      frameCount_{upperPow2(std::clamp(options_.frameCount, 64u, kMaxFrameCount))}, frameSize_{options_.frameSize},
      mask_{frameCount_ - 1} {
    auto const pageSize = static_cast<unsigned>(::sysconf(_SC_PAGESIZE));
    if (options_.interface.empty() || !std::has_single_bit(frameSize_) || frameSize_ < 2048 || frameSize_ > pageSize ||
        options_.redirectAll != options_.udpPorts.empty() || (options_.source && options_.groups.empty())) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "XDPCore"};
    }
    for (auto const& group : options_.groups) {
        if (!group.isMulticast() || (options_.source && options_.source->family() != group.family())) {
            throw std::system_error{makeErrorCode(Error::InvalidOptions), "XDPCore"};
        }
    }

    auto const size = std::size_t{frameCount_} * frameSize_;
    void* const addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    if (addr == MAP_FAILED) {
        throw std::system_error{makePosixErrorCode(errno), "mmap (UMEM)"};
    }
    umem_ = MappedRegion{static_cast<std::byte*>(addr), size};
}

template <typename Backend>
auto XDPCore<Backend>::create(Backend& ring, XDPOptions options) -> std::unique_ptr<XDPCore> {
    return std::unique_ptr<XDPCore>{new XDPCore{ring, std::move(options)}};
}

template <typename Backend>
XDPCore<Backend>::~XDPCore() noexcept {
    memberships_.clear();
    program_.reset();
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

template <typename Backend>
auto XDPCore<Backend>::open() -> std::expected<void, std::error_code> {
    if (state_ != ConnectionState::Idle && state_ != ConnectionState::Closed) {
        return std::unexpected(makeErrorCode(Error::InvalidState));
    }
    rxHead_ = rxCachedTail_ = fillTail_ = 0;
    batchTime_ = {};
    error_.clear();
    diagnostic_.clear();
    zeroCopy_ = native_ = timestamps_ = false;

    unsigned const ifindex = ::if_nametoindex(options_.interface.c_str());
    if (ifindex == 0) {
        return this->failSync(makePosixErrorCode(errno));
    }

    fd_ = ::socket(AF_XDP, SOCK_RAW | SOCK_CLOEXEC, 0);
    if (fd_ < 0) {
        return this->failSync(makePosixErrorCode(errno));
    }

    // UMEM and rings.
    xdp_umem_reg reg{};
    reg.addr = std::bit_cast<std::uintptr_t>(umem_.data());
    reg.len = umem_.size();
    reg.chunk_size = frameSize_;
    reg.headroom = 0;
    int const ringSize = static_cast<int>(frameCount_);
    std::error_code ec = setOption(fd_, SOL_XDP, XDP_UMEM_REG, &reg, sizeof(reg));
    if (!ec) {
        ec = setIntOption(fd_, SOL_XDP, XDP_UMEM_FILL_RING, ringSize);
    }
    if (!ec) {
        ec = setIntOption(fd_, SOL_XDP, XDP_UMEM_COMPLETION_RING, kCompletionRingSize);
    }
    if (!ec) {
        ec = setIntOption(fd_, SOL_XDP, XDP_RX_RING, ringSize);
    }
    if (ec) {
        return this->failSync(ec);
    }

    xdp_mmap_offsets offsets{};
    socklen_t length = sizeof(offsets);
    if (::getsockopt(fd_, SOL_XDP, XDP_MMAP_OFFSETS, &offsets, &length) != 0) {
        return this->failSync(makePosixErrorCode(errno));
    }
    auto rxMemory = mapRing(fd_, offsets.rx.desc + std::size_t{frameCount_} * sizeof(xdp_desc), XDP_PGOFF_RX_RING);
    if (!rxMemory) {
        return this->failSync(rxMemory.error());
    }
    auto fillMemory =
        mapRing(fd_, offsets.fr.desc + std::size_t{frameCount_} * sizeof(std::uint64_t), XDP_UMEM_PGOFF_FILL_RING);
    if (!fillMemory) {
        return this->failSync(fillMemory.error());
    }
    auto const setRing = [](Ring& ring, MappedRegion memory, xdp_ring_offset const& offset) {
        ring.memory = std::move(memory);
        auto* const base = ring.memory.data();
        ring.producer = std::bit_cast<std::uint32_t*>(base + offset.producer);
        ring.consumer = std::bit_cast<std::uint32_t*>(base + offset.consumer);
        ring.flags = std::bit_cast<std::uint32_t*>(base + offset.flags);
        ring.entries = base + offset.desc;
    };
    setRing(rxRing_, std::move(*rxMemory), offsets.rx);
    setRing(fillRing_, std::move(*fillMemory), offsets.fr);

    if (options_.busyPoll) {
        ec = setIntOption(fd_, SOL_SOCKET, SO_PREFER_BUSY_POLL, 1);
        if (!ec) {
            ec = setIntOption(fd_, SOL_SOCKET, SO_BUSY_POLL, static_cast<int>(options_.busyPoll->count()));
        }
        if (!ec) {
            ec = setIntOption(fd_, SOL_SOCKET, SO_BUSY_POLL_BUDGET, static_cast<int>(options_.busyPollBudget));
        }
        if (ec) {
            return this->failSync(ec);
        }
    }

    // Bind to the queue: zero-copy first unless disabled. XDP_USE_NEED_WAKEUP lets the driver sleep
    // until told otherwise (fill ring flag), instead of polling the fill ring.
    sockaddr_xdp address{};
    address.sxdp_family = AF_XDP;
    address.sxdp_ifindex = ifindex;
    address.sxdp_queue_id = options_.queue;
    bool bound = false;
    if (options_.zeroCopy != XDPZeroCopy::Disable) {
        address.sxdp_flags = XDP_USE_NEED_WAKEUP | XDP_ZEROCOPY;
        int const error = bindSocket(fd_, address);
        if (error == 0) {
            bound = true;
        } else if (error == EBUSY) {
            diagnostic_ = std::format("bind: queue {} of {} is in use; ", options_.queue, options_.interface);
            return this->failSync(makePosixErrorCode(error));
        } else if (options_.zeroCopy == XDPZeroCopy::Require) {
            diagnostic_ = std::format("bind (zero-copy): {}; ", std::strerror(error));
            return this->failSync(makeErrorCode(Error::XdpZeroCopyUnavailable));
        } else {
            diagnostic_ = std::format("zero-copy unavailable ({}), copy mode; ", std::strerror(error));
        }
    }
    if (!bound) {
        address.sxdp_flags = XDP_USE_NEED_WAKEUP | XDP_COPY;
        if (int const error = bindSocket(fd_, address); error != 0) {
            diagnostic_ += std::format("bind: {}; ", std::strerror(error));
            return this->failSync(makePosixErrorCode(error));
        }
    }
    needWakeup_ = true;
    xdp_options socketOptions{};
    length = sizeof(socketOptions);
    if (::getsockopt(fd_, SOL_XDP, XDP_OPTIONS, &socketOptions, &length) == 0) {
        zeroCopy_ = (socketOptions.flags & XDP_OPTIONS_ZEROCOPY) != 0;
    }

    // Every frame goes to the NIC.
    auto* const fill = static_cast<std::uint64_t*>(fillRing_.entries);
    for (std::uint32_t i = 0; i < frameCount_; ++i) {
        fill[i] = std::uint64_t{i} * frameSize_;
    }
    fillTail_ = frameCount_;
    std::atomic_ref{*fillRing_.producer}.store(fillTail_, std::memory_order_release);

    // The program, then this socket into its map.
    std::string programDiagnostic;
    auto program = XdpProgram::attach(
        {
            .ifindex = ifindex,
            .maxQueue = options_.queue,
            .udpPorts = options_.udpPorts,
            .allowNative = options_.attachMode != XDPAttachMode::Generic,
            .allowGeneric = options_.attachMode != XDPAttachMode::Native,
            .timestamps = options_.hardwareTimestamps,
        },
        programDiagnostic);
    diagnostic_ += programDiagnostic;
    if (!program) {
        return this->failSync(program.error());
    }
    program_.emplace(std::move(*program));
    native_ = program_->native();
    timestamps_ = program_->timestamps();
    if (auto ec2 = program_->addSocket(options_.queue, fd_)) {
        diagnostic_ += std::format("XSKMAP update: {}; ", ec2.message());
        return this->failSync(ec2);
    }

    // Multicast memberships (RFC 3678 protocol-independent API).
    for (auto const& group : options_.groups) {
        UniqueFd socket{::socket(group.family(), SOCK_DGRAM | SOCK_CLOEXEC, 0)};
        if (!socket) {
            return this->failSync(makePosixErrorCode(errno));
        }
        int const level = group.family() == AF_INET ? IPPROTO_IP : IPPROTO_IPV6;
        sockaddr_storage groupAddress{};
        socklen_t const groupLength = Endpoint{group, 0}.toSockaddr(groupAddress);
        if (options_.source) {
            sockaddr_storage sourceAddress{};
            socklen_t const sourceLength = Endpoint{*options_.source, 0}.toSockaddr(sourceAddress);
            group_source_req req{};
            req.gsr_interface = ifindex;
            std::memcpy(&req.gsr_group, &groupAddress, groupLength);
            std::memcpy(&req.gsr_source, &sourceAddress, sourceLength);
            ec = setOption(socket.get(), level, MCAST_JOIN_SOURCE_GROUP, &req, sizeof(req));
        } else {
            group_req req{};
            req.gr_interface = ifindex;
            std::memcpy(&req.gr_group, &groupAddress, groupLength);
            ec = setOption(socket.get(), level, MCAST_JOIN_GROUP, &req, sizeof(req));
        }
        if (ec) {
            return this->failSync(ec);
        }
        memberships_.push_back(std::move(socket));
    }

    diagnostic_ += std::format("{} mode, {}{}", native_ ? "native" : "generic", zeroCopy_ ? "zero-copy" : "copy",
        timestamps_ ? ", hardware timestamps" : "");

    state_ = ConnectionState::Ready;
    this->wakeup();
    if (options_.wakeReactor) {
        this->armPoll();
    }
    if (state_ != ConnectionState::Ready) {
        return std::unexpected(error_);
    }
    return {};
}

template <typename Backend>
void XDPCore<Backend>::close() noexcept {
    switch (state_) {
    case ConnectionState::Idle: state_ = ConnectionState::Closed; break;
    case ConnectionState::Connecting: [[fallthrough]];
    case ConnectionState::Handshaking: [[fallthrough]];
    case ConnectionState::Ready: this->beginClose(); break;
    case ConnectionState::Closing: [[fallthrough]];
    case ConnectionState::Closed: break;
    }
}

template <typename Backend>
auto XDPCore<Backend>::statistics() const noexcept -> XDPStatistics {
    if (fd_ < 0) {
        return {};
    }
    xdp_statistics stats{};
    socklen_t length = sizeof(stats);
    if (::getsockopt(fd_, SOL_XDP, XDP_STATISTICS, &stats, &length) != 0) {
        return {};
    }
    return {
        .dropped = stats.rx_dropped,
        .rxQueueFull = stats.rx_ring_full,
        .fillQueueEmpty = stats.rx_fill_ring_empty_descs,
        .invalidDescriptors = stats.rx_invalid_descs,
    };
}

template <typename Backend>
auto XDPCore<Backend>::refresh() noexcept -> std::size_t {
    if (state_ != ConnectionState::Ready) {
        return 0;
    }
    auto tail = std::atomic_ref{*rxRing_.producer}.load(std::memory_order_acquire);
    if (tail == rxCachedTail_) {
        // Nothing new. Let the driver run if it asked for it (or we busy poll), then look again.
        if (options_.busyPoll ||
            (std::atomic_ref{*fillRing_.flags}.load(std::memory_order_relaxed) & XDP_RING_NEED_WAKEUP)) {
            this->wakeup();
            tail = std::atomic_ref{*rxRing_.producer}.load(std::memory_order_acquire);
        }
        if (tail == rxCachedTail_) {
            if (options_.wakeReactor && !pollArmed_) {
                this->armPoll();
            }
            return 0;
        }
    }
    rxCachedTail_ = tail;
    batchTime_ = std::chrono::time_point_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now());
    return rxCachedTail_ - rxHead_;
}

template <typename Backend>
void XDPCore<Backend>::wakeup() noexcept {
    // A zero-length receive runs the driver's NAPI (or busy polls); the data comes in the ring.
    [[maybe_unused]] auto const rc = ::recvfrom(fd_, nullptr, 0, MSG_DONTWAIT, nullptr, nullptr);
}

template <typename Backend>
void XDPCore<Backend>::armPoll() noexcept {
    if (!ring_.pollFd(this, fd_, POLLIN, nullptr)) [[unlikely]] {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
        return;
    }
    pollArmed_ = true;
    ++inflight_;
}

template <typename Backend>
void XDPCore<Backend>::onCompletion(OpCode op, std::int32_t res, std::uint32_t /*flags*/) noexcept {
    switch (op) {
    case OpCode::Poll:
        // Frames arrived (or closing). Re-armed by the rx check that finds the queue empty.
        --inflight_;
        pollArmed_ = false;
        if (res < 0 && res != -ECANCELED && res != -EINTR) {
            this->fail(makePosixErrorCode(-res));
        }
        break;
    case OpCode::Cancel: --inflight_; break;
    case OpCode::Connect: [[fallthrough]];
    case OpCode::Timer: [[fallthrough]];
    case OpCode::Recv: [[fallthrough]];
    case OpCode::Send: assert(false); break;
    }
    if (state_ == ConnectionState::Closing && inflight_ == 0) {
        this->finishClose();
    }
}

template <typename Backend>
auto XDPCore<Backend>::failSync(std::error_code ec) noexcept -> std::unexpected<std::error_code> {
    memberships_.clear();
    program_.reset();
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    rxRing_ = Ring{};
    fillRing_ = Ring{};
    state_ = ConnectionState::Closed;
    error_ = ec;
    return std::unexpected(ec);
}

template <typename Backend>
void XDPCore<Backend>::fail(std::error_code ec) noexcept {
    if (state_ == ConnectionState::Closing || state_ == ConnectionState::Closed) {
        return;
    }
    error_ = ec;
    this->beginClose();
}

template <typename Backend>
void XDPCore<Backend>::beginClose() noexcept {
    state_ = ConnectionState::Closing;
    if (inflight_ == 0) {
        this->finishClose();
        return;
    }
    if (ring_.cancel(this, fd_)) {
        ++inflight_;
    }
}

template <typename Backend>
void XDPCore<Backend>::finishClose() noexcept {
    memberships_.clear(); // leaves the groups
    program_.reset();     // detaches the program: frames go to the kernel again
    if (fd_ >= 0) {
        ring_.forget(fd_);
        ::close(fd_);
        fd_ = -1;
    }
    rxRing_ = Ring{};
    fillRing_ = Ring{};
    rxHead_ = rxCachedTail_ = 0;
    pollArmed_ = false;
    state_ = ConnectionState::Closed;
}

} // namespace detail

template <typename Backend>
XDPConnection<Backend>::XDPConnection(Reactor<Backend>& reactor, XDPOptions options)
    : core_{detail::XDPCore<Backend>::create(reactor.backend(), std::move(options))}, rx{core_->rx} {
    reactor.backend().attach(core_.get());
}

// The backends this library is built with.
namespace detail {
#if TURBOQ_REACTOR_IO_URING
template class XDPCore<IoUringBackend>;
#endif
template class XDPCore<EpollBackend>;
} // namespace detail
#if TURBOQ_REACTOR_IO_URING
template class XDPConnection<IoUringBackend>;
#endif
template class XDPConnection<EpollBackend>;

} // namespace turboq::reactor
