// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "UDPConnection.h"

#include <liburing.h> // io_uring_recvmsg_out: the layout of received datagrams (both backends)

#include <linux/errqueue.h>
#include <linux/net_tstamp.h>
#include <net/if.h>
#include <netdb.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <ctime>

#include <turboq/Math.h>

#include "Error.h"
#include "Reactor.h"

#ifndef IPV6_MULTICAST_ALL
#define IPV6_MULTICAST_ALL 29 // linux/in6.h, missing from older libc headers
#endif

namespace turboq::reactor {
namespace {

constexpr unsigned kMaxBufferCount = 32768; // io_uring provided buffer ring limit

/// Anonymous, pre-faulted memory owned by a MappedRegion.
[[nodiscard]] auto mapAnonymous(std::size_t size) -> MappedRegion {
    void* const addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    if (addr == MAP_FAILED) {
        throw std::system_error{makePosixErrorCode(errno), "mmap"};
    }
    return MappedRegion{static_cast<std::byte*>(addr), size};
}

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

[[nodiscard]] auto toTimestamp(timespec const& ts) noexcept -> Timestamp {
    return Timestamp{std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec}};
}

} // namespace

namespace detail {

template <typename Backend>
UDPCore<Backend>::UDPCore(Backend& ring, UDPOptions options)
    : ring_{ring}, options_{std::move(options)},
      bufferCount_{upperPow2(std::clamp(options_.bufferCount, 1u, kMaxBufferCount))} {
    if (options_.maxDatagramSize == 0 || options_.maxDatagramSize > 65535 || options_.txBufferSize == 0 ||
        options_.txQueueDepth == 0) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "UDPCore"};
    }

    // Every receive buffer holds io_uring_recvmsg_out + sender address + control messages + payload.
    // The kernel lays the buffer out as out-header | name | control | payload with no padding, so
    // the name size is rounded up to keep the cmsghdr array 8-byte aligned.
    static_assert(sizeof(io_uring_recvmsg_out) % alignof(cmsghdr) == 0);
    recvTemplate_.msg_namelen = alignUp<socklen_t>(sizeof(sockaddr_in6), alignof(cmsghdr));
    recvTemplate_.msg_controllen = CMSG_SPACE(sizeof(scm_timestamping)) + CMSG_SPACE(sizeof(std::uint32_t));
    bufferLength_ = static_cast<unsigned>(sizeof(io_uring_recvmsg_out) + recvTemplate_.msg_namelen +
                                          recvTemplate_.msg_controllen + options_.maxDatagramSize);
    bufferStride_ = alignUp<std::size_t>(bufferLength_, kCacheLineSize);

    buffers_ = mapAnonymous(bufferCount_ * bufferStride_);

    rxEntries_ = std::make_unique<Entry[]>(bufferCount_);

    auto txBuffer = MirroredBuffer::create({.capacityHint = options_.txBufferSize});
    if (!txBuffer) {
        throw std::system_error{txBuffer.error(), "MirroredBuffer::create (tx)"};
    }
    txBuffer_ = std::move(*txBuffer);
    txLengths_.resize(upperPow2(std::size_t{options_.txQueueDepth}));
    txMask_ = txLengths_.size() - 1;

    // Last step, so that a throwing constructor never leaves registered buffers behind. They are
    // unregistered in onRetired() (or go away with the backend).
    pool_.emplace(ring_, buffers_.data(), bufferStride_, bufferLength_, bufferCount_);
}

template <typename Backend>
auto UDPCore<Backend>::create(Backend& ring, UDPOptions options) -> UDPCore* {
    return new UDPCore{ring, std::move(options)};
}

template <typename Backend>
void UDPCore<Backend>::onRetired() noexcept {
    pool_->release();
}

template <typename Backend>
UDPCore<Backend>::~UDPCore() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

template <typename Backend>
auto UDPCore<Backend>::open() -> std::expected<void, std::error_code> {
    if (state_ != ConnectionState::Idle && state_ != ConnectionState::Closed) {
        return std::unexpected(makeErrorCode(Error::InvalidState));
    }

    // Leftovers of the previous session: give their buffers back to the kernel.
    while (rxHead_ != rxTail_) {
        this->recycle(this->front().bufferId);
        ++rxHead_;
    }
    kernelDrops_ = 0;
    rxStalled_ = false;
    txBuffer_.clear();
    txHead_ = txTail_ = 0;
    prepared_ = 0;
    txErrors_ = 0;
    txLastError_.clear();
    txDirty_ = false;
    error_.clear();
    remoteLength_ = 0;

    auto const& group = options_.group;
    auto const& source = options_.source;
    auto const& local = options_.local;
    auto const& remote = options_.remote;
    if ((group && !group->address.isMulticast()) || (source && !group)) {
        return this->failSync(makeErrorCode(Error::InvalidOptions));
    }

    // The family comes from the group, an explicit local address or the destination; everything
    // given must agree on it.
    int family = AF_INET;
    if (group) {
        family = group->address.family();
    } else if (local) {
        family = local->address.family();
    } else if (remote) {
        family = remote->address.family();
    }
    if ((local && local->address.family() != family) || (remote && remote->address.family() != family) ||
        (source && source->family() != family)) {
        return this->failSync(makeErrorCode(Error::InvalidOptions));
    }
    if (remote) {
        remoteLength_ = remote->toSockaddr(remote_);
    }

    unsigned interfaceIndex = 0;
    if (options_.interface) {
        interfaceIndex = ::if_nametoindex(options_.interface->c_str());
        if (interfaceIndex == 0) {
            return this->failSync(makePosixErrorCode(errno));
        }
    }

    fd_ = ::socket(family, SOCK_DGRAM | SOCK_CLOEXEC, IPPROTO_UDP);
    if (fd_ < 0) {
        return this->failSync(makePosixErrorCode(errno));
    }

    if (options_.reuseAddress) {
        if (auto ec = setIntOption(fd_, SOL_SOCKET, SO_REUSEADDR, 1)) {
            return this->failSync(ec);
        }
    }
    if (options_.socketRecvBufferSize) {
        if (setIntOption(fd_, SOL_SOCKET, SO_RCVBUFFORCE, *options_.socketRecvBufferSize)) {
            if (auto ec = setIntOption(fd_, SOL_SOCKET, SO_RCVBUF, *options_.socketRecvBufferSize)) {
                return this->failSync(ec);
            }
        }
    }
    if (options_.socketSendBufferSize) {
        if (auto ec = setIntOption(fd_, SOL_SOCKET, SO_SNDBUF, *options_.socketSendBufferSize)) {
            return this->failSync(ec);
        }
    }
    if (auto ec = setIntOption(fd_, SOL_SOCKET, SO_RXQ_OVFL, 1)) {
        return this->failSync(ec);
    }
    if (options_.timestamping != Timestamping::None) {
        int flags = SOF_TIMESTAMPING_RX_SOFTWARE | SOF_TIMESTAMPING_SOFTWARE;
        if (options_.timestamping == Timestamping::Hardware) {
            flags |= SOF_TIMESTAMPING_RX_HARDWARE | SOF_TIMESTAMPING_RAW_HARDWARE;
        }
        if (auto ec = setIntOption(fd_, SOL_SOCKET, SO_TIMESTAMPING, flags)) {
            return this->failSync(ec);
        }
    }

    // Multicast options. IP_MULTICAST_ALL defaults to 1 on Linux, which delivers datagrams of every
    // group joined by any socket on the host to all sockets bound to the port: always turn it off.
    if (family == AF_INET) {
        std::error_code ec = setIntOption(fd_, IPPROTO_IP, IP_MULTICAST_ALL, 0);
        if (!ec) {
            ec = setIntOption(fd_, IPPROTO_IP, IP_MULTICAST_TTL, options_.multicastTtl);
        }
        if (!ec) {
            ec = setIntOption(fd_, IPPROTO_IP, IP_MULTICAST_LOOP, options_.multicastLoop ? 1 : 0);
        }
        if (!ec && interfaceIndex != 0) {
            ip_mreqn mreq{};
            mreq.imr_ifindex = static_cast<int>(interfaceIndex);
            ec = setOption(fd_, IPPROTO_IP, IP_MULTICAST_IF, &mreq, sizeof(mreq));
        }
        if (ec) {
            return this->failSync(ec);
        }
    } else {
        // IPV6_MULTICAST_ALL needs Linux 4.20; not fatal on older kernels.
        [[maybe_unused]] auto const ignored = setIntOption(fd_, IPPROTO_IPV6, IPV6_MULTICAST_ALL, 0);
        std::error_code ec = setIntOption(fd_, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, options_.multicastTtl);
        if (!ec) {
            ec = setIntOption(fd_, IPPROTO_IPV6, IPV6_MULTICAST_LOOP, options_.multicastLoop ? 1 : 0);
        }
        if (!ec && interfaceIndex != 0) {
            ec = setIntOption(fd_, IPPROTO_IPV6, IPV6_MULTICAST_IF, static_cast<int>(interfaceIndex));
        }
        if (ec) {
            return this->failSync(ec);
        }
    }

    // Bind.
    // No local endpoint: the wildcard address of the family and an ephemeral port.
    Endpoint bindTo =
        local.value_or(Endpoint{family == AF_INET6 ? IPAddress{IPv6Address::any()} : IPAddress{IPv4Address::any()}, 0});
    if (group) {
        bindTo.port = group->port;
        if (options_.bindToGroup) {
            bindTo.address = group->address;
        }
    }
    sockaddr_storage bindAddress{};
    socklen_t const bindLength = bindTo.toSockaddr(bindAddress);
    if (::bind(fd_, reinterpret_cast<sockaddr const*>(&bindAddress), bindLength) != 0) {
        return this->failSync(makePosixErrorCode(errno));
    }

    // Join (RFC 3678 protocol-independent API: works for both families, by interface index).
    if (group) {
        sockaddr_storage groupAddress{};
        socklen_t const groupLength = group->toSockaddr(groupAddress);
        int const level = family == AF_INET ? IPPROTO_IP : IPPROTO_IPV6;
        std::error_code ec;
        if (source) {
            sockaddr_storage sourceAddress{};
            socklen_t const sourceLength = Endpoint{*source, 0}.toSockaddr(sourceAddress);
            group_source_req req{};
            req.gsr_interface = interfaceIndex;
            std::memcpy(&req.gsr_group, &groupAddress, groupLength);
            std::memcpy(&req.gsr_source, &sourceAddress, sourceLength);
            ec = setOption(fd_, level, MCAST_JOIN_SOURCE_GROUP, &req, sizeof(req));
        } else {
            group_req req{};
            req.gr_interface = interfaceIndex;
            std::memcpy(&req.gr_group, &groupAddress, groupLength);
            ec = setOption(fd_, level, MCAST_JOIN_GROUP, &req, sizeof(req));
        }
        if (ec) {
            return this->failSync(ec);
        }
    }

    state_ = ConnectionState::Ready;
    this->armRecv();
    if (state_ != ConnectionState::Ready) {
        return std::unexpected(error_);
    }
    return {};
}

template <typename Backend>
void UDPCore<Backend>::close() noexcept {
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
auto UDPCore<Backend>::localEndpoint() const noexcept -> Endpoint {
    if (fd_ < 0) {
        return {};
    }
    sockaddr_storage address{};
    socklen_t length = sizeof(address);
    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
        return {};
    }
    return Endpoint::fromSockaddr(reinterpret_cast<sockaddr const*>(&address), length).value_or(Endpoint{});
}

template <typename Backend>
void UDPCore<Backend>::onCompletion(detail::OpCode op, std::int32_t res, std::uint32_t flags) noexcept {
    switch (op) {
    case detail::OpCode::Recv: {
        if (res >= 0 && (flags & detail::kCompletionBuffer)) [[likely]] {
            this->onDatagram(res, flags);
        } else if (res < 0 && res != -ENOBUFS && res != -ECANCELED && res != -EINTR) {
            // -ENOBUFS: every buffer is queued for the user; re-armed below or by consume().
            this->fail(makePosixErrorCode(-res));
        }
        if (!(flags & detail::kCompletionMore)) {
            // The multishot request has terminated (out of buffers, error, cancel, or the kernel
            // just decided to stop): re-arm if still running.
            recvInFlight_ = false;
            --inflight_;
            if (state_ == ConnectionState::Ready) {
                this->armRecv();
            }
        }
        break;
    }

    case detail::OpCode::Send:
        --inflight_;
        sendInFlight_ = false;
        if (res >= 0) [[likely]] {
            this->popDatagram();
        } else if (res == -ECANCELED) {
            // closing
        } else if (res != -EAGAIN && res != -EINTR) {
            this->dropDatagram(-res);
        }
        this->startSend();
        break;

    case detail::OpCode::Cancel: --inflight_; break;

    case detail::OpCode::Connect: [[fallthrough]];
    case detail::OpCode::Timer: [[fallthrough]];
    case detail::OpCode::Poll: assert(false); break;
    }

    if (state_ == ConnectionState::Closing && inflight_ == 0) {
        this->finishClose();
    }
}

template <typename Backend>
void UDPCore<Backend>::onDatagram(std::int32_t res, std::uint32_t flags) noexcept {
    auto const bufferId = static_cast<std::uint16_t>(flags >> detail::kCompletionBufferShift);
    auto* const buffer = buffers_.data() + std::size_t{bufferId} * bufferStride_;

    auto* const out = ::io_uring_recvmsg_validate(buffer, res, &recvTemplate_);
    if (!out) [[unlikely]] {
        this->recycle(bufferId);
        return;
    }

    assert(rxTail_ - rxHead_ < bufferCount_);
    auto& entry = rxEntries_[rxTail_ & (bufferCount_ - 1)];
    entry.bufferId = bufferId;
    entry.payload = {static_cast<std::byte const*>(::io_uring_recvmsg_payload(out, &recvTemplate_)),
        ::io_uring_recvmsg_payload_length(out, res, &recvTemplate_)};
    entry.info = DatagramInfo{};
    entry.info.receiveTime = ring_.now();
    entry.info.truncated = (out->flags & MSG_TRUNC) != 0;
    if (out->namelen > 0) {
        entry.info.source = Endpoint::fromSockaddr(static_cast<sockaddr const*>(::io_uring_recvmsg_name(out)),
            std::min<socklen_t>(out->namelen, recvTemplate_.msg_namelen))
                                .value_or(Endpoint{});
    }

    for (auto* cmsg = ::io_uring_recvmsg_cmsg_firsthdr(out, &recvTemplate_); cmsg != nullptr;
         cmsg = ::io_uring_recvmsg_cmsg_nexthdr(out, &recvTemplate_, cmsg)) {
        if (cmsg->cmsg_level != SOL_SOCKET) {
            continue;
        }
        if (cmsg->cmsg_type == SCM_TIMESTAMPING) {
            scm_timestamping ts;
            std::memcpy(&ts, CMSG_DATA(cmsg), sizeof(ts));
            entry.info.softwareTimestamp = toTimestamp(ts.ts[0]);
            entry.info.hardwareTimestamp = toTimestamp(ts.ts[2]);
        } else if (cmsg->cmsg_type == SO_RXQ_OVFL) {
            std::memcpy(&kernelDrops_, CMSG_DATA(cmsg), sizeof(kernelDrops_));
        }
    }

    ++rxTail_;
}

template <typename Backend>
void UDPCore<Backend>::onTxReady() noexcept {
    this->startSend();
}

template <typename Backend>
auto UDPCore<Backend>::failSync(std::error_code ec) noexcept -> std::unexpected<std::error_code> {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    remoteLength_ = 0;
    state_ = ConnectionState::Closed;
    error_ = ec;
    return std::unexpected(ec);
}

template <typename Backend>
void UDPCore<Backend>::fail(std::error_code ec) noexcept {
    if (state_ == ConnectionState::Closing || state_ == ConnectionState::Closed) {
        return;
    }
    error_ = ec;
    this->beginClose();
}

template <typename Backend>
void UDPCore<Backend>::beginClose() noexcept {
    state_ = ConnectionState::Closing;
    prepared_ = 0;
    txDirty_ = false;
    if (inflight_ == 0) {
        this->finishClose();
        return;
    }
    // shutdown() does not wake up operations on a UDP socket: cancel them.
    if (ring_.cancel(this, fd_)) {
        ++inflight_;
    }
}

template <typename Backend>
void UDPCore<Backend>::finishClose() noexcept {
    if (fd_ >= 0) {
        ring_.forget(fd_);
        ::close(fd_); // leaves the multicast group
        fd_ = -1;
    }
    recvInFlight_ = false;
    sendInFlight_ = false;
    state_ = ConnectionState::Closed;
}

template <typename Backend>
void UDPCore<Backend>::armRecv() noexcept {
    if (rxTail_ - rxHead_ == bufferCount_) {
        // Every buffer is queued for the user: nothing to receive into. consume() resumes.
        rxStalled_ = true;
        return;
    }
    rxStalled_ = false;

    if (!ring_.recvMultishot(this, fd_, &recvTemplate_, *pool_)) [[unlikely]] {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
        return;
    }
    recvInFlight_ = true;
    ++inflight_;
}

template <typename Backend>
void UDPCore<Backend>::resumeRecv() noexcept {
    if (state_ == ConnectionState::Ready && !recvInFlight_) {
        this->armRecv();
    }
}

template <typename Backend>
void UDPCore<Backend>::markTxDirty() noexcept {
    if (state_ == ConnectionState::Ready) {
        ring_.schedule(this);
    }
}

template <typename Backend>
void UDPCore<Backend>::flushTx() noexcept {
    if (state_ != ConnectionState::Ready || sendInFlight_) {
        return;
    }
    txDirty_ = false;
    if (txHead_ == txTail_) {
        return;
    }
    if (options_.directSend) {
        this->sendDirect();
    } else {
        this->startSend();
        ring_.submitNoThrow();
    }
}

template <typename Backend>
void UDPCore<Backend>::sendDirect() noexcept {
    while (txHead_ != txTail_) {
        auto const length = txLengths_[txHead_ & txMask_];
        auto const data = txBuffer_.readable();
        auto const rc = ::sendto(fd_, data.data(), length, MSG_DONTWAIT | MSG_NOSIGNAL,
            reinterpret_cast<sockaddr const*>(&remote_), remoteLength_);
        if (rc >= 0) [[likely]] {
            this->popDatagram();
        } else if (errno == EINTR) {
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // Socket send buffer is full: the backend waits for room.
            this->startSend();
            ring_.submitNoThrow();
            return;
        } else {
            this->dropDatagram(errno);
        }
    }
}

template <typename Backend>
void UDPCore<Backend>::startSend() noexcept {
    if (state_ != ConnectionState::Ready || sendInFlight_ || txHead_ == txTail_) {
        return;
    }
    // One datagram per request, one request in flight: keeps datagram order.
    sendIov_.iov_base = const_cast<std::byte*>(txBuffer_.readable().data());
    sendIov_.iov_len = txLengths_[txHead_ & txMask_];
    sendMsg_ = msghdr{};
    sendMsg_.msg_name = &remote_;
    sendMsg_.msg_namelen = remoteLength_;
    sendMsg_.msg_iov = &sendIov_;
    sendMsg_.msg_iovlen = 1;
    if (!ring_.sendMsg(this, fd_, &sendMsg_)) [[unlikely]] {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
        return;
    }
    sendInFlight_ = true;
    ++inflight_;
}

template <typename Backend>
void UDPCore<Backend>::popDatagram() noexcept {
    txBuffer_.consume(txLengths_[txHead_ & txMask_]);
    ++txHead_;
}

template <typename Backend>
void UDPCore<Backend>::dropDatagram(int error) noexcept {
    this->popDatagram();
    ++txErrors_;
    txLastError_ = makePosixErrorCode(error);
}

} // namespace detail

template <typename Backend>
UDPConnection<Backend>::UDPConnection(Reactor<Backend>& reactor, UDPOptions options)
    : core_{detail::UDPCore<Backend>::create(reactor.backend(), std::move(options))}, rx{core_->rx}, tx{core_->tx} {
    reactor.backend().attach(core_);
}

// The backends this library is built with.
namespace detail {
template class UDPCore<IoUringBackend>;
template class UDPCore<EpollBackend>;
} // namespace detail
template class UDPConnection<IoUringBackend>;
template class UDPConnection<EpollBackend>;

} // namespace turboq::reactor
