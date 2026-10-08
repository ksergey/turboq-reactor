// Copyright (c) Sergey Kovalevich <inndie@gmail.com>
// SPDX-License-Identifier: MIT

#include "UdpConnection.h"

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

[[nodiscard]] auto resolve(std::string const& host, std::uint16_t port, bool numeric, sockaddr_storage& address,
    socklen_t& addressLength) noexcept -> std::error_code {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_NUMERICSERV | (numeric ? AI_NUMERICHOST : 0);

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

/// Wildcard address of the given family.
void makeWildcard(int family, std::uint16_t port, sockaddr_storage& address, socklen_t& addressLength) noexcept {
    std::memset(&address, 0, sizeof(address));
    if (family == AF_INET6) {
        auto& in6 = reinterpret_cast<sockaddr_in6&>(address);
        in6.sin6_family = AF_INET6;
        in6.sin6_addr = in6addr_any;
        in6.sin6_port = htons(port);
        addressLength = sizeof(sockaddr_in6);
    } else {
        auto& in = reinterpret_cast<sockaddr_in&>(address);
        in.sin_family = AF_INET;
        in.sin_addr.s_addr = htonl(INADDR_ANY);
        in.sin_port = htons(port);
        addressLength = sizeof(sockaddr_in);
    }
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

[[nodiscard]] auto toNs(timespec const& ts) noexcept -> std::uint64_t {
    return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000u + static_cast<std::uint64_t>(ts.tv_nsec);
}

} // namespace

namespace detail {

UdpCore::UdpCore(Ring& ring, UdpOptions options, std::uint16_t bufferGroupId)
    : ring_{ring}, options_{std::move(options)}, bufferGroupId_{bufferGroupId},
      bufferCount_{upperPow2(std::clamp(options_.bufferCount, 1u, kMaxBufferCount))} {
    if (options_.maxDatagramSize == 0 || options_.maxDatagramSize > 65535 || options_.txBufferSize == 0 ||
        options_.txQueueDepth == 0) {
        throw std::system_error{makeErrorCode(Error::InvalidOptions), "UdpCore"};
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

    auto const pageSize = static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
    buffers_ = mapAnonymous(bufferCount_ * bufferStride_);
    bufferRingMemory_ = mapAnonymous(alignUp<std::size_t>(bufferCount_ * sizeof(io_uring_buf), pageSize));

    rxEntries_ = std::make_unique<Entry[]>(bufferCount_);

    auto txBuffer = MirroredBuffer::create({.capacityHint = options_.txBufferSize});
    if (!txBuffer) {
        throw std::system_error{txBuffer.error(), "MirroredBuffer::create (tx)"};
    }
    txBuffer_ = std::move(*txBuffer);
    txLengths_.resize(upperPow2(std::size_t{options_.txQueueDepth}));
    txMask_ = txLengths_.size() - 1;

    // Last step, so that a throwing constructor never leaves a registered ring behind. The ring is
    // unregistered in onRetired() (or goes away with the io_uring instance).
    bufferRing_ = reinterpret_cast<io_uring_buf_ring*>(bufferRingMemory_.data());
    ::io_uring_buf_ring_init(bufferRing_);
    io_uring_buf_reg reg{};
    reg.ring_addr = reinterpret_cast<std::uint64_t>(bufferRing_);
    reg.ring_entries = bufferCount_;
    reg.bgid = bufferGroupId_;
    if (int const rc = ::io_uring_register_buf_ring(ring_.native(), &reg, 0); rc < 0) {
        throw std::system_error{makePosixErrorCode(-rc), "io_uring_register_buf_ring"};
    }
    auto const mask = ::io_uring_buf_ring_mask(bufferCount_);
    for (unsigned i = 0; i < bufferCount_; ++i) {
        ::io_uring_buf_ring_add(bufferRing_, buffers_.data() + i * bufferStride_, bufferLength_,
            static_cast<unsigned short>(i), mask, static_cast<int>(i));
    }
    ::io_uring_buf_ring_advance(bufferRing_, static_cast<int>(bufferCount_));
}

auto UdpCore::create(Ring& ring, UdpOptions options) -> UdpCore* {
    auto const bufferGroupId = ring.allocateBufferGroup();
    try {
        return new UdpCore{ring, std::move(options), bufferGroupId};
    } catch (...) {
        ring.releaseBufferGroup(bufferGroupId);
        throw;
    }
}

void UdpCore::onRetired() noexcept {
    ::io_uring_unregister_buf_ring(ring_.native(), bufferGroupId_);
    ring_.releaseBufferGroup(bufferGroupId_);
}

UdpCore::~UdpCore() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

auto UdpCore::open() -> std::expected<void, std::error_code> {
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

    sockaddr_storage group{}, source{}, local{};
    socklen_t groupLength = 0, sourceLength = 0, localLength = 0;
    bool const multicast = !options_.group.empty();

    if (multicast) {
        if (auto ec = resolve(options_.group, options_.localPort, true, group, groupLength)) {
            return this->failSync(ec);
        }
        if (!options_.source.empty()) {
            if (auto ec = resolve(options_.source, 0, true, source, sourceLength)) {
                return this->failSync(ec);
            }
        }
    }
    if (!options_.remoteAddress.empty()) {
        if (auto ec = resolve(options_.remoteAddress, options_.remotePort, false, remote_, remoteLength_)) {
            return this->failSync(ec);
        }
    }
    if (!options_.localAddress.empty()) {
        if (auto ec = resolve(options_.localAddress, options_.localPort, true, local, localLength)) {
            return this->failSync(ec);
        }
    }

    int family = AF_INET;
    if (multicast) {
        family = group.ss_family;
    } else if (localLength > 0) {
        family = local.ss_family;
    } else if (remoteLength_ > 0) {
        family = remote_.ss_family;
    }
    if ((localLength > 0 && local.ss_family != family) || (remoteLength_ > 0 && remote_.ss_family != family) ||
        (sourceLength > 0 && source.ss_family != family)) {
        return this->failSync(makeErrorCode(Error::InvalidOptions));
    }

    unsigned interfaceIndex = 0;
    if (!options_.interface.empty()) {
        interfaceIndex = ::if_nametoindex(options_.interface.c_str());
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
    if (options_.socketRecvBufferSize > 0) {
        if (setIntOption(fd_, SOL_SOCKET, SO_RCVBUFFORCE, options_.socketRecvBufferSize)) {
            if (auto ec = setIntOption(fd_, SOL_SOCKET, SO_RCVBUF, options_.socketRecvBufferSize)) {
                return this->failSync(ec);
            }
        }
    }
    if (options_.socketSendBufferSize > 0) {
        if (auto ec = setIntOption(fd_, SOL_SOCKET, SO_SNDBUF, options_.socketSendBufferSize)) {
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
    sockaddr_storage bindAddress{};
    socklen_t bindLength = 0;
    if (multicast && options_.bindToGroup) {
        bindAddress = group;
        bindLength = groupLength;
    } else if (localLength > 0) {
        bindAddress = local;
        bindLength = localLength;
    } else {
        makeWildcard(family, options_.localPort, bindAddress, bindLength);
    }
    if (::bind(fd_, reinterpret_cast<sockaddr const*>(&bindAddress), bindLength) != 0) {
        return this->failSync(makePosixErrorCode(errno));
    }

    // Join (RFC 3678 protocol-independent API: works for both families, by interface index).
    if (multicast) {
        int const level = family == AF_INET ? IPPROTO_IP : IPPROTO_IPV6;
        std::error_code ec;
        if (sourceLength > 0) {
            group_source_req req{};
            req.gsr_interface = interfaceIndex;
            std::memcpy(&req.gsr_group, &group, groupLength);
            std::memcpy(&req.gsr_source, &source, sourceLength);
            ec = setOption(fd_, level, MCAST_JOIN_SOURCE_GROUP, &req, sizeof(req));
        } else {
            group_req req{};
            req.gr_interface = interfaceIndex;
            std::memcpy(&req.gr_group, &group, groupLength);
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

void UdpCore::close() noexcept {
    switch (state_) {
    case ConnectionState::Idle: state_ = ConnectionState::Closed; break;
    case ConnectionState::Connecting: [[fallthrough]];
    case ConnectionState::Handshaking: [[fallthrough]];
    case ConnectionState::Ready: this->beginClose(); break;
    case ConnectionState::Closing: [[fallthrough]];
    case ConnectionState::Closed: break;
    }
}

auto UdpCore::localPort() const noexcept -> std::uint16_t {
    if (fd_ < 0) {
        return 0;
    }
    sockaddr_storage address{};
    socklen_t length = sizeof(address);
    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
        return 0;
    }
    if (address.ss_family == AF_INET6) {
        return ntohs(reinterpret_cast<sockaddr_in6 const&>(address).sin6_port);
    }
    return ntohs(reinterpret_cast<sockaddr_in const&>(address).sin_port);
}

void UdpCore::onCompletion(detail::OpCode op, std::int32_t res, std::uint32_t flags) noexcept {
    switch (op) {
    case detail::OpCode::Recv: {
        if (res >= 0 && (flags & IORING_CQE_F_BUFFER)) [[likely]] {
            this->onDatagram(res, flags);
        } else if (res < 0 && res != -ENOBUFS && res != -ECANCELED && res != -EINTR) {
            // -ENOBUFS: every buffer is queued for the user; re-armed below or by consume().
            this->fail(makePosixErrorCode(-res));
        }
        if (!(flags & IORING_CQE_F_MORE)) {
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
    case detail::OpCode::ConnectTimeout: [[fallthrough]];
    case detail::OpCode::HandshakePoll: assert(false); break;
    }

    if (state_ == ConnectionState::Closing && inflight_ == 0) {
        this->finishClose();
    }
}

void UdpCore::onDatagram(std::int32_t res, std::uint32_t flags) noexcept {
    auto const bufferId = static_cast<std::uint16_t>(flags >> IORING_CQE_BUFFER_SHIFT);
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
    entry.info.receiveTimeNs = ring_.now();
    entry.info.truncated = (out->flags & MSG_TRUNC) != 0;
    if (out->namelen > 0) {
        entry.info.source = static_cast<sockaddr const*>(::io_uring_recvmsg_name(out));
        entry.info.sourceLength = std::min<socklen_t>(out->namelen, recvTemplate_.msg_namelen);
    }

    for (auto* cmsg = ::io_uring_recvmsg_cmsg_firsthdr(out, &recvTemplate_); cmsg != nullptr;
         cmsg = ::io_uring_recvmsg_cmsg_nexthdr(out, &recvTemplate_, cmsg)) {
        if (cmsg->cmsg_level != SOL_SOCKET) {
            continue;
        }
        if (cmsg->cmsg_type == SCM_TIMESTAMPING) {
            scm_timestamping ts;
            std::memcpy(&ts, CMSG_DATA(cmsg), sizeof(ts));
            entry.info.softwareTimestampNs = toNs(ts.ts[0]);
            entry.info.hardwareTimestampNs = toNs(ts.ts[2]);
        } else if (cmsg->cmsg_type == SO_RXQ_OVFL) {
            std::memcpy(&kernelDrops_, CMSG_DATA(cmsg), sizeof(kernelDrops_));
        }
    }

    ++rxTail_;
}

void UdpCore::onTxReady() noexcept {
    this->startSend();
}

auto UdpCore::failSync(std::error_code ec) noexcept -> std::unexpected<std::error_code> {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    remoteLength_ = 0;
    state_ = ConnectionState::Closed;
    error_ = ec;
    return std::unexpected(ec);
}

void UdpCore::fail(std::error_code ec) noexcept {
    if (state_ == ConnectionState::Closing || state_ == ConnectionState::Closed) {
        return;
    }
    error_ = ec;
    this->beginClose();
}

void UdpCore::beginClose() noexcept {
    state_ = ConnectionState::Closing;
    prepared_ = 0;
    txDirty_ = false;
    if (inflight_ == 0) {
        this->finishClose();
        return;
    }
    // shutdown() does not wake up operations on a UDP socket: cancel them.
    if (auto* sqe = ring_.getSqe(); sqe) {
        ::io_uring_prep_cancel_fd(sqe, fd_, IORING_ASYNC_CANCEL_ALL);
        ::io_uring_sqe_set_data64(sqe, detail::encodeUserData(this, detail::OpCode::Cancel));
        ++inflight_;
    }
}

void UdpCore::finishClose() noexcept {
    if (fd_ >= 0) {
        ::close(fd_); // leaves the multicast group
        fd_ = -1;
    }
    recvInFlight_ = false;
    sendInFlight_ = false;
    state_ = ConnectionState::Closed;
}

void UdpCore::armRecv() noexcept {
    if (rxTail_ - rxHead_ == bufferCount_) {
        // Every buffer is queued for the user: nothing to receive into. consume() resumes.
        rxStalled_ = true;
        return;
    }
    rxStalled_ = false;

    auto* sqe = ring_.getSqe();
    if (!sqe) [[unlikely]] {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
        return;
    }
    ::io_uring_prep_recvmsg_multishot(sqe, fd_, &recvTemplate_, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = bufferGroupId_;
    ::io_uring_sqe_set_data64(sqe, detail::encodeUserData(this, detail::OpCode::Recv));
    recvInFlight_ = true;
    ++inflight_;
}

void UdpCore::resumeRecv() noexcept {
    if (state_ == ConnectionState::Ready && !recvInFlight_) {
        this->armRecv();
    }
}

void UdpCore::markTxDirty() noexcept {
    if (state_ == ConnectionState::Ready) {
        ring_.schedule(this);
    }
}

void UdpCore::flushTx() noexcept {
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

void UdpCore::sendDirect() noexcept {
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
            // Socket send buffer is full: io_uring waits for room.
            this->startSend();
            ring_.submitNoThrow();
            return;
        } else {
            this->dropDatagram(errno);
        }
    }
}

void UdpCore::startSend() noexcept {
    if (state_ != ConnectionState::Ready || sendInFlight_ || txHead_ == txTail_) {
        return;
    }
    auto* sqe = ring_.getSqe();
    if (!sqe) [[unlikely]] {
        this->fail(makeErrorCode(Error::SubmissionQueueFull));
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
    ::io_uring_prep_sendmsg(sqe, fd_, &sendMsg_, MSG_NOSIGNAL);
    ::io_uring_sqe_set_data64(sqe, detail::encodeUserData(this, detail::OpCode::Send));
    sendInFlight_ = true;
    ++inflight_;
}

void UdpCore::popDatagram() noexcept {
    txBuffer_.consume(txLengths_[txHead_ & txMask_]);
    ++txHead_;
}

void UdpCore::dropDatagram(int error) noexcept {
    this->popDatagram();
    ++txErrors_;
    txLastError_ = makePosixErrorCode(error);
}

} // namespace detail

UdpConnection::UdpConnection(Reactor& reactor, UdpOptions options)
    : core_{detail::UdpCore::create(reactor.ring(), std::move(options))}, rx{core_->rx}, tx{core_->tx} {
    reactor.ring().attach(core_);
}

} // namespace turboq::reactor
